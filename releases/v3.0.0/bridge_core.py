#!/usr/bin/env python3
"""Supabase bridge 2.5. Full-trust commands with bounded file workers.
Claims are never automatically re-executed. Unknown outcomes require review.
Completion receipts and the executed flag support result redelivery.
No exactly-once guarantee is made for external side effects.
"""

import argparse
import tempfile
import stat
import uuid
import fcntl
import base64
import json
import os
import select
import shutil
import signal
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path
from urllib.parse import quote

VERSION = "supabase-bridge 2.5"
BRIDGE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BRIDGE_DIR / "config.json"
STATE_PATH = BRIDGE_DIR / "state.json"
ENV_PATH = BRIDGE_DIR / ".env"
STOP_PATH = BRIDGE_DIR / "STOP"
LOG_PATH = BRIDGE_DIR / "bridge.log"
OUTPUT_DIR = BRIDGE_DIR / "outputs"
# receipts are persisted to the first writable location (fallbacks survive a
# full primary disk); every write is fsynced before rename
RECEIPT_DIRS = [OUTPUT_DIR,
                Path.home() / ".local/share/supabase-bridge/receipts",
                Path("/tmp/supabase-bridge-receipts")]
LOG_MAX_BYTES = 5_000_000

TAIL_KEEP = 50_000          # bytes of tail kept inline when output is spilled
HEAD_LIMIT = 2_000_000      # max bytes buffered in memory for the inline result
DISK_CAP = 1_000_000_000    # per-command bytes written to disk (flood guard)
HEARTBEAT_EVERY = 30.0      # seconds between heartbeats during long commands
OUTPUT_RETENTION = 7 * 86400

DEFAULT_CONFIG = {
    "poll_interval_sec": 3,
    "exec_timeout_sec": 600,
    "max_per_poll": 10,
    "output_max_chars": 1_000_000,
    "claim_grace_sec": 0,        # legacy; grace now computed per-row in the RPC
    "orphan_grace_sec": 86400,   # legacy; kept for config compatibility
    "workdir": str(BRIDGE_DIR / "workdir"),
}

STOP_REQUESTED = False
CURRENT_CHILD = None  # running subprocess, so SIGTERM can interrupt it


def log(msg):
    try:
        if LOG_PATH.exists() and LOG_PATH.stat().st_size > LOG_MAX_BYTES:
            LOG_PATH.replace(LOG_PATH.with_suffix(".log.1"))
    except OSError:
        pass
    line = f"{datetime.now(timezone.utc).isoformat(timespec='seconds')} {msg}"
    print(line, flush=True)
    try:
        with open(LOG_PATH, "a") as f:
            f.write(line + "\n")
    except OSError:
        pass


def sd_notify(payload):
    """Best-effort systemd notification (Type=notify / WatchdogSec support)."""
    addr = os.environ.get("NOTIFY_SOCKET")
    if not addr:
        return
    if addr.startswith("@"):
        addr = "\0" + addr[1:]
    try:
        with socket.socket(socket.AF_UNIX, socket.SOCK_DGRAM) as s:
            s.connect(addr)
            s.send(payload.encode())
    except OSError as e:
        log(f"sd_notify failed: {e}")


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# ---- config / state ------------------------------------------------------

def load_config():
    cfg = dict(DEFAULT_CONFIG)
    if CONFIG_PATH.exists():
        cfg.update(json.loads(CONFIG_PATH.read_text()))
    validate_config(cfg)
    return cfg


def validate_config(cfg):
    ints = {
        "poll_interval_sec": (1, 3600), "exec_timeout_sec": (1, 7200),
        "max_per_poll": (1, 100),
        "output_max_chars": (1000, 100_000_000), "orphan_grace_sec": (60, 30 * 86400),
    }
    for k, (lo, hi) in ints.items():
        if not isinstance(cfg.get(k), int) or not lo <= cfg[k] <= hi:
            raise ValueError(f"config.{k} must be an int in [{lo}, {hi}], got {cfg.get(k)!r}")
    g = cfg.get("claim_grace_sec", 0)
    if not isinstance(g, int) or g < 0 or g > 86400:
        raise ValueError(f"config.claim_grace_sec must be an int in [0, 86400], got {g!r}")
    if not isinstance(cfg.get("workdir"), str):
        raise ValueError("config.workdir must be a string")


def load_state():
    if STATE_PATH.exists():
        try:
            return json.loads(STATE_PATH.read_text())
        except json.JSONDecodeError:
            log("state.json unreadable — starting fresh")
    return {"stats": {"done": 0, "error": 0}}


def save_state(state):
    state.setdefault("stats", {"done": 0, "error": 0})
    atomic_write(STATE_PATH, json.dumps(state, indent=1).encode())  # durable


def load_creds():
    """SUPABASE_URL plus credentials, from the environment, else .env.

    Either a service key (bypasses RLS) or a publishable key + BRIDGE_SECRET
    (RLS policies and the reaper RPCs match the x-bridge-secret request header
    against a secret stored in the project's vault).
    """
    file_vals = {}
    if ENV_PATH.exists():
        for line in ENV_PATH.read_text().splitlines():
            if "=" in line and not line.lstrip().startswith("#"):
                k, v = line.split("=", 1)
                file_vals.setdefault(k.strip(), v.strip())

    def get(*names):
        for n in names:
            v = os.environ.get(n) or file_vals.get(n)
            if v:
                return v
        return None

    url = get("SUPABASE_URL")
    service_key = get("SUPABASE_SERVICE_KEY", "SUPABASE_KEY")
    pub_key = get("SUPABASE_PUBLISHABLE_KEY", "SUPABASE_ANON_KEY")
    secret = get("BRIDGE_SECRET")

    if not url:
        sys.exit(f"missing SUPABASE_URL in {ENV_PATH} (chmod 600) or environment")
    if service_key:
        return url.rstrip("/"), service_key, None
    if pub_key and secret:
        return url.rstrip("/"), pub_key, secret
    sys.exit("missing credentials — put SUPABASE_SERVICE_KEY=... (or "
             "SUPABASE_PUBLISHABLE_KEY=... + BRIDGE_SECRET=...) in "
             f"{ENV_PATH} (chmod 600) or export them")


class BridgeError(Exception):
    """Supabase-side failure — triggers backoff, not a crash."""


class Supabase:
    def __init__(self, cfg, url, key, secret=None):
        self.base = url
        self.key = key
        self.secret = secret  # x-bridge-secret header value (publishable-key mode)

    def rest(self, method, path, body=None, prefer="return=representation"):
        url = f"{self.base}{path}"
        data = json.dumps(body).encode() if body is not None else None
        headers = {
            "apikey": self.key,
            "Authorization": f"Bearer {self.key}",
            "Content-Type": "application/json",
            "Prefer": prefer,
        }
        if self.secret:
            headers["x-bridge-secret"] = self.secret
        req = urllib.request.Request(url, data=data, method=method, headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=30) as r:
                raw = r.read()
                return r.status, json.loads(raw) if raw else []
        except urllib.error.HTTPError as e:
            raw = e.read()
            try:
                body_json = json.loads(raw) if raw else []
            except json.JSONDecodeError:
                body_json = [{"error": raw.decode(errors="replace")[:300]}]
            raise BridgeError(f"{method} {path.split('?')[0]} -> HTTP {e.code}: {str(body_json)[:200]}")
        except (urllib.error.URLError, TimeoutError, ConnectionError) as e:
            raise BridgeError(f"{method} {path.split('?')[0]} unreachable: {e}")

    def rpc(self, fn, body):
        code, rows = self.rest("POST", f"/rest/v1/rpc/{fn}", body)
        return rows or []

    # ---- agent_commands -------------------------------------------------
    def fetch_pending(self, limit):
        code, rows = self.rest(
            "GET", f"/rest/v1/agent_commands?status=eq.pending"
                   f"&order=created_at.asc&limit={limit}"
                   f"&select=id,command,timeout_sec,kind,payload,executed,created_at")
        return rows

    def fetch_undelivered(self, runner):
        _, rows = self.rest('GET', '/rest/v1/agent_commands?claimed_by=eq.' + quote(runner, safe='') + '&status=eq.running&order=created_at.asc&limit=100&select=id,kind,executed')
        return rows

    def claim(self, cmd_id, runner):
        """pending -> running, atomically; False if the row moved underneath us."""
        code, rows = self.rest(
            "PATCH", f"/rest/v1/agent_commands?id=eq.{cmd_id}&status=eq.pending&select=id",
            {"status": "running", "started_at": now_iso(), "claimed_by": runner})
        return code in (200, 204) and bool(rows)

    def mark_executed(self, cmd_id):
        for attempt in range(3):
            sd_notify('WATCHDOG=1')
            try:
                code, rows = self.rest('PATCH', f'/rest/v1/agent_commands?id=eq.{cmd_id}&status=in.(running,pending,blocked)&select=id', {'executed': True})
                if code == 200 and rows: return True
            except BridgeError as e:
                log(f'executed marker failed for {cmd_id}: {e}')
            time.sleep(1)
        return False

    def finish(self, cmd_id, fields):
        for attempt in range(3):
            sd_notify('WATCHDOG=1')
            try:
                code, rows = self.rest('PATCH', f'/rest/v1/agent_commands?id=eq.{cmd_id}&status=in.(running,pending,blocked)&select=id', fields)
                if code == 200 and rows: return True
            except BridgeError as e:
                log(f'result delivery failed for {cmd_id}: {e}')
            time.sleep(attempt + 1)
        return False

    # ---- reapers (RPCs; grace respects each row's own timeout_sec) --------
    def requeue_stale(self, runner, default_timeout):
        rows = self.rpc("requeue_stale_bridge",
                        {"p_runner": runner, "p_default_timeout": default_timeout})
        return len(rows)

    def reap_orphans(self, default_timeout):
        rows = self.rpc("reap_orphaned_commands", {"p_default_timeout": default_timeout})
        return len(rows)

    # ---- heartbeat --------------------------------------------------------
    def heartbeat(self, state):
        self.rest('POST', '/rest/v1/bridge_status', {'id':1,'last_seen':now_iso(),'version':VERSION,'hostname':socket.gethostname(),'pid':os.getpid(),'stats':state.get('stats',{}),'last_error':state.get('last_error'),'last_error_at':state.get('last_error_at')}, prefer='resolution=merge-duplicates,return=minimal')

    def heartbeat_alive(self):
        """Light heartbeat for use DURING long command execution (best-effort)."""
        try:
            self.rest("POST", "/rest/v1/bridge_status",
                      {"id": 1, "last_seen": now_iso(),
                       "hostname": socket.gethostname(), "pid": os.getpid()},
                      prefer="resolution=merge-duplicates,return=minimal")
        except BridgeError:
            pass


# ---- execution ---------------------------------------------------------

def child_env():
    env = os.environ.copy()  # full trust: children get a natural environment
    env.pop("NOTIFY_SOCKET", None)  # children must not talk to our systemd watchdog
    env.setdefault("HOME", os.path.expanduser("~"))
    env["PATH"] = "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:" \
                  + env.get("PATH", "")
    env.setdefault("LANG", "C.UTF-8")
    env.setdefault("TERM", "dumb")
    return env


def kill_proc_group(proc, sig=signal.SIGKILL):
    try:
        os.killpg(proc.pid, sig)
    except (ProcessLookupError, PermissionError):
        try:
            proc.kill()
        except OSError:
            pass


def execute(cfg, cmd_id, script, timeout, progress):
    """Run script, streaming merged stdout+stderr RAW to outputs/<id>.log.

    Memory-bounded (head + tail windows only), disk-bounded (DISK_CAP),
    watchdog pings and heartbeats continue while the command runs.
    Returns (meta, head_bytes, tail_bytes). Raises RuntimeError (before
    spawning) if the output file cannot be opened (e.g. disk full).
    """
    out_path = OUTPUT_DIR / f"{cmd_id}.log"
    head_cap = min(cfg["output_max_chars"], HEAD_LIMIT) + 1
    try:
        f = open(out_path, "wb", buffering=0)  # fail BEFORE spawning if disk is full
    except OSError as e:
        raise RuntimeError(f"cannot open {out_path}: {e}")

    global CURRENT_CHILD
    proc = None
    total = discarded = 0
    head = bytearray()
    tail = bytearray()  # last TAIL_KEEP bytes, trimmed exactly
    timed_out = False
    t0 = time.monotonic()
    deadline = t0 + timeout
    last_ping = [t0]

    def ping():
        sd_notify("WATCHDOG=1")
        if time.monotonic() - last_ping[0] >= HEARTBEAT_EVERY:
            last_ping[0] = time.monotonic()
            progress()  # light heartbeat

    try:
        proc = subprocess.Popen(
            ["bash", "-c", script], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL, cwd=cfg["workdir"], env=child_env(),
            start_new_session=True)
        CURRENT_CHILD = proc
        fd = proc.stdout.fileno()
        code = None
        while not timed_out:
            now = time.monotonic()
            remaining = deadline - now
            if remaining <= 0:
                kill_proc_group(proc)
                timed_out = True
                break
            r, _, _ = select.select([fd], [], [], min(remaining, 5.0))
            if r:
                try:
                    chunk = os.read(fd, 65536)
                except BlockingIOError:
                    continue
                if not chunk:
                    break  # output closed; the process may still be running
                writable = max(0, min(len(chunk), DISK_CAP - total))
                if writable: f.write(chunk[:writable])
                discarded += len(chunk) - writable
                total += len(chunk)
                if len(head) < head_cap:
                    head += chunk[:head_cap - len(head)]
                tail += chunk
                if len(tail) > TAIL_KEEP:
                    del tail[:len(tail) - TAIL_KEEP]  # byte-exact tail window
                if now - last_ping[0] >= HEARTBEAT_EVERY:
                    ping()
            else:
                ping()  # idle slice: keep watchdog + heartbeat alive
        # Output closed (or deadline hit). If the process is still alive, wait
        # until its ACTUAL deadline — daemonizing commands must not be killed
        # just because they stopped printing. Pings continue throughout.
        while not timed_out:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                kill_proc_group(proc)
                timed_out = True
                break
            try:
                code = proc.wait(timeout=min(remaining, 5.0))
                break
            except subprocess.TimeoutExpired:
                ping()
    finally:
        CURRENT_CHILD = None
        try:
            if proc is not None and proc.poll() is None:
                kill_proc_group(proc)
                try: proc.wait(timeout=5)
                except subprocess.TimeoutExpired: pass
        finally:
            if proc is not None and proc.stdout is not None: proc.stdout.close()
            try:
                f.flush(); os.fsync(f.fileno())
            finally: f.close()

    duration_ms = int((time.monotonic() - t0) * 1000)
    if timed_out:
        exit_code = 124
    elif code is not None and code < 0:
        exit_code = code  # negative = killed by signal -N
    else:
        exit_code = code if code is not None else 126
    meta = {"exit_code": exit_code, "duration_ms": duration_ms,
            "finished_at": now_iso(), "total_bytes": total,
            "file": str(out_path), "discarded_bytes": discarded,
            "timed_out": timed_out,
            "signal": -code if (code or 0) < 0 else None}
    return meta, bytes(head), bytes(tail)


def atomic_write(path, data):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix='.' + path.name + '.', dir=path.parent)
    try:
        with os.fdopen(fd, 'wb') as f:
            f.write(data); f.flush(); os.fsync(f.fileno())
        os.replace(tmp, path)
        dfd = os.open(path.parent, os.O_RDONLY | getattr(os,'O_DIRECTORY',0))
        try: os.fsync(dfd)
        finally: os.close(dfd)
    finally:
        if os.path.exists(tmp): os.unlink(tmp)


def save_meta(cmd_id, meta):
    return persist_receipt(cmd_id, meta, '.json')


def load_meta(cmd_id):
    for d in RECEIPT_DIRS:
        p = d / f'{cmd_id}.json'
        try:
            if p.is_symlink(): continue
            meta = json.loads(p.read_text())
            if isinstance(meta, dict) and isinstance(meta.get('exit_code'), int): return meta
        except (OSError,ValueError): pass
    return None


def _note(meta):
    bits = [f"full output ({meta['total_bytes']} bytes) saved to {meta['file']}"]
    if meta.get("discarded_bytes"):
        bits.append(f"{meta['discarded_bytes']} further bytes discarded "
                    f"(disk cap {DISK_CAP})")
    return ("[... output spilled — " + "; ".join(bits)
            + f"; retrieve more with e.g. sed -n '1000,1200p' {meta['file']} ...]")


def inline_result(meta, head, tail):
    prefix = ''
    if meta.get('timed_out'): prefix += '[timed out]\n'
    if meta.get('signal'): prefix += f"[terminated by signal {meta['signal']}]\n"
    if meta.get('discarded_bytes'): prefix += f"[output loss: {meta['discarded_bytes']} bytes discarded at the {DISK_CAP}-byte disk limit]\n"
    total = meta['total_bytes']
    if total <= len(head): return prefix + head.decode('utf-8','replace')
    shown_head=head[:TAIL_KEEP]
    omitted=max(0,total-len(shown_head)-len(tail))
    return prefix + shown_head.decode('utf-8','replace') + f'\n[... {omitted} bytes omitted; {_note(meta)} ...]\n' + tail.decode('utf-8','replace')


def inline_result_from_disk(meta):
    """Rebuild the inline preview from the durable file (re-delivery path)."""
    p = Path(meta["file"])
    head = tail = b""
    try:
        with open(p, "rb") as f:
            head = f.read(TAIL_KEEP)
            written = p.stat().st_size
            if written > len(head):
                f.seek(-min(TAIL_KEEP, written - len(head)), os.SEEK_END)
                tail = f.read()
    except OSError as e:
        return f"[result file unavailable: {e}]"
    return inline_result(meta, head, tail)


def deliver(sb, cmd_id, meta, result):
    status = "done" if meta["exit_code"] == 0 else "error"
    return sb.finish(cmd_id, {"status": status, "result": result,
                              "exit_code": meta["exit_code"],
                              "duration_ms": meta["duration_ms"],
                              "finished_at": now_iso()})


def redeliver(sb, cmd_id, meta):
    if meta is None:
        return block_uncertain(sb, cmd_id, 'Execution was claimed, but no durable completion receipt is available. Inspect external effects before submitting a new command.')
    result = meta['result'] if 'result' in meta else inline_result_from_disk(meta)
    return deliver(sb, cmd_id, meta, result)


# ---- structured file tools ----------------------------------------------

def run_file_op(cfg, kind, payload):
    t0=time.monotonic()
    try:
        if not isinstance(payload,dict): raise ValueError('payload must be an object')
        raw=payload.get('path')
        if not isinstance(raw,str) or not raw: raise ValueError('payload.path must be a nonempty string')
        path=Path(os.path.expanduser(raw))
        if not path.is_absolute(): path=Path(cfg['workdir'])/path
        def number(key,default,lo,hi):
            value=payload.get(key,default)
            if isinstance(value,bool) or not isinstance(value,int) or not lo<=value<=hi: raise ValueError(f'{key} must be an integer in [{lo},{hi}]')
            return value
        if kind=='file.read':
            offset=number('offset',0,0,2**63-1); length=number('length',600000,1,4000000)
            fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK)
            with os.fdopen(fd,'rb') as f:
                st=os.fstat(f.fileno())
                if not stat.S_ISREG(st.st_mode): raise ValueError('file.read requires a regular file')
                f.seek(offset); data=f.read(length)
            body={'path':str(path),'size':st.st_size,'offset':offset,'bytes_read':len(data),'has_more':offset+len(data)<st.st_size}
            if payload.get('text'): body['text']=data.decode('utf-8','replace')
            else: body['base64']=base64.b64encode(data).decode()
        elif kind=='file.write':
            if 'text' in payload and 'content_base64' in payload: raise ValueError('use text or content_base64, not both')
            if 'content_base64' in payload:
                encoded=payload['content_base64']
                if not isinstance(encoded,str) or len(encoded)>5333336: raise ValueError('base64 write exceeds 4000000 bytes')
                data=base64.b64decode(encoded,validate=True)
            else:
                text=payload.get('text','')
                if not isinstance(text,str): raise ValueError('text must be a string')
                data=text.encode()
            if len(data)>4000000: raise ValueError('write exceeds 4000000 bytes; use smaller chunks')
            if payload.get('mkdirs',True): path.parent.mkdir(parents=True,exist_ok=True)
            append=bool(payload.get('append',False))
            fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_NONBLOCK|(os.O_APPEND if append else 0),0o600)
            with os.fdopen(fd,'wb') as f:
                if not stat.S_ISREG(os.fstat(f.fileno()).st_mode): raise ValueError('file.write requires a regular file')
                if not append: os.ftruncate(f.fileno(),0)
                f.write(data); f.flush(); os.fsync(f.fileno())
            body={'path':str(path),'written':len(data),'append':append}
        elif kind=='file.list':
            offset=number('offset',0,0,10000); limit=number('limit',200,1,1000)
            entries=[]; truncated=False
            with os.scandir(path) as iterator:
                for entry in iterator:
                    if len(entries)>=10000: truncated=True; break
                    st=entry.stat(follow_symlinks=False)
                    entries.append({'name':entry.name,'size':st.st_size,'is_dir':entry.is_dir(follow_symlinks=False),'mtime':datetime.fromtimestamp(st.st_mtime,timezone.utc).isoformat()})
            entries.sort(key=lambda e:e['name'])
            page=entries[offset:offset+limit]
            body={'path':str(path),'entries':page,'offset':offset,'next_offset':offset+len(page),'has_more':offset+len(page)<len(entries),'truncated':truncated}
        elif kind=='file.stat':
            st=path.stat(); body={'path':str(path),'size':st.st_size,'is_dir':stat.S_ISDIR(st.st_mode),'is_file':stat.S_ISREG(st.st_mode),'mtime':datetime.fromtimestamp(st.st_mtime,timezone.utc).isoformat()}
        elif kind=='file.mkdir':
            path.mkdir(parents=True,exist_ok=bool(payload.get('exist_ok',True))); body={'path':str(path),'created':True}
        elif kind=='file.delete':
            if path.is_symlink(): path.unlink()
            elif path.is_dir():
                if payload.get('recursive'): shutil.rmtree(path)
                else: path.rmdir()
            else: path.unlink()
            body={'path':str(path),'deleted':True}
        else: raise ValueError(f'unsupported file operation {kind}')
        code=0
    except Exception as e:
        code=1; body={'error':f'{type(e).__name__}: {e}'}
    result=json.dumps(body,ensure_ascii=True)
    return {'kind':kind,'exit_code':code,'duration_ms':int((time.monotonic()-t0)*1000),'finished_at':now_iso(),'total_bytes':len(result.encode()),'result':result}


def housekeep(sb):
    cutoff=time.time()-OUTPUT_RETENTION
    artifacts={}
    for d in dict.fromkeys(RECEIPT_DIRS+[OUTPUT_DIR]):
        try:
            for p in d.iterdir():
                if p.is_symlink() or not p.is_file() or p.suffix not in ('.json','.log','.intent'): continue
                try: uuid.UUID(p.stem)
                except ValueError: continue
                artifacts.setdefault(p.stem,[]).append(p)
        except OSError: continue
    ids=sorted(cid for cid,paths in artifacts.items() if all(p.stat().st_mtime<cutoff for p in paths))
    for i in range(0,len(ids),100):
        batch=ids[i:i+100]
        try:
            code,rows=sb.rest('GET','/rest/v1/agent_commands?select=id,status,finished_at&id=in.('+','.join(batch)+')')
        except BridgeError: continue
        if code!=200: continue
        for row in rows:
            cid=row['id']
            if cid not in artifacts or row['status'] not in ('done','error') or not row.get('finished_at'): continue
            try: finished=datetime.fromisoformat(row['finished_at'].replace('Z','+00:00')).timestamp()
            except (TypeError,ValueError): continue
            if finished>=cutoff: continue
            try:
                for p in artifacts[cid]: p.unlink(missing_ok=True)
            except OSError: continue
            try:
                sb.rest('DELETE','/rest/v1/agent_commands?id=eq.'+cid+'&status=in.(done,error)',prefer='return=minimal')
            except BridgeError: pass
    # Rows with no remaining artifacts may be removed. Never infer absence of
    # a row from a missing entry in a bulk response, and retain blocked rows.
    cut=quote(datetime.fromtimestamp(cutoff,timezone.utc).isoformat(),safe='')
    try: code,rows=sb.rest('GET','/rest/v1/agent_commands?select=id&status=in.(done,error)&finished_at=lt.'+cut+'&order=finished_at.asc&limit=100')
    except BridgeError: return
    if code==200:
        for row in rows:
            cid=row['id']
            if any((d/(cid+suffix)).exists() for d in dict.fromkeys(RECEIPT_DIRS+[OUTPUT_DIR]) for suffix in ('.json','.log','.intent')): continue
            try: sb.rest('DELETE','/rest/v1/agent_commands?id=eq.'+cid+'&status=in.(done,error)',prefer='return=minimal')
            except BridgeError: pass


# ---- main loop ---------------------------------------------------------

def process_once(cfg, runner, dry_run=False):
    url,key,secret=load_creds(); sb=Supabase(cfg,url,key,secret); state=load_state()
    state.setdefault('stats',{'done':0,'error':0})
    for keyname in ('done','error','blocked'): state['stats'].setdefault(keyname,0)
    # With the exclusive daemon lock, these claims belong to an earlier pass.
    recovery=sb.fetch_undelivered(runner)
    pending=sb.fetch_pending(cfg['max_per_poll'])
    if dry_run:
        log(f'dry run: {len(pending)} pending, {len(recovery)} recovery rows'); return
    OUTPUT_DIR.mkdir(parents=True,exist_ok=True)
    for row in recovery:
        if STOP_REQUESTED: break
        meta=load_meta(row['id'])
        redeliver(sb,row['id'],meta)
    sb.requeue_stale(runner,cfg['exec_timeout_sec'])
    sb.reap_orphans(cfg['exec_timeout_sec'])
    def progress(): sb.heartbeat_alive()
    for row in pending:
        if STOP_REQUESTED: break
        sd_notify('WATCHDOG=1'); cid=row['id']; kind=row.get('kind') or 'shell'
        meta=load_meta(cid)
        if meta is not None or row.get('executed') or has_intent(cid):
            redeliver(sb,cid,meta); continue
        if not sb.claim(cid,runner): continue
        try:
            persist_receipt(cid,{'claimed_at':now_iso(),'runner':runner,'kind':kind},'.intent')
            timeout=row.get('timeout_sec') or cfg['exec_timeout_sec']
            if kind=='shell':
                meta,head,tail=execute(cfg,cid,(row.get('command') or '').strip(),timeout,progress)
                result=inline_result(meta,head,tail)
            else:
                meta=run_file_bounded(cfg,kind,row.get('payload'),timeout,progress)
                result=meta['result']
            try: save_meta(cid,meta)
            except RuntimeError as e: log(f'receipt persistence failed for {cid}: {e}')
            sb.mark_executed(cid)
            if deliver(sb,cid,meta,result): state['stats']['done' if meta['exit_code']==0 else 'error']+=1
        except Exception as e:
            log(f'execution outcome requires review for {cid}: {type(e).__name__}: {e}')
            if block_uncertain(sb,cid,'Execution or completion recording was interrupted. '+type(e).__name__+': '+str(e)[:300]): state['stats']['blocked']+=1
    housekeep(sb)
    state.pop('last_error',None); state.pop('last_error_at',None)
    save_state(state); sb.heartbeat(state)


def _request_stop(signo, _frame):
    global STOP_REQUESTED
    STOP_REQUESTED = True
    child = CURRENT_CHILD
    if child is not None:
        kill_proc_group(child, signal.SIGTERM)


def sleep_interruptible(seconds):
    deadline = time.monotonic() + seconds
    while time.monotonic() < deadline:
        if STOP_REQUESTED or STOP_PATH.exists():
            return
        time.sleep(0.25)


def loop(cfg, dry_run=False):
    runner = socket.gethostname()
    sd_notify("READY=1")
    log(f"loop mode started ({VERSION}, host {runner}) — touch {STOP_PATH} "
        f"or send SIGTERM to halt")
    streak = 0
    while not STOP_REQUESTED and not STOP_PATH.exists():
        sd_notify("WATCHDOG=1")
        try:
            process_once(cfg, runner, dry_run=dry_run)
            if streak:
                log(f"recovered after {streak} failed pass(es)")
            streak = 0
            delay = cfg["poll_interval_sec"]
        except BridgeError as e:
            streak += 1
            if streak == 1 or streak % 10 == 0:
                log(f"supabase unreachable (streak {streak}): {e}")
            delay = min(cfg["poll_interval_sec"] * (2 ** min(streak - 1, 4)), 300)
        except Exception as e:
            streak += 1
            if streak == 1 or streak % 10 == 0:
                log(f"pass error (streak {streak}): {type(e).__name__}: {e}")
            delay = min(cfg["poll_interval_sec"] * (2 ** min(streak - 1, 4)), 300)
        sleep_interruptible(delay)

    if STOP_PATH.exists():
        STOP_PATH.unlink()
        log("STOP file found — exiting")
    else:
        log("SIGTERM received — exiting cleanly")
    sd_notify("STOPPING=1")


def main():
    ap = argparse.ArgumentParser(description="Supabase <-> local machine command bridge")
    ap.add_argument("--once", action="store_true", help="single pass, then exit")
    ap.add_argument("--loop", action="store_true", help="poll forever")
    ap.add_argument("--dry-run", action="store_true", help="show what would run, execute nothing")
    args = ap.parse_args()

    cfg = load_config()  # raises on invalid config -> systemd restarts & journals it
    Path(cfg["workdir"]).mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    if args.once:
        process_once(cfg, socket.gethostname(), dry_run=args.dry_run)
    elif args.loop:
        signal.signal(signal.SIGTERM, _request_stop)
        signal.signal(signal.SIGINT, _request_stop)
        loop(cfg, dry_run=args.dry_run)
    else:
        ap.error("choose --once or --loop")



def persist_receipt(cmd_id, meta, suffix):
    data=json.dumps(meta,ensure_ascii=True).encode(); last=None
    for d in RECEIPT_DIRS:
        try:
            if d.is_symlink(): raise OSError('receipt directory must not be a symlink')
            d.mkdir(parents=True,exist_ok=True,mode=0o700)
            if d.stat().st_uid!=os.getuid(): raise OSError('receipt directory owner mismatch')
            d.chmod(0o700)
            atomic_write(d/(cmd_id+suffix),data)
            return d/(cmd_id+suffix)
        except OSError as e: last=e
    raise RuntimeError(f'no writable receipt location: {last}')

def has_intent(cmd_id):
    return any((d/(cmd_id+'.intent')).exists() for d in RECEIPT_DIRS)

def block_uncertain(sb,cmd_id,reason):
    return sb.finish(cmd_id,{'status':'blocked','exit_code':125,'result':'[manual review required; not re-executing] '+reason,'finished_at':now_iso()})

def run_file_bounded(cfg,kind,payload,timeout,progress):
    global CURRENT_CHILD
    started=time.monotonic(); last_hb=started
    with tempfile.TemporaryDirectory(prefix='supabase-file-worker-') as directory:
        request=Path(directory)/'request.json'; response=Path(directory)/'response.json'
        atomic_write(request,json.dumps({'cfg':{'workdir':cfg['workdir']},'kind':kind,'payload':payload}).encode())
        proc=subprocess.Popen([sys.executable,str(Path(__file__).resolve()),'--file-worker',str(request),str(response)],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL,env=child_env(),start_new_session=True)
        CURRENT_CHILD=proc
        timed_out=False
        try:
            while True:
                remaining=timeout-(time.monotonic()-started)
                if remaining<=0:
                    timed_out=True; kill_proc_group(proc); proc.wait(timeout=5); break
                try: proc.wait(timeout=min(remaining,1.0)); break
                except subprocess.TimeoutExpired:
                    sd_notify('WATCHDOG=1')
                    if time.monotonic()-last_hb>=HEARTBEAT_EVERY:
                        last_hb=time.monotonic(); progress()
            if timed_out:
                return {'kind':kind,'exit_code':124,'duration_ms':int((time.monotonic()-started)*1000),'finished_at':now_iso(),'total_bytes':0,'result':json.dumps({'error':'file operation timed out; partial file changes are possible'})}
            if proc.returncode!=0 or not response.exists(): raise RuntimeError('file worker ended without a completion record; effects may be partial')
            if response.stat().st_size>32_000_000: raise RuntimeError('file response exceeds 32 MB')
            meta=json.loads(response.read_text()); meta['duration_ms']=int((time.monotonic()-started)*1000)
            return meta
        finally:
            CURRENT_CHILD=None
            if proc.poll() is None:
                kill_proc_group(proc)
                try: proc.wait(timeout=5)
                except subprocess.TimeoutExpired: pass

def file_worker(request,response):
    request=Path(request)
    if request.stat().st_size>32_000_000: raise ValueError('request too large')
    body=json.loads(request.read_text())
    meta=run_file_op(body['cfg'],body['kind'],body['payload'])
    atomic_write(Path(response),json.dumps(meta).encode())

def acquire_daemon_lock():
    global DAEMON_LOCK
    DAEMON_LOCK=open(BRIDGE_DIR/'.daemon.lock','a')
    try: fcntl.flock(DAEMON_LOCK.fileno(),fcntl.LOCK_EX|fcntl.LOCK_NB)
    except BlockingIOError: raise SystemExit('another bridge instance holds the daemon lock')

if __name__ == "__main__":
    if len(sys.argv)==4 and sys.argv[1]=="--file-worker":
        file_worker(sys.argv[2],sys.argv[3])
    else:
        acquire_daemon_lock()
        main()
