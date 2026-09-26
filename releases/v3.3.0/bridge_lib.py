#!/usr/bin/env python3
"""Shared transport, execution and file helpers for supabase-bridge 3.x.

This module is a library only. It has no daemon, no queue lifecycle, no entry
point and no legacy status vocabulary. agent_bridge.py owns coordination and
workspace_ops.py owns the structured operations. No exactly-once guarantee is
made for external side effects.
"""

import base64
import json
import os
import select
import shutil
import signal
import socket
import stat
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

VERSION = "supabase-bridge 3.2"  # agent_bridge.py sets the authoritative value
BRIDGE_DIR = Path(__file__).resolve().parent
CONFIG_PATH = BRIDGE_DIR / "config.json"
STATE_PATH = BRIDGE_DIR / "state.json"
ENV_PATH = BRIDGE_DIR / ".env"
LOG_PATH = BRIDGE_DIR / "bridge.log"
OUTPUT_DIR = BRIDGE_DIR / "outputs"
LOG_MAX_BYTES = 5_000_000

TAIL_KEEP = 50_000          # bytes of tail kept inline when output is spilled
HEAD_LIMIT = 2_000_000      # max bytes buffered in memory for the inline result
DISK_CAP = 1_000_000_000    # per-command bytes written to disk (flood guard)
HEARTBEAT_EVERY = 30.0      # seconds between progress heartbeats during long commands

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
            with urllib.request.urlopen(req, timeout=8) as r:
                raw = r.read()
                self.last_headers = {k.lower(): v for k, v in r.headers.items()}
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

    def claim(self, cmd_id, runner):
        """pending -> running, atomically; False if the row moved underneath us."""
        code, rows = self.rest(
            "PATCH", f"/rest/v1/agent_commands?id=eq.{cmd_id}&status=eq.pending&select=id",
            {"status": "running", "started_at": now_iso(), "claimed_by": runner})
        return code in (200, 204) and bool(rows)

    def finish(self, cmd_id, fields):
        for attempt in range(3):
            sd_notify('WATCHDOG=1')
            try:
                code, rows = self.rest('PATCH', f'/rest/v1/agent_commands?id=eq.{cmd_id}&status=in.(running,pending)&select=id', fields)
                if code == 200 and rows: return True
            except BridgeError as e:
                log(f'result delivery failed for {cmd_id}: {e}')
            time.sleep(attempt + 1)
        return False

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
