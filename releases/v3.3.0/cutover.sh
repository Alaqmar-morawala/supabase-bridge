#!/usr/bin/env bash
# Supervised cutover of the supabase-bridge coordinator to a release directory, with automatic rollback.
# Usage: cutover.sh <new_release> <old_release> <expected_new_version> [health_timeout_sec]
# Run it detached from any bridge job so the coordinator restart cannot kill it, for example:
#   systemd-run --user --collect --unit=sf-bridge-cutover-$(date +%s) bash ~/supabase-bridge/releases/v3.2.0/cutover.sh v3.2.0 v3.1.0 'supabase-bridge 3.2'
# Exit codes: 0 new release healthy; 2 reverted to old release; 3 revert not confirmed; 4 preflight failure.
set -u
NEW="${1:-v3.2.0}"; OLD="${2:-v3.1.0}"; WANT="${3:-supabase-bridge 3.2}"; LIMIT="${4:-90}"
BASE="$HOME/supabase-bridge"; LAUNCHER="$BASE/supabase_bridge.py"; RELEASES="$BASE/releases"
STAMP=$(date -u +%Y%m%dT%H%M%SZ); BACKUP="$BASE/cutover-backup-$STAMP"; LOG="$BACKUP/cutover.log"
mkdir -p "$BACKUP" && chmod 700 "$BACKUP"
log(){ echo "$(date -u +%FT%TZ) $*" | tee -a "$LOG"; }
fail(){ log "FAILED: $1"; exit "${2:-1}"; }
[ -d "$RELEASES/$NEW" ] || fail "release $NEW missing" 4
[ -d "$RELEASES/$OLD" ] || fail "release $OLD missing" 4
[ -f "$RELEASES/$NEW/agent_bridge.py" ] || fail "release $NEW has no agent_bridge.py" 4
cp -p "$LAUNCHER" "$BACKUP/supabase_bridge.py.before" || fail "launcher backup" 4
cp -p "$BASE/config.json" "$BACKUP/config.json.before" || fail "config backup" 4
[ -f "$BASE/state.json" ] && cp -p "$BASE/state.json" "$BACKUP/state.json.before"
( cd "$RELEASES/$NEW" && sha256sum *.py *.sql *.json *.md *.sh 2>/dev/null ) > "$BACKUP/release-$NEW.sha256"
write_launcher(){
  local tmp="$LAUNCHER.tmp.$$"
  cat > "$tmp" <<EOF
#!/usr/bin/env python3
from pathlib import Path
import sys,runpy
release=Path.home()/'supabase-bridge/releases/$1'
sys.path.insert(0,str(release))
runpy.run_path(str(release/'agent_bridge.py'),run_name='__main__')
EOF
  chmod 755 "$tmp" && mv -f "$tmp" "$LAUNCHER" && sync
}
healthy(){
  python3 - "$1" "$BASE/state.json" "$RESTART_AT" <<'PY'
import json,sys,time
from pathlib import Path
want=sys.argv[1]; p=Path(sys.argv[2]); since=float(sys.argv[3])
try:
    st=json.loads(p.read_text()); age=time.time()-p.stat().st_mtime
    # Only a state.json written after the restart proves the NEW coordinator is alive;
    # the old coordinator preserves unknown keys such as a stale version string.
    ok=age<15 and p.stat().st_mtime>since+1 and not st.get('last_error') and (not want or st.get('version')==want)
    sys.exit(0 if ok else 1)
except Exception:
    sys.exit(1)
PY
}
wait_healthy(){ local want="$1" limit="$2" i=0; while [ "$i" -lt "$limit" ]; do sleep 2; i=$((i+2)); if healthy "$want"; then return 0; fi; done; return 1; }
log "cutover $OLD -> $NEW (expect '$WANT'); backup in $BACKUP"
write_launcher "$NEW" || fail "could not write launcher" 4
RESTART_AT=$(date +%s)
systemctl --user restart supabase-bridge.service || log "restart command returned $?"
if wait_healthy "$WANT" "$LIMIT"; then
  log "SUCCESS: service $(systemctl --user is-active supabase-bridge.service), version '$WANT' healthy"
  exit 0
fi
log "new release not healthy within ${LIMIT}s; capturing status and reverting to $OLD"
systemctl --user status supabase-bridge.service --no-pager -n 40 >> "$LOG" 2>&1 || true
write_launcher "$OLD" || fail "could not restore launcher; manual attention required" 3
RESTART_AT=$(date +%s)
systemctl --user restart supabase-bridge.service || true
if wait_healthy "" 60; then log "REVERTED: $OLD running"; exit 2; fi
log "REVERT NOT CONFIRMED; manual attention required"; exit 3
