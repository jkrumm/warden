#!/usr/bin/env bash
# warden's own `make deploy` / `make verify`. A script, not recipe lines: the loop probes
# the target with `make -n deploy`, and GNU make runs any recipe line that names $(MAKE)
# even under -n.
#
# The loop runs `make deploy` itself, from inside com.jkrumm.warden-loop, right after it
# fast-forwarded this checkout. The periodic agents read the checkout on their next tick,
# so deploy never boots them out (that would kill the caller); only the long-running
# warden-api holds old code and is kickstarted. A changed plist is reported for
# `make agents`, never reloaded here. Rollback is to HEAD@{1} — the commit before the
# fast-forward — via `git reset --keep`, which refuses rather than discard local changes.
set -uo pipefail

REPO="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LIVE_REPO="$HOME/SourceRoot/warden"
PY="$REPO/.venv/bin/python3"
API_URL="http://127.0.0.1:7735"
API_LABEL="gui/$(id -u)/com.jkrumm.warden-api"
LA="$HOME/Library/LaunchAgents"
PLISTS="com.jkrumm.warden-loop com.jkrumm.warden-poll com.jkrumm.warden-sweep com.jkrumm.warden-backup com.jkrumm.warden-api"

IMPORT_SMOKE='
import importlib.util, sys
sys.path.insert(0, ".")
for name in ("ledger", "triage", "api", "warden", "dispatch-sweep", "watchdog-poll"):
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), name + ".py")
    module = sys.modules[spec.name] = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
'

# The new code answers /health. A 503 passes only when it is the schema mismatch the
# loop's next boot migrates away — an unreachable ledger or a crashing payload fails.
health() {
  "$PY" -m compileall -q "$REPO/scripts" >/dev/null || { echo "  ✗ compileall"; return 1; }
  (cd "$REPO/scripts" && "$PY" -c "$IMPORT_SMOKE") || { echo "  ✗ import smoke"; return 1; }
  for name in $PLISTS; do
    sed "s|__HOME__|$HOME|g" "$REPO/launchd/$name.plist.template" | cmp -s - "$LA/$name.plist" \
      || echo "  ! $name plist changed — run 'make agents'"
  done
  launchctl kickstart -k "$API_LABEL" >/dev/null 2>&1 || { echo "  ✗ kickstart warden-api"; return 1; }
  # launchd throttles a KeepAlive restart for up to 10s (ThrottleInterval); wait past it.
  local body code
  for _ in $(seq 1 20); do
    body=$(curl -s --max-time 2 -w '\n%{http_code}' "$API_URL/health")
    code=${body##*$'\n'}
    case "$code" in
      200) echo "  ✓ warden-api healthy"; return 0 ;;
      503) if [[ "$body" == *schema_version=* ]]; then
             echo "  ✓ warden-api answering (schema migrates on the loop's next boot)"; return 0
           fi
           echo "  ✗ warden-api 503: ${body%$'\n'*}"; return 1 ;;
    esac
    sleep 1
  done
  echo "  ✗ warden-api not answering on $API_URL/health"
  return 1
}

deploy() {
  [ "$REPO" = "$LIVE_REPO" ] || { echo "warden: deploy runs only in $LIVE_REPO, the checkout the agents run"; return 1; }
  [ -x "$PY" ] || { echo "warden: no venv — run 'make venv'"; return 1; }
  local prev
  prev=$(git -C "$REPO" rev-parse -q --verify 'HEAD@{1}' || true)
  health && { echo "  ✓ deployed $(git -C "$REPO" rev-parse --short HEAD)"; return 0; }
  if [ -z "$prev" ] || [ "$prev" = "$(git -C "$REPO" rev-parse HEAD)" ]; then
    echo "  ✗ deploy failed, no previous commit to roll back to"; return 1
  fi
  echo "  ✗ deploy failed — rolling back to $prev"
  if git -C "$REPO" reset -q --keep "$prev" && health; then
    echo "  ✓ rolled back to $(git -C "$REPO" rev-parse --short HEAD)"
  else
    echo "  ✗ rollback failed — read 'make logs'"
  fi
  return 1
}

# What a warden deploy can break: the API serving the expected schema and the loop
# still completing passes. Other pollers and the backup are `make status`'s business —
# failing them here would revert a good merge for an unrelated cause.
verify() {
  local out
  out=$(curl -fsS --max-time 5 "$API_URL/health") || { echo "  ✗ $API_URL/health unreachable or not 200"; return 1; }
  echo "$out" | "$PY" -c '
import json, sys
h = json.load(sys.stdin)
loop = h["pollers"]["loop"]
schema_ok = h["schema_version"] == h["schema_version_expected"]
mark = lambda ok: "✓" if ok else "✗"
print("  %s schema %s/%s" % (mark(schema_ok), h["schema_version"], h["schema_version_expected"]))
print("  %s loop heartbeat %s min (limit %s)" % (mark(loop["ok"]), loop["age_minutes"], loop["threshold_minutes"]))
sys.exit(0 if schema_ok and loop["ok"] else 1)'
}

case "${1:-}" in
  deploy) deploy ;;
  verify) verify ;;
  *) echo "usage: $0 deploy|verify" >&2; exit 64 ;;
esac
