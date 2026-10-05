#!/usr/bin/env bash
# warden's own `make deploy` / `make verify`. A script, not recipe lines: the loop probes
# the target with `make -n deploy`, and GNU make runs any recipe line that names $(MAKE)
# even under -n.
#
# The loop runs `make deploy` itself, from inside com.jkrumm.warden-loop, right after it
# fast-forwarded this checkout. The periodic agents read the checkout on their next tick,
# so deploy never boots them out (that would kill the caller); only the long-running
# warden-api holds old code and is kickstarted. A changed plist is reported for
# `make agents`, never reloaded here. Rollback goes to the commit before the
# fast-forward — WARDEN_DEPLOY_PREV (the loop's sync_checkout passes the pre-merge SHA),
# else HEAD@{1} — only when it is a strict ancestor of HEAD, via `git reset --keep`,
# which refuses rather than discard local changes. WARDEN_DEPLOY_HEAD is the commit the
# sync landed on: the lock is released between sync and deploy, so when HEAD is no longer
# it the checkout moved on its own, and a rollback would reset past commits this deploy
# never saw — the health check still runs, but a failure is reported, never reset.
#
# Deploy and verify run under one flock (deploy.lock, shared with rollout.sync_checkout,
# which fast-forwards this very checkout): macOS has no flock(1), so the script re-execs
# itself under rollout.exec_locked, whose fd the kernel releases however the run ends.
#
# Sourceable: tests source it and call the functions; main runs only when executed.
set -uo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO="$(cd "$HERE/.." && pwd)"
LIVE_REPO="$HOME/SourceRoot/warden"
PY="$REPO/.venv/bin/python3"
API_URL="${WARDEN_API_URL:-http://127.0.0.1:7735}"
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

# $1 = a 503 /health body, {"error": "...schema_version=<ledger>... LEDGER_SCHEMA_VERSION=<code>..."}
# (scripts/ledger.py assert_schema_version). True only when the ledger is OLDER than the
# code — the loop's next boot migrates that. A ledger ahead of the code, an unreachable
# ledger or anything unparseable is false.
schema_migratable() {
  printf '%s' "$1" | "$PY" -c '
import json, re, sys
try:
    error = json.load(sys.stdin)["error"]
    ledger, code = (re.search(rf"\b{key}=(\d+)", error) for key in ("schema_version", "LEDGER_SCHEMA_VERSION"))
    sys.exit(0 if ledger and code and int(ledger[1]) < int(code[1]) else 1)
except Exception:
    sys.exit(1)'
}

# The new code answers /health. A 503 passes only when it is the migratable schema
# mismatch above — an unreachable ledger, a crashing payload or a too-new ledger fails.
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
      503) if schema_migratable "${body%$'\n'*}"; then
             echo "  ✓ warden-api answering (schema migrates on the loop's next boot)"; return 0
           fi
           echo "  ✗ warden-api 503: ${body%$'\n'*}"; return 1 ;;
    esac
    sleep 1
  done
  echo "  ✗ warden-api not answering on $API_URL/health"
  return 1
}

# The commit a failed deploy rolls back to: WARDEN_DEPLOY_PREV, else HEAD@{1}. Prints the
# first candidate that is a commit, differs from HEAD and is an ancestor of HEAD; exits 1
# when none is (HEAD@{1} may be a checkout or a reset target, not "before the deploy").
rollback_target() {
  local head cand
  head=$(git -C "$REPO" rev-parse HEAD) || return 1
  for cand in "${WARDEN_DEPLOY_PREV:-}" "$(git -C "$REPO" rev-parse -q --verify 'HEAD@{1}' 2>/dev/null || true)"; do
    [[ "$cand" =~ ^[0-9a-f]{7,64}$ ]] || continue
    cand=$(git -C "$REPO" rev-parse -q --verify "$cand^{commit}") || continue
    [ "$cand" != "$head" ] || continue
    git -C "$REPO" merge-base --is-ancestor "$cand" "$head" || continue
    echo "$cand"; return 0
  done
  return 1
}

deploy() {
  [ "$REPO" = "$LIVE_REPO" ] || { echo "warden: deploy runs only in $LIVE_REPO, the checkout the agents run"; return 1; }
  [ -x "$PY" ] || { echo "warden: no venv — run 'make venv'"; return 1; }
  local prev moved=0
  prev=$(rollback_target || true)
  if [ -n "${WARDEN_DEPLOY_HEAD:-}" ] && [ "$WARDEN_DEPLOY_HEAD" != "$(git -C "$REPO" rev-parse HEAD)" ]; then
    moved=1
  fi
  health && { echo "  ✓ deployed $(git -C "$REPO" rev-parse --short HEAD)"; return 0; }
  if [ "$moved" = 1 ]; then
    echo "  ✗ deploy failed — checkout moved since sync, not rolling back"; return 1
  fi
  if [ -z "$prev" ]; then
    echo "  ✗ deploy failed, no valid previous commit to roll back to"; return 1
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

main() {
  case "${1:-}" in
    deploy|verify) ;;
    *) echo "usage: $0 deploy|verify" >&2; exit 64 ;;
  esac
  if [ -z "${WARDEN_DEPLOY_LOCKED:-}" ]; then
    [ -x "$PY" ] || { echo "warden: no venv — run 'make venv'"; exit 1; }
    exec "$PY" -c 'import sys; sys.path.insert(0, sys.argv[1]); from lifecycle import rollout; rollout.exec_locked(sys.argv[2:])' \
      "$HERE" bash "${BASH_SOURCE[0]}" "$@"
  fi
  "$1"
}

if [[ "${BASH_SOURCE[0]}" == "$0" ]]; then
  main "$@"
fi
