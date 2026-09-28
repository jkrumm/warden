#!/bin/zsh
# Restore drill — fetch a ledger snapshot (default: the newest OFF-BOX copy on
# homelab), restore it into a fresh temp dir, verify it (integrity, schema via
# the migrator, data plausibility, one loop dry-run pass, repo bundle), clean up.
#
#   scripts/warden-restore.sh                 # latest snapshot on homelab
#   scripts/warden-restore.sh local-latest    # latest snapshot in ~/.warden/backups
#   scripts/warden-restore.sh <path/to.db>    # a specific snapshot
#
# It cannot touch ~/.warden/warden.db: every write goes through restore.py's
# assert_safe_target(). To actually put a verified copy back after a loss, stop
# the agents (`make unload`), copy the snapshot to ~/.warden/warden.db by hand,
# and `make setup` — deliberately a human step; this script only proves it works.
# One result line on stdout, exit 0/1, and ~/.warden/restore-drill.json, which
# the loop's self-audit reads (a failed or stale drill becomes a warden item).
set -u
REPO="${WARDEN_REPO:-${0:A:h:h}}"
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"
exec "$REPO/.venv/bin/python3" "$REPO/scripts/restore.py" "$@"
