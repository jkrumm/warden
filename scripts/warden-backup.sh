#!/bin/zsh
# Daily backup — snapshot ~/.warden/warden.db, bundle the repo, then rsync ~/.warden/ to homelab.
#
# WHY A SNAPSHOT AND NOT JUST rsync. The ledger is live WAL-mode SQLite with the loop
# writing to it every 10 minutes. rsync of an open database copies a torn file: the
# main db and its -wal can be captured at different instants, and the result restores
# as either stale or corrupt with nothing announcing which. `VACUUM INTO` takes a
# transactionally consistent copy through SQLite itself, so what ships is restorable.
# The live file rides along too — it costs nothing and a torn copy is still better
# than no copy if the snapshot step is what failed.
#
# WHERE IT ENDS UP. homelab:/mnt/hdd/backups/warden/. That whole /mnt/hdd/backups
# directory is already mounted read-only into the restic container (homelab's
# docker-compose.yml, `- /mnt/hdd/backups:/sources/hermes-backup:ro`) which runs daily
# at 03:30 to Backblaze B2 with an append-only key, keep-daily 14 / weekly 8 /
# monthly 12 / yearly 5. So this needs no homelab-side change at all — a new
# subdirectory under a path restic already walks is covered the moment it exists.
# 03:10 is chosen to land after hermes-backup (03:00) and before restic (03:30).
#
# There is no restore path yet, on a machine with FileVault off and unattended
# reboots. Getting the bytes off the box is the first half; the second half is
# tracked in STATE.md and is not this script's job.

set -u

REPO="${WARDEN_REPO:-${0:A:h:h}}"   # this script lives in <repo>/scripts/
PY="$REPO/.venv/bin/python3"
SRC_DIR="$HOME/.warden"
DB="$SRC_DIR/warden.db"
SNAP_DIR="$SRC_DIR/backups"
DEST="homelab:/mnt/hdd/backups/warden/"
KEEP=7

SECRETS_RUN="$HOME/.local/bin/secrets-run"
PUSH_REF="${WARDEN_BACKUP_PUSH_REF:-op://hermes/uptime-kuma/warden-backup-push-url}"
# Fallback, and the one that actually resolves today. 1Password is the convention
# for push URLs on this host — the three hermes ones live there — but a new secret
# cannot be put there from the mini: `op` is not interactively signed in, and
# seeding the offline cache is a biometric step. So the URL for the "Warden Backup
# - Push" monitor lives in a mode-600 file instead, which is the same shape
# homelab already uses for garmin-relogin-push-url and for the same class of
# reason. op:// stays FIRST so this converges the moment the ref exists, and this
# file can be deleted then.
PUSH_FILE="${WARDEN_BACKUP_PUSH_FILE:-$HOME/.config/uptime-kuma/warden-backup-push-url}"

# launchd hands the job a minimal PATH (as cron did); prepend Homebrew so secrets-run
# finds its tools and `sqlite3` resolves. Prepend, not replace.
export PATH="/opt/homebrew/bin:/usr/local/bin:$PATH"

# Single-instance lock. Two concurrent `rsync --delete` runs onto the same destination
# race each other's file list — one deleting what the other is still writing. `mkdir`
# is the atomic primitive: it fails if the dir exists and cannot half-exist the way a
# bare PID file can. Same shape as hermes-backup.sh and brain-sync.sh.
LOCK_DIR="${WARDEN_BACKUP_LOCK_DIR:-$HOME/Library/Caches/warden-backup.lock}"
if ! mkdir "$LOCK_DIR" 2>/dev/null; then
  HOLDER=$(cat "$LOCK_DIR/pid" 2>/dev/null || true)
  # `kill -0` returns EPERM (not ESRCH) for a live pid owned by another user and cannot
  # tell the two apart; absence from `ps` is unambiguous.
  if [[ -n "$HOLDER" ]] && ps -p "$HOLDER" >/dev/null 2>&1; then
    echo "another backup (pid $HOLDER) is still running — skipping this run" >&2
    exit 0
  fi
  LOCK_BORN=$(stat -f %m "$LOCK_DIR" 2>/dev/null || echo 0)
  LOCK_AGE=$(( $(date +%s) - LOCK_BORN ))
  # A pidless lock is either a run that died between mkdir and the write, or one
  # claiming it right now. Age decides — treating a fresh one as abandoned is how two
  # runs end up sharing a lock.
  if [[ -z "$HOLDER" && $LOCK_AGE -lt 60 ]]; then
    echo "$LOCK_DIR has no pid yet and is only ${LOCK_AGE}s old — skipping this run" >&2
    exit 0
  fi
  echo "reclaiming the lock left by pid ${HOLDER:-unknown} (${LOCK_AGE}s old)" >&2
  rm -rf "$LOCK_DIR"
  mkdir "$LOCK_DIR" 2>/dev/null || { echo "lost the race for $LOCK_DIR — skipping this run" >&2; exit 0; }
fi
printf '%s' "$$" >"$LOCK_DIR/pid"
trap 'rm -rf "$LOCK_DIR"' EXIT INT TERM

# --- snapshot ---------------------------------------------------------------------
# A failed snapshot does NOT abort the run: shipping the live file is still worth more
# than shipping nothing, and the missing snapshot is visible in the destination.
SNAP_RC=0
if [[ -f "$DB" ]]; then
  mkdir -p "$SNAP_DIR"
  STAMP=$(date -u +%Y%m%dT%H%M%SZ)
  OUT="$SNAP_DIR/warden-$STAMP.db"
  # VACUUM INTO refuses to overwrite, so a colliding name is an error, not a silent
  # clobber. The quoting is safe by construction: $OUT is built from a fixed directory
  # and a date -u format string, neither of which can contain a quote.
  # Through warden's own venv and scripts/ledger.py, NOT the `sqlite3` CLI. Measured
  # 2026-09-09: macOS ships /usr/bin/sqlite3 3.51 with no `-uri` flag, so
  # `sqlite3 "file:$DB?mode=ro" ...` opens that whole URI as a LITERAL FILENAME and
  # dies with "unable to open database file (14)" — a snapshot that silently never
  # happened while rsync still reported success. Using ledger.snapshot() also means
  # there is one VACUUM INTO implementation in this repo rather than two that can
  # disagree about what a consistent copy is.
  if "$PY" -c "
import importlib.util, sys
from pathlib import Path
spec = importlib.util.spec_from_file_location('ledger', '$REPO/scripts/ledger.py')
led = importlib.util.module_from_spec(spec); spec.loader.exec_module(led)
import sqlite3
conn = sqlite3.connect('file:$DB?mode=ro', uri=True)
led.snapshot(conn, '$OUT')
conn.close()
"; then
    echo "snapshot $OUT ($(du -h "$OUT" | cut -f1))"
  else
    echo "snapshot FAILED for $DB — shipping the live file only" >&2
    rm -f "$OUT"
    SNAP_RC=1
  fi
  # Rotate: keep the newest $KEEP. restic holds the long tail, so this is only about
  # not growing the rsync source without bound. `(N)` is zsh's null_glob qualifier —
  # without it an unmatched glob is a hard error here, which is what the very first
  # run (no snapshots yet) always is.
  #
  # `om`, NOT `Om`. zsh's time-based sort qualifiers read backwards from the
  # name-based ones: `om` is newest-first and `Om` is its reverse, oldest-first.
  # This was `Om` until 2026-09-12, so `[KEEP+1,-1]` selected everything past the
  # seven OLDEST — i.e. it deleted the snapshot it had just taken, every run, the
  # moment the directory reached $KEEP. Measured that day: the freshest surviving
  # snapshot was 2 days old and every run since had silently destroyed its own
  # output, leaving the rsynced live `warden.db` + `-wal` pair (captured at two
  # different instants — the exact thing CLAUDE.md's § The ledger forbids relying
  # on) as the only current copy on homelab.
  for old in "$SNAP_DIR"/warden-*.db(Nom[$((KEEP + 1)),-1]); do
    rm -f "$old"
  done
else
  echo "no ledger at $DB — nothing to snapshot" >&2
  SNAP_RC=1
fi

# --- repo bundle -----------------------------------------------------------------
# The repo at $REPO has no git remote (2026-09-11: five commits on one disk, per the
# MacBook-side review). A bundle of every ref rides along in the same snapshot
# directory, so restic's B2 walk covers the code that decides what to do with the
# ledger, not just the ledger. Overwritten each run; restic keeps the history.
# `git bundle verify` is the restore-side check; the restore itself is
# `git clone warden-repo.bundle`. Non-fatal like the snapshot, and it gates the
# heartbeat the same way: a backup that silently lost the repo must not read as ok.
BUNDLE_RC=0
BUNDLE="$SNAP_DIR/warden-repo.bundle"
if git -C "$REPO" rev-parse --git-dir >/dev/null 2>&1; then
  mkdir -p "$SNAP_DIR"
  # `verify` needs a repository to check prerequisites against: launchd runs this
  # from `/`, where a bare `git bundle verify` fails every night (§67).
  # stderr stays on: three nights of "bundle FAILED" never said why.
  if git -C "$REPO" bundle create "$BUNDLE.tmp" --all >/dev/null \
     && git -C "$REPO" bundle verify "$BUNDLE.tmp" >/dev/null; then
    mv -f "$BUNDLE.tmp" "$BUNDLE"
    echo "bundle $BUNDLE ($(du -h "$BUNDLE" | cut -f1), $(git -C "$REPO" rev-parse --short HEAD))"
  else
    echo "bundle FAILED for $REPO — shipping the ledger only" >&2
    rm -f "$BUNDLE.tmp"
    BUNDLE_RC=1
  fi
else
  echo "no git repo at $REPO — nothing to bundle" >&2
  BUNDLE_RC=1
fi

# --- ship -------------------------------------------------------------------------
PUSH_URL=""
[[ -x "$SECRETS_RUN" ]] && PUSH_URL=$(timeout 10 "$SECRETS_RUN" read "$PUSH_REF" 2>/dev/null)
[[ -z "$PUSH_URL" && -r "$PUSH_FILE" ]] && PUSH_URL=$(<"$PUSH_FILE")

/usr/bin/rsync -az --delete \
  --exclude='*.lock' \
  --exclude='*.pid' \
  --exclude='*-shm' \
  "$SRC_DIR/" "$DEST"

RC=$?

# The heartbeat fires only on a clean rsync AND a clean snapshot, so a silently
# degraded backup still trips the "warden backup last successful run" alert rather
# than reporting success. A failed ping never overrides rsync's exit code (RC is
# captured before it), and an unresolvable ref is non-fatal: the backup still ran,
# only the ping is skipped, which UptimeKuma reads as a missed heartbeat.
if [[ $RC -eq 0 && $SNAP_RC -eq 0 && $BUNDLE_RC -eq 0 && -n "${PUSH_URL:-}" ]]; then
  # uptime.jkrumm.com sits behind Cloudflare, which 403s a default library
  # User-Agent — curl's own UA is what the other push monitors already send.
  /usr/bin/curl -fsS --max-time 10 "$PUSH_URL" >/dev/null
fi

exit $RC
