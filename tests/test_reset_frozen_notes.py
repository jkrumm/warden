#!/usr/bin/env python3
"""Regression suite for `scripts/reset-frozen-notes.py` — the one-time revive of
the `note`-frozen `slack_alert` family (items 1117/1118).

The script touches the LIVE ledger, so its selection rule is the part worth
pinning down: `note`-state `slack_alert` rows only, unresolved events only, a
dry run that cannot write at all, and a second run that moves nothing. Every
case here runs against a throwaway migrated ledger in a temp dir — the
developer's own `~/.warden/warden.db` is never opened.

Run: .venv/bin/python3 tests/test_reset_frozen_notes.py  (or: make test)
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sys
import tempfile
import traceback
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent


def _load(name: str, relpath: str):
    spec = importlib.util.spec_from_file_location(name, REPO_ROOT / relpath)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


triage = _load("triage", "scripts/triage.py")
# Loaded by path, and its own `import triage` resolves to this same module
# (scripts/ lands on sys.path through the file's own insert), so the script and
# this suite cannot disagree about what STATE_NOTE / _set_state mean.
reset = _load("reset_frozen_notes", "scripts/reset-frozen-notes.py")

NOW = dt.datetime.now(dt.timezone.utc)


def _fresh_ledger() -> Path:
    tmp = Path(tempfile.mkdtemp(prefix="reset-notes-test-")) / "warden.db"
    conn = triage._ledger.connect(tmp, migrate=True)
    conn.close()
    return tmp


def _seed(db: Path, *, signature: str, title: str = "HomeLab CPU above threshold",
          state: str = triage.STATE_NOTE, source: str = "slack_alert",
          resolved_at: str | None = None) -> int:
    conn = triage._ledger.connect(db)
    try:
        cur = conn.execute(
            "INSERT INTO events(source, external_id, title, url, payload_json, first_seen, "
            "reminder_count, resolved_at) VALUES (?,?,?,?,?,?,?,?)",
            (source, signature.split(":", 1)[-1], title, "", json.dumps({}),
             (NOW - dt.timedelta(days=8)).isoformat(), 0, resolved_at),
        )
        event_id = cur.lastrowid
        conn.execute(
            "INSERT INTO triage_items(event_id, signature, state, occurrences, first_seen, "
            "last_seen, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (event_id, signature, state, 3, (NOW - dt.timedelta(days=8)).isoformat(),
             NOW.isoformat(), (NOW - dt.timedelta(days=8)).isoformat(), NOW.isoformat()),
        )
        conn.commit()
        return event_id
    finally:
        conn.close()


def _state(db: Path, event_id: int) -> tuple[str, str | None]:
    conn = triage._ledger.connect(db, readonly=True)
    try:
        row = conn.execute("SELECT state, note FROM triage_items WHERE event_id=?",
                           (event_id,)).fetchone()
        return row["state"], row["note"]
    finally:
        conn.close()


def test_dry_run_writes_nothing():
    """No `--apply` means no write at all — the connection is opened read-only,
    so this is a property of the handle, not of a careful code path."""
    db = _fresh_ledger()
    eid = _seed(db, signature="slack_alert:homelab-cpu-above-threshold")
    assert reset.main(["--db", str(db)]) == 0
    assert _state(db, eid) == (triage.STATE_NOTE, None), (
        "a dry run must leave the row exactly as it found it")


def test_apply_revives_unresolved_note_rows_only():
    db = _fresh_ledger()
    live = _seed(db, signature="slack_alert:homelab-cpu-above-threshold")
    resolved = _seed(db, signature="slack_alert:homelab-disk-usage-above-threshold",
                     title="HomeLab disk usage above threshold",
                     resolved_at=(NOW - dt.timedelta(hours=16)).isoformat())
    other_source = _seed(db, signature="uk:warden-backup-push", title="[Warden Backup - Push] Down",
                         source="uk")
    already_moved = _seed(db, signature="slack_alert:already-new", state=triage.STATE_NEW)

    assert reset.main(["--db", str(db), "--apply"]) == 0

    state, note = _state(db, live)
    assert state == triage.STATE_NEW, f"an unresolved note row must be revived, got {state!r}"
    assert note == reset.REVIVE_NOTE, "the revive must carry its own reason onto the row"
    assert _state(db, resolved)[0] == triage.STATE_NOTE, (
        "an event that already resolved must not be revived — that would card a condition "
        "that is over")
    assert _state(db, other_source)[0] == triage.STATE_NOTE, (
        "the family is `slack_alert` only; a `uk` row is a different producer's problem")
    assert _state(db, already_moved) == (triage.STATE_NEW, None), (
        "classify()-state rows classify() still owns are not this script's business")


def test_second_run_is_a_no_op():
    """Idempotence is what makes running it twice by accident harmless."""
    db = _fresh_ledger()
    eid = _seed(db, signature="slack_alert:homelab-cpu-above-threshold")
    reset.main(["--db", str(db), "--apply"])
    first = _state(db, eid)

    assert reset.main(["--db", str(db), "--apply"]) == 0
    assert _state(db, eid) == first, (
        "a second run must find no `note` rows and change nothing — including not "
        "clobbering the note the first run wrote")


def main() -> int:
    tests = [(name, fn) for name, fn in sorted(globals().items())
             if name.startswith("test_") and callable(fn)]
    passed = 0
    failures: list[str] = []
    for name, fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as e:
            failures.append(f"{name}: {e}")
        except Exception:
            failures.append(f"{name}: unexpected exception\n{traceback.format_exc()}")

    print(f"{passed}/{len(tests)} passed")
    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
