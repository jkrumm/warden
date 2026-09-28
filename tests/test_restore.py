"""Regression suite for scripts/restore.py — the ledger restore drill (§104).

Hand-rolled runner like every suite here: module-level `test_*` functions,
argument-free, exit non-zero on failure. No network: every drill here restores
a synthetic snapshot from a local path and skips the loop boot (the live drill
exercises that); the guard tests never need a snapshot at all.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import importlib.util
import json
import os
import sqlite3
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("restore", REPO / "scripts" / "restore.py")
restore = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(restore)
ledger = restore._ledger

SNAP_TS = dt.datetime(2026, 9, 28, 1, 10, 5, tzinfo=dt.timezone.utc)


def _home() -> Path:
    """A throwaway WARDEN_HOME with a 'live' ledger in it, wired into the module."""
    home = Path(tempfile.mkdtemp(prefix="restore-home-"))
    restore.WARDEN_HOME = home
    restore.LIVE_DB = home / "warden.db"
    restore.RESULT_FILE = home / "restore-drill.json"
    restore.LOCAL_SNAPSHOTS = home / "backups"
    conn = ledger.connect(restore.LIVE_DB, migrate=True)
    conn.close()
    return home


def _snapshot(*, newest: dt.datetime, version: int | None = None, corrupt: bool = False,
              empty: bool = False) -> Path:
    src_dir = Path(tempfile.mkdtemp(prefix="restore-src-"))
    work = src_dir / "build.db"
    conn = ledger.connect(work, migrate=True)
    if not empty:
        conn.execute("INSERT INTO events(source, external_id, title, first_seen) VALUES ('uk','1','t',?)",
                     (newest.isoformat(),))
        conn.execute("INSERT INTO dispatches(job_id, tier, repo, brief, status, created_at) "
                     "VALUES ('j1','investigate','r','b','done',?)", (newest.isoformat(),))
    conn.execute("INSERT INTO cursors(key, value, updated_at) VALUES ('triage_last_run','{}',?)",
                 (newest.isoformat(),))
    if version is not None:
        conn.execute("UPDATE schema_version SET version=?", (version,))
    conn.commit()
    snap = src_dir / f"warden-{SNAP_TS.strftime('%Y%m%dT%H%M%SZ')}.db"
    conn.execute(f"VACUUM INTO '{snap}'")
    conn.close()
    if corrupt:
        data = bytearray(snap.read_bytes())
        for i in range(2048, min(len(data), 8192)):
            data[i] = 0xFF
        snap.write_bytes(bytes(data))
    return snap


def _sha(p: Path) -> str:
    return hashlib.sha256(p.read_bytes()).hexdigest()


def _drill(source: Path) -> tuple[int, dict]:
    rc = restore.drill(str(source), boot=False)
    return rc, json.loads(restore.RESULT_FILE.read_text())


# --- the guard ---------------------------------------------------------------

def test_guard_refuses_the_live_ledger_its_siblings_and_its_home():
    home = _home()
    for target in (home / "warden.db", home / "warden.db-wal", home / "warden.db-shm",
                   home / "backups" / "x.db", home / "anything"):
        try:
            restore.assert_safe_target(target)
        except restore.UnsafeTarget:
            continue
        raise AssertionError(f"guard let {target} through")


def test_guard_follows_symlinks_into_the_live_ledger():
    home = _home()
    elsewhere = Path(tempfile.mkdtemp(prefix="restore-link-"))
    link = elsewhere / "innocent.db"
    link.symlink_to(home / "warden.db")
    try:
        restore.assert_safe_target(link)
    except restore.UnsafeTarget:
        pass
    else:
        raise AssertionError("a symlink to the live ledger must be refused")
    linked_dir = elsewhere / "dir"
    linked_dir.symlink_to(home)
    try:
        restore.assert_safe_target(linked_dir / "warden.db")
    except restore.UnsafeTarget:
        pass
    else:
        raise AssertionError("a symlinked directory into WARDEN_HOME must be refused")


def test_guard_allows_a_temp_target():
    _home()
    target = Path(tempfile.mkdtemp(prefix="restore-ok-")) / "warden-x.db"
    assert restore.assert_safe_target(target) == Path(os.path.realpath(target))


def test_a_drill_whose_source_is_the_live_ledger_never_changes_it():
    """Even pointed straight at the live file, the drill only reads it: the
    copy goes to a temp dir, and the live bytes are identical afterwards."""
    home = _home()
    live = home / "warden.db"
    before = _sha(live)
    restore.drill(str(live), boot=False)
    assert _sha(live) == before, "the live ledger's bytes changed"
    assert not (live.parent / "warden.db-journal").exists()


# --- the drill ---------------------------------------------------------------

def test_a_good_snapshot_restores_verifies_and_cleans_up():
    _home()
    snap = _snapshot(newest=SNAP_TS - dt.timedelta(minutes=4))
    rc, rec = _drill(snap)
    assert rc == 0 and rec["ok"], rec
    assert rec["integrity"] == "ok" and rec["events"] == 1 and rec["dispatches"] == 1
    assert rec["schema"] == f"{ledger.LEDGER_SCHEMA_VERSION}->{ledger.LEDGER_SCHEMA_VERSION}"
    assert rec["cleaned_up"] is True


def test_an_older_schema_snapshot_is_migrated_not_refused():
    """A real schema-(N-1) ledger — built by the migrator stopped one version
    short — restores and is brought to N. The normal case: tonight's snapshot
    predates any schema bump shipped today (the live drill ran 10 -> 11)."""
    _home()
    current = ledger.LEDGER_SCHEMA_VERSION
    ledger.LEDGER_SCHEMA_VERSION = current - 1
    try:
        snap = _snapshot(newest=SNAP_TS - dt.timedelta(minutes=4))
    finally:
        ledger.LEDGER_SCHEMA_VERSION = current
    conn = sqlite3.connect(snap)
    assert conn.execute("SELECT MAX(version) FROM schema_version").fetchone()[0] == current - 1
    conn.close()
    rc, rec = _drill(snap)
    assert rc == 0, rec
    assert rec["schema"] == f"{current - 1}->{current}", rec


def test_a_corrupt_snapshot_fails_loudly_and_still_cleans_up():
    _home()
    snap = _snapshot(newest=SNAP_TS - dt.timedelta(minutes=4), corrupt=True)
    rc, rec = _drill(snap)
    assert rc == 1 and not rec["ok"] and rec["cleaned_up"] is True, rec
    assert "rror" in rec["error"], rec


def test_a_snapshot_newer_than_this_warden_fails():
    _home()
    snap = _snapshot(newest=SNAP_TS - dt.timedelta(minutes=4), version=ledger.LEDGER_SCHEMA_VERSION + 1)
    rc, rec = _drill(snap)
    assert rc == 1 and "newer than this warden" in rec["error"], rec


def test_a_snapshot_whose_data_stopped_long_before_it_was_taken_fails():
    """The loop writes every 10 minutes; a snapshot whose newest write is
    hours older than the snapshot itself is a stale ledger shipped as fresh."""
    _home()
    snap = _snapshot(newest=SNAP_TS - dt.timedelta(hours=5))
    rc, rec = _drill(snap)
    assert rc == 1 and "older than the snapshot" in rec["error"], rec


def test_an_empty_ledger_is_not_a_plausible_restore():
    _home()
    snap = _snapshot(newest=SNAP_TS - dt.timedelta(minutes=4), empty=True)
    rc, rec = _drill(snap)
    assert rc == 1 and "implausibly empty" in rec["error"], rec


def test_a_missing_snapshot_is_a_failure_record_not_a_crash():
    _home()
    rc, rec = _drill(Path("/nonexistent/warden-20260928T011005Z.db"))
    assert rc == 1 and "no such snapshot" in rec["error"], rec


def main() -> int:
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failures = []
    for name, fn in tests:
        try:
            fn()
        except Exception:  # noqa: BLE001
            failures.append((name, traceback.format_exc()))
    passed = len(tests) - len(failures)
    print(f"{passed}/{len(tests)} passed")
    if not tests:
        print("no tests found")
        return 1
    if failures:
        print("FAILURES:")
        for name, tb in failures:
            print(f"  {name}:\n{tb}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
