#!/usr/bin/env python3
"""Regression suite for scripts/warden-backup.sh — what reaches the destination.

The script runs against a throwaway $HOME-less sandbox: WARDEN_BACKUP_SRC_DIR holds a throwaway
WAL-mode ledger, WARDEN_BACKUP_DEST is a local directory, and the lock and push-file paths are
temp paths. Nothing touches ~/.warden or the network.

Run: .venv/bin/python3 tests/test_backup_sh.py  (or: make test, from warden/)
"""

from __future__ import annotations

import os
import sqlite3
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCRIPT = REPO / "scripts" / "warden-backup.sh"


def _sandbox(tmp: Path):
    """A throwaway WAL-mode ledger; the caller closes the returned connection."""
    src, dest = tmp / "src", tmp / "dest"
    src.mkdir()
    dest.mkdir()
    conn = sqlite3.connect(src / "warden.db")
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA wal_autocheckpoint=0")
    conn.execute("CREATE TABLE t (v TEXT)")
    conn.executemany("INSERT INTO t VALUES (?)", [("row",)] * 20)
    conn.commit()  # committed rows live in the -wal only: the torn-copy hazard
    return src, dest, conn


def _run(tmp: Path, src: Path, dest: Path, rsync: Path | None = None):
    env = {**os.environ, "HOME": str(tmp / "home"),
           "WARDEN_BACKUP_SRC_DIR": str(src), "WARDEN_BACKUP_DEST": f"{dest}/",
           "WARDEN_BACKUP_LOCK_DIR": str(tmp / "lock"),
           "WARDEN_BACKUP_PUSH_FILE": str(tmp / "no-push-url"),
           **({"WARDEN_BACKUP_RSYNC": str(rsync)} if rsync else {})}
    return subprocess.run(["/bin/zsh", str(SCRIPT)], env=env, capture_output=True, text=True, timeout=120)


def test_only_a_consistent_snapshot_ships():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        src, dest, conn = _sandbox(tmp)
        assert (src / "warden.db-wal").exists()
        proc = _run(tmp, src, dest)
        assert proc.returncode == 0, proc.stderr
        shipped = sorted(p.name for p in dest.rglob("*") if p.is_file())
        assert not any(n.startswith("warden.db") for n in shipped), shipped
        snaps = list((dest / "backups").glob("warden-2*.db"))
        assert len(snaps) == 1, shipped
        copy = sqlite3.connect(snaps[0])
        assert copy.execute("PRAGMA integrity_check").fetchone()[0] == "ok"
        assert copy.execute("SELECT count(*) FROM t").fetchone()[0] == 20, "snapshot lost WAL-only rows"
        conn.close()


def test_stale_live_files_on_the_receiver_are_removed():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        src, dest, conn = _sandbox(tmp)
        for name in ("warden.db", "warden.db-wal", "warden.db-shm"):
            (dest / name).write_text("stale")
        assert _run(tmp, src, dest).returncode == 0
        left = sorted(p.name for p in dest.iterdir())
        assert not any(n.startswith("warden.db") for n in left), left
        conn.close()


def test_a_failed_snapshot_exits_nonzero_and_ships_no_live_file():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        src, dest, conn = _sandbox(tmp)
        conn.close()
        (src / "warden.db").write_bytes(b"this is not a sqlite database" * 100)
        for ext in ("-wal", "-shm"):
            (src / f"warden.db{ext}").unlink(missing_ok=True)
        proc = _run(tmp, src, dest)
        assert proc.returncode != 0, "a failed snapshot must fail the run"
        assert "snapshot FAILED" in proc.stderr, proc.stderr
        shipped = sorted(p.name for p in dest.rglob("*") if p.is_file())
        assert not any(n.startswith("warden.db") or n.endswith(".partial") for n in shipped), shipped


def test_a_failed_snapshot_never_syncs_destructively():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        src, dest, conn = _sandbox(tmp)
        conn.close()
        (src / "warden.db").write_bytes(b"not a database" * 100)
        stub = tmp / "rsync-stub"
        stub.write_text(f'#!/bin/sh\necho "$@" > "{tmp}/rsync-args"\n')
        stub.chmod(0o755)
        assert _run(tmp, src, dest, rsync=stub).returncode != 0
        args = (tmp / "rsync-args").read_text()
        assert "--delete" not in args.split() and "--delete-excluded" not in args.split(), args
        assert "--exclude=*.tmp" in args and "--exclude=/warden.db" in args, args


def test_a_missing_ledger_exits_nonzero():
    with tempfile.TemporaryDirectory() as t:
        tmp = Path(t)
        src, dest = tmp / "src", tmp / "dest"
        src.mkdir()
        dest.mkdir()
        assert _run(tmp, src, dest).returncode != 0


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
