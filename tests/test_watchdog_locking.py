#!/usr/bin/env python3
"""Regression guard for the lock the ingest poller used to hold across its own
network calls.

Run: .venv/bin/python3 tests/test_watchdog_locking.py

THE BUG, measured. On 2026-09-09 at 13:05Z the act-loop died on the first INSERT
of its pass:

    sqlite3.OperationalError: database is locked
      scripts/triage.py:959 in ingest

`ledger.py` sets `busy_timeout=5000` on every writable handle, so something held
the ledger's write lock for more than five seconds. It was this poller.
`_run_poll()` had no commit of its own — `main()` committed once, at the very
end — and Python's sqlite3 opens a DEFERRED write transaction on the first
INSERT and holds it until commit. So from `reconcile()`'s first write onward,
the poller held the write lock across every probe still to come: two `ssh` round
trips for docker, one per op-refs host, a `gh` subprocess with a 30s timeout,
and two Slack HTTP fetches.

The loop is the process whose whole job is noticing that things have stopped.
The ingest poller stopped it.

THE INVARIANT THIS FILE ENFORCES: **no probe in `_run_poll()` is ever called
with an open write transaction.** `conn.in_transaction` is False at the moment
every `poll_*` function is entered. That is a property of where the commits sit,
and it is exactly the property a later refactor would silently lose.

HOUSE CONVENTION, not pytest: every check is a plain argument-free `test_*`
function; main() discovers and calls them reflectively, matching every other
tests/*.py in this repo.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sqlite3
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location("watchdog_poll", REPO / "scripts" / "watchdog-poll.py")
assert _spec is not None and _spec.loader is not None
wp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wp)

NOW = dt.datetime(2026, 9, 9, 12, 0, tzinfo=dt.timezone.utc)


def _fresh_conn() -> sqlite3.Connection:
    tmp = Path(tempfile.mkdtemp(prefix="wd-lock-")) / "warden.db"
    wp.DB_PATH = tmp
    # watchdog-poll.py asserts the schema, it does not create it (see
    # ledger.py — one migrator). Stand in for the loop's boot migrate.
    wp._ledger.connect(tmp, migrate=True).close()
    return wp.db_connect()


def _observed(external_id: str, title: str = "something broke") -> list[dict]:
    return [{"external_id": external_id, "title": title, "url": "", "payload": {}}]


def _stub_every_probe(conn: sqlite3.Connection, seen: list[tuple[str, bool]]) -> None:
    """Replace every network probe `_run_poll()` calls with a stub that records
    whether a write transaction was open at the moment it was entered.

    `uk` and `hermes_log` return REAL rows so `reconcile()` and
    `upsert_grouped()` actually write. Without that this whole suite would pass
    vacuously: no writes means no transaction means nothing to hold, and the
    test would be green against the very bug it exists to catch.
    """

    def probe(name: str, result):
        def _call(*_args, **_kwargs):
            seen.append((name, conn.in_transaction))
            return result
        return _call

    wp.poll_uk = probe("poll_uk", _observed("uk-1"))
    wp.poll_docker = probe("poll_docker", _observed("docker-1"))
    wp.poll_op_refs = probe("poll_op_refs", (_observed("opref-1"), True))
    wp.poll_github = probe("poll_github", {"github_pr": _observed("pr-1")})
    wp.poll_hermes_cron = probe("poll_hermes_cron", _observed("cron-1"))
    wp.poll_stray_skills = probe("poll_stray_skills", _observed("skill-1"))
    wp.poll_hermes_logs = probe("poll_hermes_logs", [
        {"external_id": "log-1", "title": "slack_bolt: Failed to connect",
         "count": 5, "payload": {"batch_count": 5}},
    ])
    wp.poll_slack_messages = probe("poll_slack_messages", ([], None, True))


def test_no_probe_runs_inside_an_open_write_transaction():
    """The invariant itself. Every probe is entered with no write transaction
    open, so the ledger's write lock is never held across a `gh` call, an `ssh`
    round trip or a Slack fetch."""
    conn = _fresh_conn()
    seen: list[tuple[str, bool]] = []
    _stub_every_probe(conn, seen)

    wp._run_poll(conn, NOW, {"HOMELAB_API_KEY": "x"}, deliver=True)

    held = [name for name, in_tx in seen if in_tx]
    assert not held, (
        f"these probes were called while holding the ledger's write lock: {held}. "
        f"A network call inside an open write transaction is what killed the act-loop "
        f"on 2026-09-09 — see this file's docstring."
    )


def test_the_guard_is_not_vacuous():
    """The test above is only meaningful if `_run_poll()` actually wrote
    something. Prove it did: rows landed, and at least one probe ran after the
    first write (so there WAS a window in which a lock could have been held)."""
    conn = _fresh_conn()
    seen: list[tuple[str, bool]] = []
    _stub_every_probe(conn, seen)

    wp._run_poll(conn, NOW, {"HOMELAB_API_KEY": "x"}, deliver=True)

    rows = conn.execute("SELECT COUNT(*) FROM events").fetchone()[0]
    assert rows > 0, "no rows written — the invariant test would pass vacuously"
    assert len(seen) >= 8, f"expected every probe to be exercised, saw {len(seen)}: {seen}"
    # `uk` is the first source and it writes; everything after it is a probe
    # that would have run inside its transaction under the old code.
    assert seen[0][0] == "poll_uk", seen
    assert len(seen) > 1, "no probe ran after the first write — nothing to protect"


def test_run_poll_leaves_no_transaction_open_for_its_caller():
    """`main()` still commits after `_run_poll()` returns. That commit must be
    the tail of a short transaction, not of one spanning the whole poll — so
    whatever is open at return is only `sweep_stale_grouped()`'s own writes."""
    conn = _fresh_conn()
    seen: list[tuple[str, bool]] = []
    _stub_every_probe(conn, seen)

    wp._run_poll(conn, NOW, {"HOMELAB_API_KEY": "x"}, deliver=True)
    conn.commit()
    assert not conn.in_transaction


def main() -> int:
    tests = [(n, o) for n, o in sorted(globals().items())
             if n.startswith("test_") and callable(o)]
    assert tests, "no tests found"
    failures: list[str] = []
    for name, fn in tests:
        try:
            fn()
        except Exception:
            failures.append(f"{name}:\n{traceback.format_exc()}")
    if failures:
        print(f"{len(tests) - len(failures)}/{len(tests)} passed\n")
        print("FAILURES:")
        for f in failures:
            print(f"  {f}")
        return 1
    print(f"{len(tests)}/{len(tests)} passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
