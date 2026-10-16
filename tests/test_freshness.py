#!/usr/bin/env python3
"""Regression suite for scripts/lifecycle/freshness.py — the one definition of "a poller is stale"
that `/health` and the `Warden Loop` Kuma push (triage.py) share.

Run: .venv/bin/python3 tests/test_freshness.py  (or: make test, from warden/)
"""

from __future__ import annotations

import datetime as dt
import json
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import ledger  # noqa: E402
from lifecycle import freshness  # noqa: E402

NOW = dt.datetime(2030, 1, 2, 12, 0, tzinfo=dt.timezone.utc)


def _db():
    path = Path(tempfile.mkdtemp(prefix="freshness-test-")) / "warden.db"
    return ledger.connect(path, migrate=True)


def _beat(conn, key, age_min, value="{}"):
    at = (NOW - dt.timedelta(minutes=age_min)).isoformat()
    conn.execute("INSERT OR REPLACE INTO cursors(key, value, updated_at) VALUES (?, ?, ?)", (key, value, at))
    conn.commit()


def _all_fresh(conn):
    _beat(conn, "triage_last_run", 1)
    _beat(conn, "watchdog_poll_last_run", 5, json.dumps({"slack_blind": False}))
    _beat(conn, "dispatch_sweep_last_run", 2)


def test_all_fresh_is_empty():
    conn = _db()
    _all_fresh(conn)
    assert freshness.stale_pollers(conn, NOW) == []


def test_a_never_recorded_poller_is_stale():
    conn = _db()
    assert freshness.stale_pollers(conn, NOW) == ["loop", "watchdog_poll", "dispatch_sweep"]


def test_a_dead_sweep_is_named_at_three_intervals():
    conn = _db()
    _all_fresh(conn)
    _beat(conn, "dispatch_sweep_last_run", 14)   # < 3 x 300 s = 15 min
    assert freshness.stale_pollers(conn, NOW) == []
    _beat(conn, "dispatch_sweep_last_run", 16)
    assert freshness.stale_pollers(conn, NOW) == ["dispatch_sweep"]


def test_a_dead_poller_is_named_at_ninety_minutes():
    conn = _db()
    _all_fresh(conn)
    _beat(conn, "watchdog_poll_last_run", 91)
    assert freshness.stale_pollers(conn, NOW) == ["watchdog_poll"]


def test_slack_blind_withholds_like_the_retired_hermes_watchdog_push():
    conn = _db()
    _all_fresh(conn)
    _beat(conn, "watchdog_poll_last_run", 5, json.dumps({"slack_blind": True}))
    assert freshness.stale_pollers(conn, NOW) == ["slack_blind"]


def test_an_unreadable_poll_cursor_value_is_not_a_crash():
    conn = _db()
    _all_fresh(conn)
    _beat(conn, "watchdog_poll_last_run", 5, "not json")
    assert freshness.stale_pollers(conn, NOW) == []


def test_health_and_the_push_share_one_threshold_table():
    import api
    assert api.POLLERS is freshness.POLLERS
    assert api.STALE_MULTIPLE == freshness.STALE_MULTIPLE


def main() -> int:
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failures: list[str] = []
    for name, fn in tests:
        try:
            fn()
        except AssertionError as e:
            failures.append(f"{name}: {e}")
        except Exception:
            failures.append(f"{name}: unexpected exception\n{traceback.format_exc()}")
    print(f"{len(tests) - len(failures)}/{len(tests)} passed")
    for f in failures:
        print(f"  {f}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
