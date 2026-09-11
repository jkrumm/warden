#!/usr/bin/env python3
"""Regression suite for state-log.md §49 — "the notifier does not read the
ledger". `reconcile()`'s reminder branch used to anchor purely on
`events.last_reminder_at`/`notified_at` against `REM_HOURS[source]`, with no
read of the event's own `triage_items` row — so a settled verdict
(`ignored`/`note`/other TERMINAL_STATES) reminded on the source's raw cadence
forever, and a `needs_human` item (a real outstanding decision) reminded on
the same operational-outage cadence as a monitor that is still down.

HOUSE CONVENTION, not pytest — see tests/test_triage.py's own docstring for
the full rationale; this file follows the identical shape: plain
`def test_*(): assert ...` functions, no arguments, no fixtures, discovered
and run via reflection so a bare interpreter or pytest both work unmodified.

Run:

    .venv/bin/python3 tests/test_watchdog_reminders.py
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent

_wp_spec = importlib.util.spec_from_file_location("watchdog_poll", REPO / "scripts" / "watchdog-poll.py")
assert _wp_spec is not None and _wp_spec.loader is not None
wp = importlib.util.module_from_spec(_wp_spec)
_wp_spec.loader.exec_module(wp)


def fresh_db():
    """A throwaway, fully-migrated ledger — never the live ~/.warden/warden.db."""
    tmp = Path(tempfile.mkdtemp(prefix="wd-rem-test-")) / "warden.db"
    wp.DB_PATH = tmp
    wp._ledger.connect(tmp, migrate=True).close()
    return wp.db_connect()


def observed(external_id: str = "sig-a", title: str = "something broke") -> list[dict]:
    return [{"external_id": external_id, "title": title, "url": "", "payload": {}}]


def event_id(conn, external_id: str = "sig-a") -> int:
    return conn.execute("SELECT id FROM events WHERE external_id=?", (external_id,)).fetchone()["id"]


def seed_triage_item(conn, eid: int, state: str, note: str | None = None) -> None:
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO triage_items(event_id, signature, state, created_at, updated_at, note) "
        "VALUES(?,?,?,?,?,?)",
        (eid, f"sig-{eid}", state, now_iso, now_iso, note),
    )
    conn.commit()


def _notify(conn, source: str, t0: dt.datetime) -> int:
    """Fire the initial NEW so the row has a notified_at to anchor reminders on.
    Returns the event id."""
    new, _rem, _res = wp.reconcile(conn, source, observed(), t0, 0, 6, deliver=True)
    assert len(new) == 1, "fixture: expected exactly one NEW event"
    return event_id(conn)


def test_terminal_state_skips_reminder_and_leaves_anchor_untouched():
    conn = fresh_db()
    t0 = dt.datetime(2026, 9, 1, 0, 0, tzinfo=dt.timezone.utc)
    eid = _notify(conn, "uk", t0)
    seed_triage_item(conn, eid, wp._ledger.STATE_FIXED)

    t1 = t0 + dt.timedelta(hours=7)  # past REM_HOURS["uk"] == 6
    _new, rem, _res = wp.reconcile(conn, "uk", observed(), t1, 0, 6, deliver=True)
    assert len(rem) == 0, "a terminal-state item must not remind"
    row = conn.execute("SELECT last_reminder_at FROM events WHERE id=?", (eid,)).fetchone()
    assert row["last_reminder_at"] is None, "skipping a terminal item must not bump the anchor"
    conn.close()


def test_ignored_state_specifically_skips():
    conn = fresh_db()
    t0 = dt.datetime(2026, 9, 1, 0, 0, tzinfo=dt.timezone.utc)
    eid = _notify(conn, "uk", t0)
    seed_triage_item(conn, eid, wp._ledger.STATE_IGNORED)

    t1 = t0 + dt.timedelta(hours=7)
    _new, rem, _res = wp.reconcile(conn, "uk", observed(), t1, 0, 6, deliver=True)
    assert len(rem) == 0
    conn.close()


def test_note_state_specifically_skips():
    conn = fresh_db()
    t0 = dt.datetime(2026, 9, 1, 0, 0, tzinfo=dt.timezone.utc)
    eid = _notify(conn, "uk", t0)
    seed_triage_item(conn, eid, wp._ledger.STATE_NOTE)

    t1 = t0 + dt.timedelta(hours=7)
    _new, rem, _res = wp.reconcile(conn, "uk", observed(), t1, 0, 6, deliver=True)
    assert len(rem) == 0
    conn.close()


def test_needs_human_uses_its_own_24h_cadence_not_the_source_6h():
    conn = fresh_db()
    t0 = dt.datetime(2026, 9, 1, 0, 0, tzinfo=dt.timezone.utc)
    eid = _notify(conn, "uk", t0)
    seed_triage_item(conn, eid, wp._ledger.STATE_NEEDS_HUMAN, note="verdict: waiting on a human call")

    # 7h — past the source's own 6h REM_HOURS["uk"], but well under the 24h
    # needs_human cadence — must NOT remind yet.
    t1 = t0 + dt.timedelta(hours=7)
    _new, rem, _res = wp.reconcile(conn, "uk", observed(), t1, 0, 6, deliver=True)
    assert len(rem) == 0, "needs_human must not fire on the source's 6h cadence"
    conn.close()


def test_needs_human_fires_at_25h_with_note_text_present():
    conn = fresh_db()
    t0 = dt.datetime(2026, 9, 1, 0, 0, tzinfo=dt.timezone.utc)
    eid = _notify(conn, "uk", t0)
    verdict_text = "verdict: waiting on a human call"
    seed_triage_item(conn, eid, wp._ledger.STATE_NEEDS_HUMAN, note=verdict_text)

    t1 = t0 + dt.timedelta(hours=25)
    _new, rem, _res = wp.reconcile(conn, "uk", observed(), t1, 0, 6, deliver=True)
    assert len(rem) == 1, "needs_human must fire once its own 24h cadence elapses"
    assert rem[0].get("triage_note") == verdict_text

    bullet = wp._render_bullet(rem[0], "reminder", t1)
    assert verdict_text in bullet, "the rendered reminder line must carry the verdict text"
    conn.close()


def test_no_item_row_is_unchanged_behavior():
    conn = fresh_db()
    t0 = dt.datetime(2026, 9, 1, 0, 0, tzinfo=dt.timezone.utc)
    _notify(conn, "uk", t0)
    # No triage_items row inserted at all.

    t1 = t0 + dt.timedelta(hours=7)
    _new, rem, _res = wp.reconcile(conn, "uk", observed(), t1, 0, 6, deliver=True)
    assert len(rem) == 1, "an event with no triage_items row must remind on the source's own cadence"
    assert rem[0].get("triage_note") is None
    conn.close()


def test_missing_triage_items_table_is_unchanged_behavior():
    conn = fresh_db()
    conn.execute("DROP TABLE triage_items")
    conn.commit()

    t0 = dt.datetime(2026, 9, 1, 0, 0, tzinfo=dt.timezone.utc)
    _notify(conn, "uk", t0)

    t1 = t0 + dt.timedelta(hours=7)
    _new, rem, _res = wp.reconcile(conn, "uk", observed(), t1, 0, 6, deliver=True)
    assert len(rem) == 1, "a ledger with no triage_items table at all must behave exactly as before"
    conn.close()


def test_poll_uk_drops_group_monitors_and_keeps_the_rest():
    def fake_http_get(url, headers=None, timeout=15):
        return {"monitors": [
            {"id": 95, "name": "VPS", "status": "down", "type": "group"},
            {"id": 179, "name": "Services", "status": "down", "type": "group"},
            {"id": 186, "name": "Local", "status": "down", "type": "group"},
            {"id": 1, "name": "argo", "status": "down", "type": "http"},
            {"id": 2, "name": "push-thing", "status": "down", "type": "push"},
            {"id": 3, "name": "keyword-thing", "status": "down", "type": "keyword"},
            {"id": 4, "name": "docker-thing", "status": "down", "type": "docker"},
        ]}

    saved = wp.http_get
    try:
        wp.http_get = fake_http_get
        out = wp.poll_uk({"HOMELAB_API_KEY": "x"})
    finally:
        wp.http_get = saved

    ids = {o["external_id"] for o in out}
    assert ids == {"1", "2", "3", "4"}, f"group monitors must be dropped, got {ids!r}"


def test_ledger_terminal_states_matches_triage():
    """scripts/ledger.py's TERMINAL_STATES is a hand-mirrored copy of
    scripts/triage.py's own (triage.py is off-limits to this change — see
    ledger.py's comment on the copy). triage.py is imported here, and only
    here, so this is the one place a drift between the two would be caught."""
    _tr_spec = importlib.util.spec_from_file_location("triage_for_reminder_test", REPO / "scripts" / "triage.py")
    assert _tr_spec is not None and _tr_spec.loader is not None
    triage = importlib.util.module_from_spec(_tr_spec)
    _tr_spec.loader.exec_module(triage)

    assert wp._ledger.TERMINAL_STATES == triage.TERMINAL_STATES
    assert wp._ledger.STATE_NEEDS_HUMAN == triage.STATE_NEEDS_HUMAN


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
