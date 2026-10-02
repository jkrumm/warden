#!/usr/bin/env python3
"""Regression suite for scripts/api.py — GET /metrics and GET /health.

Covers real arithmetic against a seeded ledger (not smoke tests), the honesty
rules (`null` + a reason, never a fabricated `0`; `history_since` telling an
empty-by-construction window apart from a measured zero), the 503-on-schema-
mismatch contract, the GET-only method surface, and — structurally, via a
filesystem that forbids writes entirely — that the handler never opens a
writable connection.

Run: .venv/bin/python3 tests/test_api.py
"""

from __future__ import annotations

import datetime as dt
import http.client
import importlib.util
import json
import socket
import sqlite3
import stat
import sys
import tempfile
import time
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _load(name: str, relpath: str):
    spec = importlib.util.spec_from_file_location(name, REPO / relpath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ledger = _load("ledger", "scripts/ledger.py")
api = _load("api", "scripts/api.py")


def _iso(d: dt.datetime) -> str:
    return d.isoformat()


def _tmp_db() -> Path:
    return Path(tempfile.mkdtemp(prefix="api-test-")) / "warden.db"


def _fresh_conn():
    path = _tmp_db()
    return ledger.connect(path, migrate=True), path


def _event(conn, event_id: int, now: dt.datetime, source: str = "s") -> None:
    conn.execute(
        "INSERT INTO events (id, source, external_id, title, first_seen) VALUES (?,?,?,?,?)",
        (event_id, source, f"e{event_id}", f"title{event_id}", _iso(now)),
    )


def _item(conn, event_id: int, *, state: str, repo: str | None = "r", dispatch_job: str | None = None,
         implement_job: str | None = None, validation_job: str | None = None,
         origin: str = "alert", max_tier: str = "implement", note: str | None = None,
         pr_url: str | None = None, occurrences: int = 0,
         now: dt.datetime, updated_at: dt.datetime | None = None) -> None:
    conn.execute(
        "INSERT INTO triage_items (event_id, signature, repo, state, dispatch_job, implement_job, "
        "validation_job, origin, max_tier, note, pr_url, occurrences, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (event_id, f"sig{event_id}", repo, state, dispatch_job, implement_job, validation_job,
         origin, max_tier, note, pr_url, occurrences, _iso(now), _iso(updated_at or now)),
    )


def _dispatch(conn, job_id: str, *, tier: str = "investigate", verdict_json: str | None = "{}",
             origin_event_id: int | None = 1, now: dt.datetime) -> None:
    """`origin_event_id` defaults to loop-originated (matches DESIGN.md's
    "the loop originated this" reading of the column) — pass None to seed an
    interactive warden dispatch, which metric 1 must exclude entirely."""
    conn.execute(
        "INSERT INTO dispatches (job_id, tier, repo, brief, status, verdict_json, origin_event_id, created_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (job_id, tier, "r", "brief", "done", verdict_json, origin_event_id, _iso(now)),
    )


def _transition(conn, event_id: int, from_state: str | None, to_state: str, at: dt.datetime) -> None:
    conn.execute(
        "INSERT INTO item_transitions (event_id, from_state, to_state, at) VALUES (?,?,?,?)",
        (event_id, from_state, to_state, _iso(at)),
    )


def _cursor(conn, key: str, at: dt.datetime, value: str = "{}") -> None:
    conn.execute(
        "INSERT INTO cursors (key, value, updated_at) VALUES (?,?,?)",
        (key, value, _iso(at)),
    )


def _operation(conn, op_id: str, *, event_id: int, kind: str = "implement", repo: str = "r",
              authorized_by: str = "auto-from-item", now: dt.datetime) -> None:
    conn.execute(
        "INSERT INTO operations (op_id, event_id, kind, repo, authorized_by, started_at) VALUES (?,?,?,?,?,?)",
        (op_id, event_id, kind, repo, authorized_by, _iso(now)),
    )


def _seed_old_history_anchor(conn, now: dt.datetime) -> None:
    """Push `history_since` safely before any WINDOW_DAYS window, so a test
    exercising a metric's REAL in-window computation isn't itself caught by
    the leaf-level history guard (`api._history_guard_reason`) meant for a
    young table. Event id chosen unlikely to collide with a test's own ids."""
    _transition(conn, 999999, None, "new", now - dt.timedelta(days=30))


# --- Metric 1: verdicts -> recorded disposition --------------------------------

def test_metric1_arithmetic_numerator_denominator():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    for i in range(1, 5):
        _event(conn, i, now)
    _dispatch(conn, "j1", verdict_json="{}", now=now)   # eligible denom
    _dispatch(conn, "j2", verdict_json="{}", now=now)   # eligible denom
    _dispatch(conn, "j3", verdict_json=None, now=now)   # NOT eligible — no verdict
    _dispatch(conn, "j4", tier="implement", verdict_json="{}", now=now)  # NOT eligible — wrong tier
    _item(conn, 1, state="fixed", dispatch_job="j1", now=now)     # disposition
    _item(conn, 2, state="working", dispatch_job="j2", now=now)  # NOT a disposition
    conn.commit()

    m = api._metric_verdicts_recorded_disposition(conn)
    assert m["denominator"] == 2, m
    assert m["numerator"] == 1, m
    assert m["value"] == 0.5, m
    assert m["unavailable"] is None
    assert m["item_states"] == {"fixed": 1, "working": 1}, m
    assert m["excluded_interactive"] == 0, m
    conn.close()


def test_metric1_excludes_interactive_dispatches_and_item_states_can_exceed_denominator():
    """The denominator's discriminator is `origin_event_id IS NOT NULL` ("the
    loop originated this"), never "has a linked triage_item" — the latter
    would circularly exclude the very no-item failures this metric exists to
    count. `item_states` counts triage_items, not dispatches, so one
    clustered dispatch's several items can push its total past the
    denominator; the payload must disclose both facts, not just be silently
    inconsistent."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    for i in range(1, 5):
        _event(conn, i, now)
    # Loop-originated, clustered over 3 items under one dispatch.
    _dispatch(conn, "j1", verdict_json="{}", origin_event_id=1, now=now)
    _item(conn, 1, state="fixed", dispatch_job="j1", now=now)
    _item(conn, 2, state="fixed", dispatch_job="j1", now=now)
    _item(conn, 3, state="needs_decision", dispatch_job="j1", now=now)
    # Loop-originated, single item, `quiet` — not a recorded disposition.
    _dispatch(conn, "j2", verdict_json="{}", origin_event_id=4, now=now)
    _item(conn, 4, state="quiet", dispatch_job="j2", now=now)
    # Interactive: a human asked warden directly. No origin_event_id, no item.
    _dispatch(conn, "j3", verdict_json="{}", origin_event_id=None, now=now)
    conn.commit()

    m = api._metric_verdicts_recorded_disposition(conn)
    assert m["denominator"] == 2, "interactive dispatch (origin_event_id NULL) must not enter the denominator"
    assert m["numerator"] == 1, "only j1's cluster reached a disposition state"
    assert m["value"] == 0.5, m
    assert m["excluded_interactive"] == 1, m
    assert m["excluded_interactive_note"], "excluding it must be disclosed, not silent"
    total_items = sum(m["item_states"].values())
    assert total_items == 4, m  # 3 from j1's cluster + 1 from j2
    assert total_items > m["denominator"], "item_states counts items and may legitimately exceed dispatch counts"
    assert m["item_states_note"], "the payload must say item_states can exceed the denominator"
    conn.close()


def test_metric1_quiet_is_not_a_disposition():
    """DESIGN.md principle 5 / § Lifecycle: silence-resolve is never an
    outcome. `quiet` must NOT count toward the numerator."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _dispatch(conn, "j1", verdict_json="{}", now=now)
    _item(conn, 1, state="quiet", dispatch_job="j1", now=now)
    conn.commit()

    m = api._metric_verdicts_recorded_disposition(conn)
    assert m["denominator"] == 1
    assert m["numerator"] == 0, "quiet must not count as a recorded disposition"
    assert m["value"] == 0.0
    conn.close()


def test_metric1_zero_denominator_is_null_not_zero():
    conn, _ = _fresh_conn()
    m = api._metric_verdicts_recorded_disposition(conn)
    assert m["value"] is None
    assert m["unavailable"]
    conn.close()


# --- Metric 2: verified fixes vs silence ----------------------------------------

def test_metric2_arithmetic_restricted_to_mapped_signatures():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    for i in range(1, 6):
        _event(conn, i, now)
    _item(conn, 1, state="fixed", repo="r", now=now)
    _item(conn, 2, state="fixed", repo="r", now=now)
    _item(conn, 3, state="quiet", repo="r", now=now)
    _item(conn, 4, state="closed", repo="r", now=now)
    _item(conn, 5, state="fixed", repo=None, now=now)  # unmapped — excluded
    conn.commit()

    m = api._metric_verified_fixes_vs_silence(conn)
    assert m["fixed"] == 2, m
    assert m["quiet"] == 1, m
    assert m["closed"] == 1, m
    assert m["value"] == 2 / 3, m
    assert m["unavailable"] is None
    conn.close()


def test_metric2_zero_denominator_is_null():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _item(conn, 1, state="closed", repo="r", now=now)
    conn.commit()
    m = api._metric_verified_fixes_vs_silence(conn)
    assert m["value"] is None
    assert m["unavailable"]
    conn.close()


# --- Metric 3: median needs_decision -> decision --------------------------------

def test_metric3_pairs_every_exit_from_needs_decision():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _seed_old_history_anchor(conn, now)  # keep history_since well before the 7d window
    _event(conn, 1, now)
    _event(conn, 2, now)

    # Nothing expires a needs_decision item, so every exit is somebody deciding.
    # Event 1: needs_decision -> closed after 8h.
    _transition(conn, 1, "working", "needs_decision", now - dt.timedelta(hours=10))
    _transition(conn, 1, "needs_decision", "closed", now - dt.timedelta(hours=2))

    # Event 2: needs_decision -> working (a decision to go ahead) after 4h.
    _transition(conn, 2, "working", "needs_decision", now - dt.timedelta(hours=8))
    _transition(conn, 2, "needs_decision", "working", now - dt.timedelta(hours=4))
    conn.commit()

    history_since = api._item_transitions_history_since(conn)
    m = api._metric_median_needs_decision_to_decision(conn, now, history_since)
    assert m["pairs"] == 2, m
    assert abs(m["value"] - 6.0) < 0.01, m
    assert "excluded_dismissed_pairs" not in m, "there is no expiry clock to exclude any more"
    conn.close()


def test_metric3_no_pairs_is_null_not_zero():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    history_since = api._item_transitions_history_since(conn)
    m = api._metric_median_needs_decision_to_decision(conn, now, history_since)
    assert m["value"] is None
    assert m["unavailable"]
    conn.close()


def test_metric3_windowed_on_entry():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    # Entry 10 days ago — outside the 7-day window — must not count. history_since
    # lands at -10d too, which is still before window_start(-7d), so the guard
    # does not swallow this: it's a real "no pairs in window", not a young table.
    _transition(conn, 1, "working", "needs_decision", now - dt.timedelta(days=10))
    _transition(conn, 1, "needs_decision", "working", now - dt.timedelta(days=9))
    conn.commit()
    history_since = api._item_transitions_history_since(conn)
    m = api._metric_median_needs_decision_to_decision(conn, now, history_since)
    assert m["pairs"] == 0, m
    assert m["value"] is None
    conn.close()


def test_metric3_null_with_history_reason_when_table_is_young():
    """A REAL needs_decision -> decision pair exists here (would compute a 1h
    median under the old code), but item_transitions has no history before
    it — the leaf guard must still null the value, and the reason must name
    `history_since`, not the generic "no pairs" message, so a reader can tell
    "too young to trust" apart from "genuinely nothing happened"."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _transition(conn, 1, "working", "needs_decision", now - dt.timedelta(hours=2))
    _transition(conn, 1, "needs_decision", "working", now - dt.timedelta(hours=1))
    conn.commit()
    history_since = api._item_transitions_history_since(conn)
    m = api._metric_median_needs_decision_to_decision(conn, now, history_since)
    assert m["value"] is None, m
    assert "history_since" in m["unavailable"], m
    conn.close()


# --- Metric 4: verified unattended fixes per week -------------------------------
#
# Schema 5's `operations` table makes the "unattended" qualifier derivable —
# see state-log.md §46 Correction 1 and api.py's own docstring on this metric.
# With the signed-approval gate gone, every item that entered `fixed` in the
# window is unattended.

def test_metric4_counts_a_fixed_item_as_unattended():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _seed_old_history_anchor(conn, now)
    _event(conn, 1, now)
    _transition(conn, 1, "working", "fixed", now - dt.timedelta(hours=1))
    _operation(conn, "op1", event_id=1, authorized_by="auto-from-item", now=now - dt.timedelta(hours=2))
    conn.commit()

    history_since = api._item_transitions_history_since(conn)
    m = api._metric_verified_unattended_fixes_per_week(conn, now, history_since)
    assert m["value"] == 1, m
    assert m["unavailable"] is None, m
    assert m["fixed_in_window"] == 1, m
    assert m["unattended_in_window"] == 1, m
    conn.close()


def test_metric4_counts_an_item_once_even_if_it_reaches_fixed_twice():
    """The numerator is a count of ITEMS, so the `fixed` transitions must be
    DISTINCT on event_id. An item can genuinely enter `fixed` more than once
    in one window (`fixed` -> a failed liveness probe reopens it to `new` ->
    `fixed` again) and still counts once."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _seed_old_history_anchor(conn, now)
    _event(conn, 1, now)
    _transition(conn, 1, "working", "fixed", now - dt.timedelta(hours=5))
    _transition(conn, 1, "new", "fixed", now - dt.timedelta(hours=1))
    conn.commit()

    history_since = api._item_transitions_history_since(conn)
    m = api._metric_verified_unattended_fixes_per_week(conn, now, history_since)
    assert m["fixed_in_window"] == 1, f"one item, twice fixed, counts once: {m}"
    assert m["value"] == 1, m
    conn.close()


def test_metric4_null_with_history_reason_when_no_fixed_transitions_at_all():
    """An old-enough history anchor with zero `fixed` transitions anywhere
    in the window — a real, honest 'no basis', never a fabricated 0."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _seed_old_history_anchor(conn, now)
    conn.commit()

    history_since = api._item_transitions_history_since(conn)
    m = api._metric_verified_unattended_fixes_per_week(conn, now, history_since)
    assert m["value"] is None, m
    assert m["unavailable"] and len(m["unavailable"]) > 0, m
    assert m["fixed_in_window"] == 0, m
    conn.close()


def test_metric4_null_not_zero_when_history_is_young():
    """Same shape as the metric-3 guard test: item_transitions' only row is
    1h old, so the 7-day window predates history_since — the metric must
    stay null with a reason, never the fabricated `1` (or `0`) a naive count
    would serve here."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _transition(conn, 1, "working", "fixed", now - dt.timedelta(hours=1))
    _operation(conn, "op1", event_id=1, authorized_by="auto-from-item", now=now - dt.timedelta(hours=2))
    conn.commit()

    history_since = api._item_transitions_history_since(conn)
    m = api._metric_verified_unattended_fixes_per_week(conn, now, history_since)
    assert m["value"] is None, m
    assert "history_since" in m["unavailable"] or "no rows yet" in m["unavailable"], m
    assert m["fixed_in_window"] == 0, m
    conn.close()


# --- Metric 5: poller ages --------------------------------------------------------

def test_metric5_per_poller_ages_and_worst_case():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _cursor(conn, "triage_last_run", now - dt.timedelta(minutes=5))
    _cursor(conn, "watchdog_poll_last_run", now - dt.timedelta(minutes=120))
    conn.commit()
    # dispatch_sweep_last_run: never recorded.

    m = api._metric_poller_ages(conn, now)
    assert m["pollers"]["loop"]["age_minutes"] is not None
    assert abs(m["pollers"]["loop"]["age_minutes"] - 5) < 0.1
    assert m["pollers"]["watchdog_poll"]["stale"] is True  # 120min > 90min threshold
    assert m["pollers"]["dispatch_sweep"]["age_minutes"] is None
    assert m["pollers"]["dispatch_sweep"]["unavailable"]
    assert abs(m["value"] - 120) < 0.1, "worst-case age across available pollers"
    conn.close()


# --- Metric 6: reverts and reopen-after-fixed -----------------------------------

def test_metric6_reopen_after_fixed_counts_and_reverts_is_a_real_zero():
    """`reverts` is `revert_pr` on the item — a `0`
    here is a genuine measurement, not the permanent `null` it used to be."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _seed_old_history_anchor(conn, now)  # keep history_since well before the 7d window
    _event(conn, 1, now)
    _transition(conn, 1, "fixed", "new", now - dt.timedelta(hours=1))
    conn.commit()

    history_since = api._item_transitions_history_since(conn)
    m = api._metric_reverts_and_reopens(conn, now, history_since)
    assert m["reopen_after_fixed"]["value"] == 1, m
    assert m["reverts"]["value"] == 0, m
    assert m["reverts"]["unavailable"] is None, m
    conn.close()


def test_metric6_reverts_counts_revert_pr_within_window():
    """`warden revert` leaves the item `failed` with `revert_pr` set — windowed on
    `updated_at` like every other leaf here. An item updated outside the window
    must not count."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _seed_old_history_anchor(conn, now)
    _event(conn, 1, now)
    _item(conn, 1, state="failed", now=now, updated_at=now - dt.timedelta(hours=1))
    conn.execute("UPDATE triage_items SET revert_pr = 41 WHERE event_id = 1")
    _event(conn, 2, now)
    _item(conn, 2, state="verifying", now=now, updated_at=now - dt.timedelta(hours=1))
    conn.execute("UPDATE triage_items SET revert_pr = 42 WHERE event_id = 2")
    _event(conn, 3, now)
    _item(conn, 3, state="failed", now=now, updated_at=now - dt.timedelta(days=30))  # outside window
    conn.execute("UPDATE triage_items SET revert_pr = 43 WHERE event_id = 3")
    _event(conn, 4, now)
    _item(conn, 4, state="failed", now=now, updated_at=now - dt.timedelta(hours=1))  # failed, but not a revert
    conn.commit()

    history_since = api._item_transitions_history_since(conn)
    m = api._metric_reverts_and_reopens(conn, now, history_since)
    assert m["reverts"]["value"] == 2, m
    assert m["reverts"]["unavailable"] is None, m
    conn.close()


def test_metric6_reopen_after_fixed_is_null_not_zero_when_history_is_young():
    """Same transition as the test above MINUS the old anchor: the only row
    in item_transitions is 1h old, so the 7-day window predates
    history_since. `reopen_after_fixed` must be null with a reason — this is
    the exact leaf a mutation serving a fabricated `0` on a young table would
    turn green again."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _transition(conn, 1, "fixed", "new", now - dt.timedelta(hours=1))
    conn.commit()

    history_since = api._item_transitions_history_since(conn)
    m = api._metric_reverts_and_reopens(conn, now, history_since)
    assert m["reopen_after_fixed"]["value"] is None, m
    assert m["reopen_after_fixed"]["unavailable"], m
    conn.close()


# --- history_since ---------------------------------------------------------------

def test_history_since_null_on_empty_item_transitions():
    conn, _ = _fresh_conn()
    payload = api.metrics_payload(conn)
    assert payload["history_since"] is None
    conn.close()


def test_history_since_is_earliest_transition():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    earliest = now - dt.timedelta(days=3)
    _transition(conn, 1, "new", "working", earliest)
    _transition(conn, 1, "working", "needs_decision", now - dt.timedelta(days=1))
    conn.commit()
    payload = api.metrics_payload(conn)
    assert payload["history_since"] == _iso(earliest)
    conn.close()


# --- /board ------------------------------------------------------------------

def test_board_counts_items_shape_ordering_and_terminal_24h():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    for i, state in enumerate(api.CHAIN_STATES, start=1):
        _event(conn, i, now)
        _item(conn, i, state=state, dispatch_job=f"j{i}", now=now, updated_at=now - dt.timedelta(minutes=i))
    human_id = len(api.CHAIN_STATES) + 1
    _event(conn, human_id, now)
    _item(conn, human_id, state="needs_decision", origin="human", now=now, updated_at=now - dt.timedelta(seconds=5))
    fixed_id = human_id + 1
    _event(conn, fixed_id, now)
    _item(conn, fixed_id, state="fixed", now=now, updated_at=now - dt.timedelta(hours=1))
    closed_id = fixed_id + 1
    _event(conn, closed_id, now)
    _item(conn, closed_id, state="closed", now=now, updated_at=now - dt.timedelta(days=3))
    conn.commit()

    payload = api.board_payload(conn)
    for state in api.CHAIN_STATES:
        expected = 2 if state == "needs_decision" else 1
        assert payload["counts"][state] == expected, payload["counts"]
    assert payload["terminal_24h"] == 1, "only the 1h-old `fixed` row, not the 3d-old `closed` row"
    assert len(payload["items"]) == len(api.CHAIN_STATES) + 1, "terminal items must be excluded"
    assert "truncated" not in payload

    updated_ats = [item["updated_at"] for item in payload["items"]]
    assert updated_ats == sorted(updated_ats, reverse=True), "items must be ORDER BY updated_at DESC"

    one = next(item for item in payload["items"] if item["event_id"] == 1)
    assert set(one.keys()) == {
        "event_id", "origin", "repo", "state", "close_reason", "strikes", "retry_at", "max_tier", "title", "note",
        "pr_url", "dispatch_job", "implement_job", "validation_job", "occurrences",
        "revision_count", "created_at", "updated_at", "origin_channel", "origin_thread_ts",
        "availableActions", "issue",
    }
    assert one["title"] == "title1"
    conn.close()


def test_board_item_available_actions_and_issue_shape():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)

    _event(conn, 1, now)
    _item(conn, 1, state="working", now=now)

    _event(conn, 2, now)
    _item(conn, 2, state="closed", now=now)

    issue_payload = json.dumps({"repo": "foo", "number": 3, "author": "someone-else", "labels": ["bug"]})
    conn.execute(
        "INSERT INTO events (id, source, external_id, title, url, payload_json, first_seen) "
        "VALUES (?,?,?,?,?,?,?)",
        (3, "s", "e3", "title3", "https://github.com/jkrumm/foo/issues/3", issue_payload, _iso(now)),
    )
    _item(conn, 3, state="working", origin="github_issue", now=now)

    conn.execute(
        "INSERT INTO events (id, source, external_id, title, url, payload_json, first_seen) "
        "VALUES (?,?,?,?,?,?,?)",
        (4, "s", "e4", "title4", None, json.dumps([1, 2, 3]), _iso(now)),
    )
    _item(conn, 4, state="working", origin="github_issue", now=now)
    conn.commit()

    rows = {
        row["event_id"]: row
        for row in conn.execute(
            "SELECT ti.*, e.title AS event_title, e.url AS event_url, "
            "e.payload_json AS event_payload_json FROM triage_items ti "
            "JOIN events e ON e.id = ti.event_id"
        ).fetchall()
    }

    working_item = api._board_item(rows[1])
    assert set(working_item["availableActions"]) == {"note"}, "a working item is in nobody's hands"
    assert working_item["issue"] is None

    closed_item = api._board_item(rows[2])
    assert closed_item["availableActions"] == [], closed_item["availableActions"]
    assert closed_item["issue"] is None

    issue_item = api._board_item(rows[3])
    assert issue_item["issue"] == {
        "repo": "foo",
        "number": 3,
        "url": "https://github.com/jkrumm/foo/issues/3",
        "author": "someone-else",
        "trusted": False,
        "labels": ["bug"],
    }

    # A github_issue-origin row whose payload_json is valid JSON but not an
    # object (e.g. a list) must yield `issue: None`, never raise.
    non_dict_payload_item = api._board_item(rows[4])
    assert non_dict_payload_item["issue"] is None
    conn.close()


def test_available_actions_follow_the_new_state_machine():
    """The owner's verbs per state — needs_decision and failed are the two states an
    owner acts on; merge is offered only for a PR whose review confirmed."""
    a = api._available_actions
    assert set(a("needs_decision")) == {"implement", "dismiss", "reinvestigate", "note"}
    assert set(a("needs_decision", mergeable=True)) == {"implement", "merge", "dismiss", "reinvestigate", "note"}
    assert set(a("failed", mergeable=True)) == {"implement", "merge", "dismiss", "reinvestigate", "note"}
    assert set(a("new")) == {"dismiss", "note"} and set(a("triaged")) == {"dismiss", "note"}
    assert set(a("quiet")) == {"dismiss", "reinvestigate"}
    for in_flight in ("working", "merging", "verifying"):
        assert a(in_flight, mergeable=True) == ["note"], in_flight
    for terminal in ("fixed", "closed"):
        assert a(terminal) == [], terminal


def test_awaiting_owner_lists_needs_decision_and_failed_without_the_retired_fields():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    for i, state in enumerate(("needs_decision", "failed", "working"), start=1):
        _event(conn, i, now)
        _item(conn, i, state=state, now=now, note=f"note {i}")
        _transition(conn, i, "new", state, now - dt.timedelta(days=i))
    conn.commit()
    rows = api.awaiting_owner(conn, now)
    assert [r["event_id"] for r in rows] == [2, 1], rows   # oldest first
    assert {r["state"] for r in rows} == {"needs_decision", "failed"}
    for r in rows:
        assert r["kind"] == "item" and "parked_recurrences" not in r, r
    conn.close()


def test_board_caps_items_at_200_and_reports_truncated():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    for i in range(1, api.BOARD_ITEMS_CAP + 5):
        _event(conn, i, now)
        _item(conn, i, state="new", now=now, updated_at=now - dt.timedelta(seconds=i))
    conn.commit()

    payload = api.board_payload(conn)
    assert len(payload["items"]) == api.BOARD_ITEMS_CAP
    assert payload["truncated"] is True
    conn.close()


# --- /items/<event_id> ---------------------------------------------------------

def test_item_payload_full_shape():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _item(conn, 1, state="working", dispatch_job="j1", now=now)
    verdict = {
        "summary": "s", "nextAction": "implement", "confidence": "high",
        "recommendation": "do it", "outcome": "pending", "schemaVersion": 1,
    }
    _dispatch(conn, "j1", tier="investigate", verdict_json=json.dumps(verdict), origin_event_id=1, now=now)
    _operation(conn, "op1", event_id=1, now=now)
    _transition(conn, 1, "new", "working", now)
    conn.commit()

    payload = api.item_payload(conn, 1)
    assert payload["item"]["event_id"] == 1
    assert payload["item"]["state"] == "working"
    assert payload["item"]["brief_truncated"] is False
    assert payload["event"]["title"] == "title1"
    assert payload["event"]["payload"] is None

    assert len(payload["dispatches"]) == 1, payload["dispatches"]
    d = payload["dispatches"][0]
    assert d["job_id"] == "j1"
    assert d["verdict"] == verdict

    assert len(payload["operations"]) == 1, payload["operations"]
    assert payload["operations"][0]["op_id"] == "op1"

    # The fixture item was inserted directly (no _record_created_transition()
    # row), so item_payload() prepends the synthetic `created` entry ahead of
    # the one real transition.
    assert len(payload["transitions"]) == 2, payload["transitions"]
    assert payload["transitions"][0]["from_state"] is None
    assert payload["transitions"][0]["to_state"] == "new"
    assert payload["transitions"][0]["synthetic"] is True
    assert payload["transitions"][1]["from_state"] == "new"
    assert payload["transitions"][1]["to_state"] == "working"
    assert payload["transitions_total"] == 1
    assert payload["operations_total"] == 1

    conn.close()


def test_item_payload_brief_truncated_at_2000_chars():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _item(conn, 1, state="working", origin="human", now=now)
    conn.execute("UPDATE triage_items SET brief = ? WHERE event_id = 1", ("x" * 3000,))
    conn.commit()

    payload = api.item_payload(conn, 1)
    assert payload["item"]["brief_truncated"] is True
    assert len(payload["item"]["brief"]) == 2000
    conn.close()


def test_item_payload_raises_not_found():
    conn, _ = _fresh_conn()
    try:
        api.item_payload(conn, 999999)
        raise AssertionError("expected ItemNotFoundError")
    except api.ItemNotFoundError:
        pass
    conn.close()


def test_item_payload_synthetic_created_entry_for_legacy_item():
    """A legacy item (inserted directly, like every fixture in this file, and
    like every item created before _record_created_transition() existed) has
    no `from_state IS NULL` row. item_payload() must fabricate one, reading
    the state it was created into off the first REAL transition's from_state
    — never off the item's current state, which would lie once the item has
    moved on."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _item(conn, 1, state="needs_decision", now=now)
    _transition(conn, 1, "new", "working", now)
    _transition(conn, 1, "working", "needs_decision", now)
    conn.commit()

    payload = api.item_payload(conn, 1)
    assert len(payload["transitions"]) == 3, payload["transitions"]
    synthetic = payload["transitions"][0]
    assert synthetic["id"] is None
    assert synthetic["event_id"] == 1
    assert synthetic["from_state"] is None
    assert synthetic["to_state"] == "new"
    assert synthetic["note"] == "created"
    assert synthetic["synthetic"] is True
    assert payload["transitions_total"] == 2, "synthetic entry must not count toward the real total"
    conn.close()


def test_item_payload_synthetic_created_entry_falls_back_to_item_state():
    """An item with zero recorded transitions has no earlier fact to read —
    the synthetic entry falls back to the item's own current state."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _item(conn, 1, state="new", now=now)
    conn.commit()

    payload = api.item_payload(conn, 1)
    assert len(payload["transitions"]) == 1, payload["transitions"]
    assert payload["transitions"][0]["to_state"] == "new"
    assert payload["transitions_total"] == 0
    conn.close()


def test_item_payload_event_reminder_fields_present():
    """`event.reminder_count`/`event.last_reminder_at` are watchdog-poll.py's
    grouped-source re-emit clock (the occurrence count triage reads)."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _item(conn, 1, state="new", now=now)
    conn.execute(
        "UPDATE events SET reminder_count = 3, last_reminder_at = ? WHERE id = 1", (_iso(now),)
    )
    conn.commit()

    payload = api.item_payload(conn, 1)
    assert payload["event"]["reminder_count"] == 3
    assert payload["event"]["last_reminder_at"] == _iso(now)
    conn.close()


def test_item_payload_history_limit_truncates_newest_first_oldest_returned():
    """`history_limit` keeps the newest N transitions/operations, still
    returned oldest-first, with the real totals always present — and skips
    the synthetic `created` entry once truncated, since the fetched window
    can no longer prove no genuine `from_state IS NULL` row exists earlier."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _item(conn, 1, state="needs_decision", now=now)
    # A real `created` row up front (from_state NULL) so the unbounded read
    # below already has one and this test isn't also exercising the
    # synthetic-entry path covered elsewhere.
    _transition(conn, 1, None, "new", now)
    states = ["new", "investigating", "verdict", "implementing", "needs_decision"]
    for i in range(len(states) - 1):
        _transition(conn, 1, states[i], states[i + 1], now + dt.timedelta(minutes=i + 1))
    for i in range(3):
        _operation(conn, f"op{i}", event_id=1, now=now + dt.timedelta(minutes=i))
    conn.commit()

    full = api.item_payload(conn, 1)
    assert len(full["transitions"]) == 5, full["transitions"]
    assert full["transitions_total"] == 5

    limited = api.item_payload(conn, 1, history_limit=2)
    assert limited["transitions_total"] == 5
    assert len(limited["transitions"]) == 2, limited["transitions"]
    # Newest 2 of the 4 real transitions, still oldest-first, no synthetic entry.
    assert limited["transitions"][0]["from_state"] == "verdict"
    assert limited["transitions"][0]["to_state"] == "implementing"
    assert limited["transitions"][1]["from_state"] == "implementing"
    assert limited["transitions"][1]["to_state"] == "needs_decision"
    assert not any(t.get("synthetic") for t in limited["transitions"])

    assert limited["operations_total"] == 3
    limited_ops = api.item_payload(conn, 1, history_limit=2)["operations"]
    assert len(limited_ops) == 2, limited_ops
    assert [o["op_id"] for o in limited_ops] == ["op1", "op2"]
    conn.close()


def test_items_invalid_id_is_400_and_unknown_id_is_404():
    conn, path = _fresh_conn()
    conn.close()
    handle = _ServerHandle(path)
    try:
        status, body = handle.get("/items/abc")
        assert status == 400, (status, body)
        assert "event_id" in body.get("error", ""), body
        status, body = handle.get("/items/999999")
        assert status == 404, (status, body)
        assert "999999" in body.get("error", ""), body
    finally:
        handle.stop()


def test_items_prefix_does_not_hijack_itemsx():
    conn, path = _fresh_conn()
    conn.close()
    handle = _ServerHandle(path)
    try:
        status, body = handle.get("/itemsx")
        assert status == 404, (status, body)
        assert body.get("error") == "no such path: /itemsx", body
    finally:
        handle.stop()


def test_board_is_200_over_http():
    conn, path = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _item(conn, 1, state="new", now=now)
    conn.commit()
    conn.close()
    handle = _ServerHandle(path)
    try:
        status, body = handle.get("/board")
        assert status == 200, body
        for key in ("generated_at", "schema_version", "counts", "items", "terminal_24h"):
            assert key in body, f"missing {key}"
    finally:
        handle.stop()


def test_items_by_id_is_200_over_http():
    conn, path = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _item(conn, 1, state="new", now=now)
    conn.commit()
    conn.close()
    handle = _ServerHandle(path)
    try:
        status, body = handle.get("/items/1")
        assert status == 200, body
        for key in ("item", "event", "dispatches", "operations", "transitions"):
            assert key in body, f"missing {key}"
    finally:
        handle.stop()


# --- HTTP-level: schema mismatch, method surface, read-only structural guard ----

def _wait_for_socket(host: str, port: int, *, attempts: int = 200, interval: float = 0.025) -> None:
    """Bounded-RETRY-COUNT readiness check, not a fixed sleep: a raw TCP
    connect, cheaper and less scheduling-sensitive than a full HTTP round
    trip (a mutation-test run driving many servers back to back previously
    hit `TimeoutError: ... never came up` here purely from thread-scheduling
    delay under load, unrelated to the code under test). `listen()` already
    ran synchronously inside `HTTPServer.__init__` before this is ever
    called, so a successful connect means the OS has a live listener even if
    `serve_forever()` hasn't reached its first `accept()` yet."""
    last_err: OSError | None = None
    for _ in range(attempts):
        try:
            with socket.create_connection((host, port), timeout=interval):
                return
        except OSError as e:
            last_err = e
            time.sleep(interval)
    raise TimeoutError(f"server on {host}:{port} never came up: {last_err}")


class _ServerHandle:
    def __init__(self, db_path: Path):
        import http.server
        self._original_db_path = api.DB_PATH
        api.DB_PATH = db_path
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), api.Handler)
        self.port = self.server.server_address[1]
        import threading
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()
        try:
            _wait_for_socket("127.0.0.1", self.port)
        except Exception:
            # A failed startup must never leak a live thread + bound socket:
            # the caller's own `try/finally: handle.stop()` never runs when
            # THIS constructor is what raises (`handle` is never assigned).
            self.server.shutdown()
            self.server.server_close()
            api.DB_PATH = self._original_db_path
            raise

    def get(self, path: str) -> tuple[int, dict]:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("GET", path)
        resp = conn.getresponse()
        body = json.loads(resp.read().decode())
        status = resp.status
        conn.close()
        return status, body

    def request(self, method: str, path: str) -> int:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request(method, path)
        resp = conn.getresponse()
        resp.read()
        status = resp.status
        conn.close()
        return status

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        api.DB_PATH = self._original_db_path


def test_schema_mismatch_returns_503_not_200():
    path = _tmp_db()
    conn = ledger.connect(path, migrate=True)
    conn.execute("UPDATE schema_version SET version = ?", (ledger.LEDGER_SCHEMA_VERSION + 1,))
    conn.commit()
    conn.close()

    handle = _ServerHandle(path)
    try:
        status, body = handle.get("/metrics")
        assert status == 503, (status, body)
        assert "schema_version" in body.get("error", ""), body
        status, body = handle.get("/health")
        assert status == 503, (status, body)
        status, body = handle.get("/board")
        assert status == 503, (status, body)
        assert "schema_version" in body.get("error", ""), body
        status, body = handle.get("/items/1")
        assert status == 503, (status, body)
        assert "schema_version" in body.get("error", ""), body
    finally:
        handle.stop()


def test_post_is_405_and_unknown_path_is_404():
    conn, path = _fresh_conn()
    conn.close()
    handle = _ServerHandle(path)
    try:
        assert handle.request("POST", "/metrics") == 405
        assert handle.request("PUT", "/health") == 405
        status, body = handle.get("/nope")
        assert status == 404, (status, body)
    finally:
        handle.stop()


def test_metrics_and_health_are_200_with_expected_top_level_keys():
    conn, path = _fresh_conn()
    conn.close()
    handle = _ServerHandle(path)
    try:
        status, body = handle.get("/metrics")
        assert status == 200, body
        for key in ("verdicts_recorded_disposition", "verified_fixes_vs_silence",
                    "median_needs_decision_to_decision_hours", "verified_unattended_fixes_per_week",
                    "poller_ages", "reverts_and_reopens", "history_since", "window_days"):
            assert key in body, f"missing {key}"
        status, body = handle.get("/health")
        assert status == 200, body
        assert "ok" in body
    finally:
        handle.stop()


def test_handler_serves_normally_off_a_read_only_file():
    """Structural, not a comment: chmod the ledger FILE itself to 0400 (owner
    read-only — the directory stays writable, which is what lets a read-only
    WAL connection create the `-shm` index it needs regardless of who can
    write the data) and confirm both endpoints still serve 200. If the
    handler's connection ever attempted a real write, this file's permission
    bits would turn that into a 503, not a comment turning green on its own."""
    conn, path = _fresh_conn()
    conn.close()

    file_mode = path.stat().st_mode
    path.chmod(stat.S_IRUSR)
    try:
        handle = _ServerHandle(path)
        try:
            status, _ = handle.get("/metrics")
            assert status == 200, "handler must serve fine off a file it cannot write"
            status, _ = handle.get("/health")
            assert status == 200
        finally:
            handle.stop()
    finally:
        path.chmod(file_mode)


def test_handler_connection_refuses_an_insert():
    """Proves the underlying MECHANISM: a connection opened with
    `ledger.connect(path, readonly=True)` — the `mode=ro` URI flag, not
    filesystem permission bits — refuses a write. This test builds that
    connection itself, hardcoded, so it can NEVER observe what `_serve()`
    actually passes at its own call site; a mutation dropping `readonly=True`
    from `_serve()` would sail straight through this test unnoticed. That
    call-site guarantee is `test_serve_opens_connection_with_readonly_true`
    below, which drives a real request and inspects the handler's own
    `ledger.connect` invocation."""
    conn, path = _fresh_conn()
    conn.close()
    ro = api._ledger.connect(path, readonly=True)
    try:
        ro.execute("INSERT INTO cursors(key,value,updated_at) VALUES ('x','y','z')")
        raise AssertionError("a connection opened exactly as api.py's handler does allowed a write")
    except sqlite3.OperationalError:
        pass
    finally:
        ro.close()


def test_serve_opens_connection_with_readonly_true():
    """Observes `_serve()`'s OWN call to `ledger.connect`, not a hardcoded
    replica of it: monkeypatch `api._ledger.connect` to record every call's
    args/kwargs, drive one real request through a running server, then assert
    `readonly=True` was passed AND that a connection opened with those exact
    recorded args/kwargs refuses a write. Dropping `readonly=True` from
    `_serve()` turns this test red BY NAME — verified by mutation."""
    conn, path = _fresh_conn()
    conn.close()

    calls: list[tuple[tuple, dict]] = []
    original_connect = api._ledger.connect

    def _recording_connect(*args, **kwargs):
        calls.append((args, kwargs))
        return original_connect(*args, **kwargs)

    api._ledger.connect = _recording_connect
    try:
        handle = _ServerHandle(path)
        try:
            status, _ = handle.get("/health")
            assert status == 200, "sanity: the request driving this test must actually succeed"
        finally:
            handle.stop()
    finally:
        api._ledger.connect = original_connect

    assert calls, "the handler never called ledger.connect at all"
    args, kwargs = calls[0]
    assert kwargs.get("readonly") is True, (
        f"_serve() must open its connection with readonly=True — got args={args} kwargs={kwargs}"
    )
    # Re-derive a connection with the handler's own exact recorded args/kwargs
    # (the handler's own connection is already closed by _serve()'s finally
    # by the time this test regains control) and prove it refuses a write.
    ro = original_connect(*args, **kwargs)
    try:
        try:
            ro.execute("INSERT INTO cursors(key,value,updated_at) VALUES ('x','y','z')")
            raise AssertionError("a connection opened with the handler's own recorded call args allowed a write")
        except sqlite3.OperationalError:
            pass
    finally:
        ro.close()


# --- runner ------------------------------------------------------------------

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
