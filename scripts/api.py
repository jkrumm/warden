"""warden HTTP API — GET /metrics, /health, /board, /items/<event_id>, read-only.

WHAT THIS IS STILL NOT. `POST /items/:id/note` is DESIGN.md's real contract
(§ HTTP API) and is Wave 4, built alongside Argo — the only consumer of the
write-adjacent endpoints. Building it now, unconsumed, would be dead surface with
no caller to prove it correct. `/board` and `/items/<event_id>` (Wave 6.3) are the
two read-only projections that ARE useful the moment they exist: a
funnel-snapshot list of every non-terminal item, and the full detail behind any
one item — dispatches, verdicts, operations, transition history.

BIND AND AUTH. `127.0.0.1:7735` (reserved by comment in dotfiles' Caddyfile
registry; it sat on 7734 until 2026-09-11, where sy-serendipity's `kill-port
7734` dev script would have killed it), loopback only, no bearer token. Per DESIGN.md § Security
model, an episode on this host runs unrestricted `Bash` under
`--dangerously-skip-permissions` — a bearer token would be theatre, not a
boundary, because anything that can read a token file can also just query this
socket directly from the same host. The actual protections are the loopback
bind (nothing off-box can reach it at all) and the read-only handle (nothing
that *does* reach it can write). No Caddy or tailnet door is added in this
wave — that belongs with Argo (Wave 4), the only intended remote consumer.

GET ONLY. Every other method is 405; an unknown path is 404. Both as JSON.

ONE FRESH READ-ONLY CONNECTION PER REQUEST, never a handle held across the
process lifetime. Two reasons:
  1. A WAL reader connection pinned at startup can keep serving a snapshot from
     the moment it was opened — SQLite's own read consistency model, not a bug —
     so a long-lived handle would silently stop reflecting new commits.
  2. `ledger.assert_schema_version()` checked once at boot says nothing about the
     ledger ten minutes later, after the loop has migrated it forward (or, in the
     failure case that matters, DIDN'T and the file is now stuck mid-migration).
A failed assertion is a 503 with the mismatch in the body, never a 200 with
stale numbers: this is a control plane, and a metrics endpoint that lies about
its own liveness is the exact failure mode the rest of this repo exists to
remove.

Note on `ledger.connect(DB_PATH, readonly=True)`: the readonly branch of
`connect()` does NOT call `assert_schema_version()` — it opens
`file:...?mode=ro` and returns immediately (see ledger.py's `connect()`, the
`if readonly:` branch). So this module calls `assert_schema_version()` itself,
explicitly, on every request's connection, rather than trusting that opening
readonly already checked it.

JSON, NOT PROMETHEUS TEXT FORMAT, despite the path being named `/metrics`
(matching DESIGN.md's own naming). The named consumer is Argo (DESIGN.md §
Observability: "Funnel metrics — the six numbers above, at /metrics, rendered
in Argo"), and the six numbers are a funnel snapshot, not a time series a
scrape-and-store system would want. A future Prometheus consumer is not
precluded — it would be a second endpoint, not a reinterpretation of this one.

Run: .venv/bin/python3 scripts/api.py --serve
"""

from __future__ import annotations

import datetime as dt
import http.server
import importlib.util
import json
import os
import sqlite3
import statistics
import sys
from pathlib import Path
from typing import Any, Callable

# Same env-var-first, documented-default-second shape every other script in
# this repo uses for DB_PATH — see ledger.py's own comment on why it is a
# module-level global re-read at call time rather than baked into a default
# argument.
WARDEN_HOME = (
    Path(os.environ["WARDEN_HOME"]).expanduser() if os.environ.get("WARDEN_HOME") else Path.home() / ".warden"
)
DB_PATH = Path(os.environ["WARDEN_DB"]).expanduser() if os.environ.get("WARDEN_DB") else WARDEN_HOME / "warden.db"

# scripts/ledger.py — loaded by path, the same mechanism watchdog-poll.py and
# dispatch-sweep.py already use for their own sibling loads.
_LEDGER_PATH = Path(__file__).resolve().parent / "ledger.py"
_ledger_spec = importlib.util.spec_from_file_location("ledger", _LEDGER_PATH)
assert _ledger_spec and _ledger_spec.loader, "Failed to load scripts/ledger.py"
_ledger = importlib.util.module_from_spec(_ledger_spec)
_ledger_spec.loader.exec_module(_ledger)

# scripts/ (this file's own directory) onto sys.path so `clients` is
# importable as a real package — same reasoning as triage.py's own sys.path
# insert: `clients` has internal `from clients import` statements that only
# resolve through a normal import, not exec_module().
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from clients import github as _github  # noqa: E402

BIND_HOST = "127.0.0.1"
BIND_PORT = 7735

# --- Funnel window + disposition vocabulary -----------------------------------

WINDOW_DAYS = 7

# Every state that counts as a RECORDED disposition for a verdict: the item left
# the investigation and landed somewhere visible. `quiet` is deliberately absent:
# a signal going quiet cancels the need to START work, it never discharges an
# obligation a verdict already created. `new`, `triaged` and `working` are absent
# because the verdict has not reached a disposition yet — they are mid-funnel.
# Spelled with ledger.py's STATE_* constants (the one place the vocabulary lives); this
# module still never imports the act-loop itself.
DISPOSITION_STATES = (
    _ledger.STATE_MERGING, _ledger.STATE_VERIFYING, _ledger.STATE_FIXED, _ledger.STATE_CLOSED,
    _ledger.STATE_NEEDS_DECISION, _ledger.STATE_FAILED,
)

# --- /board's state vocabulary -------------------------------------------------
#
# The non-terminal states of the machine, in the order an item moves through
# them, each guaranteed a (possibly zero) entry in `/board`'s counts.
BOARD_ITEMS_CAP = 200

CHAIN_STATES = (
    _ledger.STATE_NEW, _ledger.STATE_TRIAGED, _ledger.STATE_WORKING, _ledger.STATE_MERGING,
    _ledger.STATE_VERIFYING, _ledger.STATE_NEEDS_DECISION, _ledger.STATE_FAILED,
)

# --- /board's per-item `availableActions` ---------------------------------
#
# Mirrors triage.py's apply_argo_actions() per-verb allowed-state sets
# (ARGO_ACTION_VERBS = {"implement", "merge", "dismiss", "reinvestigate",
# "note"}). Must stay in sync by hand with apply_argo_actions()'s handlers if
# either changes. implement and merge share one set: both act on an item that is
# waiting on the owner.
_OWNER_STATES = (_ledger.STATE_NEEDS_DECISION, _ledger.STATE_FAILED)
_DISMISS_STATES = (_ledger.STATE_NEW, _ledger.STATE_TRIAGED, *_OWNER_STATES, _ledger.STATE_QUIET)
_REINVESTIGATE_STATES = (*_OWNER_STATES, _ledger.STATE_QUIET)


def _available_actions(state: str, mergeable: bool = False, reverted: bool = False) -> list[str]:
    """Zero or more of `implement`/`merge`/`dismiss`/`reinvestigate`/`note` —
    what the owner could click for a card in `state`, from state alone. The
    real per-repo/per-tier gate runs server-side in triage.py's
    apply_argo_actions() when an action is actually applied; this list is
    only what the UI offers. A `reverted` item (`revert_pr` set) already merged and was
    rolled back by hand: re-implementing or re-merging it would redo the very change
    that was reverted, so neither is offered."""
    actions: list[str] = []
    if state in _OWNER_STATES and not reverted:
        actions.append("implement")
    # `mergeable`: a PR whose step-7 review confirmed — the one shape an owner
    # merge can land.
    if mergeable and state in _OWNER_STATES and not reverted:
        actions.append("merge")
    if state in _DISMISS_STATES:
        actions.append("dismiss")
    if state in _REINVESTIGATE_STATES:
        actions.append("reinvestigate")
    if state not in _ledger.TERMINAL_STATES:
        actions.append("note")
    return actions

# cursors key -> the LaunchAgent StartInterval it heartbeats against (seconds),
# read straight from each plist template. Named individually, not folded into
# one "poller last ran" number, so /health and /metrics can answer "which one
# is blind" rather than only "something is."
LOOP_INTERVAL_S = 600      # com.jkrumm.warden-loop.plist.template
POLL_INTERVAL_S = 1800     # com.jkrumm.warden-poll.plist.template
SWEEP_INTERVAL_S = 300     # com.jkrumm.warden-sweep.plist.template
STALE_MULTIPLE = 3         # a poller silent for 3x its own interval is a finding, not noise

POLLERS: dict[str, tuple[str, int]] = {
    "loop": ("triage_last_run", LOOP_INTERVAL_S),
    "watchdog_poll": ("watchdog_poll_last_run", POLL_INTERVAL_S),
    "dispatch_sweep": ("dispatch_sweep_last_run", SWEEP_INTERVAL_S),
}


def _poller_age_minutes(conn: sqlite3.Connection, now: dt.datetime, key: str) -> tuple[float | None, str | None]:
    """(age_minutes, last_run_iso) for one poller's heartbeat cursor, or
    (None, None) if it has never recorded one."""
    row = conn.execute("SELECT updated_at FROM cursors WHERE key=?", (key,)).fetchone()
    if row is None:
        return None, None
    last_run = dt.datetime.fromisoformat(row["updated_at"])
    return (now - last_run).total_seconds() / 60, row["updated_at"]


# --- The six funnel numbers ----------------------------------------------------
#
# Every metric below returns the same leaf shape: {"value": ..., "unavailable":
# ...} plus whatever extra fields explain the value. `value` is None ONLY when
# paired with a non-empty `unavailable` reason — never a fabricated 0. A window
# that predates item_transitions (see metrics_payload()'s `history_since`) is
# empty BY CONSTRUCTION, not a measured zero, and nothing here is allowed to
# blur that distinction.

def _metric_verdicts_recorded_disposition(conn: sqlite3.Connection) -> dict[str, Any]:
    """# 1 — verdicts -> recorded disposition. All-time.

    Denominator is investigate dispatches with a verdict AND
    `origin_event_id IS NOT NULL` — that column is exactly "the loop
    originated this", not "a triage_item happens to link to it" (the latter
    would circularly exclude the very no-item failures this metric exists to
    count). The other verdict-carrying investigate dispatches
    (`origin_event_id IS NULL`, `origin_channel`/`origin_thread_ts` set
    instead) are interactive Slack dispatches a human asked `warden` to
    run: they were never items in warden's funnel and no item will ever point
    at them, so they are reported separately as `excluded_interactive` rather
    than silently dropped or miscounted into the denominator.

    `item_states`' total is deliberately allowed to exceed the denominator: a
    clustered dispatch joins several `triage_items` rows, so counting items
    (not dispatches) in the breakdown while dispatches (not items) are the
    numerator/denominator was the exact confusion that made a live 5/17
    ratio read next to a 16-item breakdown as if it were about the same 17."""
    denominator = conn.execute(
        "SELECT COUNT(*) AS n FROM dispatches "
        "WHERE tier='investigate' AND verdict_json IS NOT NULL AND origin_event_id IS NOT NULL"
    ).fetchone()["n"]
    excluded_interactive = conn.execute(
        "SELECT COUNT(*) AS n FROM dispatches "
        "WHERE tier='investigate' AND verdict_json IS NOT NULL AND origin_event_id IS NULL"
    ).fetchone()["n"]
    excluded_interactive_note = (
        "verdict-carrying investigate dispatches with no origin_event_id — a human asked warden "
        "to investigate interactively; these never entered warden's funnel as an item and never will, "
        "so they are excluded from numerator and denominator rather than silently miscounted"
    )
    item_states_note = "counts triage_items, not dispatches — one clustered dispatch contributes several, so this total may exceed denominator"
    if denominator == 0:
        return {
            "value": None,
            "unavailable": "no loop-originated investigate dispatches with a recorded verdict yet",
            "numerator": 0, "denominator": 0,
            "item_states": {}, "item_states_note": item_states_note,
            "excluded_interactive": excluded_interactive, "excluded_interactive_note": excluded_interactive_note,
            "windowed": False,
        }
    placeholders = ",".join("?" for _ in DISPOSITION_STATES)
    numerator = conn.execute(
        f"SELECT COUNT(DISTINCT d.id) AS n FROM dispatches d "
        f"JOIN triage_items ti ON ti.dispatch_job = d.job_id "
        f"WHERE d.tier='investigate' AND d.verdict_json IS NOT NULL AND d.origin_event_id IS NOT NULL "
        f"AND ti.state IN ({placeholders})",
        DISPOSITION_STATES,
    ).fetchone()["n"]
    item_states = {
        row["state"]: row["n"]
        for row in conn.execute(
            "SELECT ti.state AS state, COUNT(*) AS n FROM dispatches d "
            "JOIN triage_items ti ON ti.dispatch_job = d.job_id "
            "WHERE d.tier='investigate' AND d.verdict_json IS NOT NULL AND d.origin_event_id IS NOT NULL "
            "GROUP BY ti.state"
        )
    }
    return {
        "value": numerator / denominator,
        "unavailable": None,
        "numerator": numerator, "denominator": denominator,
        "item_states": item_states, "item_states_note": item_states_note,
        "excluded_interactive": excluded_interactive, "excluded_interactive_note": excluded_interactive_note,
        "windowed": False,
    }


def _metric_verified_fixes_vs_silence(conn: sqlite3.Connection) -> dict[str, Any]:
    """# 2 — verified fixes vs silence, restricted to mapped signatures
    (triage_items.repo IS NOT NULL — DESIGN.md's own qualifier). All-time."""
    counts = {
        row["state"]: row["n"]
        for row in conn.execute(
            "SELECT state, COUNT(*) AS n FROM triage_items WHERE repo IS NOT NULL GROUP BY state"
        )
    }
    fixed = counts.get(_ledger.STATE_FIXED, 0)
    quiet = counts.get(_ledger.STATE_QUIET, 0)
    closed = counts.get(_ledger.STATE_CLOSED, 0)
    denominator = fixed + quiet
    return {
        "value": (fixed / denominator) if denominator else None,
        "unavailable": None if denominator else "no fixed/quiet closes yet for a mapped signature",
        "fixed": fixed, "quiet": quiet, "closed": closed,
        "windowed": False,
    }


def _history_guard_reason(history_since: str | None, window_start: dt.datetime) -> str | None:
    """Leaf-level guard shared by every metric computed from `item_transitions`.

    The table was created EMPTY by migration 4 and records nothing
    retroactively (ledger.py's `_MIGRATION_4` docstring), so a window whose
    start is before the earliest recorded row — or a still-empty table — is
    empty BY CONSTRUCTION, not a measured zero. The top-level `history_since`
    field alone doesn't prevent a leaf from fabricating a `0` here: a consumer
    has to remember to cross-reference it, which is advisory, not enforced.
    This makes the check load-bearing at the leaf itself. Returns the reason
    string when the guard applies (caller must report `null`, never `0`), or
    None when the window is safely inside recorded history."""
    if history_since is None:
        return "item_transitions has no rows yet (table created empty by schema migration 4) — not a measured zero"
    if window_start < dt.datetime.fromisoformat(history_since):
        return (
            f"window start predates history_since ({history_since}) — item_transitions records "
            "nothing before that point, so this window is empty by construction, not a measured zero"
        )
    return None


def _metric_median_needs_decision_to_decision(
    conn: sqlite3.Connection, now: dt.datetime, history_since: str | None,
) -> dict[str, Any]:
    """# 3 — median needs_decision -> owner's decision, in hours. Windowed on entry.
    (The payload key is `median_needs_decision_to_decision_hours`.)

    Pairs each transition INTO needs_decision with the very next transition for
    that same event_id (item_transitions is per-event chronological, and
    _set_state() only ever appends on a REAL state change — see triage.py's
    own docstring — so "the next row" IS "the exit from needs_decision"). Nothing
    expires a needs_decision item, so every exit is somebody deciding."""
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    guard_reason = _history_guard_reason(history_since, window_start)
    if guard_reason:
        return {
            "value": None,
            "unavailable": guard_reason,
            "pairs": 0,
            "windowed": True, "window_days": WINDOW_DAYS,
        }

    rows = conn.execute(
        "SELECT event_id, from_state, to_state, at FROM item_transitions ORDER BY event_id, id"
    ).fetchall()
    by_event: dict[int, list[sqlite3.Row]] = {}
    for row in rows:
        by_event.setdefault(row["event_id"], []).append(row)

    deltas_hours: list[float] = []
    for transitions in by_event.values():
        for i, t in enumerate(transitions):
            if t["to_state"] != _ledger.STATE_NEEDS_DECISION:
                continue
            if i + 1 >= len(transitions):
                continue  # still open — no exit recorded yet
            exit_t = transitions[i + 1]
            entry_at = dt.datetime.fromisoformat(t["at"])
            if entry_at < window_start:
                continue
            exit_at = dt.datetime.fromisoformat(exit_t["at"])
            deltas_hours.append((exit_at - entry_at).total_seconds() / 3600)

    if not deltas_hours:
        return {
            "value": None,
            "unavailable": "no needs_decision -> decision pairs in window",
            "pairs": 0,
            "windowed": True, "window_days": WINDOW_DAYS,
        }
    return {
        "value": statistics.median(deltas_hours),
        "unavailable": None,
        "pairs": len(deltas_hours),
        "windowed": True, "window_days": WINDOW_DAYS,
    }


_FIXED_STATE = _ledger.STATE_FIXED


def _metric_verified_unattended_fixes_per_week(
    conn: sqlite3.Connection, now: dt.datetime, history_since: str | None,
) -> dict[str, Any]:
    """# 4 — verified UNATTENDED fixes per week. With the signed-approval gate
    gone every fix is unattended, so this is the count of distinct items that
    entered `fixed` in the window.

    Returns `null` when the window predates `history_since` or holds no
    `fixed` transition — "no basis", never a fabricated 0.
    """
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    window_start_iso = window_start.isoformat()
    guard_reason = _history_guard_reason(history_since, window_start)
    if guard_reason:
        return {
            "value": None,
            "unavailable": guard_reason,
            "fixed_in_window": 0,
            "unattended_in_window": 0,
            "windowed": True, "window_days": WINDOW_DAYS,
        }
    # DISTINCT, and the unit is the ITEM, not the transition: an item can
    # enter `fixed` more than once in one window (fixed -> reopened by a
    # failed liveness probe -> fixed again) and counts once.
    fixed_event_ids = [
        row["event_id"] for row in conn.execute(
            "SELECT DISTINCT event_id FROM item_transitions WHERE to_state=? AND at >= ?",
            (_FIXED_STATE, window_start_iso),
        )
    ]
    if not fixed_event_ids:
        return {
            "value": None,
            "unavailable": "no `fixed` transitions in window",
            "fixed_in_window": 0,
            "unattended_in_window": 0,
            "windowed": True, "window_days": WINDOW_DAYS,
        }
    unattended = len(fixed_event_ids)
    return {
        "value": unattended,
        "unavailable": None,
        "fixed_in_window": len(fixed_event_ids),
        "unattended_in_window": unattended,
        "windowed": True, "window_days": WINDOW_DAYS,
    }


def _metric_poller_ages(conn: sqlite3.Connection, now: dt.datetime) -> dict[str, Any]:
    """# 5 — minutes with no poller running, per poller plus the worst case."""
    pollers: dict[str, Any] = {}
    ages: list[float] = []
    for name, (key, interval_s) in POLLERS.items():
        age_minutes, last_run = _poller_age_minutes(conn, now, key)
        threshold_minutes = interval_s * STALE_MULTIPLE / 60
        if age_minutes is None:
            pollers[name] = {
                "age_minutes": None, "unavailable": "no heartbeat recorded yet",
                "last_run": None, "threshold_minutes": threshold_minutes, "stale": True,
            }
            continue
        ages.append(age_minutes)
        pollers[name] = {
            "age_minutes": age_minutes, "unavailable": None,
            "last_run": last_run, "threshold_minutes": threshold_minutes,
            "stale": age_minutes > threshold_minutes,
        }
    return {
        "value": max(ages) if ages else None,
        "unavailable": None if ages else "no poller has ever recorded a heartbeat",
        "pollers": pollers,
        "windowed": False,
    }


def _metric_reverts_and_reopens(
    conn: sqlite3.Connection, now: dt.datetime, history_since: str | None,
) -> dict[str, Any]:
    """# 6 — reverts and reopen-after-fixed.

    `reverts` is `warden revert` (Wave 5.3): `triage_items.revert_pr` records the
    revert PR and the item lands `failed` with a "reverted: PR #n" note. The count
    is windowed on `updated_at` like every other metric here, and reads `0`
    freely, because `0` is a real measurement, not a stand-in for "not tracked".
    `reopen_after_fixed` is unchanged: `item_transitions`-
    derived, behind the same leaf-level history guard as metrics 3 and 4
    (`_history_guard_reason`) for the same reason — an empty or young table
    must never present a fabricated `0`."""
    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    guard_reason = _history_guard_reason(history_since, window_start)
    if guard_reason:
        reopen_value: int | None = None
        reopen_reason = guard_reason
    else:
        reopen_value = conn.execute(
            "SELECT COUNT(*) AS n FROM item_transitions WHERE from_state=? AND at >= ?",
            (_FIXED_STATE, window_start.isoformat()),
        ).fetchone()["n"]
        reopen_reason = None
    reverts_value = conn.execute(
        "SELECT COUNT(*) AS n FROM triage_items WHERE revert_pr IS NOT NULL AND updated_at >= ?",
        (window_start.isoformat(),),
    ).fetchone()["n"]
    return {
        "reopen_after_fixed": {
            "value": reopen_value, "unavailable": reopen_reason,
            "windowed": True, "window_days": WINDOW_DAYS,
        },
        "reverts": {
            "value": reverts_value, "unavailable": None,
            "windowed": True, "window_days": WINDOW_DAYS,
        },
    }


def _item_transitions_history_since(conn: sqlite3.Connection) -> str | None:
    """MIN(at) across `item_transitions`, or None when the table is empty —
    the one query `metrics_payload()`'s top-level `history_since` field and
    every leaf-level history guard (`_history_guard_reason`) share, so there
    is exactly one definition of "how far back does our history go"."""
    return conn.execute("SELECT MIN(at) AS min_at FROM item_transitions").fetchone()["min_at"]


def metrics_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    """The whole `/metrics` body. A thin function so the HTTP handler below is
    the deep-module shape this repo's code-style rules ask for: the handler
    is a shell, this is what a test actually drives."""
    now = dt.datetime.now(dt.timezone.utc)
    history_since = _item_transitions_history_since(conn)
    return {
        "generated_at": now.isoformat(),
        "window_days": WINDOW_DAYS,
        # item_transitions was created empty by migration 4 and records nothing
        # retroactively (ledger.py's _MIGRATION_4 docstring). Any window whose
        # start predates this timestamp is empty BY CONSTRUCTION — a consumer
        # must be able to tell that apart from a measured zero, which is the
        # whole reason this field exists. Metrics 3/4/6 additionally enforce
        # this at the LEAF (`_history_guard_reason`) rather than relying on a
        # consumer to cross-reference this field themselves.
        "history_since": history_since,
        "verdicts_recorded_disposition": _metric_verdicts_recorded_disposition(conn),
        "verified_fixes_vs_silence": _metric_verified_fixes_vs_silence(conn),
        "median_needs_decision_to_decision_hours": _metric_median_needs_decision_to_decision(conn, now, history_since),
        "verified_unattended_fixes_per_week": _metric_verified_unattended_fixes_per_week(conn, now, history_since),
        "poller_ages": _metric_poller_ages(conn, now),
        "reverts_and_reopens": _metric_reverts_and_reopens(conn, now, history_since),
    }


def health_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    """The whole `/health` body — schema version, ledger file identity, every
    poller's heartbeat age against its own named threshold, and one `ok`
    boolean folding all of it together."""
    now = dt.datetime.now(dt.timezone.utc)
    version_row = conn.execute("SELECT version FROM schema_version").fetchone()
    version = version_row["version"] if version_row is not None else None
    schema_ok = version == _ledger.LEDGER_SCHEMA_VERSION

    db_file = conn.execute("PRAGMA database_list").fetchone()["file"]
    try:
        db_mtime = dt.datetime.fromtimestamp(Path(db_file).stat().st_mtime, tz=dt.timezone.utc).isoformat()
    except OSError:
        db_mtime = None

    pollers: dict[str, Any] = {}
    all_pollers_ok = True
    for name, (key, interval_s) in POLLERS.items():
        age_minutes, last_run = _poller_age_minutes(conn, now, key)
        threshold_minutes = interval_s * STALE_MULTIPLE / 60
        ok = age_minutes is not None and age_minutes <= threshold_minutes
        all_pollers_ok = all_pollers_ok and ok
        pollers[name] = {
            "age_minutes": age_minutes, "last_run": last_run,
            "threshold_minutes": threshold_minutes, "ok": ok,
        }

    return {
        "ok": schema_ok and all_pollers_ok,
        "schema_version": version,
        "schema_version_expected": _ledger.LEDGER_SCHEMA_VERSION,
        "db_path": db_file,
        "db_mtime": db_mtime,
        "pollers": pollers,
        "checked_at": now.isoformat(),
    }


# --- /board and /items/<event_id> -----------------------------------------------

class ItemNotFoundError(Exception):
    """Raised by `item_payload()` when no `triage_items` row exists for the
    given event_id. Caught by `Handler._serve()` and turned into a 404 —
    never a 503, which stays reserved for a rejected connection or a schema
    mismatch."""


def board_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    """The whole `/board` body: a funnel-snapshot counts map plus the
    non-terminal `triage_items` rows themselves, newest first. Same honesty
    posture as `/metrics` — `schema_version` is the constant, not a fresh
    read, because `_serve()` already ran `assert_schema_version()` on this
    exact connection before calling here; a mismatch would have 503'd before
    this function ever ran."""
    now = dt.datetime.now(dt.timezone.utc)
    terminal_placeholders = ",".join("?" for _ in _ledger.TERMINAL_STATES)

    counts = {state: 0 for state in CHAIN_STATES}
    for row in conn.execute(
        f"SELECT state, COUNT(*) AS n FROM triage_items "
        f"WHERE state NOT IN ({terminal_placeholders}) GROUP BY state",
        _ledger.TERMINAL_STATES,
    ):
        counts[row["state"]] = row["n"]

    rows = conn.execute(
        f"SELECT ti.*, e.title AS event_title, e.url AS event_url, "
        f"(SELECT d.validation_status FROM dispatches d WHERE d.job_id = ti.implement_job) AS validation_status, "
        f"e.payload_json AS event_payload_json FROM triage_items ti "
        f"JOIN events e ON e.id = ti.event_id "
        f"WHERE ti.state NOT IN ({terminal_placeholders}) "
        f"ORDER BY ti.updated_at DESC LIMIT ?",
        (*_ledger.TERMINAL_STATES, BOARD_ITEMS_CAP + 1),
    ).fetchall()
    truncated = len(rows) > BOARD_ITEMS_CAP
    items = [_board_item(row) for row in rows[:BOARD_ITEMS_CAP]]

    terminal_24h_start = (now - dt.timedelta(hours=24)).isoformat()
    terminal_24h = conn.execute(
        f"SELECT COUNT(*) AS n FROM triage_items "
        f"WHERE state IN ({terminal_placeholders}) AND updated_at >= ?",
        (*_ledger.TERMINAL_STATES, terminal_24h_start),
    ).fetchone()["n"]

    payload: dict[str, Any] = {
        "generated_at": now.isoformat(),
        "schema_version": _ledger.LEDGER_SCHEMA_VERSION,
        "counts": counts,
        "items": items,
        "terminal_24h": terminal_24h,
        "awaiting_owner": awaiting_owner(conn, now),
    }
    if truncated:
        payload["truncated"] = True
    return payload


AWAITING_OWNER_STATES = (_ledger.STATE_NEEDS_DECISION, _ledger.STATE_FAILED)


def _age_days(since: str | None, now: dt.datetime) -> float | None:
    if not since:
        return None
    try:
        then = dt.datetime.fromisoformat(since.replace("Z", "+00:00"))
    except ValueError:
        return None
    if then.tzinfo is None:
        then = then.replace(tzinfo=dt.timezone.utc)
    return round((now - then).total_seconds() / 86400, 1)


def awaiting_owner(conn: sqlite3.Connection, now: dt.datetime) -> list[dict[str, Any]]:
    """Everything that is waiting on Johannes and nothing else, oldest first:
    every `needs_decision` item (and every `failed` one, which only an owner
    action moves) with how long it has sat there and why."""
    placeholders = ",".join("?" for _ in AWAITING_OWNER_STATES)
    out: list[dict[str, Any]] = []
    for row in conn.execute(
        f"SELECT ti.event_id, ti.repo, ti.state, ti.note, ti.pr_url, ti.revision_count, ti.revert_pr, "
        f"e.title, (SELECT d.validation_status FROM dispatches d WHERE d.job_id = ti.implement_job) "
        f"AS validation_status, (SELECT MAX(at) FROM item_transitions t WHERE t.event_id = ti.event_id "
        f"AND t.to_state = ti.state) AS entered_at FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        f"WHERE ti.state IN ({placeholders})",
        AWAITING_OWNER_STATES,
    ):
        out.append({
            "kind": "item", "event_id": row["event_id"], "repo": row["repo"], "title": row["title"],
            "state": row["state"], "pr_url": row["pr_url"], "age_days": _age_days(row["entered_at"], now),
            "reason": row["note"], "revision_count": row["revision_count"],
            "availableActions": _available_actions(
                row["state"], bool(row["pr_url"]) and row["validation_status"] == "confirmed",
                reverted=row["revert_pr"] is not None),
        })
    out.sort(key=lambda x: (x["age_days"] is None, -(x["age_days"] or 0)))
    return out


def _board_item_issue(row: sqlite3.Row) -> dict[str, Any] | None:
    """The `issue` sub-object for a `github_issue`-origin row, sourced from
    the parent event's `payload_json` (written by triage.py's
    ingest_github_issues()) and `url`. `None` for any other origin, and
    `None` (never a raise) if the payload is missing or not valid JSON."""
    if row["origin"] != "github_issue":
        return None
    raw = row["event_payload_json"]
    if not raw:
        return None
    try:
        payload = json.loads(raw)
    except (TypeError, ValueError):
        return None
    if not isinstance(payload, dict):
        return None
    return {
        "repo": payload.get("repo"),
        "number": payload.get("number"),
        "url": row["event_url"],
        "author": payload.get("author"),
        "trusted": payload.get("author") == _github.GH_OWNER,
        "labels": payload.get("labels"),
    }


def _board_item(row: sqlite3.Row) -> dict[str, Any]:
    """One `/board` item, in the exact shape the brief names — never a raw
    `dict(row)`, which would leak `card_hash`/`deploy_expect_json`/etc and
    silently reshape itself on every future schema migration."""
    return {
        "event_id": row["event_id"],
        "origin": row["origin"],
        "repo": row["repo"],
        "state": row["state"],
        "close_reason": row["close_reason"],
        "strikes": row["strikes"],
        "retry_at": row["retry_at"],
        "max_tier": row["max_tier"],
        "title": row["event_title"],
        "note": row["note"],
        "pr_url": row["pr_url"],
        "dispatch_job": row["dispatch_job"],
        "implement_job": row["implement_job"],
        "validation_job": row["validation_job"],
        "occurrences": row["occurrences"],
        "revision_count": row["revision_count"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
        "origin_channel": row["origin_channel"],
        "origin_thread_ts": row["origin_thread_ts"],
        "availableActions": _available_actions(
            row["state"], bool(row["pr_url"]) and row["validation_status"] == "confirmed",
            reverted=row["revert_pr"] is not None),
        "issue": _board_item_issue(row),
    }


def item_payload(
    conn: sqlite3.Connection, event_id: int, *, history_limit: int | None = None
) -> dict[str, Any]:
    """The whole `/items/<event_id>` body — the item itself, its parent
    event, and every dispatch/operation/transition that names it.
    Raises `ItemNotFoundError` for the 404 case; `Handler._serve()` owns
    turning that into an HTTP response, same shell/deep-module split as
    `metrics_payload()`/`health_payload()`/`board_payload()`.

    `history_limit`, when set, keeps only the NEWEST `history_limit` rows of
    `transitions` and `operations` (still returned oldest-first — a consumer
    diffing this against the unbounded `GET /items/<id>` shape should see the
    same order, just a shorter prefix cut off the front). `transitions_total`
    and `operations_total` are always present at the top level, limited or
    not, so a caller can tell a full history from a truncated one. This
    exists because `build_argo_snapshot()` embeds one of these per board item
    on every tick — an item with a flapping state accumulates transitions
    without bound, and unbounded here means the whole snapshot grows with it
    (item 543: ~780B/tick, 232KB before this cap). `GET /items/<id>` itself
    passes no limit and stays unbounded.

    The synthetic `created` transition (see `_synthetic_created_transition()`)
    is only prepended when the history is NOT truncated — a truncated
    `transitions` list is missing its own oldest rows already, so the
    fetched head is not reliable evidence that no genuine `from_state IS
    NULL` row exists further back."""
    item_row = conn.execute("SELECT * FROM triage_items WHERE event_id = ?", (event_id,)).fetchone()
    if item_row is None:
        raise ItemNotFoundError(f"no item {event_id}")
    item = dict(item_row)
    brief = item.get("brief")
    item["brief_truncated"] = bool(brief) and len(brief) > 2000
    if item["brief_truncated"]:
        item["brief"] = brief[:2000]

    event_row = conn.execute(
        "SELECT id, source, external_id, title, url, payload_json, first_seen, resolved_at, "
        "reminder_count, last_reminder_at "
        "FROM events WHERE id = ?",
        (event_id,),
    ).fetchone()
    event = dict(event_row) if event_row is not None else None
    if event is not None:
        payload_json = event.pop("payload_json")
        event["payload"] = json.loads(payload_json) if payload_json else None

    dispatch_rows = conn.execute(
        "SELECT job_id, tier, repo, status, created_at, finished_at, reported_at, merged_at, artifact_url, "
        "validation_job_id, validation_status, delivery_status, verdict_json "
        "FROM dispatches WHERE origin_event_id = ? OR job_id IN (?, ?, ?) ORDER BY created_at",
        (event_id, item.get("dispatch_job"), item.get("implement_job"), item.get("validation_job")),
    ).fetchall()
    dispatches = [_dispatch_detail(row) for row in dispatch_rows]

    operations, operations_total = _bounded_history(
        conn, "operations", "started_at", event_id, history_limit
    )
    operations = [dict(row) for row in operations]

    transition_rows, transitions_total = _bounded_history(
        conn, "item_transitions", "id", event_id, history_limit
    )
    transitions = [dict(row) for row in transition_rows]
    truncated = history_limit is not None and transitions_total > len(transitions)
    if not truncated and not any(t["from_state"] is None for t in transitions):
        transitions.insert(0, _synthetic_created_transition(item, transitions))

    return {
        "item": item,
        "event": event,
        "dispatches": dispatches,
        "operations": operations,
        "operations_total": operations_total,
        "transitions": transitions,
        "transitions_total": transitions_total,
    }


def _bounded_history(
    conn: sqlite3.Connection, table: str, order_col: str, event_id: int, limit: int | None
) -> tuple[list[sqlite3.Row], int]:
    """Shared by `operations` and `transitions`: total count (unaffected by
    `limit`) plus the newest `limit` rows, still in oldest-first order — a
    plain `ORDER BY ... DESC LIMIT ?` fetches the tail cheaply, and the
    `reversed()` here restores the order every other caller of this table
    already expects. `table`/`order_col` reach raw SQL, so this is only ever
    called with the two literals above, never with a request-controlled
    value."""
    total = conn.execute(f"SELECT COUNT(*) FROM {table} WHERE event_id = ?", (event_id,)).fetchone()[0]
    if limit is None:
        rows = conn.execute(
            f"SELECT * FROM {table} WHERE event_id = ? ORDER BY {order_col}", (event_id,)
        ).fetchall()
        return rows, total
    rows = conn.execute(
        f"SELECT * FROM {table} WHERE event_id = ? ORDER BY {order_col} DESC LIMIT ?", (event_id, limit)
    ).fetchall()
    return list(reversed(rows)), total


def _synthetic_created_transition(item: dict[str, Any], transitions: list[dict[str, Any]]) -> dict[str, Any]:
    """A legacy item — created before `_record_created_transition()` existed
    — has no `from_state IS NULL` row at all. Rather than leave its earliest
    real transition looking like the item's whole history started mid-flight,
    fabricate the row it should have had: `to_state` is whatever state the
    first RECORDED transition moved it FROM (the state it must have been
    created into), or the item's current state when there is no recorded
    transition at all — the oldest fact still available."""
    to_state = transitions[0]["from_state"] if transitions else item["state"]
    return {
        "id": None,
        "event_id": item["event_id"],
        "from_state": None,
        "to_state": to_state,
        "at": item["created_at"],
        "note": "created",
        "synthetic": True,
    }


def _dispatch_detail(row: sqlite3.Row) -> dict[str, Any]:
    d = dict(row)
    verdict_json = d.pop("verdict_json")
    d["verdict"] = _parse_verdict(verdict_json)
    return d


def _parse_verdict(verdict_json: str | None) -> dict[str, Any] | None:
    if verdict_json is None:
        return None
    verdict = json.loads(verdict_json)
    return {
        "summary": verdict.get("summary"),
        "nextAction": verdict.get("nextAction"),
        "confidence": verdict.get("confidence"),
        "recommendation": verdict.get("recommendation"),
        "outcome": verdict.get("outcome"),
        "schemaVersion": verdict.get("schemaVersion"),
    }


# --- HTTP handler ---------------------------------------------------------------

_ROUTES: dict[str, Callable[[sqlite3.Connection], dict[str, Any]]] = {
    "/metrics": metrics_payload,
    "/health": health_payload,
    "/board": board_payload,
}

# `/items/<event_id>` is a prefix match, not an exact one — kept OUT of
# _ROUTES (which do_GET still checks first for everything else) rather than
# reshaping that dict into something route-pattern-aware for one path.
_ITEMS_PREFIX = "/items/"


class Handler(http.server.BaseHTTPRequestHandler):
    server_version = "warden-api/1"

    def _write_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _serve(self, fn: Callable[[sqlite3.Connection], dict[str, Any]]) -> None:
        """Open one fresh read-only connection for this request, assert its
        schema version explicitly (readonly connect() does not — see this
        module's docstring), run `fn`, always close. A schema mismatch or an
        unreachable file is 503 with the reason in the body — never a 200
        built on a connection this process could not actually trust."""
        try:
            conn = _ledger.connect(DB_PATH, readonly=True)
        except sqlite3.OperationalError as e:
            self._write_json(503, {"error": f"ledger unreachable at {DB_PATH}: {e}"})
            return
        try:
            _ledger.assert_schema_version(conn)
            self._write_json(200, fn(conn))
        except ItemNotFoundError as e:
            self._write_json(404, {"error": str(e)})
        except RuntimeError as e:
            self._write_json(503, {"error": str(e)})
        finally:
            conn.close()

    def do_GET(self) -> None:
        fn = _ROUTES.get(self.path)
        if fn is not None:
            self._serve(fn)
            return
        if self.path.startswith(_ITEMS_PREFIX):
            raw_id = self.path[len(_ITEMS_PREFIX):]
            if not raw_id.isdigit():
                self._write_json(400, {"error": "event_id must be an integer"})
                return
            event_id = int(raw_id)
            self._serve(lambda conn: item_payload(conn, event_id))
            return
        self._write_json(404, {"error": f"no such path: {self.path}"})

    def _method_not_allowed(self) -> None:
        self._write_json(405, {"error": f"{self.command} not allowed — GET only"})

    do_POST = _method_not_allowed
    do_PUT = _method_not_allowed
    do_DELETE = _method_not_allowed
    do_PATCH = _method_not_allowed
    do_HEAD = _method_not_allowed
    do_OPTIONS = _method_not_allowed

    def log_message(self, fmt: str, *args: Any) -> None:  # noqa: A003 — stdlib's own name
        # Default BaseHTTPRequestHandler.log_message writes to stderr, which is
        # exactly where the LaunchAgent's StandardErrorPath sends it — kept
        # as-is (not silenced) so a request log exists at all for this process.
        super().log_message(fmt, *args)


def main(argv: list[str] | None = None) -> int:
    argv = list(argv if argv is not None else sys.argv[1:])
    if "--serve" not in argv:
        print(__doc__ or "", file=sys.stderr)
        return 2
    server = http.server.ThreadingHTTPServer((BIND_HOST, BIND_PORT), Handler)
    print(f"warden-api: listening on {BIND_HOST}:{BIND_PORT} (loopback only)", file=sys.stderr)
    try:
        server.serve_forever()
    finally:
        server.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
