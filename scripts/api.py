"""warden HTTP API — GET /metrics and GET /health, read-only.

WHAT THIS IS NOT YET. `/board`, `/items/:id`, `POST /items/:id/intent` and
`POST /items/:id/note` are DESIGN.md's real contract (§ HTTP API) and are Wave 4,
built alongside Argo — the only consumer of the write-adjacent endpoints. Building
them now, unconsumed, would be dead surface with no caller to prove it correct.
This wave is deliberately narrower: the six funnel numbers and a liveness check,
because those are useful the moment they exist, with nothing else to wire up.

BIND AND AUTH. `127.0.0.1:7734` (7734 is the next free port in dotfiles'
Caddyfile registry), loopback only, no bearer token. Per DESIGN.md § Security
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

BIND_HOST = "127.0.0.1"
BIND_PORT = 7734

# --- Funnel window + disposition vocabulary -----------------------------------

WINDOW_DAYS = 7

# The implement chain, plus needs_human and dismissed — every state DESIGN.md's
# "What done means" table counts as a RECORDED disposition for a verdict.
# `quiet` is deliberately absent: DESIGN.md's own principle 5, "silence is
# never an outcome" — a signal going quiet cancels the need to START work, it
# never discharges an obligation a verdict already created. Mirrors
# triage.py's STATE_* constants by literal value rather than by import: this
# module stays a read-only, dependency-free reader of the ledger, and copying
# eight string literals that change on the same rare cadence as the lifecycle
# diagram itself (DESIGN.md § Lifecycle) is a smaller risk than pulling in the
# whole 3400-line act-loop module just to read its constants.
DISPOSITION_STATES = (
    "implementing", "validating", "merge_blocked", "merged", "liveness_pending",
    "pr_open", "fixed", "closed", "needs_human", "dismissed",
)

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
    """# 1 — verdicts -> recorded disposition. All-time."""
    denominator = conn.execute(
        "SELECT COUNT(*) AS n FROM dispatches WHERE tier='investigate' AND verdict_json IS NOT NULL"
    ).fetchone()["n"]
    if denominator == 0:
        return {
            "value": None,
            "unavailable": "no investigate dispatches with a recorded verdict yet",
            "numerator": 0, "denominator": 0, "states": {},
            "windowed": False,
        }
    placeholders = ",".join("?" for _ in DISPOSITION_STATES)
    numerator = conn.execute(
        f"SELECT COUNT(DISTINCT d.id) AS n FROM dispatches d "
        f"JOIN triage_items ti ON ti.dispatch_job = d.job_id "
        f"WHERE d.tier='investigate' AND d.verdict_json IS NOT NULL AND ti.state IN ({placeholders})",
        DISPOSITION_STATES,
    ).fetchone()["n"]
    states = {
        row["state"]: row["n"]
        for row in conn.execute(
            "SELECT ti.state AS state, COUNT(*) AS n FROM dispatches d "
            "JOIN triage_items ti ON ti.dispatch_job = d.job_id "
            "WHERE d.tier='investigate' AND d.verdict_json IS NOT NULL GROUP BY ti.state"
        )
    }
    return {
        "value": numerator / denominator,
        "unavailable": None,
        "numerator": numerator, "denominator": denominator, "states": states,
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
    fixed = counts.get("fixed", 0)
    quiet = counts.get("quiet", 0)
    closed = counts.get("closed", 0)
    dismissed = counts.get("dismissed", 0)
    denominator = fixed + quiet
    return {
        "value": (fixed / denominator) if denominator else None,
        "unavailable": None if denominator else "no fixed/quiet closes yet for a mapped signature",
        "fixed": fixed, "quiet": quiet, "closed": closed, "dismissed": dismissed,
        "windowed": False,
    }


def _metric_median_needs_human_to_decision(conn: sqlite3.Connection, now: dt.datetime) -> dict[str, Any]:
    """# 3 — median needs_human -> human decision, in hours. Windowed on entry.

    Pairs each transition INTO needs_human with the very next transition for
    that same event_id (item_transitions is per-event chronological, and
    _set_state() only ever appends on a REAL state change — see triage.py's
    own docstring — so "the next row" IS "the exit from needs_human"). A pair
    whose exit is `dismissed` is excluded: that is the 7-day clock expiring
    (STATE_DEADLINES' needs_human rule), not a human deciding anything, and
    counting it would make the metric improve the longer a human ignores the
    card — the Goodhart failure REVIEW.md's C3 already named.
    """
    rows = conn.execute(
        "SELECT event_id, from_state, to_state, at FROM item_transitions ORDER BY event_id, id"
    ).fetchall()
    by_event: dict[int, list[sqlite3.Row]] = {}
    for row in rows:
        by_event.setdefault(row["event_id"], []).append(row)

    window_start = now - dt.timedelta(days=WINDOW_DAYS)
    deltas_hours: list[float] = []
    excluded_dismissed = 0
    for transitions in by_event.values():
        for i, t in enumerate(transitions):
            if t["to_state"] != "needs_human":
                continue
            if i + 1 >= len(transitions):
                continue  # still open — no exit recorded yet
            exit_t = transitions[i + 1]
            entry_at = dt.datetime.fromisoformat(t["at"])
            if entry_at < window_start:
                continue
            if exit_t["to_state"] == "dismissed":
                excluded_dismissed += 1
                continue
            exit_at = dt.datetime.fromisoformat(exit_t["at"])
            deltas_hours.append((exit_at - entry_at).total_seconds() / 3600)

    if not deltas_hours:
        return {
            "value": None,
            "unavailable": "no needs_human -> decision pairs in window (excluding expiry-to-dismissed)",
            "pairs": 0, "excluded_dismissed_pairs": excluded_dismissed,
            "windowed": True, "window_days": WINDOW_DAYS,
        }
    return {
        "value": statistics.median(deltas_hours),
        "unavailable": None,
        "pairs": len(deltas_hours), "excluded_dismissed_pairs": excluded_dismissed,
        "windowed": True, "window_days": WINDOW_DAYS,
    }


def _metric_verified_unattended_fixes_per_week(conn: sqlite3.Connection, now: dt.datetime) -> dict[str, Any]:
    """# 4 — verified UNATTENDED fixes per week. The qualifier itself is not
    derivable: `dispatch_approvals` carries no `event_id` and no item link of
    any kind (verified on the live ledger 2026-09-09: 5 rows, 1 with
    `argv_json`, ZERO containing `--auto-from-item`), so an approval can never
    be attributed to the item it eventually fixed. That link is the operation
    id DESIGN.md's Crash recovery section calls for — Wave 3, not this wave.
    Serves the raw ingredients instead of a fabricated number: the plain
    `fixed` transition count in the window (not attendance-filtered — labelled
    as such), and `dispatch_approvals.spent_at` in the window as the closest
    available context.
    """
    window_start_iso = (now - dt.timedelta(days=WINDOW_DAYS)).isoformat()
    fixes = conn.execute(
        "SELECT COUNT(*) AS n FROM item_transitions WHERE to_state='fixed' AND at >= ?",
        (window_start_iso,),
    ).fetchone()["n"]
    approvals = conn.execute(
        "SELECT COUNT(*) AS n FROM dispatch_approvals WHERE spent_at IS NOT NULL AND spent_at >= ?",
        (window_start_iso,),
    ).fetchone()["n"]
    return {
        "value": None,
        "unavailable": (
            "the 'unattended' qualifier cannot be derived without an operation id linking an approval "
            "to an item (DESIGN.md § Crash recovery, Wave 3) — dispatch_approvals has no event_id "
            "and 0/5 rows carry --auto-from-item"
        ),
        "fixes_in_window": fixes,
        "fixes_in_window_note": "raw `fixed` transition count — NOT filtered for attendance",
        "approvals_spent_in_window": approvals,
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


def _metric_reverts_and_reopens(conn: sqlite3.Connection, now: dt.datetime) -> dict[str, Any]:
    """# 6 — reverts and reopen-after-fixed. `reverts` has no primitive yet
    (DESIGN.md § Abort and revert: `warden revert` is Wave 3) so it is served
    `null` with a reason, never `0` — a `0` here would read as "measured, zero
    reverts happened" instead of "not tracked at all"."""
    window_start_iso = (now - dt.timedelta(days=WINDOW_DAYS)).isoformat()
    reopen = conn.execute(
        "SELECT COUNT(*) AS n FROM item_transitions WHERE from_state='fixed' AND at >= ?",
        (window_start_iso,),
    ).fetchone()["n"]
    return {
        "reopen_after_fixed": {
            "value": reopen, "unavailable": None,
            "windowed": True, "window_days": WINDOW_DAYS,
        },
        "reverts": {
            "value": None,
            "unavailable": "no revert primitive exists yet — `warden revert` is Wave 3 (DESIGN.md § Abort and revert)",
            "windowed": True, "window_days": WINDOW_DAYS,
        },
    }


def metrics_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    """The whole `/metrics` body. A thin function so the HTTP handler below is
    the deep-module shape this repo's code-style rules ask for: the handler
    is a shell, this is what a test actually drives."""
    now = dt.datetime.now(dt.timezone.utc)
    history_since = conn.execute("SELECT MIN(at) AS min_at FROM item_transitions").fetchone()["min_at"]
    return {
        "generated_at": now.isoformat(),
        "window_days": WINDOW_DAYS,
        # item_transitions was created empty by migration 4 and records nothing
        # retroactively (ledger.py's _MIGRATION_4 docstring). Any window whose
        # start predates this timestamp is empty BY CONSTRUCTION — a consumer
        # must be able to tell that apart from a measured zero, which is the
        # whole reason this field exists.
        "history_since": history_since,
        "verdicts_recorded_disposition": _metric_verdicts_recorded_disposition(conn),
        "verified_fixes_vs_silence": _metric_verified_fixes_vs_silence(conn),
        "median_needs_human_to_decision_hours": _metric_median_needs_human_to_decision(conn, now),
        "verified_unattended_fixes_per_week": _metric_verified_unattended_fixes_per_week(conn, now),
        "poller_ages": _metric_poller_ages(conn, now),
        "reverts_and_reopens": _metric_reverts_and_reopens(conn, now),
    }


def health_payload(conn: sqlite3.Connection) -> dict[str, Any]:
    """The whole `/health` body — schema version, ledger file identity, every
    poller's heartbeat age against its own named threshold, and one `ok`
    boolean folding all of it together."""
    now = dt.datetime.now(dt.timezone.utc)
    version_row = conn.execute("SELECT version FROM schema_version").fetchone()
    version = version_row["version"] if version_row is not None else None
    schema_ok = version == _ledger.SCHEMA_VERSION

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
        "schema_version_expected": _ledger.SCHEMA_VERSION,
        "db_path": db_file,
        "db_mtime": db_mtime,
        "pollers": pollers,
        "checked_at": now.isoformat(),
    }


# --- HTTP handler ---------------------------------------------------------------

_ROUTES: dict[str, Callable[[sqlite3.Connection], dict[str, Any]]] = {
    "/metrics": metrics_payload,
    "/health": health_payload,
}


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
        except RuntimeError as e:
            self._write_json(503, {"error": str(e)})
        finally:
            conn.close()

    def do_GET(self) -> None:
        fn = _ROUTES.get(self.path)
        if fn is None:
            self._write_json(404, {"error": f"no such path: {self.path}"})
            return
        self._serve(fn)

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
