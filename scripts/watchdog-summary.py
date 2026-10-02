"""Watchdog summary — read-only snapshot of currently open watchdog items.

Emits a compact block consumed by briefing-context.py and the morning
briefing prompt's Infrastructure section. Does not mutate state.

Source of truth: ~/SourceRoot/warden/scripts/watchdog-summary.py
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import os
import sqlite3
import sys
from pathlib import Path

# The ledger. `~/.warden/warden.db` since the extraction — the same file
# scripts/ledger.py resolves, and the same two env vars, so a `--db` override, a
# test fixture and the module default cannot disagree about which database this
# is. It moved out of ~/.hermes because the control plane cannot keep living
# inside the thing it supervises; ~/.hermes/watchdog.db is left in place,
# untouched, as the rollback.
WARDEN_HOME = (Path(os.environ["WARDEN_HOME"]).expanduser()
               if os.environ.get("WARDEN_HOME") else Path.home() / ".warden")
DB_PATH = (Path(os.environ["WARDEN_DB"]).expanduser()
           if os.environ.get("WARDEN_DB") else WARDEN_HOME / "warden.db")

# scripts/ledger.py — loaded by path, the same mechanism triage.py/
# watchdog-poll.py/dispatch-sweep.py already use (the sibling filenames here
# are not importable). Used below only for its own connect(readonly=True),
# so this read-only script opens the database the one way every other
# reader/writer of it does.
_LEDGER_PATH = Path(__file__).resolve().parent / "ledger.py"
_ledger_spec = importlib.util.spec_from_file_location("ledger", _LEDGER_PATH)
assert _ledger_spec and _ledger_spec.loader, "Failed to load scripts/ledger.py"
_ledger = importlib.util.module_from_spec(_ledger_spec)
_ledger_spec.loader.exec_module(_ledger)

# scripts/ (this file's own directory) onto sys.path so `clients` is
# importable as a real package — the same reason triage.py/watchdog-poll.py/
# dispatch-sweep.py do this.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from clients import sideclaw as _sideclaw  # noqa: E402

# "Overnight" for the morning briefing: a dispatch that finished within this
# many hours of the poll is still worth mentioning; older ones have already
# been seen (delivered into their origin thread by dispatch-sweep.py) and
# would just be noise here.
DISPATCH_RECENT_HOURS = 18


def fmt_age(now: dt.datetime, iso: str) -> str:
    when = dt.datetime.fromisoformat(iso)
    secs = (now - when).total_seconds()
    if secs < 3600:
        return f"{int(secs / 60)}m"
    if secs < 86400:
        return f"{int(secs / 3600)}h"
    return f"{int(secs / 86400)}d"


def _dispatch_outcome_note(status: str, verdict_json: str | None) -> str:
    """Renders clients/sideclaw.py's classify_dispatch_outcome() into the
    briefing's own wording."""
    kind, detail = _sideclaw.classify_dispatch_outcome(status, verdict_json)
    if kind == "failed":
        return detail or status
    if kind == "no_verdict":
        return "done, no verdict"
    if kind == "unreadable":
        return "done, verdict unreadable"
    if kind == "degraded":
        return "degraded (tool failure, not a finding)"
    return detail or "done, no summary"


def emit_dispatches(conn: sqlite3.Connection, now: dt.datetime) -> None:
    """Dispatch-bridge projection (Phase 3, docs/dispatch-bridge.md § 'Morning
    briefing' row): what's still running, and what landed overnight. Emits
    nothing at all -- not even an empty-bracket block -- when there is
    nothing to say, so an ordinary morning with an idle dispatch bridge adds
    zero lines to the briefing prompt. Conservative: any failure to read the
    dispatches table (doesn't exist yet) is treated as "nothing to project,"
    matching this script's read-only, never-mutating contract.
    """
    try:
        open_rows = conn.execute(
            "SELECT repo, tier, job_id, status, created_at FROM dispatches "
            "WHERE status IN ('queued','running') ORDER BY created_at"
        ).fetchall()
        cutoff = (now - dt.timedelta(hours=DISPATCH_RECENT_HOURS)).isoformat()
        recent_rows = conn.execute(
            "SELECT repo, tier, job_id, status, verdict_json, artifact_url, finished_at "
            "FROM dispatches "
            "WHERE finished_at IS NOT NULL AND finished_at >= ? ORDER BY finished_at DESC",
            (cutoff,),
        ).fetchall()
    except sqlite3.OperationalError:
        return  # dispatches table doesn't exist yet -- nothing to project

    if not open_rows and not recent_rows:
        return

    if open_rows:
        print("DISPATCHES_OPEN=[")
        for r in open_rows:
            age = fmt_age(now, r["created_at"])
            print(f"  - {r['repo']} (tier {r['tier']}, {r['status']}) — running {age} — job {r['job_id'][:8]}")
        print("]")

    if recent_rows:
        print("DISPATCHES_RECENT=[")
        for r in recent_rows:
            age = fmt_age(now, r["finished_at"])
            note = _dispatch_outcome_note(r["status"], r["verdict_json"])
            # The artifact is the whole point of the author/implement tiers and the only
            # line Johannes can act on before coffee. Without it the briefing says a PR
            # was opened overnight and makes him go find it.
            artifact = (r["artifact_url"] or "").strip()
            suffix = f" — {artifact}" if artifact else ""
            print(f"  - {r['repo']} (tier {r['tier']}) — finished {age} ago — {note}{suffix}")
        print("]")


def main() -> int:
    if not DB_PATH.exists():
        print("WATCHDOG_AVAILABLE=false")
        return 0

    conn = _ledger.connect(DB_PATH, readonly=True)
    now = dt.datetime.now(dt.timezone.utc)

    open_rows = conn.execute(
        "SELECT source, external_id, title, url, first_seen, notified_at "
        "FROM events WHERE resolved_at IS NULL AND notified_at IS NOT NULL "
        "ORDER BY source, first_seen"
    ).fetchall()

    week_ago = (now - dt.timedelta(days=7)).isoformat()
    resolved_7d = conn.execute(
        "SELECT source, COUNT(*) AS n FROM events "
        "WHERE resolved_at IS NOT NULL AND resolved_at >= ? "
        "GROUP BY source ORDER BY source",
        (week_ago,),
    ).fetchall()

    print("WATCHDOG_AVAILABLE=true")
    if not open_rows:
        print("WATCHDOG_OPEN=[]")
    else:
        print("WATCHDOG_OPEN=[")
        for r in open_rows:
            age = fmt_age(now, r["first_seen"])
            url_part = f" {r['url']}" if r["url"] else ""
            print(f"  - [{r['source']}] {r['title']} (open {age}){url_part}")
        print("]")

    if not resolved_7d:
        print("WATCHDOG_RESOLVED_7D=[]")
    else:
        print("WATCHDOG_RESOLVED_7D=[")
        for r in resolved_7d:
            print(f"  - {r['source']}: {r['n']}")
        print("]")

    emit_dispatches(conn, now)
    conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
