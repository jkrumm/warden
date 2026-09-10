"""items — the one place outside triage.py that moves a `triage_items` row
between states, for the two new CLI-only transitions (`warden abort`,
`warden revert`) that triage.py's own state machine does not drive.

Deliberately NOT a general-purpose port of triage.py's `_set_state()`
(triage.py:1378-1486, off limits to this change — see that repo's own
docstring): this module owns exactly two target states, both terminal
(`closed`, `reverted`), so it skips `_set_state()`'s deadline-rule lookup and
`dismissed`-needs-a-reason check, which exist only for the many non-terminal
transitions this module never makes. What it keeps, because it is the
load-bearing part: `triage_items.state` has exactly ONE additional writer
outside triage.py, this function, and it appends exactly one
`item_transitions` row on a REAL state change (rowcount>0 from the UPDATE AND
the prior state differs from the new one) — the same guard, for the same
reason, as `_set_state()`'s own docstring.
"""

from __future__ import annotations

import datetime as dt
import sqlite3
from typing import Any

# Closed allowlist of extra columns a caller may set alongside `state` and
# `note` — column names reach SQL here, so the list stays closed on purpose,
# same principle as triage.py's own `_SET_STATE_COLUMNS`. `revert_pr` is the
# only one `warden revert` needs; add to this tuple only for a new verb that
# writes a specific column, never as a general escape hatch.
_EXTRA_COLUMNS = ("revert_pr",)


def _now_iso(now: dt.datetime) -> str:
    return now.astimezone(dt.timezone.utc).isoformat() if now.tzinfo else now.isoformat()


def transition(
    conn: sqlite3.Connection,
    event_id: int,
    *,
    to_state: str,
    now: dt.datetime,
    note: str | None = None,
    extra: dict[str, Any] | None = None,
) -> int:
    """Write `triage_items.state` (+ `state_deadline=NULL` — both target
    states this module ever writes are terminal, so no deadline applies —
    `note`, and any `extra` closed-allowlist column), and append one
    `item_transitions` row when, and only when, the state actually changed.
    Returns the UPDATE's rowcount (0 means no such event_id)."""
    extra = extra or {}
    unknown = tuple(c for c in extra if c not in _EXTRA_COLUMNS)
    if unknown:
        raise ValueError(f"{unknown} not in _EXTRA_COLUMNS={_EXTRA_COLUMNS} — column names reach SQL here")

    prev_row = conn.execute("SELECT state FROM triage_items WHERE event_id=?", (event_id,)).fetchone()
    prev_state = prev_row["state"] if prev_row is not None else None

    sql = "UPDATE triage_items SET state=?, state_deadline=NULL, note=?, updated_at=?"
    params: list[Any] = [to_state, note, _now_iso(now)]
    for col, value in extra.items():
        sql += f", {col}=?"
        params.append(value)
    sql += " WHERE event_id=?"
    params.append(event_id)

    rowcount = conn.execute(sql, params).rowcount

    if rowcount > 0 and prev_state != to_state:
        conn.execute(
            "INSERT INTO item_transitions(event_id, from_state, to_state, at, note) VALUES (?,?,?,?,?)",
            (event_id, prev_state, to_state, _now_iso(now), note),
        )
    return rowcount
