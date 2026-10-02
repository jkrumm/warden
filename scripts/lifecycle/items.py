"""items — the one place outside triage.py that moves a `triage_items` row
between states, for the CLI-only transitions (`warden abort`, `warden revert`,
`warden close`) that triage.py's own state machine does not drive.

Deliberately NOT a general-purpose port of triage.py's `_set_state()`: this
module owns exactly two target states (`closed`, `failed`), so it skips
`_set_state()`'s strike bookkeeping, which exists only for the pipeline
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
# same principle as triage.py's own `_SET_STATE_COLUMNS`. `revert_pr` is what
# `warden revert` needs, `close_reason` what every `closed` target carries; add to
# this tuple only for a new verb that writes a specific column, never as a general
# escape hatch.
_EXTRA_COLUMNS = ("revert_pr", "close_reason")

# An item `note` is one short line: it is what Slack and Argo show. Both writers of
# the column (this module and triage.py's `_set_state()`) cap it here.
NOTE_MAX = 200


def cap_note(note: str | None) -> str | None:
    """Collapse all whitespace (newlines included) to single spaces and truncate to
    NOTE_MAX characters, ending in an ellipsis when anything was cut."""
    if note is None:
        return None
    text = " ".join(note.split())
    if len(text) <= NOTE_MAX:
        return text
    return text[: NOTE_MAX - 1] + "…"


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
    """Write `triage_items.state` (+ `note`, and any `extra` closed-allowlist
    column), and append one
    `item_transitions` row when, and only when, the state actually changed.
    Returns the UPDATE's rowcount (0 means no such event_id)."""
    note = cap_note(note)
    extra = extra or {}
    unknown = tuple(c for c in extra if c not in _EXTRA_COLUMNS)
    if unknown:
        raise ValueError(f"{unknown} not in _EXTRA_COLUMNS={_EXTRA_COLUMNS} — column names reach SQL here")

    # `closed` always carries its reason; every other state clears it — the same
    # invariant triage.py's `_set_state()` enforces.
    if to_state == "closed":
        if not extra.get("close_reason"):
            raise ValueError("a `closed` transition must carry extra={'close_reason': ...}")
    else:
        extra = {**extra, "close_reason": None}

    prev_row = conn.execute("SELECT state FROM triage_items WHERE event_id=?", (event_id,)).fetchone()
    prev_state = prev_row["state"] if prev_row is not None else None

    # card_hash records the state a Slack line was posted for; entering a different state
    # clears it so a later re-entry posts again (triage.py `notify_cluster()`).
    sql = "UPDATE triage_items SET state=?, note=?, updated_at=?, card_hash=CASE WHEN state=? THEN card_hash END"
    params: list[Any] = [to_state, note, _now_iso(now), to_state]
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
