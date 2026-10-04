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
import json
import sqlite3
from typing import Any

import ledger

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


def _payload(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def occurrence_mark(event: sqlite3.Row | dict[str, Any] | None) -> str | None:
    """An opaque fingerprint of which occurrences `event` has produced so far,
    for triage.py's reopen_if_needed() to compare with `!=` — never with `>` or `MAX()`.

    Five `|`-separated slots, each the raw string value (empty for None/missing),
    in fixed position:

        payload_json->$.ts_last | last_reminder_at | notified_at | first_seen | reminder_count

    Slot 1 (grouped sources only, via a guarded json.loads() — never SQL json_extract, so
    a malformed payload degrades to "absent" instead of raising) is a Slack
    `ts` float-string ("1788850795.862159"). Slots 2-4 are ISO-8601
    ("2026-09-08T07:00:20..."). These are TWO DIFFERENT CLOCKS IN TWO DIFFERENT
    FORMATS — a `MAX()` or `>` across them is a lexical compare of "1788…"
    against "2026…" that reads as correct and is not. Fixed slots plus whole-
    string equality never compares one clock against the other.

    All five slots are required, not redundant with each other:
    watchdog-poll.py's upsert_grouped() re-stamps last_reminder_at/notified_at
    ONLY when it emits (cooldown-gated) — a suppressed occurrence moves only
    payload_json.ts_last. A state source has no ts_last at all; its reopen
    signal is watchdog-poll.py:878 resetting resolved_at=NULL, first_seen=<now>,
    notified_at=NULL, last_reminder_at=NULL, reminder_count=0 on that same
    UPDATE — which moves the ISO slots. Every slot moves only on a genuine
    occurrence, or on that reopen reset (itself a genuine occurrence). Title
    and URL churn is deliberately NOT in the mark.

    Returns None if `event` is None — reopen_if_needed() reads this as "no
    event row", which cannot happen for a joined query but keeps the function
    total rather than partial."""
    if event is None:
        return None
    payload = _payload(event["payload_json"])
    ts_last = payload.get("ts_last")
    slots = (
        ts_last if isinstance(ts_last, str) else "",
        event["last_reminder_at"] or "",
        event["notified_at"] or "",
        event["first_seen"] or "",
        str(event["reminder_count"] if event["reminder_count"] is not None else ""),
    )
    return "|".join(slots)


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
        if extra.get("close_reason") not in ledger.CLOSE_REASONS:
            raise ValueError(f"a `closed` transition must carry extra={{'close_reason': one of {ledger.CLOSE_REASONS}}}")
    else:
        extra = {**extra, "close_reason": None}
    # `duplicate_of` belongs to `closed(duplicate)` alone (only the loop writes that).
    extra = {**extra, "duplicate_of": None}

    prev_row = conn.execute("SELECT state FROM triage_items WHERE event_id=?", (event_id,)).fetchone()
    prev_state = prev_row["state"] if prev_row is not None else None

    # card_hash records the state a Slack line was posted for; entering a different state
    # clears it so a later re-entry posts again (triage.py `notify_cluster()`).
    # Stamped on EVERY transition, exactly as triage.py's `_set_state()` does: a human
    # `warden close` must record the occurrence it closed against, or reopen_if_needed()
    # reads the stale mark as a fresh occurrence and undoes it.
    event = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
    sql = ("UPDATE triage_items SET state=?, note=?, updated_at=?, occurrence_mark=?, "
           "card_hash=CASE WHEN state=? THEN card_hash END")
    params: list[Any] = [to_state, note, _now_iso(now), occurrence_mark(event), to_state]
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
