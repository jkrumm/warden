"""The ingest door's logic: a pushed alert (Uptime Kuma, HyperDX) -> one normalized event, and the
event -> the `events` ledger. `scripts/api.py` is the HTTP shell (auth, body cap, status codes);
nothing here touches a socket.

Both senders feed STATE sources, level-based like `uk` was when watchdog-poll.py polled it
(`reconcile()`): one `events` row per (source, external_id), open while the alert fires,
`resolved_at` set when the sender says it is over, re-armed (a new occurrence mark) the next time it
fires. The open / reopen / refresh branches below are `reconcile()`'s, statement for statement
(tests/test_ingest.py runs both over the same sequence and compares the rows), so the webhook
keeps the row semantics the snapshot poll had. The `uk` poll is retired: Kuma's UP webhook is the only
thing that resolves a `uk` row. One (source, external_id) row means one item.

Pure of I/O apart from the connection the caller hands in: `normalize_*()` take a parsed JSON object
and raise `IngestError` on a malformed one (the shell's 400); `apply()` runs one `BEGIN IMMEDIATE`
transaction and commits it.
"""

from __future__ import annotations

import dataclasses
import datetime as dt
import json
import sqlite3
from typing import Any

SOURCE_KUMA = "uk"
SOURCE_HYPERDX = "hyperdx"

KIND_DOWN = "down"
KIND_UP = "up"
KIND_IGNORE = "ignore"

ACTION_OPENED = "opened"
ACTION_REFRESHED = "refreshed"
ACTION_RESOLVED = "resolved"
ACTION_IGNORED = "ignored"

# Sender-controlled text lands in `events` and from there in a triage brief: bounded here, on top of the
# shell's 64 KiB body cap.
MAX_TITLE_CHARS = 200
MAX_URL_CHARS = 500
MAX_MESSAGE_CHARS = 2000
MAX_LABEL_CHARS = 200

# Kuma's heartbeat statuses: 0 DOWN, 1 UP, 2 PENDING, 3 MAINTENANCE.
_KUMA_DOWN, _KUMA_UP, _KUMA_PENDING, _KUMA_MAINTENANCE = 0, 1, 2, 3

_HYPERDX_FIRING = frozenset({"alert", "firing"})
_HYPERDX_OK = frozenset({"ok", "resolved"})


class IngestError(ValueError):
    """The body is JSON but not a shape this endpoint accepts: the shell answers 400."""


@dataclasses.dataclass(frozen=True)
class IngestEvent:
    kind: str                       # KIND_DOWN | KIND_UP | KIND_IGNORE
    source: str = ""
    external_id: str = ""
    title: str = ""
    url: str = ""
    payload: dict[str, Any] = dataclasses.field(default_factory=dict)


IGNORED = IngestEvent(kind=KIND_IGNORE)


def _text(value: Any, limit: int) -> str:
    if value is None:
        return ""
    return str(value).strip()[:limit]


def normalize_kuma(body: dict[str, Any]) -> IngestEvent:
    """Kuma's webhook body `{heartbeat: {status, msg, time}, monitor: {id, name, url, type, tags}, msg}`
    in the shape the retired snapshot poll produced. A body with neither `heartbeat` nor `monitor` is the
    notification's own "Test" button: accepted and ignored, so the sender's setup screen reads success.
    A group monitor is dropped as the retired poll dropped it (its children alert on their own);
    PENDING and MAINTENANCE are not a state change either way."""
    heartbeat, monitor = body.get("heartbeat"), body.get("monitor")
    if heartbeat is None and monitor is None:
        return IGNORED
    if not isinstance(heartbeat, dict) or not isinstance(monitor, dict):
        raise IngestError("heartbeat and monitor must both be objects")
    if monitor.get("type") == "group":
        return IGNORED
    monitor_id = monitor.get("id")
    if isinstance(monitor_id, bool) or not isinstance(monitor_id, (int, str)) or str(monitor_id).strip() == "":
        raise IngestError("monitor.id is required")
    name = _text(monitor.get("name"), MAX_TITLE_CHARS) if isinstance(monitor.get("name"), str) else ""
    if not name:
        raise IngestError("monitor.name is required")
    status = heartbeat.get("status")
    if isinstance(status, bool) or status not in (_KUMA_DOWN, _KUMA_UP, _KUMA_PENDING, _KUMA_MAINTENANCE):
        raise IngestError("heartbeat.status must be 0, 1, 2 or 3")
    if status in (_KUMA_PENDING, _KUMA_MAINTENANCE):
        return IGNORED
    external_id = str(monitor_id).strip()
    if status == _KUMA_UP:
        return IngestEvent(kind=KIND_UP, source=SOURCE_KUMA, external_id=external_id, title=name)
    payload: dict[str, Any] = {"type": monitor.get("type"), "status": _KUMA_DOWN}
    if monitor.get("tags"):
        payload["tags"] = monitor["tags"]
    payload["via"] = "webhook"
    url = monitor.get("url")
    return IngestEvent(kind=KIND_DOWN, source=SOURCE_KUMA, external_id=external_id, title=name,
                       url=_text(url, MAX_URL_CHARS) if isinstance(url, str) else "", payload=payload)


def _fingerprint(title: str) -> str:
    """The one event-identity function (`loop.core.fingerprint`, which is watchdog-poll.py's). Imported
    in the function body: lifecycle/ stays importable without the loop, and core imports lifecycle/."""
    from loop import core
    return core.fingerprint(title)


def normalize_hyperdx(body: dict[str, Any]) -> IngestEvent:
    """OUR contract, which the HyperDX webhook template renders to:
    `{alert, state: ALERT|OK|firing|resolved, message?, url?, service?, value?}`. The dedup key is the
    title fingerprint `slack_alert` uses, so one alert is one event across occurrences whatever numbers
    or timestamps its name carries."""
    alert = body.get("alert")
    full_name = alert.strip() if isinstance(alert, str) else ""
    if not full_name:
        raise IngestError("alert is required")
    title = full_name[:MAX_TITLE_CHARS]
    state = body.get("state")
    state_key = state.strip().lower() if isinstance(state, str) else ""
    if state_key not in _HYPERDX_FIRING and state_key not in _HYPERDX_OK:
        raise IngestError("state must be ALERT, OK, firing or resolved")
    external_id = _fingerprint(full_name)  # the whole name: two names sharing a 200-char prefix stay two events
    if not external_id:
        raise IngestError("alert has no identifying text")
    if state_key in _HYPERDX_OK:
        return IngestEvent(kind=KIND_UP, source=SOURCE_HYPERDX, external_id=external_id, title=title)
    payload: dict[str, Any] = {"state": "ALERT"}
    message = _text(body.get("message"), MAX_MESSAGE_CHARS)
    if message:
        payload["message"] = message
    service = _text(body.get("service"), MAX_LABEL_CHARS)
    if service:
        payload["service"] = service
    value = body.get("value")
    if value is not None:
        payload["value"] = value if isinstance(value, (int, float, bool)) else _text(value, MAX_LABEL_CHARS)
    payload["via"] = "webhook"
    url = body.get("url")
    return IngestEvent(kind=KIND_DOWN, source=SOURCE_HYPERDX, external_id=external_id, title=title,
                       url=_text(url, MAX_URL_CHARS) if isinstance(url, str) else "", payload=payload)


def _open(conn: sqlite3.Connection, ev: IngestEvent, now_iso: str) -> dict[str, Any]:
    """`watchdog-poll.reconcile()`'s three branches for one observed event: insert; reopen a resolved row
    (a recurrence: first_seen restarts, every notification stamp clears); refresh an open row's
    title/url/payload WITHOUT touching first_seen or the notification stamps."""
    payload_json = json.dumps(ev.payload)
    row = conn.execute("SELECT id, resolved_at FROM events WHERE source=? AND external_id=?",
                       (ev.source, ev.external_id)).fetchone()
    if row is None:
        cur = conn.execute(
            "INSERT INTO events(source, external_id, title, url, payload_json, first_seen) VALUES(?,?,?,?,?,?)",
            (ev.source, ev.external_id, ev.title, ev.url, payload_json, now_iso))
        return {"accepted": True, "action": ACTION_OPENED, "event_id": cur.lastrowid}
    if row["resolved_at"] is not None:
        conn.execute(
            "UPDATE events SET resolved_at=NULL, first_seen=?, notified_at=NULL, "
            "last_reminder_at=NULL, reminder_count=0, title=?, url=?, payload_json=? WHERE id=?",
            (now_iso, ev.title, ev.url, payload_json, row["id"]))
        return {"accepted": True, "action": ACTION_OPENED, "event_id": row["id"]}
    conn.execute("UPDATE events SET title=?, url=?, payload_json=? WHERE id=?",
                 (ev.title, ev.url, payload_json, row["id"]))
    return {"accepted": True, "action": ACTION_REFRESHED, "event_id": row["id"]}


def _resolve(conn: sqlite3.Connection, ev: IngestEvent, now_iso: str) -> dict[str, Any]:
    """Set `resolved_at` on the open row; nothing to do (and nothing accepted) when there is none or it
    already resolved: an UP for a monitor warden never saw DOWN, or a repeated OK."""
    row = conn.execute("SELECT id, resolved_at FROM events WHERE source=? AND external_id=?",
                       (ev.source, ev.external_id)).fetchone()
    if row is None or row["resolved_at"] is not None:
        result: dict[str, Any] = {"accepted": False, "action": ACTION_IGNORED}
        if row is not None:
            result["event_id"] = row["id"]
        return result
    conn.execute("UPDATE events SET resolved_at=? WHERE id=?", (now_iso, row["id"]))
    return {"accepted": True, "action": ACTION_RESOLVED, "event_id": row["id"]}


def apply(conn: sqlite3.Connection, ev: IngestEvent, now: dt.datetime | None = None) -> dict[str, Any]:
    """Write `ev` to the ledger in one immediate transaction (the read-then-write must not race a
    concurrent writer into a UNIQUE violation or a mid-transaction SQLITE_BUSY) and return the
    response body. `conn` is a writable connection the caller opened without migrating."""
    if ev.kind == KIND_IGNORE:
        return {"accepted": False, "action": ACTION_IGNORED}
    now_iso = (now or dt.datetime.now(dt.timezone.utc)).isoformat()
    conn.execute("BEGIN IMMEDIATE")
    try:
        result = _open(conn, ev, now_iso) if ev.kind == KIND_DOWN else _resolve(conn, ev, now_iso)
        conn.commit()
    except BaseException:
        conn.rollback()
        raise
    return result
