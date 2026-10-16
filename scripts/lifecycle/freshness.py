"""Which pollers are alive. One definition for `/health` (api.py) and the `Warden Loop` Kuma push
(triage.py), so the page and the endpoint can never disagree about "stale".

Each poller heartbeats a `cursors` row per completed pass. It is stale when silent for
`STALE_MULTIPLE` x its own LaunchAgent interval (read straight from the plist templates).
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3

LOOP_INTERVAL_S = 600      # com.jkrumm.warden-loop.plist.template
POLL_INTERVAL_S = 1800     # com.jkrumm.warden-poll.plist.template
SWEEP_INTERVAL_S = 300     # com.jkrumm.warden-sweep.plist.template
STALE_MULTIPLE = 3         # a poller silent for 3x its own interval is a finding, not noise

# Named individually, not folded into one "poller last ran" number, so /health and /metrics can
# answer "which one is blind" rather than only "something is."
POLLERS: dict[str, tuple[str, int]] = {
    "loop": ("triage_last_run", LOOP_INTERVAL_S),
    "watchdog_poll": ("watchdog_poll_last_run", POLL_INTERVAL_S),
    "dispatch_sweep": ("dispatch_sweep_last_run", SWEEP_INTERVAL_S),
}


def poller_age_minutes(conn: sqlite3.Connection, now: dt.datetime, key: str) -> tuple[float | None, str | None]:
    """(age_minutes, last_run_iso) for one poller's heartbeat cursor, or (None, None) if it has
    never recorded one."""
    row = conn.execute("SELECT updated_at FROM cursors WHERE key=?", (key,)).fetchone()
    if row is None:
        return None, None
    last_run = dt.datetime.fromisoformat(row["updated_at"])
    return (now - last_run).total_seconds() / 60, row["updated_at"]


def stale_pollers(conn: sqlite3.Connection, now: dt.datetime) -> list[str]:
    """Names of pollers that never heartbeated or are silent past their threshold, plus
    `slack_blind` while the watchdog poll reports its Slack sources blind; [] = all fresh."""
    stale = []
    for name, (key, interval_s) in POLLERS.items():
        age, _ = poller_age_minutes(conn, now, key)
        if age is None or age > interval_s * STALE_MULTIPLE / 60:
            stale.append(name)
    row = conn.execute("SELECT value FROM cursors WHERE key=?", (POLLERS["watchdog_poll"][0],)).fetchone()
    if row is not None:
        try:
            blind = bool(json.loads(row["value"]).get("slack_blind"))
        except (ValueError, AttributeError):
            blind = True   # an unreadable payload means blindness is unknown: withhold, like the retired push did
        if blind:
            stale.append("slack_blind")
    return stale
