#!/usr/bin/env python3.11
"""Does the improvement loop have anything to look at? Prints one line per outcome since the cursor
and exits 0 either way; `quiet` is the only output when nothing happened.

An outcome is a transition INTO `failed`, `needs_decision`, or a `failed`/`needs_decision` caused by
exhausted revisions, newer than the cursor (`~/.warden/improve.cursor`, an ISO timestamp). The
cursor moves with `--ack`, so a reader that looks and does not act sees the same lines again.
Read-only on the ledger.
"""

from __future__ import annotations

import os
import sqlite3
import sys
from pathlib import Path

DB = Path(os.path.expanduser(os.environ.get("WARDEN_DB") or "~/.warden/warden.db"))
CURSOR = Path(os.path.expanduser(os.environ.get("WARDEN_IMPROVE_CURSOR") or "~/.warden/improve.cursor"))
OUTCOME_STATES = ("failed", "needs_decision")


def outcomes(conn: sqlite3.Connection, since: str) -> list[sqlite3.Row]:
    marks = ",".join("?" * len(OUTCOME_STATES))
    return conn.execute(
        f"SELECT t.event_id, t.to_state, t.at, i.repo, i.failure_class, substr(COALESCE(t.note, i.note, ''), 1, 140) AS note "
        f"FROM item_transitions t JOIN triage_items i ON i.event_id = t.event_id "
        f"WHERE t.to_state IN ({marks}) AND i.state = t.to_state AND t.at > ? ORDER BY t.at",
        (*OUTCOME_STATES, since),
    ).fetchall()


def main(argv: list[str]) -> int:
    since = CURSOR.read_text().strip() if CURSOR.exists() else "1970-01-01T00:00:00"
    conn = sqlite3.connect(f"file:{DB}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    rows = outcomes(conn, since)
    if not rows:
        print("quiet")
        return 0
    for r in rows:
        print(f"#{r['event_id']} {r['repo'] or '-'} {r['to_state']}"
              f"{'/' + r['failure_class'] if r['failure_class'] else ''} {r['at'][:16]} {r['note']!r}")
    if "--ack" in argv:
        CURSOR.write_text(rows[-1]["at"] + "\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
