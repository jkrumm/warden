#!/usr/bin/env python3
"""Revive of the `note`-frozen `slack_alert` rows (written for items 1117/1118;
the standing repair after a re-fire since §90).

WHY THIS EXISTS. `classify()` used to run the structural
`ignoreUnstructuredSlackProse` filter BEFORE matching `config/triage-policy.json`'s
rules. That filter is a prefix test on the title, so a producer that emits plain
sentences — Beszel's `HomeLab CPU above threshold`, no bracket, no emoji —
failed it on every occurrence and was routed to the terminal `note` state
before any rule was ever consulted. Two consequences, both measured live:

  * a `note` row never escalates and a rule added afterwards can never reach it,
    so ten rows sat frozen while the daily digest printed their signatures under
    "Unstructured notes in #alerts" day after day; and
  * `_propose_mapping_candidates()` kept appending rules for signatures no rule
    could ever reach — 134 rule entries for 61 unique match values, 41 ignore
    entries for 12.

`classify()` itself now matches rules first (the filter runs last, only for a
row no rule matched — see its own docstring). This script is the other half of
that fix, and it is **not a one-time run**. The rows it was written for predate
the reorder, so they had to be handed back to `new` for a classify() pass —
but a `slack_alert` signature that *re-fires* clears `events.resolved_at` back
to NULL, which puts its row back in the digest's "Unstructured notes" heading
while `classify()` (only ever touches `new`) and `reopen_if_needed()` (skips
`note`) both leave it alone. The row is then stale *and* immune to the policy
entry that now covers it. Run this again after any such re-fire — that is the
standing repair, measured live on event 999 (§90).

WHAT IT TOUCHES. Rows in state `note` only, whose event `source` is
`slack_alert`, and whose event is NOT resolved. A resolved event is skipped on
purpose: reviving it would produce a card for a condition that is already over
(the live example at the time of writing: the homelab disk alert, resolved
before this ran). Each revived row carries the reason in its own `note`.

Idempotent in the narrow sense that it only ever selects rows still in state
`note` whose event is unresolved, so a second immediate run reports zero and
writes nothing. It is **not** a one-time script: a later re-fire of a covered
signature selects its row again (§90). Writes only with `--apply`; without it
(or with `--dry-run`) it prints the plan over a READ-ONLY connection, so a dry
run cannot write even by accident. `--db PATH` points it at a throwaway copy of
the ledger for exactly that kind of inspection.

Nothing here shells out or touches Slack — the next 600 s `--run` pass does the
classifying, which is where the effect should be watched.
"""

from __future__ import annotations

import datetime as dt
import sys
from pathlib import Path

_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

import triage  # noqa: E402  (scripts/ is on sys.path via the insert above)

# Written onto every revived row. This is the durable record: it says WHY ten
# historical rows were brought back from a terminal state, which is the only
# thing a reader of the ledger in six months has to go on.
REVIVE_NOTE = (
    "revived by scripts/reset-frozen-notes.py (one-time): classify() ran the "
    "ignoreUnstructuredSlackProse filter before rule matching, so this "
    "un-prefixed slack_alert was routed to `note` — terminal, never escalates, "
    "and unreachable by a rule added afterwards. Matching order is fixed; this "
    "row goes back to `new` for one more classify() pass."
)


def _frozen_rows(conn):
    """Every `note`-state `slack_alert` row, oldest event first, with the two
    facts the decision needs: what it is, and whether its event is still live."""
    return conn.execute(
        "SELECT ti.event_id, ti.signature, ti.state, e.title, e.resolved_at "
        "FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        "WHERE ti.state = ? AND e.source = 'slack_alert' "
        "ORDER BY ti.event_id ASC",
        (triage.STATE_NOTE,),
    ).fetchall()


def main(argv: list[str] | None = None) -> int:
    argv = list(argv if argv is not None else sys.argv[1:])
    apply_changes = "--apply" in argv
    triage._apply_db_override(argv)
    # assert_schema_version() only — the loop owns migration (AGENTS.md §The
    # ledger). And read-only unless we are actually writing.
    conn = triage._ledger.connect(triage.DB_PATH, readonly=not apply_changes)
    try:
        rows = _frozen_rows(conn)
        revivable = [r for r in rows if not r["resolved_at"]]
        skipped = [r for r in rows if r["resolved_at"]]
        print(f"ledger: {triage.DB_PATH}")
        print(f"note-state slack_alert rows: {len(rows)} — {len(revivable)} revivable, "
              f"{len(skipped)} resolved (left alone)")
        for r in revivable:
            print(f"  -> new   {r['event_id']:>5}  {r['signature']}  ({(r['title'] or '')[:60]})")
        for r in skipped:
            print(f"  skip     {r['event_id']:>5}  {r['signature']}  (resolved {r['resolved_at']})")
        if not apply_changes:
            print("dry run — nothing written (pass --apply to write)")
            return 0
        now = dt.datetime.now(dt.timezone.utc)
        moved = 0
        for r in revivable:
            # expect_state is a compare-and-set: a row that moved for any other
            # reason between the SELECT and here is left alone, never clobbered.
            moved += triage._set_state(conn, r["event_id"], triage.STATE_NEW, now,
                                        note=REVIVE_NOTE, expect_state=triage.STATE_NOTE)
        conn.commit()
        print(f"revived {moved} row(s) to `new` — the next 600 s loop pass classifies them")
        return 0
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
