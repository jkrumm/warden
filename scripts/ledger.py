"""ledger — the one module that owns the connection, the schema and the
migrations for `warden.db`. Everything else asks this module for a
connection; nothing else runs DDL.

WHY THIS EXISTS. Before this module, four processes each carried their own
copy of `db_connect()`: `executescript(SCHEMA)` plus a hand-rolled block of
`ALTER TABLE ... ADD COLUMN` guarded by `PRAGMA table_info`. `dispatches` was
declared in three places, `events` and `cursors` in two, with no version
table and no agreement on which copy was authoritative — the DDL itself was
the only source of truth, re-derived from `PRAGMA table_info` on every single
connect, by every single process, forever. That is not a migration system,
it is four independent guesses that happened to converge because nobody had
changed the schema yet. The day they diverge — one process updated, three
not — there is nothing here to notice, only a `sqlite3.OperationalError`
raised from whichever query touches the missing column first, in production,
possibly at 3am.

The fix is not "add a fifth copy that is more careful." It is one module that
is the schema: one `LEDGER_SCHEMA_VERSION`, one ordered `MIGRATIONS` map, one
`migrate()` that only the process that owns the loop's 10-minute boot is
allowed to run, and one `assert_schema_version()` that every other process
calls instead — so a poller or a sweeper that starts before the loop has
ever touched a fresh ledger fails loudly on a version mismatch rather than
quietly inventing its own tables.

The ledger this module was written against is not new — it is a live,
un-versioned SQLite file that four processes have been writing to for
months, on `journal_mode=delete`, with `user_version=0`. `migrate()` has to
treat *adopting* that file, unmodified, as its ordinary first case, not as a
one-off recovery path. See `migrate()`'s own docstring for why that case is
handled first and separately from every later version.
"""

import datetime as dt
import os
import sqlite3
import sys
from pathlib import Path
from typing import Callable

# Same env-var-first, documented-default-second shape every CLI binary path in
# this repo already uses — an operator or a test overrides one variable and
# every reader of this module (this file, and anything that imports it) agrees
# without a second constant to keep in sync.
WARDEN_HOME = (
    Path(os.environ["WARDEN_HOME"]).expanduser() if os.environ.get("WARDEN_HOME") else Path.home() / ".warden"
)

# DB_PATH is a module-level global, not a value baked into a default
# argument, and every function below re-reads it at call time rather than
# capturing it at import — that is what lets a test (or a script's own `--db`
# override) monkeypatch `ledger.DB_PATH` and have `connect()` see the new
# value on its very next call, exactly like loop/core.py's own DB_PATH already
# works today.
DB_PATH = Path(os.environ["WARDEN_DB"]).expanduser() if os.environ.get("WARDEN_DB") else WARDEN_HOME / "warden.db"

# This IS the live ledger, since the cutover on 2026-09-09. Four LaunchAgents
# and the warden CLI all resolve here. ~/.hermes/watchdog.db is still on disk,
# frozen at its pre-cutover state and written by nothing, kept as the rollback —
# so if you are reading this to work out which database is real, it is this one.

# Named LEDGER_SCHEMA_VERSION, not SCHEMA_VERSION, deliberately: it pins this
# module's own SQLite schema, distinct from scripts/clients/sideclaw.py's
# DISPATCH_SCHEMA_VERSION/REVIEW_SCHEMA_VERSION, which pin sideclaw's published
# verdict schemas and are asserted per job by assert_result_schema — two independent pins that must
# never be conflated.
LEDGER_SCHEMA_VERSION = 15


class LedgerBehind(RuntimeError):
    """The ledger is older than this process's schema and the loop has not
    migrated it yet — a transient, expected state during a deploy, never a
    data hazard."""

# Single row, updated in place — never a history table. "Which version was
# this database at three migrations ago" is not a question anything here
# needs to answer; "is it at the version this process expects, right now" is
# the only one that matters, and one row answers it in one query with no
# ORDER BY / LIMIT to get wrong.
_SCHEMA_VERSION_TABLE = """
CREATE TABLE IF NOT EXISTS schema_version (
    version    INTEGER NOT NULL,
    applied_at TEXT    NOT NULL
)
"""

# Version 1 — every CREATE TABLE and CREATE INDEX currently spread across
# the CLI, triage.py, watchdog-poll.py and dispatch-sweep.py, with the
# columns that today only exist because of a runtime `ALTER TABLE` declared
# inline, in the exact column order and position SQLite's own ALTER TABLE
# ADD COLUMN produces (new column text is spliced in after the last existing
# column, before any table-level constraint — which is why `dispatch_id`
# below sits before `UNIQUE(source, external_id)` rather than after it).
# Column lists, index definitions and column order were read directly off
# the then-live ~/.hermes/watchdog.db via a read-only connection — not
# reconstructed from memory of the four DDL blocks — so this is a faithful
# copy, not a redesign.
BASE_SCHEMA = """
CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    source TEXT NOT NULL,
    external_id TEXT NOT NULL,
    title TEXT NOT NULL,
    url TEXT,
    payload_json TEXT,
    first_seen TEXT NOT NULL,
    notified_at TEXT,
    last_reminder_at TEXT,
    reminder_count INTEGER NOT NULL DEFAULT 0,
    resolved_at TEXT,
    dispatch_id INTEGER,
    UNIQUE(source, external_id)
);
CREATE INDEX IF NOT EXISTS idx_events_open ON events(source) WHERE resolved_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_events_resolved_at ON events(resolved_at);

CREATE TABLE IF NOT EXISTS cursors (
    key TEXT PRIMARY KEY,
    value TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dispatches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_id TEXT NOT NULL UNIQUE,
    tier TEXT NOT NULL,
    repo TEXT NOT NULL,
    brief TEXT NOT NULL,
    why TEXT,
    origin_channel TEXT,
    origin_thread_ts TEXT,
    origin_event_id INTEGER,
    status TEXT NOT NULL,
    verdict_json TEXT,
    artifact_url TEXT,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    reported_at TEXT,
    merged_at TEXT,
    poll_misses INTEGER NOT NULL DEFAULT 0,
    validation_job_id TEXT,
    validation_status TEXT
);
CREATE INDEX IF NOT EXISTS idx_dispatches_open ON dispatches(status) WHERE reported_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_dispatches_created ON dispatches(created_at);

CREATE TABLE IF NOT EXISTS triage_items (
  event_id      INTEGER PRIMARY KEY REFERENCES events(id),
  signature     TEXT NOT NULL,
  repo          TEXT,
  verb          TEXT,
  state         TEXT NOT NULL,
  card_channel  TEXT,
  card_ts       TEXT,
  card_hash     TEXT,
  dispatch_job  TEXT,
  artifact_url  TEXT,
  occurrences   INTEGER NOT NULL DEFAULT 0,
  first_seen    TEXT,
  last_seen     TEXT,
  snoozed_until TEXT,
  created_at    TEXT NOT NULL,
  updated_at    TEXT NOT NULL,
  note          TEXT,
  implement_job      TEXT,
  validation_job     TEXT,
  pr_url             TEXT,
  deploy_expect_json TEXT,
  liveness_deadline  TEXT,
  propose_unsure_at  TEXT
);
CREATE INDEX IF NOT EXISTS idx_triage_state ON triage_items(state);
"""

# Target version -> its DDL. Adding version 2 is appending one entry here —
# `migrate()` below already walks every version strictly greater than the
# database's current one, in order, so nothing else changes.
# Version 2 — `state_deadline`, one column, so that every non-terminal state can
# name the moment it stops being allowed to sit there. DESIGN.md § Deadlines:
# "Every non-terminal state gets a `state_deadline` and a named poller."
#
# ONE column, not one per state, and not a second table: the deadline is a
# property OF the row's current state, so it is rewritten by the same UPDATE that
# writes the state and is meaningless without it. A separate table would let the
# two disagree, and "the deadline says investigating, the row says merged" is a
# question nothing could answer.
#
# NULL means "no deadline applies" — a terminal state, or `new`, which is bounded
# by silence-resolve rather than by a clock (see loop/intake.py's
# _SILENCE_RESOLVE_ELIGIBLE_STATES). It is NOT "the deadline was forgotten": the
# sweeper treats NULL on a non-terminal state as a finding, not as permission.
#
# `liveness_deadline` (version 1) is deliberately left alone rather than folded
# into this column. It is the deploy path's own probe window and predates this;
# merging them would be a data migration on a live ledger to save one column.
_MIGRATION_2 = """
ALTER TABLE triage_items ADD COLUMN state_deadline TEXT;
CREATE INDEX IF NOT EXISTS idx_triage_state_deadline ON triage_items(state_deadline);
"""

# Version 3 — `occurrence_mark`, one column, so loop/intake.py's reopen_if_needed()
# can tell "this resolved/dismissed row is still quiet" from "a new occurrence
# arrived" without asking `events.resolved_at IS NULL` — which for a GROUPED
# source (slack_alert, hermes_log) stays NULL for up to 7 idle days by design,
# so that question was true on every single pass and reopened (then
# immediately re-resolved) a quiet-resolved row every 10 minutes, forever.
# Measured on the live ledger 2026-09-09: 23 of 30 `resolved` rows were stuck
# in that loop, invisible only because the re-rendered card is byte-identical
# and card_hash short-circuited the Slack call. It had already destroyed
# history twice — see loop/intake.py's reopen_if_needed() and docs/history/triage.md.
#
# NULL here is NOT a forgotten stamp — it is a row that closed before this
# column existed. reopen_if_needed() treats NULL as "baseline unknown",
# stamps it with the event's current mark, and does not reopen. That adoption
# rule is the entire backfill this migration needs; it deliberately writes no
# data itself.
_MIGRATION_3 = "ALTER TABLE triage_items ADD COLUMN occurrence_mark TEXT;"

# Version 4 — the `resolved` -> `fixed`/`quiet`/`closed` split (the state machine
# in loop/core.py, DESIGN.md § the state machine) plus `item_transitions`, an
# append-only history table. Three of the six /metrics funnel numbers (median
# needs_human -> decision, verified unattended fixes per week, reopen-after-
# `fixed`) are not derivable from the ledger without it: `triage_items.
# updated_at` cannot serve — ingest() rewrites it on every open row on every
# pass regardless of state (see loop/core.py's _set_state() docstring), so it
# cannot answer "when did this item enter/leave a state" at all.
#
# The literal 'resolved' below, not a symbolic constant: STATE_RESOLVED no
# longer exists in loop/core.py after this migration lands, and this module owns
# the schema and must not import triage's constants to migrate triage's own
# data. On a fresh database the UPDATE is a harmless no-op — nothing to match.
#
# Every one of the live ledger's 30 `resolved` rows becomes `quiet`, and NONE
# becomes `fixed` — this reads like laziness and is not. All 30 are silence
# closes: by note (`signal quiet since …`) or by an empty note
# (apply_resolutions() clears it on a disappearance-resolve). The recovery-
# pairing note prefix (RECOVERY_PAIRED_NOTE_PREFIX) appears on ZERO rows.
# DESIGN.md and REVIEW.md both name items 931/932 as the only two verified-
# recovery closes this system has ever produced (after jkrumm/vps#8 merged) —
# but the pre-occurrence_mark reopen churn (see migration 3) overwrote both
# rows' notes with its own text, dispatches.merged_at is NULL for both, the PR
# is recorded on a dispatch whose origin_event_id is NULL, and each row's
# dispatch_job points at a LATER investigate episode, not the one that
# produced the fix. The ledger cannot substantiate `fixed` for either row, and
# back-dating a state from a prose document is not a migration. Both become
# `quiet` with the rest — /metrics will read 0 verified fixes for the period
# before this column existed, understating true history by two. That is the
# correct direction to be wrong: a window that predates this table is empty
# by construction, not zero, and a future reader must not mistake the two.
_MIGRATION_4 = """
UPDATE triage_items SET state = 'quiet' WHERE state = 'resolved';

CREATE TABLE item_transitions (
  id         INTEGER PRIMARY KEY AUTOINCREMENT,
  event_id   INTEGER NOT NULL REFERENCES triage_items(event_id),
  from_state TEXT,
  to_state   TEXT NOT NULL,
  at         TEXT NOT NULL,
  note       TEXT
);
CREATE INDEX IF NOT EXISTS idx_item_transitions_event ON item_transitions(event_id, id);
"""

# Version 5 — `operations`, the crash-recovery unit DESIGN.md § Crash recovery
# calls for: "an operation id recorded before dispatch... unknown is an
# explicit outcome, reconciled before any retry — never silently read as
# failure."
#
# `outcome IS NULL` means "in flight" — an operation this process recorded as
# STARTED but has not yet recorded the result of, whether because it crashed
# before it could, or because the external call it covers (`warden dispatch
# --tier implement`, `warden merge --confirm`) returned ambiguously (a
# subprocess timeout, unparseable stdout) and the call site deliberately left
# it open for loop/work.py's reconcile_operations() to resolve on the very next
# pass, before anything else acts on the item (see that file's module
# docstring, step 0). The partial index below is what makes that scan cheap —
# the whole point of recording the id BEFORE the external call is that this
# table is checked on every single pass, not just after a genuine crash.
#
# No data migration, same as item_transitions in migration 4: the table
# starts empty and records nothing retroactively. A /metrics window
# predating it is empty BY CONSTRUCTION, not zero — see this module's own
# migration 4 comment and api.py's _history_guard_reason() for the pattern
# this repeats.
_MIGRATION_5 = """
CREATE TABLE operations (
  op_id          TEXT PRIMARY KEY,
  event_id       INTEGER REFERENCES events(id),
  kind           TEXT NOT NULL,
  repo           TEXT NOT NULL,
  authorized_by  TEXT NOT NULL,
  started_at     TEXT NOT NULL,
  outcome        TEXT,
  outcome_at     TEXT,
  receipt_json   TEXT,
  reconciled_at  TEXT,
  note           TEXT
);
CREATE INDEX IF NOT EXISTS operations_open  ON operations(outcome) WHERE outcome IS NULL;
CREATE INDEX IF NOT EXISTS operations_event ON operations(event_id);
"""

# Version 6 — `dispatches.delivery_status`, so the delivery outcome stops
# overloading `reported_at` (a TEXT column that is supposed to hold a
# timestamp) with the string UNDELIVERABLE_SENTINEL
# ("undeliverable:no-origin-channel") whenever a dispatch has no
# origin_channel to answer into. This landed alongside dispatch-sweep.py's
# move off `hermes send` (a Slack Socket Mode subprocess, in a reconnect
# loop) onto the plain HTTP client scripts/slack_client.py already uses —
# see that file's module docstring.
#
# Values: 'delivered' (the verdict/notice reached Slack), the literal
# sentinel string above (never deliverable — no thread existed to answer
# into), or 'failed:<short reason>' (the sweep's last attempt errored;
# `reported_at` stays NULL and the row is retried next pass, exactly as
# before this migration — a failed attempt is not a terminal delivery
# outcome, so it does not get its own reported_at treatment here). NULL
# means "delivery never attempted yet", true of every row with
# `reported_at IS NULL` today.
#
# Backfill, in two passes so the second cannot re-stamp what the first just
# wrote:
#   1. Every row whose `reported_at` IS the sentinel gets `delivery_status`
#      set to that same string and `reported_at` rewritten to a REAL
#      timestamp (`finished_at`, or `created_at` if the dispatch never
#      reached a terminal status) — the sentinel meant "closed, nothing to
#      deliver," not "no timestamp exists for this row." This is what keeps
#      these rows out of `idx_dispatches_open` (`WHERE reported_at IS
#      NULL`) exactly as the sentinel did, now via a real IS NOT NULL
#      timestamp instead of a string masquerading as one.
#   2. Every remaining row with a non-NULL `reported_at` (i.e. a real
#      timestamp already, untouched by pass 1) gets `delivery_status =
#      'delivered'` — that is what a non-NULL, non-sentinel `reported_at`
#      has always meant. Guarded by `delivery_status IS NULL` so pass 1's
#      rows (already stamped, and by now also holding a real `reported_at`)
#      are not overwritten.
# Rows with `reported_at IS NULL` (delivery never attempted, or a `failed`
# row awaiting retry) match neither UPDATE and keep `delivery_status NULL`
# — correct, since this migration runs once, retroactively, and cannot know
# a still-open row's eventual outcome.
_MIGRATION_6 = """
ALTER TABLE dispatches ADD COLUMN delivery_status TEXT;

UPDATE dispatches
   SET delivery_status = reported_at,
       reported_at = COALESCE(finished_at, created_at)
 WHERE reported_at LIKE 'undeliverable:%';

UPDATE dispatches
   SET delivery_status = 'delivered'
 WHERE reported_at IS NOT NULL
   AND delivery_status IS NULL;
"""

# Version 7 — Wave 5.2 (the policy/dispatch port).
#
#   triage_items.revert_pr           `warden revert <item>` (Wave 5.3, not yet
#     built) records the revert PR's URL here. Added now, alongside the other
#     lifecycle columns, so a single migration covers both of Wave 5's halves
#     rather than forcing a Wave-5.3-only schema bump for one column.
#
# (This migration also used to add four columns to `dispatch_approvals`; that
# table is gone — see version 12 — so they are not replayed here.)
_MIGRATION_7 = """
ALTER TABLE triage_items ADD COLUMN revert_pr INTEGER;
"""

# Version 8 — Wave 6.1 (every origin opens an item). Three columns, one
# table, for the same reason: `triage_items` needs to represent a row that
# did not come from `ingest()`'s seven alert sources at all.
#
#   triage_items.origin       `alert | human | github_issue` (DESIGN.md:177).
#     DEFAULT 'alert' so every pre-existing row — the entire ingest() family —
#     keeps its true origin with no backfill. Written once, at INSERT, by
#     `open_origin_item()` for the two new sources and by `ingest()` (which
#     never sets it, so it rides the default) for the seven alert ones.
#
#   triage_items.max_tier     the ceiling THIS item may reach on its own —
#     `implement` (the pre-Wave-6 behaviour: the verdict decides,
#     same as every alert item today) or `investigate` (maybe_auto_implement()
#     and lifecycle/policy.py's require_auto_from_item() both refuse to cross
#     it). DEFAULT 'implement' for the same backfill-free reason as `origin`:
#     every alert item today already behaves as if its ceiling were
#     `implement`, so the default is honest, not a placeholder.
#
#   triage_items.brief        the human's brief, or the GitHub issue's body.
#     NULL for `alert` (an alert's "brief" is built fresh every escalation by
#     _build_cluster_brief() from the clustered signatures — there is no
#     single fixed text to store), set once at INSERT for the other two
#     origins and read back verbatim by escalate_origin_items().
#
#   triage_items.origin_channel / origin_thread_ts   the Slack thread a
#     `warden run --origin-channel --origin-thread` call answers into (Hermes,
#     replying in its own thread) — set once at INSERT by `open_origin_item()`
#     from `cmd_run`'s flags, NULL for every other origin. Read by
#     `_dispatch_investigate_and_advance()` to build the investigate episode's
#     `Origin`: when set, the verdict must land in the ASKER's thread, not the
#     shared triage card's — a card is a projection, not the origin.
_MIGRATION_8 = """
ALTER TABLE triage_items ADD COLUMN origin TEXT NOT NULL DEFAULT 'alert';
ALTER TABLE triage_items ADD COLUMN max_tier TEXT NOT NULL DEFAULT 'implement';
ALTER TABLE triage_items ADD COLUMN brief TEXT;
ALTER TABLE triage_items ADD COLUMN origin_channel TEXT;
ALTER TABLE triage_items ADD COLUMN origin_thread_ts TEXT;
"""

# Version 9 — the `needs_human`/`merge_blocked` reminder (DESIGN.md:247's
# "7d, reminder at 1d", the one row of its own deadline table triage.py's
# STATE_DEADLINES comment and docs/api.md's carried-debt note both flagged
# as NOT built). Two columns on `triage_items`, mirroring the same
# reminder_count/last_reminder_at SHAPE `events` already carries for the
# grouped-source poller's OWN reminder cadence (BASE_SCHEMA) — but never the
# same COLUMNS: `events.reminder_count`/`last_reminder_at` belong to
# watchdog-poll.py's occurrence-batching poller, an entirely different
# mechanism against a different table, and reusing them here would make one
# counter answer two unrelated questions ("how many times has this alert
# recurred" vs "how many reminder replies has a human been sent").
#
#   triage_items.reminder_count    0 (DEFAULT, so every pre-existing row
#     reads as "never reminded", correctly — this mechanism did not exist
#     before this migration) until remind_needs_human() posts a reminder,
#     then 1, then 2 — capped there for good, by that function's own logic,
#     never by a CHECK constraint (closed vocabularies live in code in this
#     repo, not in DDL — see e.g. _valid_host_verb_rule()).
#
#   triage_items.last_reminder_at  NULL until the first reminder, then the
#     timestamp of the MOST RECENT one — read by nothing yet (eligibility is
#     computed from `item_transitions`, not this column — see
#     remind_needs_human()'s own docstring for why), kept purely as the
#     human-visible "when did warden last poke me about this" fact a card or
#     a future /items response can read without re-deriving it.
_MIGRATION_9 = """
ALTER TABLE triage_items ADD COLUMN reminder_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE triage_items ADD COLUMN last_reminder_at TEXT;
"""

# Version 10 — the 2026-09-12 defect (item 253, `docker_homelab:unhealthy:
# garmin-collector`): a sideclaw `investigate` episode was killed by a
# timeout, `verdict_json` was left NULL, and fold_dispatch_verdict() (which
# never read `dispatches.status`) computed `result = {}` -> `next_action = ""`
# -> `new_state = STATE_VERDICT` with `note = NULL`. The item parked as a
# verdict-less verdict, invisible until its 24h deadline, and the ledger held
# no reason at all — sideclaw's own failure text reached Slack via
# dispatch-sweep.py's format_message() but was never persisted anywhere.
# DESIGN.md's "deferral must be visible" is exactly the property this closes.
#
#   dispatches.error   sideclaw's terminal failure text verbatim (job.get
#     ("error") — see clients/sideclaw.py's own job shape), or warden's own
#     pruned-notice reason for a row _mark_pruned() closes without ever
#     polling a real answer (dispatch-sweep.py's `_mark_pruned()`). NULL on
#     every successful dispatch, and NULL on every pre-existing row — no
#     backfill, because a row that failed before this migration genuinely has
#     no recorded reason, and inventing one would be worse than admitting the
#     gap (the same "empty by construction, not zero" rule migrations 4 and 5
#     already document for their own no-backfill tables).
#
# A COLUMN, not a field folded into `verdict_json`: that column is sideclaw's
# own PUBLISHED verdict schema (see AGENTS.md § Talking to sideclaw — "the
# verdict schema is published by sideclaw, not copied here"), and a failed
# dispatch has no verdict at all to carry a field on. The reason needs
# somewhere of its own that is not shaped like an answer to a question
# nobody answered.
_MIGRATION_10 = """ALTER TABLE dispatches ADD COLUMN error TEXT;"""

# Version 11 — the chain stops ending in a parked item (2026-09-28, state-log
# §92). Four columns, all additive, all correct as their DEFAULT on every
# pre-existing row:
#
#   triage_items.revision_count      how many times maybe_revise_blocked() sent
#     a blocked implementation back to a fresh implement episode with the
#     reviewer's findings. 0 on every existing row: no revision ever ran.
#   triage_items.parked_mark          the event's _occurrence_mark() when the
#     item was last seen parked (needs_human / merge_blocked) — the baseline
#     track_parked_recurrences() compares against. NULL until first seen parked.
#   triage_items.parked_recurrences   occurrences observed while parked. A
#     parked row absorbs every recurrence of its signal (reopen_if_needed()
#     never touches it), so without this nothing shows the fix is overdue.
#   triage_items.recurrence_reminded_at  when the N-recurrences reminder
#     posted, so it posts once per parking, independent of reminder_count.
_MIGRATION_11 = """
ALTER TABLE triage_items ADD COLUMN revision_count INTEGER NOT NULL DEFAULT 0;
ALTER TABLE triage_items ADD COLUMN parked_mark TEXT;
ALTER TABLE triage_items ADD COLUMN parked_recurrences INTEGER NOT NULL DEFAULT 0;
ALTER TABLE triage_items ADD COLUMN recurrence_reminded_at TEXT;
"""

# Version 12 — the signed-approval stack is deleted (agent-platform.md §Warden:
# review is the gate, not a Slack click). `dispatch_approvals` and its index held
# nothing the loop reads any more; nothing replaces them.
_MIGRATION_12 = """
DROP INDEX IF EXISTS idx_approvals_hash;
DROP TABLE IF EXISTS dispatch_approvals;
"""

# Version 13 — the state machine collapses to the spec's states (agent-platform.md
# §Warden) and every clock-driven expiry becomes one retry rule.
#
#   triage_items.close_reason   closed -> duplicate | fixed_by | ignored | resolved.
#     NULL on every other state.
#   triage_items.strikes        consecutive infrastructure failures of the current
#     step; 3 -> `failed`. Reset when the item advances forward in the pipeline.
#   triage_items.retry_at       a poller that submits skips the row until then.
#
# Dropped, dead since Wave 1 (nothing reads or writes them; the reminder and
# parked-recurrence features are gone, `snoozed` is gone, and the generic
# STATE_DEADLINES clock is gone with `state_deadline`): reminder_count,
# last_reminder_at, parked_mark, parked_recurrences, recurrence_reminded_at,
# propose_unsure_at, snoozed_until, state_deadline. `events.reminder_count` and
# `events.last_reminder_at` are a different table and are NOT touched.
#
# State mapping (old -> new), applied to triage_items.state AND to both columns of
# item_transitions. History rows are rewritten rather than left as history because
# live code reads them back (funnel metrics 3/4/6 and the chronic-recurrence count
# compare from_state/to_state against the new vocabulary):
#
#   | old                                          | new                  |
#   | new                                          | new                  |
#   | split                                        | triaged              |
#   | investigating, verdict, implementing,        | working              |
#   |   remediating                                |                      |
#   | validating, pr_open                          | merging              |
#   | merged, liveness_pending                     | verifying            |
#   | needs_human, merge_blocked, reverted         | failed (note kept)   |
#   | fixed                                        | fixed                |
#   | quiet                                        | quiet                |
#   | closed, dismissed                            | closed, resolved     |
#   | ignored, note                                | closed, ignored      |
#   | snoozed                                      | new                  |
#
# `dismissed` was a deadline expiry, never a human judgement, so it maps to `resolved`
# (a recurrence reopens it) and not to `ignored` (a human's "this is noise", which stays).
#
# `close_reason` is only written on triage_items (history has no reason column).
#
# `triage_items.card_hash` changes meaning: it used to be a hash of a rendered Slack
# card, it is now the state a one-line Slack post was made for (loop/notify.py
# `notify_cluster()` posts when an item is in `fixed`/`needs_decision` and
# `card_hash` differs from that state). Every item already `fixed` has been dealt
# with, so it is stamped `fixed` here — otherwise the first pass on this schema would
# announce the whole backlog. The same goes for a `closed` item with its own origin thread:
# it is answered there once (state `answered`), and every one already closed has been.
_STATE_MAP_13 = {
    "new": "new", "split": "triaged",
    "investigating": "working", "verdict": "working", "implementing": "working", "remediating": "working",
    "validating": "merging", "pr_open": "merging",
    "merged": "verifying", "liveness_pending": "verifying",
    "needs_human": "failed", "merge_blocked": "failed", "reverted": "failed",
    "fixed": "fixed", "quiet": "quiet", "closed": "closed",
    "ignored": "closed", "dismissed": "closed", "note": "closed",
    "snoozed": "new",
}


def _case_13(column: str) -> str:
    whens = " ".join(f"WHEN '{old}' THEN '{new}'" for old, new in _STATE_MAP_13.items())
    return f"CASE {column} {whens} ELSE {column} END"


_MIGRATION_13 = f"""
DROP INDEX IF EXISTS idx_triage_state_deadline;
ALTER TABLE triage_items DROP COLUMN reminder_count;
ALTER TABLE triage_items DROP COLUMN last_reminder_at;
ALTER TABLE triage_items DROP COLUMN parked_mark;
ALTER TABLE triage_items DROP COLUMN parked_recurrences;
ALTER TABLE triage_items DROP COLUMN recurrence_reminded_at;
ALTER TABLE triage_items DROP COLUMN propose_unsure_at;
ALTER TABLE triage_items DROP COLUMN snoozed_until;
ALTER TABLE triage_items DROP COLUMN state_deadline;
ALTER TABLE triage_items ADD COLUMN close_reason TEXT;
ALTER TABLE triage_items ADD COLUMN strikes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE triage_items ADD COLUMN retry_at TEXT;
UPDATE triage_items SET
  close_reason = CASE state WHEN 'closed' THEN 'resolved' WHEN 'dismissed' THEN 'resolved'
                            WHEN 'ignored' THEN 'ignored' WHEN 'note' THEN 'ignored' END,
  state = {_case_13("state")};
UPDATE triage_items SET card_hash = 'fixed' WHERE state = 'fixed';
UPDATE triage_items SET card_hash = 'answered' WHERE state = 'closed' AND origin_channel IS NOT NULL;
UPDATE item_transitions SET
  from_state = {_case_13("from_state")},
  to_state = {_case_13("to_state")};
"""

# Version 14 — intake and dedup (agent-platform.md §Warden steps 2-3). Four nullable
# columns on triage_items, all correct as NULL on every existing row:
#
#   root_cause    the verdict's own `rootCause` key (kebab, <=80) — what a later
#     verdict is compared against to merge two items that are one defect.
#   duplicate_of  event_id of the item this one was merged into; set together with
#     state closed / close_reason duplicate. NULL on every other row.
#   triage_job    the sideclaw `triage` job that is deciding this item's intake
#     (the single-shot triage step); NULL until one is opened. Cleared again when the
#     fold settles it (except an `ignore`, which is how a model's ignore is told from a
#     human's).
#   triage_job_at when `triage_job` was claimed/submitted — what a job stuck non-terminal is
#     aged from (cancelled and struck after TRIAGE_JOB_STALE_MINUTES). Written with
#     `triage_job`, NULL whenever that is.
#
# The last implement episode's `dispatch/*` branch gets NO column: it already lives
# in dispatches.verdict_json (`result.branch`) of the item's `implement_job`, which
# is where a revision reads it.
_MIGRATION_14 = """
ALTER TABLE triage_items ADD COLUMN root_cause TEXT;
ALTER TABLE triage_items ADD COLUMN duplicate_of INTEGER;
ALTER TABLE triage_items ADD COLUMN triage_job TEXT;
ALTER TABLE triage_items ADD COLUMN triage_job_at TEXT;
"""

# Version 15 — deploy and verify through the repo's own Makefile (agent-platform.md
# §Warden, Wave 4). Four columns on triage_items describing a `verifying` item's
# verification, all NULL / 0 on every other row:
#
#   verify_started_at  when the verify window opened — set once `make deploy` succeeded
#     (or the repo has no deploy target, or a host verb restarted the process). NULL on a
#     `verifying` item means "the deploy has not run yet": the verify pass runs it.
#   verify_mark        the event's occurrence mark at that moment — what "the item's own
#     signal recurred since verification started" is a comparison against (NULL: no signal
#     to watch, so no recurrence check).
#   verify_failures    consecutive passes in which verification failed (`make verify`
#     non-zero, or the item's own Kuma monitor not UP after the window); 3 -> verify failure.
#   verify_result      the last verification result, one short line, for the card/Argo.
#
# `liveness_deadline` is no longer written (the window is `verify_started_at` + the loop's
# VERIFY_WINDOW_HOURS); the column stays, unwritten — no table rebuild for it. Items already
# `verifying` were deployed by the old rollout, so they start their window now and skip the
# deploy.
#
# And the merge train — a `merging` item walks update -> checks -> review -> merge, one item
# per repo at a time (scripts/loop/train.py advance_merge_trains()):
#
#   train_stage     the stage the item is on; NULL outside `merging`.
#   train_sha       the PR head the train is checking, reviewing and will merge — set by the
#     update stage from sideclaw's `update_pr` result; NULL outside `merging`.
#   train_job       the in-flight `update_pr` job (or the `claiming` sentinel during its submit);
#     NULL otherwise.
#   reviewed_sha    the last PR head a step-7 review confirmed. Survives leaving `merging` (an
#     owner merge of the same head need not re-review); a new implement attempt clears it.
#   train_evidence  what ended a train in a revision (the rebase conflict, the failed checks),
#     read by the revision brief — the update_pr job that said so is not a dispatch row.
#   train_rewinds   times the train went back to update (the PR head moved off its SHA) since the
#     item entered `merging`; past a small limit each rewind strikes. 0 outside `merging`.
#   train_pushed_at when the update stage last pushed (`update_pr` -> `updated`): right after a
#     push GitHub may not have registered the check runs yet, so "none" reads as pending.
#
# Items already `merging` restart the train at `update`: the review they may be in the middle
# of read a head nobody pinned.
#
# And the automatic revert of a change that failed verification (loop/verify.py _on_verify_failure()):
#
#   merged_sha      the commit the item's last merge landed as; set on entering `verifying` from
#     a merge, NULL on every other entry (a host verb has nothing to revert).
#   merge_method    how that merge landed (`squash`/`merge`/`rebase`, `unknown` when no receipt says);
#     NULL when the item did not enter `verifying` from a merge. Only a squash or a merge commit
#     is one commit a mechanical revert can undo.
#   reverting_sha   the merged commit being reverted, set from the verify failure until the
#     revert has landed and passed `make verify`. Set at merge time, it marks a revert merge.
#   revert_json     {sha, pr, title, evidence}: what was reverted and why — the revert's brief
#     and review context, then the fresh attempt's context. Cleared when that attempt's PR
#     joins the merge train.
#
# And the fixed-by sweep after every fix merge (loop/verify.py advance_fixed_by_sweeps()), whose
# bookkeeping lives on the MERGED item, independent of that item's own state:
#
#   sweep_pr          the merged pull request awaiting its sweep; set when a non-revert merge lands,
#     NULL once the sweep folded or gave up.
#   sweep_job         the sideclaw `triage` job deciding the sweep (or its `claiming:<time>`
#     sentinel during the submit); NULL until submitted.
#   sweep_job_at      when `sweep_job` was claimed/submitted — what a stuck job is aged from.
#   sweep_attempts    failed submits/jobs so far; the sweep gives up (quietly) at the limit.
#   sweep_candidates  JSON {event_id: state} of the items the prompt showed — the only ids a
#     match may name.
#
# and on the SWEPT item:
#
#   fixed_by_pr       the merged PR a sweep matched this item to; set while the item is `verifying`
#     on signal alone (no deploy, no `make verify` — the merging item already ran both), NULL on
#     every other row.
_MIGRATION_15 = """
ALTER TABLE triage_items ADD COLUMN verify_started_at TEXT;
ALTER TABLE triage_items ADD COLUMN verify_mark TEXT;
ALTER TABLE triage_items ADD COLUMN verify_failures INTEGER NOT NULL DEFAULT 0;
ALTER TABLE triage_items ADD COLUMN verify_result TEXT;
UPDATE triage_items SET verify_started_at = updated_at WHERE state = 'verifying';
ALTER TABLE triage_items ADD COLUMN train_stage TEXT;
ALTER TABLE triage_items ADD COLUMN train_sha TEXT;
ALTER TABLE triage_items ADD COLUMN train_job TEXT;
ALTER TABLE triage_items ADD COLUMN reviewed_sha TEXT;
ALTER TABLE triage_items ADD COLUMN train_evidence TEXT;
ALTER TABLE triage_items ADD COLUMN train_rewinds INTEGER NOT NULL DEFAULT 0;
ALTER TABLE triage_items ADD COLUMN train_pushed_at TEXT;
UPDATE triage_items SET train_stage = 'update' WHERE state = 'merging';
ALTER TABLE triage_items ADD COLUMN merged_sha TEXT;
ALTER TABLE triage_items ADD COLUMN merge_method TEXT;
ALTER TABLE triage_items ADD COLUMN reverting_sha TEXT;
ALTER TABLE triage_items ADD COLUMN revert_json TEXT;
ALTER TABLE triage_items ADD COLUMN sweep_pr TEXT;
ALTER TABLE triage_items ADD COLUMN sweep_job TEXT;
ALTER TABLE triage_items ADD COLUMN sweep_job_at TEXT;
ALTER TABLE triage_items ADD COLUMN sweep_attempts INTEGER NOT NULL DEFAULT 0;
ALTER TABLE triage_items ADD COLUMN sweep_candidates TEXT;
ALTER TABLE triage_items ADD COLUMN fixed_by_pr TEXT;
"""

MIGRATIONS: dict[int, str] = {
    1: BASE_SCHEMA,
    2: _MIGRATION_2,
    3: _MIGRATION_3,
    4: _MIGRATION_4,
    5: _MIGRATION_5,
    6: _MIGRATION_6,
    7: _MIGRATION_7,
    8: _MIGRATION_8,
    9: _MIGRATION_9,
    10: _MIGRATION_10,
    11: _MIGRATION_11,
    12: _MIGRATION_12,
    13: _MIGRATION_13,
    14: _MIGRATION_14,
    15: _MIGRATION_15,
}

# The four tables BASE_SCHEMA declares, i.e. what "this is the live
# pre-versioned ledger" looks like from the outside. Used only by the
# adoption check in migrate() — never by connect()/assert_schema_version(),
# which only ever look at schema_version itself.
_ADOPTABLE_TABLES = ("events", "cursors", "dispatches", "triage_items")

# Every table this schema declares AT LEDGER_SCHEMA_VERSION, used only by
# _expected_columns()/_verify_columns() — deliberately a DIFFERENT set from
# _ADOPTABLE_TABLES above, and the two must stay separate. _ADOPTABLE_TABLES
# answers "is this the live pre-versioned ledger" (table presence only, at the
# instant BEFORE the migration loop runs — an adopted ledger legitimately has
# neither item_transitions nor operations yet, since both are created by
# migrations 4 and 5, which adoption falls through into immediately after).
# _VERSIONED_TABLES answers a different question — "does a database stamped
# at LEDGER_SCHEMA_VERSION actually have the columns that version declares" — and has
# to include every table ever added by a migration, or a later migration's
# table silently stops being checked. This closes docs/history/state-log.md §41 known-open item
# 5: item_transitions' shape was created by migration 4 but never asserted by
# _verify_columns(), because it only ever walked _ADOPTABLE_TABLES.
_VERSIONED_TABLES = _ADOPTABLE_TABLES + ("item_transitions", "operations")

# triage_items.state vocabulary, mirrored from scripts/loop/core.py's own STATE_*
# constants. ledger.py is the schema owner, so it is the home for the state names
# that OTHER files need without pulling in triage.py itself — chiefly api.py and
# warden.py's item transitions, which triage.py imports and so cannot import back.
STATE_NEW = "new"
STATE_TRIAGED = "triaged"
STATE_WORKING = "working"
STATE_MERGING = "merging"
STATE_VERIFYING = "verifying"
STATE_NEEDS_DECISION = "needs_decision"
STATE_FAILED = "failed"
STATE_FIXED = "fixed"
STATE_QUIET = "quiet"
STATE_CLOSED = "closed"
TERMINAL_STATES = (STATE_FIXED, STATE_QUIET, STATE_CLOSED)

# `closed` always carries one of these in triage_items.close_reason.
CLOSE_REASONS = ("duplicate", "fixed_by", "ignored", "resolved")


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


# Public alias — scripts/lifecycle/operations.py used to carry a
# byte-identical zero-arg copy of this; it now calls this one instead of
# hand-mirroring it. (scripts/lifecycle/items.py's and
# scripts/loop/core.py's own `_now_iso(now)` take an argument and are
# deliberately NOT this function — see the one-line comment on each.)
now_iso = _now_iso


def _stamp_version_sql(version: int, applied_at: str) -> str:
    """SQL text (no parameter binding — see migrate()'s docstring for why)
    that stamps schema_version's single row to `version`, inserting it if
    this is the very first stamp this database has ever received and
    updating it in place on every later one. `version` is our own int
    constant and `applied_at` is an isoformat() timestamp, neither ever
    attacker- or user-controlled, so string-formatting them into DDL/DML
    text here — the only way to fold this into the same executescript() as
    the surrounding CREATE statements, and therefore the same transaction —
    is safe."""
    return (
        f"UPDATE schema_version SET version = {version}, applied_at = '{applied_at}';\n"
        f"INSERT INTO schema_version (version, applied_at)\n"
        f"  SELECT {version}, '{applied_at}' WHERE NOT EXISTS (SELECT 1 FROM schema_version);\n"
    )


def _current_version(conn: sqlite3.Connection) -> int:
    """0 whenever schema_version does not exist yet — a fresh database and
    the live pre-versioned one look identical from here; migrate() is what
    tells them apart."""
    row = conn.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='schema_version'").fetchone()
    if row is None:
        return 0
    row = conn.execute("SELECT version FROM schema_version").fetchone()
    return int(row[0]) if row is not None else 0


def _tables_exist(conn: sqlite3.Connection, names: tuple[str, ...]) -> bool:
    present = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()}
    return all(name in present for name in names)


def _run_migrations(conn: sqlite3.Connection) -> None:
    """The single most dangerous function in this repo — read this before
    changing it.

    It has to serve two cases that look identical from `_current_version()`
    (both report 0) but must be handled in opposite ways:

    1. A genuinely fresh database — nothing exists yet. Run BASE_SCHEMA.
    2. THE LIVE LEDGER, adopted for the first time. `schema_version` has
       never existed because nothing before this module ever wrote one, but
       `events`, `cursors`, `dispatches` and
       `triage_items` are already there, already carrying the columns that
       used to arrive via a runtime ALTER TABLE, already holding real rows
       going back months. This is the NORMAL case this module was written
       for, not an edge case: warden's ledger did not appear out of nowhere,
       it is being handed a file four other processes have been writing to
       since before this module existed.

    Case 2 must NOT run BASE_SCHEMA. Even though every statement in it is
    `IF NOT EXISTS` and would therefore be a no-op against a table whose
    columns already match, "no-op today" is not the guarantee this function
    exists to make — the guarantee is that adopting a live database is a
    pure read (three queries: does schema_version exist, do the four tables
    exist, what version do we stamp) plus one small, well-understood write
    (create schema_version, stamp it to 1). Nothing here drops, recreates or
    rewrites a single one of the four business tables. Case 2 is detected
    and handled BEFORE the migration loop below ever runs, and returns
    immediately.

    Refuses loudly, before touching anything, if the database is already
    past LEDGER_SCHEMA_VERSION — that can only mean an older warden was pointed at
    a ledger a newer warden already migrated, and the only safe move is to
    stop, not to guess which of its own migrations might still apply.
    """
    conn.execute(_SCHEMA_VERSION_TABLE)
    current = _current_version(conn)

    if current > LEDGER_SCHEMA_VERSION:
        raise RuntimeError(
            f"ledger schema_version={current} is newer than this warden's LEDGER_SCHEMA_VERSION={LEDGER_SCHEMA_VERSION} — "
            "refusing to migrate. This means an older warden was started against a ledger a newer warden "
            "has already migrated forward; running an old migrator against it would guess, and guessing is "
            "how data gets destroyed. Upgrade this warden (or point it at a different database) before "
            "running it against this ledger again."
        )

    if current == 0 and _tables_exist(conn, _ADOPTABLE_TABLES):
        # Adopting the live, pre-versioned ledger — see this function's own
        # docstring, case 2. Stamp only; BASE_SCHEMA never runs.
        conn.executescript(f"BEGIN;\n{_stamp_version_sql(1, _now_iso())}COMMIT;\n")
        # ...and then FALL THROUGH to the migration loop below rather than
        # returning. Adoption establishes that this file is at version 1's
        # shape; it does not establish that it is at LEDGER_SCHEMA_VERSION. This
        # `return` used to be unconditional, and was harmless for exactly as
        # long as LEDGER_SCHEMA_VERSION stayed 1 — the moment it became 2, adopting a
        # pre-versioned ledger would stamp it 1, skip migration 2, and then fail
        # `_verify_columns()` on a column the migration it skipped would have
        # added. The path that does that is the ROLLBACK
        # (~/.warden-cutover-backup/ holds a pre-versioned ledger), so it would
        # have failed at exactly the moment it was needed.
        current = 1

    for version in range(current + 1, LEDGER_SCHEMA_VERSION + 1):
        ddl = MIGRATIONS[version]
        # executescript() commits any pending transaction before it runs and
        # performs no other implicit transaction control of its own (stdlib
        # sqlite3 docs) — so the BEGIN/COMMIT bracketing the whole script
        # text below is what actually makes "this version's DDL plus its
        # schema_version stamp" one transaction: a crash mid-script leaves
        # the database at its pre-migration version, never half-migrated.
        conn.executescript(f"BEGIN;\n{ddl}\n{_stamp_version_sql(version, _now_iso())}COMMIT;\n")

    _verify_columns(conn)


def _expected_columns() -> dict[str, set[str]]:
    """What the schema AT `LEDGER_SCHEMA_VERSION` says each table has, derived by
    replaying BASE_SCHEMA and then every migration up to it against an
    in-memory database, rather than by keeping a second hand-written list
    beside it. A hand-written list is a copy, and a copy of a schema is a
    thing that drifts from the schema."""
    mem = sqlite3.connect(":memory:")
    try:
        # BASE_SCHEMA is version 1's shape, so replaying it alone would describe
        # a schema this module stopped having the moment MIGRATIONS gained a
        # second entry — and `_verify_columns()` would then silently stop
        # checking every column added after version 1. Replay the migrations in
        # order instead, which is the same "derive it, never copy it" property
        # one version further on.
        for version in sorted(MIGRATIONS):
            if version <= LEDGER_SCHEMA_VERSION:
                mem.executescript(MIGRATIONS[version])
        return {
            t: {r[1] for r in mem.execute(f"PRAGMA table_info('{t}')")}
            for t in _VERSIONED_TABLES
        }
    finally:
        mem.close()


def _verify_columns(conn: sqlite3.Connection) -> None:
    """Refuse a database that is stamped but structurally incomplete.

    Adoption (case 2 in `_run_migrations`) decides on TABLE PRESENCE alone —
    all four tables exist, so this is the live ledger, stamp it and touch
    nothing. That is the right call for the ledger this module was written to
    adopt, whose columns were verified to match exactly. It is the wrong call
    for a database where the four tables exist but one of them predates a
    column that arrived through a runtime ALTER TABLE on some other machine:
    adoption would stamp it version 1, every later query would be reading a
    schema that is not there, and the failure would surface as a bare
    `no such column` from somewhere deep in the loop, hours later, with
    nothing connecting it back to this decision.

    So the stamp is checked against the schema it claims. Missing columns are
    fatal and named. EXTRA columns are not — a newer warden may have added
    one, and refusing to read a database that has more than we expect would
    turn a forward-compatible situation into an outage."""
    expected = _expected_columns()
    problems: list[str] = []
    for table, want in expected.items():
        have = {r[1] for r in conn.execute(f"PRAGMA table_info('{table}')")}
        missing = want - have
        if missing:
            problems.append(f"  {table}: missing {sorted(missing)}")
    if problems:
        raise RuntimeError(
            f"ledger is stamped schema_version={LEDGER_SCHEMA_VERSION} but does not match that schema:\n"
            + "\n".join(problems)
            + "\n\nThis is a database that carried all the expected TABLES (so it was adopted rather "
            "than created) while missing columns the schema declares. Adopting it further would "
            "produce 'no such column' from inside the loop instead of here. Fix the database, or "
            "point warden at a different one."
        )


def migrate(conn: sqlite3.Connection) -> None:
    """Public entry point for `_run_migrations()` — kept as a thin wrapper
    so `connect(migrate=True)` can call the real logic under a different
    name. (`connect()`'s own `migrate` keyword argument shadows this
    function's name inside `connect()`'s body; the split avoids relying on
    Python's scoping rules to route around that.)"""
    _run_migrations(conn)


def assert_schema_version(conn: sqlite3.Connection) -> None:
    """What every non-migrating process calls instead of migrate(). Raises
    with both version numbers named, so the fix is obvious from the error
    alone: either the migrating process (triage.py, the loop) has not run
    yet against this file, or this process is stale and needs upgrading."""
    version = _current_version(conn)
    if version != LEDGER_SCHEMA_VERSION:
        # The connection's own idea of its file, not the module-global DB_PATH —
        # a caller that passed an explicit `path=` (a test, a --db override) would
        # otherwise get an error naming the wrong file.
        db_file = conn.execute("PRAGMA database_list").fetchone()["file"]
        message = (
            f"warden ledger at {db_file} is at schema_version={version}, this process expects "
            f"LEDGER_SCHEMA_VERSION={LEDGER_SCHEMA_VERSION}. Only the loop (triage.py, via `connect(migrate=True)`) "
            "is allowed to migrate this file — run it at least once, or if this ledger has already been "
            "migrated past this process's version, upgrade this process before pointing it here."
        )
        if version < LEDGER_SCHEMA_VERSION:
            raise LedgerBehind(message)
        raise RuntimeError(message)


def connect(
    path: Path | str | None = None,
    *,
    readonly: bool = False,
    migrate: bool = False,
) -> sqlite3.Connection:
    """The one place anything in warden opens `warden.db`.

    `path` defaults to the module-global DB_PATH, re-read at call time so a
    test or a script's own `--db` override can rebind `ledger.DB_PATH` (or
    pass `path=` directly) and have it take effect on the very next call —
    see the DB_PATH comment above.

    `readonly=True` opens `file:<path>?mode=ro` and sets no pragma that
    writes — a dashboard or a diagnostic reader must never be able to open a
    writable handle by accident, and must never bring a database file into
    existence just by trying to read it.

    On a writable connection: WAL, an explicit busy_timeout, and
    synchronous=NORMAL, in that order (see the inline comments below for
    why each one, and why NORMAL specifically is correct under WAL).

    `migrate=True` runs the migrator (see `_run_migrations()` — only the
    loop's boot path should ever pass this). `migrate=False` (the default)
    calls `assert_schema_version()` and raises on any mismatch, INCLUDING
    "the file doesn't have a schema at all yet" — an assert-only process
    that races the migrator on a brand-new mini must fail loudly, not
    silently create its own tables the way the four old `db_connect()`s did.
    """
    p = Path(path).expanduser() if path is not None else DB_PATH

    if readonly:
        conn = sqlite3.connect(f"file:{p}?mode=ro", uri=True)
        conn.row_factory = sqlite3.Row
        return conn

    p.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(p)
    conn.row_factory = sqlite3.Row

    conn.execute("PRAGMA journal_mode=WAL")
    # sqlite3.connect(..., timeout=5.0) already sets busy_timeout=5000 as a
    # per-connection Python stdlib default — but that is true only by
    # accident of the driver, not by anything this file declares. A
    # non-Python writer of this database (a bare `sqlite3` CLI invocation, a
    # shell one-liner) gets no timeout at all under that accident. Setting
    # it explicitly here makes the 5s wait a property this module asserts on
    # every writable connection it hands out, not a side effect of which
    # client happened to open the file.
    conn.execute("PRAGMA busy_timeout=5000")
    # NORMAL, not FULL, and specifically because this connection is WAL:
    # under WAL, synchronous=NORMAL still guarantees the database can never
    # be corrupted by a crash or power loss — only that the last few
    # committed transactions might be lost, which then get re-derived on
    # this loop's very next 10-minute pass, not silently missed the way a
    # lost row in a one-shot system would be. FULL buys durability across
    # that crash window at the cost of an fsync on every single commit,
    # which is a guarantee a 10-minute polling loop does not need and a cost
    # every one of its six writers would otherwise pay on every write.
    conn.execute("PRAGMA synchronous=NORMAL")

    if migrate:
        _run_migrations(conn)
    else:
        assert_schema_version(conn)

    return conn


def apply_db_override(argv: list[str], set_db_path: Callable[[Path], None], *,
                       env_var: str | None = None) -> None:
    """--db PATH — lets a test or a `--dry-run` inspection point a caller's
    own module-level DB_PATH at a throwaway copy of the ledger without
    touching the real ~/.warden/warden.db. `set_db_path` rebinds the
    CALLER's own global; this function owns no `DB_PATH` of its own to
    assume — triage.py and dispatch-sweep.py each carry their own module
    global and must keep doing so. `env_var`, when given, is checked ONLY
    as a fallback for a bare `--db` with nothing after it (or no `--db` at
    all) — triage.py passes `HERMES_CC_DB` here; dispatch-sweep.py passes
    none, since its own WARDEN_DB override is already applied once at
    module load, before argv is ever parsed."""
    if "--db" in argv:
        idx = argv.index("--db")
        if idx + 1 < len(argv):
            set_db_path(Path(argv[idx + 1]).expanduser())
            return
    if env_var:
        env_override = os.environ.get(env_var)
        if env_override:
            set_db_path(Path(env_override).expanduser())


def snapshot(conn: sqlite3.Connection, dest: Path | str) -> Path:
    """`VACUUM INTO` a consistent, compacted copy of the currently-open
    database to `dest`. Refuses if `dest` already exists — SQLite's own
    behaviour, not ours, and deliberately not overridable here: a caller
    that wants to replace an old snapshot deletes it first, explicitly,
    rather than this function silently clobbering one."""
    dest_path = Path(dest).expanduser()
    conn.execute("VACUUM INTO ?", (str(dest_path),))
    return dest_path


# ── CLI ────────────────────────────────────────────────────────────────────────
#
# So a process that is NOT warden can prepare or check a ledger without carrying
# its own copy of the schema. That is not a convenience: the CLI owns two
# of the four tables and still writes them, and the only alternative to this door
# is the one that was there before — a second, unversioned migrator running
# `executescript` on every connect, which is the exact defect this module exists
# to remove. A shell script cannot import a Python module; it can run one.
#
#   python3 ledger.py --migrate <path>   create or adopt, then stamp. The loop's job;
#                                        also what a test fixture needs.
#   python3 ledger.py --check <path>     assert the version and print it. Exit 1 on
#                                        mismatch, so a caller can fail closed.
#   python3 ledger.py --version          print LEDGER_SCHEMA_VERSION and exit.

def _main(argv: list[str]) -> int:
    if "--version" in argv:
        print(LEDGER_SCHEMA_VERSION)
        return 0
    for flag in ("--migrate", "--check"):
        if flag in argv:
            i = argv.index(flag)
            if i + 1 >= len(argv):
                print(f"ledger: {flag} needs a path", file=sys.stderr)
                return 2
            path = Path(argv[i + 1]).expanduser()
            try:
                conn = connect(path, migrate=(flag == "--migrate"))
            except Exception as err:  # noqa: BLE001 — the message IS the output
                print(f"ledger: {err}", file=sys.stderr)
                return 1
            version = _current_version(conn)
            conn.close()
            print(version)
            return 0
    print(__doc__ or "", file=sys.stderr)
    return 2


if __name__ == "__main__":
    import sys as _sys
    _sys.exit(_main(_sys.argv[1:]))
