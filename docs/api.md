# warden HTTP API

`scripts/api.py`, run as `com.jkrumm.warden-api` (`launchd/com.jkrumm.warden-api.plist.template`).
See that script's own module docstring for the full reasoning; this is the
reference for the endpoints and the six funnel numbers' exact definitions.

## Bind and auth

`127.0.0.1:7735`, loopback only, no bearer token. Per DESIGN.md § Security model,
an episode on this host runs unrestricted `Bash` under
`--dangerously-skip-permissions` — a bearer token would be theatre, not a
boundary, since anything that can read a token file can query this socket
directly. The real protections are the loopback bind (nothing off-box reaches it
at all) and the read-only handle (nothing that does reach it can write).

**Not built yet**, and deliberately: a Caddy or tailnet door, `POST
/items/:id/intent`, `POST /items/:id/note`. DESIGN.md's real HTTP API contract
includes those; they belong with Argo (Wave 4), the only intended remote
consumer, and building them unconsumed now would be dead surface with nothing to
prove them correct against. `/board` and `/items/<event_id>` (Wave 6.3) are
built — see below.

## Endpoints

`GET` only — any other method is `405`. An unknown path is `404`. Both as JSON.

Every request opens a **fresh read-only connection** to the ledger
(`ledger.connect(DB_PATH, readonly=True)`) and explicitly asserts its schema
version before running the query — the readonly branch of `ledger.connect()`
does **not** call `assert_schema_version()` itself, so this module does. A
mismatch (or an unreachable file) is a **503** with the reason in the body,
never a 200 built on stale or untrustworthy data. This is a control plane; a
metrics endpoint that lies about its own liveness is the exact failure mode the
rest of this repo exists to remove.

A connection is opened per request, not held for the process lifetime, for two
reasons: a WAL reader pinned at startup can keep serving the snapshot from the
moment it was opened, and a schema check made once at boot says nothing about
the ledger ten minutes later.

### `GET /health`

Schema version (actual vs. expected), the ledger file's path and mtime, each
poller's heartbeat age against its own named threshold, and one `ok` boolean.

`ok` is `false` when the schema assertion fails (though that already produces a
503 for the whole request — see above) or any poller's heartbeat is older than
**3× its own LaunchAgent's `StartInterval`**:

| Poller | Cursor key | Interval | Stale threshold |
|-|-|-|-|
| `com.jkrumm.warden-loop` | `triage_last_run` | 600s | 30 min |
| `com.jkrumm.warden-poll` | `watchdog_poll_last_run` | 1800s | 90 min |
| `com.jkrumm.warden-sweep` | `dispatch_sweep_last_run` | 300s | 15 min |

### `GET /metrics`

JSON, not Prometheus text format, despite the name — matching DESIGN.md's own
naming (§ Observability: "Funnel metrics — the six numbers above, at `/metrics`,
rendered in Argo"). The numbers are a funnel snapshot, not a time series a
scrape-and-store system would want.

Every leaf metric shares one shape: `{"value": ..., "unavailable": ...}` plus
whatever extra fields explain it. **`value` is `null` only when paired with a
non-empty `unavailable` reason — never a fabricated `0`.** A gap in the ledger
and a measured zero are different facts, and this API refuses to blur them.

Top-level fields: `generated_at`, `window_days` (7, for every windowed metric),
`history_since` — the earliest `item_transitions.at`, or `null` if that table is
empty. **`item_transitions` was created empty by schema migration 4 and records
nothing retroactively.** Any window whose start predates `history_since` is
empty *by construction*, not a measured zero — this field is what lets a
consumer (Argo, a human reading the JSON) tell the two apart.

| # | Key | Definition | Windowed |
|-|-|-|-|
| 1 | `verdicts_recorded_disposition` | numerator/denominator (both **dispatch counts**) plus an `item_states` breakdown for investigate-tier dispatches with a recorded verdict AND `origin_event_id IS NOT NULL` (loop-originated — see below), reaching a state DESIGN.md counts as a recorded disposition (the implement chain, `needs_human`, or `dismissed`). `quiet` does **not** count — DESIGN.md principle 5, silence is never an outcome. `item_states` counts `triage_items` rows, **not** dispatches, so its total may legitimately exceed the denominator — one clustered dispatch joins several items (see `item_states_note`). Verdict-carrying investigate dispatches with `origin_event_id IS NULL` are interactive Slack dispatches a human asked `warden` to run directly; they never entered warden's funnel as an item and never will, so they're excluded from the ratio and reported separately as `excluded_interactive` (with `excluded_interactive_note`) rather than silently dropped. | all-time |
| 2 | `verified_fixes_vs_silence` | `fixed`/`quiet`/`closed`/`dismissed` counts and `fixed / (fixed + quiet)`, restricted to mapped signatures (`triage_items.repo IS NOT NULL`). | all-time |
| 3 | `median_needs_human_to_decision_hours` | median hours between a transition into `needs_human` and the item's next transition, **excluding** pairs whose exit is `dismissed` (that is the 7-day expiry clock, not a human deciding — REVIEW.md's C3 Goodhart concern). | windowed on entry |
| 4 | `verified_unattended_fixes_per_week` | **derivable as of schema 5** — an item that transitioned to `fixed` in the window counts as unattended unless one of its `operations` rows carries `authorized_by LIKE 'signed:%'` (see DESIGN.md § Crash recovery, docs/history/state-log.md §46 Correction 1: the unattended door, `--auto-from-item`, structurally never produces a `dispatch_approvals` row, so `operations` — written on both doors — is the only table that can answer this). `fixed_in_window`/`unattended_in_window` are the raw counts; `value` is `unattended_in_window`. Still reads `null` today, for a different and correct reason than before: zero `fixed` transitions have ever occurred in production (docs/history/state-log.md §46 Correction 3), so either the window predates `history_since` or there is simply nothing to count — the derivation becoming *possible* is what changed; the number moving needs the auto-implement chain to actually run. | windowed |
| 5 | `poller_ages` | per-poller age in minutes plus the worst case across all three, named individually. | current (not windowed) |
| 6 | `reverts_and_reopens` | `reopen_after_fixed` (real count, transitions with `from_state='fixed'`) and `reverts` (always `null` — no revert primitive exists before `warden revert`, DESIGN.md § Abort and revert, Wave 3). | windowed |

**Leaf-level history guard.** Metrics 3, 4 (`fixed_in_window`) and 6
(`reopen_after_fixed`) are all computed from `item_transitions`, which was
created empty by schema migration 4. Each checks `history_since` against its
own window start *at the leaf*, not just at the top level: an empty table, or
a window whose start predates `history_since`, returns `null` with a reason
naming which case applied — never a fabricated `0`. This means all three
currently read `null` and will keep doing so until the window no longer
predates `history_since` (7 days after the earliest row lands).

**`reverts` (metric 6) is a real windowed count, not `null`.** `warden revert`
(Wave 5.3) gave it a primitive: `triage_items.revert_pr IS NOT NULL` OR
`state = 'reverted'` (`ledger.STATE_REVERTED`), counted within the 7-day
window on `updated_at` — an OR of both signals because a row can carry either
independently depending on exactly when the sweep observed it. Unlike metrics
3/4/6's `reopen_after_fixed`, this is **not** gated by the `item_transitions`
history guard: it reads `triage_items` directly, which has existed since
schema version 1, so `0` here is a real measurement from the moment this
column existed (schema 7), never a stand-in for "not tracked".

### `GET /board`

A funnel-snapshot board: every non-terminal `triage_items` row, plus counts.
Same fresh-read-only-connection, 503-on-schema-mismatch contract as above.

```json
{
  "generated_at": "2026-09-11T00:00:00+00:00",
  "schema_version": 9,
  "counts": {
    "new": 0, "investigating": 1, "verdict": 0, "implementing": 0,
    "validating": 0, "merged": 0, "liveness_pending": 0, "needs_human": 2,
    "merge_blocked": 0, "split": 0
  },
  "items": [ {
    "event_id": 42, "origin": "alert", "repo": "warden", "state": "needs_human",
    "state_deadline": "2026-09-18T00:00:00+00:00", "max_tier": "implement",
    "title": "watchdog: sideclaw dispatch stuck", "note": null,
    "pr_url": null, "dispatch_job": "j-abc", "implement_job": null,
    "validation_job": null, "occurrences": 3,
    "created_at": "2026-09-10T00:00:00+00:00", "updated_at": "2026-09-11T00:00:00+00:00",
    "origin_channel": null, "origin_thread_ts": null
  } ],
  "terminal_24h": 4,
  "truncated": true
}
```

`counts` always carries a zero for each of the ten non-terminal chain states
(`new`, `investigating`, `verdict`, `implementing`, `validating`, `merged`,
`liveness_pending`, `needs_human`, `merge_blocked`, `split` — DESIGN.md's own
lifecycle order) even when nothing is in it, so a reader always sees the whole
shape; any *other* non-terminal state actually present in the ledger (e.g.
`snoozed`) still appears in `counts`, just without a guaranteed zero when
absent. `items` holds only non-terminal rows, `ORDER BY updated_at DESC`,
capped at 200 — `truncated: true` appears only when the cap was hit (omitted
otherwise, never a fabricated `false`). `origin_channel`/`origin_thread_ts`
read `null` on a row from before the migration that added them, rather than
raising. `terminal_24h` counts items whose state is terminal (`ledger.
TERMINAL_STATES`) and `updated_at` is within the last 24 hours.

```bash
curl -s http://127.0.0.1:7735/board | jq .
```

### `GET /items/<event_id>`

The full detail behind one item: itself, its parent event, and every
dispatch/operation/approval/transition that names it. `<event_id>` must be
all-digits — `GET /items/abc` is `400 {"error": "event_id must be an
integer"}`; an id with no matching row is `404 {"error": "no item <id>"}`.
`/items/` is a path-prefix match ahead of the exact-path table, so
`/itemsx` correctly falls through to the generic 404 rather than being
captured by it.

```json
{
  "item": { "...every triage_items column, as stored, plus": null,
            "brief": "(truncated to 2000 chars)", "brief_truncated": false },
  "event": { "id": 42, "source": "slack_alert", "external_id": "...",
             "title": "...", "url": null, "first_seen": "...",
             "resolved_at": null, "payload": null,
             "reminder_count": 0, "last_reminder_at": null },
  "dispatches": [ {
    "job_id": "j-abc", "tier": "implement", "repo": "warden", "status": "done",
    "created_at": "...", "finished_at": "...", "reported_at": "...",
    "merged_at": null, "artifact_url": null, "validation_job_id": null,
    "validation_status": null, "delivery_status": "delivered",
    "verdict": { "summary": "...", "nextAction": "implement", "confidence": "high",
                 "recommendation": "...", "outcome": "...", "schemaVersion": 1 }
  } ],
  "operations": [ { "op_id": "op-1", "event_id": 42, "kind": "implement",
                     "repo": "warden", "authorized_by": "auto-from-item",
                     "started_at": "...", "outcome": "ok", "outcome_at": "...",
                     "receipt_json": null, "reconciled_at": null, "note": null } ],
  "approvals": [ { "id": 7, "verb": "implement", "repo": "warden", "tier": "implement",
                    "created_at": "...", "expires_at": "...", "decided_at": null,
                    "decision": null, "decided_by": null, "spent_at": null,
                    "spent_job_id": null, "spend_error": null } ],
  "transitions": [ { "id": 100, "event_id": 42, "from_state": "new",
                      "to_state": "investigating", "at": "...", "note": null } ],
  "transitions_total": 1,
  "operations_total": 1
}
```

`item` is every `triage_items` column as stored, except `brief` — the
human's own text (`origin='human'`) — which is truncated to 2000 characters
with `brief_truncated: true` when that happened (`false` otherwise, never
omitted). `dispatches` matches every row whose `origin_event_id` equals this
event, OR whose `job_id` is one of the item's own `dispatch_job` /
`implement_job` / `validation_job`, `ORDER BY created_at`; `verdict` is
`null` when `verdict_json` is `null`, otherwise the six named fields parsed
out of it. `operations` and `transitions` key off `event_id` directly
(`item_transitions` oldest-first). `dispatch_approvals` has no `event_id`
column at all — an approval links to an item only through
`params_json.origin_event_id` (the closed parameter dict the spend replays,
schema 7) — so `approvals` is derived from that, keyed by SQLite's own
`rowid` as a stable, non-secret `id` since the table's real primary key
(`nonce`) is never returned. `stdin_text`/`context_text` are never returned
either — see this file's own secrets note above.

`event.reminder_count`/`event.last_reminder_at` are watchdog-poll.py's
grouped-source alert reminders — how many times, and when most recently,
Slack was reminded about this alert recurring — and are a different counter
from `item.reminder_count`/`item.last_reminder_at` (schema 9), which count
the `needs_human`/`merge_blocked` "still waiting on you" reminders instead.
Same shape, deliberately different tables/columns: see `scripts/ledger.py`'s
migration 9 comment for why reusing one counter for both would answer two
unrelated questions with one number.

```bash
curl -s http://127.0.0.1:7735/items/42 | jq .
```

**Every creation site now writes its own `created` transition.** The two
`INSERT INTO triage_items` sites (`ingest()`, `open_origin_item()`) call
`_record_created_transition()` right after the insert, appending
`from_state=NULL, to_state=<the state it was created into>, note='created'`
— this is the one write of `item_transitions` outside `_set_state()`, because
a raw `INSERT` (unlike every later move) never goes through it. A **legacy**
item predating this — every item created before this endpoint's field
addition — has no such row; `item_payload()` detects that (no transition in
the (unbounded) returned list has `from_state IS NULL`) and prepends a
synthetic one instead: `{"id": null, "event_id": ..., "from_state": null,
"to_state": <the first recorded transition's from_state, or item.state if it
has none>, "at": item.created_at, "note": "created", "synthetic": true}`.
This synthetic entry is only added when the returned history is NOT
truncated by `history_limit` (below) — a truncated list is missing its own
oldest rows, so a genuine `from_state IS NULL` row further back cannot be
ruled out.

**`history_limit`.** `item_payload()` (Python call only, not a query
parameter on this HTTP endpoint) accepts an optional `history_limit: int`
that keeps only the newest N `transitions` and the newest N `operations`,
still returned oldest-first. `transitions_total`/`operations_total` are
always present at the top level — the true row counts, independent of any
limit — so a caller can tell a full history from a truncated one. `GET
/items/<event_id>` itself calls `item_payload()` with no limit and stays
fully unbounded; only the embedded per-item detail in the Argo snapshot
(below) is bounded, at `ARGO_SNAPSHOT_HISTORY_LIMIT = 50`, because that
detail is re-embedded on every 10-minute tick and an item with a flapping
state otherwise grows the snapshot unbounded (item 543: ~780B/tick, 232KB
before this cap).

## Why these six and not more

DESIGN.md § What "done" means names exactly these six as the metrics that
matter; this wave implements all six honestly rather than a subset dishonestly.
One of them (#6's `reverts`) still cannot be computed at all — no revert
primitive exists before `warden revert` (Wave 3). #4's "unattended" qualifier
*is* now derivable (schema 5's `operations` table) but still reads `null`
because production has never produced a `fixed` transition to evaluate — both
are served as `null` with a named reason rather than omitted, so a consumer
can render "not yet measurable" instead of silently missing a key.

## Argo push

This endpoint is pull-only and loopback-bound, so Argo (the dashboard on the
VPS) cannot reach it at all — there is no Caddy or tailnet door onto it, by
design (see § Bind and auth above). Instead, `scripts/triage.py`'s loop pushes
its own projection to Argo, over `scripts/clients/argo.py`'s `push_snapshot()`,
as the last step of every 10-minute pass (`push_argo_snapshot()`, after
`record_heartbeat()`).

The pushed payload is `build_argo_snapshot()`'s output: `machine`,
`generatedAt`, and this same module's `health_payload()`/`metrics_payload()`/
`board_payload()` verbatim, plus `budget` (the same object `warden run`/`dispatch`/`list`
report under `budget`), `items` (`item_payload()` detail for the first 50 board items,
keyed by event_id as a string, each bounded to its newest 50 transitions/operations via
`history_limit=ARGO_SNAPSHOT_HISTORY_LIMIT` — see `GET /items/<event_id>`'s own
`history_limit` note above) with `itemsTruncated` alongside it, and
`intents` (spooled-intent state from `~/.warden/intents`: full `pending`/
`rejected` counts, but at most 20 per-status file entries — `entriesTruncated`
says whether either status is currently over that cap. Because `rejected/`
files are never deleted, this cap is what stops the intents section from
growing the whole snapshot past `clients.argo.MAX_BODY_BYTES` and silently
taking health/metrics/board down with it. An entry never carries `signature`/
`nonce` (the fields that carry authority in an `approval_decision` intent),
and a rejected entry carries `has_error: bool` only — never its `.err`
sibling's text, which can itself embed the raw rejected signature/nonce).

Every push logs exactly one line to `warden-loop.err`. Every status the line
can carry, and what an operator does about it:

| Status | Meaning | Operator action |
|-|-|-|
| `ok` | 2xx from Argo. | None — this is the steady state. |
| `no-secret` | `ARGO_API_SECRET`/`op://common/api/SECRET` did not resolve; nothing was sent. | Re-seed the secrets cache (`make secrets-seed` in dotfiles, biometric, MacBook-only). |
| `too-large` | The encoded snapshot exceeds `clients.argo.MAX_BODY_BYTES` (1 MB); nothing was sent. | Check `board`/`items`/`intents` sizing — one of the caps above is not holding. |
| `encode-error` | The snapshot was not JSON-serializable; nothing was sent. | A bug in a builder — check the most recent code change to `build_argo_snapshot()`. |
| `http-error:<code>` | Argo answered with a non-2xx. `404` is expected and a non-event until `POST /warden/snapshot` deploys on the Argo side. | `404`: none, this is expected pre-deploy. Any other code: check the Argo side. |
| `network-error` | Unreachable host, timeout, or any other transport failure. | Check Argo's own health/connectivity from the mini. |
| `build-failed` | Building or JSON-encoding the snapshot raised. | A bug — check the stderr line's own exception text, then the most recent code change. |
| `client-error` | `clients.argo.push_snapshot()` itself raised, defensively caught (its own contract is never-raise). | A bug in the client — this should never happen; treat as a regression. |
| `dry-run` | `--dry-run`: nothing was built into a real request or sent. | None — this is the expected `--dry-run` line. |

```
triage: argo push — ok (5054 bytes, 1 items)
triage: argo push — http-error:404 (3801 bytes, 0 items)
triage: argo push — dry-run, would push 5049 bytes (1 items)
```

A bug building or sending the snapshot is always caught and logged, never
raised — the loop finishes its pass regardless. `--dry-run` never pushes at
all (nor does it build a real network request) — see triage.py's own
DRY-RUN CONTRACT.

Argo is a **projection** of this pushed snapshot, never a second source of
truth — the ledger (`~/.warden/warden.db`) remains the only one.
