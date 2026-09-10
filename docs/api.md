# warden HTTP API

`scripts/api.py`, run as `com.jkrumm.warden-api` (`launchd/com.jkrumm.warden-api.plist.template`).
See that script's own module docstring for the full reasoning; this is the
reference for the endpoints and the six funnel numbers' exact definitions.

## Bind and auth

`127.0.0.1:7734`, loopback only, no bearer token. Per DESIGN.md § Security model,
an episode on this host runs unrestricted `Bash` under
`--dangerously-skip-permissions` — a bearer token would be theatre, not a
boundary, since anything that can read a token file can query this socket
directly. The real protections are the loopback bind (nothing off-box reaches it
at all) and the read-only handle (nothing that does reach it can write).

**Not built in this wave**, and deliberately: a Caddy or tailnet door, `/board`,
`/items/:id`, `POST /items/:id/intent`, `POST /items/:id/note`. DESIGN.md's real
HTTP API contract includes those; they belong with Argo (Wave 4), the only
intended remote consumer, and building them unconsumed now would be dead surface
with nothing to prove them correct against.

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
| 1 | `verdicts_recorded_disposition` | numerator/denominator (both **dispatch counts**) plus an `item_states` breakdown for investigate-tier dispatches with a recorded verdict AND `origin_event_id IS NOT NULL` (loop-originated — see below), reaching a state DESIGN.md counts as a recorded disposition (the implement chain, `needs_human`, or `dismissed`). `quiet` does **not** count — DESIGN.md principle 5, silence is never an outcome. `item_states` counts `triage_items` rows, **not** dispatches, so its total may legitimately exceed the denominator — one clustered dispatch joins several items (see `item_states_note`). Verdict-carrying investigate dispatches with `origin_event_id IS NULL` are interactive Slack dispatches a human asked `hermes-cc` to run directly; they never entered warden's funnel as an item and never will, so they're excluded from the ratio and reported separately as `excluded_interactive` (with `excluded_interactive_note`) rather than silently dropped. | all-time |
| 2 | `verified_fixes_vs_silence` | `fixed`/`quiet`/`closed`/`dismissed` counts and `fixed / (fixed + quiet)`, restricted to mapped signatures (`triage_items.repo IS NOT NULL`). | all-time |
| 3 | `median_needs_human_to_decision_hours` | median hours between a transition into `needs_human` and the item's next transition, **excluding** pairs whose exit is `dismissed` (that is the 7-day expiry clock, not a human deciding — REVIEW.md's C3 Goodhart concern). | windowed on entry |
| 4 | `verified_unattended_fixes_per_week` | **derivable as of schema 5** — an item that transitioned to `fixed` in the window counts as unattended unless one of its `operations` rows carries `authorized_by LIKE 'signed:%'` (see DESIGN.md § Crash recovery, STATE.md §46 Correction 1: the unattended door, `--auto-from-item`, structurally never produces a `dispatch_approvals` row, so `operations` — written on both doors — is the only table that can answer this). `fixed_in_window`/`unattended_in_window` are the raw counts; `value` is `unattended_in_window`. Still reads `null` today, for a different and correct reason than before: zero `fixed` transitions have ever occurred in production (STATE.md §46 Correction 3), so either the window predates `history_since` or there is simply nothing to count — the derivation becoming *possible* is what changed; the number moving needs the auto-implement chain to actually run. | windowed |
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

**`item_transitions` is history after the first state, not from creation.**
`ingest()` inserts a brand-new `triage_items` row directly with `state='new'`
rather than going through `_set_state()` (there is no prior state to
transition from), so an item's entry into `new` is never itself recorded as a
row here — the table's first entry for an item is always its departure from
`new` (or later).

## Why these six and not more

DESIGN.md § What "done" means names exactly these six as the metrics that
matter; this wave implements all six honestly rather than a subset dishonestly.
One of them (#6's `reverts`) still cannot be computed at all — no revert
primitive exists before `warden revert` (Wave 3). #4's "unattended" qualifier
*is* now derivable (schema 5's `operations` table) but still reads `null`
because production has never produced a `fixed` transition to evaluate — both
are served as `null` with a named reason rather than omitted, so a consumer
can render "not yet measurable" instead of silently missing a key.
