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
| 1 | `verdicts_recorded_disposition` | numerator/denominator/`states` breakdown for investigate-tier dispatches with a recorded verdict, reaching a state DESIGN.md counts as a recorded disposition (the implement chain, `needs_human`, or `dismissed`). `quiet` does **not** count — DESIGN.md principle 5, silence is never an outcome. | all-time |
| 2 | `verified_fixes_vs_silence` | `fixed`/`quiet`/`closed`/`dismissed` counts and `fixed / (fixed + quiet)`, restricted to mapped signatures (`triage_items.repo IS NOT NULL`). | all-time |
| 3 | `median_needs_human_to_decision_hours` | median hours between a transition into `needs_human` and the item's next transition, **excluding** pairs whose exit is `dismissed` (that is the 7-day expiry clock, not a human deciding — REVIEW.md's C3 Goodhart concern). | windowed on entry |
| 4 | `verified_unattended_fixes_per_week` | always `null` — the "unattended" qualifier needs an operation id linking an approval to the item it fixed (DESIGN.md § Crash recovery), which does not exist before Wave 3. Serves `fixes_in_window` (raw `fixed` transition count, explicitly **not** attendance-filtered) and `approvals_spent_in_window` as the closest available context. | windowed |
| 5 | `poller_ages` | per-poller age in minutes plus the worst case across all three, named individually. | current (not windowed) |
| 6 | `reverts_and_reopens` | `reopen_after_fixed` (real count, transitions with `from_state='fixed'`) and `reverts` (always `null` — no revert primitive exists before `warden revert`, DESIGN.md § Abort and revert, Wave 3). | windowed |

## Why these six and not more

DESIGN.md § What "done" means names exactly these six as the metrics that
matter; this wave implements all six honestly rather than a subset dishonestly.
Two of them (#4's "unattended" qualifier, #6's `reverts`) cannot be computed
today without ledger primitives that are explicitly Wave 3 work — they are
served as `null` with a named reason rather than omitted, so a consumer can
render "not yet measurable" instead of silently missing a key.
