# warden HTTP API

`scripts/api.py`, run as `com.jkrumm.warden-api` (`launchd/com.jkrumm.warden-api.plist.template`,
`api.py --serve`). Four read-only GET endpoints: `/health`, `/metrics`, `/board`,
`/items/<event_id>`. Write-side actions are not here: Argo's owner actions are pulled by
the loop (`apply_argo_actions()`), see § Argo push.

## Bind and auth

`127.0.0.1:7735`, loopback only, no bearer token. An episode on this host runs
unrestricted `Bash` (DESIGN.md § Boundaries), so a token would be theatre: anything that can
read a token file can query the socket directly. The protections are the loopback bind
(nothing off-box reaches it; no Caddy or tailnet door) and the read-only handle (nothing
that does reach it can write).

## Contract

- `GET` only. Any other method is `405`, an unknown path `404`; both as JSON.
- Every request opens a **fresh read-only connection** (`ledger.connect(DB_PATH,
  readonly=True)`) and calls `ledger.assert_schema_version()` itself (the readonly branch
  of `connect()` does not). A mismatch or an unreachable file is a **`503`** with the reason
  in the body, never a `200` on untrustworthy data. A held handle would pin a WAL snapshot,
  and a boot-time schema check says nothing about the ledger ten minutes later.
- `/items/<event_id>`: a non-digit id is `400 {"error": "event_id must be an integer"}`, an
  id with no `triage_items` row is `404 {"error": "no item <id>"}`.

## `GET /health`

`ok`, `schema_version`, `schema_version_expected`, `db_path`, `db_mtime`, `checked_at`, and
`pollers.<name>` = `{age_minutes, last_run, threshold_minutes, ok}`. `ok` is true only when
the schema matches and every poller's heartbeat age is at most **3x its LaunchAgent's
`StartInterval`**; a poller that never recorded a heartbeat is not ok.

| Poller key | Cursor key | Interval | Stale threshold |
|-|-|-|-|
| `loop` | `triage_last_run` | 600s | 30 min |
| `watchdog_poll` | `watchdog_poll_last_run` | 1800s | 90 min |
| `dispatch_sweep` | `dispatch_sweep_last_run` | 300s | 15 min |

## `GET /metrics`

JSON, not Prometheus text, despite the path: the numbers are a funnel snapshot, not a time
series. Top level: `generated_at`, `window_days` (7, every windowed metric), `history_since`
(earliest `item_transitions.at`, `null` when empty), then the six metrics below.

Every leaf is `{"value", "unavailable", ...}`. **`value` is `null` only with a non-empty
`unavailable` reason, never a fabricated `0`.** `item_transitions` records nothing before
`history_since`, so a window starting earlier is empty by construction, not a measured zero.
That guard is applied at each leaf computed from `item_transitions` (metrics 3, 4 and
`reopen_after_fixed`): an empty table or a window predating `history_since` returns `null`
with a reason naming which.

| # | Key | Definition | Windowed |
|-|-|-|-|
| 1 | `verdicts_recorded_disposition` | `numerator / denominator` over **dispatches**: denominator is investigate-tier dispatches with a verdict and `origin_event_id IS NOT NULL` (loop-originated); numerator is those whose item (`triage_items.dispatch_job`) is in `merging`, `verifying`, `fixed`, `closed` (any `close_reason`), `needs_decision` or `failed`. `quiet` does not count: silence is never an outcome. `item_states` is a per-state breakdown of `triage_items` rows (not dispatches), so its total may exceed the denominator (`item_states_note`). Verdict-carrying investigate dispatches with `origin_event_id IS NULL` (interactive, never an item) are excluded and reported as `excluded_interactive` (+ `excluded_interactive_note`). Also `numerator`, `denominator`. `null` when the denominator is 0. | all-time |
| 2 | `verified_fixes_vs_silence` | `fixed`, `quiet`, `closed` counts over `triage_items WHERE repo IS NOT NULL` (mapped signatures); `value = fixed / (fixed + quiet)`, `null` when that is 0. | all-time |
| 3 | `median_needs_decision_to_decision_hours` | Median hours from each transition into `needs_decision` to the same item's next transition, for entries inside the window; open entries are skipped. Nothing expires a `needs_decision` item, so every exit is a decision. Also `pairs`. `null` when there are no pairs. | on entry |
| 4 | `verified_unattended_fixes_per_week` | Distinct `event_id`s with an `item_transitions.to_state='fixed'` row in the window (a reopened-then-refixed item counts once). `fixed_in_window` and `unattended_in_window` are the same count; `value` is `unattended_in_window`. `null` when there is none. | yes |
| 5 | `poller_ages` | `value` = worst `age_minutes` across pollers; `pollers.<name>` = `{age_minutes, last_run, threshold_minutes, stale, unavailable}` (same keys and thresholds as `/health`; a never-run poller is `stale: true`, age `null`). `null` when none has ever run. | no |
| 6 | `reverts_and_reopens` | Two leaves. `reopen_after_fixed`: `item_transitions` rows with `from_state='fixed'` in the window (history-guarded). `reverts`: `triage_items` with `revert_pr IS NOT NULL` and `updated_at` in the window; reads `triage_items` directly, so it is not history-guarded and `0` is a real measurement. | yes |

## `GET /board`

Every non-terminal `triage_items` row plus counts.

```json
{
  "generated_at": "...",
  "schema_version": 15,
  "counts": {"new": 0, "triaged": 0, "working": 1, "merging": 0,
             "verifying": 0, "needs_decision": 2, "failed": 0},
  "items": [{
    "event_id": 42, "origin": "alert", "repo": "warden", "state": "needs_decision",
    "close_reason": null, "strikes": 0, "retry_at": null, "max_tier": "implement",
    "title": "...", "note": null, "pr_url": null, "dispatch_job": "j-abc",
    "implement_job": null, "validation_job": null, "occurrences": 3,
    "revision_count": 0, "train_stage": null, "created_at": "...", "updated_at": "...",
    "origin_channel": null, "origin_thread_ts": null,
    "availableActions": ["implement", "dismiss", "reinvestigate", "note"],
    "issue": null
  }],
  "terminal_24h": 4,
  "awaiting_owner": [{
    "kind": "item", "event_id": 42, "repo": "warden", "title": "...",
    "state": "needs_decision", "pr_url": null, "age_days": 1.5, "reason": null,
    "revision_count": 0, "availableActions": ["implement", "dismiss", "reinvestigate", "note"]
  }],
  "truncated": true
}
```

- `counts` always carries all seven non-terminal states (`new`, `triaged`, `working`,
  `merging`, `verifying`, `needs_decision`, `failed`), zero when empty. Terminal states
  (`fixed`, `quiet`, `closed`) are not in `counts` or `items`.
- `items` is `ORDER BY updated_at DESC`, capped at 200; `truncated: true` appears only when
  the cap was hit (omitted otherwise, never `false`). `terminal_24h` counts terminal-state
  items with `updated_at` in the last 24 hours.
- `awaiting_owner`: every `needs_decision` and `failed` item (the two states only an owner
  action moves), oldest first by time in the current state (`age_days`, `null` if unknown);
  `reason` is the item's `note`.
- `availableActions` is what the UI offers, from the item's state, its PR and its revert
  status; `apply_argo_actions()` re-validates when an action is applied. `implement`:
  `needs_decision`, `failed`. `merge`: the same, plus a PR whose implement dispatch's
  `validation_status` is `confirmed`. Neither is offered once `revert_pr` is set. `dismiss`:
  `new`, `triaged`, `needs_decision`, `failed`, `quiet`. `reinvestigate`: `needs_decision`,
  `failed`, `quiet`. `note`: any non-terminal state.
- `issue` is `null` except for `origin: "github_issue"`, where it is `{repo, number, url,
  author, trusted, labels}` from the event's stored payload (`trusted` = `author ==
  clients.github.GH_OWNER`); a missing or unparsable payload gives `null`, not a `500`.

## `GET /items/<event_id>`

The full detail behind one item.

```json
{
  "item": {"...every triage_items column, plus": null,
           "brief": "(max 2000 chars)", "brief_truncated": false},
  "event": {"id": 42, "source": "...", "external_id": "...", "title": "...", "url": null,
            "first_seen": "...", "resolved_at": null, "payload": null,
            "reminder_count": 0, "last_reminder_at": null},
  "dispatches": [{"job_id": "j-abc", "tier": "implement", "repo": "warden", "status": "done",
                  "created_at": "...", "finished_at": "...", "reported_at": "...",
                  "merged_at": null, "artifact_url": null, "validation_job_id": null,
                  "validation_status": null, "delivery_status": "delivered",
                  "verdict": {"summary": "...", "nextAction": "...", "confidence": "...",
                              "recommendation": "...", "outcome": "...", "schemaVersion": 1}}],
  "operations": [{"op_id": "op-1", "event_id": 42, "kind": "implement", "repo": "warden",
                  "authorized_by": "...", "started_at": "...", "outcome": "ok",
                  "outcome_at": "...", "receipt_json": null, "reconciled_at": null,
                  "note": null}],
  "operations_total": 1,
  "transitions": [{"id": 100, "event_id": 42, "from_state": null, "to_state": "new",
                   "at": "...", "note": "created"}],
  "transitions_total": 1
}
```

- `item` is `SELECT *` from `triage_items`; only `brief` is cut, to 2000 characters, with
  `brief_truncated` always present. `event.payload` is the parsed `payload_json`.
  `event.reminder_count` / `last_reminder_at` are the `events` columns (`item` has no
  reminder columns).
- `dispatches`: rows whose `origin_event_id` is this event or whose `job_id` is the item's
  `dispatch_job` / `implement_job` / `validation_job`, by `created_at`. `verdict` is `null`
  without `verdict_json`, else the six fields shown. `stdin_text` and `context_text` are
  never returned.
- `operations` (by `started_at`) and `transitions` (by `id`) are oldest first. The `*_total`
  fields are the true row counts.
- Each item's creation writes a `from_state=NULL`, `note='created'` transition. A legacy item
  without one gets a synthetic first entry (`id: null`, `at` = `item.created_at`,
  `to_state` = the first recorded transition's `from_state`, else the current state,
  `synthetic: true`), only when the list is not truncated.
- `item_payload(conn, id, history_limit=N)` (Python only, not a query parameter) keeps just
  the newest N `transitions` and `operations`, still oldest-first. The HTTP endpoint passes
  no limit.

## Argo push

Argo (the dashboard on the VPS) cannot reach this loopback socket, so the loop pushes its own
projection. `triage.py`'s `run()` ends every 10-minute pass (after `record_heartbeat()` and
`apply_argo_actions()`) with `push_argo_snapshot()`, which POSTs `build_argo_snapshot()` via
`clients.argo.push_snapshot()`:

`machine` (`WARDEN_MACHINE`, default `mini`), `generatedAt`, `health`, `metrics`, `board`
(this module's payloads verbatim), `items` (`item_payload()` for the first
`ARGO_SNAPSHOT_ITEMS_CAP = 50` board items, keyed by `event_id` as a string, each with
`history_limit=ARGO_SNAPSHOT_HISTORY_LIMIT = 50`), and `itemsTruncated`.

Each push logs one line to `warden-loop.err`, `triage: argo push — <status> (<bytes> bytes,
<n> items)`, and never raises; the pass finishes regardless. `--dry-run` builds and sizes the
snapshot but sends nothing.

| Status | Meaning |
|-|-|
| `ok` | 2xx from Argo. |
| `no-secret` | `ARGO_API_SECRET` / `op://common/api/SECRET` did not resolve; nothing sent. Re-seed the secrets cache. |
| `too-large` | Encoded body over `clients.argo.MAX_BODY_BYTES` (1,000,000); nothing sent. A cap above is not holding. |
| `encode-error` | Body not JSON-serializable; nothing sent. |
| `http-error:<code>` | Non-2xx from Argo. Check the Argo side. |
| `network-error` | Unreachable host, timeout, or other transport failure. |
| `build-failed` | Building or encoding the snapshot raised (line reads `build failed: <exception>`). |
| `client-error` | `push_snapshot()` itself raised (line reads `client raised: <exception>`); its contract is never-raise, so this is a regression. |
| `dry-run` | `--dry-run`; line reads `dry-run, would push <bytes> bytes (<n> items)`. |

Argo is a projection of this snapshot, never a second source of truth; the ledger is.
