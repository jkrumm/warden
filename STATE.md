# STATE — warden

**Read this before anything else. It is the memory; the conversation is not.**
Authority order: `DESIGN.md` → `FLOWS.md` → `REVIEW.md` → this file →
`docs/history/state-log.md`.

| | |
|-|-|
| Last updated | 2026-09-12 (§66) |
| Current wave | Estate chain Wave 8 done (§57); field look §58; autonomy §59. Wave 9, the field review, is the owner's to start — authority `~/SourceRoot/dotfiles/docs/waves/PLAN.md` |
| Repo state | `master`, five LaunchAgents on the mini |
| Ledger | `~/.warden/warden.db`, schema 10 |
| Tests | `tests/test_triage.py` 248/248 is the gate; `make test` runs all suites |
| Next action | see § Next action (bottom) |

---

## What is live

Five LaunchAgents run the whole control plane; no `hermes cron` job is in the
loop.

| Agent | Runs | Interval |
|-|-|-|
| `com.jkrumm.warden-loop` | `scripts/triage.py --run` | 600s |
| `com.jkrumm.warden-poll` | ingest | 1800s |
| `com.jkrumm.warden-sweep` | `scripts/dispatch-sweep.py` | 300s |
| `com.jkrumm.warden-backup` | `scripts/warden-backup.sh` | daily 03:10 |
| `com.jkrumm.warden-api` | `scripts/api.py --serve` (GET /metrics, /health) on `127.0.0.1:7735` | long-running, `KeepAlive` |

- The `warden` CLI (`run`, `dispatch`, `status`, `list`, `merge`, `abort`,
  `revert`) replaces the old bash verbs; the loop calls `scripts/lifecycle/`
  as functions, not shell scripts.
- Every origin opens an item. `triage_items.origin` is one of `alert` (the
  pollers), `github_issue` (the staleness poller and the `warden:go` label,
  event source `github_go`), or `human` (`warden run`, typed in herdr or
  through Hermes's door with `--origin-channel`/`--origin-thread`). See
  `docs/history/state-log.md` §55.
- Automatic investigate/implement dispatches pass `AUTO_DISPATCH_MODEL`
  (`glm-5.3-flash`, env `TRIAGE_AUTO_DISPATCH_MODEL`) and run on the IU
  backend, never Max. Validation is a sideclaw `review` job on the pull
  request; `TRIAGE_VALIDATION_DISPATCH_MODEL` can re-point it but defaults to
  `None`, i.e. sideclaw's JUDGE route — review is the one tool where the cheap
  tier has been measured failing (§64).
- `needs_human` / `merge_blocked` cards carry an `Action required` section:
  `Do this: <note>` plus a day-granularity auto-dismiss countdown.
- `push_argo_snapshot()` runs as step 10 of `run()`, after every tick; argo
  PR #19 merged 2026-09-11 (62d9633), the first `argo push — ok` landed at
  11:06Z, the `/warden` board is live.
- `maybe_auto_remediate()` runs between `run_verbs()` and
  `maybe_auto_implement()`: a `hostVerbs` policy match plus a folded verdict
  at or above `hostVerbMinConfidence` (medium) runs one `HOST_VERB_ALLOWLIST`
  argv per verb per pass, cooldown and attempt cap keyed per verb, receipt in
  `operations` (`kind=host`), then `liveness_pending` verified by
  `kuma-push-fresh`. First real run 11:36Z: `restart-hermes-gateway`, three
  items discharged (§59).
- sideclaw's per-tool routing lives in `server/lib/routing.ts`, not its
  `.env` (§64): `dispatch` on `glm-5.3-flash`/IU (the `AGENT` tier), `review`
  and `otel` held on Sonnet/Max (`JUDGE`), `narrative`/`excalidraw` on
  Sonnet/Max (`PROSE`). Live table: `GET /api/routing`.
- A sideclaw worker is killed by an idle watchdog (5 min with no stdout) plus
  a 60 min ceiling, not one wall-clock timer — a slow glm episode is not a
  wedged one (§64). A dispatch that ends terminal with no verdict folds to
  `needs_human` carrying `dispatches.error`, never into `verdict`.
- **Workers have no turn limit and no wall-clock ceiling** (sideclaw
  `8459357`, §66): the idle watchdog (no stdout for 5 min) is the only kill
  rule. Slow is not stuck. Lifecycle deadlines on items (verdict 24 h,
  needs_human 7 d) are a different fact and stay.
- sideclaw's runner reads the result envelope before stderr on a non-zero
  exit (`6a9325c`, §65); the CLI's `unrecognized_model … generate_session_title`
  stderr line is benign noise on every gateway model and is stripped from
  constructed errors.
- `warden close <event-id> --why` resolves an open item from the terminal
  (§66); in-flight states refuse, `abort` is their verb. Run it with
  `env -u CLAUDECODE` from inside a session.
- `make check-routing` is the third drift check next to `check-schemas` and
  `check-policy`: warden's dispatch/validation model pins against sideclaw's
  live `GET /api/routing` (§66).
- A ledger stamped *behind* the process (the window between a schema bump
  landing and the loop's next tick) raises `ledger.LedgerBehind`; poll and
  sweep skip the pass with one stderr line and exit 0, the poll still pushing
  its Kuma heartbeat (§65). A ledger *ahead* of the process stays a loud
  `RuntimeError`.
- Slack delivery is a plain HTTP client (`chat.postMessage`/`chat.update`),
  never the gateway's live `slack_bolt` connection.
- Cards, receipts and reminders post under warden's own Slack app, `warden`
  (app `A0C13NMFLD9`, bot user `U0C15C9QZFX`), live since 2026-09-11:
  `resolve_slack_token()` resolves `op://common/slack/WARDEN_BOT_TOKEN` from
  the headless cache; the Hermes fallback exists only for a missing cache.
  The read path (`#alerts`/`#updates` history in `watchdog-poll.py`) stays
  pinned to Hermes's token (`resolve_alerts_read_token()`) — a
  `chat:write`-only app cannot read.
- The `#agents` overview digest is retired.
- sideclaw's published verdict schemas are pinned in
  `scripts/clients/sideclaw.py` (`DISPATCH_SCHEMA_VERSION=2`,
  `REVIEW_SCHEMA_VERSION=1`) and checked by `make check-schemas`.

## Open — owner actions

- The §55 4.3 "human types in Slack" acceptance is still open.
- `warden-api`'s "LAST EXIT -15" is the §55 kickstart; cosmetic.

## Carried debt

- `triage.py` stays one 6.6k-line file: 248 tests patch its globals by
  name, a split buys no behaviour (§66). Dead code: none (AST-verified, §65).
- `warden.py` still carries its own `_secrets_run_path` copy (left out of the
  §66 consolidation because the `close` verb landed in the same file at the
  same time).
- sideclaw `96c917a`: a dispatch killed mid-episode is resumed on the next
  boot (`jobs.session_id` + kept worktree, `--resume`, attempt cap 2), and
  the self-drain waits for running jobs with no wall-clock cap. Unmeasured
  in the field; the vendored CLI notes warn a mid-tool-call kill can resume
  a corrupted transcript — the cap and the salvage bundle bound it.
- sideclaw `fallow` fails at HEAD — pre-existing, not this repo's.
- Cost per Warden item is a usage-tracker query on `sub_tool` now that
  sideclaw tags every session (`sideclaw:<tool>`, §60); the ledger join is
  still not built.
- `#agents` is warden-only since 2026-09-11 (Hermes's narratives cron moved to
  `#hermes`); approval buttons still post to `#hermes`.
- No ledger restore path yet. The repo itself has no remote; since §62 the
  daily backup ships a `git bundle` of every ref next to the ledger snapshots.

## History

- Wave 0 — `docs/history/state-log.md` §§13–25
- Wave 1 — §§26–36
- Wave 2 — §§37–43
- Wave 3 — §§44–51
- Estate chain Wave 4 — §§52–53
- Wave 5 — §54
- Wave 6 — §55
- Wave 7 — §56
- Wave 8 — §57
- First field look — §58
- Autonomy: host verbs, Argo live, GLM routing — §59
- Closing the queue: reminders, the heartbeat probe, Kuma sync — §60
- Warden's own Slack identity — §61
- MacBook field report folded in: PAT was never gated, repo bundle — §62
- Warden Slack app live — §63
- A killed episode is not a verdict: idle watchdog, `dispatches.error` — §64
- The consolidation look: max_turns misreported, the deploy window, the
  audit, the comparison — §65
- No limits; `warden close`; `check-routing`; helpers folded; hermes-agent
  roadkill — §66

Every wave appends a § to the log and rewrites this file; never edit the
log's past sections.

### Next action

`needs_human` is 0: item 1007 (the owner's brief, answered by §65) and item
253 (pre-§64 leftover) are closed by hand with `warden close`. Item 996 landed (meteo master 95d3e3b, watchdog
105/105, Kuma 220 UP with `degraded: true`). Owner: nothing. Loop: the first host-verb `fixed` is still ahead — the three hermes
items resolved as `quiet` before the heartbeat probe landed; a sideclaw host
verb for uk:204-shaped crashes, guarded by no dispatch in flight; the ledger
restore path. Wave 9, the field review, from `docs/handover-field-review.md`
after a few days of this.
