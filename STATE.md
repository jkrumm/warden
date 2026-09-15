# STATE — warden

**Read this before anything else. It is the memory; the conversation is not.**
Authority order: `DESIGN.md` → `FLOWS.md` → `REVIEW.md` → this file →
`docs/history/state-log.md`.

| | |
|-|-|
| Last updated | 2026-09-15 (§72) |
| Current wave | GitHub-issues-in-warden chain (`docs/waves/PLAN.md`): Wave 1 done (§70), Wave 2 done (§71), Wave 3 done (argo API: the action queue — no warden-repo state-log entry, see argo's own `docs/waves/PLAN.md`), Wave 4 done (§72, argo dashboard: issues + one-click triage), Wave 5 active (end to end on a real issue). Separately: estate chain Wave 8 done (§57); field look §58; autonomy §59; Wave 9, the field review, still the owner's to start — authority `~/SourceRoot/dotfiles/docs/waves/PLAN.md` |
| Repo state | `master`, five LaunchAgents on the mini |
| Ledger | `~/.warden/warden.db`, schema 10 |
| Tests | `tests/test_triage.py` 265/265 is the gate; `make test` runs all suites |
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
  pollers), `github_issue` (every open GitHub issue under `_github.GH_OWNER`,
  minus `warden:skip` as the one opt-out — no label gate anymore, event
  source stays `github_go`, a historical artifact of the old label-only
  intake), or `human` (`warden run`, typed in herdr or through Hermes's door
  with `--origin-channel`/`--origin-thread`). See
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
- `apply_argo_actions()` runs as step 9.5, right before the snapshot push
  (§71): pulls the owner's pending Argo actions and applies
  `implement`/`merge`/`dismiss`/`reinvestigate`/`note` through a closed verb
  allowlist, `authorized_by="owner:argo"` — the same plain gate a signed
  Slack approval satisfies (DESIGN.md § *2026-09-15 override*). Every board
  item now carries `availableActions` and, for `github_issue` origins, an
  `issue` sub-object. Every daily count budget (`WARDEN_DAILY_BUDGET`/
  `WARDEN_IMPLEMENT_BUDGET`/`WARDEN_MERGE_BUDGET`/`DAILY_INVESTIGATE_BUDGET`)
  is gone — `MAX_OPEN_INVESTIGATIONS` and the per-repo lock are the only
  ceilings left on autonomous spend. `GET /warden/actions`/
  `POST /warden/actions/:id/ack` are live (argo Wave 3, deployed to prod);
  `fetch_actions()` no longer 404s. Argo's `/warden` page now has a GitHub-
  issues section rendering `availableActions`/`issue` with one-click
  implement/merge/dismiss/reinvestigate/note buttons (Wave 4, §72).
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
- A sideclaw worker is killed by an idle watchdog (5 min with no stdout) and
  nothing else — the 60 min ceiling of §64 went in §66; a slow glm episode is
  not a wedged one. A dispatch that ends terminal with no verdict folds to
  `needs_human` carrying `dispatches.error`, never into `verdict`.
- **Workers have no turn limit and no wall-clock ceiling** (sideclaw
  `8459357`, §66): the idle watchdog (no stdout for 5 min) is the only kill
  rule. Slow is not stuck. Lifecycle deadlines on items (verdict 24 h,
  needs_human 7 d) are a different fact and stay. The rule is global since
  2026-09-12 (`dotfiles/rules/agent-limits.md`) and applied the same day in
  hermes-agent (`max_turns: 0`), research-gateway (idle watchdog for step
  caps and deadlines), audio-gateway, and the MCP client side
  (`MCP_TOOL_TIMEOUT` 24 h, sideclaw entry `timeout` 30 min).
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
- Three recurring alerts: backup bundle verify, tier-cap flap, Kuma interval — §67
- Creation transitions, event reminders, bounded snapshot; secrets-run relay; VPN interval — §68
- The GitHub poll that resolved every open issue — §69
- GitHub issues in warden, Wave 1: no-label intake, `warden:skip`, third-party
  verdicts land in `needs_human` — §70
- GitHub issues in warden, Wave 2: owner actions pulled from Argo, every
  daily budget removed — §71
- GitHub issues in warden, Wave 4: the dashboard's one-click triage section
  (Wave 3, the argo action-queue API, has no warden-repo entry — it happened
  entirely in argo) — §72

Every wave appends a § to the log and rewrites this file; never edit the
log's past sections.

### Next action

Wave 5 of `docs/waves/PLAN.md` (end to end on a real issue) is next: open an
owner issue in `dispatch-scratch`, watch it become an item and get
investigated, then approve/dismiss from the now-live Argo `/warden` GitHub-
issues section and confirm the transition lands in both the ledger and the
page. Also confirm the existing backlog (research-gateway #3–#7, basalt-ui
#51/#52, rollhook #21, sideclaw #3/#4 → investigate-only, ntfy-mac #12
third-party) shows correct assessments there — most of it already does, per
§72's live chrome-devtools check. Once Wave 5 closes, rewrite this file's
history section one more time and delete `docs/waves/PLAN.md`.

Left behind by Wave 1, still unresolved: sideclaw's own dispatch-policy
boundary still allows `implement` on `sideclaw`/`warden` — `make check-policy`
disagrees until sideclaw's side is capped too (out of this plan's scope, a
sideclaw-repo change).

Left behind by Wave 4 (full detail in argo's `docs/waves/PLAN.md`): fallow's
audit in argo never went fully green — confirmed pre-existing, unrelated
dependency debt plus an architectural duplication call (`board-section.tsx`
vs `issues-section.tsx`'s card/responsive-switch/empty-wrapper skeletons)
the review itself flagged as needing a deliberate decision, not chased
further this wave.

§67/§68 closed the recurring-alert list: backup heartbeat, tier-cap flap
(543 now `needs_human`), creation transitions, alert reminders on the event,
the snapshot bounded to 50 rows per item, `secrets-run`'s group relay
(dotfiles `a9410e7`), the VPN Watchdog interval (homelab-private `a71ded1`).
argo's item modal is live (`4213502`), its AI gateway on deepseek-v4.1-flash
(`6765121`, vps `83c4bc6`). hermes-agent pushed, gateway restarted onto the
start-grace/skill-hint/darwin-forensics patches; 543 closed. Argo's GitHub-
issues dashboard section (§72) was verified live in a browser, replacing the
"unverified: the modal in a browser" note that used to sit here. Still ahead
from before: the first host-verb `fixed`, the ledger restore path, Wave 9
from `docs/handover-field-review.md`.
