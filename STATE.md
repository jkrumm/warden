# STATE — warden

**Read this before anything else. It is the memory; the conversation is not.**
Authority order: `DESIGN.md` → `FLOWS.md` → `REVIEW.md` → this file →
`docs/history/state-log.md`.

| | |
|-|-|
| Last updated | 2026-09-28 (§103 — synthetic trip: a fixed Kuma push monitor's shadow must go DOWN in its live window before `fixed`; proven live; HyperDX and non-push Kuma are named gaps) |
| Current wave | GitHub-issues-in-warden chain is DONE — all five waves complete (§70 Wave 1, §71 Wave 2, Wave 3 in argo's own history, §72 Wave 4, §73 Wave 5). `docs/waves/PLAN.md` deleted in the same commit as §73; no chain currently active. Separately: estate chain Wave 8 done (§57); field look §58; autonomy §59; Wave 9, the field review, still the owner's to start — authority `~/SourceRoot/dotfiles/docs/waves/PLAN.md` |
| Repo state | `master`, five LaunchAgents on the mini |
| Ledger | `~/.warden/warden.db`, schema 11 |
| Tests | `tests/test_triage.py` 338/338 is the gate; `make test` runs all suites (19 files, incl. `tests/test_dispatch_sweep_pipeline.py` and `tests/test_watchdog_hermes_log_probe.py`) |
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
- Automatic dispatches send no model id: `AUTO_DISPATCH_MODEL` /
  `AUTO_IMPLEMENT_MODEL` both default to `None`, so sideclaw routes each tier
  per its own table (`GET /api/routing` — investigate/author on
  DeepSeek-V4-Flash, implement on DeepSeek-V4-Pro). The env vars
  `TRIAGE_AUTO_DISPATCH_MODEL` / `TRIAGE_AUTO_IMPLEMENT_MODEL` are the
  operator's escape hatch; both run on the IU backend, never Max. Validation
  is a sideclaw `review` job on the pull request;
  `TRIAGE_VALIDATION_DISPATCH_MODEL` can re-point it but defaults to `None`,
  i.e. sideclaw's JUDGE route — review is the one tool where the cheap tier
  has been measured failing (§64).
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
  `.env` (§64): `dispatch` on `DeepSeek-V4-Flash`/IU (the `AGENT` tier, §79;
  `check`/`overview` stay on `glm-5.3-flash`), `review`
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
- `warden abort <event-id> --why` cancels the episode **and discharges the
  cluster**: every other row sharing that `dispatch_job` still in an episode
  state (`investigating`/`implementing`/`validating`) closes with the same
  note, and an already-terminal job (sideclaw's 409) is tolerated instead of
  refused — a sibling left behind had no exit at all, only its deadline (§82).
  It honours `--dry-run` like every other verb since §83 (until then the
  preview was the effect: it cancelled and transitioned for real).
- `make check-routing` is the third drift check next to `check-schemas` and
  `check-policy`: any operator-set dispatch/validation model override against
  sideclaw's live `GET /api/routing`; with nothing pinned it reports
  "nothing pinned" and exits 0 (§66).
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
- `ingest_github_issues()`'s disappearance-resolve no longer trusts
  "missing from `search_issues()`'s result set" as proof an issue closed —
  a repo the token can't search (a real case, see Open — owner actions)
  looks identical to a repo with no open issues. It now confirms via a
  direct `_github.read_issue()` fetch and only resolves on a genuine
  `state == "closed"`; any error leaves the event alone (§73).
- `_render_env_check_note()` reads both of env-check's failure shapes, not
  just `danglingItems`: an `ok: false` with an empty dangling list (rate
  limit, network, expired token) now renders the host's raw `error` text —
  the transient wording is reserved for a genuine clean pass (§75).
- **Synthetic trip (§103).** After a Kuma-verified deploy and positive
  liveness, a notification-free shadow of the monitor's live push config must go
  DOWN within its window (`scripts/kuma-trip.py` over `ssh homelab`); otherwise
  the item reopens with `detection no longer fires:`. Hourly residue sweep;
  shadows never ingested. Gaps: non-push Kuma, HyperDX.
- **Self-audit (§100).** Hourly `check_invariants()` (INV-1…7, DESIGN.md §
  Executable invariants) + `self_audit_findings()`; findings become
  `warden_self` events → repo `warden` → ordinary items; `/health.self_audit`.
- **Default unattended merge scope (§99).** A repo with no `autoMergePaths` of
  its own and not merge-approval gated merges anything `NEVER_AUTO_MERGE`
  (merge.py: CI, Makefiles, scripts, launchd, Docker/compose, manifests,
  lockfiles, env templates) does not name; gated repos and an unreadable
  dispatch policy fail closed. Review gate = GitHub's branch rules (§97).
- **Live evidence in briefs (§95).** `launchd-restarts` (Dev Host),
  `beszel-alerts` (homelab temp/CPU/load/disk thresholds + firings),
  `kuma-monitor-config` (public monitors.yaml block + last 25 heartbeats).
- **Waiting on the owner is one list (§94).** `/board.awaiting_owner`: parked
  items (age, reason, recurrences, revisions, actions) + stranded PRs
  (`reconcile_stranded_prs()`, hourly, also lands PRs merged by hand). An owner
  merge (`owner:argo` only — a CLI confirm is forgeable by an episode, §96)
  skips the path scope and zero-CI acknowledgement only, never the confirmed
  review; the gated repos' Merge click works; `needs_human` with a confirmed
  PR offers Merge.
- **The last mile reaches three more repos (§93).** homelab
  (`uptime-kuma/monitors.yaml` → `uk-sync` → `kuma-push-fresh`), weatherorb
  (watchdog/tests/docs → `weatherorb-pull` → `kuma-push-fresh`),
  research-gateway (`src/**` etc. → `deployByPoller` → `mini-checkout-live`).
  The step-7 review gets the goal and `VALIDATION_GATE_QUESTIONS`; a
  policy-refused merge retries once the policy file changes. Hard human gate
  unchanged for `warden`/`sideclaw`/`dotfiles` and warden's policy files.
- **Blocked fixes are revised (§92).** `maybe_revise_blocked()` (first step of
  `advance_implement_chain()`) sends a `merge_blocked`/`needs_human` item whose
  review blocked it, or whose checks failed before push, back to a fresh
  implement episode with the findings, from the previous branch; the old PR is
  closed with a pointer. `revisionMaxAttempts` (2) per item.
- **Parked items count recurrences (§92).** `track_parked_recurrences()`: card
  line `Recurred N× since it parked here`, `parked_recurrences`/`revision_count`
  on `/board`, one extra reminder at `parkedRecurrenceReminder` (5).
- **Chronic signatures escalate (§91).** A mapped row that reopened ≥
  `chronicRecurrences` (3) times in `chronicWindowDays` (7) is held out of all
  three silence paths, so a self-clearing alert that keeps returning reaches
  `escalate()` instead of cycling `new → quiet` in one pass; its brief carries
  a `CHRONIC:` line. `resolve_quiet_grouped()` anchors on the later of the ISO
  clocks and `payload_json.ts_last`, so a cooldown-suppressed occurrence no
  longer reads as hours of silence.
- **Advance on completion (§87).** `triage.advance_implement_chain()` —
  `maybe_auto_implement` → `poll_implement_jobs` → `poll_validation_jobs`,
  the exact code `run()`'s own 600s tick calls — is now ALSO called,
  unconditionally, once per pass, at the end of dispatch-sweep.py's `main()`
  (300s), right after that pass's own per-row fold. An item no longer waits
  for the loop's own tick to cross verdict → implementing → validating →
  merge/merge_blocked; it advances on whichever of the two cron processes
  next observes the ledger in the eligible state, safely, with no new lock
  (every step is already CAS-guarded and re-derives its own eligibility
  every call) and no second loop (no new schedule was added — the sweep
  already ran every 300s). `dispatches.finished_at` now reads sideclaw's own
  `finishedAt` (`clients.sideclaw.finished_at_iso()`), not the wall clock of
  whichever process happened to observe the job terminal — §79's upper-bound
  problem is fixed, not just documented. No LaunchAgent plist changed.

## Open — owner actions

- The §55 4.3 "human types in Slack" acceptance is still open.
- `warden-api`'s "LAST EXIT -15" is the §55 kickstart; cosmetic.
- `op://mini/github/token` (fine-grained PAT) is missing `Issues: Read` on
  `dispatch-scratch` specifically (the one private repo in the fleet —
  every issue there is invisible to intake until granted) and missing
  `Issues: Write` repo-wide (the "comment back on the owner's own issue"
  feature has silently 403'd since Wave 1). Grant both at
  github.com/settings/personal-access-tokens; neither blocks routing (§73).
- **Stale since 2026-09-27: warden now has a GitHub remote** (`jkrumm/warden`,
  public). The bullet below predates it; implement episodes on warden work from
  `origin/master`, so the remote must be kept pushed.
- **`warden` is `implement`-reachable in both policy copies (its own
  `config/dispatch-repos.json` default, §74, and sideclaw's
  `/api/dispatch-policy`) and has no git remote at all (§62) — so every
  worktree-based `implement` episode on this repo dies in ~40 ms inside
  `resolveRepoIdentity()` and lands the item `merge_blocked` with no artifact
  (§76, job `a8850cc5`).** Pick one: add an `origin` (GitHub, private), route
  warden's own fixes through a `workspace: "in-place"` episode, or cap warden
  at `investigate` again — the current combination promises a draft PR and
  cannot produce one. Until then, self-repo fixes land by hand.

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
- `config/triage-policy.json` deduplicated to 76 rules / 15 ignores (§98),
  proven identical on all live events; §76's dedup stops regrowth.
- `_fetch_note_rows()` excludes rows whose event has resolved and has no age
  filter, so the digest's "Unstructured notes" heading carries a still-open
  incident and reprints it every UTC day (§81, §90). The rows §76's revive
  skipped because their events had resolved (105, 542, 918) stay terminal
  `note` and are silent while that holds. **A `slack_alert` signature that
  re-fires clears `events.resolved_at` back to NULL, and the row re-enters the
  heading — immune to any `ignore`/`rules` entry added since, because
  `classify()` only ever touches `new` and `reopen_if_needed()` skips `note`.**
  Event 999 (the VPN self-healing notice) did exactly that on 2026-09-26, eight
  days after the `ignore` entry covering it landed. `scripts/reset-frozen-notes.py`
  is the standing repair, re-runnable by design (§90; its docstring said
  "one-time" before that).
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
- GitHub issues in warden, Wave 5 (final): a real owner issue and a real
  third-party-shaped fixture run end to end through Argo; the
  disappearance-resolve bug this surfaced and fixed (a repo the token can't
  search looked identical to a repo with no open issues); two GitHub PAT
  gaps found and left for the owner — §73
- The self-repo cap lifted: sideclaw and warden implement-reachable again,
  the loop stays open at merge — §74
- env-check's second failure shape: a rate limit rendered as "likely
  transient"; the renderer now reads `error`, not just `danglingItems` — §75
- `classify()` matched rules after the prose filter, freezing ten
  `slack_alert` rows in the terminal `note` state; rule matching now runs
  first, the proposal pass dedups against the policy file, and the frozen
  rows were revived once from a ledger snapshot — §76
- The field look after ten unattended days (153 dispatches, 6 failed, 0
  watchdog kills, 1 of 25 draft PRs merged); `classify()` re-froze an
  already-mapped row in `note` on its second pass — §77
- The dispatch model measured (DeepSeek-V4-Flash 1.00 at 190 tok/s vs glm's
  0.81 at 13.3), the nine open PRs reviewed, OpenCode proven as a possible
  second lane — §78
- Dispatch moves to `DeepSeek-V4-Flash` after a twelve-episode POC through
  this lane; V4-Pro measured and rejected; `finished_at` is observation time,
  not completion time — §79
- First full lifecycle on the new model: two draft PRs in 27 min, under nine
  of them work — the 600 s tick is now an item's latency; a second operator
  re-filed and deduped this session's items through Hermes's door — §80
- The digest's "Unstructured notes" heading stops carrying rows whose incident
  already resolved: the four §76 left behind now filter out, so the digest is
  silent until a real unactioned note appears — §81
- `abort` discharges the whole cluster, not one row: a sibling left in an
  in-flight state had no exit (`close` refuses those, the sweep never re-reads
  a reported row, a second abort hit sideclaw's 409), and an already-terminal
  job is now tolerated — §82
- `abort` honours `--dry-run`: it was the one verb that fell straight through
  the flag and cancelled the episode for real — §83

Every wave appends a § to the log and rewrites this file; never edit the
log's past sections.
- The automatic model split: investigate on Flash, implement on Pro; Pro's
  poor cache reuse on this gateway recorded and under measurement — §84
- §85's cache diagnosis withdrawn on a proper probe: Pro's cache advances like
  Flash's; the real-episode gap is reproduced but unexplained — §86
- Advance on completion: dispatch-sweep.py's 300s pass now also runs the
  verdict → implementing → validating → merge chain, and `finished_at` reads
  sideclaw's own timestamp instead of the poll's — §87
- Instructions move to `AGENTS.md`; `CLAUDE.md` is exactly the `@AGENTS.md`
  shim — §88
- The deliberate fallback probe's 404 no longer reaches the poller: a content
  match on the sentinel model id (`PROBE_SENTINEL_RE`), deliberately not an
  `ignore` entry — that key truncates the model id, so it would have suppressed
  a real brain 404 too. One probe wrote exactly three ERROR lines and landed on
  `minOccurrences` — §89
- A re-fired `note` row returns to the digest: the recurrence clears
  `events.resolved_at`, the row is immune to the `ignore` entry that now covers
  it, and `reset-frozen-notes.py` is recorded as the standing repair rather than
  a one-time run — §90
- Chronic signatures escalate instead of recovery-resolving in the pass that
  reopened them; the quiet timer reads `ts_last` — §91
- Blocked fixes revised with the reviewer's findings; parked items count their
  recurrences; schema 11 — §92
- The last mile: homelab, weatherorb, research-gateway review → merge → deploy →
  verify unattended; C3 disposition updated — §93
- `awaiting_owner` incl. stranded PRs; a gated merge is one working click — §94
- Live read-only evidence for the families verdicts kept handing to a human — §95
- Review of §91–§95: three blocking merge-sharing defects fixed — §96
- GitHub's rules, not a local list, decide review gates; policy dedup; default
  unattended scope with `NEVER_AUTO_MERGE` in code — §97–§99
- Named executable invariants and a self-audit that makes its own gaps work — §100
- Approval buttons clickable again (posted as Hermes); Hermes skills aligned — §101
- Review of §97–§101: five blocking findings fixed — §102
- Synthetic trip for Kuma push monitors, proven live — §103

### Next action

**§87 just landed** (advance on completion — verdict → implementing →
validating → merge/merge_blocked no longer waits for the loop's 600s tick;
`finished_at` is sideclaw's own timestamp), but only on the
`worktree-advance-on-completion` branch — it built and tested green from an
isolated background session and has not been merged to `master` yet, so the
five live LaunchAgents are still running the pre-§87 code. Merge it
(fast-forward, this repo's own direct-to-master convention), then watch a
real lifecycle (`warden run dispatch-scratch --tier implement` with a
trivial brief is the safe target) for stage boundaries landing within one
300s sweep pass of each other instead of on the next 600s multiple — see
§87's own "not yet measured" paragraph for the exact comparison. No plist
changed, nothing to reload.

**§77, the field look (read it first).** The dispatch lane held up alone for
ten days; the open work is the owner's, not the loop's: 24 draft PRs across
nine repos are unreviewed, which is also the only way to learn whether
`glm-5.3-flash`'s implement output is good — the ledger measures that it
finished, not that it was right. Items waiting: 543 and 1133 (`dotfiles`,
`needs_human`), 1062 (`rollhook`, `merge_blocked`, PR review required). 543 is
the decision behind the loudest recurring alert (host health check grades
memory-pressure level 2 as FAIL). Outside this repo: the gateway
context-window table in dotfiles and sideclaw has one row, so any model other
than `glm-5.3-flash` auto-compacts at 200k — add measured rows before trying
another model, not after.

**§79 made the routing change §78 describes** — dispatch now runs on
`DeepSeek-V4-Flash` in both repos. Watch `dispatches.error`, idle-watchdog
kills and tool errors for a week; `TRIAGE_AUTO_DISPATCH_MODEL=glm-5.3-flash`
is the way back. Open from §79: `dispatches.finished_at` records when warden
observed a job finishing, so every ledger duration is an upper bound. The
paragraph below is kept for its reasoning.

**§78, the next routing change.** Evidence says move dispatch from
`glm-5.3-flash` to `DeepSeek-V4-Flash` (same leg, same harness, ~14x the
in-loop rate, $0.09 vs $0.035 per ccbench suite). It is three edits that land
together — sideclaw `GATEWAY_CONTEXT_TOKENS` (1,000,000) and `routing.ts`
AGENT, then `AUTO_DISPATCH_MODEL` here — and sideclaw's tree had another
session's uncommitted work on 2026-09-20, so none of it was started. Gateway
ids are case-sensitive. After the switch, watch `dispatches.error` and the
idle watchdog for a week: one ccbench suite is thin next to 153 field
dispatches. §78 also has the per-PR verdicts for the nine open PRs.

No wave is active — the GitHub-issues-in-warden chain (`docs/waves/PLAN.md`,
deleted this commit) is fully done across all five waves. Nothing queued
here; the next piece of work is whatever the owner picks up next, starting
with the two `op://mini/github/token` grants in Open — owner actions if this
repo's issue intake is to reach `dispatch-scratch` and actually post
comments back.

The Wave 1 `investigate` cap on `sideclaw`/`warden` is lifted (§74, owner):
both are implement-reachable again, `make check-policy` agrees with sideclaw,
and the loop stays open because neither repo ever gets `autoMergePaths` — a
change to either merges only on the owner's Argo click. The PAT now has
Issues read/write; it still 403s on Checks and Actions read.

§75 landed the `hermes-agent#2` renderer fix by hand: `hermes-agent` is capped
at `investigate`, so item 1089 folded to `needs_human` (correct — that cap is
not a bug) and the change to `triage.py` was made directly. The underlying
1Password budget exhaustion is item 1088, on `homelab`, and the loop is
carrying it.

§76 was the same shape for a different reason: `warden` is implement-reachable
(§74) but has no remote, so item 1118's implement episode failed in 39 ms and
the `classify()` change landed by hand, on that episode's verdict. The
one-time revive (`scripts/reset-frozen-notes.py --apply`) was run against the
live ledger in the same session, after the fixed `classify()` was live: 6 of
the 10 frozen rows came back to `new`, the next 600 s pass routed the
above-threshold homelab trio (`repo=homelab`) and `ignore`d the below-threshold
trio. Item 1118 closed with the commit in `--why`. The tier-vs-remote decision
is the first line of Open — owner actions above.

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
