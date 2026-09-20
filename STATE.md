# STATE — warden

**Read this before anything else. It is the memory; the conversation is not.**
Authority order: `DESIGN.md` → `FLOWS.md` → `REVIEW.md` → this file →
`docs/history/state-log.md`.

| | |
|-|-|
| Last updated | 2026-09-20 (§78) |
| Current wave | GitHub-issues-in-warden chain is DONE — all five waves complete (§70 Wave 1, §71 Wave 2, Wave 3 in argo's own history, §72 Wave 4, §73 Wave 5). `docs/waves/PLAN.md` deleted in the same commit as §73; no chain currently active. Separately: estate chain Wave 8 done (§57); field look §58; autonomy §59; Wave 9, the field review, still the owner's to start — authority `~/SourceRoot/dotfiles/docs/waves/PLAN.md` |
| Repo state | `master`, five LaunchAgents on the mini |
| Ledger | `~/.warden/warden.db`, schema 10 |
| Tests | `tests/test_triage.py` 274/274 is the gate; `make test` runs all suites (17 files, incl. the new `tests/test_reset_frozen_notes.py`) |
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

## Open — owner actions

- The §55 4.3 "human types in Slack" acceptance is still open.
- `warden-api`'s "LAST EXIT -15" is the §55 kickstart; cosmetic.
- `op://mini/github/token` (fine-grained PAT) is missing `Issues: Read` on
  `dispatch-scratch` specifically (the one private repo in the fleet —
  every issue there is invisible to intake until granted) and missing
  `Issues: Write` repo-wide (the "comment back on the owner's own issue"
  feature has silently 403'd since Wave 1). Grant both at
  github.com/settings/personal-access-tokens; neither blocks routing (§73).
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
- `config/triage-policy.json` carries 151 rule entries for 61 distinct match
  values and 49 ignore entries for 12 (§77). §76's dedup stops the growth;
  nothing has collapsed what accumulated. First match wins, so it is inert.
- `_fetch_note_rows()` does not filter resolved events, so a `note` row whose
  incident has since resolved keeps appearing under the digest's "Unstructured
  notes" heading forever (§76). Digest-content call, not yet made.
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

Every wave appends a § to the log and rewrites this file; never edit the
log's past sections.

### Next action

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
