# STATE — warden

**Read this first. It is the memory; the conversation is not.** `DESIGN.md` is what
warden is; this file is where it is; `docs/history/state-log.md` is the append-only
build log (§1–§150).

| | |
|-|-|
| Last updated | 2026-10-15 (§165 — `make verify` baseline at merge; a red already present before the merge is not the merge's) |
| Current work | agent-platform rewrite (`docs/waves/PLAN.md`) — Waves 1–6 done; spec `~/SourceRoot/dotfiles/docs/agent-platform.md` |
| Repo | `master`, pushed to `jkrumm/warden` (public); five LaunchAgents run this checkout |
| Ledger | `~/.warden/warden.db`, schema 22 (live ledger migrates on the loop's next boot after the deploy) |
| Tests | `make check` — `tests/test_triage.py` at the count AGENTS.md names |

## What is live

The whole loop in `DESIGN.md` § The loop runs: one intake pool → single-shot agent-gateway
triage (`attach | new | fixed_by | ignore`) → investigate → implement at any
confidence, revisions on the same PR (capped at 3 STALLED rounds, §150) → one merge train per repo
(`update_pr` → checks → review → squash, all on one SHA) → `make deploy` / `make
verify` + the item's own signal quiet → automatic revert on failure → fixed-by sweep
after every fix merge. Slack hears one line on `fixed` / `needs_decision` and a daily
`failed` count; Argo `/warden` is the queue.

- **Shape.** `scripts/triage.py` is the entry point; the loop lives in
  `scripts/loop/` by stage. warden meets the repo contract itself: `make check`,
  `make deploy` (kickstart `warden-api`, health check, roll back to the
  pre-merge commit under a lock shared with the loop's sync), `make verify` (schema + loop heartbeat), `make logs`; `make setup` writes
  `~/.local/bin/warden`.
- **`make check` is self-contained.** The repo contract's check is a bare `make check`,
  run in a FRESH linked worktree (agent-gateway's check job, a dispatch episode) with no
  `.venv`; `check` runs `ensure-venv` first, which creates a bare venv from the pinned
  `python3.11` (no pip, no network) so the suites that hardcode `.venv/bin/python3`
  (`scripts/warden`, `deploy.sh`, `warden-backup.sh`) have one. `make venv` still installs
  `requirements.txt`; `make test` alone still needs a venv.
- **Self-deploy.** A merged warden PR is fast-forwarded into the live checkout by
  the loop and deployed by `make deploy`; Hermes's `warden-live-sync` cron is gone.
- **Rollout PATH.** `lifecycle/rollout.py` widens PATH for every `make`/`git`
  subprocess with the existing host tool dirs (`HOST_TOOL_DIRS`), so a repo's
  `make deploy` finds `op`/`brew` under launchd's minimal PATH; a caller's own
  PATH in `extra_env` still wins. An empty inherited PATH component (POSIX CWD)
  is preserved, and an unset PATH falls back to `/usr/bin:/bin` while an
  explicitly empty one stays empty.
- **Merge gate:** unreadable check runs fall back to Actions runs; an empty
  fallback waits (no timeout) — a private repo with no workflows and an unreadable
  checks API never merges until the PAT can read checks. A PR merged by hand
  without a review at its head verifies by signal only.
- **Base-red baseline.** The train's checks stage compares a failing check on the
  train SHA with `train_base_sha` — the base commit the SHA was rebased onto,
  captured from `update_pr`'s result when the train SHA was BUILT, never the PR's
  current `base.sha` (which may have advanced past it). When every failing check
  also fails on that base, the red is the base's, so the item parks (no strike, no
  revision) until the base goes green rather than spending a stalled round on a fix
  that cannot land — but not on the frozen base commit forever: the park sends the
  item back to `update` (`retry_at` +PARK_MINUTES, no `train_rewinds` bump, since the
  head did not move), so the next pass re-runs `update_pr` and re-derives
  `train_base_sha`/`train_sha` against the live base tip, and a base fix landing as a
  later commit is observed. A base green on that check, a check absent from the
  base, no recorded base, or an unreadable base keeps the `checks_failed` revision
  path. `update_pr`'s own `checks.passed == false` result is baselined the same way
  (its `baseSha` is the base just rebased onto), so a park's re-entry on a base that
  advanced (A -> B) and stayed red parks again instead of becoming a revision that
  cannot fix it. `baseSha` is now required in `update_pr_result()`: a `updated` or
  `up_to_date` result missing it is a loud refusal, never a silent no-op.
- **Verify baseline (§165, schema 22).** The merge train takes `make verify` once before it lands a
  pull request (`train._train_merge` → `verify.take_baseline`, only with a verify target, every merge
  attempt) and stores `{ok, exit, tail, at}` in `triage_items.verify_baseline`; a timeout or error is
  stored as a red probe and never blocks or strikes the merge. `verify._verify_item` reads it: a red
  baseline AND a red `make verify` after the deploy is pre-existing — not a failing pass (no strike
  toward the limit of 3, no revert), named in `verify_result` / the fixed note; alert items still wait
  their window and are judged by their own signal. Green or missing baseline, `_verify_revert` and the
  fixed-by sweep (which never runs `make verify`) are unchanged. Residual: while verify stays red, a
  second regression is not attributed to its merge; the red is its own alert. The verify twin of the
  base-red CI park below (item 1545: weatherorb #93 healed its canary, `make verify` stayed red on an
  unrelated alert opened before the merge, three failing passes ended it `failed`). Not live-validated.
- **Ledger:** every pending migration first writes
  `~/.warden/backups/pre-migration-v<from>-to-<to>-<stamp>-<pid>.db` (never pruned).
- **`failed` is classified.** Every `failed` row carries `failure_class` (`infra` |
  `policy` | `work`) and a re-entry recipe (`redrive_json`). The loop re-drives infra
  after 60/180/480 min (≤3, `redrives` is that budget; it resets once the item reaches
  merging+), policy once per change of agent-gateway's `GET /api/dispatch-policy` hash; work
  waits for `warden retry <id> [--why]` or Argo's Retry button. A hand-reverted item is
  never re-driven. `failed` still never posts; the daily count does.
- **Reinvestigate** (`warden reinvestigate` / Argo) sends an item back to `triaged` for a fresh
  investigation and closes the pull request it clears (best-effort GitHub call), keeping its URL in
  the note as `superseded PR <url>`.
- **Reopen.** A recurrence reopens a closed item to `new`, keeping `artifact_url` (the brief's "a PR
  already exists") and `dispatch_job` (the escalation cooldown anchor), and clearing every implement
  handle the closed attempt left — `implement_job`, `validation_job`, `pr_url`, `reviewed_sha`,
  `blocked_sha`, `revision_count`, `stalled_rounds` — exactly as reinvestigate does, so the new
  occurrence starts a fresh investigation instead of resuming the old attempt (item 543). The reopen
  is an atomic compare-and-set on the closed state AND the stored occurrence mark, so two concurrent
  passes cannot both claim the same recurrence, and only the winning caller closes the superseded PR.
  The PR the cleared `pr_url` named is closed best-effort (after the commit) and its URL kept in the
  note as `superseded PR <url>`, as reinvestigate does, so it is never orphaned beside the fresh
  run's own — but only when it is still open: a `fixed` item's PR is already merged (the `fixed`
  transition keeps `pr_url`), so a merged one is left alone rather than posted a false 'Superseded'
  comment. Under `--dry-run` the transition (and its note) still run locally, but the PR close is
  skipped — no outbound call.
- **Adopt** (`warden adopt <event-id> --pr <n>` / Argo's Adopt action) points an item at a pull
  request warden itself opened for it — a `dispatches` row, still open and unmerged — restores its
  `implement_job`/`pr_url` and puts it back on its repo's merge train, which re-reviews the current
  head and lands it. The owner's alternative to hand-merging on GitHub (which leaves the ledger
  blind and the superseded draft open); a prior `pr_url` is closed best-effort. The Argo card takes
  the PR number as free-text owner input (`payload.pr`), like dismiss's `reason` and note's `text`.
  A PR warden did not open is refused — the merge gate only ever lands a `dispatch/…` branch warden cut.
- **Re-route.** A `human` verdict whose optional `owningRepo` names a different known repo moves the
  item back to `triaged` in that repo (once per item; the re-route's transition note is the
  ping-pong guard) instead of paging the owner — a misrouted finding re-investigates where it
  belongs (`work._reroute_repo()`).
- **Ceiling diversion.** `maybe_auto_implement()` reads agent-gateway's dispatch policy once per pass:
  an item in a repo capped below `implement` is parked in `needs_decision` with
  `<repo> is <ceiling>-only for agents — apply by hand: <recommendation>` instead of being submitted
  (a 4xx would otherwise land it `failed(policy)` and bury the recommendation). A repo with no rule
  has no ceiling; an unreadable policy submits as before, leaving agent-gateway to refuse.
- **Owner-only escalation.** A `human` verdict reaches `needs_decision` only with an
  `escalationCategory` (product, data_loss, spend, other_people, security, blocker); the note carries
  it as `[category]`. An investigate verdict without one is re-driven once ("decide it yourself"), then
  warden takes its `recommendation` (`work._member_outcome`).
  The step-7 review does the same (`train._fold_review`, review schema {1, 2}): `needs-human` with a category and NO fixable code finding (or a process-only finding only) →
  `needs_decision`, `[category]`-led; the same shape carrying a fixable code finding is a revision
  first (`blocked` — the findings are a work order, the owner is paged only once none is left); a schema-2 one without a category is re-reviewed once per head (latch
  `dispatches.validation_status = 'redriven:<sha>'`, a `PRIOR ESCALATION` paragraph in the review context), then
  read as the decision: blocking findings revise, none confirms. Schema-1 and revert reviews keep the old path.
  A review whose own pipeline failed — a reviewer session that never examined the diff, which
  agent-gateway still answers `needs-human`/`blocker` and marks with a `(review pipeline)` `discussions`
  entry — is an infrastructure failure: it strikes and re-runs on the same SHA, never `needs_decision`.
- **Revision rejoin.** A step-7 review that blocks records the refused head in `blocked_sha`
  (migration 18), which survives leaving `merging`. A revision `no_changes` on an open PR then
  compares the PR's current head (read from GitHub) to it: a head that moved rejoins the merge
  train for a fresh review instead of paging the owner (item 1464: weatherorb #60 blocked at
  `34fd019`, `b35b86e` landed after); an unchanged head, a missing `blocked_sha`, or an
  unreadable PR still goes to `needs_decision`. `TRAIN_START`, a redriven train and
  `core.reinvestigate()` all clear it.
- **Converging revisions (§150).** The revision cap counts STALLED rounds (`stalled_rounds`, migration 19),
  not attempts. A revision's review context quotes the previous round's blocking findings and asks the
  reviewer to start any still-unresolved one with `UNRESOLVED PRIOR FINDING:`; a blocked round without it
  is converging and uncapped, one with it (or a first review, red checks, a conflict, a revert) is stalled,
  and the third stalled round lands `failed` with a `stalled: N revisions did not resolve …` note.
  `revision_count` still means the attempt ordinal. `warden retry` keeps both counts (as it always kept
  `revision_count`); reinvestigate and adopt reset both. Not live-validated: the reviewer following the
  marker instruction is measured on the next converging chain — if it never marks, every chain is
  "converging" and uncapped, if it marks everything the old cap returns.
- **Prior resolution in the brief.** A reopened item's most recent SUBSTANTIVE close note and its
  date (`item_transitions`, terminal state; a later silence close or a blank row never shadows a
  real answer, and a pure-silence close on its own yields no `PRIOR RESOLUTION` line at all) ride
  into the fresh investigate brief as a dated `PRIOR RESOLUTION` line (`work._latest_terminal_note`).
  The line states the note explains ONLY its own signature, and that `nextAction=none` closes the
  whole cluster only if every member is independently explained, so the episode does not re-ask the
  owner what a prior one answered nor silently close an unexplained sibling.
- **Live state in the alert brief.** Every alert cluster brief closes with
  `work.LIVE_STATE_INSTRUCTION`: the answer usually sits on the host, so read it READ-ONLY through
  the repo's own `AGENTS.md` commands (`make logs`/`status`, monitor history) before concluding, and
  "needs live inspection" is never a `blocker` — a blocker must name the command tried and the error
  it returned (item 121 paged the owner with `[blocker] inspect the host or dismiss?`). It also says
  how to close a benign one-off: `nextAction=none` with the evidence read, or `implement` when the
  monitor itself is miscalibrated in a repo that owns its config. The fixed closing block (and any
  decide-paragraph) is reserved BEFORE the variable member/sibling text spends budget, and the
  decide-paragraph — the one unbounded input, it quotes a re-driven item's stored decision question —
  is itself capped first, so a cluster over `MAX_BRIEF_CHARS` cannot lose the instruction to
  `_cap_brief()`'s tail truncation (`_truncate()` returns "" for a spent budget, never the
  nearly-full text); the re-driven `_decide_brief_paragraph()` carries the same stricter `blocker`
  wording, so both live paths to item 121 read it the same way.
- **Board after the W7 backlog pass:** 12 `failed` before the deploy (9 closed as
  superseded/fixed), 2 `needs_decision`.

## Open — owner actions

- The fine-grained PAT (`op://mini/github/token`) has no Checks permission (fine-grained
  PATs cannot have one); the merge gate already falls back to Actions workflow runs, which
  it can read — that is **not** what stalled weatherorb. It lacks `Issues: Read` on
  `dispatch-scratch` and `Issues: Write` repo-wide (issue comment-back 403s).
- Repo contract: every repo in scope now has `check`/`deploy`/`verify`/`logs`
  (`free-planning-poker` and `basalt-ui` already had them on master; `homelab-private`, 681a836).
  `homelab-private` stays investigate-only in agent-gateway's dispatch policy.
- **herdr is not under launchd yet.** The plist is ready (`make herdr-launchd-status` in
  dotfiles); the cutover is `make herdr-restart YES=1`, which kills every pane — owner-timed.
- agent-gateway serves `dispatch_implement_escalation`; attempt 3+ escalates to it.
- hermes-agent's agent-gateway ceiling is `investigate`: its 6 policy-failed items
  (1114, 1420–1424) re-drive once after the deploy, are refused again, and wait for
  the ceiling to change. Lift it in agent-gateway or close them.
- agent-gateway#10 (item 1302) is an owner call (keep matching `op://` pointers in the diff
  secret-scan or not) — left `failed(work)`.

## Carried debt

- **Concurrency:** reconcile's merge → `verifying` write is not compare-and-set; a
  PR merged on GitHub mid-train (checks/review stage) lands `failed`; a revert waits
  behind an implement episode already running in its repo; lease-refusal retry
  (10 min, no strike) is unbounded.
- **Claims:** three hand-rolled claim mechanisms (`card_hash` prefix, `expect_state`
  CAS, `retry_at` as review-claim expiry) remain; the split kept them as they were.
- **Triage:** an owner dismiss racing an in-flight triage job reads as a model
  ignore later. A `checks_failed` revision loses earlier review findings. Policy
  `rules` stay a label tier until the `## Verify & Monitor` sections cover the
  fleet and a replay routes at least as well.
- `rollout.checkout_in_use()` and `_sync_checkout()` classify the busy checkout twice (the latter after a fetch); a shared predicate would stop them drifting.
- **Review** is not delta-only (agent-gateway has no PR delta scope or reviewed SHA).
- **Metrics:** auto-reverts are missing from `/metrics`' revert count.
- **Backup** is `VACUUM INTO` → homelab → restic → B2; restoring from B2 has never
  been drilled.
- `deliver=False` in watchdog `reconcile`/`upsert_grouped` has no non-test caller.
- `warden.py` keeps its own `_secrets_run_path` copy.
- `rollout.WARDEN_CHECKOUT` and `deploy.sh`'s `LIVE_REPO` name the live checkout
  twice. `land_already_merged_item` carries two outcomes (gated vs unreviewed hand
  merge) behind one bool — a split was proposed, not done. Pre-migration snapshots
  have no retention. `lifecycle/policy.py` and `lifecycle/intake.py` still spell
  some state names as literals.
- Deferred by the orchestrator: a further `work.py` split and breaking the loop
  modules' import cycle (`tests/test_loop_imports.py` guards the function-body-only
  rule meanwhile).

## Wave 6 (2026-10-10)

- **Parking.** A repo whose checkout is dirty, off-default or ahead of origin, or has a herdr agent
  working in it, parks its merge (`train._train_merge`) and deploy (`verify._deploy_item`): same state,
  note `parked: …`, `retry_at` +25 min, no strike (`core.park`, `rollout.checkout_in_use`).
- **Revision cap 2.** `MAX_IMPLEMENT_ATTEMPTS` 4 → 3; the last attempt is the one escalation.
- **Duplicates at the door.** A `warden run` whose brief overlaps (Jaccard ≥ 0.8) an open item of the
  same repo reuses that item (`intake.overlapping_open_item`); alert duplicates were already merged by
  `rootCause`.
- **The 1h `merged` deadline** is gone from the code; migration 17 backfilled the five weatherorb
  items it had closed as `resolved` (1281, 1290, 1314, 1317, 1321) to `fixed`.
- **Kuma** `Warden Loop - Push`; log rotation was already declared in dotfiles `log-rotate.sh`
  (16 MB cap, `warden-*.err` included).
- **improve loop** is outcome-triggered (`scripts/improve-trigger.py`).
- **v6 refusals.** agent-gateway's synchronous 400s already arrive as `SubmitRefused` →
  `failed(policy)`; `pending`/`queuedBehind` is just a non-terminal job. A test now pins the shape.

## Next action

After the deploy: watch the first merge that rides the train in a repo with a `make verify` target
store a baseline (`verify_baseline` non-NULL on the `verifying` item), and the first red-before-and-after
item land `fixed` with "pre-existing" in its note. Also still: the first live re-drives after the W7 deploy: 1398/1400 (infra → re-investigate),
1370 (dotfiles PR #12 → merge train), 1389/1390 (owner-retried into the train). Then
the first warden PR that rides the train end to end and the first revert in the field;
then the carried debt above, concurrency first.
