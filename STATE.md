# STATE — warden

**Read this first. It is the memory; the conversation is not.** `DESIGN.md` is what
warden is; this file is where it is; `docs/history/state-log.md` is the append-only
build log (§1–§142).

| | |
|-|-|
| Last updated | 2026-10-10 (§142 — a reopened item's prior close note reaches the fresh investigation) |
| Current work | agent-platform rewrite (`docs/waves/PLAN.md`) — Waves 1–6 done; spec `~/SourceRoot/dotfiles/docs/agent-platform.md` |
| Repo | `master`, pushed to `jkrumm/warden` (public); five LaunchAgents run this checkout |
| Ledger | `~/.warden/warden.db`, schema 17 |
| Tests | `make check` — `tests/test_triage.py` at the count AGENTS.md names |

## What is live

The whole loop in `DESIGN.md` § The loop runs: one intake pool → single-shot agent-gateway
triage (`attach | new | fixed_by | ignore`) → investigate → implement at any
confidence, revisions on the same PR up to 3 attempts → one merge train per repo
(`update_pr` → checks → review → squash, all on one SHA) → `make deploy` / `make
verify` + the item's own signal quiet → automatic revert on failure → fixed-by sweep
after every fix merge. Slack hears one line on `fixed` / `needs_decision` and a daily
`failed` count; Argo `/warden` is the queue.

- **Shape.** `scripts/triage.py` is the entry point; the loop lives in
  `scripts/loop/` by stage. warden meets the repo contract itself: `make check`,
  `make deploy` (kickstart `warden-api`, health check, roll back to the
  pre-merge commit under a lock shared with the loop's sync), `make verify` (schema + loop heartbeat), `make logs`; `make setup` writes
  `~/.local/bin/warden`.
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
- **Re-route.** A `human` verdict whose optional `owningRepo` names a different known repo moves the
  item back to `triaged` in that repo (once per item; the re-route's transition note is the
  ping-pong guard) instead of paging the owner — a misrouted finding re-investigates where it
  belongs (`work._reroute_repo()`).
- **Prior resolution in the brief.** A reopened item's most recent close note (`item_transitions`,
  terminal state) rides into the fresh investigate brief as a `PRIOR RESOLUTION` line
  (`work._latest_terminal_note`), so the episode does not re-ask the owner what a prior one answered.
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

Watch the first live re-drives after the W7 deploy: 1398/1400 (infra → re-investigate),
1370 (dotfiles PR #12 → merge train), 1389/1390 (owner-retried into the train). Then
the first warden PR that rides the train end to end and the first revert in the field;
then the carried debt above, concurrency first.
