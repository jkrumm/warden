# STATE — warden

**Read this first. It is the memory; the conversation is not.** `DESIGN.md` is what
warden is; this file is where it is; `docs/history/state-log.md` is the append-only
build log (§1–§134).

| | |
|-|-|
| Last updated | 2026-10-07 (§134 — a `no_changes` with a PR on record goes to the owner, not closed) |
| Current work | agent-platform rewrite (`docs/waves/PLAN.md`) — Waves 1–6 done; spec `~/SourceRoot/dotfiles/docs/agent-platform.md` |
| Repo | `master`, pushed to `jkrumm/warden` (public); five LaunchAgents run this checkout |
| Ledger | `~/.warden/warden.db`, schema 16 |
| Tests | `make check` — `tests/test_triage.py` at the count AGENTS.md names |

## What is live

The whole loop in `DESIGN.md` § The loop runs: one intake pool → single-shot sideclaw
triage (`attach | new | fixed_by | ignore`) → investigate → implement at any
confidence, revisions on the same PR up to 4 attempts → one merge train per repo
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
- **Merge gate:** unreadable check runs fall back to Actions runs; an empty
  fallback waits (no timeout) — a private repo with no workflows and an unreadable
  checks API never merges until the PAT can read checks. A PR merged by hand
  without a review at its head verifies by signal only.
- **Ledger:** every pending migration first writes
  `~/.warden/backups/pre-migration-v<from>-to-<to>-<stamp>-<pid>.db` (never pruned).
- **`failed` is classified.** Every `failed` row carries `failure_class` (`infra` |
  `policy` | `work`) and a re-entry recipe (`redrive_json`). The loop re-drives infra
  after 60/180/480 min (≤3, `redrives` is that budget; it resets once the item reaches
  merging+), policy once per change of sideclaw's `GET /api/dispatch-policy` hash; work
  waits for `warden retry <id> [--why]` or Argo's Retry button. A hand-reverted item is
  never re-driven. `failed` still never posts; the daily count does.
- **Board after the W7 backlog pass:** 12 `failed` before the deploy (9 closed as
  superseded/fixed), 2 `needs_decision`.

## Open — owner actions

- The fine-grained PAT (`op://mini/github/token`) lacks `Checks: Read` (and
  `Actions: Read`) on private repos: weatherorb's check runs read 403, so the train
  refuses its merges (`failed`). It also lacks `Issues: Read` on `dispatch-scratch`
  and `Issues: Write` repo-wide (issue comment-back 403s).
- Repo contract gaps the train hits: `free-planning-poker`, `homelab-private`,
  `basalt-ui` have no `make deploy`. (`vps`'s `make deploy` without `APP=` deploys
  the affected apps, forwarded to the VPS; `homelab` master has `check`/`verify` and
  the contract sections, 9bc1b5d.)
- sideclaw serves `dispatch_implement_escalation`; attempt 3+ escalates to it.
- hermes-agent's sideclaw ceiling is `investigate`: its 6 policy-failed items
  (1114, 1420–1424) re-drive once after the deploy, are refused again, and wait for
  the ceiling to change. Lift it in sideclaw or close them.
- sideclaw#10 (item 1302) is an owner call (keep matching `op://` pointers in the diff
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
- **Review** is not delta-only (sideclaw has no PR delta scope or reviewed SHA).
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

## Next action

Watch the first live re-drives after the W7 deploy: 1398/1400 (infra → re-investigate),
1370 (dotfiles PR #12 → merge train), 1389/1390 (owner-retried into the train). Then
the first warden PR that rides the train end to end and the first revert in the field;
then the carried debt above, concurrency first.
