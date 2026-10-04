# STATE — warden

**Read this first. It is the memory; the conversation is not.** `DESIGN.md` is what
warden is; this file is where it is; `docs/history/state-log.md` is the append-only
build log (§1–§120).

| | |
|-|-|
| Last updated | 2026-10-04 (§120 — agent-platform Wave 5: docs and shape) |
| Current work | agent-platform rewrite (`docs/waves/PLAN.md`) — Waves 1–5 done; spec `~/SourceRoot/dotfiles/docs/agent-platform.md` |
| Repo | `master`, pushed to `jkrumm/warden` (public); five LaunchAgents run this checkout |
| Ledger | `~/.warden/warden.db`, schema 15 |
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
  commit before the fast-forward), `make verify` (schema + loop heartbeat), `make logs`; `make setup` writes
  `~/.local/bin/warden`.
- **Self-deploy.** A merged warden PR is fast-forwarded into the live checkout by
  the loop and deployed by `make deploy`; Hermes's `warden-live-sync` cron is gone.
- **Board at close-out of W5:** 1 `needs_decision`, 21 `failed`, nothing in flight.

## Open — owner actions

- The fine-grained PAT (`op://mini/github/token`) lacks `Checks: Read` (and
  `Actions: Read`) on private repos: weatherorb's check runs read 403, so the train
  refuses its merges (`failed`). It also lacks `Issues: Read` on `dispatch-scratch`
  and `Issues: Write` repo-wide (issue comment-back 403s).
- Repo contract gaps the train hits: `vps`'s `make deploy` needs `APP=` (a merged
  vps fix strikes to `failed` at deploy); `homelab` has no `make verify` (signal
  only); `free-planning-poker`, `homelab-private`, `basalt-ui` have no `make deploy`.
- sideclaw has no `dispatch_implement_escalation` route, so attempt 3+ uses the
  default implement model.
- The 21 `failed` items are pre-train history; triage them in Argo (reinvestigate
  or dismiss).

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

## Next action

Agent-platform waves are complete. Watch the first warden PR that rides the train
end to end (merge → fast-forward → `make deploy` → `make verify`) and the first
revert in the field; then the carried debt above, concurrency first.
