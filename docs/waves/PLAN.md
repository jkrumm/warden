# warden — a self-driving loop with quality gates only

**Goal:** warden matches `~/SourceRoot/dotfiles/docs/agent-platform.md` §Warden: nine
states, single-shot triage dedup, one merge train per repo, `make deploy` /
`make verify` instead of per-repo config, `needs_decision` as the only human exit,
terse output.

**Gate:** `make test` green (and `make check` once W5 adds it), plus `/review` on each wave's diff.

**Spec:** `~/SourceRoot/dotfiles/docs/agent-platform.md` — read it first. The audit
numbers behind it are in its "Why this rewrite" table. Where this repo's
DESIGN.md / docs contradict the spec, the spec wins; W5 rewrites the docs.

**Live system:** the LaunchAgents run from this checkout. The orchestrator paused
`warden-loop`, `warden-sweep` and `warden-poll` before W1; they stay paused until
the orchestrator redeploys. Never `launchctl` or `make agents` from a wave. The
ledger is `~/.warden/warden.db` — schema changes ship as a migration in
`ledger.py`, tested against a copy (`cp ~/.warden/warden.db /tmp/`), never run
against the live file by a wave.

**Model:** spawn successors with `RD_WAVE_MODEL=opus`.

**Deleting is the job.** Delete code together with its tests and docs. No
compatibility shims, no feature flags for removed gates. Git history is the archive.

## Wave 1 — cut the gates            <!-- status: active -->
- [ ] Delete the signed-approval stack: `lifecycle/approvals.py`, `clients/signer.py`, `intents.py`, `config/approval-spec.json`, the `warden dispatch --tier implement` approval path and `--why`, the `dispatch_approvals` table (migration drops it). Leave a note for Hermes (its `plugins/dispatch-approval` is removed in hermes-agent's plan).
- [ ] Delete warden's repo/tier policy: `config/dispatch-repos.json`, `policy.resolve_repo/resolve_tier`, `check-dispatch-policy.py`, `check-routing.py`, `TRIAGE_*_MODEL` env knobs. sideclaw is the only boundary; a sideclaw 4xx marks the item `failed` with the message — never a retry loop.
- [ ] Merge gate shrinks to: PR open, checks green (or none exist), review `confirmed`, GitHub rules allow. Delete `EXECUTOR_REPOS`, `merge_approval`, `autoMergePaths`, `noCiRequired`, the size/`.github`/ceiling checks, `_MERGE_RETRY_CAP` and the policy-mtime retry.
- [ ] Delete meta-machinery: reminders + parked-recurrence reminders, self-audit invariants, stranded-PR sweep, `chaos.py` + crash points, restore drill (keep the backup), `check-schemas` cross-checks that duplicate sideclaw, `propose_mappings` (W3 replaces routing), the env-check verb, `warden_self` items. Relax `require_no_recursion` so agents may create items (`warden run`).
- [ ] Auto-implement on any verdict with `nextAction=implement` (drop the confidence gate and the 24 h `verdict` wait). A review synthesis/serialization failure retries the review instead of `needs_human`.
**Left behind:**

## Wave 2 — nine states, terse output, one queue            <!-- status: pending -->
- [ ] States → `new, triaged, working, merging, verifying, fixed, needs_decision, failed` + terminal `quiet, closed(reason)`. Ledger migration maps every old state (document the mapping in the migration). Replace the 12-row deadline table with one rule: infra failure → retry with backoff, 3 strikes → `failed`; `needs_decision` and `failed` never expire silently.
- [ ] `needs_decision` only when the verdict carries `decisionQuestion` (sideclaw W1 adds it; until then derive from `nextAction=human` + summary). Everything else that used to page goes to retry or `failed`.
- [ ] Output: Slack posts only on `fixed` and `needs_decision`, one line `<icon> <repo>: <summary> — <state> [Argo link]`. Delete card chrome (snooze footer, countdown, evidence lines, digest of non-actionable items). Item `note` ≤200 chars. GitHub issue comment-back ≤3 lines.
- [ ] Argo `/warden` reads the new states (`~/SourceRoot/argo` `features/warden/*` + `api/src/routes/warden.ts`): one "needs you" list (= `needs_decision`), remove the intents/approve UI. Argo deploys via CI on push — commit there too, gate with its own `bun run` checks.
**Left behind:**

## Wave 3 — intake and dedup            <!-- status: pending -->
Requires sideclaw Wave 1 (`triage` job, `rootCause`) — check `~/SourceRoot/sideclaw/docs/waves/PLAN.md`; if not done, stop and say so.
- [ ] Fingerprint: normalize titles (strip timestamps, hex ids, UUIDs, paths, numbers, log-file name) in `watchdog-poll.py` and intake; the same line from two log files is one event. Backfill-test against the 1,406 events in a ledger copy: report the item count before vs after.
- [ ] Triage step via sideclaw `triage`: input = new event + open items of candidate repos + items fixed in the last 14 days with PR titles; output `attach | new(repo,title) | fixed_by | ignore`. Issues, alerts and `warden run` share one pool. Route by the signal's own label (Kuma tag, OTel `service.name`, GitHub repo) first; triage decides the rest from the candidate repos' `## Verify & Monitor` sections. Delete the 77 glob rules and 15 ignores once the replay shows equal or better routing.
- [ ] Revisions are attempts on the same item (sideclaw `revisionOf`, W2 there), never new items. Up to 4 attempts; attempt 3+ uses the escalation implement model from sideclaw's registry.
- [ ] Root-cause merge: a verdict whose `rootCause` matches another open item's merges them (keep the older item, close the other `closed(duplicate)`).
**Left behind:**

## Wave 4 — merge train, deploy, verify, revert            <!-- status: pending -->
Requires sideclaw Wave 2 (`update_pr`, per-repo lease).
- [ ] One merge train per repo, single-flight: `update_pr` onto latest base → wait for checks on that SHA (poll, no fixed budget) → review on that SHA (delta-only when a prior review confirmed) → squash merge pinned to the SHA. Conflict → new attempt from the new base with the old diff as context.
- [ ] Deploy = `make deploy` in the repo (delete `clients/rollout.py`'s argv table, `deployOnMerge`, `deployByPoller`); verify = `make verify` when present + the item's own signal quiet for its window (keep the generic Kuma own-monitor probe; delete the per-repo liveness keys and evidence gatherers). A repo without `make verify` verifies by signal only.
- [ ] Verify failure → automatic revert PR through the same train, item back to `working` with the evidence.
- [ ] Fixed-by sweep after every merge: triage re-checks the repo's open items against the merged diff; matches move to `verifying`.
**Left behind:**

## Wave 5 — docs and shape            <!-- status: pending -->
- [ ] DESIGN.md ≤200 lines describing what exists, linking the spec; archive `docs/triage.md`, `never-auto-merge-widening.md`, `handover-field-review.md`, the state-log to `docs/history/`. AGENTS.md gets `## Validate`, `## Deploy`, `## Verify & Monitor`, `## Gotchas`; Make targets `check`, `deploy` (restart LaunchAgents, health-check, roll back to the previous commit on failure), `verify`, `logs`.
- [ ] Split `triage.py` into modules along the loop (intake, triage, work, merge, notify) and drop history comments (git has them). No behaviour change; tests prove it.
- [ ] Put `warden` on PATH via `make setup` (`~/.local/bin/warden`).
**Left behind:**
