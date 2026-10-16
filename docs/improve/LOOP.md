# Improvement loop — brief

A long-lived herdr tab (`improve`, warden workspace) runs Claude Code's `/loop` with
"Read docs/improve/LOOP.md and run one iteration." It is **outcome-triggered, not hourly**: an
iteration that finds nothing new costs one script call, no commit and no journal line. Each real
iteration measures the agent platform, picks the single biggest friction, gets it fixed, and writes
one journal line. Spec it serves: `~/SourceRoot/dotfiles/docs/agent-platform.md`.

## Trigger (step 0, every wakeup)

`scripts/improve-trigger.py` prints one line per item that landed `failed` or `needs_decision`
since the cursor (`~/.warden/improve.cursor`), or `quiet`.

- `quiet` **and** nothing of yours in flight: stop here. No journal line, no commit, no metrics.
  Schedule the next wakeup at the maximum (60 min) — the cost of a quiet check is one script run.
- Lines printed: they are the iteration's evidence. Run it, then `scripts/improve-trigger.py --ack`
  so the same outcomes are not read twice.
- Something of yours is in flight (`warden status`): look at it only, then wake in 20–30 min.

## One iteration

1. **Measure** (read-only, cheap):
   - warden: `curl -s localhost:7735/health`, `/board`, `/metrics`; ledger funnel since
     the last journal entry (read-only sqlite, `?mode=ro`): items in, fixed, failed by
     `failure_class`, needs_decision, closed as duplicate/fixed_by, re-drives, median
     time new → fixed, merge-train refusals, auto-reverts.
   - agent-gateway: `curl -s localhost:7705/api/jobs/health`, `job.fail` events in
     `~/Library/Logs/agent-gateway.jsonl` since the last entry, degraded routes.
   - OpenCode episodes (agw; evidence in `docs/opencode-v2.md`), since the last entry:
     - server health: `curl -s localhost:7705/api/jobs/health | jq .opencodeServer` (`ok`, `version`
       == pinned); restarts = `launchctl print gui/$UID/com.jkrumm.agent-gateway-opencode | grep -E 'pid|runs'`.
       Any `ok:false` is a page-worthy friction.
     - transport errors: `jq 'select(.event=="session.opencode_error" or .event=="session.opencode_poll_slow")'`
       on the jsonl. The message names `[METHOD path after Nms]`; a recurring route is the item to file.
     - re-attach after agw restarts: `session.opencode_detached` vs `session.opencode_reattach` counts
       (+ `job.recover`). Detached without a matching reattach = a lost episode.
     - steers: `job.steer` events per dispatch job; compare the steered job's outcome with the median.
       Fewer than ~5 steers total is no signal — say so, don't conclude.
   - Per-route cost and pass rate: pass rate from `sqlite3 -readonly ~/.local/share/agent-gateway/jobs.db
     "select tool,model,status,count(*) from job_stats where finished_at > <ms> group by 1,2,3"`;
     cost from `~/.local/share/usage-tracker/agent-gateway-sessions.jsonl` (`costUsd` by `tool`, `model`,
     `outcome`). `job_stats.model` is often empty — join on tool, not model. A route whose pass rate
     drops or whose $/pass doubles is a friction candidate; `GET /api/routing` names the route's model.
   - Decision-model shadow: `python3 scripts/lifecycle/shadow.py report` (agreement by confidence
     bucket, rows by status, cost). **Promotion check each iteration:** if the rule in
     `docs/decision-shadow.md` holds (≥ 50 settled rows, ≥ 95% action / ≥ 90% target agreement at
     confidence ≥ 0.95, no new→ignore), file the promotion as a normal `warden run` item. Until then
     report only "N/50 rows" in the journal line — never promote on the replay.
   - Route moves to the OpenCode Haiku route (agw `docs/routing-and-quota.md`, 2026-10-15): on
     frontend-heavy PRs, compare the `frontend` angle's findings with what a later fix names
     (a miss or a nit flood is the friction; the revert is one env line in that doc). An OpenCode
     server restart still kills attached episodes: a restart in the window plus lost episodes is the
     item to file, not a re-drive.
   - Hermes: replies in #agents / #hermes since the last entry and their length
     (`~/.hermes/state.db`, read-only); any reply over 3 lines is friction.
   - Argo `/warden`: every few iterations, have `@verifier` screenshot it and confirm
     the "needs you" list equals warden's `needs_decision` and `failed` items (`awaiting_owner`
     in `scripts/api.py`: only an owner action moves either) and nothing else.
2. **Pick one** friction, ranked by: owner was paged without a real question >
   work stuck or lost > duplicate work > wasted cost > noise. If nothing ranks after
   measuring, say so in one line of chat — no journal entry.
3. **Fix it**, preferring the platform's own lanes:
   - `warden run <repo> <<'BRIEF' … BRIEF` for anything in a repo warden can work on
     (including warden itself). This is the default — it also exercises the loop.
   - Only when the loop cannot fix itself (the bug blocks intake/train/deploy):
     a git worktree of the affected repo, a minimal fix, `make check`, `/review`,
     fast-forward, `make deploy`.
   - At most **one** improvement of yours in flight at a time; check the previous
     one landed (`fixed`) before filing the next.
4. **Journal** (only when you acted or found something): append one line to `docs/improve/JOURNAL.md`
   `YYYY-MM-DD HH:MM · metric snapshot · friction · action (item id / commit) · result of the previous action`,
   commit (`docs(improve): …`), push.
5. **Pace**: 20–30 min while something is in flight, the 60 min maximum otherwise (see Trigger).
   A quiet wakeup writes nothing.

## Rules

- Never ask the owner to confirm anything. A genuine decision (product, irreversible
  data, spend, another person, security policy) goes into the journal as
  `DECISION:` with two options and your recommendation, and you move on.
- Never touch weatherorb's running mother/lead processes, `homelab-private`, live
  ledger writes, or LaunchAgents outside a repo's `make deploy`.
- Never add a gate, approval, budget or per-repo rule to make a problem go away —
  fix the cause. Quality gates (CI, review, verify) are the only gates.
- Keep docs in step: any behaviour you change updates the owning AGENTS.md/DESIGN.md
  in the same commit; anything that changes the model updates agent-platform.md.
- Model ids never in prose — `GET /api/routing`.
