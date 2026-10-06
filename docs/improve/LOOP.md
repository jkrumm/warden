# Improvement loop — brief

A long-lived herdr tab (`improve`, warden workspace) runs Claude Code's `/loop` with
"Read docs/improve/LOOP.md and run one iteration." Each iteration measures the agent
platform, picks the single biggest friction, gets it fixed, and writes one journal
line. Spec it serves: `~/SourceRoot/dotfiles/docs/agent-platform.md`.

## One iteration

1. **Measure** (read-only, cheap):
   - warden: `curl -s localhost:7735/health`, `/board`, `/metrics`; ledger funnel since
     the last journal entry (read-only sqlite, `?mode=ro`): items in, fixed, failed by
     `failure_class`, needs_decision, closed as duplicate/fixed_by, re-drives, median
     time new → fixed, merge-train refusals, auto-reverts.
   - sideclaw: `curl -s localhost:7705/api/jobs/health`, `job.fail` events in
     `~/Library/Logs/sideclaw.jsonl` since the last entry, degraded routes.
   - Hermes: replies in #agents / #hermes since the last entry and their length
     (`~/.hermes/state.db`, read-only); any reply over 3 lines is friction.
   - Argo `/warden`: every few iterations, have `@verifier` screenshot it and confirm
     the "needs you" list equals warden's `needs_decision` items and nothing else.
2. **Pick one** friction, ranked by: owner was paged without a real question >
   work stuck or lost > duplicate work > wasted cost > noise. If nothing ranks,
   write "quiet" and schedule the next wakeup long.
3. **Fix it**, preferring the platform's own lanes:
   - `warden run <repo> <<'BRIEF' … BRIEF` for anything in a repo warden can work on
     (including warden itself). This is the default — it also exercises the loop.
   - Only when the loop cannot fix itself (the bug blocks intake/train/deploy):
     a git worktree of the affected repo, a minimal fix, `make check`, `/review`,
     fast-forward, `make deploy`.
   - At most **one** improvement of yours in flight at a time; check the previous
     one landed (`fixed`) before filing the next.
4. **Journal**: append one line to `docs/improve/JOURNAL.md`
   `YYYY-MM-DD HH:MM · metric snapshot · friction · action (item id / commit) · result of the previous action`,
   commit (`docs(improve): …`), push.
5. **Pace**: next wakeup 1–2 h when something is in flight or broken, 4–6 h when quiet.

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
