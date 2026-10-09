# warden — design

**warden turns a signal into a verified outcome and is the only thing that holds
that state.** Pollers feed it, sideclaw executes for it, Argo and Slack render it.
It is the "Warden — the loop" part of the platform spec,
`~/SourceRoot/dotfiles/docs/agent-platform.md`; where this file and the spec
disagree, the spec wins. This file describes what exists in this repo.

## Why a separate repo

A control plane cannot live inside the thing it supervises. On 2026-09-07 the
Hermes gateway crash-looped seven times in 3m34s while a gateway-hosted act-loop
kept ticking against a ledger that had stopped receiving signals. So warden runs
on its own LaunchAgents, never `hermes cron`, and posts to Slack through a plain
HTTP client under its own app identity, never the gateway's connection.

| Agent | Runs | Interval |
|-|-|-|
| `com.jkrumm.warden-loop` | `scripts/triage.py --run` — the loop below | 600s |
| `com.jkrumm.warden-poll` | `scripts/watchdog-poll.py --post` — intake | 1800s |
| `com.jkrumm.warden-sweep` | `scripts/dispatch-sweep.py` — folds finished episodes, then runs the implement chain | 300s |
| `com.jkrumm.warden-backup` | `scripts/warden-backup.sh` | daily 03:10 |
| `com.jkrumm.warden-api` | `scripts/api.py --serve` — `GET /health /metrics /board /items/<id>` on `127.0.0.1:7735` | KeepAlive |

## States

```
new → triaged → working → merging → verifying → fixed
        ↑          │          │          │
        └─ re-route┤          │          │
                   └──────────┴──────────┴──→ needs_decision | failed
quiet · closed(duplicate | fixed_by | ignored | resolved)      terminal
```

`needs_decision` is the only human exit, reached by a `nextAction=human` verdict
(its `decisionQuestion` is the Slack line) — unless that verdict's optional
`owningRepo` names a different known repo, which re-routes the item to `triaged`
in that repo instead (once per item; the re-route's transition note is the
ping-pong guard). `needs_decision` and `failed` never expire. Every
infrastructure failure strikes (`core.strike()`): 10 then 30 minutes of backoff,
the third strike is `failed` carrying the error. A sideclaw 4xx on submit is a
refusal, not a strike: the item ends `failed` with sideclaw's message — except a
refused escalation `model` (resubmitted once without it), a lease refusal (retry
in 10 minutes, no strike) and a refused triage submit (strikes; never the item's
fault).
An implement whose repo check TOOL failed to run (sideclaw's `checks_tool_failed`,
dispatch schema v5) is an infrastructure failure too: it strikes and never spends a
revision — only a red suite (`checks_failed`) goes back to the implementer.
An implement that found nothing to change (`no_changes`) is the opposite: a terminal
answer, not infrastructure. An attempt with nothing on record closes `closed(resolved)`
with its summary; a revision, or an attempt with a pull request already on record (which
would have to touch it), goes to `needs_decision` instead, its note naming the open PR —
never a strike and another episode.

`failed` is classified where it happens (`failure_class`) and is not a graveyard for
what was never the work's fault. `infra` (sideclaw 5xx/unreachable, a synthesis
failure, the third strike) is re-driven after 60, 180, 480 minutes, three times;
`policy` (a sideclaw 4xx refusal) once whenever sideclaw's
dispatch policy hash differs from the one stored with the refusal, so a refusal
under the new policy waits for the next change; `work` (checks failed, review
blocked past the last attempt, a rewind loop, a revert by hand) never. A failed row
carries `redrive_json` — the state to re-enter and the columns that clear the failed
attempt's handle — and a re-drive (`redrive_failed()`, first in the pass) puts it
back there silently, keeping the PR and the revision count. `warden retry` and
Argo's `retry` do the same for any class with a fresh budget.

## The loop

One pass of `triage.run()`; module per stage under `scripts/loop/`.

1. **Intake** (`intake.py`, `watchdog-poll.py`). Every source writes an event
   keyed by `fingerprint(title)` — timestamps, hex ids, UUIDs, paths, numbers and
   the log-file name stripped, so the same line from two log files is one event.
   Alerts, GitHub issues and `warden run` become `new` items in one pool.
   `classify()` closes `ignore`-list and chat-prose alerts `closed(ignored)` with
   no model call. A `new` alert whose signal goes quiet resolves `quiet`
   (recovery-paired first, `quietResolveHours` as the fallback); silence resolves
   `new` and nothing else.
2. **Triage** (`triaging.py`). Each `new` item gets one single-shot sideclaw
   `triage` job — alerts once debounced (≥`minOccurrences` or ≥`minOpenMinutes`
   open), issues and runs at once. Candidates come from the signal's own label
   (`label_route()`: Kuma tag, container name, OTel `service.name`, the issue's
   repo; then a policy `rules` match — a rule is a label, not a route). With no
   label every checkout under the repos root with an `AGENTS.md` is a candidate and
   the job reads their `## Verify & Monitor` sections. The answer is
   `attach | new(repo, title) | fixed_by | ignore`, validated against the ledger
   before anything moves (`_fold_triage_job()`); `triaged` is its only output. A
   model's `ignore` reopens on recurrence after `cooldownHours`; a human's does not.
3. **Work** (`work.py`). `escalate()` clusters `triaged` items per repo and
   dispatches one investigate episode; origin items go through
   `escalate_origin_items()`. The verdict carries a ≤200-char `summary`, a
   `rootCause` and a `nextAction`. A reopened signature's brief carries the note of its most recent
   informative terminal transition (`_prior_resolution_note()`), capped and paired with the
   instruction to report `none` citing it when it still explains the occurrence. `implement`
   dispatches at any confidence —
   review is the gate. A `human` verdict whose optional `owningRepo` names a
   different known repo re-routes the item to `triaged` in that repo instead of
   paging (`_reroute_repo()`, once per item). A matching `rootCause` on another
   open item merges them (older survives, the other `closed(duplicate)`). A
   blocked review is a revision on the same item and PR (`revisionOf`), up to 4
   attempts; attempt 3+ asks for sideclaw's escalation model. `conflict`
   re-dispatches from the new base with the old diff as context. Confident host
   restarts run through `HOST_VERB_ALLOWLIST` and verify on
   `HOST_VERB_LIVENESS_MONITOR`.
4. **Merge train** (`train.py`). One per repo, oldest item first, single-flight:
   sideclaw `update_pr` onto the latest base → GitHub checks green (or none — only
   a readable check-runs API may say so; unreadable, the gate reads Actions runs
   and none there is still pending) on that SHA → review `confirmed` on that SHA → squash merge pinned to it. A head
   that moves goes back to `update`. The gate is exactly those four facts plus
   GitHub's own rules (`lifecycle.merge`); `warden merge --confirm` uses the same
   gate and additionally requires a reviewed pin — the train's SHA while merging,
   else the item's last review-confirmed head — and refuses without one (intended).
   A PR found already merged is landed from the ledger only when its head is the
   pinned one and a review confirmed that head; merged by hand otherwise, the item
   verifies by signal only (no deploy, no `make verify`).
5. **Deploy + verify** (`verify.py`, `lifecycle/rollout.py`). A merged item waits
   in `verifying`. If the checkout is clean, on the default branch and ends at
   origin, it is fast-forwarded and `make deploy` runs (else a strike); then
   `make verify` if defined; then, for an alert, its own signal quiet for
   `VERIFY_WINDOW_HOURS`. No signal → `fixed` once `make verify` passes. Every
   `make`/`git` call runs under a PATH widened with the host tool dirs
   (`rollout.HOST_TOOL_DIRS`), so a repo's recipe can reach `op`, `brew` or a
   `~/.local/bin` tool under launchd's minimal environment; a caller's own PATH
   in `extra_env` still wins.
6. **Revert.** Signal recurrence or three failing verify passes revert the merged
   commit through an implement episode (`git revert --no-edit <sha>`, nothing
   else) whose PR rides the same train with no revisions. A passing revert gives
   the item a fresh attempt with the evidence and the reverted diff; the failed
   fix counts as an attempt, the revert does not.
7. **Fixed-by sweep.** Every fix merge (never a revert) queues one sideclaw
   `triage` job over the PR's title, body and diff against the repo's `triaged`
   and idle `working` items. A match enters `verifying` with `fixed_by_pr` and
   closes `closed(fixed_by)` once its own signal stays quiet; recurrence sends it
   back to `triaged`. `-private` repos are never swept.
8. **Notify** (`notify.py`). Slack hears one line on `fixed` and
   `needs_decision` — `<icon> <repo>: <summary> — <state> <Argo link>` — plus a
   daily `failed` count. Argo `/warden` gets a snapshot every pass and is the
   queue; owner actions (`implement`, `merge`, `dismiss`, `reinvestigate`, `note`, `retry`)
   come back through `apply_argo_actions()` as `owner:argo`.

`dispatch-sweep.py` folds finished episodes every 5 minutes and runs the
implement chain (`work.advance_implement_chain()`) so a verdict does not wait for
the loop's 10-minute tick.

## Boundaries

- **sideclaw is the only boundary.** warden depends on `submit(tier, repo, brief)
  -> jobId` and `get(jobId)`. It carries no repo allowlist, tier ceiling or model
  choice; sideclaw enforces its allowlist and routes each tier
  (`GET /api/routing`). The verdict schema is sideclaw's; a version mismatch is a
  loud refusal, never a best-effort parse.
- **The episode is not contained.** `readOnly` is three tool names on a CLI flag
  under `--dangerously-skip-permissions`; `Bash` is unrestricted and the brief is
  attacker-influenceable (issues, alert text, log lines). A token on this host is
  not an authorization boundary against an episode.
- **The tailnet is the trust boundary.** Argo is reachable only over the owner's
  tailnet; an action pulled from its queue is the owner.
- **A policy file names and parameterises, never expresses.** Code owns every
  argv: `make -C <repo> deploy|verify` and `HOST_VERB_ALLOWLIST`.
- **warden reads nothing about a repo except its `AGENTS.md` and Makefile** (the
  repo contract in the spec).

## The ledger

`~/.warden/warden.db`, SQLite, WAL, `busy_timeout`, not in git. `scripts/ledger.py`
is the one migrator; only the loop migrates, at boot; every other process asserts
`schema_version` and refuses on mismatch. Read-only handles are
`file:…?mode=ro`. Backup is `VACUUM INTO` (never a copy of an open file), shipped
to homelab where restic already carries it to B2. `item_transitions` records
every state change; `operations` records every side effect with an outcome so a
crash mid-act is reconciled (`reconcile_operations()`), never repeated blindly.

## Observability

`/health` is schema version plus each poller's heartbeat age against 3× its
interval — the gateway-crash mode as an alarm instead of silence. `/metrics` is
the funnel (`docs/api.md` has the exact definitions; a number it cannot compute
is `null` with a reason, never `0`). `make status` and `make verify` read the
same facts; `make logs` tails every agent.

## What must not be lost

Nine details that read like accidents and are not. Check against them before
every merge.

1. **Silence may cancel the need to start work, never discharge a verdict or an
   in-flight operation.** Quiet-resolve applies to `new` only.
2. **Overflow waits, never drops.** Triage submits past the per-run cap stay
   `new`; cluster members past the cap stay `triaged`.
3. **Every act two crons can both reach is a compare-and-set claim first** — a
   submit, a handoff, a Slack post. The loop and the sweep drive one ledger.
4. **The triage submit and fold are compare-and-set on `triage_job`** (a
   `claiming:<time>` sentinel, released after 5 minutes). Entering `new` clears
   it; the fold clears it on every outcome except `ignore`, so a `closed(ignored)`
   row that still carries one is a model's ignore and an owner's dismiss never
   reopens.
5. **Triage never leaks a private repo.** A `-private` candidate is a name only;
   an item in one sends `(private repo — content withheld)`; everything else is
   fenced as untrusted data.
6. **Grouped-source policy patterns match `fingerprint(title)`**, which has no
   digits — a pattern with one is dead.
7. **A dispatch that ends terminal with no verdict is not a verdict.** It strikes;
   the third strike is `failed` carrying `dispatches.error`.
8. **Dissolving a cluster leaves `dispatch_job` set** as a cooldown anchor;
   clearing it lets `escalate()` re-fuse the pair in the same pass.
9. **The dry-run contract**: never touches Slack, never shells out, never submits
   a triage job, everything else real. It is the only pre-production surface.

## Known limits

- Review is not delta-only: sideclaw `review` has no PR delta scope and reports
  no reviewed SHA; warden pins by reading the PR head before submit and after the
  fold.
- Policy `rules` remain a label tier until the `## Verify & Monitor` sections
  cover the fleet and a triage replay routes at least as well without them.
- warden deploys itself: a merged warden fix is fast-forwarded into the live
  checkout and `make deploy` kickstarts `warden-api`; the periodic agents read the
  new code on their next tick. A migration in that code runs on the loop's next
  boot, after the deploy's own health check.

History — the measured failures behind each rule, four design reviews, the old
flows — is `docs/history/` and git.
