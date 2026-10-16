# warden — design

**warden turns a signal into a verified outcome and is the only thing that holds
that state.** Pollers feed it, agent-gateway executes for it, Argo and Slack render it.
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
(its `decisionQuestion` is the Slack line, led by a `[category]` tag) that names an owner-only
`escalationCategory` (product, data_loss, spend, other_people, security, blocker). An investigate
verdict that names none is sent back once to `triaged` ("re-driven to decide", the brief quotes its
own question and says decide it yourself); a second one takes its `recommendation` (implement, or
close with the answer on an investigate-only item) — unless that verdict's optional
`owningRepo` names a different known repo, which re-routes the item to `triaged`
in that repo instead (once per item; the re-route's transition note is the
ping-pong guard). `needs_decision` and `failed` never expire. Every
infrastructure failure strikes (`core.strike()`): 10 then 30 minutes of backoff,
the third strike is `failed` carrying the error. An agent-gateway 4xx on submit is a
refusal, not a strike: the item ends `failed` with agent-gateway's message — except a
refused escalation `model` (resubmitted once without it), a lease refusal (retry
in 10 minutes, no strike) and a refused triage submit (strikes; never the item's
fault).
An implement whose repo check TOOL failed to run (agent-gateway's `checks_tool_failed`,
dispatch schema v5) is an infrastructure failure too: it strikes and never spends a
revision — only a red suite (`checks_failed`) goes back to the implementer.
An implement that found nothing to change (`no_changes`) is the opposite: a terminal
answer, not infrastructure. An attempt with nothing on record closes `closed(resolved)`
with its summary; a revision, or an attempt with a pull request already on record (which
would have to touch it), goes to `needs_decision` instead, its note naming the open PR —
never a strike and another episode. An implement whose repo agent-gateway caps below
`implement` is never submitted to: `maybe_auto_implement()` reads the dispatch policy once
per pass and parks the item `needs_decision` with
`<repo> is <ceiling>-only for agents — apply by hand: <recommendation>`, so the
recommendation is not buried in a `failed(policy)` (a repo with no rule has no ceiling; an
unreadable policy submits as before).

`failed` is classified where it happens (`failure_class`) and is not a graveyard for
what was never the work's fault. `infra` (agent-gateway 5xx/unreachable, a synthesis
failure, the third strike) is re-driven after 60, 180, 480 minutes, three times;
`policy` (an agent-gateway 4xx refusal) once whenever agent-gateway's
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
   The `job` source joins them without a poller: each pass runs `jobs-grade`
   (`clients/jobs_grade.py`) and a label graded `failing | stale | flapping |
   unloaded` is one event `job:<machine>:<label>` (`ingest_jobs()`, folded by
   watchdog-poll.py's `reconcile()` as a `uk` monitor is). It is level-based: one
   event while the grade stays bad, resolved when the label grades `ok` or leaves
   the output, re-armed when it turns bad again. Its item has origin `job`,
   `max_tier=investigate`, and is routed by `route_job_items()` straight to the
   `owner` repo its state file names — no triage job, no debounce (the grader's
   own thresholds are the debounce). An owner warden cannot dispatch to
   (`?`, a repo outside the repos root) is `needs_decision`, not a guess. A pass
   in which `jobs-grade` cannot answer is blind: nothing opens, nothing resolves.
   `classify()` closes `ignore`-list and chat-prose alerts `closed(ignored)` with
   no model call. A `new` alert whose signal goes quiet resolves `quiet`
   (recovery-paired first, `quietResolveHours` as the fallback); silence resolves
   `new` and nothing else.
2. **Triage** (`triaging.py`). Each `new` item gets one single-shot agent-gateway
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
   `rootCause` and a `nextAction`. `implement` dispatches at any confidence —
   review is the gate. A `human` verdict whose optional `owningRepo` names a
   different known repo re-routes the item to `triaged` in that repo instead of
   paging (`_reroute_repo()`, once per item). A reopened `job` item is CHRONIC when
   warden had already acted on it (it has a `dispatch_job`) and it reopens within
   `jobChronicWindowDays` (7) of the terminal state it left, `jobChronicRecurrences`
   (1) times; a `fixed` one that fails again within `jobRegressionHours` (24) is a
   fix that did not hold. Both go to `needs_decision` (`[chronic] …`) instead of
   another dispatch (`job_chronic_reason()`; thresholds in `config/triage-policy.json`).
   An item that healed `quiet` before any dispatch is new work, not chronic. A matching `rootCause` on another
   open item merges them (older survives, the other `closed(duplicate)`). A
   blocked review is a revision on the same item and PR (`revisionOf`). The review's context
   (`work.validation_context()`) carries the owner's brief and the investigation's goal; when both
   are present it says the brief wins where they disagree (`VALIDATION_BRIEF_WINS`), so a drift
   between them is judged against the brief, never escalated as an owner question. The cap counts
   only STALLED rounds (`triage_items.stalled_rounds`, `MAX_IMPLEMENT_ATTEMPTS` = 3): a
   revision's review context quotes the previous round's blocking findings and asks the
   reviewer to begin any of them still unresolved with `UNRESOLVED PRIOR FINDING:`
   (`work.UNRESOLVED_PRIOR_MARKER`, matched case-insensitively at the start of the message).
   A blocked round with no such finding is converging (the earlier findings are fixed, the
   new ones consequences of the narrow fix) and keeps revising up to the absolute ceiling
   `core.MAX_REVISIONS` = 5 on `revision_count`; a round with
   the marker, a first review (nothing to compare with), red checks, a conflict or a revert
   is stalled, and the third stalled round ends the item for the owner (`stalled: N
   revisions did not resolve …`). The ceiling stops a reviewer that drifts to a new narrower
   finding every round (items 1478, 1480, 1565): a review that still blocks at `revision_count`
   6 lands `needs_decision` with its findings (`revision ceiling (5), review still blocks: …`),
   while a review that does not block is `confirmed` and goes to the merge train as always —
   the ceiling never loosens what blocks a merge. One finding is read as resolved, not as a
   block: a blocking finding on a file an earlier blocked round of the same PR named, which the
   PR's current file list (`github.pr_files()`, every page) no longer contains — the revision
   dropped that change (item 1559; `train._repeats_resolved_by_diff()`). Exact file match; any
   unreadable listing drops nothing; the drop is named on the hop note. Red checks and a conflict at the ceiling stay
   `failed` (PR left open). `revision_count` is otherwise the attempt ordinal: the
   escalation model starts at attempt 3 (`implement_model()`). warden-only: agent-gateway's
   review schema is unchanged, the marker is text in the finding's message. `conflict`
   re-dispatches from the new base with the old diff as context. Confident host
   restarts run through `HOST_VERB_ALLOWLIST` and verify on
   `HOST_VERB_LIVENESS_MONITOR`.

   **Per-repo in-flight lock** (`policy.check_repo_not_in_flight()`). One implement episode per repo:
   a `merging` item, a `working` item with an `implement_job` (a claim sentinel, an episode, or a
   waiting revision), or the operations ledger. A waiting revision (`working`, its implement dispatch
   judged: `validation_status IS NOT NULL`) holds no episode, only a claim on the next one, so it
   does not block a revision of an OLDER item (`as_revision=True`, from `maybe_revise_blocked()`, and
   `open_episode()` threads it through): revisions go oldest first. Without that, two waiting revisions
   each count the other as in flight and neither is ever revised (the weatherorb deadlock, §167). A
   younger item with an episode, a claim, or a `merging` train still defers the revision.
4. **Merge train** (`train.py`). One per repo, oldest item first, single-flight:
   agent-gateway `update_pr` onto the latest base → GitHub checks green (or none — only
   a readable check-runs API may say so; unreadable — a fine-grained token has no Checks
   permission, nothing to grant — the gate reads Actions runs, and none there is still pending) on that SHA → review `confirmed` on that SHA → squash merge pinned to it. A head
   that moves goes back to `update`. A red check that also fails on the base the SHA was rebased onto
   (`train_base_sha`, captured at `update`) is the
   base's, not the PR's: the item parks (no strike, no revision) until the base goes green, re-entering
   `update` each park so it re-derives that base against the live branch tip rather than a frozen
   red commit. A red the base does not share first re-runs the failed Actions jobs once on that
   same SHA (`ci_rerun_sha`, `_rerun_once()`): a flaky test is not the PR's defect. Only a second
   red on the SHA, nothing re-runnable, or a refused re-run request goes back for a revision. The gate is exactly those four facts plus
   GitHub's own rules (`lifecycle.merge`); `warden merge --confirm` uses the same
   gate and additionally requires a reviewed pin — the train's SHA while merging,
   else the item's last review-confirmed head — and refuses without one (intended).
   A `needs-human` review labelled `product` or `blocker` that carries a fixable code
   finding goes back for a revision first (`blocked`) — the owner is paged only once no
   code finding is left, or the listing is process-only; `security`, `data_loss`, `spend`
   and `other_people` always reach the owner; one whose own
   pipeline failed (a reviewer session that did not examine the diff, marked by a
   `(review pipeline)` entry in the verdict's `discussions`) is an infrastructure failure
   and the review re-runs on the same SHA, never `needs_decision`.
   A PR found already merged is landed from the ledger only when its head is the
   pinned one and a review confirmed that head; merged by hand otherwise, the item
   verifies by signal only (no deploy, no `make verify`).
5. **Deploy + verify** (`verify.py`, `lifecycle/rollout.py`). A merged item waits
   in `verifying`. If the checkout is clean, on the default branch and ends at
   origin, it is fast-forwarded and `make deploy` runs (else a strike); then
   `make verify` if defined; then, for an alert, its own signal quiet for
   `VERIFY_WINDOW_HOURS`. No signal → `fixed` once `make verify` passes. A red
   `make verify` that was already red before the merge is not the merge's: the
   train probes `make verify` once just before it lands the pull request (on
   every merge attempt; a timeout or error is stored as a red probe, never a
   block or a strike) and keeps the result in `verify_baseline`. Red before and
   red after is pre-existing — no failing pass, no revert, the item judged on its
   own signal (or `fixed` at once with no signal), the red named in the note.
   A green or missing baseline, and a revert's own verify, behave as before. Every
   `make`/`git` call runs under a PATH widened with the host tool dirs
   (`rollout.HOST_TOOL_DIRS`), so a repo's recipe can reach `op`, `brew` or a
   `~/.local/bin` tool under launchd's minimal environment; a caller's own PATH
   in `extra_env` still wins.
6. **Revert.** Signal recurrence or three failing verify passes (a pre-existing
   red is not one, step 5) revert the merged
   commit through an implement episode (`git revert --no-edit <sha>`, nothing
   else) whose PR rides the same train with no revisions. A passing revert gives
   the item a fresh attempt with the evidence and the reverted diff; the failed
   fix counts as an attempt, the revert does not.
7. **Fixed-by sweep.** Every fix merge (never a revert) queues one agent-gateway
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

## The kill-switch

`warden pause [--reason <text>]` writes `~/.warden/paused` (JSON `{reason, since}`;
`$WARDEN_PAUSE_FILE` overrides, `$WARDEN_HOME` moves it); `warden resume` removes it.
`lifecycle/pause.py` is the only reader (`core.is_paused()` re-exports it): the loop reads
it at the start of every pass and again in each stage that starts something, the sweep
at the start of its pass, `/health` and the Argo snapshot on every build. It needs no
ledger, backend or agent-gateway, so it works when those are the problem.

**While paused, held:** every episode start — investigate (`escalate`,
`escalate_origin_items`, so `warden run` opens and triages its item but dispatches
nothing), implement (`maybe_auto_implement`), revision (`maybe_revise_blocked`), revert
(`maybe_submit_reverts`), host verbs (`maybe_auto_remediate`), `warden dispatch`, the
owner's Argo `implement` — plus the whole merge train (`advance_merge_trains`: `update_pr`,
review, merge) and the **deploy** of a merged item (`maybe_verify` skips an item with no
`verify_started_at`). Under all of them `dispatch.open_episode()` / `open_review()` refuse
with `PausedError` (a `PolicyError`, exit 4), which the callers already read as a deferral.

**While paused, running:** ingest (alerts, GitHub issues, `jobs-grade`), `classify`, the
silence paths, the `job` routing, every triage job and its fold (a classification, not an
action), polling and folding episodes already in flight, `make verify` on an
already-deployed item (observation; its revert, should it fail, is held), the fixed-by
sweep, Slack lines, the Argo snapshot, `redrive_failed` (state only), the owner's explicit
Argo `merge` / `warden merge` (an explicit human act on a PR the owner chose) and every state-only
CLI verb (`close`, `retry`, `reinvestigate`, `adopt`: they move an item, the held stage pollers
do the starting). Conservative on purpose where it touches production: the loop's merge and
deploy are held even though neither starts an agent job. The owner's explicit merge is the one
deliberate exception — a human act, not the loop's — so the switch stops what warden does on its own,
not what the owner does by hand.

Fail closed: a flag file that exists but cannot be read or parsed still pauses; only a
missing file is "running". `/health` carries `paused: null | {reason, since}` and a pause
never fails `ok` (a deliberately idle warden is healthy, and `make deploy`'s health gate
must not roll back for it). Nothing is posted to Slack for a pause.

## Boundaries

- **agent-gateway is the only boundary.** warden depends on `submit(tier, repo, brief)
  -> jobId` and `get(jobId)`. It carries no repo allowlist, tier ceiling or model
  choice; agent-gateway enforces its allowlist and routes each tier
  (`GET /api/routing`). The verdict schema is agent-gateway's; a version mismatch is a
  loud refusal, never a best-effort parse.
- **The episode is not contained.** On a `claude` route `readOnly` is three tool names on a CLI
  flag and `Bash` is unrestricted; on an OpenCode route (the dispatch default) it is the
  `agw-readonly` agent, an allow-listed shell that is a cost-raising deny list, not a boundary.
  Either way the brief is
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

Ten details that read like accidents and are not. Check against them before
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
9. **The dry-run contract**: never touches Slack, never shells out (the `jobs-grade`
   subprocess included), never submits a triage job, everything else real. It is the only
   pre-production surface.
10. **Pause holds starts, never observation, and fails closed.** Every site that starts an
   agent job, a host verb, a merge or a deploy asks `core.is_paused()`; ingest, triage,
   polling and folding do not. A new dispatch site without that gate (and without the
   `open_episode()` backstop behind it) is a hole in the only kill-switch.

## Known limits

- Review is not delta-only: agent-gateway `review` has no PR delta scope and reports
  no reviewed SHA; warden pins by reading the PR head before submit and after the
  fold.
- Policy `rules` remain a label tier until the `## Verify & Monitor` sections
  cover the fleet and a triage replay routes at least as well without them.
- While `make verify` stays red, a second regression is not attributed to its
  merge: the baseline only tells "red before" from "red after", not which red.
  The red is its own alert, and alert items are still judged by their own signal.
- warden deploys itself: a merged warden fix is fast-forwarded into the live
  checkout and `make deploy` kickstarts `warden-api`; the periodic agents read the
  new code on their next tick. A migration in that code runs on the loop's next
  boot, after the deploy's own health check.

History — the measured failures behind each rule, four design reviews, the old
flows — is `docs/history/` and git.
