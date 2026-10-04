# warden — Developer Notes

**Read `STATE.md` before touching anything.** It is where the implementation
actually is: what is done, what is in flight, what failed and why, and the exact
next action. `DESIGN.md` is authoritative for *what warden is*; `FLOWS.md` for how
six real scenarios run end to end; `REVIEW.md` records what four reviews rejected,
so settled arguments stay settled. Do not re-litigate `REVIEW.md` without new
evidence, and say so explicitly if you have some. `docs/history/state-log.md` is
the verbatim build log (§§1–56 and onward, append-only); `STATE.md` itself is a
two-page current-state summary that gets rewritten each wave, while the log is
only ever appended to.

`DESIGN.md` § *What must not be lost* is nine details that read like accidents and
are not. Check against it before every merge.

## Architecture

warden is a deterministic control plane over one SQLite ledger. **Code owns every
state transition.** Its only LLM inputs are sideclaw's episode verdicts and the
single-shot intake triage answer (sideclaw `triage`), and each is validated against
the ledger (an open target, a candidate repo, the origin guards in
`_fold_triage_job()`) before any item moves; an answer that fails validation is
treated as `new` or strikes. Pollers feed it, sideclaw executes for
it, Slack and Argo render it.

**Intake is one pool.** Alerts, GitHub issues and `warden run` are `new` items;
each gets one triage job (alerts once debounce-eligible, the rest at once), and
`triaged` is that step's output — `escalate()`/`escalate_origin_items()` pick up
nothing else. Before triage, `classify()` closes `ignore`-list and chat-prose alerts
`closed(ignored)` with no model call. The signal's own label then picks the candidate
repos (`_label_route()`): a native label first (`lifecycle/intake.py`: a Kuma tag, a
container name, an OTel `service.name`, the issue's repo), then a policy `rules`
match — **a rule is a label, not a route**; triage still runs for a labelled item
(dedup: attach / fixed_by / ignore) with the label's repo as the only candidate. With no
label every checkout under the repos root with an `AGENTS.md` is a candidate and the job
reads their `## Verify & Monitor` sections. `rules` is the tier a later wave deletes once
those sections cover the fleet (replay: 86/86 rule-routed alerts keep their repo as a
label; triage alone matched 88 of 108 and misrouted a few for lack of repo knowledge).
A model's `closed(ignored)` is not forever: on recurrence after `cooldownHours` it
reopens to `new` (the ignore list and prose filter re-close it without a model call); a
human `--ignore` stays closed.

Six LaunchAgents, never `hermes cron` jobs, and that is the whole reason this
repo is separate from `hermes-agent`: a gateway cron job runs *inside* the
`ai.hermes.gateway` process, so the loop whose job is noticing Hermes is broken
cannot run when Hermes is down. That was measured, not theorized — the gateway
crash-looped seven times in 3m34s on 2026-09-07 while the act-loop kept ticking
against a ledger that had stopped receiving signals.

| Agent | Runs | Interval |
|-|-|-|
| `com.jkrumm.warden-loop` | `scripts/triage.py --run` | 600s |
| `com.jkrumm.warden-poll` | ingest | 1800s |
| `com.jkrumm.warden-sweep` | `scripts/dispatch-sweep.py` | 300s |
| `com.jkrumm.warden-backup` | `scripts/warden-backup.sh` | daily 03:10 |
| `com.jkrumm.warden-api` | `scripts/api.py --serve` (GET /metrics, /health) | long-running, `KeepAlive` |

`warden-api` is the odd shape: a long-running server, not a periodic job — see
its own plist template for why that changes `KeepAlive`/`StartInterval` and adds
a `ThrottleInterval`. It binds `127.0.0.1:7735` only, loopback and no auth — see
`docs/api.md` for the endpoints, the six funnel numbers' exact definitions, the
honesty rules (`null` + reason, never a fabricated `0`), and what is deliberately
not built yet.

Slack delivery from the loop is a **plain HTTP client** (one text-only
`chat.postMessage` per notification, with a token from `resolve_slack_token()`), never the gateway's live
`slack_bolt` connection. That is what makes a gateway-independent agent safe, and
it is not an implementation detail — it is the property. It posts under warden's
own Slack app identity, falling back to Hermes's token until that app is seeded —
see `slack/README.md` for creating and seeding it.

## Running it

```bash
make setup     # venv + plists + load the agents
make test      # every tests/*.py
make status    # what is loaded, what ran last, is the ledger reachable
make unload    # stop the agents
```

**Never start a second loop.** Two loops against one ledger double every post and
every dispatch. Before loading an agent, check nothing else is already running the
same script — during the extraction that means `com.jkrumm.warden-loop` in
particular.

## Python

Pinned to **3.11.15** via `python3.11` (uv-managed). The box's default `python3` is
3.14 — do not use it. `make venv` fails loudly rather than falling back, because
the extracted loop was written and proven against 3.11 and an interpreter change
is its own verifiable step.

**The loop is pure stdlib.** `requirements.txt` has no entries. Keep it that way: a dependency here is a dependency in the
thing that decides whether to touch production.

## Tests

Hand-rolled runners, **not pytest** — this venv has none. Each file collects its
own module-level `test_*` functions reflectively, calls them with no arguments,
and exits non-zero on failure. Every test function is argument-free and
fixture-free, so the files stay valid pytest input if that ever changes.

```bash
make test                                  # all suites
.venv/bin/python3 tests/test_triage.py     # one suite
```

`tests/test_triage.py` is the regression gate at **423/423**. Any other number is a
finding to report, not a count to edit. `_triage_env()` builds a throwaway DB in a
temp dir and monkeypatches the module globals and every client boundary
(`_sideclaw`, `_github`, `_argo`, the Slack posters), so nothing reaches Slack,
sideclaw, GitHub or Argo. **Do not delete, skip or weaken a test
to make it pass** — if it cannot pass without changing production behaviour, that
is the finding.

`make test` fails when it finds zero tests. A suite that silently runs nothing is
the failure mode this whole project exists to remove.

## The ledger

`~/.warden/warden.db` — SQLite, WAL, **not in git**, live. It is the source of
truth; Argo pages are projections, and Slack hears one line when an item is `fixed` or
`needs_decision` (plus a daily `failed` count).

- **One migrator.** `scripts/ledger.py` owns the schema and `schema_version`;
  only the loop migrates, at boot. Everything else asserts the version and
  refuses on mismatch. Four processes racing unversioned `CREATE TABLE` /
  `ALTER TABLE` is what this replaced.
- **Read-only means read-only**: `sqlite3.connect(f"file:{p}?mode=ro", uri=True)`.
- Backup is `VACUUM INTO` (never a bare `cp` or `rsync` of an open database —
  that captures the main file and its `-wal` at different instants and restores
  as either stale or corrupt with nothing saying which), shipped to
  `homelab:/mnt/hdd/backups/warden/`, which the existing restic container already
  walks on its way to B2.

## Talking to sideclaw

Warden depends on exactly two things: `submit(tier, repo, brief, model) -> jobId`
and `get(jobId) -> {status, result}`. Everything else about sideclaw is its own.

- **The verdict schema is published by sideclaw**, not copied here. Copying it
  guarantees drift, and drift presents as "verdict silently ignored" — the exact
  failure warden exists to fix. A version mismatch is a loud refusal, never a
  best-effort parse.
- **sideclaw is the only boundary.** Warden carries no repo allowlist, tier
  ceiling or model choice: a dispatch names a repo (`lifecycle.policy.repo_cwd()`
  composes the one `cwd` the wire protocol still needs) and sends no `model` key —
  sideclaw enforces its own allowlist (`GET /api/dispatch-policy`) and routes each
  tier (`GET /api/routing`). A sideclaw **4xx on submit is a refusal**
  (`clients.errors.SubmitRefused`): the item ends `failed` carrying sideclaw's
  message (`triage._end_on_refusal()`) and is never retried — except a refused
  escalation `model` (attempt 3+), resubmitted once without it, and a refused
  **triage** submit, which strikes like any other failure (a triage refusal is never
  the item's fault); 5xx and connection
  errors strike (`triage._strike()`: 10/30 min backoff, third strike → `failed`). `warden dispatch --model` stays — it is the
  owner's explicit choice, not policy.
- **The merge gate is four facts**: PR open, checks green (or none exist), step-7
  review `confirmed`, and GitHub's own rules allowing the merge call
  (`lifecycle.merge.merge_gate_check()` / `plan_or_land()`). No path scope, size
  ceiling, per-repo carve-out or executor-repo exception; `warden merge <job>
  --confirm` goes through the same gate.
- **An episode is not contained.** `readOnly` is three tool names on a CLI flag
  under `--dangerously-skip-permissions`; `Bash` is unrestricted and the brief is
  attacker-influenceable (public issues, alert text, log lines all reach it). A
  bearer token on this host is not an authorization boundary against an episode.

## Things that are load-bearing and look like they are not

- **A signal going quiet may cancel the need to *start* work. It may never
  discharge a verdict or an in-flight operation.**
  Silence-resolve applies to `new` and to nothing else.
- **Overflow waits, never drops.** Triage submissions past the per-run cap stay `new`;
  cluster members past the cap stay `triaged`.
- **The triage submit and fold are compare-and-set** on `triage_job` (a
  `claiming:<time>` sentinel before the call, the job id after, a stale claim
  released after 5 minutes). Two passes finding the same finished job fold it once.
  Entering `new` clears `triage_job`, so a recurrence is triaged afresh; the fold clears it
  on every outcome except `ignore`, so a row that still carries one is a *model's* ignore
  (revisited after `cooldownHours`) and an owner's dismiss never reopens. Every triage
  submit failure, a sideclaw 4xx included, strikes under a CAS on the claim (`failed` on
  the third); a job still not terminal 30 minutes after submit (`triage_job_at`) is
  cancelled best-effort and struck.
- **Triage never leaks a private repo.** A `-private` candidate is a name only (no item
  lines), and an item in one sends the model `(private repo — content withheld)` instead
  of its title/payload; everything else the event says is fenced as untrusted data.
  Grouped-source policy patterns (`slack_alert`, `slack_update`, `hermes_log`) match
  `fingerprint(title)`, which has no digits — a pattern with one is dead.
- **The dry-run contract**: never touches Slack, never shells out, never submits a
  triage job (it prints what it would), everything else real. With no staging environment it is the only pre-production surface there is.
- **A policy file may name and parameterise, never express.** Config carries
  validated values; code owns the argv array. The three closed allowlists
  (liveness, deploy, `HOST_VERB_ALLOWLIST`) are one principle in three
  instances (the host-verb one added 2026-09-11 for the owner's host-restart
  decision — DESIGN.md § The host-verb carve-out).
- **Deferral must be visible.** A budget hit that only reaches a `.err` file is
  indistinguishable from a broken loop.
- **A dispatch that ends terminal with no verdict is not a verdict.** It is an
  infrastructure failure: it strikes and retries, and the third strike is
  `failed` carrying `dispatches.error` — never a verdict-less `working` row (§64).
- **`needs_decision` is the only human exit**, and only a verdict with
  `nextAction=human` reaches it. `needs_decision` and `failed` never expire.
- **Two crons drive one ledger** (loop and sweep). Every act on a row they can
  both reach — a submit, a handoff, a Slack post — is a compare-and-set claim
  first (§117).
- **An action pulled from Argo's queue is the owner, full stop.**
  `apply_argo_actions()` records `authorized_by="owner:argo"` and acts — Argo
  is reachable only over his own tailnet, which is the trust boundary.

## Cross-repo facts

`hermes-ops.sh`, `agents-overview.py` and a
six-line dispatch-shim script (which `exec`s into this repo's `scripts/warden`)
live in `hermes-agent` and are reached, not vendored.
`~/.hermes/{scripts,config}` are **whole-directory symlinks** into that repo, which
is why the extraction was never a `git mv`.

## Git

Direct-to-master. `/commit` per logical concern, no attribution footers.
Append a § to `docs/history/state-log.md` and rewrite `STATE.md` in the same
commit as the work it describes — it is the memory, and the next session is a
stranger.
