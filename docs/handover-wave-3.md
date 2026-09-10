# Wave 3 — handover prompt

Written for a stranger. Nothing below assumes the Wave 2 conversation.

---

You are the long-lived orchestrator for warden's **Wave 3**. Multi-day,
multi-session. Read this whole file before acting.

## Authority

Read in this order, before touching anything:

- **`STATE.md`** — where the implementation actually is. **§§37-43 are Wave 2**
  and are mandatory. §41 is the stop-condition roll-up, §42 is the boundary
  reviewer's seven findings *including three false claims the orchestrator made
  about its own work*, and **§43 is the first night in production and the defect
  it caught**. §§26-36 are Wave 1, §§24-25 Wave 0. **Never rewrite its history** —
  the corrections are the most useful thing in the file.
- **`DESIGN.md`** — authoritative for what warden is. § *What must not be lost* is
  nine details that read like accidents and are not. Check against it before every
  merge.
- **`FLOWS.md`** — six end-to-end flows, and where a human is genuinely needed.
- **`REVIEW.md`** — four reviews and their dispositions. Settled arguments stay
  settled. If you are about to propose something it rejected, don't — unless you
  have new evidence, in which case say so and cite it.
- `docs/triage.md` (the loop, in detail), `docs/api.md` (the six metrics and their
  honesty rules).

## Wave 2 is DONE and LIVE. Do not rebuild it.

Three slices shipped, plus a boundary review and its remediation:

1. **The reopen churn is fixed.** `reopen_if_needed()` compares an occurrence
   fingerprint (`triage_items.occurrence_mark`, schema 3) instead of asking
   `events.resolved_at IS NULL`. Before: 23 of 30 `resolved` rows reopened and
   re-closed **every ten minutes**, silently, overwriting their own notes.
   Measured after: 67 live loop passes produced **18** transitions, all real.
   The mark is five `|`-separated slots compared with `!=`, **never** `>` —
   `ts_last` is a Slack float-string and the rest are ISO-8601. Do not
   "simplify" that to a `MAX()`.
2. **`resolved` split into `fixed` / `quiet` / `closed`** (schema 4), all 30 live
   rows migrated to `quiet`, plus an append-only `item_transitions` table written
   only by `_set_state()`. `resolve_recovery_paired()` produces **`quiet`, not
   `fixed`** — DESIGN.md § What must not be lost item 4, *"neither ever claims a
   fix"*. A test exists solely to fail if you change that.
3. **`GET /metrics` and `GET /health`** on `127.0.0.1:7734`, a fifth LaunchAgent,
   read-only, one fresh connection per request with its own schema assertion.
   Two of the six numbers serve **`null` with a reason** rather than a fabricated
   `0`; both reasons are Wave 3 work (below).

`DESIGN.md` was amended once, by the operator's decision: § *What "done" means*
row 2 is now **`0 / 28`**, because the old `2 / 28` counted two
recovery-message closes that item 4 forbids from claiming a fix.

### Current numbers — any other number is a finding, not a count to edit

```
warden        test_triage.py 108/108 · test_api.py 23/23 · test_ledger.py 16/16
              test_intents.py 19/19 · test_watchdog_locking.py 3/3
              three "all cases as expected" suites
hermes-agent  test_dispatch_approval.py 83 checks · test_hermes_cc.py 165 cases
ledger        schema_version 4, WAL, live at ~/.warden/warden.db
agents        FIVE LaunchAgents: loop, poll, sweep, backup, api
```

## First: reconnaissance against the RUNNING system, not the docs. No edits.

```bash
cd ~/SourceRoot/warden
make status        # five agents ✓ AND last exit 0 — the second column is the exit status
make test          # every suite
make check-policy  # the two copies of the dispatch policy must agree
curl -s 127.0.0.1:7734/health | python3 -m json.tool
curl -s 127.0.0.1:7734/metrics | python3 -m json.tool
```

Then confirm for yourself, don't take `STATE.md`'s word:

- `schema_version` is **4**, no non-terminal row has a NULL `state_deadline`, and
  no row is still in `resolved`.
- **Count `item_transitions` against loop passes.** Transitions should be a
  handful per day, not per pass. If they scale with passes, the churn is back and
  that is your first finding.
- `~/Library/Logs/warden-*.err` — read them. Check **mtime and content**: a stale
  traceback is not a live one. `warden-loop.err` legitimately contains old
  material from Waves 0-1.
- `grep -c 'UPDATE triage_items SET state=' scripts/triage.py` → **1**.
- The `needs_human` queue and its deadlines. Those items are what this whole
  project exists to protect. Watch what your changes do to them.

**Append your findings to `STATE.md` as a new section.**

## Then: Wave 3 only. Do not start Wave 4.

`DESIGN.md` § Migration: *"Abort, revert, per-repo lock, crash reconciliation with
`unknown`. Prove one complete path survives a kill at every boundary."*

### 0. FIRST — the dissolve edge launders a verdict into a discardable state

`STATE.md` §43 has the full measurement. It is not in DESIGN.md's Wave 3 list and
it goes first anyway, because it is **losing correct verdicts in production right
now** and it is small next to crash reconciliation.

Observed live, 2026-09-09 21:09→21:39Z:

```
quiet -> new -> investigating -> verdict -> new -> quiet
```

`_dissolve_cluster()` sends a split cluster's members back to **`new`** (DESIGN's
own lifecycle edge, and it deliberately keeps `dispatch_job` as a cooldown anchor
— must-not-be-lost item 2). `escalate()` then declines inside that cooldown. The
signal recovered inside the window, and `apply_resolutions()` closed both as
`quiet` — correctly, because **`new` is the one state silence may discharge**.

Net effect: a substantive verdict identifying a real, active `slack_sdk`
`SocketModeClient.connect()` race in `hermes-agent` reached nobody. Both items are
`quiet` with `note` NULL and `card_ts` NULL. The verdict survives only in
`dispatches.verdict_json`, which nothing reads.

`_SILENCE_RESOLVE_ELIGIBLE_STATES = (STATE_NEW,)` is **not wrong** and must not be
widened. The defect is that the dissolve edge launders an item carrying an
obligation into the one state that carries none — Wave 1 closed the front door
and this is the same failure through the side door.

**Do not patch it by stuffing the verdict into `note`.** `apply_resolutions()`
clears `note` precisely because a `new` row is supposed to carry no obligation.
The honest fix is a distinct state: a dissolved member *has been evaluated*,
carries a verdict, and is not silence-eligible until it has been re-evaluated
individually. That is a state-machine change, it needs a deadline and a named
poller like every other non-terminal state (`STATE_DEADLINES` is one closed
table), and it is schema 5.

### 1. The operation id, and crash reconciliation

`DESIGN.md` § *Crash recovery — the ledger cannot be atomic with the world*.
SQLite can atomically change a row; it cannot atomically change a row **and**
merge a PR, start a job, or deploy a service.

- An **operation id** recorded before dispatch; the approval binds to it.
- **Remote receipts** where available (PR merge sha, deploy run id).
- **`unknown` as an explicit outcome**, reconciled before any retry — never
  silently read as failure.

This is also what makes `/metrics`' *"verified unattended fixes per week"* stop
returning `null`: today `dispatch_approvals` has no `event_id` and **0 of 5 rows
carry `--auto-from-item`**, so an approval cannot be attributed to the item it
fixed. See `docs/api.md`.

### 2. Abort and revert, and the per-repo in-flight lock

- `cancel` does not cancel: sideclaw exposes submit/list/get only, so warden marks
  its own row `abandoned` while the episode keeps running. Needs
  `POST /api/jobs/:id/cancel` **in sideclaw**, and `warden abort <item>` as a
  lifecycle transition.
- `warden revert <item>` as a first-class transition recording the revert PR on
  the item. This is what makes `/metrics`' `reverts` stop returning `null`, and it
  is half of DESIGN's automatic-demotion rule.
- **A per-repo in-flight lock.** Two implement episodes on one repo cut two
  branches from the same base and open two unaware draft PRs.
- An item whose episode was interrupted mid-push needs a ledger field for *"there
  is an orphan branch or PR from this item"*.

### 3. The stop condition's real test

**Prove one complete path — actionable verdict → recorded disposition →
authorized operation → reconciled deployment → positive verification, with an
explicit failure outcome — and kill the process at every boundary to show it
neither drops the obligation nor repeats an unsafe action.**

That is DESIGN's own words and it is the reason Wave 3 exists before Argo.

Note a hard dependency you will hit: **metric 2 (`fixed` vs `quiet`) is
structurally pinned at 0** until at least one repo has a deploy target and a
liveness probe, because `maybe_check_liveness()`'s positive branch is the only
producer of `fixed`. `DESIGN.md` says so now. If proving the complete path needs a
deploy target, that is a **case 1 human decision** (FLOWS.md flow 2: the first
deploy into an environment is the unscoped one) — ask, do not seed one yourself.

## Traps that will cost you a day each

- **THERE IS NO STAGING WINDOW IN THIS REPO.** The LaunchAgents execute
  `scripts/*.py` **from the working tree**, and `~/.hermes/{scripts,config}` are
  whole-directory symlinks into `hermes-agent` — so an uncommitted edit is in
  production the moment it is saved. Wave 2 learned this by having the live ledger
  migrate itself three minutes after an edit landed. **Run `make unload` before
  any non-additive change**, and `make agents` after.
- **TWO LOOPS AGAINST ONE LEDGER** double every card and every dispatch. Before
  `launchctl kickstart`, check `pgrep -f triage.py`.
- **The ledger is LIVE.** Read it with
  `sqlite3.connect("file:...?mode=ro", uri=True)`. Never open it writable to "just
  check something". Mode 600; keep it that way.
- **ONE MIGRATOR.** Only the loop migrates, at boot; everything else asserts and
  refuses. `scripts/ledger.py` exposes `--migrate/--check/--version`. To migrate
  the live ledger: **snapshot first** (`VACUUM INTO` from a read-only handle),
  then let the loop do it. Bumping `SCHEMA_VERSION` means bumping
  `WARDEN_SCHEMA_VERSION` in `hermes-agent/scripts/hermes-cc.sh` **in the same
  breath** — a mismatch is a loud exit 2.
- **`ledger.connect(path, readonly=True)` does NOT assert the schema version.**
  Its readonly branch opens `mode=ro` and returns. Any read-only consumer must
  call `assert_schema_version()` itself, as `api.py` does per request.
- **`_run_migrations()`'s adoption branch** stamps version 1 then falls through
  the loop. That fall-through is what makes the rollback restorable. Do not
  "simplify" it into a `return`.
- **`git commit -am` will sweep a running subagent's half-finished work into your
  commit.** Stage explicit paths.
- **A subagent's report is a claim; the diff is the proof.** Read every line it
  says it changed and re-run its mutation checks yourself.
- **The dry-run contract**: never touches Slack, never shells out, everything else
  real — with two documented carve-outs, `drain_intents()` and
  `sweep_deadlines()`. If you add a step that writes, decide which side it is on
  and say why.
- **Python's `os.path.realpath` does NOT correct case; Bun's `realpathSync` does.**
  A path allowlist reimplemented in Python must compare case-insensitively.
- The rollback is `~/.warden-cutover-backup/` plus `~/.warden/backups/`.

## How to work

- **Delegate multi-file edits to `@implementer`** with a self-contained brief:
  exact paths, the change, acceptance criteria, scope limits, what NOT to touch,
  and **the reasoning** — it reads a brief literally and cannot tell a wrong
  instruction from a right one without the why. Wave 2's briefs contained two
  factual errors; the worker caught both **because they were reasoned, not
  ordered**.
- **Delegate search to `Explore`.** Never read ten files in the orchestrator.
- Run independent subagents in parallel in one message.
- **`mcp__sideclaw__check` / `review`** — async: submit → `job_wait({jobId})` →
  loop while `stillRunning: true`. The submit call is not the answer.
- **`/research`** for any library/API/version fact. Never from memory.
- Long shell work runs in the background; do not poll with `sleep`.
- Keep the orchestrator holding the plan and the verdicts, not the raw material.

### Evidence, not claims

Never write "done", "working" or "passing" without the command output that proves
it. If a check fails, paste it verbatim.

**Run the thing.** Every wave's worst defects were invisible to reading: a
heartbeat that skipped the normal case, a backup that reported success while
taking no snapshot, a commit placed where two `continue`s jumped past it, a
metrics endpoint that divided dispatch counts by dispatch counts while breaking
them down by item counts — and the orchestrator then misread its own instrument in
`STATE.md`.

**Mutation-test anything load-bearing.** Break it, confirm a test fails *by name*,
restore. A test that does not fail when you break the thing is not a test. Wave 2
shipped a read-only guarantee whose test could not observe the call site it
claimed to guard; only a mutation found it.

### At the wave boundary

A **fresh reviewer** — a new subagent, no context from you — checks the diff and
the running system against `DESIGN.md` and `FLOWS.md`. Your own read of your own
work does not count. Give it explicit hard rules: no edits, no commits, read the
ledger read-only, restart nothing. It has caught a real defect at all three
boundaries so far, including three false claims in `STATE.md` itself.

## State, because this outlives your context

After every slice, update `STATE.md`: done / in-progress / blocked, files changed,
commands run **and their output**, decisions made, approaches that failed and why,
and the exact next action. Update it in the same commit as the work it describes.
**End the wave with a roll-up against the stop condition, item by item.**
Compaction is not memory — the file is.

## Escalate, don't guess

Stop and ask only for: a conflict between `DESIGN.md` and the code that changes
the design; a destructive or irreversible operation; anything needing a credential
or a present human; the same verification failing three times. Everything else you
decide and record. Note that "needs a human" is often false — Wave 0 parked a
monitor as human-essential that was fully automatable, and Wave 1's one genuine
escalation resolved by *deleting a requirement* rather than building it.

## Known-open, inherited — decide these deliberately

1. **The dissolve edge** (§43) — item 0 above.
2. A forged spool file can permanently block a real approval click
   (`AND decision IS NULL`), and Slack renders **"already decided"** — a denial,
   correctly, but with a misleading label. Say "superseded" when the signature
   does not verify. Lives in `hermes-agent/plugins/dispatch-approval`.
3. `needs_human`'s *"reminder at 1d"* is deliberately unbuilt — a reminder is a
   notification, not a deadline.
4. **`hermes_log` has no `ts_last`**, and a state source with a continuously-open
   event moves no mark slot except `reconcile()`'s reminder bump — so reopen
   latency is bounded by `REM_HOURS`: 6h for `uk`, **168h** for `github_issue` and
   `stray_skill`. Documented in `docs/triage.md`, not fixed. It belongs with the
   **self-tuning quiet window**, which Wave 2 unblocked and which is still open.
5. `ledger._verify_columns()` still checks only the original five tables, so
   `item_transitions`' shape is created but not asserted by the adoption path.
6. `/metrics` metric 3 loads all of `item_transitions` into memory to pair entries
   with exits. Fine now; wants a windowed query long before it is a problem.
7. `DESIGN.md` § 365's justification for Argo's Postgres read-cache does not
   survive its own objection (see `STATE.md` §29). That is **Wave 4**, not now.

## Stop condition

Wave 3 is done when: a dissolved cluster member can no longer be discharged by
silence, proven on a live-shaped copy; an operation id links an approval to an
item and `/metrics`' *unattended* number stops returning `null`; `warden abort`
and `warden revert` exist as lifecycle transitions and `reverts` stops returning
`null`; a per-repo in-flight lock prevents two implement episodes on one repo; and
**one complete path has been killed at every boundary and shown to neither drop
the obligation nor repeat an unsafe action.**

Report against that list item by item, with evidence. Then stop and hand back —
do not roll into Wave 4.
