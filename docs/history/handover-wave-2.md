# Wave 2 — handover prompt

Paste this whole file as the opening prompt of a fresh session in
`~/SourceRoot/warden`. It is written for a stranger; nothing below assumes the
Wave 1 conversation.

---

You are the long-lived orchestrator for warden's **Wave 2**. Multi-day,
multi-session. Read this whole prompt before acting.

## Authority

Read in this order, before touching anything:

- **`STATE.md`** — where the implementation actually is. **§§26-36 are Wave 1**
  and are mandatory. §35 is the stop-condition roll-up and the fresh reviewer's
  findings; §36 is why the TTY decide path was *withdrawn* rather than built.
  §§24-25 are Wave 0. Never rewrite its history — the corrections are the most
  useful thing in the file.
- **`DESIGN.md`** — authoritative for what warden is. § *What must not be lost*
  is nine details that read like accidents and are not; all nine survived Waves
  0 and 1. Check against it before every merge.
- **`FLOWS.md`** — six end-to-end flows, and where a human is genuinely needed.
- **`REVIEW.md`** — findings from four reviews and their dispositions. It exists
  so settled arguments stay settled. If you are about to propose something it
  rejected, don't — unless you have new evidence, in which case say so and cite
  it.

Do not silently narrow scope. If a slice turns out bigger than stated, say so
and continue.

## Wave 1 is DONE and LIVE. Do not rebuild it.

Closed at **four built, one withdrawn**:

1. **Silence-resolve applies to `new` and nothing else.** All three silence paths
   (`apply_resolutions`, `resolve_quiet_grouped`, `resolve_recovery_paired`) share
   one inclusion allowlist, `_SILENCE_RESOLVE_ELIGIBLE_STATES = (STATE_NEW,)`. It
   is an inclusion list of one *on purpose* — an exclusion list silently admits
   every state added after it was written, which is how `needs_human` became
   discardable. **Wave 2 adds states. Do not widen that tuple.**
2. **The intent queue.** `scripts/intents.py` — a spool at `~/.warden/intents/`
   plus a single drain. `record()` opens no database at all; `drain(conn)` takes a
   caller-owned connection. The Slack approval plugin signs, spools, and drains
   synchronously; the loop drains as a backstop (step 0 of `run()`).
3. **The approval plugin no longer writes the ledger** — both its handles are
   `mode=ro`. Six writers are now two: warden, and `hermes-cc.sh` (a documented,
   deliberate exception).
4. **`state_deadline` + a named poller on every non-terminal state.**
   `STATE_DEADLINES` is one closed table; all 26 transitions route through
   `_set_state()`; `sweep_deadlines()` acts on expiry. `schema_version` is **2**.
5. **The CLI decide path was WITHDRAWN**, not built. Read §36 before you even
   think about a second signer — the blocker is estate-level, not warden's.

### Current numbers — any other number is a finding, not a count to edit

```
warden        test_triage.py 96/96 · test_ledger.py 12/12 · test_intents.py 19/19
              test_watchdog_locking.py 3/3 · three "all cases as expected" suites
hermes-agent  test_dispatch_approval.py 83 checks · test_hermes_cc.py 165 cases
ledger        schema_version 2, WAL, live at ~/.warden/warden.db
```

## First: reconnaissance against the RUNNING system, not the docs. No edits.

```bash
cd ~/SourceRoot/warden
make status        # all four agents ✓ AND last exit 0 — the second column is the exit status
make test          # every suite
make check-policy  # the two copies of the dispatch policy must agree
```

Then confirm for yourself, don't take `STATE.md`'s word:

- `schema_version` is 2 and **no non-terminal row has a NULL `state_deadline`**:
  ```sql
  SELECT COUNT(*) FROM triage_items WHERE state_deadline IS NULL
   AND state IN ('investigating','verdict','implementing','validating',
                 'merge_blocked','merged','needs_human','pr_open');
  ```
  It should be 0. If it is not, a transition site is failing to write one and that
  is your first finding.
- `~/Library/Logs/warden-*.err` — read them. A stale traceback is not a live one;
  check the mtime and the content before concluding anything.
- `grep -c 'UPDATE triage_items SET state=' scripts/triage.py` → **1**. More than
  one means someone added a raw transition that writes no deadline.
- The four `uk:*` rows in `needs_human` carry deadlines around **2026-09-16**.
  They are the four items this whole project exists to protect. Watch what your
  changes do to them.

**Append your findings to `STATE.md` as a new section.**

## Then: Wave 2 only. Do not start Wave 3.

`DESIGN.md` § Migration: *"Honest states + `dismissed` (cheap — the enum value
already exists unconsumed). `/metrics`. Fix the reopen condition before any
re-render work."*

In this order — and the ordering is load-bearing, it is stated in DESIGN.md
itself:

### 1. FIX THE REOPEN CONDITION FIRST

`docs/triage.md` § *"Known: grouped reopen churn"* has the measurement.
`reopen_if_needed()` reopens on `events.resolved_at IS NULL`, which for a grouped
source stays NULL for months — so a quiet-resolved item is reopened on the very
next run, quiet-resolves again, and repeats every ten minutes forever. It is
currently invisible **only by luck**: the re-rendered card is byte-identical, so
`card_hash` short-circuits the Slack call. Any change that varies the resolve note
by one character turns that silent churn into a `chat.update` every ten minutes.

The fix is to reopen on a **new occurrence** (the grouped payload's `ts_last`
moving), not on `resolved_at IS NULL`. DESIGN.md says explicitly: **before any
re-render work.** Doing the state split first would make the churn visible and
noisy.

Note Wave 1 extended this function: `dismissed` reopens too, and deliberately —
it means *nobody answered*, unlike `ignored`/`note` where a human said benign.
Preserve that distinction.

### 2. HONEST STATES — split `resolved`

`DESIGN.md` § the state machine: `fixed` = a change landed and a positive signal
confirmed it. `quiet` = the signal stopped and nothing shipped. `closed` = a human
said done. Today all three are one `resolved`, which is why *"closes that are
verified fixes vs. silence"* reads 2/28.

`dismissed` already exists (Wave 1 built it as the deadline table's expiry
target, with a required reason). Two lines in `STATE_DEADLINES` are marked as
changing here — `merged` currently expires to `resolved` where DESIGN.md says
`closed`. Grep for the comments; they name themselves.

**This is a data migration on a live ledger with 30 `resolved` rows.** It is
`schema_version` 3. `scripts/ledger.py` owns it; bumping `SCHEMA_VERSION` means
bumping `WARDEN_SCHEMA_VERSION` in `hermes-agent/scripts/hermes-cc.sh` in the
same breath — they are pinned to each other and a mismatch is a loud exit 2.

### 3. `/metrics`

The six funnel numbers from DESIGN.md § Observability, read-only, off the ledger.
`DESIGN.md` § HTTP API is explicit: **the API opens the database read-only**,
`file:…?mode=ro`. It records intents through `intents.py`; it never writes.

## Traps that will cost you a day each

- **TWO LOOPS AGAINST ONE LEDGER** double every card and every dispatch. Before
  `launchctl kickstart` on `com.jkrumm.warden-loop`, check `pgrep -f triage.py`.
- **The ledger is LIVE.** Read it with
  `sqlite3.connect("file:...?mode=ro", uri=True)`. Never open it writable to
  "just check something". It is mode 600 now; keep it that way.
- **ONE MIGRATOR.** Only the loop migrates, at boot. Everything else asserts and
  refuses. `scripts/ledger.py` exposes `--migrate/--check/--version` so even a
  shell script has no excuse. To migrate the live ledger: **snapshot first**
  (`VACUUM INTO` from a read-only handle), then `launchctl kickstart` the loop and
  let *it* migrate. Do not run `--migrate` against the live file yourself.
- **`_run_migrations()`'s adoption branch** stamps version 1 then falls through
  the loop. That fall-through was a bug fixed in Wave 1 (§33) and it is what makes
  the rollback restorable. Do not "simplify" it back into a `return`.
- **`git commit -am` will sweep a running subagent's half-finished work into your
  commit.** This happened in Wave 1. Stage explicit paths.
- **A subagent's report is a claim; the diff is the proof.** Read every line it
  says it changed, and re-run its mutation checks yourself. In Wave 1 a worker
  reported a clean comparison that had silently never executed one side.
- **The dry-run contract**: never touches Slack, never shells out, everything else
  real — with two deliberate carve-outs, both documented in code:
  `drain_intents()` (the spool is shared with the live loop) and
  `sweep_deadlines()` (it can end an item terminally, and `--dry-run` defaults to
  the live ledger). If you add a step that writes, decide which side it is on and
  say why.
- **Python's `os.path.realpath` does NOT correct case; Bun's `realpathSync` does.**
  If you reimplement a path allowlist in Python, compare case-insensitively or you
  reintroduce a fail-open the agent-gateway side already closed.
- The rollback is `~/.warden-cutover-backup/`. `cron/jobs.json` is **not in git**.

## How to work — and this session can run long

You are on Claude Code with a large context and durable tooling. Use it:

- **Delegate multi-file edits to `@implementer`** with a self-contained brief:
  exact paths, the change, acceptance criteria, scope limits, and what NOT to
  touch. It does not see your conversation and reads a brief literally — an
  ambiguous sentence becomes an assertion encoding the wrong behaviour. That
  happened in Wave 0 and again in Wave 1. Give it the *reasoning*, not just the
  instruction, so it can tell when the instruction is wrong.
- **Delegate search to `Explore`.** Never read ten files in the orchestrator to
  find one thing.
- **Run independent subagents in parallel in one message.** They hold their own
  context and their own prompt cache; that is the real argument for delegating.
- **`mcp__agent_gateway__check` / `review`** for validation and multi-angle review —
  async, submit → `job_wait({jobId})` → read `result`. agent-gateway's wait returns
  after ~50s regardless, so **loop while `stillRunning: true`**. The submit call is
  not the answer.
- **`/research`** for any library/API/version fact. Never from memory.
- **Long shell work runs in the background** (`run_in_background: true`) and
  re-invokes you when it exits. Do not poll with `sleep`.
- **Worktree isolation is opt-in, up front** — never mid-flow, or you split work
  across trees.
- Keep the orchestrator holding the plan and the verdicts, not the raw material.

### Evidence, not claims

Never write "done", "working", or "passing" without the command output that
proves it. If a check fails, paste it verbatim.

**Run the thing.** Wave 0's three worst defects and two of Wave 1's were invisible
to reading and only appeared when something was executed: a heartbeat that skipped
the normal case, a backup that took no snapshot while reporting success, a glob
that errored on every first run, a commit placed where two `continue`s jumped past
it, and a FINDING that reported four rows every ten minutes while bounding none of
them.

**Mutation-test anything load-bearing.** Break it deliberately, confirm a test
fails *by name*, restore. A test that does not fail when you break the thing is
not a test.

### At the wave boundary

A **fresh reviewer** — a new subagent, no context from you — checks the diff and
the running system against `DESIGN.md` and `FLOWS.md`. Your own read of your own
work does not count. At the Wave 0 boundary it caught the orchestrator asserting
something false; at the Wave 1 boundary it caught `STATE.md` missing an entire
stop-condition item. Give it explicit hard rules: no edits, no commits, read the
ledger read-only, do not restart anything.

## State, because this outlives your context

After every slice, update `STATE.md`: done / in-progress / blocked, files changed,
commands run **and their output**, decisions made, approaches that failed and why,
and the exact next action. Update it in the same commit as the work it describes.
Compaction is not memory — the file is.

**End the wave with a roll-up section against the stop condition, item by item.**
Wave 1 nearly shipped without one, and the reviewer correctly called that its most
consequential defect: the blocking item existed only in a chat log.

## Escalate, don't guess

Stop and ask only for: a conflict between `DESIGN.md` and the code that changes
the design; a destructive or irreversible operation; anything needing a credential
or a present human; the same verification failing three times. Everything else you
decide and record. Note that "needs a human" is often false — Wave 0 parked the
backup monitor as human-essential and it was fully automatable, and Wave 1's one
genuine escalation resolved by *deleting a requirement* rather than building it.

## Known-open, inherited — decide these deliberately

1. `sync_card()`'s never-carded guard covers only `STATE_RESOLVED`. A `dismissed`
   row that somehow lacked `card_ts` would post a first card. The state split
   touches exactly this code.
2. A forged spool file can permanently block a real approval click
   (`AND decision IS NULL`), and Slack then renders **"already decided"** — a
   denial, correctly, but with a misleading label. Say "superseded" when the
   signature does not verify.
3. `needs_human`'s *"reminder at 1d"* from DESIGN.md's deadline table is
   deliberately unbuilt — a reminder is a notification, not a deadline.
4. `DESIGN.md` § 365's justification for Argo's Postgres read-cache does not
   survive its own objection: if the mini is unreachable, warden is not running,
   so the cached board shows frozen work. See `STATE.md` §29. Drop the cache or
   re-justify it — do not let it become a mirror. That is Wave 4, not now.

## Stop condition

Wave 2 is done when: a grouped item no longer reopens every pass, and the fix is
proven by running the loop twice and observing no churn; `resolved` has split into
`fixed` / `quiet` / `closed` with the live ledger's 30 rows migrated and each one
landing in a defensible bucket; `/metrics` serves the six funnel numbers from a
read-only handle; and `make test` is green with a count you can account for.

Report against that list item by item, with evidence. Then stop and hand back — do
not roll into Wave 3.
