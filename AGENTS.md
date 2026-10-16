# warden — Developer Notes

warden is the autonomous loop of the agent platform: it owns one SQLite ledger and
drives every unattended item from a signal to `fixed`, through agent-gateway jobs.
**Read `STATE.md` first** (where the implementation is, what is next), then
`DESIGN.md` (what exists, and § *What must not be lost* — check it before every
merge). The spec is `~/SourceRoot/dotfiles/docs/agent-platform.md`; it wins over
anything here. `docs/history/` is the archive: the append-only build log
(`state-log.md`, §1 onward), old flows, design reviews.

**Code owns every state transition.** The only LLM inputs are agent-gateway's episode
verdicts and triage answers, and each is validated against the ledger before an
item moves.

## Layout

| Path | Is |
|-|-|
| `scripts/triage.py` | the loop's entry point: `run()` (one pass; reads the kill-switch first), CLI flags, heartbeat |
| `scripts/loop/` | the loop by stage: `core` (paths, states, `set_state`, strikes, policy), `intake` (alerts, issues, and the `job` source: `ingest_jobs`, `route_job_items`, `job_chronic_reason`), `triaging`, `work`, `train`, `verify`, `notify` |
| `scripts/lifecycle/` | pure-ish helpers the loop and CLI share: label routing, merge gate (check runs, falling back to Actions workflow runs when the token cannot read checks), rollout (`make deploy`/`verify`), dispatch, operations, `pause` (the kill-switch flag file), `ingest` (the ingest door's logic: a pushed Kuma / HyperDX alert -> one `events` row) |
| `scripts/clients/` | the only HTTP/CLI boundaries: agent-gateway, GitHub, Argo, Slack, Kuma (the loop heartbeat), secrets, `jobs_grade` (the local `~/.local/bin/jobs-grade` subprocess: timeout, failure = None, injectable) |
| `scripts/warden.py` | the `warden` CLI (`run`, `dispatch`, `status`, `list`, `merge`, `abort`, `revert`, `close`, `retry`, `reinvestigate`, `adopt`, and the kill-switch `pause [--reason]` / `resume`) |
| `scripts/watchdog-poll.py`, `dispatch-sweep.py`, `api.py` | the other LaunchAgents; `api.py` is read-only GETs plus `POST /ingest/{kuma,hyperdx}` (bearer-gated, the one writable connection; `docs/api.md` § Ingest door) |
| `scripts/improve-trigger.py` | read-only: outcomes (`failed` / `needs_decision`) since the improve loop's cursor, or `quiet` |
| `scripts/lifecycle/shadow.py`, `scripts/shadow-replay.py` | the decision-model shadow lane next to the LLM triage (`decision_shadow`, ledger v21; nothing decides from it) and its replay over labelled history; measured table + promotion rule: `docs/decision-shadow.md`; report: `python3 scripts/lifecycle/shadow.py report` |
| `scripts/ledger.py` | the one migrator; owns the schema and `schema_version` |
| `config/triage-policy.json` | debounce, cooldowns, host verbs, label `rules`, `ignore` patterns, the `job` source's chronic thresholds (`jobChronicRecurrences`, `jobChronicWindowDays`, `jobRegressionHours`, `jobStillRedHours`) |

## Validate

```bash
make check     # compileall + every tests/*.py (creates a bare stdlib .venv if missing)
make test      # the suites only
.venv/bin/python3 tests/test_triage.py     # one suite
```

Python is pinned to **3.11.15** via `python3.11` (uv-managed); the box's default
`python3` is 3.14 — do not use it. **The loop is pure stdlib**: `requirements.txt`
stays empty, because a dependency here is a dependency in the thing that decides
whether to touch production. `make check` provisions the venv it needs: it runs
`ensure-venv` first, which creates a bare `.venv` from `python3.11` when one is
missing — no pip, no network — so it passes in a fresh linked worktree where
`make venv` never ran (the repo contract's check job). `make venv` additionally
installs `requirements.txt`; `make test` on its own still needs a venv.

Tests are hand-rolled runners, **not pytest**: each file collects its own
argument-free `test_*` functions and exits non-zero on failure. `make test` fails
when it finds zero tests. `tests/test_triage.py` is the regression gate at
**672/672** — any other number is a finding to report, not a count to edit. `tests/test_ingest.py` (the ingest door) is its own suite, 37/37.
`_triage_env()` builds a throwaway DB and monkeypatches the loop modules' globals
and every client boundary (the `jobs-grade` client returns no jobs; the kill-switch flag is a file in
its temp dir), so nothing reaches Slack, agent-gateway, GitHub, Argo or the host. `make test` runs every
suite with `WARDEN_PAUSE_FILE` pointing at a path that never exists, so an owner's live `warden pause`
cannot fail one; run a suite by hand while paused and those that set no flag of their own see it.
Patch a name on the module that defines it (`loop.core.DB_PATH`, `loop.core.post_line`) —
cross-module references are attribute lookups, so that is the one place it takes
effect. **Never delete, skip or weaken a test to make it pass**; if it cannot pass
without changing production behaviour, that is the finding.

## Deploy

warden deploys itself from this checkout, which the LaunchAgents run directly.

```bash
make setup     # venv + plists + load the agents + ~/.local/bin/warden
make deploy    # compile + import smoke, kickstart warden-api, wait for /health;
               # on failure roll back to the pre-merge commit (git reset --keep),
               # never past a checkout that moved since the sync — scripts/deploy.sh;
               # deploy/verify hold ~/.warden/deploy.lock with the loop's sync
make agents    # (re)load the LaunchAgents — needed when a plist template changed
make unload    # stop the agents
```

The loop calls `make deploy` itself after merging a warden PR (from inside
`warden-loop`), so deploy never boots out a periodic agent — they read the new code
on their next tick; only the long-running `warden-api` is restarted. A changed plist
is reported, not reloaded. Schema migrations run on the loop's next boot. A repo's
`make deploy` runs under a PATH widened with the host tool dirs
(`rollout.HOST_TOOL_DIRS`), so a recipe can reach `op`, `brew` or a `~/.local/bin`
tool even though launchd hands the loop a minimal PATH.

## Verify & Monitor

- Ingest door: `POST /ingest/kuma` and `/ingest/hyperdx` on the same loopback port, `Authorization: Bearer` from `WARDEN_INGEST_TOKEN` / `op://mini/warden-ingest/token` (resolved lazily; 503 until it is seeded, 401 on a wrong one); an off-box door forwards only `/ingest/*`. Contract: `docs/api.md`.
- Health: `http://127.0.0.1:7735/health` (loopback only, no auth) — schema version
  and each poller's heartbeat age vs. 3× its interval, plus `paused` (`null`, or `{reason, since}`
  while the kill-switch is on; it never fails `ok`). `make verify` = `/health`
  answering 200 with the expected schema and a fresh loop heartbeat; the other pollers
  and agents are `make status`'s business.
- `make status` — agents, last exits, API, ledger file. `make logs` — tail of
  `~/Library/Logs/warden-{loop,poll,sweep,backup,api}.{log,err}`.
- Kuma monitors: `Warden Backup - Push` (the daily backup pings it on success) and
  `Warden Loop - Push` (the loop pings it at the end of every completed pass, from
  `clients/kuma.py`; URL in `~/.config/uptime-kuma/warden-loop-push-url`, mode 600; ~40 min of
  silence pages). Declared in `homelab/uptime-kuma/monitors.yaml`.
- **Kill-switch:** `warden pause [--reason <text>]` stops every loop-driven dispatch, merge and deploy of every origin (the
  loop, the sweep, `warden run` / `dispatch`, Argo's `implement`) while ingest, triage and polling continue;
  the owner's own explicit `warden merge` / Argo merge is not stopped (a human act);
  `warden resume` lifts it on the next pass. The state is the flag file `~/.warden/paused`
  (`$WARDEN_PAUSE_FILE` overrides; unreadable still means paused). What is held and what is not:
  DESIGN.md § The kill-switch. It shows on `/health` and the Argo snapshot, and every pass logs it.
- OTel `service.name`: none — warden does not export telemetry.
- The queue: Argo `/warden`. Slack #agents: one line per `fixed` / `needs_decision`.
- Continuous improvement: a herdr tab `improve` in this workspace runs `/loop` on `docs/improve/LOOP.md`, **outcome-triggered** (`scripts/improve-trigger.py`; a quiet wakeup writes and commits nothing); one line per acted-on iteration in `docs/improve/JOURNAL.md`. Steer it with `rd say`.

## Gotchas

- **The working tree is live.** The LaunchAgents import this checkout every tick; a
  saved half-edit (even a migration) acts on the real ledger. Do non-trivial work
  in a git worktree and fast-forward master when green. `make link`/`agents`/`setup`
  refuse from a worktree (`make venv`/`check` still work there) — they write `~/.local/bin/warden` from `$(shell pwd)`,
  which would point every shell at a checkout that is deleted with the worktree.
- **A `job` item is only as good as `jobs-grade`'s grade, and is `fixed` only by its own success.** The loop runs `~/.local/bin/jobs-grade` (a `bun`
  script) on every pass under a PATH widened with `rollout.HOST_TOOL_DIRS`; a failed, hung or malformed run
  logs `jobs-grade: …` and skips the source for that pass — nothing opens, nothing resolves. A job whose
  `--owner` is no checkout under the repos root goes to `needs_decision`, never to a triage guess. The first
  pass after a deploy dispatches an investigation for every job already in a bad grade (up to
  `MAX_OPEN_INVESTIGATIONS` per pass): `warden pause` first if that is not wanted. A job item
  implements (`max_tier=implement`) and `verifying` waits for a `last_ok` newer than the deploy (`verify._job_closure()`), so a
  merged fix that never makes the job succeed strikes back to `triaged`; the third such expiry is `failed(work)`, not a revert.
- **Never start a second loop.** Two loops against one ledger double every post and
  every dispatch. Check `make status` before loading anything.
- **The ledger** `~/.warden/warden.db` is live and not in git. Never migrate it from
  a session — copy it (`cp ~/.warden/warden.db /tmp/`) and test there. Read-only
  means `sqlite3.connect(f"file:{p}?mode=ro", uri=True)`. Backup is `VACUUM INTO`,
  never `cp`/`rsync` of the open file.
- **A busy checkout parks, it does not strike.** The loop fast-forwards only a clean
  default-branch checkout that ends at origin. A dirty, off-default or ahead-of-origin checkout,
  or a herdr agent `working` inside it (`rollout.checkout_in_use`), **parks** that repo's merge and
  deploy (`core.park`: same state, `retry_at` +25 min, note `parked: …`, no strike). A failed fetch
  still strikes. That includes this checkout: an agent working in `~/SourceRoot/warden` holds warden's
  own merges.
- **`scripts/triage.py` has no `--help`**: any flag it does not know runs a full live
  pass against the real ledger. Run the loop by hand only with `--dry-run` and `env -u CLAUDECODE …` (agent-gateway's
  recursion guard refuses dispatches from inside a Claude session).
- **Cross-repo:** `hermes-ops.sh`, `agents-overview.py` and the dispatch shim (which
  `exec`s `scripts/warden`) live in `hermes-agent`; `~/.hermes/{scripts,config}` are
  whole-directory symlinks into that repo.

## Git

Direct-to-master. `/commit` per logical concern, no attribution footers. Append a §
to `docs/history/state-log.md` and rewrite `STATE.md` in the same commit as the work
it describes — the next session is a stranger.
