# warden — Developer Notes

warden is the autonomous loop of the agent platform: it owns one SQLite ledger and
drives every unattended item from a signal to `fixed`, through sideclaw jobs.
**Read `STATE.md` first** (where the implementation is, what is next), then
`DESIGN.md` (what exists, and § *What must not be lost* — check it before every
merge). The spec is `~/SourceRoot/dotfiles/docs/agent-platform.md`; it wins over
anything here. `docs/history/` is the archive: the append-only build log
(`state-log.md`, §1 onward), old flows, design reviews.

**Code owns every state transition.** The only LLM inputs are sideclaw's episode
verdicts and triage answers, and each is validated against the ledger before an
item moves.

## Layout

| Path | Is |
|-|-|
| `scripts/triage.py` | the loop's entry point: `run()` (one pass), CLI flags, heartbeat |
| `scripts/loop/` | the loop by stage: `core` (paths, states, `set_state`, strikes, policy), `intake`, `triaging`, `work`, `train`, `verify`, `notify` |
| `scripts/lifecycle/` | pure-ish helpers the loop and CLI share: label routing, merge gate (check runs, falling back to Actions workflow runs when the token cannot read checks), rollout (`make deploy`/`verify`), dispatch, operations |
| `scripts/clients/` | the only HTTP/CLI boundaries: sideclaw, GitHub, Argo, Slack, secrets |
| `scripts/warden.py` | the `warden` CLI (`run`, `dispatch`, `status`, `list`, `merge`, `abort`, `revert`, `close`, `retry`, `reinvestigate`) |
| `scripts/watchdog-poll.py`, `dispatch-sweep.py`, `api.py` | the other LaunchAgents |
| `scripts/ledger.py` | the one migrator; owns the schema and `schema_version` |
| `config/triage-policy.json` | debounce, cooldowns, host verbs, label `rules`, `ignore` patterns |

## Validate

```bash
make check     # compileall + every tests/*.py
make test      # the suites only
.venv/bin/python3 tests/test_triage.py     # one suite
```

Python is pinned to **3.11.15** via `python3.11` (uv-managed); the box's default
`python3` is 3.14 — do not use it. **The loop is pure stdlib**: `requirements.txt`
stays empty, because a dependency here is a dependency in the thing that decides
whether to touch production.

Tests are hand-rolled runners, **not pytest**: each file collects its own
argument-free `test_*` functions and exits non-zero on failure. `make test` fails
when it finds zero tests. `tests/test_triage.py` is the regression gate at
**506/506** — any other number is a finding to report, not a count to edit.
`_triage_env()` builds a throwaway DB and monkeypatches the loop modules' globals
and every client boundary, so nothing reaches Slack, sideclaw, GitHub or Argo.
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
is reported, not reloaded. Schema migrations run on the loop's next boot.

## Verify & Monitor

- Health: `http://127.0.0.1:7735/health` (loopback only, no auth) — schema version
  and each poller's heartbeat age vs. 3× its interval. `make verify` = `/health`
  answering 200 with the expected schema and a fresh loop heartbeat; the other pollers
  and agents are `make status`'s business.
- `make status` — agents, last exits, API, ledger file. `make logs` — tail of
  `~/Library/Logs/warden-{loop,poll,sweep,backup,api}.{log,err}`.
- Kuma monitor: `warden-backup` push (the daily backup pings it on success); the
  loop itself has no Kuma monitor — its staleness shows on `/health`.
- OTel `service.name`: none — warden does not export telemetry.
- The queue: Argo `/warden`. Slack #agents: one line per `fixed` / `needs_decision`.
- Continuous improvement: a herdr tab `improve` in this workspace runs `/loop` on `docs/improve/LOOP.md`; one line per iteration in `docs/improve/JOURNAL.md`. Steer it with `rd say`.

## Gotchas

- **The working tree is live.** The LaunchAgents import this checkout every tick; a
  saved half-edit (even a migration) acts on the real ledger. Do non-trivial work
  in a git worktree and fast-forward master when green.
- **Never start a second loop.** Two loops against one ledger double every post and
  every dispatch. Check `make status` before loading anything.
- **The ledger** `~/.warden/warden.db` is live and not in git. Never migrate it from
  a session — copy it (`cp ~/.warden/warden.db /tmp/`) and test there. Read-only
  means `sqlite3.connect(f"file:{p}?mode=ro", uri=True)`. Backup is `VACUUM INTO`,
  never `cp`/`rsync` of the open file.
- **A dirty or diverged checkout of any repo blocks its deploys**: the loop
  fast-forwards only a clean default-branch checkout that ends at origin, else it
  strikes (third → `failed`). That includes this one.
- **`scripts/triage.py` has no `--help`**: any flag it does not know runs a full live
  pass against the real ledger. Run the loop by hand only with `--dry-run` and `env -u CLAUDECODE …` (sideclaw's
  recursion guard refuses dispatches from inside a Claude session).
- **Cross-repo:** `hermes-ops.sh`, `agents-overview.py` and the dispatch shim (which
  `exec`s `scripts/warden`) live in `hermes-agent`; `~/.hermes/{scripts,config}` are
  whole-directory symlinks into that repo.

## Git

Direct-to-master. `/commit` per logical concern, no attribution footers. Append a §
to `docs/history/state-log.md` and rewrite `STATE.md` in the same commit as the work
it describes — the next session is a stranger.
