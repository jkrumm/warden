# STATE — warden implementation

**Read this before anything else. It is the memory; the conversation is not.**
Authority order: `DESIGN.md` → `FLOWS.md` → `REVIEW.md` → this file.
This file records *what is*, not *what should be*.

| | |
|-|-|
| Last updated | 2026-09-09 |
| Current wave | **0 — reconnaissance complete, no edits made** |
| Repo state | `master`, clean, 3 commits, docs only. No code. |
| Next action | see § Next action (bottom) |

---

## 1. Baseline — measured, not assumed

All commands run 2026-09-09 on the mini, before any edit.

### Test suites (hermes-agent, hand-rolled runners, not pytest)

```
$ cd ~/SourceRoot/hermes-agent
$ ~/.hermes/hermes-agent/venv/bin/python3 tests/test_triage.py
68/68 passed
```

| Suite | Result | Note |
|-|-|-|
| `tests/test_triage.py` | **68/68 passed** | the number that must not regress |
| `tests/test_dispatch_sweep.py` | 12/12, `all cases as expected` | |
| `tests/test_dispatch_approval.py` | `51 checks, 0 failure(s)` | straddles both repos |
| `tests/test_watchdog_delivery.py` | `all cases as expected` | |
| `tests/test_watchdog_slack_blindness.py` | `all cases as expected` | |
| `tests/test_agents_overview.py` | `all cases as expected` | stays in hermes-agent |
| `tests/test_cron_allowlist.py` | `all cases as expected` | stays in hermes-agent |

`docs/triage.md` § Tests claims **64** cases. The runner reports **68**. The doc
is stale by 4 (added by `1ae44b8 feat(triage): make an idle pass distinguishable
from a dead loop` and its predecessors). Not a defect — but the doc moves with the
code, so fix the count when it moves.

### Interpreter

`~/.hermes/hermes-agent/venv/bin/python3` → **Python 3.11.15**, 125 packages.
Relevant: `cryptography==50.0.0`, `requests==2.33.0`, `PyYAML==6.0.3`,
`slack_sdk==3.43.0`, `python-dateutil`, `pytz`. This venv belongs to the *installed
hermes CLI* (`~/.hermes/hermes-agent/`), not to `~/SourceRoot/hermes-agent`.
**warden has no venv yet** — that is what DESIGN.md § Migration means by "the
Ed25519 verifier needs `cryptography` from a venv that does not exist yet".
`cryptography` itself is present in the hermes venv; the gap is warden's own.

---

## 2. The ledger — `~/.hermes/watchdog.db`

**Not in git.** 1,081,344 bytes. No `-wal` / `-shm` files present, which is the
first confirmation of the pragma reading below.

```
journal_mode     delete        <- DESIGN.md C4 confirmed: NOT WAL
busy_timeout     5000          <- see caveat
page_size        4096
user_version     0             <- no schema versioning
synchronous      2 (FULL)
foreign_keys     0             <- OFF, despite triage_items REFERENCES events(id)
```

**Caveat on `busy_timeout`.** DESIGN.md § The ledger says `db_connect()` is a bare
`sqlite3.connect()` with "no WAL, no `busy_timeout`". The *pragma* reads 5000
because Python's `sqlite3.connect(timeout=5.0)` default sets it per-connection.
So the practical exposure is narrower than the doc states: the 5s wait exists by
accident of the stdlib default, not by intent, and any non-Python writer
(`hermes-cc.sh`'s embedded python is still Python; a `sqlite3` CLI writer is not)
gets no timeout at all. **WAL is genuinely absent and is the real defect** —
under `journal_mode=delete` a reader blocks a writer outright.

### Live census (2026-09-09)

| table | rows |
|-|-|
| `events` | 956 |
| `triage_items` | 49 |
| `dispatches` | 22 (all `status='done'`) |
| `dispatch_approvals` | 5 |
| `cursors` | 9 |

`triage_items` by state: `resolved` 28 · `note` 8 · `ignored` 7 ·
`needs_human` **3** · `new` 3. Matches DESIGN.md's numbers exactly.

### Schema-accretion evidence (DESIGN.md C4: "two independent migrators, no version table")

The DDL shows raw `ALTER TABLE` accretion — added columns appear appended after
the closing paren of the original `CREATE TABLE`:

- `dispatches` … `, merged_at TEXT, poll_misses INTEGER NOT NULL DEFAULT 0, validation_job_id TEXT, validation_status TEXT)`
- `triage_items` … `, implement_job TEXT, validation_job TEXT, pr_url TEXT, deploy_expect_json TEXT, liveness_deadline TEXT, propose_unsure_at TEXT)`
- `dispatch_approvals` … `, argv_json TEXT, stdin_text TEXT, context_text TEXT)`
- `events` … `resolved_at TEXT, dispatch_id INTEGER,`

There is **no `schema_version` table and `user_version` is 0**. Confirmed.

### Backup — DESIGN.md's open question, RESOLVED

DESIGN.md § Observability: *"Open: restic runs on homelab and the mini is not
currently in its source set — verify the paths before assuming coverage."*

**It is covered today, transitively.** Chain, verified end to end:

| Step | Evidence |
|-|-|
| mini → homelab, daily 03:00 | `com.jkrumm.hermes-backup` → `scripts/hermes-backup.sh:60`, `rsync -az --delete ~/.hermes/ homelab:/mnt/hdd/backups/hermes/` |
| `watchdog.db` is included | no `--exclude` in `hermes-backup.sh:60-73` matches it (excludes are `audio_cache/ image_cache/ cache/ sandboxes/ sessions/ *.lock *.pid /.env hermes-agent/ .update_check .skills_prompt_snapshot.json`) |
| homelab → B2, daily 03:30 | `homelab/docker-compose.yml:247` mounts `/mnt/hdd/backups:/sources/hermes-backup:ro` into `restic-backup`; `BACKUP_CRON: "0 30 3 * * *"` (:222) |
| retention | `--keep-daily 14 --keep-weekly 8 --keep-monthly 12 --keep-yearly 5` (:230-233) |
| repo | `s3://…backblazeb2.com/jkrumm/backups/homelab/restic`, append-only key |

**The consequence for Wave 0 is the opposite of what the open question implied:**
coverage exists *because the ledger sits under `~/.hermes/`*. The moment warden's
ledger moves to `~/.warden/warden.db`, it falls out of the rsync source and out of
restic — silently. So the Wave 0 backup work is not "invent a path", it is
"either keep the file under a path the existing rsync covers, or add the leg,
and prove it". Also note the current backup rsyncs a **live SQLite file with no
snapshot** — which is exactly why `VACUUM INTO` is the requirement, not an extra.

---

## 3. The symlink farm — the trap

`~/.hermes/` contains **seven** whole-target symlinks into `~/SourceRoot/hermes-agent`:

| link | → target | kind |
|-|-|-|
| `~/.hermes/scripts` | `~/SourceRoot/hermes-agent/scripts` | **whole directory** |
| `~/.hermes/config` | `~/SourceRoot/hermes-agent/config` | **whole directory** |
| `~/.hermes/cron` | `~/SourceRoot/hermes-agent/cron` | whole directory |
| `~/.hermes/hooks` | `~/SourceRoot/hermes-agent/hooks` | whole directory |
| `~/.hermes/config.yaml` | `…/config.yaml` | file |
| `~/.hermes/.env.tpl` | `…/.env.tpl` | file |
| `~/.hermes/SOUL.md` | `…/SOUL.md` | file |

`~/.hermes/bin` and `~/.hermes/plugins` are **real directories** (plugins holds
per-plugin symlinks; `HERMES_PLUGINS` in the Makefile is the source of truth).

**Why this is the trap.** `~/.hermes/scripts` is one link covering 21 files, of
which only 5–6 are leaving. You cannot repoint the link. The 15 staying are:

```
agents-cron.py            agents-overview.py        brain-commit.sh
briefing-context.py       briefing-coverage.py      briefing-state.json
hermes-backup.sh          hermes-liveness.sh        hermes-ops.sh
narratives-cron.py        project-narratives.py     watchdog-summary.py
validate-dispatch-policy.py                         (+ briefing-state.example.json)
```

**And there is a hard constraint on the directory itself:** per
`hermes-agent/docs/symlinks-and-agents.md`, the gateway's cron security check
*requires pre-run scripts to live under `HERMES_HOME/scripts/`*. So any script
still registered as a `hermes cron` job cannot move out of that directory.
Wave 0 dissolves that constraint for the two jobs in scope by deleting the cron
registrations entirely (§5) — but it is the reason a partial move is impossible.

`config/` is the same shape: two files, `dispatch-repos.json` (hermes-cc.sh's
policy, **stays** — it bounds every dispatch, not just triage) and
`triage-policy.json` (**leaves**).

---

## 4. LaunchAgents — current

`launchctl list | grep -E 'hermes|warden|sideclaw'`:

```
12121   0   com.jkrumm.sideclaw-server
-       0   com.jkrumm.hermes-triage
65056   75  ai.hermes.gateway
-       0   com.jkrumm.hermes-backup
-       0   com.jkrumm.hermes-liveness
```

(`-` = not currently running, i.e. an interval agent between firings. Second
column is the last exit status; all 0 except the gateway's 75, which is its own
supervised-restart convention, not a triage concern.)

| Label | Interval | Runs | Fate in Wave 0 |
|-|-|-|-|
| `com.jkrumm.hermes-triage` | `StartInterval 600`, `RunAtLoad` | `~/.hermes/hermes-agent/venv/bin/python3 ~/SourceRoot/hermes-agent/scripts/triage.py --run` | **repoint to warden** |
| `com.jkrumm.hermes-liveness` | 300s | gateway health + Kuma push | stays |
| `com.jkrumm.hermes-backup` | daily 03:00 | rsync `~/.hermes/` → homelab | stays; see §2 backup |
| `ai.hermes.gateway` | — | the gateway | stays |
| `com.jkrumm.sideclaw-server` | — | :7705 | stays |

**Note the plist uses an absolute `~/SourceRoot/hermes-agent/scripts/triage.py`,
not the `~/.hermes/scripts/` symlink** — so the triage LaunchAgent does not
depend on the symlink farm at all. Good: one fewer coupling to unwind.

Plist templates live in `hermes-agent/launchd/` with `__HOME__` substitution,
rendered by `make setup` (`_agents` → `_render-plists`); unchanged content is a
no-op so re-running never bounces a healthy agent. warden needs the same shape.
Logs are **declared, never globbed**, in `dotfiles/scripts/log-rotate.sh`'s
`FILES` array — `hermes-{liveness,backup,triage}.{log,err}`. **A warden log that
is not added to that array is a log nothing rotates.**

---

## 5. The two gateway cron jobs — the Wave 0 targets

From `~/SourceRoot/hermes-agent/cron/jobs.json` (7 jobs total):

| id | name | script | schedule | `no_agent` | enabled | deliver |
|-|-|-|-|-|-|-|
| `4b1faabda97d` | **Watchdog** | `watchdog-slack.py` | `*/30 * * * *` | **true** | true | `slack:C0ASRULFTSS` |
| `4dd759917dd1` | **Dispatch sweep** | `dispatch-sweep-cron.py` | `*/5 * * * *` | **true** | true | `slack:C0ASRULFTSS` |
| `cc7900c424a9` | Morning briefing | `briefing-context.py` | `0 7 * * 1-5` | — | true | `slack:C0AT6TH404R` |
| `2d38c80e685c` | Evening report | `briefing-context.py` | `0 22 * * 1-4` | — | true | `slack:C0AT6TH404R` |
| `8fe7be4985d9` | Brain drift audit | — | `0 9 * * 6` | false | true | `slack:C0ASRULFTSS` |
| `72aa2fb36307` | Agents overview | `agents-cron.py` | `*/30 * * * *` | true | **false** | `slack:C0BVDE5R562` |
| `9909f808fe17` | Project narratives | `narratives-cron.py` | `30 6 * * *` | true | true | `slack:C0BVDE5R562` |

Both targets are `no_agent: true` — **no LLM call is involved**; the gateway runs
the script and posts stdout to the channel verbatim. That is what makes the
LaunchAgent promotion a straight lift rather than a redesign.

The "Watchdog" job still carries a 3169-char `prompt` from before it became
`no_agent`. It is dead text. Do not port it.

### What each wrapper actually is, and why it becomes an orphan

Both wrappers exist **only** to satisfy the gateway cron runner's contract
(invoked as `python3 <path>` with no args; stdout = the Slack body; empty stdout
= silent delivery). They are `importlib.util.spec_from_file_location` loaders
because the real filenames contain a hyphen and are not importable.

`dispatch-sweep-cron.py` — 1.5 KB. Loads `dispatch-sweep.py`, `sys.exit(main([]))`.
Its docstring records a second reason it is terse: *the cron-creation guard walks
anything that tokenizes like a referenced script and fails closed when it exhausts
its recursion budget*, so a long file — or even a short one quoting filenames — is
rejected. Under a LaunchAgent that guard does not apply. **Pure orphan; delete.**

`watchdog-slack.py` — 1.9 KB. Loads `watchdog-poll.py`, calls
`main(["--slack-body"])`. **It is NOT a pure orphan.** It carries behaviour that
must not be lost:

> **Carry-over #1 — the UptimeKuma self-health heartbeat.** On `rc == 0` only, it
> resolves `UPTIME_PUSH_WATCHDOG` via `_mod.resolve_secret()` and GETs the push URL
> with `User-Agent: curl/8.7.1` (uptime.jkrumm.com sits behind Cloudflare, which
> **403s the default Python-urllib UA**). Best-effort, exceptions swallowed, and
> deliberately stdout-silent because any output would corrupt the `no_agent` Slack
> body. A crash, hang, or non-zero exit therefore trips the *"Watchdog last
> successful run"* Kuma alert. **This is the only thing that notices the ingest
> poller has died — i.e. it is the existing instance of DESIGN.md's "Minutes with
> no poller running: 0, and alarmed".** Port it before deleting the wrapper.

---

## 6. Dispatch policy — a live violation of DESIGN.md's security model

`~/SourceRoot/hermes-agent/config/dispatch-repos.json`:

```
root         = ~/SourceRoot
defaultTier  = implement          <-- absence is NOT a denial any more
deny         = [dotfiles-private, homelab-private]
sensitive    = [dotfiles-private, homelab-private]
tiers        = { investigate: [dotfiles, brain, hermes-agent] }
```

The file used to be an inventory (absence = denial) and was deliberately changed
to a **policy** (discovery under `root`, this file records only exceptions)
because the inventory drifted — 22 listed vs 30 on disk, three unreachable for
their whole existence with nobody noticing. That rationale is sound and is not
being re-litigated.

**But the consequence, right now:**

| repo | resolves to | DESIGN.md requires |
|-|-|-|
| `sideclaw` | `implement` (default) | **never tier ≥ 1** |
| `warden` | `implement` (default) — *it exists under `~/SourceRoot` as of today* | **never tier ≥ 1** |

DESIGN.md § Security model, verbatim: *"warden may never hold tier ≥ 1 on
`sideclaw` or on `warden` itself. sideclaw is a valid dispatch target today;
auto-merging PRs against your own executor closes a loop that has no outside."*

Creating this repo **widened** that hole rather than being neutral to it. This is
not a design conflict — the design already calls it — it is an unfixed instance,
and it is squarely inside Wave 0's "re-assert the repo allowlist". It must be
fixed in **both** places: `dispatch-repos.json` (warden's copy, defence in depth)
and **inside sideclaw** (the boundary).

Also recorded, because a future edit will otherwise get it wrong: a name in `deny`
that also appears in `tiers` is a **contradiction the resolver refuses to run on**,
not a precedence question. Same for a name in `sensitive` that is not in `deny`.
`sensitive` grants exactly one thing — `investigate` only, derived from the repo
name alone with no caller-facing flag — and sets `"sensitive": true` on the
submitted body so sideclaw's `assertSensitiveTierAllowed` + `applySensitiveScan`
re-check independently.

---

## 7. External references to the moving parts

Everything outside `hermes-agent` that names `watchdog.db` or `hermes-cc.sh`:

| File | What it says | Action |
|-|-|-|
| `dotfiles/docs/architecture.md:171` | describes `com.jkrumm.hermes-triage` and why it is a LaunchAgent | **must be updated** when the agent repoints |
| `dotfiles/scripts/log-rotate.sh:78` | log rotation for `hermes-cc.sh`'s audit log | check; add warden's logs |
| `sideclaw/docs/dispatch-security.md` | the executor-side security model | read before the sideclaw change |
| `brain/wiki/engineering/incident-triage-loop.md` | knowledge page | update after the move, not during |
| `brain/wiki/engineering/hermes-as-control-surface.md` | knowledge page | ditto |
| `brain/wiki/engineering/agent-dispatch-paths.md` | knowledge page | ditto |

`hermes-agent/Makefile` couplings: `HERMES_PLISTS := com.jkrumm.hermes-liveness
com.jkrumm.hermes-backup com.jkrumm.hermes-triage` (line 22) — the triage entry
leaves. `make status` runs `scripts/validate-dispatch-policy.py` against
`config/dispatch-repos.json` (line 256) and probes sideclaw :7705 (line 266).

`hermes-agent/docs/`: `triage.md` (**1027 lines — moves with the code**),
`watchdog.md` (41), `dispatch-bridge.md` (595 — splits, like `hermes-cc.sh`).

---

## 8. sideclaw — liveness confirmed

```
$ curl -s http://127.0.0.1:7705/api/routing
{"ok":true,"routes":{ … "dispatch":{"model":"claude-sonnet-5[1m]","backend":"max","fallback":{"backend":"iu"},"transport":"session"} … },"overrides":[]}

$ curl -s -o /dev/null -w '%{http_code}' http://127.0.0.1:7705/api/jobs
200
```

`GET /api/jobs` returns 200 with **no credential presented** — the first direct
confirmation of DESIGN.md C5's "no auth" claim on the job API.

---

## 9. Open questions — things the repo does not answer

Numbered so a later session can cite them.

- **Q1. Where does warden's ledger live?** `~/.warden/warden.db` is the clean
  answer and it silently leaves the backup path (§2). `~/.hermes/watchdog.db`
  in place keeps backup coverage but keeps the coupling DESIGN.md § The ledger
  says must end (*"the Slack approval plugin must stop writing this file
  directly, or the coupling silently returns"*). **Not blocking** — decide it as
  the first Wave 0 slice and record the decision here.
- **Q2. Does `watchdog-poll.py` move at all in Wave 0?** Its `stray_skill` source
  walks `~/.hermes/skills/` and is documented as *the primary defence* for
  agent-created skills — a hermes concern, not a warden one, riding in the same
  file. DESIGN.md § Open questions #3 asks the same thing about `stray_skill` 849.
- **Q3. Who owns `hermes-cc.sh`'s ~700-line GitHub module?** DESIGN.md § Open
  questions #2, unanswered. Wave 0 can defer it by leaving `hermes-cc.sh` in
  place and having warden shell out to it exactly as `triage.py` does today —
  but that must be a *recorded* deferral, not a silent one.
- **Q4. `agents-overview.py` stays but depends on `hermes-cc.sh`.** Compat path
  or its own client? Same DESIGN.md open question. Deferrable with Q3.

---

## 10. Reconnaissance verdict

Complete. Nothing has been edited. Three things the recon changed versus the doc:

1. **The extraction is cleaner than DESIGN.md assumes** — zero hermes-package
   imports, pure stdlib, and the one named cross-dependency
   (`agents-overview.py` → `hermes-cc.sh`) does not exist. See §12.
2. **The sideclaw hole is wider than C5 states** — `sensitive` is caller-declared,
   so the `deny` list has no representation at the boundary at all. See §11.
3. **The backup open question is answered, and inverts** — coverage exists today
   *because* the ledger sits under `~/.hermes/`. Moving it is what breaks it. See §2.

None of the three changes the design. All three change Wave 0's shape.

## 11. sideclaw — the executor, mapped

Bun + Elysia. Bound `127.0.0.1:7705`, loopback only (`server/index.ts:127-131`).
Deploy is `make reload` — **never** `bun run dev`/`start` (both `exit 1`,
`Makefile:1-5`).

### Commands (there is no `make check`)

| Purpose | Command |
|-|-|
| test | `bun test` (`package.json:10`) |
| typecheck | `bun run typecheck` → `tsc --noEmit` (`:9`) |
| lint | `bun run lint` → `oxlint src server tests` (`:11`) |
| format check | `bun run format:check` → `oxfmt src server tests --check` (`:13`) |
| build | `make build` |
| deploy | `make reload` (`Makefile:84`) |

`bunfig.toml` preloads `tests/setup.ts`, which redirects every state root
(`SIDECLAW_JOBS_DB`, `SIDECLAW_WORKTREE_ROOT`, `SIDECLAW_SALVAGE_ROOT`,
`SIDECLAW_PRIVATE_VERDICTS_ROOT`) to temp dirs — so tests never touch live state.
21 test files; dispatch-relevant: `dispatch-prompt`, `dispatch-worktree`,
`dispatch-git-pure`, `shutdown-dispatch-coupling`, `routing`, `jobs-health`.

### `POST /api/jobs` — DESIGN.md C5 confirmed, and it is worse than stated

`server/routes/jobs.ts:13-29`. **No authentication, no allowlist, no rate limit.**
The only gate is `isJobTool(body.tool)` (`server/jobs/types.ts:30-32`).

`params` is `t.Optional(t.Record(t.String(), t.Unknown()))` — **completely
unvalidated at the HTTP boundary** (`jobs.ts:24-27`), stored as JSON text
(`store.ts:272-275`), and only validated at *execution* time by the handler's own
zod (`dispatch.ts:544`).

`cwd` validation is three checks, all local-filesystem-shaped, all inside
`runDispatch`:

| Check | Where |
|-|-|
| `z.string()` — no shape, no absolute-path requirement, no prefix constraint | `dispatch.ts:59-65` |
| `existsSync(cwd)` | `dispatch.ts:545` |
| `existsSync(join(cwd, ".git"))` | `dispatch.ts:546-548` |

The perimeter is stated only as a comment (`server/index.ts:127-131`): *"nothing
here carries auth of its own, so a tailnet-reachable bind would be an
unauthenticated job submitter one ACL grant away."*

The only header gate in the whole app is `x-sideclaw-shutdown: 1` on
`POST /api/shutdown` (`shutdown.ts:31,36`), and its own comment says it is
explicitly **not** auth (`shutdown.ts:29-30`).

> **NEW FINDING — sharper than DESIGN.md C5, and it changes what Wave 0 must
> cover.** DESIGN.md says the repo allowlist is missing. The concrete consequence
> is that **the `deny` list does not exist at the boundary at all.**
> `dotfiles-private` and `homelab-private` are denied *only* inside
> `hermes-cc.sh`'s `resolve_repo()`. Separately, `sensitive: boolean`
> (`dispatch.ts:99-109`) is **caller-declared and never verified** — nothing
> checks whether `cwd` is actually a secret-bearing repo. So today
> `POST /api/jobs {tool:"dispatch", params:{cwd:"~/SourceRoot/homelab-private",
> tier:"implement"}}` — with `sensitive` simply omitted — bypasses both the denial
> and `assertSensitiveTierAllowed`, from any local process, with no credential.
> `dotfiles-private` and `homelab-private` are the two repos whose entire purpose
> is that their contents are not referenced from anywhere else.
> **This is not a re-litigation of anything in REVIEW.md — it is the same C5
> hole, measured, and it means the sideclaw allowlist must be a *deny*-capable
> policy, not merely an "is it under `~/SourceRoot`" prefix check.**

### Tiers — `TIERS`, `dispatch.ts:242-268`

Enum: `z.enum(["investigate","author","implement"]).default("investigate")`
(`dispatch.ts:75-83`).

| tier | readOnly | maxTurns | timeout | worktree | pushes | artifact | `sensitive:true` |
|-|-|-|-|-|-|-|-|
| `investigate` | true (`:244`) | 25 | 8 min | `createReadWorktree`, `pushable:false` | no | — | **allowed** |
| `author` | true (`:251`) | 30 | 10 min | same | no | issue | refused (`:420-428`) |
| `implement` | false (`:258`) | 60 | 30 min | `createWorktree` from default branch, `pushable:true` | yes | **draft** PR | refused |

**Every tier gets a worktree, read-only ones included** — matches DESIGN.md.
`readOnly` is exactly three tool names, verbatim `session-runner.ts:552-557`:

```ts
  if (readOnly) {
    args.push(
      "--disallowedTools",
      ["Write", "Edit", "NotebookEdit", ...(extraDisallowedTools ?? [])].join(","),
    );
  }
```

Sessions run under `--dangerously-skip-permissions`; **Bash is unrestricted**.

### The two confirmed escapes — DESIGN.md quote located

`server/jobs/handlers/dispatch-git.ts:155-170`, on `GIT_DENY_CREDENTIALS_ENV`:

> *"It is NOT a privilege boundary… A session that wants to get around it can:
> restore the config it was denied — `GIT_CONFIG_GLOBAL=$HOME/.gitconfig git push …`;
> resolve the token itself — `secrets-run read op://mini/github/token` needs no
> prompt on this host, and the user-level CLAUDE.md this tier deliberately loads
> spells that recipe out. Both were found by adversarial review, both were
> confirmed, and neither is fixable with another environment variable — **the
> honest fix is an OS-level sandbox** (restricted PATH is theatre: an absolute
> path defeats it)."*

Corroborating: `session-runner.ts:547-551`, `mcp/tools/dispatch.ts:26`
(*"a leak backstop on the way OUT, not a sandbox"*), `dispatch-git.ts:172-173`
(*"The guarantees that ARE structural… constrain THE HANDLER — never the session"*).

**DESIGN.md § Security model is accurate. Design premise unchanged.**

### The verdict schema — `dispatch.ts:115-226`

`VERDICT_FIELDS` (`:123-156`), shared by every tier:

| field | zod |
|-|-|
| `verdict` | `z.string().min(1).max(4000)` |
| `confidence` | `z.enum(["high","medium","low"])` |
| `evidence` | `z.array(z.strictObject({file:…max(500), detail:…max(1000)})).max(30)` |
| `recommendation` | `z.string().min(1).max(2000)` |
| **`nextAction`** | **`z.enum(["none","issue","implement","human"])`** (`:148-150`) |
| `summary` | `z.string().min(1).max(200)` |

`+ ISSUE_FIELDS` (`:162-165`) for `author`, `+ PR_FIELDS` (`:167-173`) for
`implement`; `WORKER_OUTPUT` is the `strictObject`-per-tier map (`:222-226`).
`DISPATCH_OUTPUT` (`:175-213`) is the caller-facing union plus three handler-only
optionals: `degraded`, `artifactUrl`, `branch`.

- **No version field anywhere.** Grepped: no `version`, `schemaVersion`, `v`.
- **Not consumable outside the process.** Serialized exactly once, in-process and
  discarded: `z.toJSONSchema(WORKER_OUTPUT[tier])` → `--json-schema` CLI flag
  (`dispatch.ts:626`). No endpoint, no generated file, no `dist` artifact.
  `GET /api/routing` exposes routing only.
- **`outcome` already exists — in `review`, not `dispatch`.**
  `server/jobs/handlers/review.ts:127`: `outcome: z.enum(["clean","actionable","needs-human"])`.
  **This is the in-repo precedent to follow for the dispatch `outcome` enum.**
  Elsewhere `outcome` is only an ad-hoc *log* field (`dispatch.ts:943`
  `"salvaged"`; `session-runner.ts:881,892` `"ok"|"error"|"timeout"`).

### The prose concatenation — DESIGN.md's "seven handler outcomes", enumerated

Assembly: `dispatch.ts:773-779`, `verdict: data.verdict + artifactNote`.
**Nine string-level outcomes**: seven `artifactNote` branches plus two wholesale
`verdict` replacements. This is the list the typed enum must cover.

| # | Tier | Condition | Where | Effect |
|-|-|-|-|-|
| 1 | investigate | always | `:711-712` | `""`, verdict unmodified |
| 2 | author | `artifactText()` null (empty title/body) | `:714-719` | `" No issue was filed: the episode concluded there was nothing worth tracking."` |
| 3 | author | `openIssue` threw | `:731-740` | `" No issue was filed: ${err.message}"` |
| 4 | author | filed OK | `:727-730` | `""` + `artifactUrl` |
| 5 | implement | `commitCount === 0` | `:820-822` | `" No branch was pushed: the episode changed nothing."` |
| 6 | implement | `diffRefusalReason` non-null | `:825-841` | `" The branch was DISCARDED and nothing was pushed: …"` |
| 7 | implement | pushed, no PR text authored | `:846-856` | `" The branch was pushed but NO pull request was opened…"` + `branch` |
| 8 | implement | pushed, `openPullRequest` threw | `:866-884` | `" …could NOT be opened: ${err.message}…"` + `branch` |
| 9 | implement | full success | `:860-865` | `""` + `artifactUrl` + `branch` |

Two wholesale replacements:

- **Salvage** — `salvage()`, `:894-970`, entered from `:682-699`. Returns
  `degraded:true`, `confidence:"low"`, `evidence:[]`, `nextAction:"human"`, and a
  `verdict` = fixed preamble (`:954-957`) + one of three `branchNote` variants
  (`:916` / `:921-923` / `:929`) + `raw.slice(0,3000)` (`:958-960`).
- **Sensitive-withheld** — `applySensitiveScan` (`:501-533`). The exact string
  DESIGN.md names is built at **`:522-524`**:
  `` `Verdict withheld: matched ${hits.join(", ")}. The full, unmodified text was saved locally at ${path}…` ``
  Then `:525-532` overwrites `summary`, `verdict`, `recommendation`, sets
  `evidence: []` and **`nextAction: "human"`** — `confidence` deliberately
  preserved. Applied on **both** return paths, success (`:779`) and salvage
  (`:688-698`).

  > **DESIGN.md's claim verified exactly:** a withheld verdict is
  > `nextAction: "human"` with empty evidence and is **indistinguishable from a
  > genuine needs-human** except by substring-matching `"Verdict withheld: matched "`.
  > Full text persists at `~/.local/state/sideclaw/private-verdicts/`, mode 0600
  > (`dispatch-git.ts:774,795`); warn log `dispatch.verdict_withheld`
  > (`dispatch.ts:510-520`). Secret patterns: `dispatch-git.ts:66-91`, 8 of them.

### Job store — `server/jobs/store.ts`

bun:sqlite on disk, `~/.local/share/sideclaw/jobs.db` (`:29-31`), **WAL +
`busy_timeout=5000`** (`:65-66`). Table `jobs` (`:67-81`), one migration (`:86-90`).
Status enum (`types.ts:53`): `pending | running | done | failed | interrupted`;
terminal = `done|failed|interrupted` (`:56-60`). `MAX_CONCURRENT = 3` (`store.ts:38`).

**Prune confirmed, `store.ts:40-42`:** `PRUNE_TTL_MS = 24h`, `MAX_TERMINAL_ROWS = 200`.
`prune()` (`:609-623`) is **tool-agnostic** — it filters on `status` only, never
on `tool`. Called at boot (`:264`) and after **every** `finish()` (`:562`).

> **A dispatch verdict is deleted 24h after `finished_at`, or sooner if 200 newer
> terminal rows arrive — and that 200-row budget is shared with every interactive
> `/check`.** Exactly DESIGN.md's "an item whose job was pruned is stuck forever
> with nothing polling it out". Confirmed at the source.

Only tool-aware behaviour is *recovery*, not pruning:
`REQUEUE_ON_RECOVER = {check, overview, narrative, review}` (`:215-216`) —
**`dispatch` is deliberately excluded** (`:210-214`), so an interrupted dispatch
goes terminal-`interrupted` rather than re-running. (Matches DESIGN.md § Abort:
*"sideclaw deliberately never recovers `dispatch` on restart"*.)

### Cancel — does not exist (Wave 3, recorded now so it is not re-derived)

Zero hits for `cancel` across `server/**/*.ts`. `server/routes/jobs.ts` has
exactly four handlers. The child process **is** tracked but by the wrong key:
`activeProcs = Map<Bun.spawn handle, jobId|undefined>` (`session-runner.ts:793`,
set `:1035`, deleted `:1036`). Only consumers are `activeSessionCount()` (`:795`)
and `terminateActiveSessions()` (`:805-814`) — **all-or-nothing**, no per-job
variant. No `AbortController` in the session path; termination is
`SIGTERM → 5s → SIGKILL` (`:1050-1070`).

Implementing it needs: a `terminateSession(jobId)` reverse index; a
`cancelledIds` set mirroring the existing `drainKilledIds` pattern
(`store.ts:138-142`, `:467-490`) so `execute()` can tell cancel from drain from
real failure; touching `JobStatus`, `TERMINAL_STATUSES`, both `status IN (…)`
lists in `prune()` (`:612,618-620`), `recover()` (`:592`), `jobHealth()`
(`:386,396`); a route registered **before** `/:id` (ordering note `jobs.ts:34-36`);
and suppressing `salvageWorktree` in `runDispatch`'s catch (`dispatch.ts:786-794`)
while still running its `finally` teardown (`:796-802`).

### Config pattern — where the allowlist has to live

**sideclaw has no config file. Zero JSON/YAML/TOML loading anywhere in the server.**
The pattern, in order of precedence:

1. `.env` at repo root, hand-rolled parser `server/lib/load-env.ts:18-52`,
   self-invoking at `:52`; existing env always wins (`:45`). Surface documented in
   `.env.example`.
2. `process.env` read **once at module load** into a frozen TS constant. The
   canonical example is `routing.ts:223` — `const TABLE = buildRoutingTable(process.env)`
   — with a pure exported `build*(env)` for tests (`:148-207`) and a startup logger
   of applied/refused overrides (`logRoutingOverrides`, `:277-291`).
3. TS constants in the module that owns the concern: `TIERS` (`dispatch.ts:242`),
   `SECRET_PATTERNS` (`dispatch-git.ts:66`), `PRUNE_TTL_MS` (`store.ts:41`),
   `ALLOWED_ORIGINS` (`kiosk.ts:4`).

Existing things that are **not** allowlists, checked and ruled out:
`ALLOWED_ORIGINS` (kiosk URL prefixes, `kiosk.ts:4`); `WORKSPACES` (two roots from
`PERSONAL_REPOS_PATH`/`WORK_REPOS_PATH`, `lib/workspace.ts:8-14`, used only for
path display); `scanRepos()` (dirs carrying an `sc-note.md` marker,
`lib/repo-scanner.ts:12-45`, powers `GET /api/repos` for the UI and **is never
consulted by dispatch**).

> **The idiomatic home for the Wave 0 allowlist is therefore a `routing.ts`-shaped
> module**: a `const` default policy, an env override, a pure `buildX(env)` for
> tests, a read-only `GET /api/…` projection, and a startup log of applied/refused
> entries. Not a JSON file — that would be the first one in the repo, and
> DESIGN.md principle 4 says a policy file may *name and parameterise, never
> express*, which the `const`-plus-env shape satisfies without introducing a
> loader.

### Routing — `dispatch` runs on Max

`routing.ts:118`: `dispatch` → JUDGE tier → `claude-sonnet-5[1m]`, backend `max`,
fallback `{backend:"iu"}`, transport `session`. Verified live:

```
$ curl -s http://127.0.0.1:7705/api/routing
… "dispatch":{"model":"claude-sonnet-5[1m]","backend":"max","fallback":{"backend":"iu"},"transport":"session"} …
```

`TABLE` is built at module load (`:223`), so a routing flip needs `make reload`.

### Live proof of the missing boundary — the Wave 0 before/after test

Run 2026-09-09 against the live daemon, **no credential presented**, deliberately
with a nonexistent `cwd` so no worktree, no spawn and no git ever happen:

```
$ curl -s -X POST http://127.0.0.1:7705/api/jobs -H 'content-type: application/json' \
    -d '{"tool":"dispatch","params":{"cwd":"/tmp/warden-wave0-probe-does-not-exist",
         "tier":"investigate","brief":"warden Wave 0 baseline probe"}}'
{"ok":true,"job":{"id":"8ab8f56c-…","tool":"dispatch","status":"running", …}}

$ curl -s http://127.0.0.1:7705/api/jobs/8ab8f56c-…
{"ok":true,"job":{… "status":"failed",
  "error":"Directory not found: /tmp/warden-wave0-probe-does-not-exist" …}}
```

**The submission was accepted and executed.** It failed on *path existence*, not
on policy — there is no policy to fail on. `status` went `running` → `failed` in
1 ms.

**Wave 0 acceptance test, stated now so it is not invented later.** After the
sideclaw change, the same unauthenticated POST must be refused **by policy, at
submit time** (not at execution), for each of:

| `cwd` | `tier` | must be refused because |
|-|-|-|
| `~/SourceRoot/homelab-private` | `implement` | `deny` — and `sensitive` was omitted, which is the bypass today |
| `~/SourceRoot/dotfiles-private` | `author` | `deny`; `author` is refused for a sensitive name at any tier |
| `~/SourceRoot/sideclaw` | `implement` | DESIGN.md: warden may never hold tier ≥ 1 on its own executor |
| `~/SourceRoot/warden` | `implement` | DESIGN.md: nor on itself |
| `/tmp/some-git-repo` | any | outside the dispatch root entirely |

and must still **accept** `~/SourceRoot/vps` at `implement` and
`~/SourceRoot/hermes-agent` at `investigate` (its ceiling per §6).

---

## 12. hermes-agent — the stack being extracted

### Size

| File | Lines | Bytes | Fate |
|-|-|-|-|
| `scripts/triage.py` | 3407 | 172 224 | **→ warden** |
| `scripts/hermes-cc.sh` | 2711 | 135 037 | **splits four ways** |
| `scripts/watchdog-poll.py` | 1380 | 58 169 | **→ warden** (but see Q2) |
| `scripts/dispatch-sweep.py` | 751 | 34 282 | **→ warden** |
| `scripts/agents-overview.py` | 837 | 32 874 | stays |
| `scripts/watchdog-summary.py` | 153 | 5 704 | → warden (read-only reader) |
| `scripts/validate-dispatch-policy.py` | 88 | 3 087 | stays (Makefile `status` uses it) |
| `scripts/watchdog-slack.py` | 48 | 1 908 | **orphan — delete, port the heartbeat** |
| `scripts/dispatch-sweep-cron.py` | 34 | 1 546 | **orphan — delete** |
| `plugins/dispatch-approval/__init__.py` | 609 | — | stays (gateway plugin) |
| `docs/triage.md` | 1027 | — | → warden |

### The single best piece of news: **zero coupling to the hermes package**

```
$ grep -nE '^\s*(import|from)\s+(hermes|gateway|hermes_cli)' \
    scripts/{triage,watchdog-poll,dispatch-sweep,watchdog-slack,dispatch-sweep-cron,watchdog-summary}.py
  (no output)
```

**Every moving script is pure stdlib.** No `sys.path` manipulation anywhere. The
only third-party import in the entire scope is `cryptography`, and only in
`plugins/dispatch-approval/` (which stays) and its test.

So warden's venv needs: **stdlib only for the loop**, plus `cryptography` for the
Ed25519 *verifier* that comes across with `hermes-cc.sh`'s approval half. That is
one dependency, not a port.

Cross-file linkage is entirely `importlib.util.spec_from_file_location` (the
filenames contain hyphens and are not importable):

```
watchdog-slack.py ──▶ watchdog-poll.py ◀── triage.py ──▶ agents-overview.py (STAYS)
dispatch-sweep-cron.py ──▶ dispatch-sweep.py ──▶ triage.py
                                    └── subprocess ──▶ hermes-cc.sh, ~/.local/bin/hermes
```

> ### CORRECTION TO DESIGN.md § Migration
>
> DESIGN.md states: *"`agents-overview.py` (staying) depends on `hermes-cc.sh`
> (leaving)"*. **That is wrong, and it is wrong in the direction that matters.**
> Verified first-hand:
>
> ```
> $ grep -n 'hermes-cc\|hermes_cc' scripts/agents-overview.py
> 42:dispatches — that is scripts/hermes-cc.sh's job.
> $ grep -n 'subprocess\.' scripts/agents-overview.py
> 139:        r = subprocess.run(      # <- [secrets-run, "read", ref], nothing else
> ```
>
> The only occurrence is a **docstring line explicitly disclaiming the coupling**.
> `agents-overview.py`'s sole subprocess is `secrets-run`; everything else it
> reads is HTTP against sideclaw at `localhost:7705`.
>
> The real seam runs the **other way** and is Python, not shell: `triage.py:711-717`
> dynamically loads `agents-overview.py` to borrow `resolve_slack_token`. And it
> already carries a hand-mirrored fallback at `triage.py:718-737` that runs when
> the sibling cannot be loaded — 20 lines, verbatim equivalent, written precisely
> *"so this file stays independently runnable"* (`triage.py:709`).
>
> **Consequence: deleting the dynamic load and keeping the fallback severs the seam
> cleanly, with no new code.** Same shape for `normalize_title`, borrowed from
> `watchdog-poll.py` at `:739-745` with its own fallback at `:746-755` — and that
> one becomes moot since `watchdog-poll.py` moves too.
>
> This does **not** change the design. Wave 0 is still not a `git mv` (§3, and the
> DDL race below). It removes one named obstacle and makes the extraction cleaner
> than the doc assumes. Recorded, not escalated.

### Absolute paths — every one goes through `Path.home()` / `$HOME` / `expanduser`

**No literal `/Users/jkrumm` exists in any in-scope source file.** Good: warden's
port is a constant change, not a string hunt. The ones that matter:

| file:line | Expression | Purpose | Note |
|-|-|-|-|
| `triage.py:211-212` | `Path.home()/".hermes"` → `/watchdog.db` | **ledger** | Q1 |
| `triage.py:214-215` | env `HERMES_CC_BIN` else `$HERMES_HOME/scripts/hermes-cc.sh` | dispatcher | already env-overridable |
| `triage.py:220-225` | env `HERMES_CC_REPOS_JSON` / `HERMES_TRIAGE_POLICY` | the two configs | already env-overridable |
| **`triage.py:394`** | `Path(__file__).parent/"hermes-ops.sh"` | **`env-check` verb argv** | **cross-repo: `hermes-ops.sh` (62 KB) STAYS** |
| `triage.py:425-427` | `~/SourceRoot/meteo/var/health.json`, `$HERMES_HOME/gateway-starts.log`, `$HERMES_HOME/logs/errors.log` | evidence probes | two are hermes-owned files warden must keep reading |
| **`triage.py:575`** | `Path(__file__).parent.parent` = `TRIAGE_REPO_DIR` | **`git -C` target for the policy auto-commit** (`:3033`, `:3054`) | **silent-failure risk, see below** |
| `watchdog-poll.py:33-36` | `scripts/briefing-state.json`, `cron/jobs.json`, `config.yaml`, `skills/` | four **hermes-owned** inputs | this is Q2's substance |
| `dispatch-sweep.py:93` | `~/.local/bin/hermes` | **`hermes send`** — its delivery path | the one non-`urllib` Slack path |
| **`hermes-cc.sh:975`** | `$HERMES_HOME/hermes-agent/venv/bin/python3` | **Ed25519 verifier interpreter** | hard dep on the *gateway's* venv for `cryptography` |
| `hermes-cc.sh:171` | `~/.claude/pr-required-repos.json` | merge gate | **dotfiles-owned file** |

### The DDL race — DESIGN.md C4, made concrete

**Four processes race the same unversioned DDL on a `journal_mode=delete` file.**
Every connect runs `executescript(SCHEMA)` plus an ALTER block.

| Table | Declared in |
|-|-|
| `dispatches` | **three** places — `hermes-cc.sh:757` (owner), `dispatch-sweep.py:156`, `triage.py:595` |
| `events` | **two** — `watchdog-poll.py:246` (owner), `triage.py:579` |
| `cursors` | two — `watchdog-poll.py:264`, `triage.py:613` |
| `triage_items` | one — `triage.py:619` (owner) |
| `dispatch_approvals` | one — `hermes-cc.sh:784` (owner) |

Duplicated `ALTER TABLE`s: `events.dispatch_id` at `triage.py:665` **and**
`watchdog-poll.py:293`; `dispatches.merged_at` at `dispatch-sweep.py:195` **and**
`hermes-cc.sh:833`. `hermes-cc.sh` alone runs seven ALTERs (`:833-852`).

Writers on the live file — **six, exactly as DESIGN.md says**:

| Writer | Entry | Owns |
|-|-|-|
| `watchdog-poll.py:274` `db_connect` | RW | `events`, `cursors` |
| `triage.py:660` `db_connect` | RW | `triage_items`; writes `events.dispatch_id`, `dispatches.validation_*` |
| `dispatch-sweep.py:190` | RW | `dispatches` |
| `hermes-cc.sh:824` `db_py()` (embedded `python3 -c`) | RW | `dispatches`, `dispatch_approvals` |
| `hermes-cc.sh:1167` `require_signed_approval()` — a **second, separate** connect under `$APPROVAL_PY` | RW | `dispatch_approvals.spent_at` |
| `plugins/dispatch-approval/__init__.py:237` `_record_decision` | W | `dispatch_approvals` |
| (`watchdog-summary.py:108` — read-only) | R | — |

**No access from the gateway package itself** (`grep 'watchdog.db' ~/.hermes/hermes-agent --include='*.py'` → nothing). The gateway's only reach into the ledger is *through the plugin*.

`hermes-cc.sh` has **39 `python3 -c` sites plus one heredoc**, of which exactly
**two touch the DB**: `db_py()` (`:822-878`) and `require_signed_approval()`'s own
connect (`:1167`). Everything else DB-shaped routes through `db_py()`.

### `hermes-cc.sh` — the four-way split, with line ranges

**(a) Budget / policy → warden**

`require_no_recursion` 378-395 · `require_backend` 396-438 · **`resolve_repo` 439-570** ·
**`resolve_tier` 571-598** · `tier_is_gated` 599-603 · `awaiting_confirm` 604-605 ·
`tier_rank` 606-620 (unknown → 99, fails closed) · **`require_auto_from_item` 621-696** ·
`valid_origin` 727-820 · `budget_counts` 879-896 · `check_budget` 897-910 ·
`budget_json`/`budget_text` 911-940/941-991 · `merge_precheck_repo` 1681-1710 ·
`merge_count` 1711-1721 · `check_merge_budget` 1722-1751 · **`merge_gate_check` 1752-1801** ·
**`deploy_argv` 1802-1817** · `collect_expected_alerts` 1818-1858 ·
**`run_deploy_if_enabled` 1859-1915**

> **`deploy_argv` is the one-arm closed allowlist REVIEW.md confirmed.** Lines
> 1802-1817, **exactly one key**: `hyperdx-apply` → `ssh vps "cd ~/vps && make
> hyperdx-apply ENV=prod"` (`:1804`). Policy names a KEY; the code owns the argv.
> DESIGN.md § Deploy overrides the reviewer here and moves to validated data —
> that is a **Wave 5+** change, not Wave 0. Wave 0 carries this shape across
> unchanged.

Budgets in the shell, to be preserved: `MAX_DISPATCHES_PER_DAY=20`,
`MAX_IMPLEMENT_PER_DAY=5`, `MAX_MERGES_PER_DAY=3`. Triage's own, from
`docs/triage.md` § Bounds: `MAX_OPEN_INVESTIGATIONS=3` (`TRIAGE_MAX_OPEN_INVESTIGATIONS`),
`DAILY_INVESTIGATE_BUDGET=8` (`TRIAGE_DAILY_INVESTIGATE_BUDGET`, deliberately
under hermes-cc's 20), `MAX_CLUSTER_SIGNATURES=5`, `SUBPROCESS_TIMEOUT=60`,
`VERB_TIMEOUT=260`, `MAX_BRIEF_CHARS=8000`, `EVIDENCE_TIMEOUT=20`,
`EVIDENCE_CAP_CHARS=1200`, `EVIDENCE_TOTAL_CAP_CHARS=3200`,
`DEFAULT_QUIET_RESOLVE_HOURS=2`.

**Approval sub-block 947-1241 — straddles.** `approval_hash` 992-1005 ·
`post_approval_buttons` 1006-1068 · `warn_approval` 1069-1084 ·
`approval_argv_json` 1085-1109 · `mint_approval` 1110-1147 ·
**`require_signed_approval` 1148-1223** (the verifier → warden; the signer stays).

**(b) sideclaw client → thin shim**

`sideclaw_submit` 1248-1281 (body serialized by `python3` from env, **never string
concat**) · `sideclaw_get` 1282-1293 · `valid_job_id` 1294-1306 ·
`wait_for` 1468-1498 (`WAIT_TIMEOUT=170`, `WAIT_INTERVAL=5`) ·
`sideclaw_get_or_record` 1545-1562 · `sync_record` 1499-1527 · `record_dispatch` 1224-1247.

**(c) GitHub → its own module.** `gh_token` 1307-1323 (token never reaches argv —
`curl -K -`) · `github_api` 1324-1342 · `json_field` 1343-1363 ·
**`cmd_merge` 1916-2155 (240 lines)** · `pick_merge_method` 2156-2168 ·
`mark_ready_for_review` 2169-2201 (GraphQL-only un-draft).
Constants `:155-181`: `GH_OWNER=jkrumm` (a constant, not a parameter),
`GH_TOKEN_REF="op://mini/github/token"`, `MAX_MERGE_FILES=40`, `MAX_MERGE_LINES=2000`.

> **DESIGN.md's "~700 lines of GitHub API" is an overcount.** Measured: ≈400 lines
> including `cmd_merge`. The 700 figure only holds if `merge_gate_check`,
> `run_deploy_if_enabled` and `emit_merge_plan`/`emit_merged` are counted — and
> those are **warden's**. Immaterial to the split; recorded so the number is not
> re-derived wrongly.

**(d) Shared infra.** `redact` 285-295 · `audit` 296-341 · `_err` 342-356 ·
`usage_err`/`precond_err`/`remote_err`/`policy_err` (exit 64/2/3/4, `:210-215`) ·
`need` 362-363 · `in_list` 364-377 · `read_brief` 697-713 · `read_context` 714-726 ·
**`db_py` 821-878** (the single SQLite entry point, bound params from env only).

### The two invented statuses DESIGN.md says to delete

| Status | Written by | Read by | Verdict |
|-|-|-|-|
| **`queued`** | `hermes-cc.sh:1237` (`record_dispatch`) — sideclaw never emits it | `:1592` (`cmd_list` open scope), `:1534` (`record_as_job_json` → "never finished", exit 4), `:2211` (`--json` submit envelope) | delete |
| **`lost`** | **not by `hermes-cc.sh` at all** — by `dispatch-sweep.py:34` (`LOST_STATUS`), after `LOST_AFTER_MISSES=3` consecutive 404s (`:33`) | `hermes-cc.sh:1534` (accepted as terminal), `:1539-1540` (synthesizes an `error`) | delete |

**`record_as_job_json` 1528-1544** rebuilds a fake sideclaw job envelope from the
`dispatches` row when sideclaw has pruned the job, emitting `{"fromRecord": true}`.
Exit 3 = no row, exit 4 = row never terminal.

> This function is the **existing workaround for the 24h/200-row prune** confirmed
> at `sideclaw/server/jobs/store.ts:41-42` (§11). DESIGN.md says delete it, not
> move it. That is only safe once the ledger holds the verdict itself — which is
> what "the verdict is copied into the ledger the moment a terminal status is read"
> means in DESIGN.md § The model. **Deleting it before that is a regression.**
> Sequencing noted; it is a Wave 2 concern, not Wave 0.

### Callers of `hermes-cc.sh`

| Caller | file:line | Subcommand |
|-|-|-|
| `triage.py` | `:1694` | `dispatch <repo> --tier investigate --json --origin-event <id>` (brief on **stdin**) |
| `triage.py` | `:2310` | `dispatch <repo> --tier implement --auto-from-item <event_id> --json` |
| `triage.py` | `:2428` | `dispatch <repo> --tier investigate --model claude-opus-5[1m] --json` (the step-7 validation, on a different model) |
| `triage.py` | `:2280` | `status <job-id> --json` |
| `triage.py` | `:2463` | `merge <job-id> --why "…" --confirm --json` |
| `plugins/dispatch-approval/__init__.py` | `:337` `_run_cc`, from `execute_approved` `:427-440` | `bash hermes-cc.sh <stored argv> --confirm` — replays `argv_json` from the approval row |
| `Makefile` | `:255` | existence test only (`[ -x ]`), then `validate-dispatch-policy.py` at `:256` |
| `skills/claude-dispatch/SKILL.md` | `:16,19,80,264,303,362` | **prose for the Hermes LLM**, run via its terminal tool |
| `tests/test_hermes_cc.py` `:54`, `tests/test_dispatch_approval.py` `:57` | — | all verbs, real subprocess |

Not a caller: `~/.hermes/hermes-agent/tools/tirith_security.py:1416-1447` — those
are `hermes-cc.sh` mentions inside guard *refusal messages*, telling the agent to
use it instead. Never executed.

### Config — two files, two halves, two readers

`config/triage-policy.json` (15 954 B, 11 keys): `_readme` (149 lines),
`cardChannel: C0BVDE5R562`, `minOccurrences: 3`, `minOpenMinutes: 30`,
`cooldownHours: 6`, `quietResolveHours: 2`, `proposeMappingsAgeDays: 7`,
`ignoreUnstructuredSlackProse: true`, `repos: {vps: {…}}`, `rules: [38]`,
`ignore: [4]`.

Its own `:118-129` documents the split: **`triage.py` owns `rules`/`ignore`/the
thresholds; `hermes-cc.sh` reads only `repos.<name>`** for the merge/deploy gate
(`hermes-cc.sh:108-112`). So `triage-policy.json` moves to warden and
`hermes-cc.sh`'s merge half must follow it — they are one policy, already split
across two readers.

Both configs are already env-overridable (`HERMES_CC_REPOS_JSON`,
`HERMES_TRIAGE_POLICY`, `HERMES_CC_TRIAGE_POLICY_JSON`), and `triage.py:218-219`
deliberately reuses hermes-cc's variable name. **That is the migration lever:
warden can point at its own copies without touching a path constant.**

### Tests — the runner shape, and the straddle resolved

**There is no `make test` target.** Every suite is `venv/bin/python3 tests/<file>.py`.

`tests/test_triage.py`: **68 `def test_` functions**, collected reflectively at
`:1978-2001` — `sorted(globals().items())`, each called **with no arguments**,
`AssertionError` → failure line, anything else → `traceback.format_exc()`, prints
`N/M passed`, returns 1 on any failure. Zero-arg/zero-fixture means the file is
**also valid pytest input** if pytest ever appears.

DB fixture: `_triage_env()` at `:83`. `tempfile.mkdtemp(prefix="triage-test-")`,
points `triage.DB_PATH` at a throwaway (`:119`), writes throwaway
`triage-policy.json` (`:121`) and `dispatch-repos.json` (`:123`), and
monkeypatches **21+ module globals** (saved/restored dict, `:92-118`): `DB_PATH`,
`POLICY_PATH`, `DISPATCH_REPOS_JSON`, `HERMES_CC_BIN`, `resolve_slack_token`,
`post_blocks`, `update_blocks`, the five `_run_hermes_cc_*` / `_hermes_cc_status`
shims, `LIVENESS_ALLOWLIST`, `MAX_OPEN_INVESTIGATIONS`, `DAILY_INVESTIGATE_BUDGET`,
`VERB_ALLOWLIST`, `_watchdog_poll`, `METEO_HEALTH_PATH`, `GATEWAY_STARTS_LOG`,
`HERMES_ERROR_LOG`, `TRIAGE_REPO_DIR`, `_call_propose_mappings_model`,
`_resolve_openai_base_url`, `_resolve_openai_api_key`. Slack is stubbed — no
network. Two tests write a **real hermes-cc stub script** and exercise the actual
subprocess path (`:634`, `:1120`) to prove the brief travels on stdin. One test
reads the **real** `config/triage-policy.json` (`:453`).

**It moves to warden essentially verbatim.** The only edits are the `_triage_env`
entries naming hermes-owned paths (`GATEWAY_STARTS_LOG`, `HERMES_ERROR_LOG`,
`METEO_HEALTH_PATH`) and the two loader paths at `:44-47` / `:55-56`.

**`tests/test_dispatch_approval.py` — the straddle, resolved.** 20 cases. Two
constants define the seam: `CC_SCRIPT = REPO/"scripts"/"hermes-cc.sh"` (`:57`) →
**warden**; `PLUGIN = REPO/"plugins"/"dispatch-approval"/"__init__.py"` (`:58`) →
**hermes**.

| Group | Tests | Goes to |
|-|-|-|
| Verifier / policy — `require_signed_approval`, `mint_approval`, payload-hash binding, expiry, single-use, budget-before-gate | 15 (`:207,221,230,238,248,257,268,281,291,299,310,320,330,428,498`) | **warden** |
| Signer / publication — `_is_gateway_process`, `_ensure_published`, `execute_approved`, `_run_cc`, `_send_to_origin` | 4 (`:449,364,511,597`) | **hermes** |
| **`test_hash_agreement` (`:183`)** — shell `approval_hash` (`hermes-cc.sh:992`) vs plugin `payload_hash` (`__init__.py:590`), byte-for-byte | 1 | **the cross-repo contract.** Must be duplicated on both sides, or the canonical-string spec published once and pinned in both |

### `plugins/dispatch-approval/` — stays; the key model confirmed

Two files: `plugin.yaml` (manifest) and `__init__.py` (609 lines, 23 defs).

**The signing key is RAM-only.** Module-global `_SIGNING_KEY` (`:67`), minted by
`_ensure_key()` at `:188` via `Ed25519PrivateKey.generate()` — **never serialized,
a new keypair on every gateway start** (`:26-30`). Only the public half is written:
hex, atomically via `.pub.tmp` + `os.replace`, mode 0644, to
`$HERMES_HOME/dispatch-approval.pub` (`:140-153`). Live file confirmed present,
65 bytes. Publication is gated on `_is_gateway_process()` (`:128-138`) plus a
self-heal `_ensure_published()` (`:155-178`) — the fix for a 2026-08-03 clobber.

Registration: `register(ctx)` `:571-588` mints the key, then
`ctx.register_slack_action_handler("hermes_cc_approve"/"hermes_cc_deny", …)`
(`:585-586`). If minting fails it logs and returns — **the gate fails closed**,
because `hermes-cc.sh` refuses without a public key. Needs a one-time
`hermes plugins enable dispatch-approval` on top of the symlink.

It writes **exactly one statement** to the ledger (`:255-259`):
`UPDATE dispatch_approvals SET decision=?, decided_at=?, decided_by=?, signature=?
WHERE nonce=? AND decision IS NULL` — guarded so a double-click is a no-op. It
**never creates a table**; `:97-98` says *"hermes-cc.sh owns this file; we only
ever UPDATE a row it already INSERTed."*

> **This is precisely DESIGN.md § The ledger's last bullet:** *"After extraction,
> the Slack approval plugin must stop writing this file directly, or the 'control
> plane inside the thing it supervises' coupling silently returns."* One `UPDATE`,
> one table, one guard. It is a small, well-bounded surface to replace — but it is
> **Wave 1** (the intent/signature split), not Wave 0. Wave 0 must therefore leave
> the plugin pointed at whatever path the ledger ends up on (Q1).

### Ranked extraction risks

| # | Risk | Evidence |
|-|-|-|
| 1 | `~/.hermes/{scripts,config}` are whole-directory symlinks; the gateway cron guard **refuses any script resolving outside `HERMES_HOME/scripts`** | `hermes_cli/cron.py:453-462`; `Makefile:96-107` |
| 2 | `hermes-cc.sh:975` hardcodes the **gateway's** venv for Ed25519 verification | warden needs its own venv with `cryptography`, or the verifier leaves shell |
| 3 | `triage.py:394` puts `hermes-ops.sh` (**staying**, 62 KB) in warden's closed `VERB_ALLOWLIST` — a live cross-repo argv for `env-check` | `triage.py:394-397` |
| 4 | `dispatch-sweep.py:93` and the plugin `:80` both shell to `~/.local/bin/hermes send`; warden's Slack path is otherwise pure `urllib` (`triage.py:757-792`) | the one non-`urllib` delivery path |
| 5 | Four processes race unversioned DDL on a non-WAL file | §12 DDL table |
| 6 | **`triage.py:3043-3070` `git commit`s `config/triage-policy.json`** inside `TRIAGE_REPO_DIR` (`:575`). After extraction that root changes and the `_policy_git_rel_path()` guard (`:3018-3021`) returns `None`, **skipping the commit silently** rather than failing loudly | a silent-failure trap; must be made loud in Wave 0 |
| 7 | `hermes-cc.sh:171` reads `~/.claude/pr-required-repos.json` — a **dotfiles-owned** file — as the merge gate's source of truth | cross-repo config dep |

---

## Log

| When | What | Result |
|-|-|-|
| 2026-09-09 | Read DESIGN/FLOWS/REVIEW; infrastructure recon | this file |
| 2026-09-09 | Baseline all hermes-agent test suites | **68/68** on `test_triage.py`, all others pass — §1 |

---

## 13. Wave 0 — slices, and the stop condition they map to

The stop condition has five items. These slices cover them; the map column says
which. **Do not start Wave 1.**

| # | Slice | Repo | Stop-condition item | Status |
|-|-|-|-|-|
| 0.1 | Repo policy module in sideclaw: root + `deny` + per-repo tier ceiling + the two self-reference bans. Wired into `runDispatch` **and** refused at submit. `bun test` first. | sideclaw | **3 — sideclaw enforces the allowlist** | not started |
| 0.2 | Typed `outcome` enum on the dispatch verdict + a schema version, following the `review.ts:127` precedent. Publish the schema as a consumable artifact. | sideclaw | (Wave 0 scope per DESIGN.md § Migration; not in the five-item stop list, but stated work) | not started |
| 0.3 | warden repo skeleton: venv (`cryptography` only), Makefile, `launchd/*.template`, hand-rolled test runner, log-rotate registration. | warden | 1, 5 | not started |
| 0.4 | Move `triage.py`, `dispatch-sweep.py`, `watchdog-poll.py`, `watchdog-summary.py`, `triage-policy.json`, `docs/triage.md`, and the 68 tests. Sever the `agents-overview.py` seam (§12). Make the `TRIAGE_REPO_DIR` policy-commit failure **loud** (risk #6). | warden | 1, 2 | not started |
| 0.5 | Ledger: WAL + `busy_timeout`, one migrator + `schema_version`, one writer, `VACUUM INTO` backup on the heartbeat landing on a restic-covered path (§2). Stop-copy-verify if the file moves (Q1). | warden | **4** | not started |
| 0.6 | LaunchAgents for the loop, the poller and the sweeper. Delete the two cron jobs from `cron/jobs.json`. Delete `watchdog-slack.py` + `dispatch-sweep-cron.py` **after** porting the UptimeKuma heartbeat (§5, carry-over #1). | warden + hermes-agent | **1, 5** | not started |
| 0.7 | hermes-agent cleanup: `dispatch-repos.json` ceilings for `sideclaw`/`warden` (§6), `HERMES_PLISTS`, `docs/symlinks-and-agents.md`, `dotfiles/docs/architecture.md:171`. | hermes-agent + dotfiles | 3 (defence in depth) | not started |

**Ordering.** 0.1 and 0.2 are in a different repo and touch nothing warden owns —
they run first and independently, and 0.1 is the item most likely to get skipped,
which is why it is first rather than last. 0.3→0.6 are sequential. 0.7 last,
because it edits files 0.4 and 0.6 are still reading.

**Every slice: test first where a test is possible; `/check`; `/review` if
non-trivial; a fresh reviewer at the wave boundary.** The `test_triage.py`
baseline of **68/68** is the regression gate for 0.4 onward.

**Deferred out of Wave 0, recorded so it is not silently dropped:**
`hermes-cc.sh`'s four-way split (Q3/Q4) — Wave 0 leaves the shell in place and
warden shells out to it exactly as `triage.py` does today. `record_as_job_json`
deletion — unsafe until the ledger holds the verdict itself (Wave 2). The
`deploy_argv` move to validated data — Wave 5+. The plugin's direct ledger write —
Wave 1.

---

## 14. Next action

**Start slice 0.1.** Write `tests/repo-policy.test.ts` in sideclaw against the
acceptance table in §11 ("Live proof of the missing boundary"), then the policy
module in the `routing.ts` shape (`const` default + env override + pure
`buildX(env)` + read-only projection + startup log — §11 "Config pattern"), then
wire it into `runDispatch` before `existsSync(cwd)` and into the submit path.

Verify with `bun test`, `bun run typecheck`, `bun run lint`, then re-run the live
probe from §11 and paste the refusal. Deploy with `make reload`, never `bun run dev`.

