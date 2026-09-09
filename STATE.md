# STATE — warden implementation

**Read this before anything else. It is the memory; the conversation is not.**
Authority order: `DESIGN.md` → `FLOWS.md` → `REVIEW.md` → this file.
This file records *what is*, not *what should be*.

| | |
|-|-|
| Last updated | 2026-09-09 |
| Current wave | **0 — COMPLETE except one declared item (see §25). Do not start Wave 1 without reading it.** |
| Repo state | `master`. The loop, poller, sweeper and backup run here on four LaunchAgents. |
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

- **Q1. Where does warden's ledger live? — DECIDED 2026-09-09: `~/.warden/warden.db`.**
  The objection was that it leaves the backup path (§2). It does not, because the
  restic container mounts the **whole** `/mnt/hdd/backups` directory, not the
  `hermes/` subdirectory:
  `- /mnt/hdd/backups:/sources/hermes-backup:ro` (homelab `docker-compose.yml:247`,
  re-verified against the live file). So a new `warden/` sibling of `hermes/` is
  covered by B2 the moment it exists, with **zero homelab-side change**. Only the
  mini→homelab leg is new, and that is `scripts/warden-backup.sh` (slice 0.3),
  scheduled 03:10 — after `hermes-backup` at 03:00, before restic at 03:30.
  This keeps the clean separation *and* the coverage, so the coupling DESIGN.md
  § The ledger says must end (*"the Slack approval plugin must stop writing this
  file directly"*) can actually end.
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
| 6 | ~~silently skips~~ **CORRECTED — see §16.** `triage.py` does `git commit` `config/triage-policy.json` inside `TRIAGE_REPO_DIR` (`:575`), and after extraction the `_policy_git_rel_path()` guard does return `None` — but the **call site already printed** a `triage: ...` stderr line, so it was never silent. The real consequence is worse and different: `_write_policy_additions()` is never reached on that path, so proposals are **neither written nor committed** | recon error, corrected in §16 |
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
| 0.1 | Repo policy module in sideclaw: root + `deny` + per-repo tier ceiling + the two self-reference bans. Wired into `runDispatch` **and** refused at submit. `bun test` first. | sideclaw | **3 — sideclaw enforces the allowlist** | **DONE & LIVE** — sideclaw `2d225d4`, verified §20 |
| 0.2 | Typed `outcome` enum + schema version + `GET /api/dispatch-schema`. | sideclaw | DESIGN.md Wave 0 | **DONE & LIVE** — sideclaw `360990c`, §22 |
| 0.3 | warden repo skeleton: venv (`cryptography` only), Makefile, `launchd/*.template`, hand-rolled test runner, log-rotate registration. | warden | 1, 5 | **done** — `89b0c12`, see §15 |
| 0.4a | **Copy** the loop, the poller, the sweeper, the summary reader, the four suites and the two docs into warden. Sever the `agents-overview.py` seam. Repoint `hermes-ops.sh`. No behaviour change anywhere. | warden | 1, 2 | **done** — `040e3eb`, see §16 |
| 0.4b | Cut over: unload `com.jkrumm.hermes-triage`, delete the two `cron/jobs.json` entries, delete the originals and the two orphan wrappers, load warden's agents. Folded into 0.6 — one short reversible flip, so there is never an interval with two loops or none. | warden + hermes-agent | 1, 5 | not started |
| 0.5 | Ledger: WAL + `busy_timeout`, one migrator + `schema_version`. | warden + hermes-agent | **4** | **done** — `de5d80d`, completed by hermes-agent `15e50a9` (§25) |
| 0.6 | The cutover. | warden + hermes-agent | **1, 5** | **DONE & LIVE** — §23 |
| 0.7 | hermes-agent + dotfiles cleanup, the policy agreement check, the doc redirect. | hermes-agent + dotfiles | 3 (defence in depth) | **done** — `22eb31c`, `f90cf5a`, `c661a89`, dotfiles `d6d6559`/`8ce1476` |

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


---

## 15. Slice 0.3 — warden skeleton (DONE, `89b0c12`)

### What exists now

| Path | What |
|-|-|
| `Makefile` | `setup` · `venv` · `test` · `status` · `agents` · `unload` · `render-plists` |
| `requirements.txt` | one pinned entry: `cryptography==50.0.0` |
| `README.md` | the four docs and how to run it |
| `launchd/com.jkrumm.warden-loop.plist.template` | `triage.py --run`, `StartInterval 600`, `RunAtLoad` |
| `launchd/com.jkrumm.warden-sweep.plist.template` | `dispatch-sweep.py`, `StartInterval 300` |
| `launchd/com.jkrumm.warden-backup.plist.template` | `warden-backup.sh`, `StartCalendarInterval` 03:10 |
| `scripts/warden-backup.sh` | `VACUUM INTO` snapshot + rotate 7 + rsync → homelab |
| `.venv/` | Python 3.11.15, gitignored |

### Verification

```
$ make venv
  creating /Users/jkrumm/SourceRoot/warden/.venv from Python 3.11.15
  ✓ venv (Python 3.11.15)

$ .venv/bin/python3 -c "import cryptography, sys; print(cryptography.__version__); print(sys.version)"
50.0.0
3.11.15 (main, Apr  7 2026, 20:41:15) [Clang 22.1.1 ]

$ make test
  no tests found
make: *** [test] Error 1        # correct — zero tests must never report success

$ zsh -n scripts/warden-backup.sh   # exit 0
$ plutil -lint launchd/*.template   # all three OK after __HOME__ substitution
$ git check-ignore -v .venv
.gitignore:5:.venv/	.venv
```

### Decisions made in this slice

**Interpreter pinned to `python3.11` (3.11.15).** It is uv-managed at
`~/.local/share/uv/python/cpython-3.11.15-macos-aarch64-none`, shimmed at
`~/.local/bin/python3.11`. The default `python3` on this box is **3.14.7**.
Moving 3400 lines of `triage.py` to a new repo *and* a new interpreter in one
change mixes two failure sources; `make venv` fails loudly if `python3.11` is
absent rather than falling back. The upgrade is its own verifiable step, later.

**Script filenames do not change in Wave 0.** `triage.py`, `watchdog-poll.py`,
`dispatch-sweep.py` keep their names. Renaming during a lift-and-shift breaks the
68 tests' `spec_from_file_location` loader paths and hides the real diff behind
churn. (`watchdog.db` → `warden.db` *is* a rename, because DESIGN.md names it and
because the file moves anyway.)

**`make test` exists here, where hermes-agent has no test target.** Every suite is
run and its last line reported; a non-zero exit prints the full output. Zero tests
found is a failure, not a pass.

### Deliberately deferred within this slice

**`launchd/com.jkrumm.warden-poll.plist.template` is NOT written yet**, and this is
the one real piece of work hiding inside "promote the two cron jobs":

> Under the gateway cron runner with `no_agent: true`, `watchdog-poll.py --slack-body`
> printed the Slack message to **stdout** and *the gateway* delivered it to
> `slack:C0ASRULFTSS`. A LaunchAgent has no such consumer — stdout goes to a log
> file. So the poller must post to Slack **itself**, the way `triage.py` already
> does (plain `chat.postMessage` with a token from `resolve_slack_token()`), before
> its plist can exist. That is slice 0.6, and the template lands with the code
> change rather than ahead of it. `make render-plists` prints
> `✗ com.jkrumm.warden-poll [no template]` in the meantime — visibly absent, not
> silently broken.

**`dispatch-sweep.py` needs no such change for scheduling** — it posts each verdict
into its own origin thread directly and keeps stdout empty on purpose (that is what
`dispatch-sweep-cron.py`'s docstring means by *"anything on stdout would be a SECOND
message"*). Its template is written.

> **But its delivery is still gateway-coupled**, and this is worth stating plainly
> because it is easy to read the LaunchAgent promotion as more than it is:
> `dispatch-sweep.py:93` shells out to `~/.local/bin/hermes send`. After slice 0.6
> the sweeper's **scheduling** no longer depends on the gateway; its **delivery**
> still does. That is extraction risk #4 (§12), it is not in Wave 0's stop
> condition, and it is recorded here so it is not mistaken for finished.

### Needs a human (not blocking)

`scripts/warden-backup.sh` pings an UptimeKuma push monitor on a clean run, resolving
`op://hermes/uptime-kuma/warden-backup-push-url` (overridable via
`WARDEN_BACKUP_PUSH_REF`). **That monitor and that 1Password item do not exist yet** —
creating them needs a browser and a biometric `op`, DESIGN.md human-essential case 2.
Until then the script resolves nothing and skips the ping, exactly as
`hermes-backup.sh` does on a resolution failure: the backup still runs and still
exits with rsync's code. The heartbeat is additive, so nothing is blocked.

---

## 16. Slice 0.4a — the copy (DONE, `040e3eb`)

**Nothing running changed.** hermes-agent is untouched, warden's agents are
unloaded, the ledger is still `~/.hermes/watchdog.db`, and the config is still
`~/.hermes/config/triage-policy.json`. This slice only makes the code run in its
new home.

### Evidence

```
$ cd ~/SourceRoot/warden && make test
  test_dispatch_sweep.py           all cases as expected
  test_triage.py                   68/68 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_slack_blindness.py all cases as expected

$ cd ~/SourceRoot/hermes-agent && git status --short -- scripts/ tests/ docs/ config/
  (no output)
$ ~/.hermes/hermes-agent/venv/bin/python3 tests/test_triage.py
68/68 passed

$ for f in watchdog-poll.py dispatch-sweep.py watchdog-summary.py; do
    diff -q ~/SourceRoot/hermes-agent/scripts/$f ~/SourceRoot/warden/scripts/$f; done
  (no output — byte-identical)
```

`triage.py` differs in exactly three hunks, read line by line by the orchestrator,
not taken on the worker's word:

| Where | Change |
|-|-|
| `:389-397` | `_HERMES_OPS_BIN` was `Path(__file__).parent / "hermes-ops.sh"`. `hermes-ops.sh` stayed in hermes-agent, so it now takes `WARDEN_HERMES_OPS_BIN` else `HERMES_HOME/"scripts"/"hermes-ops.sh"` — the same env-then-default shape `HERMES_CC_BIN` already uses, and via `~/.hermes/scripts` (the symlink) like every other constant in the file. |
| `:702-742` | The `importlib` load of `agents-overview.py` deleted; its hand-mirrored fallback promoted to the sole `resolve_slack_token()`. Behaviour byte-identical: `SLACK_BOT_TOKEN` wins, else `secrets-run read op://hermes/slack/bot-token` with the Homebrew-prepended PATH and a 15s timeout, `""` on any failure. The `watchdog-poll.py` borrow below it is untouched — that sibling moved too. |
| `:3119-3124` | The `_policy_git_rel_path() is None` message now names both `POLICY_PATH` and `TRIAGE_REPO_DIR` and says "not written, not committed". |

Plus one test edit: `tests/test_triage.py:453` now reads `triage.POLICY_PATH`
instead of composing `REPO_ROOT / "config" / "triage-policy.json"`, because the
config did **not** move. `_triage_env()`'s `finally` restores `POLICY_PATH`, so
this is order-independent. **No test was deleted, skipped or weakened.**

### Correction to §12, extraction risk #6

STATE.md said the policy-commit guard *"returns `None` (skipping the commit
silently) rather than failing loudly."* **The silent part is wrong.** The call
site already printed a `triage: propose_mappings — …` line to stderr before this
change; the recon read the function and not its caller. What the edit did was
widen that existing message, not add a missing one.

The real consequence is different, and worse:

> When `_policy_git_rel_path()` returns `None`, `_write_policy_additions()` is
> **never reached** — the function returns first. So proposals are neither
> written nor committed, not "written but uncommitted". And in warden that branch
> is **permanently true**, because `TRIAGE_REPO_DIR` is now `~/SourceRoot/warden`
> while `POLICY_PATH` still resolves into `hermes-agent`. `propose_mappings()`'s
> self-extending signature map is therefore **inert until `triage-policy.json`
> moves**, which happens at the cutover. That is expected for this slice and is
> not a regression, but it is a live capability that is currently off, so it must
> be re-verified after the cutover rather than assumed to have survived it.

The control flow was deliberately **not** reordered to make the write happen
before the commit check — that would be a behaviour change smuggled into a move.

### Still not moved, deliberately

`config/triage-policy.json`, the ledger, `hermes-cc.sh`, `hermes-ops.sh`,
`agents-overview.py`, `validate-dispatch-policy.py`, `watchdog-slack.py`,
`dispatch-sweep-cron.py`, `plugins/dispatch-approval/`. Each has its own reason
in §12; none of them belongs in a slice whose whole property is that nothing
running changed.

---

## 17. Slice 0.1 — sideclaw repo policy (code complete, `sideclaw` working tree)

Not yet committed or deployed — a `/review` is running, and `make reload` refuses
while a job is in flight.

### What landed

| File | What |
|-|-|
| `server/lib/dispatch-policy.ts` (new) | `PINNED_RULES` (un-overridable), `DEFAULT_RULES` (mirrors `dispatch-repos.json`), `buildDispatchPolicy(env)`, `resolveDispatchTarget()`, `dispatchPolicy()`, `logDispatchPolicy()` |
| `server/routes/dispatch-policy.ts` (new) | `GET /api/dispatch-policy` — read-only projection |
| `tests/dispatch-policy.test.ts` (new) | 35 tests |
| `server/jobs/handlers/dispatch.ts` | gate after `parseParams`, before the fs checks; `effectiveSensitive` now feeds `assertSensitiveTierAllowed`, **both** `assertNoGithubForSensitive` sites and **both** `applySensitiveScan` sites |
| `server/routes/jobs.ts` | same check at submit — a refusal never creates a job row |
| `server/index.ts` | `logDispatchPolicy(logger)`, `.use(dispatchPolicyRoutes)` |
| `.env.example`, `CLAUDE.md` | the three env vars, the never-widen rule, and the corrected `sensitive` paragraph |
| `tests/setup.ts`, `tests/git-fixture.ts` | fixture repos moved under a seeded `SIDECLAW_DISPATCH_ROOTS` — **outside the original brief**, see below |

### The policy

```
roots    WORKSPACE_ROOTS = [~/SourceRoot, ~/IuRoot]   (env SIDECLAW_DISPATCH_ROOTS)
cwd      must be a DIRECT child of a root — not the root, not a nested subdir
default  { ceiling: implement, sensitive: false }

PINNED (env can never raise, remove or touch):
  sideclaw          investigate
  warden            investigate

DEFAULT (env may only narrow):
  dotfiles-private  investigate  sensitive
  homelab-private   investigate  sensitive
  dotfiles          investigate
  brain             investigate
  hermes-agent      investigate
```

`sensitive` is now **derived** from the policy and ORed with the caller's flag: a
caller may opt a policy-neutral repo *into* the scan, and can no longer opt a
policy-marked one *out* of it by omitting the field. That is the bypass §11 named.

### Validation (run by the orchestrator, not taken on the worker's word)

```
$ bun test
 523 pass
 0 fail
 1172 expect() calls
Ran 523 tests across 19 files. [15.43s]

$ bun run typecheck    # tsc --noEmit, no output
$ bun run lint         # Found 13 warnings and 0 errors  (all 13 pre-existing,
                       #  in excalidraw-hydrate.ts and session-runner.ts)
$ bun run format:check # All matched files use the correct format.
```

Every hunk of the diff was read directly. The worker also found and fixed two
`assertNoGithubForSensitive` call sites the brief did not name — correct, and
checked.

### One defect found in review and fixed by the orchestrator

`resolveDispatchTarget` canonicalized the **cwd** with `realpathSync` but only
`resolve()`d the **roots**. That asymmetry is a whole-surface outage waiting for a
symlinked root — and it had already bitten once, which is exactly why
`tests/setup.ts` has to `realpathSync` its temp root (macOS `$TMPDIR` resolves
through `/var → /private/var`). Every dispatch would be refused, fail-closed, with
a message pointing at the repo instead of at the config. Factored both sides
through one `canonical()` helper. Re-ran the four checks above; unchanged.

### The two out-of-brief test files — judged legitimate

`dispatch-policy.ts` builds its `POLICY` singleton at module load from the real
`process.env`, so fixture repos under `$TMPDIR` sat outside every root and were
refused before `runDispatch` reached the code two pre-existing
`dispatch-prompt.test.ts` cases were exercising. The fix seeds
`SIDECLAW_DISPATCH_ROOTS` in the preload and creates the fixture `repo` as a
direct child of it. **That is the fixture becoming more realistic, not a test
weakened to fit the code** — a real dispatch target has always been a repo under
a workspace root, and the fixture was only getting away with `$TMPDIR` because
nothing checked. `origin`/`worktrees`/`salvage` stay under the fixture's own root.
Flagged to the reviewer explicitly.

### Case sensitivity — checked, and it produced a finding for later

APFS is case-insensitive, `DEFAULT_RULES` keys are lowercase, and
`~/SourceRoot/Homelab-Private` **resolves to the same directory** as the denied
`homelab-private`. Whether that is a bypass depends entirely on whether the
runtime's `realpath` corrects case. Measured, both runtimes, same path:

```
python3  os.path.realpath  -> /Users/jkrumm/SourceRoot/Homelab-Private   (case NOT corrected)
bun      realpathSync      -> /Users/jkrumm/SourceRoot/homelab-private   (case corrected)
```

**sideclaw is safe**: it uses Bun's `realpathSync`, which returns the on-disk
name, so the mixed-case spelling lands on the `homelab-private` rule. A test for
this is owed and is added after the review lands.

> **But warden's defence-in-depth copy will be Python, and Python's
> `os.path.realpath` does not correct case.** A naive `basename()` check there is
> a live bypass of its own deny list. Whatever warden implements must compare
> case-insensitively, or resolve the on-disk name some other way. Recorded here
> because it is exactly the kind of thing that gets re-derived wrongly.

### Drift, and what closes it

There are now **two** copies of this policy — `dispatch-repos.json` in
hermes-agent and `DEFAULT_RULES` in sideclaw. DESIGN.md § Security model asks for
precisely that ("warden's copy is defence in depth; sideclaw's is the boundary"),
so the duplication is intended, not an accident. But it is the same drift shape
DESIGN.md warns about for the verdict schema, and drift here presents as *"the
boundary quietly allows something the control plane thinks it forbids."*

`GET /api/dispatch-policy` exists so the two can be compared. **Slice 0.7 owes an
agreement check** — fetch the projection, diff it against `dispatch-repos.json`,
fail `make status` on disagreement. Without it the second copy is a liability
rather than defence in depth.

---

## 18. Slice 0.1 — review pass 1, and what it changed

`/review` on the uncommitted sideclaw diff. Outcome **`needs-human`**, and partly
for a reason that has nothing to do with the code: **2 of 8 reviewers failed to
start** — `typescript` and `security`, both *"Session exited with code 1"*, neither
having examined the diff. A security change reviewed by everything except the
security angle is not a reviewed security change. A second pass with explicit
angles was run.

### The blocking finding was wrong, and the correct version of it was fixed anyway

The reviewer claimed — and said it had *"verified empirically"*, with the
cross-family adversary reviewer concurring — that
`basename(realpathSync(cwd))` **does not** normalize case on APFS, so
`~/SourceRoot/Dotfiles-Private` at `tier: implement` would miss the lowercase rule
key and fall through to the permissive `DEFAULT_RULE`, defeating the module at
both enforcement points and on the pinned entries too.

Measured directly, twice, on both an APFS user volume and `/private/tmp`:

```
disk RealName   query exact   -> RealName
disk RealName   query lower   -> RealName      <- on-disk spelling, not the caller's
disk RealName   query upper   -> RealName
disk lowername  query mixed   -> lowername
```

`realpathSync` returns the **on-disk** name. And every ruled repo is lowercase on
disk — checked one by one, `dotfiles-private`, `homelab-private`, `dotfiles`,
`brain`, `hermes-agent`, `sideclaw`, `warden`, all `MATCH`. **There was no live
bypass.** (An earlier note in §17 read this as "bun canonicalizes, Python does
not"; the sharper statement is that *both* return the on-disk name here — the
Python reading came from a path whose disk name already was lowercase, which
proves nothing either way.)

**The fragility underneath it is real, though, and is now fixed.** The table is
keyed lowercase while the lookup key comes from disk. Those agree today by
coincidence of naming, not by construction. If a repo's directory ever gains a
capital, it silently stops matching its own rule and falls through to
`DEFAULT_RULE` — `implement`, and `sensitive: false`, in a repo that is on the
list precisely because neither is safe. `lookupRule()` now lowercases both sides:
free on a case-insensitive volume, where two repos differing only in case cannot
coexist, and on a case-sensitive one it applies the stricter rule to both, which
is the direction to err.

### Two more fail-open paths, both real, both fixed

**Prototype-shaped lookups.** `policy.rules[repo] ?? DEFAULT_RULE` with `repo`
derived from an unauthenticated, HTTP-submitted `cwd`. Bracket access on
`constructor` / `toString` / `__proto__` returns an inherited `Object.prototype`
value rather than `undefined`, so `??` never fires, `rule.ceiling` reads
`undefined`, and `tierRank(undefined)` is 99 — making `tierRank(tier) > 99`
**false**, i.e. *admitted*, with `sensitive` falsy. Not reachable today only
because `DEFAULT_RULE` is already permissive, which is exactly why it would have
survived until `DEFAULT_RULE` was tightened. Now `Object.hasOwn`, and the two env
appliers use it instead of `in` (which walks the prototype chain) and normalize
before writing.

**`canonical()`'s catch.** I introduced this helper, and it degraded to
`resolve(p)` — the literal, unresolved string — when a path *existed* but
`realpathSync` threw (ELOOP, EACCES on an intermediate component, a TOCTOU race).
An unresolvable symlink whose raw text happens to sit under a root would be
admitted on the string while the kernel follows it elsewhere at execution time.
It now returns `null` and `resolveDispatchTarget` refuses; a root that will not
resolve is dropped rather than compared literally.

### Accepted, lower severity

- **`DispatchTier` was declared twice** — once as a union in the policy module,
  once as `z.infer` in `dispatch.ts`. `tierRank`'s fail-closed `?? 99` would have
  masked any drift by refusing the new tier everywhere instead of failing at
  compile time. Now declared once, in the policy module, with `DISPATCH_INPUT`'s
  zod enum built from that array and the type re-exported.
- **API contract.** `DISPATCH_INPUT.cwd`'s description and the MCP tool
  description both still described the old contract. Both now state the
  root/ceiling constraint, the `dispatch refused: …` shape, and point at
  `GET /api/dispatch-policy`.

### Declined, with reasons

- **Split `runDispatch`** (280 lines, cyclomatic 37, flagged CRITICAL by fallow).
  Real, and out of scope: refactoring the function this change is *gating* mixes a
  behaviour change with a structural one in a security-relevant path. Recorded as
  Wave 3+ work.
- **Drop the unused `DISPATCH_OUTPUT` export.** Unrelated to this change, and
  DESIGN.md wants the verdict schema published as a consumable artifact in slice
  0.2 — that export is likely to gain a consumer imminently. Leave it.

### Tests added for exactly the paths the review named

`tests/dispatch-policy.test.ts` grew from 35 to 46 cases: case-flipped `cwd`; a
repo directory carrying capitals against a lowercase key (the fragility above);
`constructor` / `toString` / `hasOwnProperty` as repo names; `__proto__` as an
override name; mixed-case override names against both a normal and a pinned repo;
both route-level refusals **asserting no job row is created**; the non-string-`cwd`
fallthrough; and a `GET /api/dispatch-policy` shape test.

One of those tests found a real problem in itself: the fallthrough case
legitimately *does* create a job row, the job store is one sqlite file shared
across the whole `bun test` run, and several suites assert on an empty store —
it broke `execute-drain-abandon.test.ts`. Confirmed against a stashed tree
(clean: 488 pass / 0 fail) rather than guessed at, and fixed with the same
`__resetForTests()` cleanup contract that suite already uses.

### Validation after the fixes

```
$ bun test
 534 pass
 0 fail
 1213 expect() calls
Ran 534 tests across 19 files. [16.65s]

$ bun run typecheck    # tsc --noEmit, no output
$ bun run lint         # Found 13 warnings and 0 errors   (the pre-existing baseline)
$ bun run format:check # All matched files use the correct format.
```

Two transient self-inflicted breakages were caught and fixed on the way: an
unescaped backtick that terminated the MCP tool's template literal, and a
`raw` shadow in `applySensitiveOverrides` that pushed lint from 13 to 14.

---

## 19. Slice 0.5 — the ledger (DONE, `de5d80d`)

**Nothing running changed.** The live ledger is byte-for-byte where it was, no
`-wal`/`-shm` appeared beside it, `~/.warden/` was never created, and warden's
agents are still unloaded.

### What it replaced

| Table | Was declared in |
|-|-|
| `dispatches` | **three** places — `hermes-cc.sh:757`, `dispatch-sweep.py:156`, `triage.py:595` |
| `events` | **two** — `watchdog-poll.py:246`, `triage.py:579` |
| `cursors` | two |

…plus `ALTER TABLE` blocks duplicated across pairs of files that each believed
they owned the migration, no `schema_version`, `user_version=0`, and
`journal_mode=delete`.

`scripts/ledger.py` (403 lines) now owns `BASE_SCHEMA` (5 tables + 6 indexes,
lifted from a read-only connection to the live database, not retyped),
`MIGRATIONS`, `schema_version`, `connect()`, `assert_schema_version()` and
`snapshot()`.

| Caller | Mode |
|-|-|
| `triage.py` (the loop) | `connect(migrate=True)` — **the only migrator**, per DESIGN.md |
| `watchdog-poll.py`, `dispatch-sweep.py` | `connect()` — assert-only; they used to invent their own tables if they ran first |

Pragmas on every writable connection: `journal_mode=WAL`, an **explicit**
`busy_timeout=5000` (true before only by accident of `sqlite3.connect`'s stdlib
default, and no non-Python writer of the file got it at all), and
`synchronous=NORMAL` — which under WAL still cannot corrupt the file and only
risks losing transactions this loop re-derives on its next 10-minute pass.

### Adoption is the normal case

A database carrying all five tables with no `schema_version` is the **live
ledger**, not a fresh one: it is a file four processes have been writing to for
months. It is stamped, never rebuilt — three reads and one small write, and
`BASE_SCHEMA` never runs against it.

Verified independently by the orchestrator against a read-only `VACUUM INTO` copy
of the live database:

```
schema_version before: False | after: True
  cursors              rows     9 -> 9     OK  cols OK
  dispatch_approvals   rows     5 -> 5     OK  cols OK
  dispatches           rows    22 -> 22    OK  cols OK
  events               rows   956 -> 956   OK  cols OK
  triage_items         rows    49 -> 49    OK  cols OK
indexes identical: True [idx_approvals_hash, idx_dispatches_created,
  idx_dispatches_open, idx_events_open, idx_events_resolved_at, idx_triage_state]
stamped version: 1
journal_mode: wal | busy_timeout: 5000 | synchronous: 1
```

### One trap that adoption sets for itself — closed

Deciding adoption on **table presence alone** means a database with all five
tables but missing a column that only ever arrived via a runtime `ALTER TABLE`
would be stamped version 1 and then fail as a bare `no such column` from inside
the loop, hours later, with nothing connecting it back to the stamp.

`_verify_columns()` now checks the stamp against the schema it claims, on both
migration paths. Expected columns are derived by running `BASE_SCHEMA` against an
in-memory database rather than kept as a second hand-written list — a copy of a
schema is a thing that drifts from the schema. **Missing** columns are fatal and
named; **extra** ones are not, because a newer warden's column should not be an
outage. Two tests pin both directions.

### Validation

```
$ make test
  test_dispatch_sweep.py           all cases as expected
  test_ledger.py                   11/11 passed
  test_triage.py                   68/68 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_slack_blindness.py all cases as expected

$ ls -la ~/.hermes/watchdog.db*
-rw-r--r--@ 1 jkrumm staff 1081344 Sep 9 13:34 /Users/jkrumm/.hermes/watchdog.db
   (no -wal, no -shm; mtime moves only because the LIVE hermes-triage agent is
    still running its own 10-minute pass against it, which is expected)
```

Three test fixtures changed, and they were checked for exactly the failure mode
that matters: `test_dispatch_sweep.py`, `test_watchdog_delivery.py` and
`test_watchdog_slack_blindness.py` each gained **one** line calling
`_ledger.connect(tmp, migrate=True)` before exercising the now-assert-only
`db_connect()`, standing in for the loop's boot migration. **No assertion was
changed, weakened or skipped** — the fixtures relied on `db_connect()`
self-creating tables, which is precisely the behaviour this slice removes.

Also repointed four stale `Source of truth: ~/SourceRoot/hermes-agent/scripts/…`
docstrings that have been wrong since slice 0.4a.

### Consequences to carry into the cutover

- **`watchdog-poll.py` and `dispatch-sweep.py` now fail loudly against an
  unmigrated ledger.** That is the design — an assert-only process racing the
  migrator on a fresh mini must not silently create its own tables. But it means
  at first boot after the cutover, if the poller (30 min) or the sweeper (5 min)
  fires before the loop (10 min) has migrated, it errors once and self-heals on
  the next pass. Expect it in the logs; it is not a regression. `RunAtLoad` on the
  loop's plist makes the window small.
- **`--dry-run` against the live `~/.hermes/watchdog.db`** likewise fails until
  the loop has migrated that file. Verified safe: `watchdog-poll.py --dry-run`
  does its `shutil.copy2` *before* the failure, so nothing is written to the live
  file.

### Where this leaves the stop condition's "one writer"

> **CORRECTED 2026-09-09 by the wave-boundary audit — see §24. The paragraph below
> was wrong about `one migrator`, which is worse than the gap it was busy conceding.**

Honestly: **not met, and it cannot be in Wave 0.** WAL, one migrator and
`schema_version` are done. But "one writer *process*" requires the intent queue
that DESIGN.md § Migration puts in **Wave 1**, and the approval plugin's direct
`UPDATE dispatch_approvals` — which DESIGN.md § The ledger also assigns to the
post-extraction step. Six writers remain: the loop, the poller, the sweeper,
`hermes-cc.sh` (twice), and the plugin. What Wave 0 delivers is **one writer
*module*** — a single place that owns the schema, the pragmas and the transaction
discipline — plus WAL, which removes the reader-blocks-writer failure that made
multiple writers acute. The gap is named rather than narrowed.

---

## 20. Slice 0.1 — review pass 2, and the live acceptance test

**Stop-condition item 3 — "sideclaw enforces the allowlist" — is DONE and verified
against the running daemon.** sideclaw commit `2d225d4`, deployed with `make reload`.

### Review pass 2 (security and typescript angles ran this time)

Outcome `needs-human` again, on **one blocking finding, and it was real**:

> `rules[repo] = {...}` on a plain object literal. A repo key of literally
> `__proto__` — and env values are lowercased first, so `__proto__` qualifies —
> does not create an own property. It runs the **inherited setter and reassigns
> the object's prototype**, so the entry silently vanishes while being reported
> `applied: true`. Contained today only because `lookupRule` reads through
> `Object.hasOwn`; it stops being contained the moment anything reads
> `rules[repo]` directly. Flagged independently by five reviewers plus the
> adversary.

This is the **write-side twin of the read-side guard added after pass 1** — the
same class of bug, on the other side of the same table, and pass 1 closed one half
of it. The table is now built with `Object.create(null)`.

Also applied from pass 2:

| Finding | Fix |
|-|-|
| The route hardcoded `"investigate"` as the tier default instead of reading the schema's — two "belt and suspenders" checks that could disagree about which tier a tier-omitting call gets are a gap, not redundancy | `DEFAULT_DISPATCH_TIER` exported once; the zod `.default()` and the route both read it |
| `SIDECLAW_DISPATCH_SENSITIVE` marked a repo sensitive without touching its ceiling, so an operator who forgot to also narrow it got a submission admitted by the ceiling-only route check, a job row and a queue slot spent, and the refusal only later inside `runDispatch` | marking sensitive now **clamps the ceiling to `investigate`** — the meaning it already has everywhere else in the estate |
| `.env.example` claimed "NEVER WIDENS" for all three vars, but `SIDECLAW_DISPATCH_ROOTS` **replaces** the roots rather than narrowing them | wording corrected in `.env.example` and `CLAUDE.md`; the two per-repo overrides narrow, ROOTS is called out as the one that needs thought |
| `canonical()`'s catch documented ELOOP/EACCES, but `existsSync` swallows those the same way, so in practice only a TOCTOU race reaches it | comment narrowed to say what is actually true |

One pre-existing test had to change: it asserted that adding `sensitive` left the
ceiling at `implement`. The clamp makes that **stricter**, so the expectation was
updated with the reason written into the test. Nothing was weakened.

Still declined, unchanged: splitting `runDispatch` (280 lines, CRAP 41.6 — real,
but restructuring the function this change *gates*, in the same pass, on a
security path, is how a refactor hides a behaviour change) and removing the unused
`DISPATCH_OUTPUT` export (slice 0.2 gives it a consumer).

### Final validation

```
$ bun test
 543 pass
 0 fail
 1244 expect() calls
Ran 543 tests across 19 files. [15.41s]

$ bun run typecheck    # tsc --noEmit, no output
$ bun run lint         # Found 13 warnings and 0 errors   (the pre-existing baseline)
$ bun run format:check # All matched files use the correct format.
```

Tests went 35 → 46 → **57** across the two review passes.

### The live acceptance test

`make reload`, then the same unauthenticated `POST /api/jobs` that was accepted
and executed before this change (§11). **Every refusal is a 400, by policy, at
submit — no job row created:**

```
cwd                                        tier         result
/Users/jkrumm/SourceRoot/homelab-private   implement    400  tier 'implement' exceeds the ceiling 'investigate' for repo 'homelab-private'
/Users/jkrumm/SourceRoot/dotfiles-private  author       400  tier 'author' exceeds the ceiling 'investigate' for repo 'dotfiles-private'
/Users/jkrumm/SourceRoot/sideclaw          implement    400  tier 'implement' exceeds the ceiling 'investigate' for repo 'sideclaw'
/Users/jkrumm/SourceRoot/warden            implement    400  tier 'implement' exceeds the ceiling 'investigate' for repo 'warden'
/tmp                                       investigate  400  cwd is not a repo directly under a dispatch root: /tmp
/Users/jkrumm/SourceRoot/Homelab-Private   implement    400  tier 'implement' exceeds the ceiling 'investigate' for repo 'homelab-private'
```

The last row is the case-flipped spelling landing on the lowercase rule.

**The negative control matters as much** — a boundary that refuses everything is
not a boundary. Admission was proven without starting a real episode, using paths
under a root that do not exist, so the policy passes and the handler's own
`existsSync` is what stops it:

```
/Users/jkrumm/SourceRoot/vps-probe-does-not-exist           implement    ADMITTED -> failed | Directory not found
/Users/jkrumm/SourceRoot/hermes-agent-probe-does-not-exist  investigate  ADMITTED -> failed | Directory not found
/Users/jkrumm/IuRoot/some-work-repo-probe                   implement    ADMITTED -> failed | Directory not found
```

and against the real repos, through the pure resolver rather than by dispatching:

```
/Users/jkrumm/SourceRoot/vps               implement    ALLOWED  sensitive=false
/Users/jkrumm/SourceRoot/hermes-agent      investigate  ALLOWED  sensitive=false
/Users/jkrumm/SourceRoot/brain             investigate  ALLOWED  sensitive=false
/Users/jkrumm/SourceRoot/homelab-private   investigate  ALLOWED  sensitive=true
```

`GET /api/dispatch-policy` renders the effective table, roots
`[~/SourceRoot, ~/IuRoot]`, `overrides: []`.

---

## 21. Slice 0.7 (first half) — the defence-in-depth copy, in hermes-agent

hermes-agent `22eb31c`. `config/dispatch-repos.json` now has
`tiers.investigate: ["dotfiles", "brain", "hermes-agent", "sideclaw", "warden"]`.

Both new names went into `tiers.investigate` and **not** into `deny`, for two
reasons: investigating them is legitimate and often the point — only the write
tiers are the problem — and a name appearing in both `deny` and `tiers` is a
**contradiction this resolver refuses to run on**, not a precedence question
(§6). Thirty lines of rationale went into the file's `_comment`, which is where
this repo keeps its reasoning.

```
$ ~/.hermes/hermes-agent/venv/bin/python3 scripts/validate-dispatch-policy.py config/dispatch-repos.json
28 repos dispatchable — 5 investigate, 0 author, 23 implement; 2 denied     (was 3 investigate)

$ ~/.hermes/hermes-agent/venv/bin/python3 tests/test_hermes_cc.py
all 163 cases as expected
```

`json.dump` reflowed three arrays the file deliberately kept on one line; the
formatting was restored so the diff is additive apart from the one intended line.
Committed by path — `cron/usage_audit.jsonl` in that tree is a live gateway
artifact, not part of this change.

### Still owed by 0.7

- **The agreement check.** Two copies of this policy now exist by design
  (DESIGN.md § Security model asks for exactly that), and nothing yet compares
  them. `GET /api/dispatch-policy` exists so they can be. Until something diffs
  the projection against `dispatch-repos.json` and fails loudly, the second copy
  is a liability rather than defence in depth — drift here presents as *"the
  boundary quietly allows what the control plane forbids."*
  **Careful, and this is the trap:** warden's side is Python, and
  `os.path.realpath` does **not** correct case the way Bun's `realpathSync` does.
  A naive `basename()` comparison there reintroduces the fail-open the sideclaw
  side just closed (§17, §20).
- `dotfiles/docs/architecture.md:171`, which describes `com.jkrumm.hermes-triage`
  and why it is a LaunchAgent — rewrite at the cutover, when it stops being true.
- `hermes-agent`'s `HERMES_PLISTS` and `docs/symlinks-and-agents.md` — same.

---

## 22. Slice 0.2 — the typed verdict (DONE & LIVE, sideclaw `360990c`)

Nine structurally different endings were concatenated onto one prose field, and a
tenth — the sensitive-withheld verdict — had no type at all. `DISPATCH_OUTPUT` now
carries a required `outcome` and a `schemaVersion`, and the schema is fetchable.

Live:

```
$ curl -s http://127.0.0.1:7705/api/dispatch-schema
ok: True | version: 1
outcomes: ['verdict_only','issue_declined','issue_failed','issue_filed','no_changes',
           'diff_refused','branch_no_pr','pr_failed','pr_opened','salvaged','withheld']
output schema: required = ['confidence','evidence','nextAction','outcome',
                           'recommendation','schemaVersion','summary','verdict']
worker tiers: ['author','implement','investigate']
```

Two precedence rules carry the weight, and both were read in the source rather
than taken from the report:

- **`withheld` beats everything.** `applySensitiveScan` spreads `...output` first
  and sets `outcome: "withheld"` last, on **both** return paths. The real verdict
  has been scanned out, so reporting `pr_opened` would be a lie about what reached
  the caller.
- **`salvaged` beats the tier outcomes**, because a salvaged run never reached them.

`WORKER_OUTPUT` is untouched — the worker is not asked to classify this, and would
be guessing where the handler observes. The prose strings are untouched too;
humans read those in Slack cards.

**Four of the eleven values are untested** — `verdict_only`, `issue_declined`,
`issue_filed`, `issue_failed` are set inline after a live worker session returns
and this repo has no seam for faking one. Recorded as a gap rather than covered by
a test that asserts nothing. 555 tests pass; typecheck, lint (13 pre-existing
warnings) and format clean.

---

## 23. Slice 0.6 — THE CUTOVER (DONE & LIVE)

warden commits `e811fed`, `b6ec3d6`; hermes-agent `7db53c7`; dotfiles `d6d6559`.

### Rollback, first, because it is the thing a later session will want

Everything not in git was copied to **`~/.warden-cutover-backup/`** before a single
change: `jobs.json.bak` (the cron registry — **not** in git, so `hermes cron delete`
is otherwise unrecoverable), `watchdog.db.pre-cutover`, and the old
`com.jkrumm.hermes-triage.plist`.

**`~/.hermes/watchdog.db` is still there, untouched, frozen at its pre-cutover
state.** To roll back: `make unload` in warden, restore the plist, `launchctl
bootstrap` it, and re-create the two cron jobs from `jobs.json.bak`.

### The sequence, and what was verified at each step

| # | Step | Evidence |
|-|-|-|
| 1 | Back up the un-gitted state | three files in `~/.warden-cutover-backup/` |
| 2 | `launchctl bootout com.jkrumm.hermes-triage` | no longer in `launchctl list` |
| 3 | `hermes cron delete 4b1faabda97d` / `4dd759917dd1` | *"Removed job: Watchdog"*, *"Removed job: Dispatch sweep"*; **5** jobs remain (7 − 2; an earlier draft said 4) |
| 4 | `VACUUM INTO ~/.warden/warden.db` off a **read-only** handle | 956/9/22/5/49 rows, every column set and all six indexes identical, `schema_version` 1, `journal_mode` wal |
| 5 | Repoint every reader/writer | all five warden scripts, `hermes-cc.sh`, the approval plugin, `briefing-context.py` |
| 6 | Exercise by hand before trusting a timer | loop `--dry-run` (18 open items, zero Slack), loop `--run` rc=0, poller `--post --dry-run` zero calls, sweeper rc=0 |
| 7 | `make agents` | four loaded, **all four exit status 0**, empty logs |
| 8 | Confirm the *agents* (not my manual runs) are writing | `triage_last_run` 19s old, `slack_alert_ts` advanced by the poller |
| 9 | Delete the originals + the two orphan wrappers | hermes-agent's **ten** suites all still pass |

### What moved, and the two things that were not just path edits

`config/triage-policy.json` came to warden, and that is **not** tidying:
`propose_mappings()` writes that file and then `git commit`s it inside
`TRIAGE_REPO_DIR`. While it lived outside this checkout that whole path returned
early — the signature map could not extend itself at all (§16). It can again.
`hermes-cc.sh` reads the same file for the merge/deploy half, via its own default
now pointing here. One file, two readers, as it always was.

`briefing-context.py` **stays in hermes-agent** and needs `watchdog-summary.py`,
which left. Its `_run_subscript()` is best-effort and **returns silently on a
missing file**, so deleting the script without repointing would have dropped the
Watchdog State block from the morning briefing with nothing said. Repointed to
warden's copy, env-overridable, with the silence documented at the call site.
Verified: `WATCHDOG_AVAILABLE=true` and real items.

### A regression caught by reading the code, not by the tests

The `--post` path returned **before** the UptimeKuma heartbeat when the digest was
empty. An empty digest is the **normal** case — quiet hours, or nothing new — so
the *"Watchdog last successful run"* monitor would have gone red on every silent
half hour. That monitor answers *"is the poller still running"*, not *"did it have
news"*, and an alert that fires on every quiet poll is an alert that gets ignored,
which is how eleven days of blindness went unnoticed the first time. **The test
covering it asserted the wrong thing** and was corrected with the reason written
beside it — my brief was ambiguous there, and the implementer read it reasonably.

### Two more, in the backup script, found by running it

1. macOS ships `/usr/bin/sqlite3` 3.51 **with no `-uri` flag**, so
   `sqlite3 "file:$DB?mode=ro" "VACUUM INTO …"` opened the whole URI as a literal
   filename and exited 14. The script degraded exactly as designed — shipped the
   live file, skipped the heartbeat — so **the backup looked like it worked while
   the consistent copy it exists to make was never taken.** Now goes through this
   repo's venv and `ledger.snapshot()`: one `VACUUM INTO` implementation, not two.
2. The rotation globbed unguarded, which under zsh is a hard error when nothing
   matches — i.e. on the first run, always. `(N)` fixes it.

**Backup verified end to end, not asserted:** two runs → two rotating snapshots →
both on `homelab:/mnt/hdd/backups/warden/` → one pulled back and opened:

```
   events 956 · cursors 9 · dispatches 22 · dispatch_approvals 5 · triage_items 50
   schema_version 1 · integrity_check ok
```

`triage_items` moved 49 → 50 between the migration and the restore. That is the
live loop working.

### Live state

```
$ make status
  venv                     Python 3.11.15
    ✓ com.jkrumm.warden-loop    ✓ com.jkrumm.warden-poll
    ✓ com.jkrumm.warden-sweep   ✓ com.jkrumm.warden-backup
  ledger                   968K
$ launchctl list | grep warden      # second column is last exit status
  -  0  com.jkrumm.warden-sweep     -  0  com.jkrumm.warden-backup
  -  0  com.jkrumm.warden-poll      -  0  com.jkrumm.warden-loop
```

### Known, and expected in the logs

An assert-only process that fires before the migrator on a **fresh** ledger errors
once and self-heals (§19). It did not happen here — the ledger was migrated
explicitly at step 4, before any agent was loaded — but it is the shape to expect
on a rebuild.

---

## 24. Wave-boundary audit — what it caught, including one thing this file got wrong

A **fresh** reviewer with none of this session's context audited all three repos
and the running system against `DESIGN.md` and `FLOWS.md`. Its verdict: **Wave 0
is not done**, on one item — and the important part was not that the item failed
but that **§19 asserted it was finished.**

### CRITICAL — `hermes-cc.sh` was still a second, unversioned migrator

Verified first-hand rather than taken from the report:

```
$ grep -n 'DB_PATH=' scripts/hermes-cc.sh
111:DB_PATH="${HERMES_CC_DB:-$HOME/.warden/warden.db}"     <- the NEW ledger
$ sed -n '829,834p' scripts/hermes-cc.sh
  conn = sqlite3.connect(os.environ['DB_PATH'])
  conn.executescript(os.environ['DB_SCHEMA'])              <- full CREATE block
$ grep -n 'ALTER TABLE' scripts/hermes-cc.sh | wc -l
7
$ grep -c 'schema_version' scripts/hermes-cc.sh
0
$ grep -c 'db_py ' scripts/hermes-cc.sh
11
```

Eleven call sites, every one re-running the whole DDL against the live ledger,
with no knowledge of `schema_version` and no `assert_schema_version()`. **This is
verbatim DESIGN.md C4** — *"two processes independently `ALTER TABLE` the same
tables with no version table"* — and slice 0.5 consolidated three of the four
copies while §19 claimed all of them.

It is benign only because the two schemas happen to agree. It stops being benign
the first time `SCHEMA_VERSION` becomes 2, and it silently voids the fail-loudly
property the poller and sweeper were given in the same slice.

**The concession §19 *did* make — that "one writer" is not met — the auditor
judged honest** (six writers named, the reason given, carried into the deferred
list). What was not honest was bundling "one migrator" into the done column in
the same paragraph. **Corrected in place above, and being fixed:**
`scripts/ledger.py` gained a CLI (`--migrate` / `--check` / `--version`,
warden `fe95e81`) so a shell script can prepare and assert a ledger without
carrying its own schema, and `hermes-cc.sh` is being converted from migrating to
asserting.

### MAJOR — a heartbeat semantics change nobody wrote down

`watchdog-poll.py`'s `--post` path returns 1 on a **failed Slack post**, before
the UptimeKuma ping. Under the old wrapper the gateway did delivery and rc was
unaffected by it, so `UPTIME_PUSH_WATCHDOG` answered *"did the poller run"*. It now
also answers *"did Slack accept the message"* — **a Slack outage will red the
"Watchdog last successful run" monitor even though ingest ran perfectly.**

Test 10 pins it deliberately, so it is a decision, not an accident. **Decision,
recorded now because it was not before: keep it.** A poll whose digest reached
nobody is not a successful poll — the operator learns nothing from it, which is
the same blindness by a different route. But the monitor's *name* now understates
what it covers, and if Slack outages turn out to be the common case this should
flip. The quiet-poll half is correct and confirmed: an empty digest still pings.

### The other findings, and their disposition

| Finding | Disposition |
|-|-|
| 75 KB of stale hermes-agent docs, incl. `docs/triage.md` byte-identical to warden's | fixed, `c661a89` — `docs/triage.md` and `cron/watchdog.md` deleted, `docs/watchdog.md` cut to the 9 lines still true there. `make status`: `✓ cron registry (5 jobs match)` |
| `ledger.py`'s scope note still said the cutover was "a separate, later step" — in the one file a stranger opens to learn which database is live | fixed |
| Three docstrings claiming `~/.hermes/scripts` symlinks to *this* directory | fixed — it symlinks to `hermes-agent/scripts` and always will |
| `warden/docs/triage.md` and `CLAUDE.md` still named `com.jkrumm.hermes-triage` and the hermes venv — copied verbatim in `040e3eb`, never adapted | fixed |
| `dotfiles/scripts/log-rotate.sh` said `hermes-triage` "runs today" | fixed, dotfiles `8ce1476` |
| §23 said "4 jobs remain" (5) and "nine suites" (10) | corrected in place |
| §15 called the sweeper's delivery "still gateway-coupled", contradicting `dispatch-sweep.py:99` | **§15 was wrong.** Verified in the CLI itself: `hermes_cli/send_cmd.py:258` — *"no agent loop, no running gateway required for bot-token"*. `hermes send` loads the gateway's **config** for credentials and posts through the platform adapter directly. It matters, because flow 5 is the case where the gateway is down |

### Confirmed under attack — recorded because these are the load-bearing claims

- **All nine of DESIGN.md § "What must not be lost" survive**, located line by line in the moved files. The auditor could not fault one. The dry-run contract is *better* tested than before (`--dry-run` beats `--post`, zero Slack **and** zero heartbeat). The `needs_human`-quiet-resolves bug DESIGN.md schedules for Wave 1 is correctly **still there** — not opportunistically fixed.
- **The old ledger is untouched, cryptographically:**
  `MD5(~/.hermes/watchdog.db) == MD5(~/.warden-cutover-backup/watchdog.db.pre-cutover)` = `f24b06d8ad5eae08bbc901df95bfb5b6`.
- **No split brain** — no writer in any of the three repos still resolves to the old path.
- **The boundary refuses under attack**: trailing slash, `.` segment, `../` traversal, case-flip, pinned-repo-via-traversal, a subdirectory, and the root itself — all 400 at submit, no job row. `__proto__`/`constructor`/`toString`/`hasOwnProperty` are correctly *admitted* to `DEFAULT_RULE` and then die on `Directory not found`.
- **No test deleted, skipped or weakened.** `test_triage.py` 2002 lines before and after, one line changed. The two changed assertions across the wave both moved in the **stricter** direction.
- **restic coverage discharged**: `homelab/docker-compose.yml:247` mounts the whole `/mnt/hdd/backups` read-only, and `restic-excludes.txt` excludes `*-shm`/`*-journal` but **not** `*.db`. DESIGN.md's *"verify the paths before assuming coverage"* is answered — the doc should be updated to say so.

### Two residual security gaps, by construction, worth DESIGN.md's attention

Neither is an implementation defect; both are properties of the design as built,
and DESIGN.md § Security model currently reads as if the pinned entries are
absolute.

1. **The policy keys on the directory basename, not on git identity.** A clone or
   worktree of `warden` under a different name, directly under a dispatch root,
   gets `DEFAULT_RULE` = `implement` — defeating a PINNED entry.
2. **`~/IuRoot` is a dispatch root with zero rules.** Every work repo under it is
   reachable at `implement`. That is pre-existing interactive capability, not
   something this wave introduced, and `check-dispatch-policy.py` prints it as a
   note rather than hiding it.

### Still open, declared

- `op://hermes/uptime-kuma/warden-backup-push-url` **does not exist**, so the
  backup runs unmonitored. Human-essential case 2 (§15).
- `com.jkrumm.warden-backup` has never fired via launchd — `StartCalendarInterval`
  03:10, and both existing snapshots were made by hand. First natural firing is
  the real test.

---

## 25. Wave 0 — final state, against the stop condition

| # | Stop condition | Verdict | Evidence |
|-|-|-|-|
| 1 | The repo runs its own loop on its own LaunchAgent | **DONE** | `com.jkrumm.warden-loop`, `StartInterval 600`. All four warden agents last-exit **0**. `triage_last_run` written by the agent, not by hand. `com.jkrumm.hermes-triage` gone from `launchctl list` and from `~/Library/LaunchAgents/` |
| 2 | The tests pass | **DONE** | warden `test_triage.py` **68/68** (the baseline), `test_ledger.py` **11/11**, three more suites green. hermes-agent **165** + **51** + eight more. sideclaw **555 pass / 0 fail**. **No test deleted, skipped or weakened** — audited independently; both changed assertions moved *stricter* |
| 3 | sideclaw enforces the allowlist | **DONE** | Live, both directions. Refuses traversal, `.` segments, trailing slashes, case-flips, pinned-repo-via-`../`, a subdirectory and the root itself — all 400 at submit, no job row. Still admits `vps@implement`, `hermes-agent@investigate` |
| 4 | Ledger: WAL + one writer + one migrator + a backup | **PARTIAL — one item, declared** | WAL ✓ · `schema_version` ✓ · **one migrator ✓ (as of `15e50a9`)** · backup ✓ end to end · **one writer ✗** |
| 5 | Two cron jobs → LaunchAgents, orphan wrappers deleted | **DONE** | `jobs.json` 7 → 5, missing exactly `4b1faabda97d` and `4dd759917dd1`. `watchdog-slack.py` and `dispatch-sweep-cron.py` deleted, heartbeat ported first |

### One migrator — closed, and this is what closed it

hermes-cc.sh no longer creates or alters anything. It asserts, and refuses loudly:

```
$ grep -n 'ALTER TABLE\|executescript\|CREATE TABLE' scripts/hermes-cc.sh
767:# the tables join without conversion. The schema itself (CREATE TABLE, CREATE   <- prose
```

Every remaining hit across all three repos is a comment describing what used to be
there. The only executable DDL is in `warden/scripts/ledger.py`.

Proven to refuse, not just to pass:

```
unmigrated db (no schema_version)  -> exit 2
schema_version = 99                -> exit 2
the live ledger                    -> exit 0     ("no dispatches", budget line)
--json on refusal                  -> {"ok": false, "exitCode": 2, ...}   shape preserved
```

The message names the path, both versions, and who is allowed to migrate —
deliberately the same shape as `ledger.py`'s own assertion, because they are the
same system.

### One writer — NOT met, and it cannot be in this wave

Six writers remain on `warden.db`: the loop, the poller, the sweeper,
`hermes-cc.sh` (two separate connections), and
`plugins/dispatch-approval/__init__.py:259`.

Consolidating them needs the **intent queue** and the **approval-plugin repoint**,
and DESIGN.md § Migration puts both in **Wave 1** — the plugin's direct write is
named in § The ledger as a post-extraction step, and the intent/signature split is
Wave 1's first line. Doing it here would mean building Wave 1 to close a Wave 0
checkbox.

What Wave 0 delivers instead is **one writer *module*** — a single place owning
the schema, the pragmas and the transaction discipline — plus WAL, which removes
the reader-blocks-writer failure that made six writers acute rather than merely
untidy. **The gap is named, not narrowed.**

### Where the rollback is

`~/.warden-cutover-backup/` — `jobs.json.bak` (the cron registry is **not** in git,
so `hermes cron delete` is otherwise unrecoverable), `watchdog.db.pre-cutover`, and
`com.jkrumm.hermes-triage.plist`. `~/.hermes/watchdog.db` is still on disk and
byte-identical to that backup by MD5.

To roll back: `make unload` here, restore the plist, `launchctl bootstrap` it,
re-create the two cron jobs from `jobs.json.bak`, and set `HERMES_CC_DB` back.

### The first things a Wave 1 session should know

1. ~~backup unmonitored~~ **RESOLVED, and it never needed a human.**
   `homelab/uptime-kuma/sync.py:129` says the quiet part out loud — *"The push
   token is retrievable too … so create-and-wire needs no browser."* So:
   `Warden Backup - Push` added to homelab's declarative monitor config
   (`99e16c3`), created by `make uk-sync`, token read back through the API, and
   **verified end to end — a real backup run produced `status: UP, msg: OK` on
   monitor 234.** The URL could not go into 1Password from the mini (`op` is not
   interactively signed in; seeding is biometric), so it lives in a mode-600 file
   at `~/.config/uptime-kuma/warden-backup-push-url` — the same shape homelab
   already uses for `garmin-relogin-push-url`. `op://` stays **first** in the
   lookup, so this converges on the convention the moment that ref exists and the
   file can be deleted then.
2. **`com.jkrumm.warden-backup` has never fired via launchd.** Every snapshot so
   far was made by hand; `StartCalendarInterval` 03:10 is untested in anger. The
   monitor is now what will say so — a missed 03:10 reds it within 25h.
3. **Two design-level gaps** the audit surfaced, neither an implementation defect,
   both worth a line in DESIGN.md § Security model — which currently reads as if
   the pinned entries are absolute: the policy keys on **directory basename**, so
   a clone of `warden` under another name defeats its PINNED entry; and
   **`~/IuRoot` is a dispatch root with zero rules.**
4. **The poller's heartbeat now also fails on a failed Slack post** (§24). Kept
   deliberately; the monitor's name understates its scope.
5. **`propose_mappings()` was inert** for the whole extraction and is live again
   now the policy file is inside this repo (§16). **Re-verify it rather than
   assuming it survived** — it has not run successfully since before the move.

**Do not start Wave 1 without reading §24 and this section.**

---

## 26. Wave 1 — reconnaissance against the running system (2026-09-09, ~13:15Z)

Run before any edit, against the live system rather than this file. Four of the
five checks confirm what §25 says. The fifth found a **live crash** §25 does not
mention, because it happened after §25 was written.

### What was run

```
$ make status
warden
  venv                     Python 3.11.15
  agents:
    ✓ com.jkrumm.warden-loop  [-	1	com.jkrumm.warden-loop]
    ✓ com.jkrumm.warden-poll  [-	0	com.jkrumm.warden-poll]
    ✓ com.jkrumm.warden-sweep  [-	0	com.jkrumm.warden-sweep]
    ✓ com.jkrumm.warden-backup  [-	0	com.jkrumm.warden-backup]
  policy                   ✓ both copies agree on all 30 repos
  ledger                   980K Sep 9 15:05

$ make test
  test_dispatch_sweep.py           all cases as expected
  test_ledger.py                   11/11 passed
  test_triage.py                   68/68 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_slack_blindness.py all cases as expected

$ make check-policy
✓ both copies agree on all 30 repos
notes: sideclaw also admits roots hermes-cc.sh never uses: ['/Users/jkrumm/IuRoot']
```

`test_triage.py` is **68/68**, the number §25 pins. Policy copies agree on all 30.

### FINDING 1 (live, new) — the act-loop is crashing on `database is locked`

`make status` prints `✓` for `com.jkrumm.warden-loop`, but the middle column of
launchd's own row is the **last exit status**, and it is **1**:

```
$ launchctl list | grep -i warden
-	0	com.jkrumm.warden-sweep
-	0	com.jkrumm.warden-backup
-	0	com.jkrumm.warden-poll
-	1	com.jkrumm.warden-loop
```

`~/Library/Logs/warden-loop.err`, in full:

```
Traceback (most recent call last):
  File ".../scripts/triage.py", line 3351, in <module>
    sys.exit(main())
  File ".../scripts/triage.py", line 3345, in main
    return run(conn, dry_run="--dry-run" in argv)
  File ".../scripts/triage.py", line 3160, in run
    ingest(conn, now)
  File ".../scripts/triage.py", line 959, in ingest
    conn.execute(
sqlite3.OperationalError: database is locked
```

Line 959 is `ingest()`'s `INSERT INTO triage_items` — the first DML of the pass,
i.e. the statement that opens the deferred write transaction and therefore the
one that has to take the write lock. The ledger says the same story from the
other side: `cursors.triage_last_run` is stamped **12:55:38Z** (the last pass
that completed), while the poller-offset cursors carry **13:05:36Z** — the
crashed pass got far enough to commit those and then died taking the lock.

`ledger.py:429` sets `PRAGMA busy_timeout=5000` on every writable handle, so this
means **a writer held the ledger's write lock for more than five seconds**.
`dispatch-sweep.py` is not it — its network I/O (`poll_job()`) sits outside the
write transaction and every branch commits immediately. The remaining candidates
are the other five writers. This is not a mystery to be solved before item 2 so
much as **the argument for item 2**: six writers against one SQLite file, and the
loop is the one that dies.

Two consequences, both worth stating:

- **Nothing noticed.** `make status` renders the agent `✓` while its last exit
  was 1. A loop whose whole job is noticing that something stopped, stopped, and
  its own status surface said fine. That is the same class as the Wave 0
  heartbeat that skipped the normal case.
- **The failure is unhandled, not retried.** A `SQLITE_BUSY` on one INSERT kills
  the entire pass; the 600s tick is the only retry there is.

Disposition: **fold into Wave 1 item 2** (intent queue / one writer), which is
the structural fix, plus an immediate mitigation in the same slice — a longer
`busy_timeout` and a bounded retry around the pass, and `make status` reading the
exit-status column instead of only presence. Recorded here rather than hot-fixed
first so the fix lands with the tests that prove it.

### FINDING 2 (confirms §24) — `hermes-cc.sh` asserts, it does not migrate

```
$ grep -c 'ALTER TABLE\|executescript' ~/.hermes/scripts/hermes-cc.sh
0
```

`WARDEN_SCHEMA_VERSION="${WARDEN_SCHEMA_VERSION:-1}"` at line 116; the assert
block at 794-815 exits `EX_PRECONDITION` on mismatch and names the loop as the
only migrator in its own error text. The audit's critical finding is closed.

### FINDING 3 (confirms) — the ledger

```
journal_mode      wal
schema_version    [(1, '2026-09-09T11:57:27.019920+00:00')]
tables            cursors, dispatch_approvals, dispatches, events,
                  schema_version, sqlite_sequence, triage_items
rows              triage_items 50 | dispatches 23 | dispatch_approvals 5
                  events 959 | cursors 9
states            resolved 28 | note 8 | ignored 7 | needs_human 4 | new 3
```

WAL confirmed, `schema_version` 1 confirmed. **There are four items sitting in
`needs_human` right now** — which is exactly the population Wave 1 item 1 (the
quiet rule) is about, so that bug is not hypothetical on this ledger today.

### FINDING 4 (was NOT provable from the ledger) — `propose_mappings()`

The ledger cannot answer this: `cursors.triage_propose_mappings_last_run` reads
**2026-09-08T22:33:07Z** — stamped *before* the move, and the 24h budget means it
has not been *eligible* since. So it has neither succeeded nor failed; it has not
run. §25 item 5 is right that it must be re-verified rather than assumed.

Verified instead by exercising the real module's real globals read-only, with no
monkeypatching and no write:

```
TRIAGE_REPO_DIR :  /Users/jkrumm/SourceRoot/warden
POLICY_PATH     :  /Users/jkrumm/SourceRoot/warden/config/triage-policy.json  exists: True
_policy_git_rel_path()    : config/triage-policy.json      <- was None before the move
_policy_path_is_dirty()   : False
_discoverable_repos()     : 28 repos
live candidates           : 17
base_url resolves: True | api_key resolves: True
```

`_policy_git_rel_path()` returning `config/triage-policy.json` instead of `None`
**is** the inertness being gone — that `None` was the early return that silently
discarded every proposal for the whole extraction. Every other precondition the
function checks before its model call now also passes against live state, and
there are 17 real candidates waiting.

What is still unproven, and deliberately: the model call and the
`git commit` itself, because proving those means an LLM call and a real commit to
this repo. The write-and-commit half is covered by `test_triage.py`'s temp-repo
fixture (`test_propose_mappings_policy_round_trip_preserves_readme_and_key_order`
and five siblings); what those tests monkeypatch away is precisely the path
resolution just verified above against the live globals. **Next eligible run is
2026-09-09T22:33Z** — it will fire on its own, unattended, and the result will
show up as an auto-authored `config/triage-policy.json` commit plus a line in the
daily digest. Check for it.

### Not re-litigated

Nothing in this section contradicts `REVIEW.md`. Finding 1 is new evidence about
a running process, not an argument against a settled disposition.

### Next action

Wave 1 item 1 — the quiet rule (`_GROUPED_RESOLVE_EXCLUDED_STATES` in
`scripts/triage.py`). Test first.

---

## 27. Wave 1, item 1 — the quiet rule (DONE)

`DESIGN.md` § "The quiet rule, corrected" and principle 5, implemented.
**Silence-resolve now applies to `new` and to nothing else.**

### It was three paths, not one

The prompt named `_GROUPED_RESOLVE_EXCLUDED_STATES`. That is one of three
instances, and it is not the worst one. Stated rather than narrowed:

| Path | Fired on | Old behaviour |
|-|-|-|
| `resolve_quiet_grouped()` | `quietResolveHours` of no new occurrence | resolved everything except `resolved/ignored/snoozed/note/investigating` |
| `resolve_recovery_paired()` | a `✅` recovery message in #alerts | same exclusion list |
| **`apply_resolutions()`** | `events.resolved_at` set | **worse** — same, minus even the `investigating` exclusion, **and set `note=NULL`** |

`apply_resolutions()` is the one that erases the written fix. The question of
whether it counts as a *silence* path was settled by reading the only thing that
sets `events.resolved_at`, and it is `watchdog-poll.py` in exactly two places:
the ingest sweep (`if row["external_id"] not in obs_ids`, line ~937) and
`sweep_stale_grouped()`'s 7-idle-day housekeeping (line ~1084). Never a human
decision, never the episode. So it is disappearance-from-observation, and
principle 5 covers it.

### The change

`_GROUPED_RESOLVE_EXCLUDED_STATES` → `_SILENCE_RESOLVE_ELIGIBLE_STATES = (STATE_NEW,)`,
used by all four queries across the three functions.

**An inclusion list of one, deliberately, and this is the load-bearing part.**
Wave 2 adds `implementing`, `validating`, `deploying`, `verifying` to the chain.
An exclusion list silently ADMITS every state added after it was written — which
is exactly how `needs_human` came to be discardable. An inclusion list silently
EXCLUDES them. It fails closed.

### Evidence

```
$ make test
  test_dispatch_sweep.py           all cases as expected
  test_ledger.py                   11/11 passed
  test_triage.py                   73/73 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_slack_blindness.py all cases as expected
```

**68 → 73 (+5).** All five are new. Two renames and four re-seedings are
count-neutral. Nothing deleted, skipped, or weakened. The new five:
`test_recovery_paired_never_discharges_needs_human`,
`test_quiet_timer_never_discharges_needs_human`,
`test_event_resolution_never_discharges_needs_human`,
`test_silence_resolve_eligible_states_is_new_only`,
`test_no_chain_state_is_silence_resolvable`.

**Mutation check, run here rather than taken from the worker's report** — widen
the tuple to `(new, needs_human, verdict, pr_open)` and the suite drops to
**68/73**, with the general test naming the leak:

```
test_no_chain_state_is_silence_resolvable: silence-resolved a state carrying an
obligation: {'verdict': 'resolved', 'needs_human': 'resolved', 'pr_open': 'resolved'}
```

**Old code vs new code, on snapshots of the LIVE ledger.** The four real
`needs_human` items were each given a written-fix note and their monitor made to
recover (`events.resolved_at` stamped) — the exact DESIGN.md scenario, on real
rows:

```
items where OLD and NEW disagree: 4
  event 543:  OLD state='resolved'    note=None
              NEW state='needs_human' note='blocked on a human: the fix is two lines'
  event 875:  (identical)
  event 930:  (identical)
  event 943:  (identical)
```

The old loop discarded all four human decisions and erased all four fixes. The
new one keeps them.

**A first attempt at this comparison returned "0 disagreements" and was wrong** —
the old `triage.py`, copied alone into a temp dir, crashed on its `ledger.py`
import and never ran. Recorded because the failure mode is a silent pass: a
comparison harness that reports "no difference" when one side did not execute
looks exactly like a correct result. The redone version copies the whole
`scripts/` directory so the imports resolve.

Three of the four live `needs_human` items were sitting at **2.02h quiet against
a 2h `quietResolveHours`** when this was written. They are `uk`-source, so
`resolve_quiet_grouped()` (grouped sources only) never reached them — it was
`apply_resolutions()` that was armed, and `uk` monitors flap green routinely.

### What this deliberately leaves broken until item 3

A non-`new` item now has **no automatic exit at all** — no deadline, no poller.
That is DESIGN.md's own sequencing ("exits only through its own transition or its
deadline") and the correct order: stop discarding first, then bound. Four items
are affected today. Item 3 closes it.

### Also in this slice

`make status` printed `✓` over a last exit status of 1 (§26, finding 1). Fixed in
`91defbe`, separately. All three branches — ✓, ✗-nonzero, ✗-not-loaded — were
exercised against a stubbed `launchctl`, because the bug was that the failing
branch had never run.

### Files

`scripts/triage.py`, `tests/test_triage.py`, `docs/triage.md` (five sentences that
the change made false; no broader rewrite), `Makefile` (in `91defbe`).

### Noticed, not fixed

`docs/triage.md`'s Tests section still counts "42/54/64 cases" against an actual
73. Stale before this change.

---

## 28. Decision: the ledger stays SQLite on the mini. Postgres on the VPS was offered and declined.

Raised 2026-09-09 after §26's `database is locked` crash: move the ledger to the
VPS's Postgres for queues, row locks and richer transactions. **Declined.**
Recorded here so it stays settled.

- **The crash was six writers, not the engine.** `busy_timeout` is 5s and someone
  held the write lock longer. Wave 1 item 2 removes the writers; that is the
  stop condition. Postgres would make the six-writer shape *survivable* instead
  of fixing it, preserving exactly what DESIGN.md wants gone.
- **Volume does not motivate it.** 980K, 959 events over 38 days, three agents on
  600s/1800s/300s. SQLite in WAL is already over-provisioned for this.
- **It inverts warden's founding property, and "the mini being down takes
  warden down anyway" is the argument FOR co-locating, not against.** One failure
  domain is the goal. A ledger on the VPS does not remove the mini from the
  critical path — the loop still runs there — it ADDS a second way to be down
  (VPS down, or tailnet down, with the mini perfectly healthy). Two failure
  domains where there was one. This repo is separate from
  `hermes-agent` because the loop that notices Hermes is broken must not run
  inside Hermes. DESIGN.md's system table pins warden's failure domain to "mini,
  own LaunchAgents" and Argo's to "**VPS — separate domain**"; FLOWS.md says it
  outright — *"the decision surface must not be the thing that is down."* A
  ledger over the tailnet means a VPS or network outage stops the control plane
  from recording that there is an outage. That is the 2026-09-07 gateway-crash
  failure with a longer wire.
- ~~**Postgres is already in this architecture, deliberately demoted.**~~
  **WITHDRAWN, 2026-09-09, and correctly.** This cited DESIGN.md line 365 — Argo
  caching `GET` responses in Postgres "purely so the page renders when the mini
  is unreachable" — as evidence the split was considered. The objection: *if the
  mini is down, warden is down, so the board renders a frozen snapshot of work
  that is not progressing anyway.* That is right. Uptime Kuma already reports the
  mini being down, faster and louder; the only residual value is forensic, and
  the ledger answers that when it comes back. **DESIGN.md's stated justification
  for the Argo cache does not survive the objection** — see §29. The decision
  below does not rest on this bullet.
- **It puts a client library in the process that decides whether to touch
  production.** The loop is pure stdlib plus `cryptography`, on purpose.
- **The advanced patterns buy nothing at this shape.** SQLite has transactions.
  Row locks matter with N competing workers; warden has one drainer by design, so
  the intent queue is a spool plus a single drain and needs no locking.
- **REVIEW.md's one SQLite-named finding is engine-independent.** *"SQLite cannot
  atomically change a row and merge a PR"* — neither can Postgres, which is why
  its disposition is operation-id + remote receipts + an explicit `unknown`.

**Revisit only if** warden needs genuinely concurrent workers, or a second host
starts writing. Neither is on the Wave 1–3 path.

### Next action

Wave 1 item 2 — the intent/signature split, which is also what closes "one
writer". Design already settled from reading (record it before building):
spool at `~/.warden/intents/`, a door on its own module (see §30 — it became
`scripts/intents.py`, not `ledger.py`, so the file whose own docstring calls
`_run_migrations()` "the single most dangerous function in this repo" does not
also grow business logic), the plugin signs then spools then
drains synchronously — because `execute_approved()` re-runs `hermes-cc.sh
--confirm` in a subprocess IMMEDIATELY after the click, so a 600s loop drain
would break the flow outright. `require_signed_approval()` is not touched.
Fold in the `busy_timeout` raise and a bounded retry around the pass (§26).

---

## 29. Design finding — the Argo Postgres cache is justified by an argument that does not hold

Not an implementation defect and **not for this wave** (Argo is Wave 3). Recorded
so it is decided deliberately rather than inherited.

`DESIGN.md` line 365 justifies Argo caching `GET` responses in Postgres "purely
so the page renders when the mini is unreachable, stamped with fetch time."

**If the mini is unreachable, warden is not running.** The loop, the pollers and
the sweeper are all LaunchAgents on the mini. So the page renders a frozen
snapshot of work that has stopped progressing — it cannot be acted on, and
nothing it shows will change until the mini returns. Uptime Kuma already reports
the mini being down, sooner and more loudly than a stale board does. The residual
value is forensic ("what was in flight when it died"), and the ledger answers
that directly once the mini is back.

Options for Wave 3, none of them decided here:

1. **Drop the cache.** Argo fetches live and shows a plain "mini unreachable"
   state. Least machinery, and honest about what it knows.
2. **Keep it, justified differently** — e.g. page-load latency across the tailnet,
   or surviving a warden restart rather than a mini outage. If it is kept, the
   justification in DESIGN.md must be replaced, because the current one is the
   one that fails.

What must NOT happen is the cache quietly becoming a mirror. That is the same
line §28 declines for a different reason, and it is still the line.

---

## 30. Wave 1, item 2a — the intent queue (DONE, nothing repointed yet)

`scripts/intents.py` + `tests/test_intents.py`. New files only; no existing
script changed, no schema change, no LaunchAgent touched. **This slice builds the
queue. It does not yet move any writer onto it** — that is 2b.

### The shape, and the two decisions inside it

A spool directory plus a single drain. `record()` writes one JSON file and
**opens no database at all**, which is the entire point: a process that records
an intent is not a ledger writer. `drain(conn)` is the only thing that turns a
file into a row, and it does not open the database either — the caller passes a
connection, so a drain can never become a seventh way to open the ledger with its
own pragmas.

**Decision 1: `scripts/intents.py`, not `ledger.py --record-intent`.** §28's next
action said the latter. Changed on writing the brief: `ledger.py` owns the
schema, the connection and the migrator, and its own docstring calls
`_run_migrations()` "the single most dangerous function in this repo". Growing
business logic into that file is how it stops being reviewable. `intents.py`
imports it and owns the spool.

**Decision 2: no signature verification in the drain, deliberately.** This was
the one real design question in the slice. `require_signed_approval()`
(`hermes-cc.sh:1113`) already verifies, and a second verifier is a copied
contract — the exact defect this repo refuses for sideclaw's verdict schema. So
the drain validates SHAPE and writes the row; the signature is checked where it
always was, at spend time.

The threat model that follows, written into the module docstring so it is not
re-derived later: **a forged spool file cannot mint a signature, so it cannot
cause an approval.** The worst a writer of that directory achieves is
`decision='deny'` on a pending approval — a denial of service, not an
escalation, and an episode on this host can already do worse. What makes the
forged-`deny` bounded rather than free is the `AND decision IS NULL` guard.

**`expires_at` and `payload_hash` are refused outright from a spool file** and
read from the existing row instead. They are what bind an approval to a deadline
and to specific bytes; a file that could set them could widen its own approval.
That refusal is the security-relevant line in the module and has its own named
error and its own test.

### Evidence

```
$ make test
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   11/11 passed
  test_triage.py                   73/73 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_slack_blindness.py all cases as expected

$ grep -n 'sqlite3.connect' scripts/intents.py
(none — every connection comes from ledger.connect)
```

**17 tests came from the brief; I added two**, both for guardrails that were
asserted in comments and never actually run — the class of defect Wave 0 kept
shipping:

- `test_cli_drain_refuses_a_ledger_at_the_wrong_schema_version` — `--drain`
  passes `migrate=False`, so it asserts. Stamping a throwaway ledger one version
  forward makes it exit 1 naming `schema_version`. The refusal branch never runs
  in normal operation, which is precisely why it needed running.
- `test_a_half_written_file_is_invisible_to_the_drain` — `record()` publishes by
  renaming a `.tmp` written in the same directory; the drain's `*.json` glob is
  what makes a crash mid-write invisible. Worth noting: **pathlib's `glob("*")`
  does match dotfiles** (unlike a shell glob), so the protection is the `.json`
  suffix, not the leading dot.

**Mutation checks, run here rather than taken from the worker's report:**

| Mutation | Result |
|-|-|
| drop `AND decision IS NULL` | 17/19 — a replayed `deny` overwrites a human's `approve`; ordering becomes last-write-wins |
| widen the glob to `*` | 18/19 — the half-written `.tmp` gets drained into `rejected/` |

**End-to-end through the CLI, against a VACUUM INTO snapshot of the live
ledger** (not a synthetic schema), with a pending approval shaped as
`record_approval_request()` writes one:

```
$ ... | intents.py --record        -> .../intents/20260909T134137357947-ea24e6f8.json   rc=0
                                      -rw------- , directory drwx------
$ intents.py --drain <snapshot>    -> applied=1 rejected=0   rc=0
row: decision='approve' decided_by='U_JKRUMM' decided_at=<now>
     payload_hash='deadbeefcafe' expires_at=<unchanged> spent_at=None
spool after drain: empty
```

`payload_hash` and `expires_at` came from the row and are untouched; `spent_at`
stays NULL, so the approval is still good for exactly one spend and
`require_signed_approval()` is still the thing that spends it.

### Behaviours a later reader will be tempted to "fix"

- **`rowcount == 0` on the UPDATE is SUCCESS.** Unknown nonce, or already
  decided; both are ordinary idempotency. Turning either into a rejection fills
  `rejected/` with files whose only defect is that the world moved on.
- **Unlink happens AFTER the commit.** A crash in between means the file drains
  again next pass, and the guard makes that a no-op. The safe order is "apply
  twice", never "lose one".
- **`--drain` exits 1 if any file was rejected**, even though the drain itself
  succeeded — because 2b has the plugin draining synchronously right after a
  click, and "your intent did not land" must be in the exit status.

### Next action

**2b — repoint the approval plugin.** `hermes-agent/plugins/dispatch-approval/__init__.py:229`
`_record_decision()` stops opening sqlite: sign (unchanged, RAM key), then
`intents.py --record`, then `intents.py --drain`, then read the row back
**read-only**. The drain must be synchronous, not left to the loop: the plugin's
`execute_approved()` re-runs `hermes-cc.sh --confirm` in a subprocess
IMMEDIATELY after the click, so a 600s drain would break the flow outright. That
is the constraint that decided the whole shape.

Then **2c** — `busy_timeout` off 5s and a bounded retry around the pass (§26
finding 1).

---

## 31. Wave 1, item 2c — the `database is locked` crash, root cause (DONE)

§26 finding 1 was going to be "raise `busy_timeout` and add a retry". It is not
that. Looking for *which* writer held the lock for more than five seconds found
the answer, and it is a much better fix.

### The cause

`scripts/watchdog-poll.py`'s `_run_poll()` had **no commit of its own**.
`main()` committed once, after the whole poll returned — the only two
`conn.commit()` calls in the entire file were both in `main()`.

Python's sqlite3 opens a DEFERRED write transaction on the first INSERT or
UPDATE and holds it until commit. `_run_poll()` interleaves probes and writes:

```
poll_uk()          HTTP        reconcile()   <- first write: transaction OPENS
poll_docker()      ssh x2      reconcile()
poll_op_refs()     ssh xN      reconcile()
poll_github()      gh subprocess, timeout=30
poll_hermes_cron() ...
poll_slack_messages() HTTP x2
                                             <- main() finally commits
```

So from `reconcile()`'s first write until `main()`'s commit, **the ingest poller
held the ledger's write lock across every remaining probe** — two `ssh` round
trips, one per op-refs host, a `gh` call with a 30s timeout, and two Slack
fetches. Minutes, on a bad day. `ledger.py` sets `busy_timeout=5000`. The
act-loop, ticking every 600s into a lock held that long, had no chance:

```
sqlite3.OperationalError: database is locked
  scripts/triage.py:959 in ingest
```

The loop whose entire job is noticing that things have stopped was stopped by
the ingest poller.

### The fix

Commit after each source, so the network I/O happens outside any transaction.
Seven commit points, one invariant, stated in `_run_poll()`'s docstring: **no
network call may happen inside an open write transaction.**

The trade, stated because it is one: a crash mid-poll now leaves earlier sources
committed rather than rolling the whole pass back. That is the right way round —
every `reconcile()` re-derives its rows from `observed` on the next pass, so a
partial poll is self-healing, while a deadlocked act-loop is not.

### `busy_timeout` deliberately stays at 5s

The obvious move is 5s → 30s. Declined. A 5s timeout that fails loudly is a
better detector than a 30s one that masks a regression, and `make status` now
surfaces the failed exit (§26). Fix the cause; keep the alarm sensitive.

### Evidence

`tests/test_watchdog_locking.py`, new, 3 cases. It stubs every probe and records
`conn.in_transaction` at the moment each is entered.

**The test caught a real hole in my own first fix.** I put the Slack loop's
commit at the tail of the body; both `continue` statements in that loop jump
straight to the next iteration and skip it — and they are exactly the paths that
have just written a fail-streak cursor:

```
AssertionError: these probes were called while holding the ledger's write lock:
['poll_slack_messages']
```

Moved to immediately before the probe, which is `continue`-proof. This is the
whole argument for writing the property test rather than reading the diff: the
diff looked right.

`test_the_guard_is_not_vacuous` exists because the invariant test is meaningless
if nothing wrote — no writes means no transaction means nothing to hold, and it
would have gone green against the very bug it exists to catch. So `uk` and
`hermes_log` return real rows, and the test asserts rows landed and ≥8 probes ran.

**Mutation check** — delete the first commit point (after `uk`):

```
AssertionError: these probes were called while holding the ledger's write lock:
['poll_docker']
```

```
$ make test
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   11/11 passed
  test_triage.py                   73/73 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_locking.py         3/3 passed
  test_watchdog_slack_blindness.py all cases as expected
```

Still to observe: the next real `com.jkrumm.warden-poll` run (1800s interval)
exiting 0 with the loop unaffected. Check `make status` and
`cursors.hermes_errors_log_offset`.
