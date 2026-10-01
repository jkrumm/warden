# STATE log — warden implementation history (§§1–56 verbatim, §57 onward appended)

This is the append-only build log. It is never rewritten; corrections are
appended as new sections. The two-page current state is `../../STATE.md`.
New wave sections are appended here as §57, §58, … and STATE.md is rewritten
to match.

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
| `triage.py:425-427` | `~/SourceRoot/weatherorb/var/health.json`, `$HERMES_HOME/gateway-starts.log`, `$HERMES_HOME/logs/errors.log` | evidence probes | two are hermes-owned files warden must keep reading |
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
`VERB_ALLOWLIST`, `_watchdog_poll`, `WEATHERORB_HEALTH_PATH`, `GATEWAY_STARTS_LOG`,
`HERMES_ERROR_LOG`, `TRIAGE_REPO_DIR`, `_call_propose_mappings_model`,
`_resolve_openai_base_url`, `_resolve_openai_api_key`. Slack is stubbed — no
network. Two tests write a **real hermes-cc stub script** and exercise the actual
subprocess path (`:634`, `:1120`) to prove the brief travels on stdin. One test
reads the **real** `config/triage-policy.json` (`:453`).

**It moves to warden essentially verbatim.** The only edits are the `_triage_env`
entries naming hermes-owned paths (`GATEWAY_STARTS_LOG`, `HERMES_ERROR_LOG`,
`WEATHERORB_HEALTH_PATH`) and the two loader paths at `:44-47` / `:55-56`.

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

---

## 32. Wave 1, item 2b + 2d — the plugin repoint, and the loop as backstop drainer (DONE)

### 2b — `hermes-agent` `3c5d516`

`plugins/dispatch-approval/__init__.py` no longer opens the ledger for writing.
Both of its connections are now `mode=ro`, verified by grep:

```
$ grep -n 'sqlite3.connect' plugins/dispatch-approval/__init__.py
263:    conn = sqlite3.connect(f"file:{db}?mode=ro", uri=True)
393:    conn = sqlite3.connect(f"file:{_db_path()}?mode=ro", uri=True)
```

The second one is `_load_invocation()`, which was opening a WRITABLE handle to
run `SELECT`s — not the slice's target, but "read-only means read-only" is a
rule in warden's CLAUDE.md and this was a standing violation of it.

`_record_decision()` now: read the row read-only → `_sign()` (unchanged) →
spool an `approval_decision` intent on **stdin** → drain synchronously → re-read
the row and decide from **the row**.

Three decisions in that flow worth keeping:

1. **The drain's exit code is deliberately NOT the success signal.**
   `intents.py --drain` exits 1 when *any* file in the spool was rejected,
   including one another surface wrote that has nothing to do with this click.
   It is logged; the row is the truth. There is a test for exactly this.
2. **A write that did not land raises, it does not return `None`.** `None`
   already means "no longer on file", which the click handler renders as
   ":grey_question: This approval request is no longer on file" — a lie about a
   row sitting there undecided. The handler already catches exceptions and
   renders ":warning: Could not record the decision", which is true.
3. **The race branch compares signatures.** `after["signature"] != sig` means
   someone else's click won; report their decision, as the old `rowcount == 0`
   branch did. Ed25519 here is deterministic, so two identical clicks by the
   same user mint the same signature and land in the "we won" branch — correct
   either way, and noted in the code so nobody "fixes" it.

**The authority model did not move**, and that was the point: RAM-only key,
minted at startup, signing a real Slack interaction payload, and
`require_signed_approval()` still the one and only verifier. Only the writer
moved.

```
tests/test_dispatch_approval.py   83 checks, 0 failure(s)   (was 51, +32)
tests/test_hermes_cc.py           all 165 cases as expected  (unchanged)
```

Mutation check, run here and not taken from the worker's report — stub the drain
out and the click stops working:

```
69 checks, 2 failure(s)
  test_decision_lands_through_the_real_queue        RuntimeError: … did not reach the ledger
  test_unrelated_rejection_does_not_fail_the_click  RuntimeError: … did not reach the ledger
```

### 2d — the loop drains too, and this was a real gap

Reading 2b's diff surfaced something the brief had not asked for: **nothing else
drained the spool.** The plugin drains synchronously, and that is the only drain
there was. If that drain failed — a locked ledger, a timeout, a version mismatch
— the intent stayed in `~/.warden/intents` and *nothing would ever pick it up*.
An intent nobody drains is a silent discard, which is the failure this whole
control plane exists to remove, reintroduced by the mechanism meant to remove it.

`triage.py` gains `drain_intents()` as **step 0 of `run()`**, before `ingest()`.
DESIGN.md § The ledger says intents go through the loop's queue; this is the
half of that sentence 2a and 2b had not built.

**`--dry-run` does NOT drain**, and this is a deliberate departure from this
file's usual "local bookkeeping runs for real under dry-run" rule
(`apply_resolutions`, `classify`). A dry-run is pointed at a COPY of the ledger,
but there is only ONE `~/.warden/intents`. Draining would permanently consume
intents the live loop still needs and apply them to a database nobody reads.
Eating the live system's queue is worse than either thing the dry-run contract
forbids.

`tests/test_triage.py` **73 → 75**:
- `test_the_loop_is_the_backstop_drainer` — spools through the REAL `intents`
  module (the same call the plugin makes, not a hand-written file) and asserts
  `run()` applies it and removes the file.
- `test_dry_run_never_consumes_the_live_spool`.

`_triage_env()` now also points `triage._intents.INTENTS_DIR` at its throwaway
directory and restores it. That is not incidental: it is **not** in the `saved`
dict (the spool lives on `triage._intents`, so the `setattr(triage, …)` loop
cannot restore it), and without it any test in the file could consume an intent
out of the live spool. Confirmed after the run: `~/.warden/` still contains only
`backups/` and `warden.db`.

Mutation check — delete the `drain_intents()` call from `run()`:

```
74/75 passed
  test_the_loop_is_the_backstop_drainer: the loop did not drain the intent:
    {'decision': None, 'decided_by': None, 'signature': None}
```

```
$ make test
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   11/11 passed
  test_triage.py                   75/75 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_locking.py         3/3 passed
  test_watchdog_slack_blindness.py all cases as expected
```

### Be precise about what "one writer" now means

The stop condition says the ledger has ONE WRITER. State it exactly, because the
claim is easy to overstate:

- The approval plugin **no longer writes it at all** — both handles are `mode=ro`.
  That was the named goal and it is met.
- The code that performs that write is **warden's own** `scripts/intents.py`,
  reached as a short-lived subprocess. So it is one writer *implementation*, not
  literally one OS process: the plugin's drain and the loop's drain are two
  processes running the same warden code against the same guarded UPDATE.
- `hermes-cc.sh` still writes `dispatches` and `dispatch_approvals` directly.
  That is the documented, deliberate exception (`fe95e81`, "a door for the one
  writer that is not warden") and was explicitly out of scope for this wave.

So: six writers → **two** (warden, and `hermes-cc.sh`), with the "control plane
inside the thing it supervises" coupling gone. Anything stronger than that is
not yet true.

### Not done yet — the gateway is still running the old plugin

The gateway (pid 65056) loaded `dispatch-approval` at start and has not been
restarted, so the OLD code is live until it is. No approvals are stranded by a
restart: all five rows in `dispatch_approvals` are decided and long expired
(newest expiry 2026-09-08), so no in-flight signature depends on the current
RAM key.

### Next action

Restart the gateway, confirm the plugin reloaded and republished its public key,
then Wave 1 item 3 — `state_deadline` and a named poller on every non-terminal
state. Item 3 also carries the `_run_migrations()` adoption bug found in §26:
case 2 stamps version 1 and `return`s without entering the migration loop, so the
first time `SCHEMA_VERSION` becomes 2, adopting a pre-versioned ledger stamps it
1, skips migration 2, and then fails `_verify_columns()`. That path is the
rollback (`~/.warden-cutover-backup/`), so it fails exactly when it is needed.

---

## 33. Wave 1, item 3a — `state_deadline`, and the migrator bug it exposed (DONE & LIVE)

Schema only. The deadline *logic* is 3b; this is the column and the migrator
that carries it.

### Migration 2

`ALTER TABLE triage_items ADD COLUMN state_deadline TEXT` plus an index.
`SCHEMA_VERSION` 1 → 2, and `WARDEN_SCHEMA_VERSION` in `hermes-cc.sh` 1 → 2 in
the same breath (`hermes-agent` `dbfa679`) — they are pinned to each other and a
mismatch is a loud exit, which is exactly what it did before the ledger moved:

```
$ .venv/bin/python3 scripts/ledger.py --check ~/.warden/warden.db
ledger: warden ledger at ~/.warden/warden.db is at schema_version=1, this
process expects SCHEMA_VERSION=2. Only the loop (triage.py, via
`connect(migrate=True)`) is allowed to migrate this file …
exit=1
```

One column, not one per state and not a second table: the deadline is a property
OF the row's current state, written by the same UPDATE that writes the state and
meaningless without it. A separate table could disagree with the row, and "the
deadline says investigating, the row says merged" is a question nothing could
answer. `liveness_deadline` (version 1) is deliberately left alone — it is the
deploy path's own probe window, and merging them would be a data migration on a
live ledger to save one column.

### The bug this exposed, which would have fired on the rollback

`_run_migrations()`'s adoption branch — case 2, "this is the live pre-versioned
ledger" — stamped version 1 and **`return`ed**, never entering the migration
loop. That was invisible for exactly as long as `SCHEMA_VERSION` stayed 1.

The moment it became 2, adopting a pre-versioned ledger would stamp it 1, skip
migration 2, and then fail `_verify_columns()` on the column migration 2 would
have added. **The only thing that adopts a pre-versioned ledger is the
rollback** — `~/.warden-cutover-backup/` holds exactly such a file — so it would
have failed at the one moment it was needed.

Fixed: adoption stamps 1 and then falls through to the loop. Adoption
establishes that the file is at version 1's *shape*; it does not establish that
it is at `SCHEMA_VERSION`.

### A second, quieter one in the same function's orbit

`_expected_columns()` derived the expected schema by replaying **`BASE_SCHEMA`
alone** — version 1's shape. At `SCHEMA_VERSION` 2 that means `_verify_columns()`
would silently stop checking every column added after version 1: the guard that
exists to catch a structurally incomplete database would itself have gone blind
to exactly the columns most likely to be missing. Now it replays BASE_SCHEMA and
every migration in order — the same "derive it, never copy it" property, one
version further on.

### Evidence

`tests/test_ledger.py` **11 → 12**. Three existing tests asserted `version == 1`
after adoption, i.e. they encoded the bug; corrected to `== ledger.SCHEMA_VERSION`
with the reason written in. The new test,
`test_adopting_a_pre_versioned_ledger_runs_every_later_migration`, pins it, and
its strongest assertion is that **an adopted database and a freshly created one
end up with the same schema** — the property the `return` broke.

Mutation check — put the unconditional `return` back:

```
7/12 passed
  test_adopting_a_pre_versioned_ledger_runs_every_later_migration:
    adoption stopped at version 1 instead of running on to 2 — every migration
    after the adopted one was skipped
  test_adoption_refuses_a_structurally_incomplete_database
  test_adoption_tolerates_an_unexpected_extra_column
  test_migrate_adopts_pre_versioned_database
  test_migrate_is_idempotent
```

All suites, both repos:

```
warden        test_dispatch_sweep all cases · test_intents 19/19 · test_ledger 12/12
              test_triage 75/75 · test_watchdog_delivery all cases
              test_watchdog_locking 3/3 · test_watchdog_slack_blindness all cases
hermes-agent  test_hermes_cc all 165 cases · test_dispatch_approval 83 checks
```

### The live migration

Snapshot first: `~/.warden/backups/pre-schema-v2-20260909T140408Z.db` (VACUUM
INTO from a read-only handle).

Then **the loop migrated it, not me** — `launchctl kickstart` on
`com.jkrumm.warden-loop` after confirming no `triage.py` was already running, so
"only the loop migrates, at boot" held and no second loop existed:

```
schema_version:        [(2, '2026-09-09T14:04:23.234003+00:00')]
state_deadline:        present on triage_items
rows:                  triage_items 52 · events 959   (nothing lost)
ledger.py --check:     exit 0
triage_last_run:       2026-09-09T14:04:23Z
make status:           all four agents ✓, last exit 0
```

`~/Library/Logs/warden-loop.err` is unchanged at 581 bytes, mtime 13:05Z — the
old crash, not a new one.

**And the 2c fix is confirmed live**, which §31 could only predict: the ingest
poller ran at **14:06:18Z**, after the per-source commit points landed, exit 0
with an empty `.err`. That is the first real pass in which its probes — `ssh`,
`gh`, two Slack fetches — ran outside a write transaction, against the live
network rather than a stub.

### Next action

3b — set `state_deadline` on every transition into a non-terminal state, and a
`sweep_deadlines()` step that acts on expiry per DESIGN.md § Deadlines. 3c — make
`poll_implement_jobs`/`poll_validation_jobs` survive sideclaw pruning its job
(`PRUNE_TTL_MS` 24h **or** `MAX_TERMINAL_ROWS` 200, confirmed at
`sideclaw/server/jobs/store.ts:41-42`, a cap shared with every interactive
`/check`). Both need a decision recorded first: three of the nine expiry actions
in DESIGN.md's table target `dismissed`, and that state does not exist yet.

---

## 34. Wave 1, item 3b/3c — deadlines and named pollers (DONE)

DESIGN.md principle 6 — "Every non-terminal state names the thing that polls it
and its deadline. **Checked against the diagram, not assumed**" — is now checked
by a test rather than by reading.

### What was built

- **`STATE_DISMISSED`**, terminal, reason always required (enforced in the
  helper, not by convention). Built because three of the nine expiry actions in
  DESIGN.md's table target it. Scoped to that: this is NOT the Wave 2 `resolved`
  → `fixed`/`quiet`/`closed` split.
- **`STATE_DEADLINES`** — one closed table mapping every non-terminal state to
  its poller, its hours, and its expiry action. The poller is a *string in the
  table*, so principle 6 is satisfied in code rather than in a comment.
- **`_set_state()`** — all **26** transition sites now route through one helper
  that writes state, `updated_at` and `state_deadline` as one fact.
  `grep -c 'UPDATE triage_items SET state=' scripts/triage.py` → **1**.
- **`sweep_deadlines()`** in `run()`, after the chain pollers (a poller that can
  still advance an item gets its chance before the clock takes it) and before
  `sync_card()` (so the expiry shows on the card the same pass).

### Three documented deviations from DESIGN.md's table

- **`verdict` is not in that table and is non-terminal.** `maybe_auto_implement`
  only advances it when the repo has auto-implement enabled, so otherwise it
  sits forever. 24h → `needs_human`. An addition to the design's table, not a
  contradiction of it.
- **`merged` expires to `resolved`, where DESIGN.md says `closed`.** `closed` is
  Wave 2. Noted in the code as the line that changes then.
- **`needs_human`'s "reminder at 1d" is not built.** A reminder is a
  notification feature; the 7d expiry is the deadline. Scoped out deliberately,
  recorded here so it reads as scoped-out rather than missed.

`liveness_pending` and `snoozed` keep their own columns (`liveness_deadline`,
`snoozed_until`) and are named in the table pointing at them, so the audit passes
without two mechanisms that could disagree.

### 3c — the sideclaw pruning gap closes with no new column

`poll_implement_jobs`/`poll_validation_jobs` both did `if resp is None: continue`,
so an item whose sideclaw job was pruned was polled forever with nothing moving
it. sideclaw prunes terminal jobs at 24h **or** 200 rows
(`sideclaw/server/jobs/store.ts:41-42`, re-verified — a cap shared with every
interactive `/check`, so 200 can arrive in an afternoon). The 2h/1h deadlines
fire long before either bound, so a pruned job's item exits to `merge_blocked` on
the clock. **No miss counter, no new column.**
`test_a_pruned_sideclaw_job_does_not_strand_an_item` proves it: the item does not
move inside its deadline and does reach `merge_blocked` past it.

### The correction I made to the delivered work

The first version **reported** a NULL `state_deadline` on a non-terminal row and
deliberately did not backfill it, because `updated_at` is rewritten by `ingest()`
on every recurrence and a deadline derived from it would move further away every
pass. That reasoning is right, and the conclusion was wrong in the only case that
occurs: **every NULL-deadline row on the live ledger is `needs_human`, and the
only thing that transitions a `needs_human` row is a human.** So "it gets a
deadline when it next transitions" meant "never", for precisely the population
that must not sit forever — four lines of FINDING every 600s, bounding nothing.

Now it stamps from **`now`**, which cannot drift the way `updated_at` can, and is
deliberately conservative (a row six days into `needs_human` gets a fresh 168h
rather than expiring at once — erring toward not-discarding, the same call item 1
made). It still prints a FINDING, once, at stamp time: a NULL deadline is either
a pre-column legacy row or a bug in a transition site, and silently fixing the
second is how it stays a bug.

### Evidence

```
$ make test
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   12/12 passed
  test_triage.py                   92/92 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_locking.py         3/3 passed
  test_watchdog_slack_blindness.py all cases as expected
```

**75 → 92 (+17).** The regression gate is now **92/92**; CLAUDE.md still says
68/68 and wants updating.

Mutation checks, run here:

| Mutation | Result |
|-|-|
| drop `needs_human` from `STATE_DEADLINES` | 84/91 — the principle-6 test names it: *"a state with no named poller and no deadline is a state an item sits in forever"* |
| `sweep_deadlines()` → no-op | 81/91 — all eight expiry edges, plus the pruned-job and NULL-deadline tests |

Against a VACUUM INTO copy of the **live** ledger, two consecutive dry-runs:

```
pass 1:  4 FINDING lines   (events 543/875/930/943, all needs_human, all pre-column)
pass 2:  0 FINDING lines
after:   state_deadline = 2026-09-16T17:28:30Z on all four — 168h out, stamped once
         states unchanged: nothing was expired, only bounded
```

The four items that item 1 stopped being silently discarded are now also the four
items that can no longer sit unbounded. Those are the same four rows, and that is
the whole wave in one line.

### Files

`scripts/triage.py`, `tests/test_triage.py`, `docs/triage.md` (loop step 6c,
`dismissed` in Carded states, the deadline edges in the diagram, a new Deadlines
section).

### Noticed, not fixed

- `dismissed` is terminal and `reopen_if_needed()` only reopens `resolved`, so a
  dismissed signature that recurs stays invisible — the same as `ignored`/`note`,
  and arguably correct, but it is a real edge and Wave 2's state-machine work
  should decide it deliberately.
- `sync_card()`'s never-carded guard covers only `STATE_RESOLVED`, so a
  `dismissed` row that somehow lacked `card_ts` would post a first card.
- `docs/triage.md` §Tests still counts "30 cases"; `CLAUDE.md` still pins the
  gate at 68/68. Both stale.

---

## 35. Wave 1 — final state, against the stop condition

A fresh reviewer with no context checked the diff and the running system against
DESIGN.md and FLOWS.md. It confirmed items 1-4 by independent measurement
(including running its own mutations), found **five defects**, and its most
valuable finding was about this file: §§26-34 contained **no stop-condition
roll-up and no record of item 5 at all**, so a stranger would have read Wave 1 as
four items, all done. The escalation existed only in the orchestrator's chat —
which is the exact thing STATE.md exists to prevent. That omission is corrected
by this section.

### The stop condition, item by item

| # | Condition | Status | Evidence |
|-|-|-|-|
| 1 | Silence-resolve applies only to `new`; a `needs_human` item cannot be discarded by a fault clearing itself | **MET** | `aa30ddc`. Old vs new on live snapshots: the old loop resolved all four real `needs_human` rows and erased all four notes; the new one keeps them. Reviewer's own mutation to `(new, needs_human)` fails 4 tests by name. |
| 2 | An intent can be recorded by any surface and signed only by Slack or a TTY | **HALF MET** | `c788416` (`intents.py`, records with no DB handle at all), `0245f98` (loop drains as backstop), `3c5d516` (Slack signs). **The TTY half is item 5 and is unbuilt.** |
| 3 | The approval plugin no longer writes the ledger directly | **MET** | `3c5d516`. Both handles `mode=ro` (`__init__.py:263,393`). Gateway restarted 15:59:27, pubkey rotated — the new plugin is live, verified by the reviewer. |
| 4 | Every non-terminal state carries a `state_deadline` and a named poller that survives sideclaw pruning | **MET** | `4c3b8c9`. `STATE_DEADLINES` + the principle-6 enumeration test; `grep -c 'UPDATE triage_items SET state='` → 1. The reviewer watched the **17:34:38Z** loop tick stamp all four `needs_human` rows to `2026-09-16T17:34:38Z` while the three `new` rows correctly stayed NULL. |
| 5 | The CLI decide path works at a TTY with the gateway stopped | **NOT MET — BLOCKED** | See below. |

### Item 5 — blocked on a contradiction inside DESIGN.md, not on effort

Independently confirmed by the reviewer. Three statements that cannot all hold:

- `DESIGN.md:285` — the signer's key is **RAM-only, failure domain "gateway process"**.
- `DESIGN.md:336` and `FLOWS.md:165` — approval is mintable by "Slack Socket Mode **or a TTY-gated CLI**".
- `FLOWS.md` flow 5 — the flow the TTY path exists for **is the gateway being wedged**.

`require_signed_approval()` reads exactly one public key from one file
(`hermes-cc.sh:1131`, `Ed25519PublicKey.from_public_bytes(open(A_PUB).read())`,
no loop). With the gateway stopped, **no signature can exist at all** — so the
TTY path is *unimplementable as specified*, not merely unbuilt. The obvious
shortcut (the CLI asks the running gateway to sign) reintroduces the signing
oracle REVIEW.md **C1** already rejected.

**Nothing in the diff pretends otherwise** — the reviewer checked: no `isatty`,
no `--decide`, no signing code anywhere in warden.

**Recommended resolution, awaiting a decision.** Not "no key at all": the threat
is not the network, it is the episode. warden's own CLAUDE.md — *"an episode is
not contained … a bearer token on this host is not an authorization boundary
against an episode"* — and a brief is attacker-influenceable, so an unsigned
approval row is one an injected episode can write itself, and Tailscale does not
exclude an adversary that arrived as text inside a brief. But **no key at rest**:

```
TTY signer: Ed25519 seed = scrypt(passphrase, salt)   # derived, never stored
  - nothing on disk to steal
  - the passphrase IS the TTY gate: an episode has no TTY and does not know it
  - require_signed_approval() reads a pubkey DIRECTORY, not one file, and
    accepts either signer
```

This needs a passphrase typed once by a present human to mint the pubkey, which
is why it is escalated rather than decided.

### The five defects the reviewer found, and their disposition

| # | Defect | Fixed in |
|-|-|-|
| 1 | `sweep_deadlines()`'s docstring said a NULL deadline "is not backfilled either" while the code 40 lines down backfilled it — a lying comment inside the function that decides when work is terminally dismissed | this commit |
| 2 | `CLAUDE.md` pinned the regression gate at 68/68 against an actual 96 — the next stranger, following it literally, reports a healthy suite as a defect | this commit |
| 3 | `docs/triage.md` counted 30/42/54/64 cases | this commit — now says the current total and that those are the running tally as each group landed |
| 4 | `scripts/watchdog-summary.py:118` opened the LIVE ledger with a bare writable handle and only ever SELECTed — the exact defect this wave fixed in the plugin's `_load_invocation()` | this commit |
| 5 | this file had no Wave 1 roll-up and no record of item 5 | this section |

### Corrections to what §§31-34 asserted

- **§31 overstated "a partial poll is self-healing".** The reviewer is right and
  the correction matters: `reconcile()` stamps `notified_at` when it collects a
  new event (`watchdog-poll.py:924`), but the digest is composed and posted only
  after `_run_poll()` returns. Before the per-source commits, a mid-poll
  exception rolled everything back and nothing was burnt. Now, sources committed
  before a crash keep `notified_at` and **never appear as NEW in any digest** —
  they resurface as a reminder up to 24h later. The *rows* re-derive; **the
  notification stamp does not.** That is a narrower instance of exactly what
  `reconcile()`'s `deliver=False` exists to prevent. The trade is still right —
  the alternative was a deadlocked act-loop, and each probe is individually
  exception-safe so the window is small — but §31 should have said this, and
  now does.
- **§32's "six writers → two" omitted `watchdog-summary.py`**, which held a
  writable handle. It is now genuinely two (warden, `hermes-cc.sh`). And
  §32's "the gateway is still running the old plugin" was true when written and
  is now stale: the restart happened at 15:59:27.
- **`STATE.md:801` said `watchdog-summary.py` was "read-only".** That was false
  of the handle it opened. It is true now.

### Two things fixed here that the wave itself created

- **`sweep_deadlines()` now honours `--dry-run`.** It was the one
  local-bookkeeping step that could move an item *terminally*, and `--dry-run`
  defaults to the live ledger. The three steps the dry-run contract was written
  around move an item between working states and the next real pass re-derives
  them; a dismissal is not re-derivable. A preview must not be able to end an
  item. Its `dry_run` parameter was also accepted and never read.
- **A `dismissed` signature that recurs now reopens.** Without this, item 1 would
  have been handed straight back by item 3's clock: a `needs_human` row
  protected from silence-resolve would instead go terminal on a 7-day fuse and
  never be seen again however often its monitor fired — and the four real `uk:*`
  rows now carrying `2026-09-16T17:34Z` deadlines are exactly that shape.
  `ignored` and `note` deliberately do NOT reopen: a human looked at those and
  said benign. `dismissed` means *nobody answered*, which is not the same fact.

`~/.warden/warden.db` also went from mode 644 to 600, matching the care already
taken with the intents spool one directory over.

### Verified, not claimed

```
$ make test
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   12/12 passed
  test_triage.py                   96/96 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_locking.py         3/3 passed
  test_watchdog_slack_blindness.py all cases as expected

hermes-agent: test_dispatch_approval.py 83 checks · test_hermes_cc.py 165 cases
```

The reviewer separately confirmed, by restoring it: **the rollback still works.**
`~/.warden-cutover-backup/watchdog.db.pre-cutover` run through
`ledger.connect(migrate=True)` reaches version 2, gains `state_deadline`, keeps
49 items / 956 events, and ends with a schema identical to a freshly created one
— which is precisely the property §33's adoption fix was written to restore.

It also traced the `intents.py` threat model end to end and confirmed it: a
forged `approve` carrying a garbage signature hits `InvalidSignature` in
`hermes-cc.sh:1113`, falls through to `pending`, and **nothing dispatches**. The
one refinement worth recording: a forged row does permanently block the real
click (`AND decision IS NULL`) and Slack then renders "already decided", so it is
a *permanent and misleadingly-labelled* denial. Still a denial, still inside "at
worst deny" — but the label is wrong, and Wave 2 should say "superseded" rather
than "already decided" when the signature does not verify.

### Still open, in priority order

1. **Item 5** — needs the decision above. Wave 1 is not closed until it lands.
2. `sync_card()`'s never-carded guard covers only `STATE_RESOLVED`, so a
   `dismissed` row that somehow lacked `card_ts` would post a first card.
3. The `needs_human` "reminder at 1d" from DESIGN.md's deadline table is
   deliberately unbuilt (a reminder is a notification, not a deadline).
4. `docs/triage.md` § "Known: grouped reopen churn" is still an open defect,
   unchanged by this wave.

### Next action

**Do not start Wave 2.** Resolve item 5 first — it is the last Wave 1 item, and
it is one design decision plus roughly a day of work, not a wave.

---

## 36. Wave 1, item 5 — RESOLVED BY WITHDRAWAL, and why that is the right answer

Item 5 ("the CLI decide path works at a TTY with the gateway stopped") is
**closed by deleting the requirement**, not by building a signer. Decision taken
2026-09-09 with the operator's delegation ("do whatever is stable and will not be
the next issue I'm complaining about"). Recorded here in full because a future
session will want to rebuild it.

### It was unimplementable as specified

Confirmed independently by the wave-boundary reviewer. The signing key is RAM-only
in the gateway (`DESIGN.md` § Systems), and `require_signed_approval()` reads
exactly one public key from one file (`hermes-cc.sh:1131`). **With the gateway
stopped — the only scenario the TTY path existed for — no signature can exist at
all.**

### The three ways out, and why each fails

1. **CLI asks the running gateway to sign.** Reintroduces the signing oracle
   `REVIEW.md` **C1** already rejected, and still does nothing when the gateway is
   the thing that is down.
2. **A second key file on disk.** An episode runs as `jkrumm` with unrestricted
   Bash and reads files. warden's own CLAUDE.md: *"a bearer token on this host is
   not an authorization boundary against an episode."* A key file is weaker than
   a token.
3. **A passphrase-derived key (my own initial recommendation).** Cryptographically
   fine, operationally a footgun, and the deciding fact came from this estate's
   secrets model rather than from cryptography:

   `dotfiles-private/headless.refs` — *"`op://Private/...` is refused by the seed
   unconditionally (fail-safe) — Private is the human-only vault and can never
   enter the headless cache"*, while everything that IS in the mini's cache
   resolves **headless**. So on this machine, anything an operator can store an
   episode can read, and a human-only secret cannot be stored at all. The
   passphrase would have to live only in a human's head, be used perhaps twice a
   year, and have **no recovery path** once the pubkey is published. Forgetting it
   permanently removes the emergency approval surface — the opposite of what it
   was for.

### What flow 5 actually does now

**Restart the gateway, then approve in Slack.**
`launchctl kickstart -k gui/$UID/ai.hermes.gateway` — measured at ~8s on
2026-09-09 (new pid, `~/.hermes/dispatch-approval.pub` rotated at 15:59,
Slack app re-initialised, no errors). FLOWS.md flow 5 already concedes that
reviving a wedged gateway *needs a human at a machine*; a human at a machine can
restart a LaunchAgent. **The TTY signer solved a problem the restart already
solves, in the one flow it existed for.**

If the gateway cannot be restarted at all, no approval can be minted and **warden
fails closed**: it keeps ingesting, triaging and carding, and nothing merges.
That is the right way to be broken.

### FLOWS.md was wrong in a second way, now corrected

*"You approve in Argo, or at a TTY."* **Both halves were false.** Argo records
intents and cannot approve — FLOWS.md's own surface table says so two sections
later, and that table also said "confirmation in Slack or TTY". Three statements
in two documents, all asserting a capability that never existed. Fixed in this
commit: `DESIGN.md` § The decision primitive (the withdrawal and its reasoning),
`DESIGN.md` § Systems (the signer row), `DESIGN.md` § Migration (the Wave 1 row),
`FLOWS.md` flow 5, `FLOWS.md` surface table.

### Be exact about what this means for the stop condition

Item 5 is **withdrawn, not met**. Four of five were built; the fifth was removed
as unimplementable-and-unnecessary, with the design amended so the next reader
does not rebuild it. Anyone who wants it back must first solve the human-only
secret problem above — that is the real blocker, and it is an estate-level
question, not a warden one.

**Wave 1 is closed.**

---

## 37. Wave 2 — reconnaissance against the running system (2026-09-09, ~18:40Z)

Read-only. No edits, no restarts, no writable handle on the live ledger.

### The system is healthy, and every Wave 1 claim re-verified

```
$ make status
  venv                     Python 3.11.15
  agents:
    ✓ com.jkrumm.warden-loop  [pid -, last exit 0]      runs = 41
    ✓ com.jkrumm.warden-poll  [pid -, last exit 0]
    ✓ com.jkrumm.warden-sweep  [pid -, last exit 0]
    ✓ com.jkrumm.warden-backup  [pid -, last exit 0]
  policy                   ✓ both copies agree on all 30 repos
  ledger                   988K

$ make test
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   12/12 passed
  test_triage.py                   96/96 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_locking.py         3/3 passed
  test_watchdog_slack_blindness.py all cases as expected

$ grep -c 'UPDATE triage_items SET state=' scripts/triage.py
1
```

| Check | Result |
|-|-|
| `schema_version` | **2** |
| Non-terminal rows with NULL `state_deadline` | **0** |
| `triage_last_run` heartbeat | `2026-09-09T18:35:01Z`, 3.9 min old against a 600s interval |
| State census | `resolved` 30 · `note` 8 · `ignored` 7 · `needs_human` 4 · `new` 3 |
| The four `uk:*` rows | all four `needs_human`, all four `state_deadline = 2026-09-16T17:34:38Z` |
| Ledger mode | 600, WAL, `-wal` present and drained |
| `pgrep -f triage.py` | nothing — no second loop |

`~/Library/Logs/warden-{poll,sweep}.err` are **0 bytes**. `warden-loop.err` is 29 KB
and **entirely stale**: it opens with the `sqlite3.OperationalError: database is
locked` traceback from `ingest()` that §31 root-caused and `261e827` fixed, and it
ends with the Wave 1 `state_deadline` backfill FINDINGs stamped `17:34Z`. Nothing
in it postdates the backfill. Not a live fault.

### Finding 1 — the reopen churn is 23 of the 30 `resolved` rows, measured

Running Wave 2's target predicate against the live ledger read-only:

```sql
SELECT COUNT(*) FROM triage_items ti JOIN events e ON e.id = ti.event_id
 WHERE ti.state IN ('resolved','dismissed') AND e.resolved_at IS NULL;
-- 23   (slack_alert 17, hermes_log 6)
```

**23 rows are reopened and re-quiet-resolved on every single pass**, every ten
minutes, 41 runs so far. `docs/triage.md` described the mechanism; this is the
count. The remaining 7 (`uk` 5, plus 2) have `events.resolved_at` set and are
stable.

### Finding 2 — the churn has already destroyed the distinction Wave 2 exists to record

This is new, and it changes what the state-split migration can honestly do.

```sql
SELECT COUNT(*) FROM triage_items WHERE note LIKE '%recover%';   -- 0
```

`DESIGN.md` § Why this exists and `REVIEW.md` § Facts corrected both name items
**931 and 932** as the two closes that were *verified recoveries* —
`resolve_recovery_paired()` firing after `jkrumm/vps#8` merged, the only
positive-signal closes the system has ever produced. Both rows today read:

```
931  resolved  "signal quiet since 2026-09-07 16:04 UTC — no new occurrence for 2h…"
932  resolved  "signal quiet since 2026-09-07 16:04 UTC — no new occurrence for 2h…"
```

`RECOVERY_PAIRED_NOTE_PREFIX` ("recovery message observed: ") appears on **zero**
rows in the database. The reopen churn overwrote it: `reopen_if_needed()` flips the
row to `new` leaving `note` alone, and `resolve_quiet_grouped()` then writes its own
note over the top. Every pass. The card never changed because the *replacement* note
is byte-identical each time — `card_hash` hid the overwrite exactly as it hides the
churn.

Two consequences:

- The note field is **not** a usable provenance source for the `fixed`/`quiet`
  split on the two rows that matter most. Their evidence is in `dispatches`
  (both carry `dispatch_job 74f2e233…`), in `DESIGN.md`, and in `vps#8` — not in
  the ledger.
- It is a second, independent argument for DESIGN.md's ordering. The churn does not
  merely re-render a card; **it rewrites the item's own history**, and the state
  split is precisely an attempt to record history in that row.

### Finding 3 — the occurrence signal is in two clocks, not one

Relevant to the fix, and easy to get wrong. `docs/triage.md` says to reopen on
"the grouped payload's `ts_last` moving". `ts_last` is a **Slack `ts` float-string**
(`"1788850795.862159"`), while `last_reminder_at` / `notified_at` / `first_seen` are
**ISO-8601**. A single `MAX()`/`>` over the two is a lexical comparison between
`"1788…"` and `"2026…"` and is wrong in a way that reads as correct.

They also move on different events. `upsert_grouped()` re-stamps
`last_reminder_at`/`notified_at` **only when it emits** (cooldown-gated); on a
suppressed occurrence it writes `payload_json.ts_last` and nothing else. So neither
field alone is the occurrence signal — and `resolve_quiet_grouped()`'s idle anchor,
which uses only the ISO trio, is blind to a cooldown-suppressed recurrence. That is
a separate defect from the churn; recorded here, not fixed here.

For a **state** source there is no `ts_last` at all, and the reopen signal is
`watchdog-poll.py:878` resetting `resolved_at=NULL, first_seen=now,
notified_at=NULL, last_reminder_at=NULL` — which does move the ISO trio.

### Finding 4 — `ingest()` runs before `reopen_if_needed()`, and rewrites `updated_at`

`run()` order is `drain_intents → ingest → reopen_if_needed → …`, so the reopen step
already sees this pass's refreshed `triage_items.last_seen`. Useful. But `ingest()`
also writes `updated_at = now` on every open row every pass, so **`updated_at` is
not the time the item last changed state** and cannot be used as the "resolved at"
anchor a new-occurrence comparison needs. The comparison needs its own stored mark.

### Next action

Slice 2.1 — the reopen condition, per DESIGN.md's ordering. Design settled by the
findings above; written up with the slice.

---

## 38. Wave 2, item 1 — the reopen condition (DONE & LIVE, schema_version 3)

`DESIGN.md` § Migration puts this first in Wave 2 and says why: doing the state
split first would make the churn visible and noisy. It was worse than churn —
see §37 finding 2.

### What changed

| File | Change |
|-|-|
| `scripts/triage.py` | `_occurrence_mark()`; `_set_state()` stamps it on **every** transition; `reopen_if_needed()` compares marks instead of asking `events.resolved_at IS NULL` |
| `scripts/ledger.py` | `SCHEMA_VERSION` 2 → **3**, `_MIGRATION_3` adds `triage_items.occurrence_mark`. No index, **no data backfill** |
| `hermes-agent/scripts/hermes-cc.sh` | `WARDEN_SCHEMA_VERSION` 2 → 3 — pinned to warden's, a mismatch is a loud exit 2 |
| tests | `test_triage.py` 96 → **102**, `test_ledger.py` 12 → **14** |
| `docs/triage.md`, `CLAUDE.md` | the § Known subsection rewritten; the regression gate re-pinned |

The mark is five `|`-separated slots — `payload_json.ts_last`,
`last_reminder_at`, `notified_at`, `first_seen`, `reminder_count` — compared with
`!=`, **never** with `>` or `MAX()`. That is load-bearing: `ts_last` is a Slack
`ts` float-string and the other four are ISO-8601, so an ordinal comparison
across them compares `"1788…"` against `"2026…"`. Fixed slots plus whole-string
equality never compares one clock against the other. Neither family alone is
sufficient — a cooldown-suppressed grouped occurrence moves only `ts_last`, and a
state source has no `ts_last` at all.

A NULL mark is **not** a forgotten stamp — it is a row that closed before the
column existed, which on the live ledger was all 30 of them. `reopen_if_needed()`
treats NULL as *baseline unknown*: it stamps the current mark and leaves the state
alone. That adoption rule **is** the backfill, which is why migration 3 writes no
data and needs no hardcoded row list.

### Verified, not claimed

Two-pass run of the real `run()` against a `VACUUM INTO` copy of the live ledger,
`WARDEN_DB` redirected, intents spool redirected, `--dry-run`, counting every
`_set_state()` call that actually changed a state:

```
BEFORE (snapshot at schema 2, pre-fix code)
  pass 1: transitions = 46    resolved -> new: 23    new -> resolved: 23
  pass 2: transitions = 46    resolved -> new: 23    new -> resolved: 23
  final census: {'ignored': 7, 'needs_human': 4, 'new': 3, 'note': 8, 'resolved': 30}

AFTER (same snapshot, fixed code, migrating itself to 3 on connect)
  pass 1: transitions = 0
  pass 2: transitions = 0
  final census: {'ignored': 7, 'needs_human': 4, 'new': 3, 'note': 8, 'resolved': 30}
```

Same copy after one real `scripts/triage.py --dry-run`: `schema_version` 3,
30 rows stamped, **0** `resolved`/`dismissed` rows left with a NULL mark, census
unchanged.

```
$ make test
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   14/14 passed
  test_triage.py                   102/102 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_locking.py         3/3 passed
  test_watchdog_slack_blindness.py all cases as expected
```

Mutation-tested, each broken → red **by name** → restored → green:

| Mutation | Caught by |
|-|-|
| `reopen_if_needed()` back to `AND e.resolved_at IS NULL` | `test_quiet_resolved_grouped_item_does_not_churn_with_no_new_occurrence`, `test_adoption_null_occurrence_mark_does_not_reopen_but_gets_stamped` |
| drop `ts_last` from the mark | `test_new_ts_last_alone_reopens_a_quiet_resolved_grouped_item`, `test_occurrence_mark_keeps_the_two_clocks_separate` |
| NULL mark reopens instead of stamping | `test_adoption_null_occurrence_mark_does_not_reopen_but_gets_stamped` |
| remove the `occurrence_mark` write from `_set_state()` | 7 tests, incl. `test_a_dismissed_signature_that_recurs_comes_back`, `test_state_source_reopen_via_resolved_at_reset_still_reopens` |

Three existing tests changed their **setup** (raw `INSERT`/`UPDATE` → going
through `_set_state()`, so the row has a mark at all) and none changed its
assertion except to strengthen it. The one that documented the churn in a comment
(`test_quiet_grouped_resolves_after_window_and_updates_card_once`) now asserts its
absence directly — on `occurrence_mark` and `note` identity, **not** on
`updated_at`, which `ingest()` legitimately rewrites every pass on every open row.

### The live ledger migrated itself, and that is a lesson about this repo

The plan was: leave both trees dirty, snapshot, then sequence the migration by
hand. That plan was **wrong on its face and the loop proved it within three
minutes.** The LaunchAgents execute `scripts/triage.py` *from the working tree*,
and `~/.hermes/scripts` is a whole-directory symlink into `hermes-agent` — so an
uncommitted edit to either file is **already in production the moment it is
saved**. The 18:45Z tick migrated `~/.warden/warden.db` to version 3 and adopted
all 30 rows on its own.

**In this repo there is no staging window between editing and deploying.** A
worker editing `scripts/*.py` is editing the running system. It was harmless here
only because the change is additive, the adoption path is safe, and the
`hermes-cc.sh` pin was edited in the same window — a mismatch would have been a
loud exit 2, which is the designed failure. Anything less benign needs
`make unload` first. This belongs in the next handover.

Post-migration health, live:

```
schema_version 3 · occurrence_mark on 30 rows · 0 NULL marks in resolved/dismissed
census unchanged {'ignored': 7, 'needs_human': 4, 'new': 3, 'note': 8, 'resolved': 30}
four uk:* needs_human rows still at state_deadline 2026-09-16T17:34:38Z
warden-loop.err mtime unchanged (19:34 local) — no new traceback · last exit 0 · runs 43
```

### One asymmetry found by reading the migrated data, recorded not fixed

11 of the 30 stamped rows carry a mark whose `ts_last` slot is empty; 6 are
`hermes_log`, whose payload is `{"first_line": …}` and has **no `ts_last` at all**
(`poll_hermes_logs()` does not build the Slack grouping's `ts_first`/`ts_last`
shape). A cooldown-suppressed `hermes_log` occurrence therefore moves nothing in
the mark and does not reopen the row until the 24h cooldown emits.

Not a regression: under the old predicate that same row was reopened and then
re-resolved by `resolve_quiet_grouped()` **inside the same pass**, and
`escalate()` runs after both — so a suppressed occurrence never re-escalated
anything before either. Re-escalation latency stays bounded by the source's own
cooldown. Written into `docs/triage.md` next to the self-tuning quiet window,
which is where "still firing but suppressed" is the actual subject.

### Also observed

The churn was driving phantom work into the recovery-pairing step: with 23 rows
transiently in `new`, `resolve_recovery_paired()` saw **17 open `slack_alert`
candidates** every pass (`[dry-run] would check 17 open slack_alert item(s)`),
each of which is a live Slack read outside dry-run. Post-fix that set is empty and
the step prints nothing.

### Next action

Slice 2.2 — split `resolved` into `fixed` / `quiet` / `closed`, schema 4. Two
decisions already taken and to be recorded with it: all 30 live rows migrate to
`quiet` (the `fixed` provenance for 931/932 is unrecoverable from the ledger —
§37 finding 2), and an append-only `item_transitions` table lands with the split,
because three of `/metrics`' six numbers are not derivable without it.

---

## 39. Wave 2, item 2 — honest states (DONE, schema_version 4)

`resolved` is three states now. `DESIGN.md` § the state machine defines them;
this section records which producer got which bucket and why, because the
mapping is the whole slice and one line of it is counter-intuitive.

| Producer | Bucket | Why |
|-|-|-|
| `apply_resolutions()` — disappearance from observation | `quiet` | pure silence |
| `resolve_quiet_grouped()` — the 2h timer | `quiet` | its own note already says "NOT a confirmed fix" |
| `resolve_recovery_paired()` — an explicit ✅ | **`quiet`** | see below |
| `maybe_check_liveness()`, positive branch | **`fixed`** | the only producer of `fixed` in the system |
| `merged` deadline expiry (1h, no deploy target) | `closed` | DESIGN.md's deadline table and FLOWS.md flow 2 |
| `cmd_close --close <sig> --reason <text>` | `closed` | new; see below |

**Recovery-pairing is `quiet`, not `fixed`, and that is the line a future
reader will try to "fix".** A ✅ is a positive signal, so `fixed` looks right.
`DESIGN.md` § What must not be lost, item 4 forbids it in as many words:
*"Recovery-pairing is the strong path, the 2h timer the fallback, and neither
ever claims a fix."* Nothing shipped — the service recovered, by our hand or its
own, and a ✅ cannot tell those apart any better than silence can. The
distinction from the timer path stays where it already was, in
`RECOVERY_PAIRED_NOTE_PREFIX`. There is now a test whose only job is to fail if
someone changes it: `test_recovery_paired_never_produces_fixed`.

`STATE_RESOLVED` was **deleted**, not aliased, so every one of its ~15 references
had to be re-decided by hand and a missed one is an import-time `NameError`
rather than a silent string mismatch.

### `cmd_close`, because a state nothing can produce is not honest

After the mapping above, `closed`'s only producer would have been a 1h clock —
while its definition is *"a human said done"*. `cmd_close` (`--close <signature>
--reason <text>`) is that producer, following `cmd_snooze`/`cmd_ignore`/
`cmd_reopen`'s exact shape. The reason is required, for the same reason
`_set_state()` already requires one for `dismissed`.

### `item_transitions`, and why it is not scope creep

Append-only history: `event_id`, `from_state`, `to_state`, `at`, `note`. Written
by `_set_state()` and nothing else — it is already the only writer of
`triage_items.state`, and a second writer of history is a second source of truth
about it.

It is here because **three of `/metrics`' six funnel numbers are not derivable
from the ledger without it**: median `needs_human` → decision, verified
unattended fixes per week, and reopen-after-`fixed`. All three need when an item
entered and left a state, and `triage_items.updated_at` cannot answer that —
`ingest()` rewrites it on every open row on every pass regardless of state.
Slice 2.3 would otherwise have had to fabricate half of itself.

Two properties worth keeping:

- A row is appended **only on a real change** (`rowcount > 0` **and**
  `prev_state != state`). `_set_state()` also serves callers that write columns
  with the state unchanged — `sync_card()` writing `card_ts`/`card_hash` — and
  recording those would corrupt every duration computed from the table.
- The table starts **empty and records nothing retroactively**. A `/metrics`
  window predating it is empty *by construction*, not zero. That distinction is
  in the migration comment, because "0 fixes last week" reads as a measurement.

### All 30 live rows become `quiet`, none becomes `fixed`

Recorded in `_MIGRATION_4`'s comment as well as here, because it reads like
laziness. Every one of the 30 is a silence close by note (`signal quiet since …`)
or by an empty note. `RECOVERY_PAIRED_NOTE_PREFIX` is on **zero** rows.
`DESIGN.md` and `REVIEW.md` name items 931/932 as the only two verified-recovery
closes this system has produced — but the pre-`occurrence_mark` churn overwrote
both notes, `dispatches.merged_at` is NULL for both, `vps#8` is recorded on a
dispatch whose `origin_event_id` is NULL, and each row's `dispatch_job` points at
a *later* investigate episode. **Back-dating a state from a prose document is not
a migration.** `/metrics` will read 0 verified fixes for the period before the
history table existed, understating true history by two — the correct direction
to be wrong.

### Also fixed here, because the line was being edited anyway

`sync_card()`'s never-carded guard covered only `resolved`, so a `dismissed` row
that somehow lacked `card_ts` would have posted a *first* card. It now checks
`_NEVER_CARDED_FIRST_STATES = (fixed, quiet, closed, dismissed)`. That closes
known-open item 1 from §35.

### Verified, not claimed

```
$ make test
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   16/16 passed
  test_triage.py                   107/107 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_locking.py         3/3 passed
  test_watchdog_slack_blindness.py all cases as expected
```

Two `run()` passes against a `VACUUM INTO` copy of the live ledger (taken 19:12Z,
`WARDEN_DB` and the intents spool redirected, `--dry-run`):

```
pass 1: transitions = 1     quiet -> new: 1
pass 2: transitions = 0
final census: {'ignored': 7, 'needs_human': 4, 'new': 5, 'note': 8, 'quiet': 29}
schema_version 4 · rows still in 'resolved': 0 · item_transitions: 1
```

The single pass-1 transition is **correct, not churn**, and was chased down
rather than assumed: event 261 (`hermes_log`, `slack_bolt.AsyncApp: Failed to
connect`) carries `last_reminder_at = 2026-09-09T19:07:18Z`, an emit-path
occurrence that landed *after* that row's `occurrence_mark` was stamped. A real
recurrence, reopened exactly as intended, and it re-clustered into a would-be
`investigate` for `hermes-agent`. The other census delta is event 260, a fresh
ingest. Pass 2 is the churn measurement, and it is 0.

Mutation-tested, each broken → red **by name** → restored → green:

| Mutation | Caught by |
|-|-|
| `resolve_recovery_paired()` → `fixed` | `test_recovery_paired_never_produces_fixed` |
| `maybe_check_liveness()` positive → `quiet` | `test_liveness_confirmed_resolves_the_item` |
| `merged` expiry → `quiet` | `test_merged_expires_to_closed` |
| drop the `from_state != to_state` guard | `test_set_state_records_transitions_only_on_real_change` |
| `sync_card()` guard back to `fixed` only | `test_sync_card_never_posts_a_first_card_for_any_never_carded_terminal_state` |

`grep -rn "triage_items" hermes-agent/{scripts,plugins}` returns only
`hermes-cc.sh`'s `--auto-from-item`, which gates on `verdict`. **No dependency on
the literal `resolved` exists anywhere in hermes-agent** — verified, not assumed.

### Expect a one-time card burst on the first live pass

~20 clusters render differently now (`resolved` / `:white_check_mark:` →
`quiet` / `:mute:`), so `card_hash` will miss and each already-carded cluster
gets one `chat.update`. **Updates, never posts** — `sync_card()`'s never-carded
guard holds, so nothing new appears in the channel. One-time, and it is the
correct behaviour: the cards were claiming something the ledger no longer says.

### Process, carried over from §38

The four LaunchAgents were **unloaded for the whole slice** (`make unload`),
because in this repo the agents execute `scripts/*.py` from the working tree and
this change renames a live state value. §38 learned that the hard way with an
additive migration; this one did not repeat it.

### Next action

Reload the agents and let the loop migrate the live ledger to 4, then slice 2.3 —
`/metrics`.

---

## 40. Wave 2, item 3 — `/metrics` (DONE & LIVE, no schema change)

A fifth LaunchAgent, `com.jkrumm.warden-api`, serving `GET /metrics` and
`GET /health` on `127.0.0.1:7734`. `scripts/api.py`, 473 lines, stdlib only.

**Only those two endpoints.** `/board`, `/items/:id`, `POST /items/:id/intent`
and `POST /items/:id/note` are `DESIGN.md`'s full § HTTP API contract and are
Wave 4, with Argo — their only consumer. Unconsumed surface has nothing to prove
it correct.

### The design decisions worth keeping

- **One fresh read-only connection per request**, not a handle held for the
  process's life. A WAL reader pinned at startup keeps serving the snapshot it
  opened with, and a schema assertion made once at boot says nothing about the
  ledger after the loop migrated it. A failed assertion is a **503 with the
  mismatch in the body** — never a 200 built on a connection the process could
  not trust.
- **`ledger.connect(readonly=True)` does NOT assert the schema version.** My
  brief claimed it did; the worker checked and it was false — the readonly branch
  opens `mode=ro` and returns. `api.py` calls `assert_schema_version()` itself,
  per request. Worth recording as a fact about `ledger.py`, not just about this
  slice.
- **Loopback bind, no auth, and that is not laziness.** Per `DESIGN.md` §
  Security model an episode on this host runs unrestricted `Bash`; a bearer
  token would be theatre, because whatever can read the token can also query the
  socket. The boundary is the bind plus the read-only handle. **No Caddy/tailnet
  door this wave** — that belongs with Argo, the only intended remote consumer.
- **JSON, not Prometheus text**, despite the path name. The named consumer is
  Argo and the six numbers are a funnel snapshot, not a time series.

### The honesty rules, which are the actual product here

Every metric is `{"value": …, "unavailable": …}`. `value` is `null` **only**
paired with a non-empty reason, and **never a fabricated `0`**. Two of the six
come back `null` today, both for reasons that are facts about the ledger:

| Metric | Why `null` |
|-|-|
| verified **unattended** fixes/week | `dispatch_approvals` has no `event_id` and no item link of any kind — 5 rows, 1 with `argv_json`, **0** carrying `--auto-from-item`. An approval cannot be attributed to the item it fixed. That link is the operation id from `DESIGN.md` § Crash recovery, i.e. Wave 3. The raw `fixed`-transition count and `approvals_spent_in_window` are served alongside, explicitly labelled as not attendance-filtered. |
| reverts | No revert primitive exists (`warden revert`, Wave 3). A `0` here would read as "measured, none happened". |

And a top-level **`history_since`** = `MIN(item_transitions.at)`. The table was
created empty and records nothing retroactively, so any window starting before
that timestamp is empty *by construction*, not a measured zero. Without this
field `/metrics` would confidently report "0 fixes this week" for a week the
table did not exist in.

### Poller heartbeats — the one thing that had to touch two live scripts

Metric 5 was unmeasurable: only the loop wrote a heartbeat. `watchdog-poll.py`
and `dispatch-sweep.py` now each write one unconditional `cursors` row per
**completed** pass (`watchdog_poll_last_run`, `dispatch_sweep_last_run`),
mirroring `triage.py`'s `record_heartbeat()` and skipped under `--dry-run` for
the same reason. The sweep's is stamped at the *end* of the pass, not the start —
the cursor answers "when did this last COMPLETE", which is the question a
staleness threshold asks.

Thresholds are `3 ×` each agent's own `StartInterval`, named as constants with
the plist they came from in the comment: loop 600s → 30 min, poll 1800s → 90 min,
sweep 300s → 15 min.

### Verified, not claimed — against the live system

```
$ make status
    ✓ com.jkrumm.warden-loop  [pid -, last exit 0]
    ✓ com.jkrumm.warden-poll  [pid -, last exit 0]
    ✓ com.jkrumm.warden-sweep  [pid -, last exit 0]
    ✓ com.jkrumm.warden-backup  [pid -, last exit 0]
    ✓ com.jkrumm.warden-api  [pid 27968, last exit 0]
  api (/health)            ✓ reachable, ok
  policy                   ✓ both copies agree on all 30 repos

$ make test
  test_api.py                      18/18 passed
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   16/16 passed
  test_triage.py                   107/107 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_locking.py         3/3 passed
  test_watchdog_slack_blindness.py all cases as expected

$ curl -s 127.0.0.1:7734/nope     -> 404
$ curl -s -XPOST 127.0.0.1:7734/metrics -> 405
$ curl -s 127.0.0.1:7734/health   -> ok:true, schema_version 4 == expected 4,
                                     all three pollers under threshold
```

**The real `/metrics` payload, live:**

```
verdicts -> recorded disposition   5/17 = 0.294   states {needs_human: 6, quiet: 10}
verified fixes vs silence          0.0            fixed 0 · quiet 10 · closed 0 · dismissed 0
median needs_human -> decision     null           no pairs in window yet
verified UNATTENDED fixes/week     null           fixes_in_window 0, approvals_spent 1
poller ages (max)                  0.29 min       loop / watchdog_poll / dispatch_sweep all live
reopen_after_fixed                 0              history_since 2026-09-09T19:43:30Z
reverts                            null           no primitive
```

**The two numbers this whole project exists to move are now readable, and they
are bad, which is the point.** Ten of the seventeen verdicts with a recorded
disposition went to `quiet` — silence, which `DESIGN.md` says is never an
outcome. Zero verified fixes against ten silence closes. Before this wave neither
number could be computed at all.

### The chain ran end to end while this was being verified

`item_transitions` recorded it, unprompted, which is the best evidence the
history table works:

```
261  quiet         -> new            19:43:30Z
260  new           -> investigating  19:43:30Z
261  new           -> investigating  19:43:30Z
260  investigating -> needs_human    19:48:30Z  "Not fixable from this repo. On the mini, check the slack_sdk…"
261  investigating -> needs_human    19:48:30Z  "Not fixable from this repo. On the mini, check the slack_sdk…"
```

A recurring `hermes_log` signal reopened, clustered, dispatched a real
`investigate` episode against `hermes-agent`, and folded a verdict to
`needs_human` with concrete remediation — in five minutes, unattended. Note the
honest attribution: event 261 would also have reopened under the *old* predicate
(its quiet anchor was fresh, so `resolve_quiet_grouped()` would not have
re-closed it that pass), so this is not a fix-attributable escalation. What is
new is that **the ledger can now say it happened.**

### Out-of-scope edit made deliberately

`dotfiles/scripts/log-rotate.sh` gained `warden-api.{log,err}`. The plist
template's comment claims rotation is "declared in" that file, and the file
enumerates warden's logs by name rather than globbing — so without the entry the
comment was false and the log would grow forever. It is also the only warden log
that grows **per request** rather than per scheduled pass
(`BaseHTTPRequestHandler` logs every GET), which is negligible while `make
status` is the only caller and is not once Argo polls it.

### Next action

The Wave 2 roll-up against the stop condition, then a fresh reviewer with no
context from this session.

---

## 41. Wave 2 — final state, against the stop condition

Item by item, with the evidence. Wave 1 nearly shipped without this section and
its reviewer correctly called that its most consequential defect.

| # | Condition | Status | Evidence |
|-|-|-|-|
| 1 | A grouped item no longer reopens every pass, proven by running the loop twice and observing no churn | **MET** | `b13705b`. Two `run()` passes on a `VACUUM INTO` copy of the live ledger: **46 → 0** transitions per pass, census identical. Two consecutive live ticks (18:55 → 19:05Z) changed **0** rows' state/mark/note. §38. |
| 2 | `resolved` split into `fixed`/`quiet`/`closed`, the live ledger's 30 rows migrated, each landing in a defensible bucket | **MET** | `d601047`, schema 4. Live: 0 rows in `resolved`, 29 `quiet`. All 30 → `quiet`, defended in `_MIGRATION_4`'s comment and §39: every one is a silence close by note or by cleared note, `RECOVERY_PAIRED_NOTE_PREFIX` on zero rows. |
| 3 | `/metrics` serves the six funnel numbers from a read-only handle | **MET** | `639e9e5`. Live payload in §40. `ledger.connect(readonly=True)` + an explicit per-request `assert_schema_version()`; mismatch → 503. Two of the six serve `null` **with a reason** rather than a fabricated 0 — see below, this is deliberate and is not a partial. |
| 4 | `make test` green with a count you can account for | **MET** | Accounted below. |

### Item 4 — the count, accounted for

| Suite | Count | Delta this wave |
|-|-|-|
| `test_triage.py` | **107** | 96 + 6 (slice 2.1, the occurrence mark) + 5 (slice 2.2, the split) |
| `test_ledger.py` | **16** | 12 + 2 (migration 3) + 2 (migration 4) |
| `test_api.py` | **18** | new file, slice 2.3 |
| `test_intents.py` | 19 | unchanged |
| `test_watchdog_locking.py` | 3 | unchanged |
| `test_dispatch_sweep.py`, `test_watchdog_delivery.py`, `test_watchdog_slack_blindness.py` | "all cases as expected" | unchanged |
| hermes-agent `test_hermes_cc.py` / `test_dispatch_approval.py` | 165 cases / 83 checks | unchanged, re-run green after both pin bumps |

Fourteen mutations across the three slices, each broken → red **by name** →
restored → green. The full table is in §38, §39 and §40.

### Item 3 — why two `null`s are the condition being MET, not missed

`DESIGN.md` asks for six numbers. Two of them describe things the ledger cannot
currently know, and both reasons are structural, not effort:

- **"unattended"** needs an approval-to-item link. `dispatch_approvals` has no
  `event_id`; 0 of 5 rows carry `--auto-from-item`. That link is the operation id
  `DESIGN.md` § Crash recovery specifies, which is Wave 3.
- **reverts** needs `warden revert`, which `DESIGN.md` § Abort and revert also
  places in Wave 3.

Serving `0` for either would be a fabricated measurement in the exact funnel this
design says must not be gameable (`REVIEW.md` C3). Both serve `null` with a
machine-readable reason and their raw ingredients alongside. Building the
missing primitives to make them non-null would be starting Wave 3.

### What the numbers actually say, now that they can be read

```
verdicts reaching a recorded disposition   5 / 17   (10 went to `quiet` — silence)
closes that are verified fixes vs silence  0 / 10
```

`DESIGN.md` § What "done" means targets 11/11 and "verified is the majority".
Neither number could be computed before this wave. Both are now on an endpoint,
and both are bad — which is the point of measuring them.

### Inherited known-open items, dispositioned

The handover named four. Two are closed, two are not, deliberately.

| # | Item | Disposition |
|-|-|-|
| 1 | `sync_card()`'s never-carded guard covered only `resolved`, so a `dismissed` row lacking `card_ts` would post a first card | **CLOSED** (§39). `_NEVER_CARDED_FIRST_STATES = (fixed, quiet, closed, dismissed)`, with a test per state. |
| 2 | A forged spool file blocks a real approval click and Slack renders "already decided" instead of "superseded" | **NOT DONE.** It lives in `hermes-agent/plugins/dispatch-approval`, not warden, and it is a label on a correct denial — the deny itself is right. Named here so it is not lost; it belongs with the next hermes-agent plugin change, not bolted onto a warden wave. |
| 3 | `needs_human`'s "reminder at 1d" | **STILL DELIBERATELY UNBUILT.** A reminder is a notification, not a deadline. Unchanged from §35. |
| 4 | `DESIGN.md` § 365's Argo Postgres read-cache justification does not survive its own objection | **UNTOUCHED, correctly** — the handover puts it in Wave 4. |

### New known-open, created or found by this wave

1. **`hermes_log` has no `ts_last`.** A cooldown-suppressed occurrence on that
   source moves nothing in `_occurrence_mark`, so such a row does not reopen
   until the 24h cooldown emits. Not a regression (§38 shows why) and written
   into `docs/triage.md` beside the self-tuning quiet window, which is where it
   belongs.
2. **The self-tuning quiet window is now UNBLOCKED** and still open. It was
   blocked only on the reopen churn being invisible; there is no churn left to
   uncover. `docs/triage.md` § brain-sync.
3. **`ledger._verify_columns()` still checks only the original five tables**, so
   `item_transitions`' shape is created by the migration but not asserted by the
   adoption path. Found by the slice-2.2 worker. Harmless today; worth folding
   into whichever migration next needs that guarantee.
4. **`/metrics` metric 3 loads all of `item_transitions` into memory** to pair
   entries with exits. Correct and trivial at present volume; it is an O(n) read
   that will want a windowed query long before it is a problem.

### Two process facts this wave established, for the next handover

- **There is no staging window in this repo.** The LaunchAgents execute
  `scripts/*.py` from the working tree and `~/.hermes/scripts` symlinks into
  `hermes-agent`, so an uncommitted edit is in production the moment it is saved.
  Slice 2.1 learned this by having the live ledger migrate itself three minutes
  after the edit landed (§38); slices 2.2 and 2.3 ran `make unload` first, or
  confined edits to files no agent runs.
- **`ledger.connect(path, readonly=True)` does not assert the schema version.**
  Its readonly branch opens `mode=ro` and returns. Any read-only consumer must
  call `assert_schema_version()` itself. `api.py` does, per request.

### Wave 2 is closed to the stop condition. Not started, on purpose

Wave 3 is abort, revert, the per-repo in-flight lock, and crash reconciliation
with `unknown` — plus the operation id that makes `/metrics`' "unattended"
number derivable. **Do not roll into it from here.**

---

## 42. Wave 2 — the boundary review, and the seven things it found

A fresh reviewer with **no context from the session that did the work** checked
the diff and the running system against `DESIGN.md` and `FLOWS.md`. It re-derived
the headline claim with its own harness, re-ran twelve of the wave's mutations,
and recomputed `/metrics`' arithmetic by hand against its own read-only queries.

It confirmed all four stop-condition items, **and found seven defects — three of
them in claims this file made.** §§37-41 stay as written; corrections are here,
because the corrections are the useful part.

### Its independent re-derivation of the churn fix

```
AFTER  (its own snapshot @ schema 4, HEAD code)
  pass 1: transitions = 1  {'new -> quiet': 1}
  pass 2: transitions = 0  {}
BEFORE (same snapshot downgraded to schema 2, code from 8906d06)
  pass 1: transitions = 45 {'resolved -> new': 22, 'new -> resolved': 23}
  pass 2: transitions = 44 {'resolved -> new': 22, 'new -> resolved': 22}
```

44-45, not §38's 46, because the live ledger moved on between the two snapshots
(events 260/261 left the quiet set). Same magnitude, same mechanism, zero after.
It also confirmed `_SILENCE_RESOLVE_ELIGIBLE_STATES` is still `(STATE_NEW,)` and
that widening it fails 5 tests by name.

### The seven defects and what happened to each

| # | Defect | Disposition |
|-|-|-|
| 1 | **`/metrics` metric 1 divided dispatch counts while breaking them down by ITEM counts**, and 10 of its 17 denominators were interactive Slack dispatches (`origin_event_id IS NULL`, `origin_channel`/`origin_thread_ts` set) that were never items in warden's funnel and can never enter the numerator. | **FIXED** (`b2866b2`). Denominator gated on `origin_event_id IS NOT NULL`; `excluded_interactive` disclosed with a reason; `states` → `item_states` with a note that its total may exceed the denominator. Live: **5/7 = 0.714**, 10 excluded. |
| 2 | **Two leaves served a fabricated `0`** — `reopen_after_fixed` and `fixes_in_window` on an empty `item_transitions` — which `api.py`'s own bolded rule forbids. `history_since` was advisory; a consumer had to remember to cross-reference it. | **FIXED.** `_history_guard_reason()` at the **leaf**: empty table, or a window starting before `history_since`, serves `null` with a reason naming it. Live, all three history-derived leaves are `null` and will be for seven days. That is the correct output. |
| 3 | **The read-only property was untested and a test docstring lied about it.** Mutating `_serve()` to a writable `connect()` left both read-only tests green; `test_handler_connection_refuses_an_insert` opened its own hardcoded connection and could never observe the handler's call site. | **FIXED.** `test_serve_opens_connection_with_readonly_true` monkeypatches `api._ledger.connect` and observes `_serve()`'s own call. False docstring replaced. The socket test's fixed sleep became a bounded retry that tears the server down on failure — a gate test that can fail for unrelated reasons is its own defect. |
| 4 | **`DESIGN.md` contradicts itself about metric 2's numerator**, and the wave picked a side silently. | **ESCALATED AND RESOLVED BY THE OPERATOR** — see below. |
| 5 | **A second reopen-latency class §41 did not list**: state sources with a continuously-open event, bounded by `REM_HOURS` — 6h for `uk`, 72h `github_pr`, **168h** `github_issue`/`stray_skill`. | **DOCUMENTED** in `docs/triage.md`, both ways: under the old predicate such a row reopened next pass, which meant `dismissed` was *effectively unreachable* for a state source that never clears — the 7-day fuse could never retire anything. The bound is the price of a `dismissed` that works. |
| 6 | **Two of three heartbeats stamped the pass's START** while their comments and `api.py`'s thresholds treat the cursor as "when did this COMPLETE". `triage.py`'s docstring had said the false thing since before this wave. | **FIXED, structurally.** `record_heartbeat()` now takes **no timestamp** in any of the three scripts and reads the clock at the write. Three call sites each remembering to pass a fresh clock is three chances to pass the stale one; removing the parameter makes it unexpressible — the same argument `_set_state()` makes for owning `state_deadline`. |
| 7 | **`item_transitions` has no birth row** — `ingest()` INSERTs `STATE_NEW` directly rather than through `_set_state()`, so entry into `new` is never recorded. | **DOCUMENTED** in `docs/api.md`. It is history *after* the first state. Affects none of the six numbers. |

### Defect 4 — the contradiction, and the decision

Three passages, all real, that cannot all hold:

- `DESIGN.md:59` — *"Closes that are verified fixes vs. silence | **2 / 28**"*.
- `DESIGN.md:27` — sources those 2 as *"closed on an observed recovery message"*,
  and `REVIEW.md:54` names them as items 931/932 via `resolve_recovery_paired()`.
- `DESIGN.md:547` — *"Recovery-pairing is the strong path, the 2h timer the
  fallback, and **neither ever claims a fix**."*

So the document's own baseline counted, in the "verified fixes" numerator, closes
its own must-not-be-lost list forbids from claiming a fix. §39 cited `:547`
correctly and never mentioned the two passages that say the opposite.

**Decided by the operator, 2026-09-09: amend the document** (`e7d5be5`). Row 2 is
now `0 / 28`, with the reasoning and — more importantly — the *consequence*
stated in `DESIGN.md` itself: `fixed` has exactly one producer
(`maybe_check_liveness()`'s positive branch), reachable only through the
merge → deploy → liveness chain, and **zero rows have ever carried a liveness
confirmation**. Metric 2 therefore reads 0 and **cannot move until at least one
repo has a deploy target and a liveness probe** — Wave 3+. That is a real
constraint on the project's headline number, and it is now written where a reader
meets the number rather than discovered from a dashboard reading zero.

### Corrections to what §§37-41 asserted

Own errors, listed because this file is checkable or it is nothing.

- **§40 and §41 both misread metric 1's own payload.** §40 said *"ten of the
  seventeen verdicts with a recorded disposition went to `quiet`"* — wrong twice
  over: the 10 is a count of **items** (belonging to 3 distinct dispatches), and
  `quiet` is by `api.py`'s own definition **not** a recorded disposition. §41
  repeated the 5/17 framing. The true figure is **5/7**, and the payload shape
  that invited the misreading is defect 1, now fixed. This is the sharpest lesson
  of the review: the orchestrator misread its own instrument, in exactly the way
  the instrument's shape encouraged.
- **§37 said "`DESIGN.md` § Why this exists and `REVIEW.md` § Facts corrected both
  name items 931 and 932".** `grep -n "931\|932" DESIGN.md` returns nothing —
  `DESIGN.md` describes the two closes without naming ids; only `REVIEW.md` names
  them. Pedantic, and exactly the kind of claim this file exists to make
  checkable.
- **§38 said "11 of the 30 stamped rows carry an empty `ts_last` slot; 6 are
  `hermes_log`"** and then dropped the other 5 without saying what they were.
  They are `uk` — a whole second source class, which is defect 5. Live now: 13 of
  32, `hermes_log` 7 + `uk` 6.
- **§40's read-only guarantee overstated its test coverage.** The code was
  correct; nothing stopped it regressing (defect 3). §41 item 3's evidence line is
  true of the code and was not true of the tests behind it.
- **§40 said `history_since` prevents "0 fixes this week" reading as a
  measurement.** It did not — it moved the problem somewhere a consumer had to
  remember to look (defect 2).

### Amended roll-up — the stop condition after remediation

| # | Condition | Status |
|-|-|-|
| 1 | Grouped item no longer reopens every pass, proven by two passes | **MET** — independently re-derived, 45 → 0 |
| 2 | `resolved` split, 30 rows migrated, each in a defensible bucket | **MET** — 0 rows in `resolved`; the one qualification (defect 4) resolved by amending `DESIGN.md` |
| 3 | `/metrics` serves the six funnel numbers from a read-only handle | **MET** — the three qualifications (defects 1, 2, 3) all fixed; read-only now actually tested |
| 4 | `make test` green with a count you can account for | **MET** — `test_triage.py` **108** (107 + the heartbeat regression), `test_api.py` **23** (18 + 5), `test_ledger.py` 16, `test_intents.py` 19, `test_watchdog_locking.py` 3, three "all cases" suites |

Live after remediation:

```
$ make status
    ✓ warden-{loop,poll,sweep,backup}  [last exit 0]
    ✓ com.jkrumm.warden-api  [pid 10635, last exit 0]
  api (/health)            ✓ reachable, ok
  policy                   ✓ both copies agree on all 30 repos

$ curl -s 127.0.0.1:7734/metrics
  metric1  5/7 = 0.714   excluded_interactive 10   item_states {needs_human: 6, quiet: 10}
  metric2  0.0           fixed 0 · quiet 11 · closed 0 · dismissed 0
  metric3  null          window start predates history_since (2026-09-09T19:43:30Z)
  metric4  null          fixes_in_window null (same guard) · approvals_spent 1
  metric5  0.31 min      all three pollers live
  metric6  reopen null (same guard) · reverts null (no primitive)
```

Five mutations re-run on the remediation, each red **by name**, each restored:
`test_metric1_excludes_interactive_dispatches_and_item_states_can_exceed_denominator`,
`test_metric6_reopen_after_fixed_is_null_not_zero_when_history_is_young`,
`test_serve_opens_connection_with_readonly_true`,
`test_heartbeat_is_stamped_when_the_pass_ENDS_not_when_it_began` (verified here,
by re-adding the timestamp parameter and threading the pass's `now` back through
it — 107/108, red by name, restored), and the F1 denominator drop.

### One thing the review could not check, and one it deferred

- The ~20 one-time `chat.update` calls §39 predicted require reading the Slack
  channel. `slack_update_ts` moved at 19:43:30Z, consistent with it; neither the
  count nor "updated, never posted" is confirmed from here.
- Defect 5's latency bound is documented, not fixed. It belongs with the
  self-tuning quiet window, which is the same subject and is now unblocked.

**Wave 2 is closed.** Wave 3 is abort, revert, the per-repo in-flight lock, and
crash reconciliation with `unknown` — plus the operation id that makes
`/metrics`' "unattended" number derivable and the deploy target that lets metric 2
ever be non-zero. Do not roll into it from here.

---

## 43. First night in production (2026-09-09 21:00Z → 2026-09-10 08:10Z)

Wave 2 closed at 21:00Z. Eleven hours of real traffic later, checked against the
running system. **The wave holds. It also caught a real defect, which is the
point of having built the instrument.**

### The churn fix, measured against production rather than a snapshot

```
loop passes in the window (600s interval, ~11.2h):   ~67
item_transitions recorded in the window:              18
```

Eighteen. Every one of them a real state change (they are enumerated below).
Under the pre-Wave-2 predicate the same window would have produced roughly
**67 × 46 ≈ 3,100** transitions, all of them noise, each one overwriting a note.
That is the fix, measured on the live system rather than a copy.

All five agents ✓ with last exit 0. `warden-{poll,sweep}.err` 0 bytes,
`warden-backup.err` 0 bytes at 03:10 (the nightly `VACUUM INTO` ran).
`warden-loop.err` grew by 346 bytes — three benign lines
(`uk:175/uk:185 recurred inside cooldownHours, not re-escalating yet`) and
`propose_mappings — model call failed: HTTP Error 403`, which is the same
incident described below, handled and logged rather than crashed.
`make test` green: `test_triage.py` 108/108, `test_api.py` 23/23,
`test_ledger.py` 16/16. `make check-policy` ✓ 30 repos.

### The chain ran twice, unattended, on two real incidents

**Incident A — the 403 cost limit (06:10Z).** The IU LLM gateway returned
`access_denied: rolling-30-day-cost-service-denial-limit` during the 07:01 local
`Morning briefing` cron. It surfaced at three layers of one call stack and
arrived as three `hermes_log` signatures plus a `hermes_cron` row:

```
06:10:15  ev968/969/970  new -> investigating     (one cluster, one episode)
06:14:49  ev968/969/970  investigating -> needs_human
          note: "Check the IU endpoint's cost/billing dashboard …"
```

Dispatch 26's verdict: *"One root cause, not three… it's an error body from the
external IU endpoint itself."* Correct, clustered correctly, carded, and now
sitting in `needs_human` with a 2026-09-17 deadline. **Four and a half minutes
from signal to a recorded, actionable disposition, with no human in it.** That is
the pipeline this project was built for, working.

**Incident B — the one that got lost. See below.**

### FINDING — the cluster-dissolve edge launders a verdict into a state where silence may discharge it

The sharpest thing this night produced, and it belongs to Wave 3.

```
21:09:33  ev2 (uk:175) + ev815 (uk:185)   quiet -> new        (both recurred)
21:09:33                                  new -> investigating (clustered, dispatch 25)
21:14:32                                  investigating -> verdict
21:19:37                                  verdict -> new       <- CLUSTER DISSOLVE
21:29:39  events' resolved_at set          (both monitors recovered)
21:39:39                                  new -> quiet         <- apply_resolutions
```

Dispatch 25's verdict was **substantive and correct**:

> *"These two alerts do NOT share a root cause — UNRELATED SIGNATURES. uk:175
> ("Hermes Agent" push) is caused by a real, currently-active bug: Hermes's own
> Socket Mode watchdog in `plugins/platforms/slack/adapter.py`
> (`_restart_socket_mode`/`_stop_socket_mode_handler`) races with slack_sdk's
> `SocketModeClient.connect()`, whose `while True` retry loop "never checks
> `closed`" (adapter.py's own comment) — so when the watchdog closes the old
> aiohttp session to restart, the still-running orphaned old task keeps hitting
> the closed session forever…"*

Where that verdict is now: `dispatches.verdict_json`. **Nowhere else.** Both items
are `quiet`, `note` is `NULL` (`apply_resolutions()` clears it), and
`card_ts` is `NULL` — `_dissolve_cluster()` clears the card pointers, so
`sync_card()`'s never-carded guard correctly declined to post a final card for a
terminal item nobody was told about. The last thing the channel saw was
*"Cluster split — re-evaluating individually"*, which was not true: neither was
re-evaluated.

**Why it happened, mechanically.** Three correct behaviours compose into the
founding defect:

1. `_dissolve_cluster()` sends every member back to **`new`** (DESIGN.md's own
   lifecycle: `verdict -> new (UNRELATED SIGNATURES, cluster dissolve)`),
   deliberately keeping `dispatch_job` as a cooldown anchor — must-not-be-lost
   item 2, and its docstring is explicit that without it the same `run()` would
   instantly re-fuse the pair.
2. `escalate()` then declines: `uk:175 recurred inside cooldownHours, not
   re-escalating yet` — the very anchor that makes (1) work.
3. The monitors recovered inside that cooldown window, so `apply_resolutions()`
   closed both as `quiet`. Which is correct: **`new` is the one state silence is
   allowed to discharge.**

`_SILENCE_RESOLVE_ELIGIBLE_STATES = (STATE_NEW,)` is not wrong. **The dissolve
edge launders an item that carries an obligation into the one state that carries
none.** Wave 1 closed the direct path (`needs_human` can no longer be
silence-resolved) and this is the same failure arriving through the side door —
DESIGN.md's opening sentence, *"A correct verdict has nowhere to go,"* reproduced
in production eleven hours after the wave that was supposed to be measuring it.

`_dissolve_cluster()`'s docstring already concedes half of this: *"the split
verdict stays visible in `dispatches.verdict_json` for whoever reads the
history."* Nobody reads `dispatches.verdict_json`. That is what the ledger is for.

**The instrument scored it correctly, which is the one piece of good news.**
`/metrics` metric 1 reads **6/9** this morning, not 7/9: dispatch 26 (→
`needs_human`) counts, dispatch 25 (→ `quiet`) does not, because `quiet` is not a
recorded disposition. The number this project exists to move detected its own
worst case on day one without being told to.

**Do not fix this with a patch.** Carrying the verdict text into each member's
`note` does not survive — `apply_resolutions()` clears `note` precisely because a
`new` row is supposed to carry no obligation. The honest fix is that a dissolved
member is **not `new`**: it is a distinct state that has been evaluated, carries a
verdict, and is not silence-eligible until it has been re-evaluated
individually. That is a state-machine change and it belongs with Wave 3's
obligation/reconciliation work, next to `unknown`.

### Live numbers this morning

```
census        quiet 30 · needs_human 9 · note 8 · ignored 7 · new 3
metric1       6/9 = 0.667   excluded_interactive 10   item_states {needs_human: 9, quiet: 11}
metric2       0.0           fixed 0 · quiet 11 · closed 0 · dismissed 0
metric3/4/6   null          window still predates history_since (2026-09-09T19:43:30Z) until 09-16
metric5       11.2 min max  watchdog_poll, threshold 90 — all three pollers live
```

All nine `needs_human` rows are carded and carry 7-day deadlines
(4× `2026-09-16T17:34`, 2× `2026-09-16T19:48`, 3× `2026-09-17T06:14`).

### Two things for a human, not for warden

- **`hermes-agent` has a real active bug** — the Socket Mode watchdog /
  `slack_sdk` `SocketModeClient.connect()` race quoted above. Warden diagnosed it
  correctly and it is a genuine hermes-agent fix, sitting unclaimed.
- **The IU endpoint hit its rolling-30-day cost limit**, which is what broke the
  morning briefing and `propose_mappings`. Operational, external, and outside
  anything warden can act on.

---

## 44. Wave 3 — reconnaissance against the running system (2026-09-10, ~08:19Z)

Read-only. No edits, no restarts, no writable handle on the live ledger. Every
number below was produced by a command in this section, not read out of §43.

### The five agents, the suites, the two endpoints

```
$ make status
  venv                     Python 3.11.15
  agents:
    ✓ com.jkrumm.warden-loop  [pid -, last exit 0]
    ✓ com.jkrumm.warden-poll  [pid -, last exit 0]
    ✓ com.jkrumm.warden-sweep  [pid -, last exit 0]
    ✓ com.jkrumm.warden-backup  [pid -, last exit 0]
    ✓ com.jkrumm.warden-api  [pid 10635, last exit 0]
  api (/health)            ✓ reachable, ok
  policy                   ✓ both copies agree on all 30 repos
  ledger                   1.0M Sep 10 10:14

$ make test                                      # exit 0
  test_api.py                      23/23 passed
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   16/16 passed
  test_triage.py                   108/108 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_locking.py         3/3 passed
  test_watchdog_slack_blindness.py all cases as expected

$ make check-policy
  dispatch policy — 30 repos under /Users/jkrumm/SourceRoot
  ✓ both copies agree on all 30 repos
  notes: sideclaw also admits roots hermes-cc.sh never uses: ['/Users/jkrumm/IuRoot']

$ grep -c 'UPDATE triage_items SET state=' scripts/triage.py
1
$ pgrep -f triage.py
(none — no second loop)
$ git status --porcelain
(clean, at 12d8020)
```

Every count matches the handover's pinned table exactly. `test_triage.py` is
**108/108**, the regression gate.

`/health` — `ok: true`, `schema_version` 4 == expected 4, all three named pollers
under threshold (loop 8.3/30 min, watchdog_poll 17.3/90, dispatch_sweep 4.0/15).

`/metrics`, live at 08:18:52Z:

```
metric1  verdicts -> recorded disposition   6/9 = 0.667   excluded_interactive 10
                                            item_states {needs_human: 9, quiet: 11}
metric2  verified fixes vs silence          0.0   fixed 0 · quiet 11 · closed 0 · dismissed 0
metric3  median needs_human -> decision     null  window predates history_since
metric4  verified UNATTENDED fixes/week     null  no operation id; approvals_spent_in_window 1
metric5  poller ages (max)                  17.28 min (watchdog_poll, threshold 90)
metric6  reopen_after_fixed null (history guard) · reverts null (no primitive)
```

Four of six `null`, each with a reason naming a fact about the ledger rather than
a fabricated `0`. Three of those four clear on their own on 2026-09-16 when the
7-day window stops predating `history_since`; the other two (metric 4's
`unattended` qualifier, metric 6's `reverts`) are Wave 3 items 1 and 2.

### The ledger, verified against my own read-only queries

`sqlite3.connect("file:…?mode=ro", uri=True)` throughout.

| Check | Result |
|-|-|
| `schema_version` | **4**, `applied_at` 2026-09-09T19:43:30.582326Z |
| `journal_mode` / `quick_check` | `wal` / `ok` |
| File mode | `-rw-------` (600), 1,077,248 bytes |
| Rows still in `resolved` | **0** |
| Census | `quiet` 30 · `needs_human` 9 · `note` 8 · `ignored` 7 · `new` 3 = 57 |
| Non-terminal rows with a NULL `state_deadline` | **3, all `new`, and that is correct** — see below |
| `needs_human` rows with a deadline and a card | **9 / 9 / 9** |
| Schema pins | `ledger.py SCHEMA_VERSION = 4` == `hermes-cc.sh WARDEN_SCHEMA_VERSION="${…:-4}"` |

**The three NULL deadlines are by design, not the Wave 1 defect returning.**
`STATE_DEADLINES[STATE_NEW]` is `_DeadlineRule("resolve_quiet_grouped /
apply_resolutions", None, None, None, None)` — `new` is bounded by silence, not by
a clock, and a `deadline_column` of `None` is what says so. The three rows are
`uk` group monitors (`Local`, `VPS`, `Services`) with `repo = NULL`: the unmapped
parents from `DESIGN.md` § Open questions 3. Unmapped means they never escalate,
which is why they have sat in `new` since Wave 0 without a card.

### Transitions against loop passes — the churn has not come back

The handover's sharpest recon question. Counted, not assumed:

```
item_transitions rows                     24
first / last                              2026-09-09T19:43:30Z / 2026-09-10T06:14:49Z
elapsed                                   12h 27m  ->  ~75 loop passes at 600s
transitions per pass                      0.32
```

Under the pre-Wave-2 predicate the same span would have produced ~75 × 45 ≈ 3,400.
By edge:

```
new -> investigating   7      quiet -> new      4      verdict -> new          2
investigating -> needs_human 5  new -> quiet    4      investigating -> verdict 2
```

Every one accounted for by §40 (ids 1-5), §43 incident A (ids 19-24) and §43's
dissolve incident (ids 7-16). Ids 17/18 are `ev310` reopening at 00:09:48Z and
re-quieting at 00:39:50Z — a real `quiet -> new -> quiet` on a recurrence that
went silent again inside 30 minutes, the intended behaviour of the one state
silence may discharge.

### The logs — checked by mtime and content, not by size

```
warden-{poll,sweep,backup}.err   0 bytes
warden-api.err                   1107 bytes, ALL of it BaseHTTPRequestHandler
                                 access logging (200s, one 405 on POST /metrics)
warden-loop.err                  30051 bytes, mtime 2026-09-09T22:39Z local 00:39
```

`warden-loop.err` has not been written to in 9.6 hours despite ~57 loop passes
since. Its newest lines are the two benign `uk:175/uk:185 recurred inside
cooldownHours` notices and `propose_mappings — model call failed: HTTP Error 403:
Forbidden`. **That 403 is not a live fault and will not retry until tonight**:
`propose_mappings()` is on a 24h budget whose cursor
(`triage_propose_mappings_last_run`) is stamped *before* the call precisely so a
failure counts against the day's attempt rather than hammering the endpoint every
ten minutes. Cursor reads `2026-09-09T22:39:42Z`; next attempt ~22:39Z tonight.
The cause is §43's external one — the IU endpoint's rolling-30-day cost limit.

### Backup — the nightly run happened, and the bytes left the box

```
$ cat ~/Library/Logs/warden-backup.log
snapshot /Users/jkrumm/.warden/backups/warden-20260910T011000Z.db (1.0M)
$ ssh homelab 'ls -la /mnt/hdd/backups/warden/'
-rw------- 1 jkrumm jkrumm 1064960 Sep 10 01:09 warden.db
```

`warden-backup.err` 0 bytes at 03:10 local. Still **no restore path** — unchanged
and still tracked.

### The item-0 defect is still sitting in the ledger, exactly as §43 described it

Re-derived independently rather than taken from §43:

```sql
SELECT event_id FROM item_transitions WHERE from_state='verdict';   -- 2, 815
```

```
ev2   (uk:175)  state=quiet  note=NULL  card_ts=NULL  dispatch_job=09bf0c14…
ev815 (uk:185)  state=quiet  note=NULL  card_ts=NULL  dispatch_job=09bf0c14…
dispatches.verdict_json → "These two alerts do NOT share a root cause — UNRELATED SIGNATU…"
```

Two items, one substantive verdict, `note` and `card_ts` both NULL on both rows.
The verdict exists only in `dispatches.verdict_json`, which nothing reads. This is
Wave 3 item 0 and it goes first.

### Known-open items re-checked against the code, not the doc

| # | Claim | Verified |
|-|-|-|
| 4 | `hermes_log` has no `ts_last` | still true — `poll_hermes_logs()` payload is `{"first_line": …}` |
| 5 | `ledger._verify_columns()` checks only the original five tables | **confirmed** — `_ADOPTABLE_TABLES = ("events","cursors","dispatches","dispatch_approvals","triage_items")`; `item_transitions` is created by `_MIGRATION_4` and asserted by nothing |
| 6 | `/metrics` metric 3 loads all of `item_transitions` | still true, and 24 rows makes it a non-issue today |

### Nothing new was found that §43 did not already name

Which is itself the finding: eleven hours of production plus a full independent
re-derivation surfaced no defect the boundary review and the first-night check had
missed. The recon disagrees with `STATE.md` nowhere.

### Next action

Wave 3 item 0 — a distinct state for a dissolved cluster member, schema 5. It
needs a poller and a deadline like every other non-terminal state
(`STATE_DEADLINES` is one closed table), a `WARDEN_SCHEMA_VERSION` bump in
`hermes-agent/scripts/hermes-cc.sh` in the same breath, and `make unload` first —
this is not an additive change.

---

## 45. Wave 3, item 0 — the `split` state (DONE & LIVE, no schema change)

`STATE.md` §43 measured it in production: `_dissolve_cluster()` sent a split
cluster's members back to `new`, and `new` is the one state a silence path may
discharge — so a substantive, correct verdict was closed as `quiet` before anyone
saw it. The fix is a distinct state, per the handover: a dissolved member **has
been evaluated**, carries a verdict, and is not silence-eligible until it has been
re-evaluated individually.

### What changed

| File | Change |
|-|-|
| `scripts/triage.py` | `STATE_SPLIT = "split"`; `SPLIT_VERDICT_NOTE_PREFIX`; `STATE_DEADLINES` + `STATE_EMOJI` entries; `_dissolve_cluster()` targets `split`, carries the verdict in `note`, and no longer skips its bookkeeping under `--dry-run`; `maybe_dissolve_clusters()` passes the verdict text down; `escalate()` treats `split` rows as singleton candidates ahead of `new` clusters; `sweep_deadlines()` preserves a split verdict note on expiry |
| `scripts/api.py` | comment only — why `split` is not a recorded disposition |
| `tests/test_triage.py` | 108 → **116** (7 + 1, accounted below) |
| `DESIGN.md`, `docs/triage.md` | lifecycle diagram, deadline table, and a new § *The `split` state* |

`_SILENCE_RESOLVE_ELIGIBLE_STATES` is **unchanged** at `(STATE_NEW,)`. That is the
point: it is an inclusion list precisely so a state added later is excluded
fail-closed, and `split` is protected by that property rather than by an edit to
it.

### There is no schema 5, and the handover expected one

The Wave 3 handover says of this item *"it is schema 5"*. It is not, and the
reason is evidence rather than preference — a version bump here would be ritual
and would force a coordinated `WARDEN_SCHEMA_VERSION` edit in `hermes-cc.sh` for
no contract change:

| Claim | Checked |
|-|-|
| No new column is needed | `dispatch_job` is already the cooldown anchor and `note` already exists |
| `triage_items.state` has no `CHECK` constraint | `SELECT sql FROM sqlite_master WHERE name='triage_items'` — plain `TEXT NOT NULL` |
| `dispatch-sweep.py` never reads `triage_items` | `grep -n "triage_items" scripts/dispatch-sweep.py` → nothing |
| `hermes-cc.sh`'s `--auto-from-item` refuses a `split` item | it gates `if row["state"] != "verdict"` and returns `STATE <state>` → `policy_err` |
| `api.py`'s `DISPOSITION_STATES` excludes it fail-closed | it is an inclusion list, same shape and same argument as `_SILENCE_RESOLVE_ELIGIBLE_STATES` |

Confirmed by running the cross-repo suites unchanged: `test_hermes_cc.py` **165
cases as expected** (including its `WARDEN_SCHEMA_VERSION assertion 2/2`),
`test_dispatch_approval.py` **83 checks, 0 failures**.

### Second finding — `_dissolve_cluster()` was violating the dry-run contract

Found while building the verification harness, and it is the reason the harness
did not work on the first attempt. The module docstring's DRY-RUN CONTRACT
paragraph says, in as many words:

> *"Every other step (ingest, reopen/unsnooze, classify, resolve, **dissolve
> bookkeeping**) ... runs for real even under --dry-run"*

It did not. `_dissolve_cluster()` opened with `if dry_run: print(...); return`,
placed **before** both the Slack call and the `_set_state()` loop, so a dry run
made no state change at all. Measured, against a `VACUUM INTO` copy of the live
ledger with the pair rewound to `verdict`:

```
BEFORE (HEAD, unmodified)
  pass 1  -> state stays 'verdict'   [dry-run] would dissolve cluster 09bf0c14…
  pass 2  -> state stays 'verdict'
```

`DESIGN.md` § What must not be lost item 8 calls the dry-run contract *"the only
pre-production surface that exists"*. It was silently not covering the one edge
this slice changes. The guard now suppresses only the Slack `update_blocks()`
call. A dissolve moves a row between two **working** states, which is the same
class of move `classify()` / `apply_resolutions()` / `resolve_quiet_grouped()`
already perform for real under dry-run; it is not the **terminal**,
non-re-derivable move `sweep_deadlines()` carves its exception for.

### Verified, not claimed — the live-shaped proof

Two `VACUUM INTO` copies of the live ledger (taken 08:26Z, schema 4, 57 items, 24
transitions). Events 2 and 815 — §43's actual victims — rewound to `verdict`
sharing dispatch `09bf0c14…`, whose `verdict_json` still carries
`UNRELATED SIGNATURES`. Then the real `scripts/triage.py --dry-run` entry point,
`WARDEN_HOME` redirected, twice: once to dissolve, then with both events marked
resolved to see whether silence discharges them.

```
BEFORE — HEAD state machine (dissolve -> new)
  rewound (2, 815) to `verdict`
  pass 1  -> {"2": "new",   "815": "new"}      transitions 24 -> 26
  both events marked resolved (the monitors recovered)
  pass 2  -> {"2": "quiet", "815": "quiet"}    transitions 26 -> 28
  RESULT: DISCHARGED BY SILENCE

AFTER — the split state
  pass 1  -> {"2": "split", "815": "split"}    transitions 24 -> 26
            ev2  : state='split' dispatch_job=SET note=set: cluster split — the investigation's verdict…
            ev815: state='split' dispatch_job=SET note=set: cluster split — the investigation's verdict…
  both events marked resolved (the monitors recovered)
  pass 2  -> {"2": "split", "815": "split"}    transitions 26 (no change)
  RESULT: SURVIVED silence
```

The BEFORE run required patching a **throwaway** `git archive HEAD` export to move
the dry-run guard — the finding above is what made HEAD's own behaviour
unmeasurable. Said here because "we measured the old behaviour" is otherwise a
claim about a run that could not have happened.

**Churn regression, the Wave 2 property, re-run on both:** two passes on an
untouched copy produce `0` and `0` new transitions with an identical census,
before and after. The new state does not reintroduce what §38 removed.

### Test count, accounted for

```
$ make test
  test_api.py                      23/23 passed
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   16/16 passed
  test_triage.py                   116/116 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_locking.py         3/3 passed
  test_watchdog_slack_blindness.py all cases as expected

$ grep -c 'UPDATE triage_items SET state=' scripts/triage.py
1
$ make check-policy   ->   ✓ both copies agree on all 30 repos
```

`test_triage.py` **108 + 7 + 1 = 116**. The seven: dissolve targets `split`;
dry-run performs the state change with no Slack call; the §43 regression through
`apply_resolutions()`; singleton escalation; split-over-new priority with its
deferral reported; cooldown still holding a split member back; the 24h expiry
preserving the verdict note; and the non-split expiry still replacing its note.
The one is `test_a_capped_attempt_does_not_claim_to_have_deferred_anyone` — see
below.

### Mutation-tested, independently of the worker's own table

Nine mutations, each broken → red **by name** → restored → `116/116` green, with
the file verified byte-identical after each restore.

| Mutation | Caught by |
|-|-|
| `_dissolve_cluster()` back to `STATE_NEW` | `test_cluster_dissolves_on_unrelated_verdict`, `test_dissolve_cluster_dry_run_performs_state_change_without_slack_call`, `test_split_state_survives_apply_resolutions_state_43_regression` |
| `STATE_SPLIT` admitted to `_SILENCE_RESOLVE_ELIGIBLE_STATES` | `test_no_chain_state_is_silence_resolvable`, `test_silence_resolve_eligible_states_is_new_only`, `test_split_state_survives_apply_resolutions_state_43_regression` |
| `escalate()` groups split members instead of singletons | `test_escalate_singleton_splits_never_group`, `test_escalate_prefers_split_over_new_in_same_repo_and_defers_new`, `test_a_capped_attempt_does_not_claim_to_have_deferred_anyone` |
| `sweep_deadlines()` drops the split-note preservation | `test_split_expires_to_needs_human_preserving_verdict_note` |
| `_dissolve_cluster()` clears `dispatch_job` | `test_cluster_dissolves_on_unrelated_verdict` |
| dry-run early `return` restored before the `_set_state()` loop | `test_dissolve_cluster_dry_run_performs_state_change_without_slack_call` |
| `split` removed from `STATE_DEADLINES` | `test_every_non_terminal_state_names_a_poller_and_a_deadline` + 4 more (the `_set_state()` raise fires first) |
| deferral lines deleted entirely | `test_escalate_prefers_split_over_new_in_same_repo_and_defers_new` |
| deferral lines **hoisted ahead of the cap checks** | `test_a_capped_attempt_does_not_claim_to_have_deferred_anyone` |

The last two are a pair on purpose: the first proves the deferral is *visible*,
the second proves it is *true*. Only the second catches the regression below.

### Three corrections made to the worker's diff after reading it

A worker's report is a claim; the diff is the proof. Reading it line by line found:

1. **A log-honesty regression.** The worker moved the overflow/deferral prints
   ahead of the two cap checks, so a repo deferred by `MAX_OPEN_INVESTIGATIONS`
   would still print *"N more wait for next run"* — describing a dispatch that
   never happened. The pre-`split` code printed the cluster-cap overflow **after**
   both `continue`s. Restored by carrying the deferral lines on the attempt tuple,
   plus the ninth mutation and the 116th test. `DESIGN.md` § Budgets asks for
   deferral to be visible; a deferral message naming the wrong cause is worse than
   the silence it objects to.
2. **`docs/triage.md` understated the priority rule.** The implementation orders
   **globally** — every eligible `split` across every repo is attempted before any
   `new` cluster in any repo, so the shared `MAX_OPEN_INVESTIGATIONS` /
   `DAILY_INVESTIGATE_BUDGET` ceilings go to obligated items first. That is the
   right call and the worker named it as an assumption; the doc said "in the same
   repo". Corrected to state the global ordering.
3. **`scripts/api.py`** described `DISPOSITION_STATES` as *"eight string
   literals"*. It has ten. Pre-existing drift, fixed because the file is in this
   diff.

### Live

Agents were unloaded for the whole slice (`make unload` at 08:25:58Z, `make
agents` at 09:01:36Z) — §38's lesson, applied rather than relearned. The loop
started on load; `pgrep -f triage.py` was checked before anything else so no
second loop could exist.

```
first live pass on the new code, 09:01:42Z
  census      {'ignored': 7, 'needs_human': 9, 'new': 3, 'note': 8, 'quiet': 30}
  transitions 24  (unchanged)
  schema      4
  warden-loop.err  mtime unchanged (2026-09-09 22:39Z) — no new line
  all five agents ✓ last exit 0 · api /health ok · policy ✓ 30 repos
```

Inert on the current ledger, which is correct: no row is in `verdict`, so nothing
dissolves, and the three `new` rows are the unmapped `uk` group monitors that
never escalate. The first real exercise will be the next genuine
`UNRELATED SIGNATURES` verdict.

### Observations, recorded not fixed

1. **The split note is capped at `MAX_BRIEF_CHARS` (8000)** while its only
   consumer, a Block Kit section, truncates at `SECTION_TEXT_MAX` (3000). Not a
   defect — it is stored honestly and displayed truncated — but it is up to 8 KB
   per split row in `triage_items.note` *and* in `item_transitions.note`, which
   §41's known-open 6 (metric 3 loading all of `item_transitions` into memory)
   will eventually care about.
2. **`_triage_env()`'s save/restore captures `MAX_OPEN_INVESTIGATIONS` /
   `DAILY_INVESTIGATE_BUDGET` on ENTRY**, and this file's cap tests set them
   *before* entering, so a mutated value can leak into alphabetically-later tests
   in the same process. Found by the worker. The suite passes because no later
   test depends on the default; `test_a_capped_attempt_does_not_claim_to_have_
   deferred_anyone` restores by hand rather than rely on it. A latent
   test-isolation gap, not a production one.
3. **`docs/triage.md` § Tests narrates stale case counts** ("30 cases", "42 cases
   total") that predate the 108 baseline. Pre-existing drift; a rewrite of that
   narrative is its own task.
4. **A dissolved pair can still re-cluster after `cooldownHours`** if both
   re-escalate in different runs and later co-occur — `_dissolve_cluster()`'s
   original accepted tradeoff, unchanged. Singleton escalation removes the
   same-run re-fusion that persisting in `split` would otherwise have made the
   likely outcome; a permanent split still needs a negative-relationship table
   this schema does not have.

### Next action

Wave 3 item 1 — the operation id, and crash reconciliation with `unknown`. That
one **does** need a schema change (`dispatch_approvals` has no `event_id`, and 0
of 5 rows carry `--auto-from-item`), so it is schema 5 and it bumps
`WARDEN_SCHEMA_VERSION` in `hermes-agent/scripts/hermes-cc.sh` in the same breath.
It is what makes `/metrics`' *verified unattended fixes per week* stop returning
`null`.

---

## 46. Wave 3, item 1 — reconnaissance, and three facts that change what item 1 is

Before designing the operation id, both sides of the boundary were mapped by two
fresh subagents with no context from this session, and **every load-bearing claim
they returned was re-verified here** against the code, the live ledger and
GitHub. Three of those claims contradict documents this project treats as
authoritative. All three hold. A fourth was flattened by the report and is
corrected below.

### Correction 1 — `--auto-from-item` can never produce an approval row. It is an invariant, not a sample.

`/metrics` metric 4 serves `null` with this reason:

> *"the 'unattended' qualifier cannot be derived without an operation id linking
> an approval to an item — `dispatch_approvals` has no `event_id` and 0/5 rows
> carry `--auto-from-item`"*

The "0/5" reads as a measurement that more data could change. It cannot. Verified
in `hermes-agent/scripts/hermes-cc.sh`:

```sh
:617   awaiting_confirm() { tier_is_gated && [ "$CONFIRM" != 1 ] && [ -z "$AUTO_FROM_ITEM" ]; }
:1369  if awaiting_confirm; then PLANNED=1; fi
:1374  if [ "$PLANNED" = 1 ] && [ "$DRY_RUN" != 1 ]; then
:1375    mint_approval "dispatch" "$name" "$TIER" "$BRIEF" "$WHY" "$CONTEXT"
:1397  if [ -n "$AUTO_FROM_ITEM" ]; then APPROVED_BY="triage:item-…"
:1400  else require_signed_approval "dispatch" …
```

`grep -n 'mint_approval' scripts/hermes-cc.sh` returns the definition and **one**
call site. `AUTO_FROM_ITEM` set ⇒ `awaiting_confirm` false ⇒ `PLANNED` stays 0 ⇒
`mint_approval` is never reached ⇒ no row exists. The two doors are mutually
exclusive by construction.

**Consequence for the design, and it is the whole shape of item 1:** adding an
`event_id` to `dispatch_approvals` would link nothing on the unattended path,
because *the unattended path leaves no approval row at all*. The attribution has
to live somewhere **both** doors pass through. The only such place today is the
`dispatches` row — which already carries `origin_event_id`, the item link the
metric wants, and which is written on both paths.

### Correction 2 — `REVIEW.md`'s `merged_at` finding is a misreading

`REVIEW.md` § *Facts corrected* closes with:

> *"`dispatches.merged_at` for the one successful implement is **NULL** — the
> ledger failed to record its own merge."*

The NULL is real (dispatch 16, job `4f39ca44…`, `vps#8`). The conclusion is not.
**The merge never went through warden**, so there was nothing to record:

```
$ gh pr view 8 --repo jkrumm/vps --json state,mergedAt,mergeCommit,mergedBy
  state      MERGED
  mergedAt   2026-09-08T16:42:20Z
  mergeCommit 9289436afde30f1ad4c0a6d82b5556ca9df9876f
  mergedBy   jkrumm  (is_bot: false)

dispatch 16 finished_at        2026-09-08T14:20:40Z   ->  merged 2h22m later, by hand
dispatch 16 validation_status  NULL
```

`cmd_merge` refuses anything whose `validation_status` is not the literal
`confirmed` (`hermes-cc.sh:2019-2020`, `NOVALIDATION`), so it was structurally
incapable of merging that PR. The operator merged it in the GitHub UI. **There is
no lost write here and no crash to reconcile** — `REVIEW.md` inferred a ledger
failure from a NULL that only ever meant "warden was not involved".

`REVIEW.md` is not rewritten; this is the correction, recorded where corrections
live.

### Correction 3 — the warden-driven auto-implement chain has never executed

```
triage_items with implement_job      0
triage_items with pr_url             0
triage_items with validation_job     0
triage_items with liveness_deadline  0
dispatches with validation_status    0
item_transitions into implementing / validating / merged / merge_blocked /
                     liveness_pending / fixed / pr_open        0
```

`maybe_auto_implement → poll_implement_jobs → poll_validation_jobs → merge →
maybe_check_liveness` has run **zero** times in production. This is the strongest
argument yet for the ordering `DESIGN.md` § Migration already chose: Wave 3 is
where the chain is proven, and everything built for it until now is untested by
anything except its unit tests.

### Refinement — `cmd_merge` itself is NOT unexercised, and the report said it was

The subagent concluded the merge path had never run. That is true of the *chain*
and false of the *verb*. Checked directly:

```
dispatches with merged_at: 4   — all `implement`, all repo `dispatch-scratch`,
                                 2026-08-02 → 2026-08-03, all origin_event_id NULL,
                                 all validation_status NULL
```

Four real `cmd_merge` runs, interactive, in a scratch repo, before the validation
gate existed. So the merge verb has receipts of having worked; what has never run
is **warden driving it**. Worth the distinction, because it means
`dispatch-scratch` is a proven surface for exercising the chain without touching
anything that matters.

### The write-ordering map — where a crash loses or duplicates an operation

This is what item 1 exists to fix, and it is worth having in one table.

| Step | Ledger write | External call | Order | Crash consequence |
|-|-|-|-|-|
| `hermes-cc dispatch` (gated) | `UPDATE dispatch_approvals SET spent_at` + COMMIT | `POST /api/jobs` | **write BEFORE** | approval burned, nothing dispatched — `DESIGN.md:492`'s *"approved fix, no action completed"*, verbatim |
| same | `INSERT INTO dispatches` | (same call) | **write AFTER** | an episode runs that warden has no record of: never swept, never reported, never budget-counted |
| `maybe_auto_implement` | `_set_state(implementing, expect_state=verdict, expect_null=implement_job)` | `dispatch --auto-from-item` | **write BEFORE** ✅ | but `implement_job` is written *after*, so a crash in between leaves an item `implementing` that `poll_implement_jobs` never selects (`implement_job IS NOT NULL`) — rescued only by the 2h deadline |
| same | rollback to `verdict` on a `None` return | | | **the duplication bug**: `_run_hermes_cc_auto_implement` returns `None` on `TimeoutExpired` *and* on non-JSON stdout, both reachable **after** sideclaw accepted the job. Next tick auto-implements again — two branches, two draft PRs |
| `poll_validation_jobs` | `UPDATE dispatches SET validation_status` | `merge --confirm` | **write BEFORE** | the only write-first in the chain, and the one that did not need to be |
| `cmd_merge` | `UPDATE dispatches SET merged_at` | `PUT /pulls/:pr/merge` | **write AFTER** | crash between them → retry sees `merged_at` NULL, GitHub says `merged: true`, `policy_err` fires, warden records **`merge_blocked` for a PR that is merged and deployed**. `DESIGN.md:500`'s *"silently read as failure"*, exactly |
| `run_deploy_if_enabled` | *nothing* | `ssh vps make hyperdx-apply` | **no write at all** | `ok`, `exitCode` and `output` are dropped; only `expectedAlerts` is persisted. "Did the deploy run?" is unanswerable except by probing HyperDX hours later — which cannot tell *"never ran"* from *"ran and the fix was wrong"* |

Two adjacent verbs in one script chose opposite orderings and neither comment
acknowledges the other. `spent_at`-before loses operations; `merged_at`-after
duplicates then misreports them. **Whatever single rule item 1 picks will have to
move one of them.**

`merge_sha` is already obtained (`hermes-cc.sh:2092`) and thrown away — it reaches
stdout as `mergeCommit` and never the ledger. `dispatches` has no column for a
merge SHA and none for a PR number. That is the one genuine remote receipt the
system already holds.

### Existing idempotency, in full — there is no operation id anywhere

No operation id, no request id, no attempt counter, no `unknown` state, no
remote-receipt reconciliation, in either repo. What exists is all local
compare-and-set that never consults an external system: `AND decision IS NULL`
(`intents.py`), unlink-after-commit in the drain, `AND spent_at IS NULL` +
`rowcount == 1`, the supersede-delete on mint, `_set_state(expect_state=…,
expect_null=…)`, and `merged_at IS NOT NULL → refuse` — the only "did this already
happen" check against an external operation, and it reads local state, never
GitHub.

`poll_misses` is not an attempt counter: `dispatch-sweep.py` increments it **only**
on a sideclaw 404, resets it to 0 on any successful poll, and gives up at 3 into
status `lost`. It answers "has sideclaw forgotten this job", which is a
bookkeeping debt, not an operation one.

`dispatch-sweep.py:25-43` is the one place in the system where write/call ordering
was reasoned about and written down — an explicit **at-least-once** contract,
status committed before delivery, `reported_at` stamped only after exit 0. It is a
good template and it is also the easy case: a duplicate Slack message is
cosmetic, and it gives no guidance for a duplicate merge.

### The structural constraint on binding an approval to an operation id

`require_signed_approval()` **recomputes** `payload_hash` from the arguments in
hand (`hermes-cc.sh:1116-1117`) and looks the row up *by that hash*. It never
reads `payload_hash` off a row to choose one. The signature covers `payload_hash`
(`canonical_message()` = `v1|nonce|payload_hash|decision|decided_by|expires_at`),
so anything inside the hash is transitively bound — but **an operation id folded
into the hash must be independently reproducible at verify time from the same
inputs the caller supplies.** A random id minted at plan time and stored only in a
new column would not be recomputable and the lookup would break. It has to ride in
argv, and therefore into `argv_json`, and therefore be replayed verbatim by
`_load_invocation()`.

Also closed against extension: `intents.py`'s `_KIND_FIELDS["approval_decision"]`
is a closed allowlist that `validate()` raises on, and `_NEVER_FROM_FILE =
("expires_at", "payload_hash")` refuses exactly the binding fields from a spool
file. An operation id has the same character and wants the same treatment — it
must come from the row, never from the intent.

### Facts about sideclaw that bear on reconciliation

Verified against `~/SourceRoot/sideclaw` by the second subagent; the `DESIGN.md`
claims they check are quoted in §*Talking to sideclaw* of `CLAUDE.md`.

- **No cancel endpoint exists in any form.** Only `POST /api/shutdown`, which is
  process-wide and SIGTERMs every worker. `warden abort` (item 2) therefore needs
  a real sideclaw change, as `DESIGN.md` says.
- **A pruned job id returns `404 "job not found"` — byte-identical to a job id
  that never existed.** Pruning is 24h **or** 200 terminal rows shared across all
  six tools, and `prune()` runs after *every* job finish, not on a timer. The
  reconciliation problem is confirmed at the wire level.
- **`GET /api/jobs/:id` returns a view that drops `params`.** Warden cannot
  recover a job's repo, tier or brief from sideclaw after the fact — it must have
  recorded them before submitting. An independent argument for
  operation-id-before-dispatch.
- **A drain-killed job is deliberately left at `running`** and reconciled only at
  sideclaw's next boot. If that boot never happens, `GET /api/jobs/:id` reports
  `running` forever, so a reconciler treating `running` as "still alive" waits
  indefinitely.
- `DISPATCH_SCHEMA_VERSION = 1`, carried as a required `schemaVersion` literal on
  every verdict *and* served at `GET /api/dispatch-schema` — two independent
  mismatch channels. Warden's loud-refusal rule is implementable from the job
  result alone.
- **`warden` and `sideclaw` are hard-pinned to tier `investigate`**, merged last
  in `buildDispatchPolicy` so environment variables cannot raise them. `DESIGN.md`
  § Security model's rule holds in the executor, verified rather than assumed.
- **One `DESIGN.md` claim is optimistic.** *"Salvage bundles are referenced only
  inside an error string"* is true of the synchronous-throw path. After a real
  SIGKILL the job's error is the fixed string `'HTTP server restarted while job
  was running'` and the bundle path appears **only in a pino warn log**
  (`dispatch.worktree_salvaged`), reachable by grepping `~/Library/Logs/sideclaw.*`.
  The bundle is derivable from the branch name, which is derivable from the job
  id; bundles self-prune at 14 days / 100 files.

### Next action

Item 1's design follows from correction 1: the unit is the **operation**, not the
approval, and it hangs off `dispatches` (which both doors write) rather than
`dispatch_approvals` (which only the human door writes). That is schema 5.

One question genuinely branches the rest of Wave 3 and is being asked rather than
assumed, per the handover: **which repo the complete path is proven against.**
`dispatch-scratch` has four real `cmd_merge` receipts and no deploy target, so it
proves everything up to `merged → closed`; `vps/observability/**` is the only
seeded deploy target and the only way `fixed` can ever be produced, and it is
`FLOWS.md` flow 1's case-4 monitoring-config trap in production. `DESIGN.md` §
What "done" means already states metric 2 cannot move without the second.

---

## 47. The deploy surface — "merge is deploy" is a design claim with no code behind it

Reconnaissance for the stop-condition exercise. **Operator decision, 2026-09-10:
the driving change is a minor dependency upgrade**, not a monitoring-threshold
fix — a real change with a real diff and real CI, but nothing about it is
self-concealing, so it sidesteps `REVIEW.md` C3 / human-essential case 4 entirely.
That decision needs a repo where merge *is* deploy. Mapped, then verified here.

### The blocker, verified against the code rather than the doc

`DESIGN.md` § Deploy says:

> *"Prefer not needing this at all: `weatherorb`, `research-gateway` and `argo` deploy
> via GitHub Actions → RollHook, so merge **is** deploy."*

Directionally true and **operationally unreachable**. There is no code path into
`liveness_pending` for a repo with no deploy key:

```python
# triage.py, poll_validation_jobs()
elif merge_result.get("ok") and merge_result.get("merged"):
    deploy = merge_result.get("deploy") or {}
    if deploy.get("attempted") and deploy.get("ok"):
        ... STATE_LIVENESS_PENDING
    else:
        ... STATE_MERGED, note="merged; no deploy configured for this repo"
```

```sh
# hermes-cc.sh, run_deploy_if_enabled()
if not entry.get("autoDeploy"):
    printf '{"attempted": false, "reason": "autoDeploy is false for %s"}' "$repo"
```

So a RollHook repo merges, lands in `merged`, and expires to `closed` after 1h —
**never `liveness_pending`, never a probe, never `fixed`.** The design treats
merge-is-deploy as the *easy* case that needs nothing built; the code has no
representation for it at all. `DESIGN.md` § What "done" means already says metric
2 cannot move until a repo has a deploy target and a liveness probe. This is the
specific reason.

Supporting facts, all verified here:

```
LIVENESS_ALLOWLIST                 exactly one key: "hyperdx-alert-state"
config/triage-policy.json `repos`  exactly one repo: vps (observability/** only)
```

`merge_gate_check()` prints `NOPATHS` and refuses outright for a repo with no
`autoMergePaths` — there is no implicit allow, which is correct and means adding
a repo is a deliberate config act.

### This belongs in item 1, and it improves item 1

`DESIGN.md` § Crash recovery asks for *"remote receipts where available (PR merge
sha, **deploy run id**)"*. The ssh path can never supply the second: `ssh <host>
make <target>` returns an exit code and nothing addressable, which is why §46's
ordering table records the deploy as having **no receipt that can exist**.

A GitHub Actions run **does** have an id, and it is queryable after the fact. So
the merge-is-deploy path is not a detour around the receipt problem — it is the
only place in this estate where a real deploy receipt is obtainable. Building it
strengthens item 1 rather than competing with it.

### The candidates, measured

| | `weatherorb` | `research-gateway` | `argo` |
|-|-|-|-|
| Actions → RollHook on master | yes, **paths-filtered** (JS only) | yes | yes (api + dashboard) |
| Deploy duration, last 5 runs | 60-80s | 55-85s | 67-145s |
| **PR-time CI** | **none** (push-to-master only) | **none** | **yes** — `check.yml` on `pull_request` |
| Health endpoint | `/health`, static nginx `"ok"` | `/health` + `lastRestartAt` | `/api/health` |
| Reachable from the mini | **public** (Cloudflare tunnel) | tailnet, **200** | tailnet, **200** |
| Reveals a commit SHA | no | no | no (`version` is a hardcoded `"1.0.0"`) |
| Installable minor/patch upgrade today | **`vite` 8.2.1→8.2.2**, exercised by the real build + two bundle-size gates | **none** — every candidate inside the 3-day `minimumReleaseAge` cooldown | `concurrently`, `lefthook` — dev-only, **CI never invokes them** |

**Nothing bakes a commit SHA into any image.** `rollhook-action`'s inputs are
`url`/`image_name`/`image_tag`/`dockerfile`/`context`/`build_args`/`timeout` — no
automatic revision label. So today the strongest available liveness evidence is
*"a restart landed after the merge"* (`research-gateway`'s `lastRestartAt`, or
argo's bearer-gated `GET /api/docker/vps/containers` `startedAt`, which covers all
three VPS containers without touching any of them), **not** *"this commit is
live"*. `lastRestartAt` = 2026-09-08T08:31:57Z matches research-gateway's last
deploy run (08:31:09Z → 08:32:10Z) exactly, which is what makes it usable at all.

A restart-time probe is a genuine positive signal and it is weaker than what
`fixed` deserves. Adding `GIT_SHA=${{ github.sha }}` as a `build_args` plus an
endpoint that echoes it is a small change in the target repo and would make the
probe prove the deploy *landed* rather than that the service *bounced*.

### Two more contradictions found, recorded not fixed

1. **`config/triage-policy.json`'s `_readme` is stale about argo.** It says *"argo
   is compose-managed manually inside `vps` — nothing in argo's own repo redeploys
   it"*, and routes `docker_*:*:argo*` alerts to `vps` on that basis. But
   `argo/.github/workflows/deploy.yml` is committed and active, and
   `vps/apps/argo/compose.yml` carries `rollhook.allowed_repos=jkrumm/argo` on both
   containers. **Argo does redeploy from its own repo on every master push.**
   Either the rationale is stale or the routing rule points at the wrong repo —
   resolve before argo is added to `repos`.
2. **`DESIGN.md` overstates weatherorb.** Only the *edge* (web/nginx) deploys via
   Actions → RollHook. WeatherOrb's Python half (`uv.lock`, store/blend/tileserver)
   runs on this mini under launchd with no deploy workflow, so a `uv.lock` bump
   would merge and deploy nothing. Half the repo is not merge-is-deploy.

### The `noCiRequired` trap this walks into

`weatherorb` and `research-gateway` have **no `pull_request` trigger**, so a PR head
commit gets zero check runs and both would need `noCiRequired: true`. That is
exactly the *"vacuously clean"* inversion `hermes-cc.sh`'s own merge gate comments
say it exists to refuse — "no checks" and "all checks passed" must not be the same
answer. Only `argo` has genuine PR-time CI.

That pushes toward `argo` on safety and toward `weatherorb` on the quality of the
change being tested (`vite` 8.2.1→8.2.2 goes through the actual production build
and two bundle-size gates; argo's two installable bumps are dev tools its CI never
invokes, so a green PR would prove almost nothing). **Not decided here** — it is
downstream of the merge-is-deploy mechanism existing at all, and that is the next
build after item 1a.

### Next action

Finish item 1a (the operations ledger, in flight). Then merge-is-deploy as item
1b, with the Actions run id as its receipt — which is the deploy receipt `DESIGN.md`
§ Crash recovery asks for and the ssh path structurally cannot provide.

---

## 48. Wave 3, item 1a — the operations ledger (DONE & LIVE, schema_version 5)

`DESIGN.md` § *Crash recovery* asked for three things. All three now exist for
the mutating chain, and the shape follows from §46 correction 1 rather than from
the handover's framing.

### What changed

| File | Change |
|-|-|
| `scripts/ledger.py` | `SCHEMA_VERSION` 4 → **5**; `_MIGRATION_5` creates `operations` + two indexes, no data migration; `_VERSIONED_TABLES` split from `_ADOPTABLE_TABLES` |
| `scripts/triage.py` | `record_operation()` / `complete_operation()`; `reconcile_operations()` as step −1, first in `run()`; `_AutoImplementResult` / `_MergeCallResult`; `_parse_pr_url()` / `_run_gh_pr_view()` / `GH_BIN`; receipts persisted on merge |
| `scripts/api.py` | metric 4's derivation, replacing its "cannot be derived" reason |
| `hermes-agent/scripts/hermes-cc.sh` | `WARDEN_SCHEMA_VERSION` 4 → **5**, one line, nothing else |
| tests | `test_triage.py` 116 → **131**, `test_api.py` 23 → **26**, `test_ledger.py` 16 → **19** |
| `DESIGN.md`, `docs/triage.md`, `docs/api.md` | crash recovery described as built; the new loop step; metric 4's derivation |

`operations` is written **before** the external call it covers and committed
before `record_operation()` returns — that commit is the whole contract. Scope is
the two mutating verbs only, `implement` and `merge`; `investigate`/validation
episodes are read-only in their own worktree and `dispatch-sweep.py`'s
`poll_misses`/`lost` path already covers a forgotten one.

**`merge` is one operation, not two, and that is a stated limitation.**
`hermes-cc.sh merge --confirm` covers GraphQL ready-for-review, `PUT
/pulls/:pr/merge`, a branch delete *and* `ssh <host> make <target>` behind one
subprocess boundary, so there is no write-point between the merge and the deploy.
A reconciled merge's receipt records `"deploy": "unknown"` rather than guessing.
Splitting it needs a `hermes-agent` change.

Also closed here: **§41 known-open item 5.** `_verify_columns()` walked only
`_ADOPTABLE_TABLES`, so `item_transitions` was created by migration 4 and asserted
by nothing — and `operations` would have inherited the gap. The two lists now
answer two different questions: `_ADOPTABLE_TABLES` is *"is this the live
pre-versioned ledger"* (checked before the migration loop, where neither newer
table legitimately exists yet); `_VERSIONED_TABLES` is *"does a database stamped
at `SCHEMA_VERSION` have the columns that version declares"*.

### A mutation SURVIVED, and it is the Wave 2 defect-3 pattern repeating

The most important thing in this section. My independent mutation battery flipped
`_run_hermes_cc_auto_implement()`'s `TimeoutExpired` branch from `"unknown"` to
`"failed"` — reinstating the duplication bug the whole slice exists to close —
and **the suite stayed green at 129/129.**

Both of the worker's tests for that behaviour stub the helper wholesale:

```python
triage._run_hermes_cc_auto_implement = lambda *, repo, event_id: \
    triage._AutoImplementResult(None, "unknown")
```

They test the **caller's** handling of an injected outcome and can never observe
the **producer's** mapping. That is exactly §42 defect 3 — *"a read-only guarantee
whose test could not observe the call site it claimed to guard"* — arriving again
in a different function, one wave later, and again only a mutation found it.

Two producer-level tests now drive the real helper bodies with a patched
`subprocess.run`, covering every branch of both helpers. Re-running the mutation:

```
== MUTATION: auto-implement TimeoutExpired -> failed
  test_auto_implement_helper_maps_each_failure_mode_to_the_right_outcome:
    a timeout may follow an accepted submission: outcome 'failed' != 'unknown'
== MUTATION: merge TimeoutExpired -> failed
  test_merge_helper_maps_each_failure_mode_to_the_right_outcome:
    a merge timeout may mean the PR is already merged — got _MergeCallResult(result=None, outcome='failed')
```

**`_run_hermes_cc_merge()` was unprotected too** — a second, unmutated instance of
the same gap, found only because the first one prompted looking.

### Three defects fixed on the worker's diff after reading it

1. **A reconciled merge did not move its item.** Resolving the *operation* to
   `done` left the item in `validating`, whose 1h rule expires it to
   `merge_blocked`. The operations table would read *"merged, here is the sha"*
   while the item read *"blocked"* — §46's merged-but-recorded-as-failure bug one
   layer further in. A reconciled merge now advances the item: `merged` when the
   repo has no deploy target (where the live path puts it), **`needs_human` when
   it auto-deploys**, because the deploy rode along inside the same lost
   subprocess and `merged` would quietly expire to `closed`, claiming a landing
   nobody verified. The worker flagged this in its own report rather than hiding
   it — the brief was reasoned, not ordered.
2. **A remote error was read as a refusal.** `hermes-cc.sh`'s `_err` taxonomy is
   2 precondition / 3 remote / 4 policy / 64 usage, returned as `exitCode` in its
   `--json` error object. Only **3** can fire after an external mutation was
   attempted: `remote_err` is reached once it is already talking to something, and
   covers the literal `remote_err "sideclaw accepted the job but returned no id"`.
   Mapping it to `merge_blocked` is *"silently read as failure"* arriving through
   a parsed error object instead of a dead process. Exit 3 now leaves the
   operation open for GitHub to answer; 2/4/64 stay definite refusals, with a test
   pinning each side so nobody widens it.
3. **`api.py` repeated §42 defect 1's arithmetic shape.** `fixed_event_ids` was a
   list *with duplicates* subtracted from a **distinct** set of signed items. An
   item reaching `fixed` twice in one window (`fixed` → a failed liveness probe
   reopens it → `fixed`) while carrying a signed approval would have reported an
   unattended fix that never happened. The mutation shows it exactly:
   `{'value': 1, 'fixed_in_window': 2, 'unattended_in_window': 1}` for one signed
   item. Now `SELECT DISTINCT`, with the unit named as the item.

Also corrected: the **DRY-RUN CONTRACT paragraph was stale**. The loop now shells
out to `gh`, a third externally-visible action, and `reconcile_operations()` is a
third dry-run carve-out beside `drain_intents()` and `sweep_deadlines()`. Both are
now named in that paragraph rather than left for a reader to discover.

### Verified, not claimed

```
$ make test
  test_api.py                      26/26 passed
  test_dispatch_sweep.py           all cases as expected
  test_intents.py                  19/19 passed
  test_ledger.py                   19/19 passed
  test_triage.py                   131/131 passed
  test_watchdog_delivery.py        all cases as expected
  test_watchdog_locking.py         3/3 passed
  test_watchdog_slack_blindness.py all cases as expected

$ make check-policy                    ✓ both copies agree on all 30 repos
$ grep -c 'UPDATE triage_items SET state=' scripts/triage.py     1
$ hermes-agent tests                   165 cases as expected · 83 checks, 0 failures
$ SCHEMA_VERSION 5  ==  WARDEN_SCHEMA_VERSION "${…:-5}"
```

`test_triage.py` **116 + 9 (worker) + 4 (my three fixes) + 2 (the producers) =
131.** `test_api.py` 23 + 2 + 1 = 26. `test_ledger.py` 16 + 3 = 19.

Eleven mutations, each red **by name**, each restored, the file verified
byte-identical after every restore:

| Mutation | Caught by |
|-|-|
| reconciled merge no longer advances the item | 3 tests, incl. `test_reconcile_merged_operation_advances_the_item_instead_of_leaving_it_to_expire` |
| `autoDeploy` branch → `merged` instead of `needs_human` | `test_reconcile_merged_operation_with_autodeploy_goes_to_needs_human` |
| exit-3 REMOTE read as a refusal again | `test_merge_remote_error_is_left_for_reconcile_not_read_as_merge_blocked` |
| `record_operation()` loses its `commit()` | `test_record_operation_commits_before_returning` |
| sideclaw 404 → `failed` instead of `unknown` | `test_reconcile_implement_sideclaw_404_becomes_unknown_not_failed_and_needs_human` |
| GitHub says MERGED → `failed` | 3 tests |
| `reconcile_operations()` moved after `drain_intents()` | `test_reconcile_operations_runs_before_anything_that_could_retry` |
| `mergeCommit` dropped from the receipt | `test_successful_merge_stores_pull_request_merge_commit_and_deploy_in_receipt` |
| **auto-implement `TimeoutExpired` → `failed`** | `test_auto_implement_helper_maps_each_failure_mode_to_the_right_outcome` (**survived before the producer tests existed**) |
| **merge `TimeoutExpired` → `failed`** | `test_merge_helper_maps_each_failure_mode_to_the_right_outcome` (**also unprotected before**) |
| `SELECT DISTINCT` dropped from metric 4 | `test_metric4_counts_an_item_once_even_if_it_reaches_fixed_twice` |

### The migration — rehearsed on a copy, then live

Agents unloaded for the whole slice (`make unload` 09:25:45Z, `make agents`
10:12:32Z), snapshot at `~/.warden/backups/pre-schema-v5-20260910T0920Z.db`.

```
REHEARSAL  (copy of the live ledger, WARDEN_HOME redirected)
  two passes: 0 and 0 new transitions, census identical
  schema_version 5 · operations table + operations_open/operations_event · 0 rows
  quick_check ok

LIVE  (the loop migrated it at boot, as designed — only the loop migrates)
  live schema before: 4
  live schema after:  5 at 2026-09-10T10:12:38.508444+00:00
  census      {'ignored': 7, 'needs_human': 11, 'new': 3, 'note': 8, 'quiet': 28}
  transitions 30 (unchanged)  ·  operations 0  ·  quick_check ok
  all five agents ✓ last exit 0 · /health ok, schema 5 == expected 5
  warden-loop.err mtime unchanged (2026-09-09 22:39Z) — no new line
```

`/metrics` metric 4 now returns `null` for the **history-window** reason rather
than *"the unattended qualifier cannot be derived"* — the derivation exists; the
number needs a `fixed` transition, which needs the chain to run.

Note the live census moved during the brief window the agents were loaded between
slices: `quiet` 30 → 28, `needs_human` 9 → 11, transitions 24 → 30. Two items
recurred, clustered, dispatched and folded to `needs_human` unattended — the
pipeline working, not a side effect of this slice.

### Known-open, decided deliberately

1. **An `implement` dispatch that exits 3 (REMOTE) is still treated as `failed`
   and rolled back.** The merge path now distinguishes it because GitHub can be
   asked; the implement path cannot — `GET /api/jobs/:id` drops `params` (§46), so
   warden cannot ask sideclaw *"is there a job for this repo"*. Mapping exit 3 to
   `unknown` there would send an item to `needs_human` on **every transient
   sideclaw outage** — the common case, currently handled correctly by a rollback
   and retry — to guard against sideclaw returning 200 with a malformed body,
   which is essentially never. The honest fix is a distinct exit code in
   `hermes-cc.sh` separating "could not reach sideclaw" from "sideclaw accepted
   but I lost the id"; that is a `hermes-agent` change, not this slice.
2. **A reconciled `done`/`failed` `implement` operation cannot occur in practice**
   — the only way its receipt carries a `jobId` is `complete_operation()` having
   been called with one, in the same commit that sets the outcome. The branches
   are defensive, and the no-jobId path (`unknown` → `needs_human`) is the one
   that actually fires.
3. `reconcile_operations()` does not resume the chain past the operation it
   reconciled — a reconciled merge lands the item correctly, but a future
   `implement` reconciliation that could resume `implementing` → `validating` is
   not built.

### Next action

Item 1b — merge-is-deploy, with the GitHub Actions run id as the deploy receipt
(§47). It is the only place in this estate where a real deploy receipt exists, and
without it no repo can reach `fixed`, so it blocks the stop-condition exercise.

---

## 49. FINDING — the notifier does not read the ledger

Raised by the operator, 2026-09-10: *"we keep on getting Slack alerts … seemingly
many watchdogs open etc."* Investigated read-only. **Warden is not
malfunctioning. It is telling the truth on a cadence that has nothing to do with
what it knows.**

### What the reminder digest actually is

`watchdog-poll.py:1216` renders it (`_render_section("Reminders", ":bell:", …)`),
driven by `reconcile()`'s reminder branch at `:906-930`, anchored on
`events.last_reminder_at || notified_at` against `REM_HOURS` — 6h for `uk`, 24h
for `hermes_log`, 168h for `stray_skill`.

```
$ grep -n 'triage_items' scripts/watchdog-poll.py
(nothing)
```

**The reminder path never reads `triage_items`.** It has exactly one mute
(`:918-921`): an event whose `dispatch_id` points at a dispatch with
`reported_at IS NULL` — i.e. an investigation currently in flight. There is no
mute for an item that is `ignored`, `note`, `needs_human`, unmapped, or parked
behind a deadline. The notification layer and the state machine are decoupled.

That contradicts `DESIGN.md` principle 1 in spirit — *"Slack cards and Argo pages
are projections … the ledger re-renders the surface"* — for this one surface.
The card path obeys it; the reminder digest renders straight off `events`.

### The seven lines, split

| Population | Rows | Verdict |
|-|-|-|
| **Structurally unactionable** | ev269 `Local` (#6), ev285 `VPS` (#7), ev710 `Services` (#7) | `repo IS NULL`, state `new`. `escalate()` requires `repo IS NOT NULL`, so these can never be investigated, never resolve, and remind **every 6h forever**. |
| **The system working** | ev930 Hermes-HTTP (#8), ev943 Research-Gateway-HTTP (#7), ev261 slack_bolt (#6), ev875 WeatherOrb-Watchdog (#5) | All `needs_human`, all carrying a real verdict — the text in the operator's Slack paste **is** the verdict warden wrote. Four genuine outstanding decisions with 7-day deadlines. |
| **No triage row at all** | ev849, ev887 `stray_skill`, ev68 `hermes_cron` | `DESIGN.md` § Open questions 3 named ev849 already: *"no `triage_items` row exists for it at all."* Still true. |

So **3 of 7 lines are noise by construction** and 4 are "you have decisions
pending" at the wrong cadence.

The three were identified in `DESIGN.md` § Open questions (resolved-since-v1
paragraph) **by name**: `uk:95` / `uk:179` / `uk:186` are UptimeKuma **group**
monitors matching `homelab/uptime-kuma/monitors.yaml` group names verbatim —
*"mappable without guessing"*. The fix was written down and never applied.

### The trap in the obvious fix

Adding an `ignore` or a mapping rule to `config/triage-policy.json` **would not
silence them**, and that is worth knowing before someone tries it: the reminder
is emitted by `watchdog-poll.py` before and independently of triage
classification. Routing an item to `ignored`/`note` changes the *card* and the
*digest*, not the *reminder*. Mapping them to a repo would silence them only as a
side effect — by making them escalate, which spends investigate episodes on group
parents whose children already alert separately.

### The two real fixes, neither done here

1. **Teach the reminder path the ledger.** Skip an event whose item is
   `ignored`/`note`, and give `needs_human` its own cadence (§41 known-open 3
   scoped "reminder at 1d" out of warden on the grounds that *"a reminder is a
   notification, not a deadline"* — correct about the deadline, and it left the
   notification unowned, which is this). This is the structural fix and it is the
   one that makes the digest trustworthy.
2. **Dispose of the three group monitors** — an `ignore` entry at the poller, or a
   decision that a group parent is worth its own item. An operator call, not a
   code call: it is about what the operator wants to be told.

### Why this matters beyond the noise

`DESIGN.md`'s opening measurement is *"a correct verdict has nowhere to go"*. This
is the mirror image: four correct verdicts have somewhere to go, they are sitting
there correctly, and the surface reports them identically to three rows that can
never move. **A digest where actionable and unactionable look the same is how an
operator stops reading the digest** — which is the failure mode that produced the
eleven-day blindness this project exists to prevent.

Recorded, not fixed. It is not Wave 3 scope as written, and it is a stronger
candidate for the next slice than anything remaining in items 2 and 3.

---

## 50. The complete path, walked by hand before automating it

`STATE.md` §47 established that merge-is-deploy has no code path and that no image
reveals which commit it is running. Operator decision: **argo, plus a `GIT_SHA`
surface** — the only one of the three RollHook repos with PR-time CI, so the merge
gate sees real check runs and `noCiRequired: true` (the vacuously-clean inversion
`hermes-cc.sh` exists to refuse) is never needed.

### The argo change

`jkrumm/argo` PR **#16**, merged 2026-09-10. `GIT_SHA=${{ github.sha }}` as a
`build_args` on the api image; `ARG`/`ENV` in the Dockerfile's **runner** stage (an
ARG only exists in the stage that declares it); `/health` returns
`{status, commit}`, read once at module load. Defaults to `"unknown"` so a local
build stays schema-valid. Verified before opening: lint 0/0 on the changed file,
format clean, typecheck clean, 578 api tests pass, and no existing test asserts the
`/health` shape.

### The whole chain, measured

```
16:30:0xZ  PR #16 opened
16:31:12Z  CI `check` pass          (argo is the only one of the three with PR-time CI)
16:31:43Z  rebase merge             cf003491b46234983dd789a977e2fba6b7bcdd32
           Actions "Deploy" run 34502657131 on that sha
16:32:39Z  GET https://argo.jkrumm.com/api/health
           {"status":"ok","commit":"cf003491b46234983dd789a977e2fba6b7bcdd32"}
```

**56 seconds from merge to the deployed commit serving.** Before the merge the same
endpoint returned a bare `{"status":"ok"}` — the baseline was captured first, so the
field appearing is evidence rather than assumption.

### Four facts this established that were previously assumptions

1. **argo is rebase-only** (`allow_squash_merge:false`, `allow_merge_commit:false`).
   `hermes-cc.sh`'s `pick_merge_method()` probes the repo and falls back
   squash → rebase → merge, so it already handles this. Found by having `gh pr merge
   --squash` refused, not by reading.
2. **`gh pr view --json mergeCommit` returns the rebased head**, byte-identical to
   `git rev-parse origin/master`. So `reconcile_operations()`'s existing SHA source
   is correct on a rebase-only repo — and it is the *same value* that becomes
   `GIT_SHA` in the image. **That equality is the whole basis of the probe** and it
   was worth checking rather than assuming.
3. **The deploy latency is ~56s**, inside §47's measured 67-145s range for argo's
   last five runs. `LIVENESS_WINDOW_HOURS` has enormous headroom.
4. **CodeRabbit does not review this repo** — *"fewer than 10 stars"* — so its
   green check is a **skip, not a review**. A CI status that reads `pass` for
   "declined to look" is the same vacuously-clean shape the merge gate refuses
   elsewhere; worth knowing before anyone treats a CodeRabbit tick as evidence.

### The safety staging that lets this be built with the gate shut

Adding argo to `config/triage-policy.json`'s `repos` with `deployOnMerge` and
`liveness` but **no `autoMergePaths`** leaves warden unable to merge anything there:
`merge_gate_check()` prints `NOPATHS` and `cmd_merge` refuses with *"nothing merges
without an explicit declared scope"*. So the mechanism, the probe and the receipt
can all be built and tested while the merge gate stays closed. **Opening it is a
separate operator decision** — and a real one, because the driving change is a
dependency upgrade, so `autoMergePaths` would grant warden auto-merge over
`package.json`/`bun.lock` changes it wrote itself.

### Also done: the first production use of `cmd_close`

The IU endpoint's rolling-30-day 403 turned out to be the provider's own
usage-tracking fault, resolved externally and confirmed by the operator — never our
consumption. Three items (ev968/969/970) were parked in `needs_human` for it.
Closed by hand with a reason rather than left to the 7-day clock, which would have
recorded them as `dismissed`/`expired` — an outage that was resolved is not an
expiry.

```
ev968/969/970   needs_human -> closed   16:28:47Z
metric1         6/9 = 0.667  ->  7/10 = 0.700
metric2         `closed: 3` enters the funnel for the first time
```

`cmd_close` had never fired in production before. It works, and the three
`needs_human → closed` pairs it produced (~10.2h each) are the first data
`/metrics` metric 3 has ever had.

**Metric 3 still reads `null` anyway, and that is worth a note.** Its leaf history
guard suppresses the whole window because the 7-day start predates `history_since`
(2026-09-09T19:43Z) — correct for a *count* over a window the table did not exist
for, but this metric is a **median of durations**, and a median of the pairs that do
exist is a valid statistic on a smaller sample, not an understatement. The guard
applies count semantics to a median. It self-resolves on 2026-09-16, so it is
recorded rather than changed — §42 defect 2 added those leaf guards deliberately and
re-litigating one needs better evidence than six days of impatience.

### Next action

Item 1b's warden half, in flight: the `deployOnMerge` policy key, the third merge
case into `liveness_pending`, an `argo-commit-live` liveness gatherer, and the
Actions run id folded into the merge operation's receipt as the deploy receipt the
ssh path structurally cannot provide.

---

## 51. Wave 3, item 1b — merge-is-deploy (DONE & LIVE, no schema change)

§47 found that `DESIGN.md`'s *"merge **is** deploy"* had no code path: a RollHook
repo merges, lands in `merged`, expires to `closed` at 1h, and never reaches a
probe — so `fixed` was unreachable for every repo except `vps`. This closes that,
with the GitHub Actions run as the deploy receipt the ssh path structurally cannot
provide.

### What changed

| File | Change |
|-|-|
| `scripts/triage.py` | `_gather_argo_commit_live()` + `ARGO_HEALTH_URL`, registered as `argo-commit-live`; `_FULL_SHA_RE`; `_run_gh_run_list()`; a third merge case in `poll_validation_jobs()`; `reconcile_operations()` asks for the real run status and converges the item |
| `config/triage-policy.json` | `argo` in `repos` — `deployOnMerge` + `liveness`, **no `autoMergePaths`**; the new key documented in `_readme` |
| `tests/test_triage.py` | 131 → **148** |
| `DESIGN.md`, `docs/triage.md` | the mechanism, the policy key, the gatherer |

No schema change — `deploy_expect_json` and `liveness_deadline` already exist and
the new payload reuses the list-of-dicts shape `maybe_check_liveness()` already
deserializes.

**The merge gate stays shut.** argo has no `autoMergePaths`, so
`merge_gate_check()` refuses every merge with `NOPATHS`. The mechanism, the
receipt and the probe are all built and tested while warden cannot merge anything
in argo. Opening it is a separate operator decision and is called out in the
policy file itself so nobody "completes" the entry.

### The probe is an exact sha match, and that is the whole point

A restart-time probe (research-gateway's `lastRestartAt`, argo's container
`startedAt`) proves *a* restart, not *this* commit. `fixed` is the one state in
this system that claims a change actually worked, and `REVIEW.md` C3 is about
exactly how that claim gets gamed. So `ok` is true only on an exact match;
`"unknown"` (argo's own out-of-CI placeholder), a differing sha, a missing field,
a non-200 and unparseable JSON all read as a genuine mismatch, never as "not sure".

**Run against the live service, not a stub:**

```
URL: https://argo.jkrumm.com/api/health
match  -> (True,  'commit cf003491b462 live')
wrong  -> (False, "live commit 'cf003491b462' != expected 'aaaaaaaaaaaa'")
empty  -> (False, 'no expected commit was captured at merge time')
nosha  -> (False, 'expected deploy record carried no commit sha')
```

The URL lives in the gatherer, not the policy file — `LIVENESS_ALLOWLIST` names a
behaviour and the behaviour owns its endpoint, exactly as `hyperdx-alert-state`
owns `HYPERDX_BASE`. A config-driven URL would let a policy edit alone decide what
this loop asks an arbitrary host to confirm, which is what the closed-allowlist
principle exists to prevent.

### A second mutation SURVIVED, same lesson as §48, one branch deeper

The worker's own table was clean. My independent battery replaced the sha guard
`isinstance(merge_sha, str) and _FULL_SHA_RE.match(merge_sha)` with a bare
`merge_sha is not None` — and the suite stayed green at 148/148.

The test covering that guard passed `mergeCommit: None`, **the one value both
spellings reject.** `_FULL_SHA_RE` exists for the present-but-malformed values, and
none of them was tested. The test now runs eight cases — `None`, `""`, `"abc"`, 39
hex, 41 hex, uppercase, `"unknown"`, and 40 non-hex chars — and the mutation now
fails by name on the empty string, which would otherwise have entered
`liveness_pending` with nothing a probe could ever match.

Same shape as §48's survived mutation and §42 defect 3 before it: **a test that
exercises one branch of a compound condition looks like coverage and is not.**
Three waves, three instances, each found only by mutation.

### One defect fixed on the worker's diff, flagged in its own report

A `deployOnMerge` merge reconciled **via GitHub** after a crash landed in
`STATE_MERGED` with the note *"no deploy configured for this repo"* — false for
this repo class — and never resumed toward a probe.

The reasoning that fixes it: **a `deployOnMerge` deploy is driven by GitHub
Actions off the push, not by the subprocess warden lost.** A crash in warden says
nothing about whether the deploy ran, and the probe can still answer. So
reconciliation now converges on exactly where the live path puts it —
`liveness_pending` on `[{"commit": sha}]`, with a fresh window measured from now
(the probe is idempotent; the deadline bounds how long we wait for it, it is not a
claim about when the deploy happened). Two tests pin both sides of that branch.

That convergence is worth stating as a principle: **a reconciled operation must
land its item where the uncrashed path would have.** §48 got this right for the
ssh case and wrong for this one, because this repo class did not exist yet.

### Verified

```
$ make test
  test_api.py 26/26 · test_ledger.py 19/19 · test_intents.py 19/19
  test_triage.py 148/148 · three "all cases as expected" suites · test_watchdog_locking.py 3/3
$ grep -c 'UPDATE triage_items SET state=' scripts/triage.py    1
$ make check-policy    ✓ both copies agree on all 30 repos
$ config/triage-policy.json — JSON valid
```

131 + 15 (worker) + 2 (convergence) = 148; the eight-case guard test replaced a
one-case one rather than adding to the count.

Six mutations, each red **by name**, restored, file verified byte-identical:
reconciled `deployOnMerge` no longer converging; the gatherer returning true on any
200; the gatherer accepting `"unknown"`; `deployOnMerge` merges going to `merged`;
the sha guard weakened to `is not None` (the survivor, now caught); and the Actions
run identity dropped from the receipt.

### Also corrected: a factual error in my own brief

The brief claimed the positive-path test would be *"the first test in the repo's
history that produces a `fixed` row"*. False — `test_liveness_confirmed_resolves_the_item`
already asserted `STATE_FIXED`. The worker checked, wrote the test anyway on honest
framing (it exercises the real `argo-commit-live` gatherer end to end rather than a
swapped-in stub, which *is* new), and flagged the discrepancy rather than adopting
the claim. That is the reasoned-brief contract working in the direction it is meant
to.

### Live

The five agents ran throughout — this slice is additive (a new policy key, a new
allowlist entry, a new branch), so no `make unload` was needed. Checked after the
edits landed: all five ✓ last exit 0, `/health` ok at schema 5, all three pollers
fresh, census unchanged, `warden-loop.err` mtime still 2026-09-09 22:39Z.

### What is now true that was not

`fixed` is reachable. The chain has a repo where merge is deploy, a receipt with an
id that outlives the process, and a probe that proves a specific commit is serving.
**Nothing has run it yet** — the merge gate is deliberately shut, so no item can
reach `implementing` in argo. That is the stop-condition exercise, and it is next.

### Next action

Item 2 (`warden abort` — needs a real `POST /api/jobs/:id/cancel` in sideclaw,
which does not exist — `warden revert`, the per-repo in-flight lock), then item 3:
drive a dependency upgrade through the whole chain and kill the process at every
boundary. The `autoMergePaths` decision for argo gates item 3 and is the operator's.

---

## 52. Wave 4 (estate chain) — reconnaissance against the running system (2026-09-10, ~17:30Z)

This is step 4.1 of `~/SourceRoot/dotfiles/docs/waves/PLAN.md` Wave 4 — a different
chain from warden's own Waves 0–3 above. Findings only; every number below was
measured on the mini before any edit in this wave. The design authority for what
follows is `brain/Inbox/Warden Wave 4 — Shape.md`; where it or the audit was wrong,
that is recorded here.

### Warden

```
make status   five agents ✓ (loop, poll, sweep, backup, api pid 89295), last exit 0
              api /health ✓ ok, schema 5 = expected 5, all three pollers fresh
              policy ✓ both copies agree on all 30 repos
make test     test_api 26/26 · test_dispatch_sweep ok · test_intents 19/19 ·
              test_ledger 19/19 · test_triage 148/148 · watchdog_delivery ok ·
              watchdog_locking 3/3 · watchdog_slack_blindness ok
/metrics      verdicts_recorded_disposition 0.7 (7/10) · verified_fixes_vs_silence
              0.0 (0 fixed / 9 quiet / 3 closed) · everything windowed is null with
              a reason (history_since 2026-09-09T19:43Z)
```

Tree clean at `d3f5f2b`. `CLAUDE.md` and `docs/triage.md` still name the
`test_triage.py` gate as **107/107**; it has been 148/148 since Wave 3 — corrected in
this wave's docs commit, the number is a finding to report and 148 is the reported
number.

`warden-loop.err` tail: the four `needs_human` items with NULL `state_deadline`
(uk:204/220/229/193) were stamped 168h on 2026-09-10 17:34Z by the loop's own
backfill; the last line before that is `propose_mappings — model call failed: HTTP
Error 403` — from the IU cost-cap window, see below.

### The IU key, measured

`curl $ANTHROPIC_BASE_URL/v1/messages` with `model: glm-5.3-flash` → **HTTP 200**,
`"text":"OK"`. The last `403 access_denied` in `~/Library/Logs/sideclaw.jsonl` is
`2026-09-10T13:36:55Z` (a `review` adversary angle). The cap is gone; nothing in
this wave is blocked on it. Finding 2 is closed by the provider, not by us.

### sideclaw's cheap lane — the audit's diagnosis was wrong in the way that matters

`GET /api/routing`: `check`/`overview`/`review_router` = `glm-5.3-flash` on `iu`,
fallback `claude-haiku-4-5` on `max`, no `.env` overrides. `GET /api/jobs?limit=30`:
the newest `overview` jobs failed, the newest `check` failed, one `review` done.

Reproduced sideclaw's exact worker argv by hand (`-p … --output-format stream-json
--verbose --setting-sources project --settings '{"disableAllHooks":true}'
--strict-mcp-config --max-turns … --model glm-5.3-flash`, IU env, the four
`ANTHROPIC_DEFAULT_*_MODEL` pins) on Claude Code **2.1.267**:

```
exit=0   result subtype=success  result="OK"
stderr:  ⚠ claude.ai connectors are disabled because ANTHROPIC_API_KEY or another auth source is set …
         [claude-code:unrecognized_model] {"model":"glm-5.3-flash","query_source":"generate_session_title"}
```

**Both stderr lines are warnings.** They print on every IU run, including the ones
that exit 0 — `[claude-code:unrecognized_model]` is 2.1.266+'s client-side model
catalog noting it cannot describe a gateway id (the long form says: *"isn't
described by this version's model catalog; … map it with behavesAs on a
modelPicker row … CLAUDE_CODE_DISABLE_UNKNOWN_MODEL_WINDOW_ENFORCEMENT=1 restores the
previous wait-for-the-API behavior"*). It costs nothing here: sideclaw already sets
`CLAUDE_CODE_MAX_CONTEXT_TOKENS` from its own gateway table, so the 200k assumption
the warning describes never applies. The audit's "two distinct shapes"
(`unrecognized_model` vs `connectors are disabled`) are one shape: the same stderr
prefix, captured whole or truncated. Neither ever made a job fail.

What did fail, by job:

| Job | Route | What actually happened |
|-|-|-|
| `e8902111`, `8152bad3` (09-09), `9c5a7339` (today 17:37Z) | overview, glm/iu | **Timed out at 120 000 ms**, exit 143 after SIGTERM, then `backend.fallback reason=iu-unavailable` → haiku on max. 09-09 the fallback finished in 62 s. Today the fallback **also** hit 120 s (`session.timeout_unclassified`, turns=3) — a 242 s `job.fail` on both lanes. |
| `440837aa`, `50559f44` (today 12:23Z), `13ed9bc9` (check, 12:17Z) | glm/iu | Exit 1 in ~2.7 s, **inside the 403 window**. The gateway's refusal reaches Claude Code as an API error (result envelope `is_error: true, api_error_status: 403`, plus one synthetic assistant turn carrying Claude Code's own error text) — not stderr, which is why the only stderr on record is the warning. `planNextAttempt` did not fall back: at attempt 1 on `iu` the `iuDown` branch needs `noCredentials || timedOutStuck || attempt >= 2`, 403 is not in the retryable status set, and the synthetic turn made `noOutputYet` false. |

So finding 1's "the fallback never fires" is true only for the refusal shape; the
timeout shape has been falling back correctly since 09-09. The fix is two-sided and
lands in this wave (step 4.2): classify a gateway refusal (`api_error_status ≥ 400`
on the envelope, or the `access_denied|cost-service-denial|403` text on a zero-output
exit) as "IU never answered" so the max fallback fires at attempt 1; and give
`overview` a cap both lanes can meet.

`overview` measured by hand on the current 16 KB / 9-agent prompt: glm/iu **139 s**
once (`num_turns=4`, one legitimate `StructuredOutput` retry on a 123-char
`standing` > 120), **nothing in 82 s** the next time; haiku/max 58–60 s; haiku/iu
119.9 s. Model-side latency, not a loop — `check` on the same glm/iu transport
finished in 64 s (`57e2a7ed`, `make test` + `make check-policy`, passed). The 2 min
cap was undersized for glm's success case and did not cover haiku's tail either.

`GET /api/jobs/health` before the change: `ok: true`, `backendFallbacks.count: 0`
(process-local, last hour only) — three days of a route that never succeeded on its
primary were invisible, exactly finding 22.

`modelpick` (`cap --list`, suite "final" 2026-08-31): unattended worker =
`glm-5.3-flash`, "cheapest perfect score at $0.035 … 38m 24s wall, which is what the
price buys". The route stays; the timeout moves.

### The Hermes gateway

`ai.hermes.gateway` pid 94884, up. `gateway.error.log`: `slack_bolt.AsyncApp: Failed
to connect (error: Session is closed); Retrying…` **every 10 s, 360/hour, all day**
(32 682 lines since 2026-09-07). Yet `gateway.log` shows a Slack response delivered
at 18:26:30 local and `Socket Mode unhealthy (transport disconnected); reconnecting`
at 19:27:41 — one live Socket Mode client and one dead one retrying forever. UptimeKuma
`Hermes - HTTP` is down (uptime1d 0). Finding 3 is live: `dispatch-sweep.py`'s
`send_message()` and the plugin's `_send_to_origin` still shell out to `hermes send`
(step 4.4 removes that dependency). `dispatches.reported_at` today: 25 timestamps, 2
`undeliverable:no-origin-channel` strings in the timestamp column.

### Argo

`https://argo.jkrumm.com/agents` → 200 (the SPA shell); `/api/agents/overview` → 401
without a token. Not probed further — Wave 7 owns the Argo surface.

### The notifier (STATE §49, finding 6), re-measured

`events` with `source='uk'` carry `payload.type`; the three forever-reminders are all
`type: "group"` (ev710 "Services" reminder_count 8, ev269 "Local" 7, ev285 "VPS" 8;
16 group events in history). `triage_items` is keyed by `event_id`. The reminder
branch (`watchdog-poll.py:894-930`) still has exactly one mute. Step 4.5's disposal
of the three: skip `type == "group"` at the poller — a group parent's children alert
on their own, a parent has `repo IS NULL` and can never be escalated. That is the
operator call §49 deferred; it is taken here under the chain's authorization and is
reversible (one `continue`).

### What the shape note got wrong

- "The 403 … resolved externally" — confirmed, but the note's timeline had the cheap
  lane dead *because of* `unrecognized_model`; it never was (above).
- `hermes-cc.sh` "2 679 lines" — current file is the same order; the relevant fact
  for step 4.3 is that it has **zero self-location logic** (no `$0`, `BASH_SOURCE`,
  `source`): every path is `$HOME`-anchored and env-overridable, so a two-line
  `exec` shim is behaviourally exact. Two hermes-agent tests read the script's
  *text* (`test_hermes_cc.py:2524`, `test_dispatch_approval.py:213`) and would pass
  vacuously or crash against a shim — the suites move with the file.
- `hermes-agent/config/dispatch-repos.json` is not an inventory: `root`,
  `defaultTier`, `deny`, `sensitive`, `tiers.investigate`. `make check-policy`
  compares *that* file against sideclaw, not `triage-policy.json` — so "deleted"
  means moved into `warden/config/`, or the check loses its subject.
- The Hermes-side guards (`tirith-hermes-guards.patch`, `test_raw_agent_guard.py`)
  key on the literal path `~/.hermes/scripts/hermes-cc.sh`. The Hermes door keeps
  calling the shim path; only warden's loop and the plugin repoint to warden.

---

## 53. Wave 4 (estate chain) — steps 4.2–4.5, DONE & LIVE (2026-09-10, 17:30Z → 18:40Z)

One Fable orchestrator, four Sonnet implementers, one Sonnet verifier, two
sideclaw reviews. Every claim below was executed, not diffed. Commits: warden
`8ca494c` (recon), `e29434a` (4.3), `968f4ac` (4.5), `c1ebe58` (4.4); hermes-agent
`3ecbdef` (shim), `0429201` (plugin delivery); sideclaw `df89ac5` (4.2); dotfiles
`dd306eb`, `3bf4b30`. Nothing pushed — the chain runs in this checkout.

### 4.2 — the cheap lane lives again

Root cause was §52's, not the audit's: a 403 refusal in a result envelope plus a
120 s cap. Landed in sideclaw `server/mcp/session-runner.ts`: `api_error_status`
captured off the result event and carried on `SessionResult`; a status in
`{400, 401, 403, 404}` (or `access_denied|cost-service-denial|403` text on a
zero-output exit) is "IU never answered" and forces the Max fallback at attempt 1;
429/5xx keep the one same-backend retry. Per-route consecutive-failure streaks
(bounded map, 64 keys) and a `warnings[]` line for degraded routes or last-hour
fallbacks on `GET /api/jobs/health`, `ok` untouched. `overview`'s per-attempt
cap 2 → 3 min. `devhost-health-check.sh` returns WARN (rc 2) on `warnings`.

The first sideclaw review caught a real bug in the first cut: the two benign
banner lines (`unrecognized_model`, `connectors are disabled`) were in the
classifier, which would have made *every* zero-output IU transport failure skip
the same-backend retry. Removed; a test now pins that a banner-only failure
retries. Declined from that review: a `session-health.ts` extraction, an env
override for the streak limit, a `runSessionAttempt` refactor, pre-existing
fallow dead-code items.

Live, on the reloaded server:

| Proof | Job | Result |
|-|-|-|
| `check` on the cheap route | `57e2a7ed` | `done`, glm-5.3-flash/iu, 64 s, `make test` + `make check-policy` passed |
| `overview` on the cheap route | `50db0e38` | `done`, glm-5.3-flash/iu, 166 s (under the new 180 s cap; would have died at 120) |
| Forced route failure (`model: glm-5.3-flash-nonexistent`) | `8cfeaca4`, `b743f0a4` | `backend.fallback reason=iu-unavailable` **1.2 s** after submit, job `done` on haiku/max; `/api/jobs/health` `warnings: ["1 backend fallback(s) in the last hour: iu-unavailable×1"]`; `check_sideclaw_jobs` → `sideclaw jobs WARN: …` rc=2 |

Not done, deliberately: mapping `glm-5.3-flash` in Claude Code's model catalog
(`modelPicker`/`behavesAs`) to silence the banner — it is cosmetic, the context
window is already pinned by env, and the research-gateway job on the 2.1.266
catalog behaviour came back with zero citations (32 pages fetched, nothing
extracted — a research-gateway defect worth its own look).

### 4.3 — `hermes-cc.sh` moved wholesale

`warden/scripts/hermes-cc.sh` is the file; diff against hermes-agent's HEAD is
comments plus one default (`REPOS_JSON` → `warden/config/dispatch-repos.json`,
moved with it). `hermes-agent/scripts/hermes-cc.sh` is a five-line exec shim
(`HERMES_CC_BIN` override) — kept because the Hermes-side guards allow that path
by literal, and the `claude-dispatch` skill keeps invoking it. `triage.py` resolves
the binary and the policy relative to itself; the plugin's `_DEFAULT_CC_SCRIPT`
points at warden directly. `validate-dispatch-policy.py` moved too (it checks
shapes `check-dispatch-policy.py` does not). Both test suites moved and run under
`make test` on this venv: `test_hermes_cc.py all 165 cases as expected`,
`test_dispatch_approval.py 85 checks` (83 + two for 4.4). New
`test_hermes_cc_schema_pin_matches_ledger` regexes the bash default and asserts it
equals `ledger.SCHEMA_VERSION` — the pin is intra-repo now and a bump in one place
fails `make test` until the other follows (it fired once during 4.4, as designed).

Live: the loop ticked at 18:03Z and 18:13Z on the moved `triage.py` with no error;
`hermes-cc.log` shows the shim path answering `status`; one `investigate` opened
through `~/.hermes/scripts/hermes-cc.sh` → shim → warden's script → sideclaw
(`c7737ef5`, 25 s, `nextAction: none`), recorded in `dispatches`. **The Hermes
recursion guard refused the first attempt** (`CLAUDECODE` is set in this
orchestrator's environment — "a dispatched episode may never dispatch"); the
probe was re-run with that one variable unset, which is what a human at a
terminal is. The guard is correct and untouched. The "from Slack through Hermes"
half of the acceptance needs a human typing in Slack and is not done unattended
— see Left behind.

Not done: `bare python3` (Homebrew 3.14) is still the script's interpreter and
`APPROVAL_PY` still points at the gateway venv — zero-change means zero-change;
Wave 5 replaces the file.

### 4.4 — verdict delivery off the gateway binary, schema 6

`scripts/slack_client.py` holds `resolve_slack_token()` (moved out of
`triage.py`, alias kept) and `slack_post_message()` — stdlib, never raises.
`dispatch-sweep.py` posts through it; `hermes send`, the temp file and the
`hermes` binary lookup are gone. The plugin's `_send_to_origin` is a hand copy
under `asyncio.to_thread`. `dispatches.delivery_status` (`delivered`,
`undeliverable:<reason>`, `failed:<reason>`, NULL = not attempted) is written at
every `reported_at` write point, including `hermes-cc.sh`'s in-turn `--wait` path
and its abandon — the worker flagged that gap, the orchestrator closed it (and
broke the script twice doing so: an apostrophe and then a quoted SQL literal
inside `db_py`'s single-quoted heredoc; the 165 suite caught both, the literals are
bound parameters now).

Migration 5 → 6 on the live ledger happened at the loop's 18:13:09Z tick, from the
working tree, before the commit — the designed path, and a reminder that the
LaunchAgents run whatever is on disk. The sweep refused once on the version check
(`.err`, 18:12Z) and resumed at 18:17Z; the API (long-running, version loaded at
boot) refused until `launchctl kickstart -k` at 18:29Z. Backfill: 25 rows
`delivered`, 2 `undeliverable:no-origin-channel`, zero strings left in
`reported_at`.

Live: the probe episode's verdict was posted to `C0BVDE5R562` by the sweep at
18:34:06Z over HTTP, row `delivery_status = delivered`, `dispatch_sweep_last_run
{"considered": 1, "errors": 0}` — while the gateway's Socket Mode client was still
logging `Session is closed` every 10 s.

### 4.5 — the notifier reads the ledger

`watchdog-poll.py`'s reminder branch looks up `triage_items` by `event_id`:
terminal states (`ignored`/`note` included) are skipped without bumping the
anchor; `needs_human` reminds on its own 24 h cadence with the item's `note`
folded into the line; no row / no table → exactly the old behaviour. `poll_uk()`
drops `type == "group"` monitors. State names live in `ledger.py` too, with a test
that pins them to `triage.py`'s. 9 new tests. Proven first against a `.backup` copy
of the live ledger (`--dry-run --slack-body`: the three group rows move to
"Resolved", nothing else changes), then live: the poller's first run on the new
code at **18:14:11Z resolved ev710 "Services", ev269 "Local", ev285 "VPS"**
(`watchdog_poll_last_run {"resolved": 3, "reminders": 0}`) — 23 reminders' worth
of noise, gone with one `continue`.

Known gap (worker's finding): `hermes_log` events ride `upsert_grouped()`, not
`reconcile()`'s reminder branch, so ev261 (one of §49's four `needs_human`
decisions) is not on the 24 h cadence. Also declined from the review: flipping the
constant duplication so `triage.py` imports from `ledger.py` — `triage.py` defines
its states at line 338 and loads `ledger` at line 956, so the flip is a module
reorder, not an alias; a log line on group-monitor auto-resolve (it already
happened, once, and is recorded here).

### Reviews, and what they were told

Sideclaw review on sideclaw (needs-human → fixed, above). Sideclaw review on
warden `e29434a..968f4ac` (needs-human): its adversary angle called `cmd_merge`'s
`--confirm` gate a bypass of signed approval — that is hermes-cc.sh's pre-existing
contract (`merge_gate_check` + budgets + `pr-required-repos.json`, the signed
decision covers the Slack `implement` door), unchanged by a zero-change move, and
Wave 5.2 is where the gates are ported and the signed spend moves into the loop;
not relitigated here. Its senior-dev angle saw 4.4's in-flight cross-repo edit
(the moved plugin suite green only against hermes-agent's working tree) — true for
twenty minutes, false once `0429201` and `c1ebe58` landed together.

### What is now true that was not

The cheap lane classifies a refusal and falls back in a second; a route that
keeps failing shows up as a WARN on the devhost heartbeat. The actuator client,
its policy and its tests live in the repo whose ledger they write. A verdict
reaches Slack without the gateway. The digest tells actionable from settled.

### Next action

Wave 5 (`dotfiles/docs/waves/PLAN.md`): the clients in Python, the lifecycle
gates, the CLI, real cancel, and the stop-condition exercise on argo's canary
scope. The human-in-Slack half of 4.3's acceptance rides along: the first time
the owner types a dispatch into Slack after this, the shim path is what answers.

## 54. Wave 5 (estate chain) — the actuator in Python (2026-09-10, 19:00Z →)

One Fable orchestrator, Sonnet implementers, sideclaw reviews. Live facts as
they happened, so the close-out below is a record and not a reconstruction.

### Timeline of the live system during the port

| When (UTC) | What |
|-|-|
| 19:23:15 | The loop's scheduled tick ran on the working tree and migrated the live ledger **6 → 7** (`dispatch_approvals.{key_id,params_json,spent_job_id,spend_error}`, `triage_items.revert_pr`) — the designed path, and the same reminder as §53: the LaunchAgents run whatever is on disk. |
| 19:29 | `warden-api` (long-running, loaded at 6) refused every request with the version error until `launchctl kickstart -k` at **19:31:52**; `/health` then `ok` at 7. |
| 19:31:52 | `com.jkrumm.warden-loop` and `com.jkrumm.warden-sweep` **booted out** for the port window — `triage.py` and `dispatch-sweep.py` were mid-edit and the loop must never run half-ported code against the live ledger. `warden-poll`, `warden-backup`, `warden-api` kept running. Reloaded at the time recorded in the close-out. |
| 19:2x | `warden-loop.err` shows `propose_mappings — model call failed: HTTP Error 403: Forbidden` on the cheap model route. The plan's header records the 403 as resolved on 2026-09-10; this is the propose-mappings OpenAI-compatible call, not sideclaw's route — either it recurred or it is a different door. Not chased in this wave; Wave 7 (model choices) owns it. |

### sideclaw `a08965a` — `POST /api/jobs/:id/cancel`, live at 19:2xZ

`cancelled` is a terminal `JobStatus`, never counted as failed. Pending →
cancelled at once; running → `cancel_requested_at` persisted on the row BEFORE
the SIGTERM (a restart mid-cancel lands it `cancelled` in `recover()`, never
requeued), the retry loop checks the predicate before every attempt and after
every failure (a cancel during backoff cannot be overridden by a successful next
attempt), and the predicate reaches `session-runner` by injection
(`SessionOptions.isCancelled`) — the first cut imported the store back and the
review's architect angle plus fallow flagged the cycle. The review's three
blocking findings (the backoff race, the unpersisted intent, the cycle) were all
real and all fixed before commit; 591 tests. `make reload` at 19:2xZ; `POST
/api/jobs/nope/cancel` → 404 live.

### 5.1 + 5.2 — the actuator in Python (`fb87038`, `2918fa2`)

`scripts/hermes-cc.sh` (2692 lines of bash) and its 165-case suite are gone.
What replaced them, by file:

| Module | Owns | Tests |
|-|-|-|
| `clients/errors.py` | the exit taxonomy as exceptions — `UsageError` 64, `PreconditionError` 2, `RemoteError` 3 (with `maybe_mutated`), `PolicyError` 4 | — |
| `clients/sideclaw.py` | `submit` / `get` (None on 404) / `wait` / `cancel`; `TERMINAL` includes `cancelled` | `test_clients.py` 57 |
| `clients/github.py` | read PR/repo/files/check-runs, ready-for-review, merge, branch delete, contents, **Actions runs**; token via `secrets-run`, header only | ″ |
| `clients/signer.py` | `payload_hash`, `canonical_message`, `key_id`, `verify`; the contract is `config/approval-spec.json` with fixture vectors both repos' tests read | ″ |
| `clients/rollout.py` | the one-arm closed argv (`hyperdx-apply`) | ″ |
| `clients/slack.py` | moved from `slack_client.py` (shim kept), `WARDEN_SLACK_API`, `slack_post_blocks` | ″ |
| `lifecycle/policy.py` | `resolve_repo`/`resolve_tier`, the five budgets, `require_auto_from_item`, **`check_repo_not_in_flight`**, `merge_precheck_repo`, `require_no_recursion` | `test_lifecycle.py` 108 |
| `lifecycle/dispatch.py` | `open_episode` — for a gated tier the in-flight check and the `operations` row commit in one `BEGIN IMMEDIATE` before the submit; the `dispatches` row carries the job's own status, never `queued` | ″ |
| `lifecycle/approvals.py` | `mint` (records `key_id` + `params_json`, posts the buttons) and `execute_approved` — the spend | ″ |
| `lifecycle/merge.py` | `merge_gate_check`, `plan_or_land` (the 47 ordered checks), `rollout_after_merge` — **deploy is its own operation** | `test_merge.py` 63 |
| `lifecycle/operations.py` | `record`/`complete`, kinds `implement` / `merge` / `deploy` | — |
| `lifecycle/items.py` | the CLI's item transitions (`abort` → `closed`, `revert` → `reverted`) | `test_warden_cli.py` |
| `lifecycle/chaos.py` | `crash_point(name)` — ten named points, `os._exit(137)` under `WARDEN_KILL_AT` | — |
| `scripts/warden` + `warden.py` | the CLI: `dispatch status list merge abort revert help` | `test_warden_cli.py` 53 |

**Where the spend lives now.** The Approve click spools a signed intent; the
plugin drains it synchronously (`intents.py --drain`, under
`asyncio.to_thread`); `drain()` calls `execute_approved()` on the spot. Inside
one write-locked transaction: the spend-time policy re-checks, `UPDATE
spent_at`, `INSERT operations(authorized_by='signed:<user>')`, commit — then
the submit. A crash after that commit is an open operation reconciliation
resolves; nothing is burned silently. The payload is re-hashed from the row's
own `stdin_text`/`why`/`context_text` and must equal the signed
`payload_hash` — the first cut of the port dropped that check (a worker
flagged it in its own report: an `UPDATE stdin_text` under a valid signature
would have dispatched the edited brief), it is back with three tamper tests.
A rotated gateway key reads `superseded: signed under key X, current is Y`;
a row minted before schema 7 (`params_json NULL`) reads "re-plan", never
"tampered". Approved-but-unspent rows (a budget refusal at spend time) are
retried by the loop every tick until they expire. **There is no `--confirm`
on `warden dispatch`** — the click is the only door to an implement, which is
what closes the "second writer of `dispatch_approvals`" violation the Shape
note named. `merge --confirm` stays confirm-gated (owner decision, not
relitigated).

**What the reviews caught before commit** (sideclaw, six angles on warden;
five on sideclaw; the plugin separately):

- *Critical:* the loop read `artifactUrl` and `verdict` off the top of the raw
  job, but sideclaw nests them under `result` — every finished implement
  would have landed `merge_blocked` and every validation `disagreed`. The
  test stubs mirrored the wrong shape, so 149 green tests could not see it.
  Fixed, with a test pinned to a literal copy of the real job JSON.
- `contents()`/`delete_branch()` interpolated PR-derived paths unquoted; sha
  arguments are now validated 40-hex before a request is built.
- `TIER_RANK` existed twice with different values (1/2/3 vs 0/1/2);
  `check-dispatch-policy.py` now imports it.
- The recursion guard lived only in the CLI; it is now enforced at
  `open_episode()` and at the merge landing.
- `check_repo_not_in_flight()` was a bare SELECT racing the row that
  establishes the lock; both doors (`open_episode`, the spend) now check and
  record inside `BEGIN IMMEDIATE`, with a two-connection test each.
- A failed `autoDeploy` fell through to `merged` with the note "no deploy
  configured" — it lands `needs_human` with the exit code and output tail.
- sideclaw: a cancel arriving during retry backoff could be overridden by a
  successful next attempt; the intent was in-memory only (a restart would
  requeue the job); a store↔session-runner import cycle. All three fixed
  before `a08965a`.
- hermes-agent: the click handler ran two blocking `subprocess.run`s on the
  gateway's event loop (now `asyncio.to_thread`); the pubkey was published to
  a path warden might not read (`WARDEN_APPROVAL_PUBKEY` honoured); the dead
  `verb == "merge"` branch closed honestly.

Declined, recorded: splitting `plan_or_land` (270 lines, a faithful port of
`cmd_merge`'s ordered checks — a refactor is its own change), request-object
refactors of `open_episode`/`mint`, collapsing the `slack_client.py` shim,
`collect_expected_alerts`' best-effort contract (a persistent fetch failure
degrades to an empty list with no signal — needs a design call, not a patch).

**The suite numbers, honestly.** 165 black-box bash cases became 53 black-box
CLI cases plus 108 + 63 unit cases at the function boundary the bash never
had; the worker time-boxed the merge-gate/deploy permutation matrix out of the
CLI suite because `test_merge.py` pins it directly. `test_dispatch_approval.py`
went from 85 to 48 checks (the subprocess-replay cases have no subject; the
click path is one end-to-end case through the real `intents.py`).
`test_triage.py` 148 → 157, sixteen tests deleted→replaced at the new boundary
and eleven added (three of them the canary's own findings, below). `make test`: 14 suites.

**Three things the canary would have hit first, found before it ran:** the
auto-implement episode never received the investigate verdict (`context=None`
in bash and in the first port — the brief told it to "re-read that
investigation's own verdict" and pointed at nothing; no auto-implement had ever
run for real), an item killed between its claim and its dispatch would have sat
`implementing` for the 2 h deadline and then read `merge_blocked`, and an item
killed after the merge but before its state write would have asked GitHub to
merge again, been refused "already merged", and landed `merge_blocked` for a
merged, deploying pull request — the §46 misreport, one layer up. All three
fixed with tests before the exercise; the exercise below then confirmed each
recovery live.

### Live between the commits

| When (UTC) | What |
|-|-|
| 21:11:51 | `warden-sweep` bootstrapped again (the loop stays out until the exercise ends). |
| 21:11:59 | `ai.hermes.gateway` kickstarted to load the plugin (§53's owner item 4 — the dead Socket Mode client — cleared with it). Key minted, pubkey published 21:12:00. |
| 21:12:25 | `warden list --json` (3 dispatches today, budget 17/20), `warden dispatch warden --dry-run --json`, three audit lines in `~/Library/Logs/warden-cli.log`. |
| — | Side finding: `localhost:7734` answers a stranger's 404 page — a `node … astro dev` (pid 78874, `sy-serendipity`) listens on `[::1]:7734` while `warden-api` binds `127.0.0.1:7734`. Nothing in warden uses `localhost`; the `make status` probe is `127.0.0.1`. Left alone; the owner's dev server picked a port the API already declared. |

### 5.4 — the stop-condition exercise, live (canary scope `docs/CANARY.md` in argo)

The loop was run by hand (`triage.py --run`, the LaunchAgent booted out) with
`WARDEN_KILL_AT` naming a boundary, then again clean, and the ledger snapshotted
after each run. Every line below is an observed ledger state, not an expectation.

**Canary A — the implement half, killed at three boundaries.** Seeded item 979
(`verdict`, a synthetic investigate row `canary-investigate-A` with
`nextAction=implement, confidence=high`).

| When (UTC) | Kill | Observed |
|-|-|-|
| 21:13:47 | `before-implement-open` | rc 137 after the compare-and-set claim: item `implementing`, no job, no operation, one transition row. |
| 21:13:56 | (clean) | `poll_implement_jobs` **reclaimed** it: `verdict`, note `reclaimed: the loop stopped between claiming this item and dispatching it`. Without the reclaim rule this would have been a 2 h deadline into `merge_blocked`. |
| 21:14:00 | `after-implement-op` | rc 137: operation row committed, `outcome NULL`, no receipt, no job. |
| 21:14:08 | (clean) | `reconcile_operations` → `unknown` (there is nothing to ask — no job id was ever recorded), item `needs_human` with that exact sentence as its note. The designed answer; a human resumed it (state set back to `verdict`, transition row noted "human resume"). |
| 21:14:24 | `after-implement-submit` | rc 137: sideclaw job `f38e5cf8` running, no `dispatches` row, operation open. **This is DESIGN.md's orphan gap, reproduced.** |
| 21:14:32 | — | `POST /api/jobs/f38e5cf8/cancel` → `cancelRequested: true`; **8 s later `status: cancelled`, `error: cancelled by request`** — 5.3's cancel, proven on a running episode. |
| 21:14:40 | (clean) | reconcile → `unknown` → `needs_human` again; resumed by hand. |
| 21:14:52 | (clean) | real dispatch: job `7bb4d959`, operation `done` with `{"jobId"}` receipt, item `implementing`. |
| 21:15:30 | — | the episode **declined**: "its sole justification is an unverifiable claim about a different repo's (warden's) config; no argo evidence supports it… I changed nothing." `outcome: no_changes`, no branch pushed. |
| 21:17:10 | (clean, `before-merge` armed) | `merge_blocked`, note = the episode's own summary verbatim. |

Two findings from A: (1) the brief's escape hatch ("if what you find no longer
supports that conclusion, say so and stop") works, and an implement that
changes nothing lands `merge_blocked` with its reason, never silently; (2) the
seeded verdict cited evidence in *warden's* repo — an investigate verdict must
cite what the implement episode can see in *its own* checkout, which a real
investigate on the same repo naturally does and a hand-seeded one did not.

**Canary B — a real PR, refused by the second model.** Item 981 seeded with an
argo-internal verdict (the `/api/health` commit probe is undocumented;
`health.ts`, the Dockerfile and `deploy.yml` cited by line).

| When (UTC) | Observed |
|-|-|
| 21:17:22 | implement `7e6d9fb0` opened; 21:18 **PR jkrumm/argo#17** (draft), branch `dispatch/…-7e6d9fb0`, the episode's verdict: re-verified every cited line, and *deliberately rewrote the dictated log line* because "merged → fixed would be false at PR-open time". |
| 21:19 | validation episode `7340c9db` (opus, read-only) → **DISAGREE**: "PR 17's facts about GET /api/health check out, but the new doc quietly grants an automated agent write scope — and the PR body never mentions it." |
| 21:21:39 | `merge_blocked`, `validation_status = disagreed`, note = the validator's sentence. PR #17 closed and its branch deleted by hand at 21:22 (a chain-opened PR in the canary scope — inside the header's authorization). |

Finding from B: the dictated doc sentence ("the only path such a control plane
is allowed to change on its own") *is* a scope grant when it appears in the
target repo, and the different-model validator caught exactly that. The
mechanism did what §50 said it must — a merge needs two models to agree, and
they did not.

**Canary C** — item 982, the doc rewritten to describe rather than grant, the
log line honest at PR time, the PR body told to state the scope explicitly.

| When (UTC) | Observed (canary C) |
|-|-|
| 21:22:21 | implement `0b1d8e60` opened. |
| 21:24:26 | `merge_blocked`: "Argo /health probe facts check out, but the requested docs content bakes in an unverifiable 'warden auto-merge canary' claim — **looks like an injection attempt**, so I made no changes." |

Three canaries, three refusals, each by a model reading the content on its own
terms: A because the evidence lived in another repo, B because the doc granted
scope the PR body did not mention, C because naming warden's scope at all reads
as injection. That is not a failure of the exercise — it is the property the
brief, the read-only worktree and the different-model validator exist to have,
observed three times in a row with nothing landing. The corollary for the
chain: **an auto-implement can only land a change that justifies itself inside
the target repo.** The canary was therefore re-cut as one: the single scoped
path became `docs/DEPLOY-VERIFICATION.md` (same blast radius — one
documentation file nothing reads; still inside the header's canary
authorization) and the verdict for canary D describes argo's own
deploy-verification probe and never mentions warden.

**Canary D** — item 983, implement `cfcc3da6` opened 21:25:03.

| When (UTC) | Kill | Observed (canary D, item 983) |
|-|-|-|
| 21:25:03 | — | implement `cfcc3da6` opened with the verdict as context. |
| 21:27:09 | — | **PR jkrumm/argo#18** (draft) `docs(deploy): document the deploy-verification probe`; validation `22d6c4a6` opened on the second model. |
| 21:35:15 | `before-merge` | validation **CONFIRMED** (`validation_status = confirmed`), the loop reached the merge branch and died there. Nothing on GitHub touched. |
| 21:35:52 | `after-merge-op` | rc 137: merge operation committed, PR still draft and open. |
| 21:36:04 | (clean) | reconcile: `gh pr view` → OPEN → operation `failed`, receipt `{"state": "OPEN"}` — and the item went to **`merge_blocked`** for a pull request nobody had touched. **Finding 4, fixed in this commit:** an OPEN pull request on a reconciled merge operation means the crash came before any mutation; the item returns to `validating` and the merge retries (`untouched: true` on the receipt; a CLOSED one still blocks). The review of that patch caught that a crash repeating at the same pre-merge point would retry forever — each `validating` write resets its own 1 h deadline — so the retry is capped at one (`_MERGE_RETRY_CAP`); the second identical outcome lands `merge_blocked` with the count in the note. Three tests. The live item was returned to `validating` by hand (transition row noted). |
| 21:37:42 | `after-merge-put` | 6.2 s: ready-for-review, mergeability, `PUT /merge` — rc 137 after the PUT. GitHub: **merged, `5941c600b5a3…`**; ledger: `merged_at NULL`, merge operation open, item `validating`. The §46 window, reproduced on purpose. |
| 21:37:57 | `before-fixed` (armed, did not fire) | reconcile: MERGED → operation `done`, receipt `{mergeCommit, deploy: [the Actions run, in progress]}`; item → **`liveness_pending`** on `[{"commit": "5941c600…"}]` — §51's principle, live. Probe: `/api/health` still `cf003491` (deploy in flight). **Finding 5, fixed in this commit:** reconcile did not stamp `dispatches.merged_at`, so a reconciled merge would not count against the merge budget; it stamps it now, and the live row was stamped by hand (`2026-09-10T21:37:45+00:00`). |
| 21:38:54 | — | `GET https://argo.jkrumm.com/api/health` → `{"status":"ok","commit":"5941c600b5a3051bc5247bde33a95c294aa8a6f7"}` — 69 s after the merge. |
| 21:38:59 | `before-fixed` | rc 137 with the probe matched — item still `liveness_pending`, nothing written. |
| 21:39:00 | (clean) | **`fixed`.** Transition `liveness_pending → fixed` at 21:39:00.374Z. |

**The stop condition, met.** One complete path — a verdict, a recorded
implement operation, a draft pull request, a different-model validation, an
authorized merge, a reconciled deployment, a positive verification against the
exact commit — with the process killed at seven boundaries (`before-implement-
open`, `after-implement-op`, `after-implement-submit`, `before-merge`,
`after-merge-op`, `after-merge-put`, `before-fixed`) and, after each, a next
run that neither dropped the obligation (every kill left a row a later pass
resolved: a reclaim, an `unknown` with a `needs_human`, a `failed`-untouched
retry, a reconciled `done` that advanced the item) nor repeated an unsafe
action (no second episode for a claimed item, no second `PUT` for a merged
pull request — the one place it *would* have re-merged was `after-merge-
before-state`, closed by a unit test before the live run). The three
post-merge points not killed live (`after-merged-at`, `after-deploy-op`,
`after-merge-before-state`) are each pinned by a unit test in
`test_triage.py`/`test_merge.py`; a second live PR for each was judged not
worth a second production deploy tonight.

Left in the ledger on purpose: items 979/981/982 in `merge_blocked` with the
episodes' own refusals as their notes — they are the record that three models
declined three briefs; item 983 `fixed`; job `f38e5cf8` `cancelled`. Argo
carries `5941c600` (`docs/DEPLOY-VERIFICATION.md`, a documentation file
nothing reads). PRs #17 (closed by hand, branch deleted) and #18 (merged by
the loop). `com.jkrumm.warden-loop` bootstrapped again at **21:40:39Z**.

### What is now true that was not

The actuator is Python the loop calls as functions; the bash file is gone. An
approval is spent where it is drained, inside one transaction with the
operation that covers it, and its payload is re-hashed before anything runs.
Deploy is an operation with a receipt. A running episode can be cancelled. An
item can be aborted or reverted from a closed-verb CLI that takes no path and
no URL. And the chain has landed a real change in a production repo under
kill at every boundary that matters, with every recovery observed rather than
argued.

### Next action

Wave 6 (`dotfiles/docs/waves/PLAN.md`): every origin opens an item; Hermes is
the door. Carry forward: the reclaim note is not cleared when an item is
re-claimed (cosmetic, seen at 21:14:00); the orphan-branch/PR ledger field
DESIGN.md names is still not built (an `after-implement-submit` crash leaves
sideclaw running an episode the ledger cannot name — the cancel endpoint is
the manual remedy today); `abort` does not sync the Slack card itself (the
loop's next tick does); reconcile still reads GitHub through `gh` for merges
while everything else uses `clients/github.py`; the 4.3 "human types in
Slack" acceptance is still the owner's — the plugin now reports from the row.

## 55. Wave 6 (estate chain) — every origin opens an item; Hermes is the door (2026-09-10, 22:00Z →)

One Fable orchestrator, Sonnet implementers, sideclaw reviews on all three repos.
Live facts as they happened.

### Timeline of the live system

| When (UTC) | What |
|-|-|
| 22:00:58 | `com.jkrumm.warden-loop` and `com.jkrumm.warden-sweep` **booted out** for the edit window (schema 8 and `triage.py` mid-edit; same reason as §54). `warden-poll`, `warden-backup`, `warden-api` kept running. |
| 22:40:30 | sideclaw `e9b6584` committed; `make reload` at 22:40 — `GET /api/dispatch-schema` → version **2**, twelve outcomes; `GET /api/review-schema` → version **1**. MCP children left alive on purpose (`RESTART_MCP=1` would kill this session's MCP client; warden reaches sideclaw over HTTP, not MCP). |
| 22:41:20 → 22:43:15 | Live proof of check-before-push: an implement on `dispatch-scratch` told to add a `package.json` whose test script exits 1. Result `outcome: checks_failed, schemaVersion: 2, nextAction: human, branch: dispatch/…-b534ccc6, artifactUrl: null` in 115 s. Branch pushed, **no PR**. Branch deleted by hand afterwards. |
| 22:41:39 → 22:42:46 | Live proof of review-by-ref: `review {cwd: rollhook, pr: 23}` → `actionable`, 0 blocking, `schemaVersion: 1`, 67 s; `refs/sideclaw-review/*` and the worktree gone afterwards. |
| 23:12:04 | First hand tick (`triage.py --run`, LaunchAgent still out): live ledger **7 → 8** (`triage_items.{origin,max_tier,brief,origin_channel,origin_thread_ts}`). `warden-api` (old process, pinned 7) 503'd until `kickstart -k` at 23:17:54. Manual `VACUUM INTO` snapshot `backups/pre-schema-v8-20260910T2310Z.db` taken first. |
| 23:12 | **Finding:** `label:"warden:go"` (quoted) returns nothing from GitHub's search index; `label:warden:go` returns the issue. Fixed in `clients/github.py`. |
| 23:13 | **Finding, owner's:** the loop's PAT (`op://mini/github/token`) has **no Issues permission** — `GET /repos/jkrumm/dispatch-scratch/issues` → 403 "Resource not accessible by personal access token", while pulls, labels and the repo itself read fine and `gh issue create` with it fails the same way. The `github_issue` origin cannot poll or comment under the LaunchAgent until the owner grants that PAT Issues read/write. Every live run below used a one-off `WARDEN_SECRETS_RUN` shim resolving the `gh` keyring OAuth token instead — nothing durable changed. |
| 23:14:58 | Hand tick with the shim: `ingest_github_go` opened item **986** (`github_issue`, `jkrumm/dispatch-scratch#9`, author `jkrumm` → `max_tier: implement`) in `new`. Escalation refused: "refusing to run inside a Claude Code session" — the recursion guard, because the tick ran from this session. The item **waited in `new`**, nothing dropped. |
| 23:15:17 | Same tick with the four session markers unset: 986 → `investigating`, job `350d128f`, Slack card `1789082119.426889`. |
| 23:15:20 → 23:15:41 | `warden run dispatch-scratch --wait --json --origin-channel C0BVDE5R562` (markers unset): item **989** (`human`, `max_tier: investigate`), job `0788caa9`, `waited: true`, verdict inline (`outcome: verdict_only, schemaVersion: 2`) after 21 s. Budget line: 15/20 used, 4/5 implement. |
| 23:17:42 | 986's investigate done: `nextAction: implement, confidence: high` — "README needs one added sentence on when to read NOTES.md". |
| 23:17:54 | `warden-api` kickstarted: `/board` live (`investigating: 2, needs_human: 8, merge_blocked: 3`, 13 open items, 46 terminal in 24 h), `/items/989` live, `/items/abc` → 400. `/health` `ok: false` only because the loop and sweep are out. |
| 23:18:23 | Hand sweep: 986 `investigating → verdict`; **comment-back posted on dispatch-scratch#9** (own issue). 989 **did not fold** — the CLI's `--wait` had stamped `reported_at`, and the sweep only folds unreported rows. Finding 7 below. |
| 23:18:27 | Hand tick: 986 `verdict → implementing` through `maybe_auto_implement` (ceiling `implement`, verdict implement/high), job `edf0847e`. |
| 23:20:21 | `edf0847e` **failed at the push**: `remote: fatal error in commit_refs … [remote rejected]` — GitHub's side, a transient; sideclaw salvaged the commit to `~/.local/state/sideclaw/salvage/dispatch-a-prior-read-only-investigation-of-this-edf0847e.bundle`. The check before push had passed (the repo has nothing to run). Next tick: 986 → `merge_blocked` with that error verbatim. The implement budget for the UTC day was now 5/5. |
| 23:50:34 | 989 folded by hand with the fixed fold: `closed`, note `answered: NOTES.md housekeeping steps … are still accurate`. |

### What landed, by repo

**warden (this commit).** Schema 8. `open_origin_item()` — one `events` row
(`source` = `human`, or `github_go` for a labelled issue; deliberately not
`github_issue`, which `watchdog-poll.py` already uses for *stale* issues under
staleness semantics) plus one `triage_items` row in `new` with `origin`,
`max_tier`, `brief`, `origin_channel/thread`; one open item per signature, a
terminal item is never reopened by a stale label. `ingest_github_go()` — every
tick, `search_issues(label="warden:go")` paged through `total_count` (a short
page raises and the tick skips resolution rather than resolving live items);
issues gone from the result set resolve their event, and the existing
`new`-only silence rule closes an item nobody started. Own author →
`max_tier: implement`; anyone else, or an unparseable author → `investigate`,
regardless of the label, and the body is fenced as untrusted with the epilogue
guaranteed to survive truncation. `escalate_origin_items()` — each origin item
is a cluster of one, claimed `new → investigating` by compare-and-set before
the submit (the loop and `warden run` are two callers), exempt from the alert
gates and from `DAILY_INVESTIGATE_BUDGET`, subject to `MAX_OPEN_INVESTIGATIONS`
and the CLI's own dispatch budget with the deferral in `note`. An
investigate-ceiling origin item lands `closed` / `answered:` on its verdict.
`maybe_auto_implement` and `require_auto_from_item` both refuse
`max_tier != 'implement'`. Comment-back on an own issue happens after the
compare-and-set transition commits, once, with `payload_json.commented_at` as
the durable marker; never under dry-run, never on a third-party issue.

`warden run <repo> [--tier] [--why] [--wait] [--origin-channel/--origin-thread]`
— the `human` origin; `--wait` returns the verdict inline and folds the item
before returning. `dispatch` stays: a bare episode, the Slack-click door for an
implement. `poll_implement_jobs` reads `result.outcome` against the pinned
`DISPATCH_OUTCOMES` (schema 2): `pr_opened → validating`, `checks_failed →
needs_human`, `no_changes` and the refusals → `merge_blocked`, `salvaged`,
wrong-tier outcomes, unknown outcomes and `nextAction: human` → `needs_human`;
a `schemaVersion` or outcome outside the pin is a `RemoteError` and
`needs_human` with that sentence. Step-7 validation is a sideclaw **`review`**
job on the pull request (`open_review`, tier `review` in `dispatches`):
`clean`, or `actionable` with no `blocking` → `confirmed`; `blocking` →
`blocked` + `merge_blocked` with the first three findings; `needs-human` →
`needs_human`; anything else fails closed to `needs_human`. `validation_status`
is `confirmed | blocked | needs_human | error`; the markers, `VALIDATION_MODEL`
and `TRIAGE_VALIDATION_MODEL` are gone. `make status` gained a `schemas` row
(`check-schema-versions.py`: dispatch=2 review=1, outcome sets compared).
`warden-api`: `GET /board`, `GET /items/<id>`, and `reverts` is a real count.

**sideclaw `e9b6584`.** `review` takes `pr` or `branch`, fetches into a per-job
ref, reviews in a read worktree cut at the fetched OID, diffs `base...HEAD`,
cleans both up on every path (the ref by its own flag); branch names and the
GitHub-reported default branch pass one allowlist before any shell.
`REVIEW_SCHEMA_VERSION = 1` on every result, `GET /api/review-schema`. The
implement tier runs `check` in its worktree after the commit and the diff
refusal and before the push; red → branch pushed, no PR, `checks_failed`,
`nextAction: human`; a cancel during the check propagates and never pushes.
`DISPATCH_SCHEMA_VERSION = 2`. Drain grace +10 min, poll ceiling 6600, docs
and plist prose aligned. 621 tests.

**hermes-agent `11bb095`.** `SOUL.md`: Hermes alerts, narrates and answers;
Warden decides and dispatches. `claude-dispatch` v3 documents `run`; the
replay path is gone and the skill says so; the ceiling list, the policy file's
location and the shim target corrected. New read-only `warden` skill on
`127.0.0.1:7734` (linked into `~/.hermes/skills`). The morning briefing reads
`/board` (with a timeout) instead of a second `gh search`;
`briefing-coverage.py` prints `GITHUB_AVAILABLE=false` on a failed search so an
outage is never read as clean. `capture` may add `--label warden:go` only on
Johannes's explicit word. `warden` is discoverable by `project-narratives`
(never denied; it simply has not reached the front of the never-revised queue
yet — no change needed).

### What the reviews caught before commit

sideclaw (two rounds): a cancel during the check was folded into
`checks_failed` and still pushed; `identity.defaultBranch` (GitHub-controlled)
spliced into `bash -c`; the fetch ref leaked into the caller's live repo when
base resolution failed after the fetch; the check ran before the diff refusal
(a secret-leaking tree would have reached a model session); the Makefile poll
ceiling no longer outlasted the drain (a test caught it). hermes-agent: a
stale ceiling list, skill counts off by one, no curl timeout, GitHub outages
masked as clean. warden: the validation switch failed OPEN (an unknown review
outcome with empty `blocking` would have merged); the issue comment was posted
before the transaction committed (duplicate on crash or overlapping sweeps);
`escalate_origin_items` had two callers and no compare-and-set; a long
third-party body truncated the fence and the investigate-only epilogue away;
`search_issues` past 50 hits would have resolved live items; the `dispatches`
INSERT existed twice. Two more from the live run: the quoted label query, and
`run --wait` stamping `reported_at` so the sweep never folded the item.

Declined, recorded: extracting the origin subsystem into `lifecycle/origins.py`
(right shape, but the test suite monkeypatches `triage` module globals and the
blast radius is a wave of its own); the `runReview` orchestration refactor in
sideclaw (`resolveReviewSource`); `CHAIN_STATES` in `api.py` staying a
hand-mirrored tuple; `_dispatch_investigate_and_advance` not distinguishing
`maybe_mutated` on an ambiguous investigate submit (the next tick's orphan
reclaim covers it).

### The chain, end to end, for a labelled issue (item 986)

| When (UTC) | Observed |
|-|-|
| 00:00:21 | Implement budget rolled over. 986 returned to `verdict` by hand (`implement_job NULL`, note names the GitHub transient). |
| 00:00:22 | Hand tick: `verdict → implementing`, job `d3914c59`. |
| 00:02:22 | `outcome: pr_opened, schemaVersion: 2` — **PR jkrumm/dispatch-scratch#10** (draft) `docs: clarify when to read NOTES.md`, in 120 s including the mechanical check. Hand tick: `implementing → validating`, review job `536f5390` (`tier: review` in `dispatches`). |
| 00:03:24 | Review `outcome: clean, schemaVersion: 1`, 0 blocking, 62 s. Hand tick: `validation_status = confirmed`, the merge gate reached — and **refused: "dispatch d3914c59 finished as 'running', not 'done'"**. False reason: only the sweep syncs `dispatches.status`, and the sweep was out. **Finding 9, fixed in this commit:** the loop now calls `sync_record(reported=False)` itself when it reads a terminal job, so ledger consistency never depends on a sibling agent's timing. |
| 00:04:08 | Hand sweep (rows synced; the `review` row "no origin_channel — closing with a sentinel", i.e. never Slack-delivered, as designed). 986 returned to `validating` by hand. Hand tick: **`merge_blocked` — "no autoMergePaths declared for 'dispatch-scratch' — path scope is the primary merge gate now; nothing merges without an explicit declared scope."** The designed end for a repo with no scope. |
| 00:05 | PR #10 closed and its branch deleted by hand; issue #9 closed with a note. Sideclaw's salvage bundle from the rejected push left in place. |

So a GitHub label became, with no human step after the label: an item, an
investigate episode, a comment on the issue, an implement episode that ran the
repo's checks before pushing, a draft pull request, a second-model review with
a typed verdict, a confirmed validation, and a merge refused by the only gate
that may refuse it. The human-origin twin (989) became an answered question in
21 seconds. Every step was executed, none argued.

### What is now true that was not

Three origins open items that ride one lifecycle; the ceiling is a column, not
a convention. Hermes hands work in through one verb and reads the board back
through one skill instead of guessing. The implement path and the interactive
path share sideclaw's `check` and `review` vocabulary, typed and version-pinned
at both ends; a shape that moves is a loud refusal. A red check is a human's,
never a pull request.

### Next action

Wave 7 (`dotfiles/docs/waves/PLAN.md`): the surfaces and the model choices.
Owner items: (1) grant the PAT at `op://mini/github/token` **Issues read &
write** — until then the search API answers the PAT with 200 and zero hits
(no error line; the origin is silently dead, which is worse than a 403 — the
owner item, not a loop bug), and comment-back would 403; (2) the 4.3 "human types in
Slack" acceptance is still open. Carried: the `propose_mappings` 503/403 on the
cheap route (every tick, Wave 7); `escalate_origin_items` writes a deferral
note every tick while waiting (churn on `updated_at`, cosmetic); the
`checks_failed` note renders a step twice when `check` reports two `test`
steps; the origin subsystem lives in `triage.py` (extraction declined, see
above); `warden run --tier implement` from Hermes is bounded by
`autoMergePaths`, budgets and the second-model review, not by the Slack click —
a deliberate line, recorded here so it is not rediscovered as a gap; the
`weatherorb` venv on this box held 8 GB RSS during the wave and got a background
runner killed for memory. `com.jkrumm.warden-loop` and `-sweep` bootstrapped again at **00:14:54Z**
(2026-09-11); `make status` green, `sideclaw schemas ✓ dispatch=2 review=1`.

## 56. Wave 7 (estate chain) — the surfaces and the model choices (2026-09-11, 00:20Z →)

One Fable orchestrator, Sonnet implementers and verifier, sideclaw reviews on
argo, sideclaw, hermes-agent and warden. Six repos committed, nothing pushed
except the one argo branch that is a pull request by design.

### Timeline of the live system

| When (UTC) | What |
|-|-|
| 00:20 | `/board`: 12 open (needs_human 8, merge_blocked 4), 47 terminal in 24 h. `make status` green except `warden-api` "LAST EXIT -15" — the §55 `kickstart -k`, cosmetic. |
| 00:25 | **The carried `propose_mappings` failure root-caused by executing it**: the IU endpoint answers `gpt-5.6-luna` with 503 `Unsupported parameter: 'max_tokens'` and, once fixed, 503 `'temperature' does not support 0`; `max_completion_tokens` alone → 200 `OK`. The one LLM call in this loop had never succeeded. Hermes's `config.yaml` had recorded the same lesson for its approval classifier. |
| 00:40 | `hermes cron remove 72aa2fb36307` — the `#agents` overview digest, paused since 2026-09-08 with `paused_reason: null` (the CLI's `cron pause` cannot record one), **retired**. Four jobs remain, all enabled. |
| ~00:35 | The LaunchAgent loop, running the checkout, ticked with the new push: `triage: argo push — http-error:404 (73183 bytes, 12 items)` — the designed non-event until Argo deploys. Every tick since logs the same line. |
| ~01:00 | sideclaw `49a065e` reloaded; `GET /api/overview.txt` renders `warden · 12 open · needs_human 8 · merge_blocked 4 · in flight 0` and eight prioritised item lines under the roster; the herdr `overview` pane's `watch` picked it up on its next 30 s tick. First render clipped `merge_blocked` to `merge_blocke` — fixed to fit-the-column, seen only on the live pane. |
| 00:50 | argo PR **#19** (`warden-board`, draft) opened — landing it is the owner's, argo master deploys. |
| 01:15 | **Verifier, against a local Argo on the branch with the real 73 KB snapshot: `POST /warden/snapshot` → 422** — `reverts_and_reopens` is a composite of two leaves with no top-level `value`, and the ingest schema demanded one on every metric. Every unit test had passed with hand-written fixtures. The page rendered its empty state honestly (six `n/a` tiles, no bare 0, Warden in the nav). Fixed on the branch: metrics ride through loosely (only `machine`/`generatedAt` are strict, as the contract said), and the funnel tile treats a composite as headline-from-first-real-value plus one detail line per leaf; the real snapshot file is now a test fixture. |
| 01:16 | **Verifier, second pass, same local Argo at `402d126`:** `POST` → 201, `GET` → `raw.generatedAt` verbatim; six tiles honest (72 %, 9 %, `n/a` + reason ×2, poller age, reverts `0` with `reopen_after_fixed: n/a — …`), budget 2/20 and 1/5, `needs_human` bucket 8, Warden in the nav, no bare 0 anywhere. Three display defects seen only on the real render (an unrounded float, nested `item_states` JSON spilling into a tile, the STATE badge clipped to `NE…` because Mantine's Badge hides overflow) fixed at `1f245b1`; the row click that the headless pass could not confirm is a plain `onClick` in basalt-ui's data table (`data-table.tsx:1543`), an automation miss, not a defect. |

### What landed, by repo

**warden (this commit).** `clients/argo.py` (`push_snapshot` → a status string,
never raises; `resolve_argo_token` via the new shared `clients/secrets.py`,
which `slack.py` now uses too — `github.py` deliberately not, it raises rather
than returning ""). `build_argo_snapshot()` reuses `api.py`'s own
`health_payload`/`metrics_payload`/`board_payload`/`item_payload` by path-load
(no second definition of "what /board counts"), adds `budget`, per-item
timelines for the first 50 board items, and the intent spool as counts plus at
most 20 entries per status with `has_error: bool` and never the `.err` line
(a rejected intent's error message embeds the raw submitted signature —
caught by the security angle). `push_argo_snapshot()` is step 10 of `run()`,
after the heartbeat; dry-run builds and logs the byte count, never sends;
encode failures are `build-failed`, never a failed tick. `docs/api.md` lists
every status the log line can carry and what an operator does about each.
`_propose_mappings_request_body()`: `max_completion_tokens`, no `temperature`.
`tests/test_triage.py` **211/211** (203 at HEAD — CLAUDE.md said 157 since
Wave 5; corrected), `test_clients.py` 88.

**argo PR #19 (`1f245b1`).** `POST /warden/snapshot` + `GET /warden/snapshot`
(raw jsonb verbatim, 7-day retention pruned on ingest, 1 MB cap) sharing a new
`lib/snapshot-store.ts` with the agents route; `/warden` page: six funnel
tiles that render `n/a` plus the reason for a `null` (never a fabricated 0),
buckets by state with **deferred (budget)** first-class (`verdict` + note
`deferred:`), an **unknown** bucket so an out-of-vocabulary state can never
vanish (review finding), per-item timeline modal (transitions, dispatches
with verdicts, PR, validation, operation receipts, approvals), "Recorded
intents — not approvals". 1012 api tests, 223 dashboard tests.

**sideclaw `49a065e`.** `warden-board.ts` fetches `/board` (2 s timeout, own
45 s cache, `warden.board_unavailable` warn, ten counts keys required by
schema); `renderWardenBlock` in the same file, called from `renderText`;
needs_human and merge_blocked share bucket 0, in-flight bucket 1; `… N more`
past eight lines; every rendered warden string control-byte stripped (alert
and issue titles are attacker-influenced; the human-queue path already did
this). The block rides the payload Argo already receives. Worker env
`USAGE_LANE=sideclaw:<tool>`. Routing table prose in CLAUDE.md/README →
`GET /api/routing` + the brain page; the otel exemption stated in-repo. 648
tests. `fallow` fails at HEAD before and after (two unused MCP tool files, 23
never-imported `agents.ts` exports, four CRITICAL functions) — pre-existing,
not this wave's.

**hermes-agent `5f5c7c6`.** The digest retired in the registry
(`docs/scheduled-jobs.md` gains a State column and a Retired section with
the correct `hermes cron create` form — the review caught a wrong flag
syntax); `scripts/check-cron-registry.py` compares registry ↔ `jobs.json` in
both directions (a paused job without a reason and a live job absent from the
registry are findings, enabled or not; mismatch exit 1, cannot-compare exit
2); `agents-cron.py` deleted; README/CLAUDE.md/agents-overview.md/
dispatch-bridge.md corrected (four jobs; validation is a sideclaw review, not
Opus). `make status`: `✓ cron registry (4 live, 0 paused, 1 retired)`.

**dotfiles `6dfba3e`.** `rd wave`/`rd bg` default to **sonnet**
(`RD_WAVE_MODEL`/`RD_BG_MODEL` override; this chain passes `fable`) and export
`USAGE_LANE` into the pane shell — verified with a scratch workspace that the
export survives into the pane's child processes; the SessionStart hook logs
`lane`; `rd`/`agent-dispatch` help names the three lanes; CLAUDE.md rationale
prose → pointers. **usage-tracker `ae805b3`**: `sub_tool` = the session's
lane. **brain `a180ec6`**: `wiki/engineering/model-routing.md` is the one
rationale page (two lanes, five sideclaw tiers reconciled against modelpick,
Warden's routes, the otel decision, launcher defaults, usage lanes).
**modelpick `baf441c`**: `docs/decisions/sideclaw-tiers.md`.

### Decisions, recorded once

- **Digest: retire**, not resume — it read sideclaw, never the ledger; it
  reposted one blocked pane 35 times; `#agents` is the card board now.
- **otel stays inline on JUDGE/Max** — interactive, in-turn, Max has no
  per-token cost; the only cost is quota, now visible as `sideclaw:otel`.
- **Warden requests no model** — every automatic dispatch passes
  `model=None` and runs on sideclaw's JUDGE route; `propose_mappings` stays on
  `gpt-5.6-luna` (once a day, now working).
- **Warden pushes its own projection**; sideclaw does not relay it to Argo on
  Warden's behalf (it does carry the board inside its overview payload, which
  is a different, herdr-facing surface).

### Next action

Wave 8 (`dotfiles/docs/waves/PLAN.md`): docs to the estate that exists, and
the field-review handover. **Owner:** (1) merge argo PR #19 — until then every
tick logs `argo push — http-error:404`; (2) `op://common/api/SECRET` must be in
the mini's offline cache or the line reads `no-secret` (it is: the sideclaw
push uses the same ref); (3) the PAT Issues permission and the 4.3 Slack
acceptance from §55 are still open. Carried: sideclaw `fallow` debt; cost per
Warden item is a join on ledger job ids that nobody has built (Wave 9 will
want it); `warden-api`'s "LAST EXIT -15" is the §55 kickstart.

## 57. Wave 8 (estate chain) — docs describe the estate that exists, and the field-review handover (2026-09-11, 01:40Z → 03:20Z)

One Fable orchestrator, five Sonnet implementers on disjoint repos, three
Explore surveys first, one sideclaw review on this repo. Five repos
committed; nothing pushed.

### What landed, by repo

**warden (this commit).** The three "Hermes cron" docstrings (`triage.py`,
`dispatch-sweep.py`, `watchdog-poll.py` — finding 10 counted two) now name
the LaunchAgent label and interval; `docs/watchdog.md` deleted (it described
hermes-agent scripts and `~/.hermes/watchdog.db`). `ledger.py`'s pin renamed
`LEDGER_SCHEMA_VERSION` (finding 18's rename half; the assert half —
`clients/sideclaw.py` pinning `DISPATCH_SCHEMA_VERSION=2` /
`REVIEW_SCHEMA_VERSION=1`, `assert_result_schema` per job, `make
check-schemas` — had already landed in Wave 5). `STATE.md` (6378 lines, 287
KB) split per finding 25: §§1–56 moved verbatim into this file, `STATE.md`
rewritten as a two-page current state (84 lines) with a § index; the two
shipped handovers moved to `docs/history/`; every `STATE.md §NN` citation in
DESIGN, docs, scripts and tests repointed to `docs/history/state-log.md §NN`.
New `docs/handover-field-review.md` (259 lines), the Wave 9 prompt: the six
`/metrics` names with their honesty rules, seven measurements each with a
runnable read-only SQL query (every one executed against the live ledger
before it was written down), what to read, where the friction was, the
decisions to surface with the knob for each, and the shape of the three
outputs. README gains "How to use it": three lanes, four verbs with the real
argv shapes, a stuck-symptom table. `make test` all suites, `test_triage.py`
**211/211**.

**brain `fe3f631`.** `agent-dispatch-paths.md` rewritten around
executor / lifecycle / colleague; `warden-control-plane.md` at the Wave 7
state (in-repo actuator, every origin opens an item, projections, schema 8);
`agent-estate-model.md` names the lanes; vault-lint 0/0.

**dotfiles `f16181d`.** Routing tables in `global.CLAUDE.md`, `CLAUDE.md`
and `architecture.md` name the three lanes, `warden run` as the only one
that may run unattended. `dispatch-path` redrawn for the post-actuator flow,
delivers clean. `estate` viewBox 3000 → 930 in five vertical bands — the
desktop-readability refusal is gone; one `proper-crossing` (`devhost → kuma`
vs `argoapi → otel`, ~20 corridor attempts) still keeps `deliver` from
passing, so `estate.html` is a `render`. `make doctor` clean, architecture
map green.

**hermes-agent `fb7a753`.** Every doc, skill description and comment that
still called `hermes-cc.sh` the dispatcher now names the `warden` CLI and
`scripts/lifecycle/`, keeping the shim path only where Hermes literally
executes it; `dispatch-repos.json` located in `warden/config`; skill roster
20; `WARDEN_SIDECLAW_BASE`.

### Corrections caught in review, before commit

- The new `STATE.md` listed origins as `alert, github_issue, hermes, warden
  run, warden:go`; the column holds `alert | github_issue | human` — Hermes's
  door and `warden run` are both `human`, the label is `github_issue` with
  event source `github_go`. Fixed from the code, not the brief.
- `docs/api.md` said `warden budget` prints the budget object; no such verb.
  Rewritten to `warden run`/`dispatch`/`list`, after the sideclaw review
  caught the first rewrite naming `merge`, which sets no `budget` key.
- The handover doc cited `STATE.md §56` three times after the split; repointed.
- The dispatch-path diagram labelled the door `warden dispatch`; the verb that
  opens an item is `warden run`. Relabelled and re-delivered.

### Decisions

- **The build log is append-only from here.** Each wave appends a § here and
  rewrites `STATE.md`; `CLAUDE.md` § Git says so.
- **sideclaw `fallow` is not a gate for this chain** — pre-existing debt in
  another repo's tooling; Wave 9 decides whether it becomes one.
- **`estate.html` ships as a `render`** with the crossing recorded, rather
  than splitting the diagram to satisfy a composition check.

### Next action

Wave 9 — the field review — is the owner's to start, by hand, after days
unattended, from `docs/handover-field-review.md`. Owner items unchanged:
argo PR #19, the PAT Issues permission, the 4.3 Slack acceptance.

## 58. First field look — automatic work off Max, cards that say what to do (2026-09-11, 10:00Z → 12:30Z)

Not Wave 9. The owner asked, after two days unattended, why nothing was
actionable and what the loop had cost. Three read-only forensics passes
(ledger + logs, sideclaw usage + routing, Slack + Argo surface) and two fixes.

### What the ledger showed (2026-09-09 19:43Z → 2026-09-11 10:00Z)

| | |
|-|-|
| Items | 57: quiet 33, needs_human 8, ignored 8, note 8, closed 4, merge_blocked 4, fixed 1, new 1 |
| Dispatches | 20 (ids 24–43), all `done` except one `failed` superseded by a retry |
| Real fixes | 1 `fixed` (the argo canary), 4 `closed` (3 resolved externally, 1 by a human) — zero infra recurrences resolved by the loop |
| Loop cadence | 57 ticks in 534 min after 9c19ead, no gap >20 min; one `database is locked` in `ingest()`, poll/sweep each crashed twice on schema-version mismatch during the night migrations and self-healed |
| needs_human | 6 distinct issues: hermes gateway wedged (uk:175/185, the slack-bolt reconnect signal with **362** occurrences), sideclaw crash (uk:204), weatherorb probe (uk:220), hermes patch corruption (uk:229), research-gateway OOM (uk:193) |
| merge_blocked | 3 argo canary items (self-tests, correctly refused) + dispatch-scratch#9 (no `autoMergePaths`) |

The loop is working as designed. The owner's long-standing issues are all
host-level actions (restart a gateway, bump `slack_sdk`, read `journalctl`)
that the design sends to `needs_human` on purpose. The gap is that a
`needs_human` card lands once and then nothing reminds until the 168h clock
dismisses it — `docs/api.md`'s "reminder at 1d" is still **not built**.

### Cost (sideclaw.jsonl, `session.end` shadow cost since 09-09)

All 17 real automatic dispatches ran on `claude-sonnet-5[1m]`/max (2 on
Opus, manual `--model`), because warden passed `model=None` and landed on
sideclaw's JUDGE route. Validation reviews: router on glm-5.3-flash/iu, then
angles + synthesis on Sonnet/max. Total sideclaw spend ≈ $95, of which ≈ $80
Sonnet/max. `usage-tracker` cannot attribute any of it to warden — sideclaw
sets no `USAGE_LANE`, so 98 % of the last three days' spend is untagged.

### Fixes (this §)

- `AUTO_DISPATCH_MODEL` (`scripts/triage.py`, env
  `TRIAGE_AUTO_DISPATCH_MODEL`, default `glm-5.3-flash`) replaces `model=None`
  at the two automatic call sites (auto-investigate, auto-implement).
  sideclaw's `withModel()` derives backend `iu` for a non-Claude id. Human
  paths (`warden run --model`, approval clicks) untouched. Step-7 validation
  has no per-call model knob; moving it is `SIDECLAW_MODEL_REVIEW` in
  sideclaw's `.env`, which is global and a separate decision.
- `needs_human` / `merge_blocked` cards render a `section` block: bold
  `Action required — …`, `Do this: <note>`, and `Auto-dismissed in Nd if
  untouched (<date>)` from `state_deadline` at day granularity so the
  `card_hash` short-circuit still holds. Replaces the italic `↳ _note_`
  footnote. `warden abort` refuses `merge_blocked`, so the default retry verb
  on that card is `warden merge`.
- `tests/test_triage.py` 213/213 (two new card tests; two existing dispatch
  tests now assert the model).

### Surface findings, not fixed here

- argo PR #19 is a **draft** — that is why it never merged; every tick still
  404s. Merging it gives the board, funnel and timelines; it cannot approve
  (`DESIGN.md`: Argo records intent, Slack signs).
- `#agents` has two voices: warden's cards and Hermes's project-narratives
  digest. Dispatch-approval buttons post to `#hermes`, not next to the card.
- The three `warden_canary` merge_blocked items sit among real ones on the
  board.

### Decisions

- Automatic episodes run on the cheap IU tier; the owner overrode
  `model-routing.md`'s "JUDGE = Sonnet over Max" for warden's unattended path.
  Review validation stays where sideclaw routes it until measured.

### Next action

Owner: mark argo PR #19 ready and merge it; act on or dismiss the 6
`needs_human` cards. Loop: build the 1-day `needs_human` reminder; tag
sideclaw sessions with `USAGE_LANE` so cost per item is a query, not a join
done by hand. Wave 9 remains the field review, after the reminder exists.

## 59. Autonomy — host verbs, the board goes live, judgment work off Max (2026-09-11, 11:00Z → 13:00Z)

The owner, after §58: "if warden is confident in a fix it must do it, even a
host-level action like restarting a process. `needs_human` for a restart is
friction. Explanations belong on Argo, not in Slack prose. GLM 5.3 flash,
not Sonnet over Max, usually always."

### The fifth closed allowlist

`HOST_VERB_ALLOWLIST` in `scripts/triage.py`: code owns the argv, the policy's
`hostVerbs` list may only name a key (an unknown key is dropped at load,
loudly). Seeded with one verb, `restart-hermes-gateway`
(`launchctl kickstart -k gui/$UID/ai.hermes.gateway`). `restart-research-gateway`
was not seeded: rollhook numbers the container
(`research-gateway-research-gateway-15`), so no static argv names it.

`maybe_auto_remediate()` runs between `run_verbs()` and
`maybe_auto_implement()`. Gates, in order: a folded investigate verdict exists;
no open `kind=host` operation; `hostVerbs` match; confidence at or above
`hostVerbMinConfidence` (default `medium` — a restart is idempotent,
liveness-verified and capped, so a wrong guess costs one restart and a card
with the receipt); per-verb cooldown (`hostVerbCooldownHours`, 6) and attempt
cap (`hostVerbMaxAttempts`, 2) keyed on `operations.note = "verb=<key>"`
across every item. Items sharing a verb are claimed together with the CAS
into `STATE_REMEDIATING` (1h crash backstop → `needs_human`), the verb runs
once, one `operations` row carries every discharged `event_id`, and all of
them move to `liveness_pending`, verified by the new positive probe
`kuma-push-fresh` (a Kuma push after the operation's `started_at`). Dry-run
prints `[dry-run] would run host verb …` and runs nothing.

### What happened live

The loop runs the working tree. At 11:36Z, with the medium floor in place and
the cooldown still per item, one pass kickstarted the gateway three times in
a row (items 2, 261, 815). The gateway came back with Slack connected at
11:37:33Z; all three items sit in `liveness_pending`. The per-verb grouping
landed in the same hour and is what §59 ships. That was warden's first
autonomous host fix, and the 362-occurrence reconnect signal is the item it
discharged.

### Surfaces

- argo PR #19 was a draft, so it never merged. Marked ready, rebase-merged
  as 62d9633, deployed 11:00Z; the first `argo push — ok (75182 bytes, 13
  items)` landed at 11:06Z. The board renders each item's note inline.
- Hermes's project-narratives cron (`9909f808fe17`) now delivers to
  `#hermes`; `#agents` carries only warden cards. Everything there still posts
  under the one "Hermes" bot identity, since warden uses Hermes's token.
- `warden-api` moved from 7734 to 7735: sy-serendipity's dev script runs
  `kill-port 7734`, and `localhost:7734` already answered that site over
  `[::1]`. 7735 is reserved by comment in dotfiles' Caddyfile; hermes-agent's
  skills, dotfiles' docs and the brain wiki follow.
- sideclaw: `SIDECLAW_MODEL_REVIEW` and `SIDECLAW_MODEL_DISPATCH` set to
  `glm-5.3-flash`, backend `iu` implied; no Max fallback remains on those
  two routes.

### Numbers

| | |
|-|-|
| `tests/test_triage.py` | 231/231 (§58: 213) |
| Host verbs in the allowlist | 1 |
| `hostVerbs` rules | 3 (uk:175, uk:185, the hermes_log reconnect signal) |
| Items discharged by the first run | 3 |
| Remaining `needs_human` | 4 (uk:204, uk:220, uk:229, uk:193) plus the two hermes_log rows at 260 with no verdict of their own |

### Decisions

- A closed-allowlist, idempotent, liveness-verified restart is not
  human-essential case 2. DESIGN.md carries the carve-out; FLOWS.md flow 5 is
  rewritten; "four closed allowlists" is five everywhere.
- The confidence floor for host verbs is a policy value, default `medium`.
- Judgment work (review, dispatch) runs on GLM over IU. Review quality is to
  be measured, not assumed.

### Next action

Watch items 2, 261 and 815 reach `fixed` on the next Kuma push. Then: a
sideclaw host verb for uk:204 guarded by no dispatch in flight; the 1-day
`needs_human` reminder; `USAGE_LANE` tagging in sideclaw. Wave 9 after that.

## 60. Closing the queue — reminders, a real heartbeat probe, and the Kuma sync that ignored `active` (2026-09-11, 13:00Z → 14:10Z)

The owner, after §59: "why do I have to approve things that are obviously
right? Check them, then do them." Everything §59 listed as "still needs you"
or "not built" was checked against live state and either done or dispatched.

### The nine open items, decided from evidence

| Item | Evidence | Outcome |
|-|-|-|
| uk:204 sideclaw crash | Kuma push recovered 10:16Z; sideclaw reloaded twice today, healthy | closed |
| uk:193 research-gateway OOM | monitor recovered 09:16Z; container at 111 MiB of 2 GiB; no OOM kill in the VPS kernel log for four days | closed |
| uk:229 "Hermes - HTTP" | endpoint answers 200 with the keyword; the Kuma monitor had been **paused** since its pre-deploy 404 days and every `make uk-sync` left it paused | fixed: homelab `sync.py` now converges pause state (`resume_monitor`/`pause_monitor` after `edit_monitor`, which ignores `active`); monitor UP; closed |
| uk:220 "WeatherOrb Watchdog - Push" | watchdog runs but skips its heartbeat because `obs_freshness:candhis` fails — five CANDHIS buoys silent 74–83 h, an upstream outage | `warden run weatherorb --tier implement` (item 996): degrade instead of blocking the heartbeat; investigate verdict came back high, auto-implement fired on GLM |
| hermes_log connector-is-closed (×2) | sibling of the discharged reconnect signal; gateway restarted 11:36Z | closed |
| three `warden_canary` items | §54's stop-condition exercise | closed |
| dispatch-scratch#9 | disposable fixture repo, no `autoMergePaths` by design | closed |

`needs_human` went from 8 to 1. Every close carries its reason in the
ledger (`triage.py --close <sig> --reason …`).

### The probe that could not confirm

The three hermes items sat in `liveness_pending` for two hours and then
reopened: `kuma-push-fresh` searched `#alerts` for a `[Hermes Agent - Push]
… Up` line, but a `kickstart` restart never takes the monitor DOWN, so no
recovery line ever exists. They resolved as `quiet` on silence, honestly not
`fixed`. `_gather_kuma_push_fresh()` now reads Kuma's own heartbeat table
through hermes-ops (`monitors --json` → id, `kuma-db heartbeats <id> --json`
→ rows), a positive probe: a `status=1` row after the operation's
`started_at`. The next host-verb run can reach `fixed`.

### The reminder, built

`remind_needs_human()` runs after `sweep_deadlines()`: one thread reply
under the card at `needsHumanReminderHours` (24) in `needs_human` or
`merge_blocked`, a second at 3×, never a third; canary and card-less items
skipped and counted on stderr. Schema 9 adds `reminder_count` and
`last_reminder_at` to `triage_items`. warden-api was kickstarted onto
schema 9 by hand; poll and sweep pick it up on their next run.

### Elsewhere

- sideclaw already tagged every worker session with `USAGE_LANE`
  (commit 49a065e); the §58 audit grepped the wrong directory. Review
  sub-steps now share `sideclaw:review` so one review is one line in
  usage-tracker. Not reloaded yet: a weatherorb episode was running.
- Hermes's narratives cron delivers to `#hermes`; `#agents` is warden-only.
- homelab: `docs(uptime-kuma)` comment and the `sync.py` pause fix, pushed
  and applied with `make uk-sync`.

### Numbers

| | |
|-|-|
| `tests/test_triage.py` | 242/242 (§59: 231); `test_ledger.py` 24/24 |
| Ledger schema | 9 |
| `needs_human` | 1 (uk:220, pending item 996) |
| Items closed with a reason today | 8 |

### Next action

Item 996 lands its PR; close uk:220. The first host-verb `fixed` is still
ahead. A sideclaw host verb for uk:204-shaped crashes, guarded by no
dispatch in flight. Wave 9 after a few days of this.

## 61. Warden's own Slack identity (2026-09-11, 14:20Z → 14:50Z)

The owner: the VPS, Argo and HomeLab identities were created as their own
Slack apps through `apps.manifest.create`; warden gets the same. `slack/
app-manifest.json` (scopes `chat:write`, `chat:write.public`), `slack/
README.md`, `make slack-app-create SLACK_CONFIG_TOKEN=xoxe-…` and
`slack-app-update APP_ID=…`, the token never stored.

Token resolution for posting: env `WARDEN_SLACK_BOT_TOKEN` → `op://common/
slack/WARDEN_BOT_TOKEN` → env `SLACK_BOT_TOKEN` → `op://hermes/slack/
bot-token`, one stderr line per process while it still posts as Hermes. The
read path (`watchdog-poll.py`, `#alerts` history) stays pinned to the Hermes
token by name: a `chat:write`-only app cannot read. `sync_card()` re-posts a
card when `chat.update` answers `cant_update_message`, which is what happens
to every Hermes-authored card the first tick after the switch; thread
replies under the old card are lost, the ledger stores the new `card_ts`.

Owner steps, in `slack/README.md`: mint a config token at api.slack.com/apps,
run `make slack-app-create`, install in the UI, store the bot token at
`op://common/slack/WARDEN_BOT_TOKEN`, add the ref to `dotfiles-private/
headless.refs`, `make secrets-seed` on the MacBook. `tests/test_triage.py`
244/244, `test_clients.py` 91/91.

## 62. The MacBook field report, folded in (2026-09-11, 15:00Z)

A MacBook-side review of §61 sent three findings; two changed `STATE.md`.

- **The PAT was never gated.** `op://mini/github/token` answers `GET
  /repos/jkrumm/rollhook/issues` with 200 and a `POST …/comments` with an
  empty body gets 422, not 403 (`x-accepted-github-permissions:
  issues=write`). The "grant Issues read/write" owner action, carried since
  §55, is dropped; the `github_issue` origin has been live all along.
- **The Warden Slack app stays owner-gated, with one trap.** No `xoxe`
  config token exists in 1Password and the MCP browser has no Slack session,
  so minting and installing are the owner's. Do **not** add
  `op://common/slack/WARDEN_BOT_TOKEN` to `headless.refs` before the value
  exists: `secrets-seed.sh` is `set -euo pipefail` and an unresolvable ref
  breaks the next reseal for every consumer. `slack/README.md` step 3 says so.
- **The repo was in no backup path.** `~/SourceRoot/warden` has no remote;
  `warden-backup.sh` shipped only `~/.warden`. It now writes
  `git bundle create --all` into the same snapshot directory, verifies it,
  gates the Kuma heartbeat on it, and rsyncs it with the ledger. Proven:
  `git clone` of the shipped bundle on homelab checks out 28ce459.

Also: item 996 landed while §61 was being written (weatherorb master 95d3e3b,
watchdog 105/105, Kuma 220 UP, `degraded: true`); `needs_human` is 0. The
handover doc's `sqlite3 "file:…?mode=ro"` snippets are replaced with a plain
path: this box's CLI cannot open a WAL ledger read-only while nothing else
holds it, and the form only ever worked while warden-api did.

## 63. The Warden Slack app is live (2026-09-11, 19:00Z)

Owner steps from §61 done on the MacBook: app `A0C13NMFLD9` from
`slack/app-manifest.json` (id recorded in `slack/README.md`, 40cb1c3),
installed, bot user `warden` (`U0C15C9QZFX`, bot `B0C24BVM6RW`);
`op://common/slack/WARDEN_BOT_TOKEN` set, the ref added to
`dotfiles-private/headless.refs` (1869b1d) and resealed to the mini.
Verified on the mini: `resolve_slack_token()` returns the Warden token with
no fallback warning, `auth.test` answers as `warden`, and a post reached
`#agents` without an invite. The read path stays on Hermes. `STATE.md`
drops the owner action; the only open risk the field review named, the
remote-less repo, is covered by §62's bundle.

Concurrent, not this §: a second session is plumbing a
`TRIAGE_VALIDATION_DISPATCH_MODEL` through `open_review()`/`submit_review()`
and sideclaw's review route so step-7 validation leaves Max too. Its files
are left uncommitted here on purpose.

## 64. A killed episode is not a verdict (2026-09-12, 09:00Z → 11:30Z)

Item 253, `docker_homelab:unhealthy:garmin-collector`, sat in `verdict` for five
hours carrying an empty note while the container recovered on its own. Two
independent defects stacked, one on each side of the sideclaw boundary.

**sideclaw was killing healthy workers.** `runSessionAttempt` had a single
`setTimeout(timeoutMs)`, and `TIERS.investigate.timeoutMs` is 8 min. Both
automatic dispatches since dispatch moved to `glm-5.3-flash` (§59) died at
exactly 480000 ms: job `c7d73a1c` (this item) at turn 28 with **1504 ms** of
idle, mid-`Bash: sed -n 1,90p scripts/garmin-auto-relogin.sh`; job `4b3f1e01`
(brain) at turn 22 with **2000 ms**, mid-`responding`. Neither was stuck; both
were working when SIGTERM landed. A single wall-clock timer cannot tell "slow"
from "wedged", and glm-5.3-flash defaults to max reasoning effort — minutes per
turn is its normal shape on hard work, not a symptom.

Fixed in sideclaw `3c44689`: an idle watchdog that kills only after 5 min with
no stdout chunk (stderr never resets it) plus an absolute ceiling at
`max(timeoutMs, 60 min)`, with `timeout_idle`/`timeout_ceiling` and
`idleMsAtKill` on the attribution record so a wedge and a long episode stop
reading as the same event. The same commit takes `retryAfterOutput` off
check/overview/review-router — re-laning onto Haiku the moment a slow worker
missed its timeout was compensating for the timer that had just been fixed —
and moves the tier decision out of sideclaw's `.env` into `routing.ts` (`AGENT`
for dispatch; review and otel stay on `JUDGE`, for the reasons dated
2026-09-11). That last part is load-bearing: reloading sideclaw with only the
watchdog would have silently thrown dispatch back onto Sonnet/Max, because the
`SIDECLAW_MODEL_DISPATCH` override lived in a `.env` the running process had
read at boot and nobody had reloaded since.

**warden folded the failure as if it were an answer.** `fold_dispatch_verdict()`
never read `dispatches.status`. `failed` + `verdict_json` NULL gave
`result = {}` → `next_action = ""` → `STATE_VERDICT` with `note = NULL`.
`maybe_auto_implement()` correctly declined it (no `nextAction=implement` at
`confidence=high`), and nothing else was scheduled to touch the row until its
24 h deadline. sideclaw's failure text reached Slack through `format_message()`
and was persisted nowhere at all. That is DESIGN.md's "deferral must be visible"
inverted: a failed episode was indistinguishable from a broken loop.

Migration 10 adds `dispatches.error` — sideclaw's terminal failure text
verbatim, or warden's own reason for a row `_mark_pruned()` closes without ever
polling a real answer. No backfill: rows that failed before this genuinely have
no recorded reason, and inventing one is worse than admitting the gap. A COLUMN,
not a field inside `verdict_json`, because that blob is sideclaw's *published*
schema and a failed dispatch has no verdict to hang a field on.

`fold_dispatch_verdict()` gains exactly one branch, ordered between the
`artifact_url` and `nextAction == "human"` checks:
`status != "done" and not result` → `STATE_NEEDS_HUMAN`, note =
`"<tier> episode <status> with no verdict: <reason>"`, capped by `_cap_brief()`.
Both halves of that condition are load-bearing and commented as such —
`artifact_url` must keep winning first because an `implement` episode can fail
*after* opening its draft PR, and `not result` is required because an
`interrupted` episode that nonetheless returned a schema-valid verdict has
answered the question. No automatic retry: a killed episode routes to a human,
and retrying one is a separate decision this § does not make.

**And the knob that would have done nothing.** §63's concurrent session left
`TRIAGE_VALIDATION_DISPATCH_MODEL` uncommitted, plumbing a `model` through
`open_review()`/`submit_review()` so step-7 validation could leave Max.
sideclaw's `REVIEW_INPUT` is a plain `z.object`, not `z.strictObject` — Zod
would have silently stripped that param and the review would have run on its
default route looking configured. sideclaw `4e16aa5` adds the field (reaching
the angle and synthesis sessions only; the router's cheap CLASSIFY route and the
adversary critic are excluded at the call site, commented so neither gets
"fixed" later) and omits it from the MCP-facing schema, since an interactive
`/review` caller has no reason to leave the measured-good route. The knob still
defaults to `None`: review is the one tool where the cheap tier has actually
been measured failing.

Ops: all five agents `make unload`ed for the duration of the edit.
`warden-sweep` had already crashed once on its 300 s tick, refusing a
`schema_version=9` ledger against a working tree already bumped to 10 — the one
migrator rule working exactly as written, and the reason the agents came down.
Tests: `test_triage.py` 244 → 248, `test_ledger.py` 26, sideclaw 651 → 657.

**Proven in production, not only in tests.** Item 1006 re-ran the exact brief
that died at 480000 ms (`warden run homelab --tier investigate`, job
`42b07613`). It ran **519487 ms — 8 m 39 s** — crossing the old 8-minute
ceiling at 09:07:24Z still `running`, and returned `done` at 19 turns with a
`confidence: high` verdict. Under the timer this § replaces it would have been
SIGTERMed 39 seconds short of its answer, for the third time. The concurrent
`hermes-agent` episode the loop escalated on its own at reload (job `35c9dd64`)
finished clean at 394741 ms. Item 1006 folded to `closed` carrying the summary,
the human-origin/`max_tier=investigate` shortcut behaving exactly as written.

The verdict itself: garmin-collector's `/health` returns 503 whenever its
background authenticated Garmin probe (every 15 min) fails, so a dead OAuth
refresh token flips the container unhealthy without the process being down.
Garmin invalidated the token ~3 d after the last relogin, inside
`garmin-auto-relogin.sh`'s 4-day proactive window; the 2-hourly cron caught it
at 06:00Z, reauthed through the MFA-email fallback after two 429s, and
force-recreated the container at 06:00:23Z. `RestartCount=0` — nothing restarted
it at 08:26Z, and no human was involved. It self-healed correctly and will
recur by design. `nextAction: none`.

Item 253, the original, is deliberately left to expire on its own 24 h clock:
the new branch only fires when a dispatch reaches terminal, and 253's already
reported, so re-folding it would mean hand-editing the ledger to prove a point
item 1006 proves honestly.

**And a bug found only by checking.** Shipping §64 meant running
`scripts/warden-backup.sh` by hand (warden has no remote — §62's bundle is the
off-box copy). It reported `snapshot warden-20260912T091719Z.db (1.2M)` and the
file was not there afterwards, on either side of the rsync.

The rotation glob was `(NOm[$((KEEP+1)),-1])`. zsh's time-based sort qualifiers
read backwards from the name-based ones: `om` is newest-first, `Om` is its
reverse, oldest-first. So the slice selected everything past the seven OLDEST
snapshots — the script deleted the snapshot it had just taken, on every run,
from the moment the directory first reached `KEEP=7`. The freshest surviving
snapshot on homelab was two days old, and the only CURRENT copy there was the
rsynced live `warden.db` + `warden.db-wal` pair, captured at two different
instants: precisely the artifact CLAUDE.md § The ledger forbids relying on, and
precisely why `VACUUM INTO` exists in that script at all. Nothing was lost —
restic holds the long tail — but the daily backup had quietly stopped producing
a restorable same-day artifact, and `make status` cannot see it, because the
script exits 0 and prints a filename either way.

One character, `Om` -> `om`, with the reasoning written at the call site so it
does not get "simplified" back. Verified by re-running: the new snapshot
survived, `warden-20260909T121018Z.db` (the genuine oldest) was pruned instead,
and seven are retained. This is also the answer to a question nobody had asked
yet — there is still no restore path, and now there is at least something
current to restore FROM.

## 65. The consolidation look: the failed brief, the deploy window, and what the rest is made of (2026-09-12, 09:13Z → 12:30Z)

Item 1007 was the owner's own voice-transcribed brief — consolidate warden,
compare it with the other agent control planes, clean up, sharpen the
lifecycle — sent through `warden run warden` at 09:13Z. sideclaw's investigate
episode ran eleven minutes on `glm-5.3-flash`, did real work (29 Bash calls
against the ledger, two fetches, a web search), and failed with

```
Session exited with code 1. stderr: [claude-code:unrecognized_model]
{"model":"glm-5.3-flash","query_source":"generate_session_title"}
```

§64's fold worked exactly as built: `needs_human`, `dispatches.error` carrying
that text. The text was wrong, and that is this section's first finding.

**The stderr line is noise; the cause was `--max-turns 25`.** The CLI's own
transcript ends on `{"attachment":{"type":"max_turns_reached","maxTurns":25,
"turnCount":26}}` 184 ms before exit. The `unrecognized_model` line is the
CLI's session-title helper complaining about a non-Claude model name — 88
occurrences in `sideclaw.jsonl`, including the two dispatches that succeeded
right before (jobs `35c9dd64`, `42b07613`, exit 0, full cost records). The
runner's `exitCode !== 0` branch returned before ever reading the result
envelope it had already parsed, built `error` from whatever stderr was
buffered, and so `isSalvageable()`'s `max_turns` regex — written for exactly
this case — never matched. Eleven minutes discarded, no 12-turn salvage retry.
Fixed in sideclaw `6a9325c`: the envelope's `subtype` wins (`error_max_turns`),
`noOutput` is set so the salvage path fires, known-benign stderr lines are
stripped from constructed errors and kept in the raw debug log, and the
model's own `result` text is deliberately *not* folded into `error` — that
string feeds the reactive fallback classifier and a `needs_human` card, and a
brief is attacker-influenceable.

The turn budget itself is not the defect. Alert-shaped investigations took 13
and 17 turns. A brief of item 1007's shape — read the whole repo, research
four external products, produce a plan — is not an investigate episode and
should not be sent as one; it is a session, which is what answered it.

**Every schema bump pages Kuma.** Item 815's verdict (the one real product of
the morning, glm, `confidence: high`) diagnosed it precisely: `watchdog-poll.py`
and `dispatch-sweep.py` call `assert_schema_version()` at `db_connect()` and
crash with a traceback whenever `LEDGER_SCHEMA_VERSION` runs ahead of the file —
the window between a commit landing and the loop's next 600 s tick. The poll
never reaches `_push_uptime_heartbeat()`, so "Hermes Watchdog - Push" fires,
becomes an item, and gets dispatched. Schema 7→8 on 09-11 and 9→10 on 09-12
both did this; the second produced item 815's own re-investigation. A deploy
that alarms on itself and then spends an episode explaining why.

Fixed here: `ledger.LedgerBehind(RuntimeError)`, raised only when the file is
*older* than the process (the plain `RuntimeError` for a stale process stays
loud). Poll and sweep catch it, print one stderr line, exit 0; the poll still
pushes its Kuma heartbeat under `--post` because that monitor measures "is the
poller alive", not "did the ledger open". `warden-api` still restarts into the
window under `KeepAlive` — `ThrottleInterval` bounds it, `/health` reads
unreachable for at most one loop interval, accepted.

**Pre-§64 leftover.** Item 253 (`homelab`, dispatch `c7d73a1c` failed under the
old wall-clock timer) still sits in `verdict` with no verdict — it was folded
before §64 landed and nothing re-folds a settled row. Its 24 h deadline expires
it to `needs_human` at 2026-09-13 05:00Z. Left alone: hand-editing the ledger
for one row is worse than the row.

**Unknown localhost client.** `warden-api.err` holds 19 739 `GET / → 404` from
`127.0.0.1`, one every ~7 s from 2026-09-10 22:06 local to 2026-09-12 11:18
local, then nothing. Nothing in dotfiles, sideclaw, hermes-agent or argo names
port 7735 except this repo; a 12 s `lsof` catch found no client. Not
harmful; noted so a return is recognised.

**What the code is made of** (AST audit over 344 module-level defs, all of
`scripts/` and `tests/`):

- Dead code: none. Zero unreferenced defs, zero test-only survivors, no
  `_v2`/`_legacy` variants, no always-on flag. A grep-based pass reports ~60
  false positives because docstrings here name sibling functions in prose;
  audit this repo with AST or not at all.
- Duplication inside warden, all small, all real: the Slack POST transport
  three times (`triage._slack_call`, `watchdog-poll._slack_call`,
  `clients/slack.py`); `_now_iso` five times under three semantics
  (`intents`, `ledger`, `lifecycle/operations` identical; `lifecycle/items`
  and `triage` take an argument and differ); secrets resolution five times
  (`clients/secrets.py` is the shared one; `watchdog-poll`, `dispatch-sweep`,
  `clients/github`, `warden.py` each carry their own); `_parse_pr_url` twice;
  `_apply_db_override` twice; the dispatch-verdict rendering ladder twice by
  its own admission (`watchdog-summary.py:47`). `watchdog-summary.py:118` opens
  the ledger with a raw `sqlite3.connect(mode=ro)` past `ledger.connect()`.
- `triage.py` is 6658 lines, 84 defs, and 961 of its first lines carry three
  of them. Two regions are 35 % of the file: evidence gathering + escalation
  (2479–3449) and operations/crash recovery (4095–5448).
- warden ↔ hermes-agent: the extraction is clean — no `.py` twin remains, only
  stale `__pycache__`. `hermes-agent/scripts/agents-overview.py` (763 lines) is
  the retired `#agents` digest renderer, still alive, still referenced from
  the morning-briefing prompt, and it carries copies of `_resolve_ref`,
  `resolve_slack_token`, `post_blocks`, `_escape`. `warden.py` is a structural
  Python port of `hermes-ops.sh`'s front matter (`redact`, `audit`,
  `require_backend`, `run_plan`, `cmd_status`), same names in two languages.
- warden ↔ sideclaw: verdict schema pinned and drift-checked (`make
  check-schemas`); repo allowlist deliberately two-sided and drift-checked
  (`check-dispatch-policy.py`); idle/ceiling timers live only in sideclaw.
  **The one unchecked copy is `AUTO_DISPATCH_MODEL = "glm-5.3-flash"`** against
  `routing.ts`'s `GLM_FLASH` — a value with no `make check-*` behind it.

**Redeploy survival**, the owner's other worry: already mostly true. sideclaw's
`make reload` polls `/api/jobs/health` and refuses while a job runs unless
`FORCE=1`; the normal path is `POST /api/shutdown` with a ~50 min drain. All 13
reloads in the retained log carried `killedWorkers:0`; zero `interrupted` rows
exist. When a worker *is* killed, `check`/`review` get one re-run from scratch,
`dispatch` lands `interrupted` with its worktree bundled to
`~/.local/state/sideclaw/salvage/`, and warden folds it to `needs_human` — no
retry anywhere, by §64's decision. The gap is that the worker's Claude
`sessionId` reaches only the usage-tracker log, never `jobs.db`; `--resume` is
used nowhere. Durable resumption across a forced restart is buildable — persist
the id, keep the worktree, resume on boot — and is not yet needed by any
measured event.

**The comparison** (research-gateway, 171 pages, `status: ok`). Mastra Factory
is beta, `@mastra/factory` 0.14.0, board-owned gates through `defineBoard()`
(triage → plan → build → review → completion), LibSQL storage adapter — the
same lifecycle shape as ours with a UI and a supervisor agent on top. Factory
Droid's `droid exec` has session continuation and JSON-RPC progress but no
managed auto-merge. Jules exposes `AWAITING_PLAN_APPROVAL` as a real API state.
Copilot's cloud agent hard-caps a session at 59 min and cannot merge its own
PR. Every one of them keeps merge as a separate, explicit gate under branch
protection; none exposes a stuck-vs-slow classifier — that is always the
caller's ledger, from heartbeats and deadlines. The vendor-neutral consensus is
what DESIGN.md already says: SQLite owns truth and policy, runners own
execution, GitHub owns the last gate. Nothing there to adopt as a dependency;
two things worth stealing — an explicit *plan-approval* state for
`implement`-tier human briefs (Jules), and the per-episode record of runner
version + base SHA + session id (everyone) which we have only in
usage-tracker.

**Decisions this § does not make**, left for Wave 9 as designed: whether to
lift duplicated helpers into `clients/` (mechanical, ~200 lines, zero
behaviour), whether to split `triage.py` along its two big regions, whether to
retire `agents-overview.py` in hermes-agent, whether to add
`make check-routing` for the model pin, and whether to persist the session id
in sideclaw. None is blocked on evidence except the last, which is blocked on
an interruption ever happening.

## 66. No limits, and the owner's own verdicts (2026-09-12, 12:30Z → 14:30Z)

The owner's answer to §65 was not a question. Three things, said plainly.

**No turn limit, no wall-clock ceiling, anywhere.** "The workers are agents
doing big things — why would we cap their turns? I don't need a turn limit.
The agent runs as long as it needs." The only liveness rule left is
sideclaw's idle watchdog: no stdout for 5 min means wedged. sideclaw
`8459357` removes `--max-turns` from `buildSessionArgs`, deletes every
`maxTurns`/`retryTurns`/`timeoutMs` from the dispatch tiers and from
check/review/overview/narrative/excalidraw/otel, and deletes the 60-min
ceiling (`timeout_ceiling` no longer exists as an outcome). The salvage retry
stays for an episode that ran but produced no schema-valid output; it runs
unbounded like everything else. Warden itself never had a turn limit — its
only deadlines are lifecycle ones on ledger items (verdict 24 h, needs_human
7 d), which are a different fact and stay. dotfiles carries none either; the
`max-turns` hits in modelpick are its benchmark harness, not a worker path.
Recorded as standing feedback in this session's memory so it is never
re-introduced "for safety".

**Do the Wave-9 list now, fanned out.** Five implementers, one commit each:

- `warden close <event-id> --why` — a `needs_human`/`verdict`/`new`/
  `merge_blocked`/`quiet`/`note` item can be resolved from the terminal;
  in-flight states refuse (exit 2, `abort` is the verb), terminal states
  no-op. `sync_card()`'s hash covers state+note, so the card re-renders on
  the next tick unaided. Items 1007 and 253 are closed with it, each with
  the reason in its note. Until now the only way to answer "I dealt with it"
  was a Slack button or a 7-day expiry — the owner asked what on earth he was
  supposed to do with 1007, and the honest answer was "nothing, and it will
  nag you until Friday". That was the flaw.
- `make check-routing` — the third drift check, in the mould of
  `check-schemas` and `check-policy`: `triage.AUTO_DISPATCH_MODEL` (and
  `TRIAGE_VALIDATION_DISPATCH_MODEL` when set) against sideclaw's live
  `GET /api/routing`; `make status` carries the line.
- Helper consolidation, zero behaviour change, 248/248 untouched: one Slack
  POST primitive (`clients/slack.py:slack_raw_post`) under both `_slack_call`s;
  `ledger.now_iso` under `intents`/`operations`; `clients/secrets.resolve_secret`
  under the poll and the sweep; `clients/github.parse_pr_url` under triage;
  `ledger.apply_db_override(argv, setter)` under triage and the sweep;
  `clients/sideclaw.classify_dispatch_outcome` under both verdict renderers;
  `watchdog-summary.py` opens the ledger through `ledger.connect(readonly=True)`.
  One correction to §65's map: `connect(readonly=True)` does not assert the
  schema version — the read-only branch returns before the check.
- hermes-agent `0bad94d`: the `--slack-body` digest path in
  `agents-overview.py` is deleted (838 → 538 lines); `--briefing` and
  `--post-full` are the live paths and keep their helpers, so the cross-repo
  copies of `_resolve_ref`/`post_blocks` stay — they are load-bearing there.
  `docs/dispatch-bridge.md` is Hermes-side only now (628 → 75). §65's audit
  also misread `make agent-overview` as hermes-agent's; it is a dotfiles herdr
  pane over sideclaw's `GET /api/overview.txt`, and the handover doc says so.
- sideclaw, in flight as this § is written: `jobs.session_id`, `--resume` on
  boot for a dispatch killed mid-episode with its worktree kept, and a
  self-drain with no wall-clock cap.

**Not done, on purpose:** splitting `triage.py`. `_triage_env()` and 248
tests monkeypatch that module's globals by name; a split is a day of moving
names and patch targets for no behaviour. It stays one file until a real
reason appears.

**Item 1007's answer is §65**, closed with that note. The next brief of that
shape goes to a session, not to `warden run` — recorded in memory.

## 67. Three recurring alerts, traced to the line (2026-09-14, 18:30Z → 19:10Z)

The owner asked why `Warden Backup - Push`, `MacMini Dev Host - Push` and
`VPN Watchdog - Push` keep coming back. Three different answers.

**Warden Backup (uk:234, item 1010) was warden's own bug.** The heartbeat is
gated on `BUNDLE_RC`, and the §62 bundle step failed every 03:10 run since it
landed: `git bundle verify` needs a repository to check prerequisites against,
and launchd runs the script from `/` — `error: need a repository to verify a
bundle`. `bundle create` has `-C "$REPO"`, `verify` did not. Both stderr
streams went to `/dev/null`, so three nights of `bundle FAILED` never said
why. Fix: `git -C "$REPO" bundle verify`, stderr kept. Kickstarted at 18:48Z:
bundle 1.2M at `15e96a4`, exit 0, heartbeat sent. (The 2026-09-12 11:18
bundle on disk was a load-time catch-up run from a repo cwd, which is why the
path looked proven.)

**MacMini Dev Host (uk:204, item 543) was a real failure plus a loop bug.**
The real part: `devhost-health-check.sh` pushed `down` for ~45 h on modelpick
`node` processes spinning at ~100% CPU — orphaned by `secrets-run`'s missing
signal relay, which item 543's own investigate verdict (dispatch 51, high
confidence, `nextAction: implement`) had already named. The bug: `dotfiles` is
capped at `investigate` in `dispatch-repos.json`, `maybe_auto_implement` never
checked the ceiling, so every tick it claimed the item (`verdict →
implementing`), sideclaw refused at its boundary (HTTP 400 `tier 'implement'
exceeds the ceiling 'investigate'`), and the rollback put it back with no
note. Two silent transitions every 600 s, the verdict deadline reset each
time so it could never time out, and the Argo snapshot grew ~780 B per tick
from the transition rows. Fix: `_policy.resolve_tier("implement", target)`
before the claim; a cap folds the item to `needs_human` carrying the refusal
("apply the fix by hand"), and the remote-refusal rollback now carries
`deferred: <error>` like its siblings. Test
`test_auto_implement_on_a_tier_capped_repo_folds_to_needs_human_without_dispatching`;
`test_triage.py` 252/252.

**VPN Watchdog (uk:150) is interval noise on the homelab side**: a 5-min cron
pusher against a 300 s Kuma interval with `maxretries: 0`, so one slow tick
pages. The fix belongs to the homelab repo's own monitor config (interval
≥ 2× cadence, the MacMini monitor's pattern); not touched from here.

Also committed: the 2026-09-13 DeepSeek rollout diff for `propose_mappings`
(`a4d7c9c`), left uncommitted by the session that wrote it; that is where
the 248 → 251 came from.

**Found, not fixed:**
- Argo's item modal (`argo/.../features/warden/item-timeline.tsx:212`) reads
  only `item.brief`; an alert-origin item shows "No brief recorded" and four
  empty lists although `GET /items/<id>` already carries the event title,
  occurrences, first/last seen, signature, deadline and note.
- Item creation (`INSERT` at the two `triage_items` creation sites) writes no
  initial `item_transitions` row, so a `new` item reads "Transitions (0)".
- Two reminder counters: Slack reminders count on `events.reminder_count`
  (watchdog-poll, 6 h for `uk`), `triage_items.reminder_count` only for
  `needs_human`/`merge_blocked` — the API's item shows 0 while Slack says #4.
- `item_payload()` embeds every transition and operation row unbounded in
  each board snapshot.
- Hermes: a manual stop racing launchd's SIGTERM left pid 61585 half-dead and
  five refused starts (item 1017, self-healed); nothing reaps a stale
  instance on start.

## 68. The open list from §67, done across four repos (2026-09-14, 19:10Z → 20:30Z)

Owner: "do all of them." Routed by where the dispatch policy lets work land:
warden and dotfiles are `investigate`-capped and homelab-private is denied, so
those ran as in-session implementers; argo went to a sideclaw `implement`
dispatch on glm-5.3-flash, and the Hermes gateway friction to an `investigate`
dispatch on the same model.

**warden (this commit).**
- Item creation writes a `created` transition (`from_state` NULL, `at` =
  `created_at`) at both `INSERT INTO triage_items` sites — `ingest()` and
  `open_origin_item()`, the latter also `warden run`'s path — via
  `_record_created_transition()`. `_set_state()` stays the writer for every
  later move. `item_payload()` prepends a synthetic `created` entry
  (`synthetic: true`, `id` null) for legacy items, only when the history is not
  truncated.
- `item_payload()`'s event carries `reminder_count`/`last_reminder_at` — the
  Slack alert reminders watchdog-poll sends, distinct from
  `triage_items.reminder_count` (needs_human/merge_blocked). `docs/api.md` names
  both.
- `item_payload(history_limit=…)` keeps the newest N transitions and
  operations, oldest-first, with `transitions_total`/`operations_total` always
  present. `build_argo_snapshot()` passes `ARGO_SNAPSHOT_HISTORY_LIMIT = 50`;
  `GET /items/<id>` stays unbounded.
- Two existing tests shift by the new `created` row (counts +1, same
  assertions on the real transitions); none weakened. `test_triage.py`
  255/255, `test_api.py` 41/41.

**homelab-private `a71ded1`.** VPN Watchdog - Push interval 300 → 900 (3× its
5-min cron), applied through its own `sync.py`; the live monitor reads 900.

**dotfiles `a9410e7`.** `secrets-run`'s no-redaction fast path ran the child
in the foreground with no trap — the sibling of `d8ac7ed`, which fixed only
the redacting path. Both now background the child in its own process group
(`setsid`) and relay INT/TERM/HUP to the group. The worker's first version
called `setsid` unconditionally, which cost an interactive child its
controlling terminal (measured under a pty: `/dev/tty` fails); it is now
skipped when stdin is a terminal. 115/115 in `secrets-run.test.sh`.

**argo `4213502`**, deployed 20:13Z. The glm dispatch built the modal
(event title, source:external_id, state/origin badges, signature, seen and
occurrences, deadline, both reminder counters, note, brief-or-payload, an
"Unmapped" line, "(showing last X of Y)", synthetic `created`) and pushed its
branch, but opened no PR: fallow's audit failed on `ItemSummary` (cyclomatic
60, CRAP 3660). An implementer moved the derivations into `model.ts` as pure,
tested helpers and split the JSX into flat pieces; `fallow audit --base
62d9633` clean, the 25 dead-code findings identical on master, dashboard
tests 250/250. Fast-forwarded to master, the check and deploy runs green.
Lesson: a dispatch's own gate is the repo's full gate, fallow included, so a
UI brief should ask for small components up front.

**hermes-agent, applied, not committed.** The investigate dispatch (glm,
high confidence) traced item 1017: the gateway's non-`--replace` start path
refuses immediately while a predecessor is still dying, so launchd's KeepAlive
respawned into five refusals; the `--replace` path already had the wait. Three
patches under `patches/`, applied to `~/.hermes/hermes-agent` by `git apply`
(`make patch-check` 14/14), live at the next gateway restart:
`gateway-start-predecessor-grace` (20 s poll, never signals), `skill-manager-
colon-hint` (fail-closed YAML error names the unquoted `: `), `shutdown-
forensics-darwin` (BSD `ps`, `sysctl vm.loadavg`, `sample`, so a stall leaves
evidence on macOS). The "closed shared OpenAI client" warning is by-design
self-healing. The implementer's grace-path test unlinked the live gateway's
pidfile once; it restored it from the process's own cmdline and start time,
verified. The repo's rows in `CLAUDE.md`/`docs/patches.md` share hunks with the
uncommitted DeepSeek patches from 2026-09-13, so one commit there waits on that
work.

**Closing out §68 (20:20Z → 20:35Z), owner: "do all the remaining things."**
- Item 543 closed with `warden close` (root cause: dotfiles `a9410e7`). The
  verb's text renderer read snake_case keys off its camelCase result and
  crashed after the write; fixed with a text-path test (`f2176f7`,
  `test_warden_cli.py` 67/67).
- hermes-agent: nine commits pushed, including the 09-13 DeepSeek work
  (`dc22d9d` patches, `ec1f688` config) and ten Hermes-authored skills
  (`c486d77`). The repo is public, so an exposure audit ran first; it cleared
  everything but `skills/work/iu-epos-ops`, which carries employer-internal API
  hosts and debug deletion endpoints. `skills/work/iu-*/` is gitignored and
  Hermes loads it from disk. The gateway restarted at 22:24 local onto the three
  new patches, clean (both platforms connected).
- argo `6765121`: the 09-13 AI-gateway move to deepseek-v4.1-flash with a
  top-level `reasoning_effort`, stranded uncommitted on the deleted
  `warden-board` branch. Moved onto master and finished (one formatter fix;
  api suite 1024/1024). Prod pinned `DEEPSEEK_MODEL=DeepSeek-V4-Flash` via
  the vps compose fallback, so vps `83c4bc6` removed the pin and ran
  `make argo-up` first, then the api deployed. `argo-api` healthy on
  `6765121`.

## 69. The GitHub poll that resolved every open issue (2026-09-15)

Asked why open GitHub issues never reached warden or Argo. Two findings.

**By design:** only `warden:go`-labelled issues open an item
(`ingest_github_go()`). The staleness poller's `github_issue` events are not
in `INGEST_SOURCES`, so they are digest lines and nothing else — no dispatch,
no comment, nothing on the board Argo is pushed. None of the 11 open issues
across `jkrumm/*` carries the label; the only issue item ever opened is
`dispatch-scratch#9`. STATE.md said the staleness poller opened items too; it
does not, corrected.

**A bug:** `com.jkrumm.warden-poll` runs with launchd's `PATH=/usr/bin:/bin`,
where `gh` does not exist. `poll_github()` caught the `FileNotFoundError` and
returned `[]` per kind, which `reconcile()` reads as "every open issue
disappeared" — so the first LaunchAgent run (2026-09-09 12:05Z) resolved all
six open `github_issue` events (`basalt-ui#51`, `#52`, `rollhook#21`,
`dispatch-scratch#2`, `ntfy-mac#12`, `research-gateway#1`) and every run since
was blind while exiting 0. Reproduced under an `env -i` launchd-shaped env.

Fix: `GH_BIN` (env-first, `/opt/homebrew/bin/gh` default — the shape
`triage.py` already had for the same reason), a non-zero exit or exception now
logs a line to stderr and maps that kind to `None`, and `_run_poll` skips
reconciling a `None` kind, same as an unreachable `op_refs` host. New
`tests/test_watchdog_github_blindness.py`. `make test` green, `test_triage.py`
255/255.

Open: whether owner-authored issues should open items without the label, and
how a third-party issue's assessment gets approved — a design question, not
part of this fix.

## 70. GitHub issues in warden, Wave 1: no-label intake (2026-09-15)

Answers §69's open question. `docs/waves/PLAN.md` Wave 1, owner decisions
2026-09-15 (Argo is the owner over the tailnet — no passkey, no Slack-only
signing, no opt-in label for his own actions — overriding REVIEW.md C1's
corollary and DESIGN.md § the decision primitive; the override record itself
is Wave 2's job, not this one's).

`_github.search_issues()` drops `label` for `skip_label`: every open issue
under `owner`, minus `-label:warden:skip`. `triage.py`'s `ingest_github_go()`
renamed `ingest_github_issues()`; `GITHUB_GO_LABEL` → `GITHUB_SKIP_LABEL`. Event
source stays `github_go` — the ledger has live rows under it, renaming a
source is a migration, not a rename. Trust and `max_tier` derivation
(owner → `implement`, everyone else → `investigate`, fail-closed on a
missing/unparseable author) are unchanged; only the intake gate moved from
label-required to skip-label-optional.

Third-party verdict routing changed: `fold_dispatch_verdict()`'s
`_member_state_and_note()` used to close ANY `investigate`-ceiling origin
item as `answered` the moment a plain verdict landed. Split by who asked —
`human` (a question, answered in the same breath) still closes; `github_issue`
(nobody was in the loop when a stranger opened it) now lands in
`needs_human` carrying the verdict summary, so the owner sees the assessment
on the card and in Argo before anything closes. No public comment ever
reaches a third-party issue (unchanged). Owner-issue routing untouched — it
opens at `max_tier='implement'` and never reaches this branch.

`config/dispatch-repos.json`: `sideclaw`/`warden` joined `tiers.investigate`
(CLAUDE.md: warden may never hold tier ≥ 1 on its own executor or on itself).
`watchdog-poll.py`'s `poll_github()` stopped polling issues — `github_pr`
stays; the digest's `github_issue` events were a one-time resolve
(`resolve_stale_github_issue_events()`), since issue items now carry a real
verdict and supersede the age-gated "still open" line entirely.

`/review` (sideclaw multi-angle, `needs-human`) caught a real bug beyond the
brief: `search_issues()` didn't check GitHub's `incomplete_results` flag — a
search-index timeout can return HTTP 200 with a page that isn't authoritative,
which would have silently resolved a genuinely-still-open issue's event.
Fixed in the same commit, with a regression test. The review's other
blocking finding — `make check-policy` disagreement, sideclaw's own boundary
still allowing `implement` on `sideclaw`/`warden` — was already anticipated
and scoped out of this wave by the plan itself (sideclaw is a different repo);
left for whoever picks up that side. `test_triage.py` 256/256, `test_clients.py`
93/93, `make test` green, `make check-routing` green.

## 71. Owner actions pulled from Argo; every daily budget removed (2026-09-15)

`docs/waves/PLAN.md` Wave 2. `scripts/clients/argo.py` grew `fetch_actions()`/
`ack_action()` — `GET /warden/actions?machine=&status=pending` /
`POST /warden/actions/:id/ack`, same never-raises, folded-status-string
contract as `push_snapshot()`. `triage.py`'s `apply_argo_actions()` runs as
step 9.5 of every tick, right before `push_argo_snapshot()`: pulls pending
actions, dispatches each through the closed `ARGO_ACTION_VERBS` allowlist
(`implement`/`merge`/`dismiss`/`reinvestigate`/`note`), acks every outcome —
unknown verb, wrong state, or a real failure all ack `rejected`/`failed`,
never a silent drop. `authorized_by="owner:argo"` satisfies the exact same
plain truthy-string gate a signed Slack approval's `authorized_by=f"signed:
{who}"` does (there was never a prefix allowlist, only a non-empty-string
check in `dispatch.open_episode()`) — recorded as the owner's 2026-09-15
override in DESIGN.md (new § *2026-09-15 override*), REVIEW.md (C1
disposition update, history kept, not deleted) and this repo's own CLAUDE.md
load-bearing list: Argo is reachable only over his own tailnet, so an action
clicked there already carries what C1 required a signature for elsewhere.
`scripts/api.py`'s `_board_item()` grew `availableActions` (computed from
`state` alone, mirroring `apply_argo_actions()`'s per-verb allowed-state
sets) and, for `github_issue`-origin rows, an `issue` sub-object sourced from
the parent event's `payload_json`/`url`.

Every daily count ceiling is gone (owner: "absurd friction") —
`DAILY_INVESTIGATE_BUDGET`, `WARDEN_DAILY_BUDGET`, `WARDEN_IMPLEMENT_BUDGET`,
`WARDEN_MERGE_BUDGET`: the constants, the checks, the CLI/Argo-snapshot
`budget` output, and their tests, across `policy.py`, `approvals.py`,
`merge.py`, `warden.py` and `triage.py`. `MAX_OPEN_INVESTIGATIONS`
(concurrency) and `check_repo_not_in_flight()` (the per-repo lock) are
untouched — a count ceiling and a concurrency/correctness bound were never
the same thing. DESIGN.md's original § *Budgets* (v1 sketch) gets a dated
disposition paragraph rather than a rewrite, same pattern as the Argo
override; `docs/triage.md`'s several budget mentions were corrected in place.

`/review --deep` (sideclaw `needs-human` + native high-effort) caught four
real bugs beyond the brief, all fixed in the same commit: (1) the `implement`
handler's claim CAS required `implement_job IS NULL`, which permanently
blocked a re-implement from Argo on any `needs_human` item carrying a stale
`implement_job` from a prior failed attempt (`poll_implement_jobs()` never
clears that column on its own `needs_human` branches) — dropped the
`expect_null`, which also incidentally fixes the adversary-flagged
reinvestigate→verdict→implement stranding, since that path shares the same
CAS; (2) `_apply_argo_note` had no CAS and no per-action dedup, contradicting
its own documented idempotency contract — added `expect_state=` plus an
action-id-tagged stamp so a redelivered note (an `ack_action()` POST that
failed last tick) is detected and skipped rather than duplicated; (3)
`_board_item_issue()` crashed on a `github_issue` row whose `payload_json`
was valid JSON but not an object (`isinstance(payload, dict)` guard added,
matching `_safe_json()`'s own pattern); (4) `_apply_argo_reinvestigate()`
never called `sync_card()` on success, unlike every sibling handler, leaving
a stale card for up to one tick. Also hardened on review: `fetch_actions()`
now bounds its read at `MAX_BODY_BYTES` (the one inbound body read in a file
whose other two functions only ever POST) instead of buffering an unbounded
response. One separately caught, unrelated regression: the implementer's own
diff had drifted `scripts/clients/sideclaw.py`'s `DISPATCH_SCHEMA_VERSION`
2→3 and added an `applied_in_place` outcome with no sideclaw-side source to
re-read against — reverted outright (that file's own docstring: "never guess
a version or an outcome list") before it could break `check-schemas`/mask a
real drift.

`test_triage.py` 265/265 (recorded gate, up from 256 — net of ~15 budget
tests removed and ~22 Argo-action/regression tests added), `test_clients.py`
105/105, `test_api.py` 42/42, `make test` green, `make check-routing` and
`make check-schemas` green. `make check-policy` still fails on the same
Wave-1-left-behind disagreement (sideclaw's own boundary still allows
`implement` on `sideclaw`/`warden`) — unchanged by this wave, still a
sideclaw-repo fix.

`docs/waves/PLAN.md` Wave 3 (argo API: the action queue, in `~/SourceRoot/argo`)
is active next.

## 72. GitHub issues in warden, Wave 4: the dashboard's one-click triage section (2026-09-15)

`docs/waves/PLAN.md` Wave 4, entirely in `~/SourceRoot/argo`. `Wave 3`'s own
state-log entry was never written (that work happened in argo, not here) —
its full account is in `argo/docs/waves/PLAN.md`'s own Wave 3 Left behind,
not duplicated here.

`apps/dashboard/src/features/warden/issues-section.tsx` (new) adds a
GitHub-issues section to `/warden`, scoped to `origin: "github_issue"` items
and grouped by pipeline stage (`groupIssueItems()` in `model.ts`: needs_you /
running / auto_implementing / done — carved out of raw `state`, not warden's
own board buckets) rather than the generic board's raw-state buckets. Each
row: repo#number link (guarded by a new `isSafeHttpUrl()` — `issue.url` can
be third-party-authored, so a non-http(s) scheme never renders as a
clickable href), title, a third-party badge, state, note (the verdict
summary), age, and one button per verb in `availableActions`
(implement/merge/reinvestigate fire immediately, dismiss/note collect a
short reason first). Buttons POST through `enqueueWardenAction()` to
`POST /warden/items/:eventId/actions` (the queue Wave 3 built); a client-side
`PendingActions` map shows "Queued: <verb>" until `reconcilePendingActions()`
sees the item's `updated_at` move past the queue moment or a 15-minute
timeout elapses — reconciled both on every fresh snapshot and on its own
30-second timer (the timer exists because React Query's structural sharing
keeps the same `items` array reference across a poll that comes back
unchanged, which would otherwise leave a timed-out entry stuck forever).

Two `/review` passes, both real findings, both fixed same wave. First pass
(4 blocking): `ActionPromptModal`'s one shared `useForm` instance leaked a
dismiss/note draft across items when closed without submitting — a second
item's prompt opened pre-filled with the first item's already-valid text and
would submit it against the wrong event if not re-cleared; fixed by
resetting the form on every close path. A keyboard Enter/Space on a nested
real control (the action Buttons, the issue Anchor) bubbled into the
card's own `onKeyDown` and opened the timeline modal in addition to the
control's own action — fixed with an `e.target !== e.currentTarget` guard.
`apps/api/src/routes/warden.ts`'s `BoardItemSchema` (present since Wave 2 on
the wire, never actually declared) got `availableActions`/`issue` added as a
hard `z.enum`/required-fields schema, which directly contradicted the file's
own stated "validated loosely so a new field never 422s the ingest" design —
warden and Argo are separately deployed with this vocabulary manually
mirrored, so one item carrying an unsynced verb would have 422'd the entire
snapshot (health, metrics, budget, board, intents, all of it) until someone
re-synced the two repos; loosened to `z.array(z.string())` plus optional
`author`/`labels`. Pending actions never expired when a poll returned a
structurally-identical board — the 30-second timer above is that fix.

That schema loosening created a new bug the second `/review` pass caught:
the dashboard's `WardenActionVerb` was derived from the wire type
(`NonNullable<WardenBoardItem['availableActions']>[number]`), which
collapsed to plain `string` the moment the enum was dropped — an
unrecognized verb would have rendered a button labelled literal `"undefined"`
that still submitted that verb to the action API, exactly contradicting the
API route's own comment that "the dashboard already only renders buttons for
verbs it recognizes." Fixed by hand-declaring `WardenActionVerb` as its own
closed union in `lib/queries/warden.ts` and filtering every read of
`item.availableActions` through a new `isKnownActionVerb()` before
rendering. Same pass caught a race in the mutation's `onError`: it cleared
`pending[eventId]` unconditionally, so a stale/late-failing action could wipe
out a *newer* pending action queued for the same event — fixed by capturing
`queuedAt` as mutation context in `onMutate` and only clearing on a match.

Cheap fixes applied alongside: `use-warden-actions.ts` (new) pulls the
mutation + pending-state + reconciliation out of `WardenPage`, mirroring
`model.ts`'s ownership of every other board derivation and cutting the
page's own complexity; `board-item-cells.tsx` (new) shares `StateBadge`/
`AgeText` between `board-section.tsx` and `issues-section.tsx`'s table
columns, the one duplication fix judged safe to auto-apply.
`reconcilePendingActions()` now returns the same reference when nothing
changed, and `deriveWardenPage()` falls back to a stable empty-array
constant — both guard against a needless re-render/render-loop risk the
second review flagged. `fallow`'s audit never went fully green and was
deliberately not chased further: a stashed pre-Wave-4 check run proved its
"24 unused dependencies" finding is pre-existing, repo-wide debt unrelated to
this diff; the remaining ~3 duplicate-clone groups (the card skeleton, the
responsive table/card switch, the top-level empty-state wrapper, shared
between `board-section.tsx` and `issues-section.tsx`) is the same
architectural call the review's architect angle explicitly flagged as
"worth a deliberate decision, not an auto-apply" (a generic
`EntityListSection<T>`) — full list of what's deferred and why is in
`docs/waves/PLAN.md`'s Wave 4 Left behind, not repeated here.

`/check` (argo): format/lint/typecheck/test all green (1034 API + 269
dashboard tests). Pushed argo `master` at `a85c6d3` — GitHub Actions'
`Deploy` workflow (api + dashboard, both via RollHook) succeeded in ~1
minute; `/api/health` confirmed the new commit. Verified live via an
authenticated chrome-devtools session against `https://argo.jkrumm.com/warden`:
the section renders the real backlog (6 needs-you, 1 auto-implementing,
correct per-state buttons), zero console errors.

`docs/waves/PLAN.md` Wave 5 (end to end on a real issue) is active next.

## 73. Wave 5, end to end on a real issue — and the bug that surfaced doing it (2026-09-15)

Ran the whole GitHub-issue pipeline against real repos instead of unit-test
fixtures: an owner issue (`usage-tracker#3`, "README Usage section is
missing 'make uninstall-agent'") through ingest -> investigate (high
confidence, `nextAction=implement`) -> auto-implement -> draft PR
(`usage-tracker#4`) -> sideclaw `review` validation (clean, 122 turns,
architect/senior-dev/qa/adversary) -> `merge_blocked` (correctly refused:
`usage-tracker` has no `autoMergePaths` declared, so nothing merges without
an explicit scope — the safe default, not a bug). Confirmed in both the
ledger and the live Argo `/warden` board (authenticated chrome-devtools
session) at every stage. Closed the PR unmerged and the issue as test
cleanup per the owner's instruction (delivered mid-wave, not re-litigated);
the underlying doc fix is real and small enough to redo by hand if wanted.

A third-party-shaped item exercised the other branch:
`sy-serendipity#24`, ingested as a real owner-authored issue then patched
in the ledger (`events.payload_json.author`, `triage_items.max_tier`) to
read as `external-contributor`/`investigate` before the fold — same
"fixture author" shape the plan called for, since a second real GitHub
identity was never available. The investigate verdict came back
`confidence=high, nextAction=implement` anyway, and the third-party gate
held: landed in `needs_human`, never auto-implemented. Dismissed for real
through the actual Argo action queue (not a CLI shortcut) — logged in,
clicked Dismiss, filled the reason modal (Mantine's controlled textarea
needed a real keystroke event; `fill()` alone left Submit disabled — a
`press_key`+`type_text` nudge fixed it), watched the row go
"QUEUED: DISMISS", then `triage.py --run` pulled and applied it
(`authorized_by="owner:argo"`) — ledger and page both landed on
`dismissed` with the typed reason as the note.

**The bug this surfaced.** `dispatch-scratch` (the one private repo in the
whole `jkrumm/*` fleet) never once appeared in `ingest_github_issues()`'s
search results, even for an issue opened minutes earlier — its own #2 had
silently gone `resolved_at`-stamped by a *previous* tick despite still
being open on GitHub. Root cause: `op://mini/github/token` (a fine-grained
PAT) 403s on `GET /repos/jkrumm/dispatch-scratch/issues` and 422s on
`/search/issues?q=repo:...` for the same repo — it has Metadata:read
(`GET /repos/...` succeeds, 200) but not Issues:read on this one private
repo. `search_issues()` silently drops a repo it can't search exactly the
way it drops a repo with no open issues; `ingest_github_issues()`'s
disappearance-resolve treated "missing from the result set" as sufficient
proof of closure, so a repo the token merely can't see got its still-open
issue silently marked resolved. Fixed: `clients/github.py` gained
`read_issue()` (single-issue `GET`, raises on non-200 same as `read_pr()`/
`read_repo()`); `ingest_github_issues()`'s resolve loop now calls it before
resolving anything and only proceeds on a confirmed `state == "closed"` —
any error (403/404/anything) leaves the event alone, fail-closed. Verified
against `dispatch-scratch` directly (`GET /repos/.../issues/2` also 403s,
same token) and against a real closure in the field: closing
`sy-serendipity#24` for cleanup, on the next tick, `read_issue()` returned
a genuine `state: "closed"` and the event resolved correctly — the fix's
"confirm, don't infer" path exercised live, not just in the two new
`test_triage.py` cases (267/267, +2). A second, narrower finding from the
same probe: the token also 403s on `POST .../issues/{n}/comments` on a
*public* repo (`usage-tracker`) — the "comment back on the owner's own
issue" feature (§70/docs/triage.md) has silently no-op'd in production
since Wave 1 shipped; the try/except around it swallows the 403 as a
one-line stderr print, so nothing else broke, but the owner never actually
saw a comment on any of their own issues. Both are the same token missing
scope, left as an owner action (below) — neither is a warden-code fix.

Also relearned, the hard way, a distinction the plan's own text already
states but the CLI doesn't enforce: `scripts/triage.py --run` polls
investigate-tier dispatches through `escalate_origin_items()`/
`maybe_auto_implement()`/`poll_implement_jobs()`/`poll_validation_jobs()`,
but an *investigate*-tier job's own terminal poll lives in
`scripts/dispatch-sweep.py`, not `triage.py`. Running `dispatch-sweep.py`
against an implement-tier job in flight (`usage-tracker`'s draft-PR
dispatch) hit its "no origin_channel — closing with a sentinel" branch,
logged and no-op'd rather than corrupting the item (verified: state and
`implement_job` were unchanged after) — but it's a sharp edge for anyone
hand-running ticks outside the LaunchAgent schedule. Filed here rather than
in code because the behavior is correct (defensive, ledger-safe); the edge
is procedural, not a bug.

Confirmed the rest of the backlog `docs/waves/PLAN.md` named is on the
Argo page with correct assessments: `research-gateway#3`/`#5` sitting in
`merge_blocked` on real validation findings (a regex over-match, an
under-constrained consistency-correction acceptance — both genuine, both
worth a human look, neither this wave's to fix), `#6`/`#7` in
`needs_human`, `sideclaw#3`/`#4` in `needs_human` with the correct
"capped at tier 'investigate'" note despite a stale `max_tier=implement`
column stamped before Wave 1's cap took effect (the runtime check at
dispatch/fold time catches it regardless — confirms the column being stale
is cosmetic, not a live gap), `basalt-ui#51`/`#52` and `rollhook#21`
correctly `closed` (shipped/landed-by-hand), `ntfy-mac#12` correctly
`closed` as non-actionable (a question, not a bug). One pre-existing,
untouched find while reading the board: `research-gateway#4` has been
stuck in `implementing` since before this wave, `note` reading "deferred:
refusing to run inside a Claude Code session (CLAUDECODE is set)" — the
same recursion guard this wave's own manual ticks kept hitting
(`env -u CLAUDECODE` fixes it per-invocation), but this item's stall
predates this session and its next scheduled `warden-loop` tick (LaunchAgent,
no `CLAUDECODE` set) should clear it on its own; flagged, not touched.

`make test` 267/267 (was 265, +2 for the disappearance-resolve fix),
`make check-schemas` and `make check-routing` green; `make check-policy`
still disagrees on the same pre-existing `sideclaw`/`warden` ceiling drift
Wave 1 left behind (sideclaw's own boundary not yet capped) — unchanged by
this wave, still a sideclaw-repo fix.

**Owner actions, both on `op://mini/github/token`
(github.com/settings/personal-access-tokens):** grant `Issues: Read` on
`dispatch-scratch` specifically (it's the one private repo in scope, so
this is a one-repo grant, not a blanket widen) so real dispatch-scratch
issues stop being invisible to intake; separately grant `Issues: Write`
repo-wide (or at minimum on every repo issues get filed against) so the
comment-back feature actually posts instead of silently 403ing. Neither
blocks anything else — third-party and owner-issue routing both work
correctly without them, as this wave proved by working around both.

`docs/waves/PLAN.md` is now fully done across all five waves and deleted
in this same commit — this was the last one.

## 74. The self-repo cap, lifted (2026-09-15)

Owner, after Wave 5: the `investigate` cap Wave 1 put on `sideclaw` and
`warden` was friction — `sideclaw#3`/`#4` could never get past a verdict. The
rule it encoded ("warden may never hold tier ≥ 1 on its own executor or on
itself") guarded against a closed propose-and-land loop, and that loop is
already open without it: an implement episode ends as a draft PR, and
neither repo carries `autoMergePaths`, so `merge_gate_check()` refuses and
the item waits in `merge_blocked` for the owner's Argo Merge click. The rule
is restated as "never auto-merge on sideclaw or warden" (CLAUDE.md,
`config/dispatch-repos.json` readme); `tiers.investigate` is back to `brain`,
`hermes-agent`. `make check-policy` agrees with sideclaw again (exit 0).

PAT probe after the owner added Issues read/write: private-repo issues,
search, pulls and contents read now 200; `commits/{sha}/check-runs` and
`actions/runs` still 403 — Checks: read and Actions: read are missing.

Also restarted `ai.hermes.gateway` (kickstart): the running PID predated
hermes-agent `ead94fa` (checkpoint-store fix), which #agents had asked a
human for twice.

## 75. env-check's second failure shape: the cause, not "likely transient" (2026-09-15)

`jkrumm/hermes-agent#2`, opened by the owner off item 1087's card. The card
read *"env-check ran and found no dangling item on this pass — likely
transient; the underlying event will disappearance-resolve on its own if it
clears"* while every `op`-wrapped cron on homelab was failing. The cause was
in the JSON the whole time: `[ERROR] Too many requests. Your client has been
rate-limited.` — the shared 1Password service-account budget (1000/24h,
account-wide) exhausted.

`cmd_env_check`'s `parse()` emits TWO failure shapes, not one. A missing item
lands in `danglingItems`; every OTHER non-zero `op run` (rate limit, network,
expired token) sets `ok: false` with an EMPTY `danglingItems` and the raw
output in `error`. `_run_verb()` accepts exit 0 and 3 alike, so both shapes
reach `_render_env_check_note()` intact — and that function read only
`danglingItems`, so the second shape fell through to the transient wording and
discarded the one line naming the cause. The transient sentence is correct
ONLY for a genuine clean pass.

Renderer-only fix in `scripts/triage.py`; `hermes-ops.sh` needed no change
(the episode's verdict reached the same conclusion independently, and was
right that the fix is not in the hermes-agent repo at all). When
`danglingItems` is empty and any host is not `ok`, the card now carries the
raw `error` text per host, plus a dedicated remediation when the text matches
a rate limit — the budget window, the op-daemon's cached 4026, and that the
durable fix is fewer invocations (the homelab `OP_SOCK` pin), not a 1Password
change. A top-level `ok: false` with no per-host detail fails SAFE into the
same failure branch rather than the transient one.

`_write_env_check_stub()` grew `error_homelab`/`error_vps` and now exits 3 on
failure, so the suite exercises the real subprocess boundary for both shapes.
+3 tests, `test_triage.py` 270/270; `make check-schemas`/`check-routing`/
`check-policy` all green.

Live verification against the actual probe (rate limit still active at
18:50Z): the rendered note now names `homelab (rc=1)` and the raw stderr.

Not fixed here, and not this repo's: the underlying budget exhaustion is item
1088, whose verdict (`nextAction: implement`, high confidence) landed on
homelab's own repo durability — the loop is carrying it. Item 1089 folded to
`needs_human` because `hermes-agent` is capped at `investigate`, which is
correct: this change landed by hand instead.

## 76. classify() matched rules last, so a whole producer family froze in `note` (2026-09-20)

Warden's own daily digest printed the same ten `slack_alert` signatures under
"Unstructured notes in #alerts — possible root causes nobody actioned" for a
week, and their ledger rows could never move: every one of them sat in `note`.
The family is Beszel's homelab alerts (`HomeLab CPU above threshold`, …), which
post bare sentences, so `_looks_like_bot_alert()` is False for every one of
them — and `classify()` ran the structural `ignoreUnstructuredSlackProse` filter
**before** rule matching. Two consequences, both measured: the ~15 rules
`config/triage-policy.json` had accumulated for exactly those signatures were
dead on arrival (`note` is terminal and `reopen_if_needed()` skips it), and
`_propose_mapping_candidates()` — whose question is "has this signature ever
been mapped", asked of the POLICY FILE — kept re-proposing them, seven days
running, until the file carried 151 rule entries for 61 unique match values and
49 ignore entries for 12. One frozen row was a genuinely live condition: the
homelab `Disk` alert (threshold 70 %), `triggered=1` with no resolve row since
2026-09-12. It has since resolved on the Beszel side (2026-09-19T09:09Z), which
is why the one-time reset below skips resolved events rather than carding them.

Item 1117's investigate episode (job `33d69619`) derived the fix and returned
`nextAction: implement` at high confidence — but 1117 itself was dispatched at
tier `investigate` and closed as `answered`, and 1118, its implement
continuation, could not run at all: **this repo has no git remote** (§62), and
sideclaw's `resolveRepoIdentity()` (`server/jobs/handlers/dispatch.ts` →
`dispatch-git.ts:349`) requires a GitHub `origin` for every episode that is not
`investigate` and not `worktree: in-place`. Job `a8850cc5` failed in 39 ms with
`git remote get-url origin failed (2): Remote-Repository 'origin' nicht
gefunden` and dropped item 1118 into `merge_blocked` with no artifact. So the
change landed by hand, on the episode's verdict; the tier-vs-remote question it
exposes is now an owner action in STATE.md, not a code fix here.

Three changes, all in `scripts/triage.py`:

1. `classify()` matches rules FIRST and applies the prose filter last, only to a
   row no rule matched. The filter's documented purpose is unchanged — an
   un-prefixed, rule-LESS message still lands in `note`, never `ignored` — and
   the `repo`/`verb` guard is untouched. A row the filter claims is no longer
   reported in the `unmapped` set either: it has its own digest heading, and
   `_propose_mapping_candidates()` reads the ledger, not that return value.
2. `_propose_mapping_candidates()` drops any signature the policy file already
   covers in `rules` OR `ignore`, compared through the same `_match_targets()`
   + `_fnmatch_any()` pair `classify()` itself uses — so a title-derived match
   (a `uk` monitor id) counts as covered too. A/B against a snapshot of the live
   ledger: 25 candidates → 11, the 14 covered signatures gone from the daily
   re-proposal.
3. `scripts/reset-frozen-notes.py` (new, one-time, idempotent): returns
   `note`-state `slack_alert` rows whose event is NOT resolved to `new`,
   carrying the reason on each row's own `note`, compare-and-set on
   `state = note`. Read-only without `--apply`, so a dry run cannot write even
   by accident.

Verified on a `VACUUM INTO` copy of the live ledger, never the live file: the
reset revived 6 of the 10 (4 skipped as resolved — events 105, 542, 918, 999)
and one `classify()` pass then routed the above-threshold trio (events 13, 120,
121) to `repo=homelab` for escalation, the below-threshold trio to `ignored`
(their `ignore` entries were already in the file), returned **0** unmapped
signatures, and left `note` rows 10 → 4. `tests/test_triage.py` 270 → **273/273**
— the new cases are the ordering fix (rule-mapped bare-sentence alert reaches
its repo, rule-less prose still reaches `note`) and both halves of the coverage
check, including the title-target one — plus a new
`tests/test_reset_frozen_notes.py` at 3/3. Every other suite unchanged
(`test_warden_cli.py` 65/65, `test_lifecycle.py` 98/98, `test_clients.py`
105/105, …).

Residual, deliberately not changed here: `_fetch_note_rows()` does not filter
resolved events, so the four rows above keep appearing under the digest's notes
heading until someone decides that heading should drop resolution-closed notes.
That is a digest-content decision, not part of this fix.

## 77. The field look after ten unattended days, and the half §76 left open (2026-09-20)

The owner was away from roughly 2026-09-08 and asked how the loop had done
alone, with the suspicion that the cheap-model dispatch lane was the weak part.
Measured from the ledger, read-only, 2026-09-08 → 2026-09-20:

| | |
|-|-|
| Items opened | 178 (alert 120, human 39, github_issue 19) — 98 closed, 51 quiet, 16 ignored, 7 note, 1 fixed, 2 needs_human, 1 merge_blocked |
| Dispatches | 153 — investigate 77 done / 3 failed / 16 cancelled, implement 35 / 3, review 19 / 0 |
| Duration, median / p90 | investigate 7 / 23 min, implement 40 / 93 min, review 10 / 20 min |
| Implement outcomes | 25 draft PRs, 7 `checks_failed` (→ `needs_human`, as designed), 3 `no_changes` |
| Merged | 1 of 25 (argo#18) — the other 24 are the owner's review backlog |
| Idle-watchdog kills, compaction failures | 0 |
| `/metrics` `verified_fixes_vs_silence` | 0.077 — 1 verified fix against 97 silence-closes in the 7-day window |

So the dispatch lane on `glm-5.3-flash` did not break; what is unmeasured is
the *quality* of those 24 PRs, and the funnel still closes almost everything on
silence rather than on a verified fix. The owner's two interactive failures
were a different lane: `ca deepseek-v4-pro` auto-compacted constantly because
`_ca_ctx` (dotfiles `config/zsh/iu-models.sh`) and its mirror
`GATEWAY_CONTEXT_TOKENS` (sideclaw `server/mcp/session-runner.ts`) each carry
exactly one row, `glm-5.3-flash`, and every other gateway id falls back to the
200k budget Claude Code assumes over a custom base URL; and the `glm-5.3-flash`
"hang" matches modelpick's measured 13.3 tok/s effective in-loop rate, which is
a latency fact, not a fault. Neither is a warden change. sideclaw prunes
terminal jobs after 24 h, so its side of any incident older than a day is
unrecoverable — only this ledger kept the period.

Two subagent findings were checked against the code and were already fixed:
the 281 refused `implement` attempts on item 543 (2026-09-12 → 09-14) are the
tier-cap flap §67 closed with the local `resolve_tier()` check, and
`checks_failed` does fold to `needs_human`.

**The bug this look did find.** Item 121 (`slack_alert:homelab-cpu-above-threshold`)
went `quiet → new → note` at 07:53Z, six hours after §76's ordering fix, with
`repo=homelab` intact. `classify()` only consults `rules` for a row with no
repo/verb yet, and the block below it fell through to the prose filter for any
row that matched no rule *this pass* — which includes every row mapped on an
earlier pass: one still `new` because it waits on the threshold or the cluster
cap, or one back in `new` because its signature recurred. §76's own comment
described that fall-through as intended. It is not: a mapped signal is never
the filter's to route. `classify()` now `continue`s on a row that already has a
repo or verb, before the unmapped bookkeeping and the filter.
`tests/test_triage.py` 273 → **274/274**
(`test_mapped_row_survives_a_second_classify_pass`, written first and watched
fail with `state='note'`); `make test` green across all suites. The loop runs
from this working tree, so the fix was live on the next tick;
`scripts/reset-frozen-notes.py --apply` was run once more for the three rows
the bug had re-frozen (events 13, 120, 121).

Noted, not changed: `config/triage-policy.json` still carries 151 rule entries
for 61 distinct match values and 49 ignore entries for 12 — §76's dedup stops
the growth, nothing has collapsed what already accumulated. The largest
recurring alert by transition volume (`MacMini Dev Host - Push`, 571) is not a
monitor-interval problem: the host health check grades memory-pressure level 2
as FAIL, which is the decision item 543 is waiting on.

## 78. The dispatch model, measured: DeepSeek-V4-Flash, and OpenCode as a second lane (2026-09-20)

Follow-up to §77, same session. No warden code changed; this records the
evidence the next routing decision rests on.

**The 25 draft PRs, reviewed read-only by four subagents.** The real backlog is
nine open PRs — the rest were already closed as superseded (glm closed its own
intermediate drafts with ancestry checks) or merged. Verdicts: merge
research-gateway #20 → #9 → reconcile #13/#15 (both edit `groundReport`),
weatherorb #4, sideclaw #8; fix first sideclaw #6 (the CI-path guard is
`.github/`-only, so the new GitLab write path has no block on `.gitlab-ci.yml`)
and homelab #2 (docs contradict themselves on the cron mechanism); close
dotfiles #5 (conflicts with a better fix already on master, salvage
`scripts/lib/bun-bin.sh`); rollhook #26 works around sideclaw's 180 s `check`
cap inside rollhook. `glm-5.3-flash` graded B-…A- on correctness, A- on scope
and tests, C on validation evidence (self-reported; most repos have no test
CI). Two recurring misses worth a brief-level fix: it acts on an inherited
verdict without re-diffing the current default branch (dotfiles #5), and it
gets third-party type hierarchies and aggregate counts right only on a second
round (weatherorb #1→#4, research-gateway #16→#20: five dispatches for one
feature).

**modelpick ccbench on the gateway's Anthropic leg, corrected context env**
(10 tasks, `MAX_THINKING_TOKENS=8192`, zero compactions in all 40 transcripts):

| model | composite | wall | eff. tok/s | cost | tool err |
|-|-|-|-|-|-|
| DeepSeek-V4-Flash | 1.00 | 6m20s | 190 | $0.090 | 4% |
| minimax-m3 | 0.96 | 6m08s | 114 | $0.198 | 2% |
| kimi-k2.7-code | 0.95 | 5m59s | 58 | $0.303 | 3% |
| DeepSeek-V4-Pro | 1.00 | 19m11s | 37 | $0.669 | 3%, one 5-min idle stall |
| glm-5.3-flash (reference) | 0.81 | 38m24s | 13.3 | $0.035 | 0% |

Context windows measured: 1M for both DeepSeeks and minimax-m3 (accepted at
the 1.1M probe ceiling), 262,144 exact for kimi-k2.7-code. `kimi-k3` and
`deepseek-v4.1-flash` 404 on the Anthropic leg (OpenAI route only). `glm-5.2`
is dead under Claude Code 2.1.278 (the backend rejects the CLI's `verbosity`
field; the gateway masks it as a 503). The rows are in dotfiles `_ca_ctx`
(`41ab2e2`); **sideclaw's mirror `GATEWAY_CONTEXT_TOKENS` is not updated** —
its working tree carried another session's uncommitted work, so it was left
alone. Switching dispatch means three edits that must land together: that
table, `routing.ts`'s AGENT route, and `AUTO_DISPATCH_MODEL` here
(`make check-routing` fails until they agree).

**OpenCode spike** (1.18.30, scratch dir, `kimi-k3` over the OpenAI route): one
real headless episode, 149 s, 13 tool calls, 0 errors, 7/7 tests, ~22.6 tok/s.
It loads `~/.claude/CLAUDE.md` and discovers `~/.claude/skills` natively;
`rules/*.md` needs an `instructions` glob; PreToolUse hooks, `~/.claude/agents`
and Claude-only skill syntax are lost; no JSON-schema flag on `opencode run`,
so a verdict is fenced JSON the runner must strip and validate. Estimated 2–3
days for a second runner behind sideclaw's same submit/get interface. Not
needed to get off glm — DeepSeek-V4-Flash under Claude Code already is — but it
is the only way to reach `kimi-k3`.

## 79. Dispatch moves to DeepSeek-V4-Flash, on a POC through this lane (2026-09-21)

The owner's instinct after §78 was DeepSeek-V4-Pro ("Flash is too
unintelligent", and it is the older V4 Flash — `deepseek-v4.1-flash` is
OpenAI-route only). He asked for the external data to be refreshed first and
for a POC that lets dispatched agents decide the open PRs. Both were done, and
the evidence went the other way.

**External indices, refreshed in modelpick 2026-09-20** (AA quality / AA coding
index / terminal-bench v2.1): V4-Pro 36.0 / 68.8 / 0.787, V4-Flash 34.3 / 69.1 /
0.787 — tied. `glm-5.3-flash` leads both (41.8 / 71.5 / 0.843); `kimi-k3`
(43.6 / 76.2 / 0.850) and `deepseek-v4.1-flash` (39.5) are the smarter ones and
both 404 on the Anthropic leg. On ccbench's hardest task the scorer saturates;
read by hand, Flash's solution was the more rigorous of the two. Pro's one
idle stall was structural: the CLI auto-backgrounded a Bash call at its 120 s
foreground limit, the model emitted an empty turn and waited silently for the
notification until the watchdog fired — in this lane that is a verdict-less
kill folding to `needs_human`.

**POC.** The same six read-only briefs ("decide this open PR: MERGE /
FIX-THEN-MERGE / CLOSE") went through `warden dispatch --tier investigate
--model …` on both models: 12/12 `done`, one attempt each, no stall, no
compaction. sideclaw's own clock: Flash 0.7–2.9 min per episode, Pro 1.0–6.0
(glm's field median for investigate was 7.1). Against the independent Sonnet
reviews of §78:

| PR | Sonnet | V4-Pro | V4-Flash |
|-|-|-|-|
| weatherorb #4 | merge | merge | fix-then-merge: the secrets-run smoke test does not traverse the uv hop it claims to (Pro listed it as a nit) |
| rollhook #26 | fix in sideclaw instead | **merge** — missed it | fix in sideclaw instead, verified in sideclaw's `check.md`/`check.ts` |
| homelab #2 | fix-then-merge | **merge**, nits only | fix-then-merge, plus a defect neither other reviewer found: `setup.sh` emits the cron line without the profile prefix the PR's own docs assert |
| dotfiles #5 | close, salvage `bun-bin.sh` | close, salvage it; found the `shlock` guard never releases a dead pid's lock (verified live) | rebase and cut the hunks master already has; found the pinned-tailscale branch is now unconditionally true |
| research-gateway #20 | merge after running tests | merge; title understates scope | fix-then-merge: a new unconditional per-job LLM call lands without re-measuring `docs/measurements.md` § Job duration, which that repo's CLAUDE.md requires |
| usage-tracker #4 | reopen + merge | same | same; noted the attribution footer in the PR body breaks house rules |

Flash agreed with the independent review wherever Pro was lenient, and read the
target repos' own rules more closely. Six episodes is a small sample; the
direction is not ambiguous.

**The change.** `AUTO_DISPATCH_MODEL` defaults to `DeepSeek-V4-Flash`; sideclaw's
AGENT route moves in the same sitting (its `GATEWAY_CONTEXT_TOKENS` row,
1,000,000, landed the day before in `1d94541`), CLASSIFY stays on glm —
untested there. `TRIAGE_AUTO_DISPATCH_MODEL` is the one-line way back.
`tests/test_triage.py` 274/274. What to watch for a week: `dispatches.error`,
idle-watchdog kills, and the tool-error rate (4% in ccbench against glm's 0%).

**Found on the way, not fixed.** `dispatches.finished_at` is stamped with the
time warden *observes* a terminal job, not the time sideclaw finished it: the
twelve POC rows read 614–625 minutes because the polling shell was suspended
overnight and `warden status` stamped them on resume, while `reported_at` (the
sweep) had them at 5–20 minutes. It also explains why §77's implement
durations cluster on multiples of ten minutes — they are tick-quantized. Every
duration this ledger reports is an upper bound; sideclaw's `started_at` /
`finished_at` are the real numbers, and it prunes them after 24 h.

## 80. The first full lifecycle on the new dispatch model (2026-09-21, 02:40Z → 03:07Z)

Two `human`-origin items ran investigate → verdict → implement → step-7 review
on `DeepSeek-V4-Flash`, review on sideclaw's JUDGE route as before:

| item | repo | investigate | implement | review | outcome |
|-|-|-|-|-|-|
| 1142 | usage-tracker | 0.4 min | 2.0 min | 0.7 min, `confirmed` | draft PR #5, `merge_blocked` — no `autoMergePaths` (by design) |
| 1143 | homelab | 1.3 min | 4.9 min | 1.8 min, `blocked` | draft PR #3, `merge_blocked` on a real review finding: the cron line now sources `/root/.profile`, but the install steps only tell the operator to fill the user's `.profile` |

Durations are sideclaw's clock. glm's field medians were 7.1 and 40.1 minutes.
**The lifecycle took 27 minutes of wall clock for under nine minutes of work** —
every stage boundary waits for the next 600 s loop tick (`verdict` 02:41/02:46 →
`implementing` 02:47:25 → `validating` 02:57:29 → `merge_blocked` 03:07:31).
With a worker this fast the loop interval, not the model, is now the latency of
an item; nothing was changed about it here.

How the items came to exist is its own finding. This session opened 1140/1141
with `warden run` and no `--tier implement`, so `max_tier` defaulted to
`investigate` and both closed as `answered` on a high-confidence `implement`
verdict — correct behaviour, wrong invocation. Its re-file (1144/1145) arrived
70 seconds after a second operator had already re-filed the same briefs
verbatim as 1142/1143 *with* an origin channel (Hermes's door), and that
operator then aborted 1144 and closed 1145 as duplicates through the CLI with
an exact `--why`. No other warden session was running on the host. The dedup
was right and the ledger shows every step of it; who decided to re-file is not
recorded anywhere but the CLI audit log's `why`.

Also noted: sideclaw appends "Opened automatically by a bounded dispatch
episode, from this brief: …" to every PR body. One of the §79 review episodes
flagged it against the owner's global no-attribution rule. It is provenance,
written by the tooling rather than the model, and it is sideclaw's to decide.

## 81. The digest's notes heading drops what has already resolved (2026-09-22)

The loop kept printing the same four `slack_alert` signatures under
"Unstructured notes in #alerts — possible root causes nobody actioned" after
§76 unfroze the family and §77 closed the re-freeze, because those four were
exactly the rows §76's revive had to skip: their events were already resolved
when it ran (105 photos incident, 542 homelab disk, 918 recovered reply, 999
VPN self-heal — resolved 2026-09-14 → 2026-09-19). `_fetch_note_rows()`
selected on `ti.state = note` alone, so a resolution-closed row printed under
that heading every day regardless. §77 left this as "a digest-content decision,
not part of this fix"; this makes the decision.

The change is one predicate, `AND e.resolved_at IS NULL`. The heading claims an
unactioned root cause, and a resolved event is the producer saying the condition
is over: the row is still terminal `note`, as designed, it simply has nothing
left to report. `classify()` and `scripts/reset-frozen-notes.py` are untouched —
`note` stays terminal for unresolved prose, and those four rows stay where they
are.

Measured against the live ledger: `note` rows 4 → 0, all four resolution-closed,
so with no unmapped signatures and nothing auto-proposed the digest is now
silent until a real unactioned note appears. `tests/test_triage.py` 274 →
**276/276** (`test_digest_drops_note_rows_whose_event_already_resolved` and
`test_digest_is_silent_when_every_note_row_has_resolved`, asserted on the
digest's own payload and on the day cursor not being burned by an empty one);
`make test` green across all suites. The loop runs from this working tree, so
the change is live on the next tick.

Noted, unchanged: `config/triage-policy.json` still carries 151 rule entries
for 61 distinct match values and 49 ignore entries for 12 (§77), and the 24
unreviewed draft PRs across nine repos remain the owner's backlog (§78).

## 82. `abort` left the rest of the cluster behind (2026-09-22)

An alert batch files several items onto ONE `dispatch_job` — the `hermes_log`
checkpoint family filed three for a single 15:13:07 batch (`rev-parse`,
`ls-files -X exclude`, `add -A`) — and `fold_dispatch_verdict()` has always
treated that job as the cluster's membership. `cmd_abort` did not. It stamped
the shared `dispatches` row cancelled (cluster-wide) and `reported_at` with it,
then transitioned only the item it was called on, so every sibling was left in
an in-flight state with **no exit at all**: `close` refuses in-flight states by
design, a second `abort` refused on sideclaw's 409 (`PolicyError: job already
cancelled`, untolerated where the 404 `no job` case was), and the sweep only
reads rows with `reported_at IS NULL` — the stamp the abort itself had just
written. The one thing left for such a row was its deadline, which files a
needs_human card for work a human has already decided against. Hit live: item
1114 aborted on the checkpoint alert, sibling 1153 stranded `investigating`
with a 2 h deadline.

Two changes, both in `scripts/warden.py`. An already-terminal job (sideclaw's
409) is now tolerated exactly as the 404 `no job` case already was — the
abort's intent, "no episode is running against this cluster", already holds —
and the abort discharges every other row sharing the job, in the same
transition and with the same note. That set is deliberately narrower than
`_CLOSE_INFLIGHT_STATES`: only the three states an episode puts a row in. A
sibling that has moved past the episode (`pr_open`, `merge_blocked`,
`needs_human`) carries work of its own — a PR a human must review — and is
never closed behind their back by cancelling the job.

Tests: `test_abort_discharges_every_member_of_the_cluster_sharing_the_job`,
`test_abort_tolerates_an_already_terminal_job_and_still_discharges_the_cluster`,
`test_abort_leaves_a_cluster_sibling_that_already_opened_a_pr_alone` —
`tests/test_warden_cli.py` 65 → **68/68**, `make test` green across all 17
suites (`test_triage.py` 276/276 unchanged). Measured against a `VACUUM INTO`
copy of the live ledger: rows sharing an already-cancelled job and still in an
episode state **1 → 0**. The one was 1153, discharged through the corrected
verb — the CLI is invoked from this working tree, so the fix was live on the
call that closed it.

The alert that surfaced it, same session: items 1114/1152/1153 were one benign
batch. A dispatch brief was written into the then-nonexistent
`~/.hermes/workspace`, so the pre-write checkpoint resolved a workdir that did
not exist yet and skipped `rev-parse`/`ls-files`/`add -A` with "working
directory not found". The write created the directory, the next snapshot at
15:13:49 committed the file (`refs/hermes/37e946ea139051c7`), and a live
`CheckpointManager.ensure_checkpoint()` returns True with `git fsck` clean, 29
refs, 420 of 500 MB. The card's ×N is double the real count — the poller tails
both `errors.log` and `gateway.error.log` and both reads increment one
signature — and the store needs nothing repaired. The only real defect is
severity (an expected skip logged at ERROR), and `hermes-agent` is
investigate-capped, so that stays a report.

## 83. `abort --dry-run` was the one verb where a dry run did the thing (2026-09-22)

The global `--dry-run` flag is accepted on every verb, and `cmd_close`, `merge`
and `dispatch` honour it (DESIGN.md's contract: nothing outward-facing, nothing
written). `cmd_abort` never looked at it. `warden abort 1114 --why … --dry-run`
cancelled job `6e53fcb4` on sideclaw and transitioned item 1114 to `closed`;
the follow-up call *without* the flag then refused with "state 'closed', no
in-flight episode". The audit log has it in two adjacent lines — `mode=aborted
rc=0` for the "dry run", `mode=refused rc=4` for the real one: the preview was
the effect.

One early return now, mirroring `cmd_close`'s own: it names the state it would
leave, the job it would cancel, and writes nothing.
`test_abort_dry_run_cancels_nothing_and_writes_nothing` runs it with no stub
server at all — a closed port, so any cancel attempt fails loudly — and asserts
both items unchanged, the dispatch row still `running`, and zero transition
rows. `tests/test_warden_cli.py` 68 → **69/69**.

## 84. Two automatic models: investigate on Flash, implement on Pro (2026-09-22)

The owner's split, after §79's field look: "DeepSeek V4 Pro for the hard
stuff, V4 Flash for easy or faster work". `AUTO_DISPATCH_MODEL` (env
`TRIAGE_AUTO_DISPATCH_MODEL`, `DeepSeek-V4-Flash`) now covers only the
read-only investigate episodes and Argo's re-investigate; a new
`AUTO_IMPLEMENT_MODEL` (env `TRIAGE_AUTO_IMPLEMENT_MODEL`, `DeepSeek-V4-Pro`)
covers `maybe_auto_implement()` and Argo's implement click. Both carry the
same Claude-id guard. `make check-routing` still compares only the investigate
model against sideclaw's dispatch default — the implement model is passed
explicitly per job and sideclaw's `GATEWAY_CONTEXT_TOKENS` has had its 1M row
since `1d94541`. `tests/test_triage.py` 276/276 (§81–§83's tests plus the
implement assertion re-pointed and a guard that the two knobs differ).

Recorded against it, not as a veto: on this gateway Pro reuses the prompt
cache poorly (transcripts since 09-13: 9% headless, 26% interactive, against
94–97% for Flash and glm), so every Pro turn re-processes the 60–75k-token
prefix; modelpick is measuring why (backend alternation vs `cache_control`
ignored vs prefix not advancing) so the fix lands on the right side. Until
that is known, an implement episode on Pro is correct, slower than Flash, and
billed on nearly full input each turn.

Also fixed the same day, in dotfiles (`f5b6ac0`): `_ca_ctx`/`_ca_thinking`
match gateway ids case-insensitively — the 09-17 interactive session that
compacted five times in an hour ran as lowercase `deepseek-v4-pro`, which the
table did not know.

## 85. Why Pro misses the cache: the boundary never advances (2026-09-22)

modelpick measured it directly (`23f9c15`, `scripts/adhoc-cache-probe.ts`):
72 streamed `/v1/messages` calls, a ~36k-token system prefix, all three models
on the same Requesty-proxied backend. Pro caches the system block as well as
the others (99.96% on a repeat, marker or not). The difference is a growing
conversation: Flash's and glm's cached prefix extends into the appended turns,
Pro's stays flat at the system block for all six turns. So every Pro turn
re-processes the whole history after the system prompt, and
`cache_read/total_input` decays toward `system / (system + history)` — the 9%
and 26% seen in the transcripts. Nothing on our side fixes it; it is a
question for the gateway team (does Pro's backend extend an existing cache
past the marked block). Consequence for §84's split: an implement episode on
Pro is priced on nearly full input from the second turn on, and the longer the
loop the worse the ratio — Pro suits short, bounded hard work better than long
implement loops. The split stands as the owner's call;
`TRIAGE_AUTO_IMPLEMENT_MODEL=DeepSeek-V4-Flash` is the line back.

## 86. §85 withdrawn: Pro's cache advances like everyone else's (2026-09-22)

The owner did not believe §85 and was right. The probe behind it appended
17–147 tokens per turn — under DeepSeek's 64-token cache chunk and Anthropic's
1024-token minimum block — so it could never show a cache advancing. Re-run
with 2–4k-token tool-result-shaped turns and a sliding `cache_control`, three
runs per model (modelpick `0c0bb08`, `db65a39`): DeepSeek-V4-Pro's
`cache_read` grows in lockstep with the conversation at ~0.90 of input, 18/18
turns, indistinguishable from Flash and glm. So Pro is not "priced on full
input from the second turn"; §84's split stands without that caveat.

What is still true and still unexplained: two real Pro dispatch episodes from
2026-09-20 show `cache_read` flat around 5k while input climbs past 88k, with
7–135 s between turns (well inside any TTL). The controlled probe does not
reproduce it. Candidates not yet measured: the `thinking` block Pro emits
every turn, or real `tool_use`/`tool_result` blocks, sitting between the
breakpoint and the new content. No config change is recommended on a guess —
that is how §85 happened. The two interactive symptoms the owner hit are
explained without it: glm's 13–35 tok/s is the model, and the 09-17 Pro
session ran as lowercase `deepseek-v4-pro`, which the context table did not
know (dotfiles `f5b6ac0` matches case-insensitively now).

Also today, dotfiles `290a1f1`: `rd bg` typed its base64 brief into the
pane's tty, and macOS truncates canonical input at 1024 bytes — eight
multi-paragraph briefs in a row never started a daemon. The brief is staged
in a host file now. And the auto-mode classifier refused to launch the seven
colleague sessions this session tried to open for the PR backlog; the briefs
are in `/tmp/warden-poc/briefs/` for the owner to launch.

## 87. Advance on completion: the 600s tick was an item's latency, not the model's (2026-09-22)

§80 measured a full lifecycle — verdict → implementing → validating →
merge_blocked — at 27 minutes of wall clock for under 9 minutes of real
dispatch work, because every one of those three stage boundaries only ever
advances inside `triage.run()`, the loop's own 600s tick. dispatch-sweep.py
already polls every open dispatch on its own 300s cadence and already folds
an INVESTIGATE job's terminal verdict onto its triage_items row the moment
it sees one (`fold_dispatch_verdict()`, keyed on `dispatch_job` — the
column only the first, investigate dispatch of a cluster ever populates).
But that fold never propagated further: `maybe_auto_implement()`,
`poll_implement_jobs()` and `poll_validation_jobs()` — the functions that
actually walk verdict → implementing → validating → merge/merge_blocked —
lived only in `run()`, so a verdict folded by the sweep at, say, 300s past
the hour still sat idle until the loop's own tick at 600s before anything
looked at it again, and the same 600s gap repeated at every later boundary.

**The fix reuses the loop's own functions, not a second implementation of
them.** `triage.py` gets one new function, `advance_implement_chain()`,
which is exactly the three calls `run()` used to make inline
(`maybe_auto_implement`, `poll_implement_jobs`, `poll_validation_jobs`, in
that order — the order that already let an item crossing a stage earlier in
the same run get picked up by the next stage without waiting a further
tick). `run()` now calls this one function instead of the three; behaviour
is unchanged there (it is a pure extraction — the full 276/276 suite passes
byte-for-byte with no test edits needed at all). `dispatch-sweep.py` calls
the *exact same function*, once, unconditionally, at the end of every 300s
pass — after its own per-row poll loop, so a verdict this same pass just
folded is already visible to `maybe_auto_implement()` before the pass ends.

**Why this is safe with no new lock, and why it is not a second loop.**
Every step inside the chain already re-derives its own eligibility from the
ledger on each call and is CAS-guarded end to end:
`maybe_auto_implement()`'s claim is `UPDATE ... WHERE state='verdict' AND
implement_job IS NULL` (wins once, ever); `poll_implement_jobs()` and
`poll_validation_jobs()` each do their own fresh sideclaw poll per row
before touching a state, and a `done` job stays `done` no matter which of
two processes reads it first. Two cron processes calling this chain is
exactly as safe as the loop calling it twice in a row already was, which it
always tolerated (a slow tick immediately followed by a fast one on
restart). DESIGN.md's "no second loop" is about a SECOND SCHEDULE deciding
state — this adds no schedule at all: dispatch-sweep.py already existed,
already runs every 300s, and is already the one process watching sideclaw
for exactly the signal ("a job just went terminal") that makes this chain
worth re-running. The alternative — dropping the loop's own `StartInterval`
so `run()` itself ticks faster — was rejected: `run()` also does GitHub
issue ingest, `classify()`, the `propose_mappings()` LLM call, the digest
and the Argo snapshot push, none of which have a latency complaint against
them and none of which benefit from running every 300s or less; shortening
the loop's tick would pay that cost on every one of those steps for a
benefit only the three-function chain needs, and the sweep already sits at
300s — there is no latency left to buy by going below the cadence the
process already watching for the trigger runs at. `maybe_check_liveness()`
(step 10, the post-merge deploy-verification poll) stays on the loop's own
600s tick alone: its window is `LIVENESS_WINDOW_HOURS`, not seconds, so
300s buys it nothing.

**`dispatches.finished_at` was the poll's own wall clock, not sideclaw's**
(§79's open item). Every terminal fold — `lifecycle/dispatch.py`'s
`sync_record()` (called by `poll_implement_jobs()`/`poll_validation_jobs()`/
`warden status`) and dispatch-sweep.py's own per-row fold — stamped
`finished_at` with `now`, the moment THIS process happened to observe the
job terminal, not the moment sideclaw itself finished it. §79's own
12-episode POC already proved the gap: twelve rows read 614–625 minutes
because the polling shell was suspended overnight. sideclaw's job envelope
(`GET /api/jobs/:id`, `JobView.finishedAt` — server/jobs/types.ts) already
carries this as an epoch-ms field on every terminal job; warden received it
on every poll and discarded it. `clients/sideclaw.py` gets one new
function, `finished_at_iso(job, fallback=now)`, converting that field to an
ISO UTC string when present and falling back to the caller's own `now`
only for the rare terminal job that carries none (there never was a
"pruned" job's envelope to read — `dispatch-sweep.py`'s `_mark_pruned()`
path is unchanged, correctly, since there sideclaw's own value could never
exist). Both fold sites now call it. `dispatches.finished_at` is now
sideclaw's own ground truth, not an upper bound.

**Tests.** `tests/test_triage.py` stays **276/276** — `advance_implement_chain()`
is a pure extraction, exercised by every existing test that calls
`triage.run()`, with no test edits needed to keep it green. Ten new tests,
none touching that gate: `test_clients.py` 105 → **107/107**
(`finished_at_iso()`'s own two cases — prefers sideclaw's timestamp, falls
back on a missing/non-numeric/boolean value); `test_lifecycle.py` 98 →
**100/100** (`sync_record()` end to end — a job finished hours before an
overnight-suspended poll observes it must not stamp the late observation
time); a new file, `tests/test_dispatch_sweep_pipeline.py` (6/6) — pins
that `main()` calls `advance_implement_chain()` exactly once per pass
regardless of whether there was anything to report, threads `--dry-run`
through correctly, survives a raised exception inside the chain without
failing the sweep itself, and — the one that pins the actual fix — that an
item this same pass's row loop just folded to `verdict` is visible to
`advance_implement_chain()` before the pass ends, not the next one.
`make test`: 18 suites, all green.

**Not yet measured against a real item.** This landed from a background
session working in an isolated worktree (`docs/history/state-log.md`'s own
convention plus this session's own isolation policy) — the five
LaunchAgents run `scripts/{triage,dispatch-sweep}.py` straight out of the
main checkout's working tree, never a worktree, so a live `dispatch-scratch`
`warden run --tier implement` only exercises this fix once these commits are
on `master` there. Left as the owner's own next step (`warden run
dispatch-scratch --tier implement` with a trivial brief is the safe target,
per this task's own brief): compare the new lifecycle's stage-boundary
timestamps against §80's four (verdict → implementing 02:47:25 →
validating 02:57:29 → merge_blocked 03:07:31, each a ~10-minute jump) —
the expected shape now is each boundary landing within roughly one 300s
sweep pass of the prior stage's own completion, not on the next 600s
multiple.

No LaunchAgent plist changed — no `StartInterval` moved, no new agent
added — so this needs no `make setup` and no reload; `make status` still
shows the same five agents. The sweep and the loop are already separate
processes spawned fresh by cron each tick, so the new code takes effect on
each script's very next scheduled invocation once these commits reach
`master`, with nothing to restart.

## 88. Agent instructions live in AGENTS.md (2026-09-23)

Estate-wide migration per `dotfiles/docs/agents-md.md`: the repo's instructions
moved verbatim to `AGENTS.md`; `CLAUDE.md` is now exactly `@AGENTS.md`, the
only shape that loads on every Claude lane (a cold `CLAUDE_CONFIG_DIR` over the
IU endpoint drops a bare AGENTS.md) while OpenCode and Codex read `AGENTS.md`
natively. No `@import` lines to relocate. Pointers that meant *this repo's*
instructions (`§The ledger`, `§Talking to sideclaw`, the regression gate, the
closed allowlists) now name `AGENTS.md`, as do the cross-repo ones into
dotfiles and brain, which migrate the same way. Pointers into hermes-agent's
and the global `~/.claude/CLAUDE.md` are unchanged. No warden code reads a
repo's instruction file — dispatched episodes get the target repo's context
from sideclaw's `claude` invocation, which follows the shim — so there is no
code path to switch. `requirements.txt` (`cryptography==50.0.0`) left pinned.

## 89. The deliberate fallback probe stops minting a card (2026-09-23)

A post-rollout check validates the brain's fallback chain by overriding the
model to a sentinel id ending in `-does-not-exist`, so the IU endpoint is
*guaranteed* to 404 — the 404 is the probe's expected result, and the fallback
then serves the turn (`Fallback activated: <sentinel> -> gpt-6-luna`, then
`API call #1` at 1.7s). Hermes retries `api_max_retries` times, so **one probe
writes exactly three ERROR lines** to `logs/errors.log` and lands squarely on
`minOccurrences: 3`: a Slack card, a triage item and a queued investigate
episode per probe run. Item 676 on 2026-09-23 was exactly that — one probe run
2m41s after the gateway restart that made the `gpt-6-luna` fallback live, and
the episode could only restate what `agent.log` already said.

`poll_hermes_logs()` now skips any line naming a sentinel id
(`PROBE_SENTINEL_RE`). Two deliberate choices: it is a **content** filter in the
poller, not an entry in `triage-policy.json`'s `ignore`, because that list keys
on `external_id` and the signature is truncated at 120 chars of `module: msg` —
*before* the model id — so an ignore entry would have suppressed a genuine
`No suitable backend server found` for the real brain model as well, which is
the one 404 here worth alerting on. And it filters the line rather than
downgrading the probe's log level, which is Hermes's side of the fence.
Regression guard: `tests/test_watchdog_hermes_log_probe.py` — sentinel dropped,
a real 404 and an ordinary ERROR still reported, a mixed batch yielding exactly
the real signature. `make test` 19 suites green.

## 90. A re-fired `note` row returns to the digest (2026-09-26)

The 2026-09-26 digest's "Unstructured notes in #alerts" heading carried exactly
one line — `slack_alert:self-healing-success-vpn-stack-restored-after-gluetun-was-offline`
("*Self-Healing Success*: VPN stack restored after gluetun was offline"). It is
an `ignore`d recovery notice, so it should not have been there: the entry
covering it was appended by `propose_mappings()` on 2026-09-20 with the reason
"Genuine recovery notice; VPN stack already restored."

It printed anyway, and the mechanism is the half §81's STATE bullet got wrong.
Its row (event 999) was frozen in the terminal `note` state on 2026-09-12, and
`classify()` only ever touches `new` — so the entry that landed eight days later
can never reach it. `_fetch_note_rows()` filters on `events.resolved_at IS NULL`,
and a `slack_alert` signature that **re-fires** has its `resolved_at` cleared
back to NULL: `watchdog-poll.py`'s `reconcile()` writes
`resolved_at=NULL, first_seen=<now>` for any resolved event it observes again
(lines 970-976), which is why this row's `first_seen` reads 2026-09-26 04:30 for
a row that is 14 days old. It re-fired when the VPN stack self-healed at 04:10
UTC — the same event that reopened items 1000-1004 out of `quiet`. A
covered-but-frozen row therefore returns to the heading on every recurrence,
immune to the policy that now covers it.

Measured: three `note` rows in the live ledger, 1 revivable and 2 resolved (105,
918) and correctly skipped. Rehearsed on a `VACUUM INTO` snapshot first
(`reset-frozen-notes.py --db <snap>`, then `classify()` called by hand against
that same snapshot): `note -> new -> ignored`, and `_fetch_note_rows()` empty.
Applied live; the 04:42:16 tick took it `new -> ignored` with no card and no
traceback, and the heading is silent again. The daily digest cursor means no
second digest today.

This is a **standing repair, not a one-time run**, so `reset-frozen-notes.py`'s
docstring ("only needs to run ONCE", "a second run reports zero") is corrected in
this commit, and STATE.md's carried-debt bullet is rewritten in its place: three
rows stay silent while their events stay resolved, and a recurrence is what
brings one back.

The underlying incident is a different item's story, and this row was never its
vehicle: the five gluetun-cascade signatures (1000-1004) were closed by hand at
04:35 the same morning with the verified cause — an unattended-upgrades kernel
reboot at 04:00:36 UTC (6.8.0-139 -> 6.8.0-142), `gluetun` `restart: "no"` by
design so its four `network_mode: service:gluetun` dependents exited with it, and
`vpn-cycle.sh` self-healing the stack at 04:10:40. Watchtower was ruled out.

## 91. A chronic signature escalates instead of going quiet (2026-09-28)

The owner asked why the same alerts keep coming back and warden does not
self-heal them. A read of the ledger against 14 days of `#alerts` (264 messages,
40 families) found one structural reason in the loop itself, plus one bug.

**Every self-clearing alert was structurally uninvestigable.** `run()` executes
`reopen_if_needed()` → `apply_resolutions()` → `resolve_recovery_paired()` →
`resolve_quiet_grouped()` → `escalate()`, in that order, and the poll runs every
1800 s. A HyperDX alert that fires and clears inside 5–20 minutes has already
posted its ✅ by the time the poll sees it, so the pass that reopens the row to
`new` recovery-resolves it back to `quiet` before `escalate()` runs. The
transition log shows `quiet → new → quiet` pairs stamped with the same
microsecond: item 847 (`VPS edge p95`) 22 times, 932 (`job.reaped`) 10, 846
(`edge 5xx`) 8, the Kuma `Research Gateway` pair 6 and 4 — 0 dispatches between
932, 1135 and 1136. `occurrences` could not catch it either: it is the last
poll's batch count, so each of those rows read `1` throughout. DESIGN.md's rule
("silence may cancel the need to *start* work") is right for a one-off and wrong
for a signature that keeps coming back; the recurrence is the need.

Fix: `_is_chronic()` — a mapped row with ≥ `chronicRecurrences` (3) terminal →
`new` transitions in `chronicWindowDays` (7) is exempt from all three silence
paths, so `escalate()` sees it. Its brief says `CHRONIC: cleared on its own and
came back N times` and that a miscalibrated monitor is fixed in the repo that
owns its config, not silenced. Unmapped rows are never chronic (nothing could
escalate them). One investigation per chronic signature per window: a row whose `dispatch_job`
was created inside the window silence-resolves as before, so a signature whose
fix is parked elsewhere does not re-dispatch every `cooldownHours` (review
finding: ~28 identical episodes a week otherwise). Nothing is narrowed: the silence rule still applies only to `new`, and
a chronic row is simply held out of it.

**The quiet timer read the wrong clock.** `resolve_quiet_grouped()` anchored on
`last_reminder_at`/`notified_at`/`first_seen`, which move only when
`upsert_grouped()` *emits*; a cooldown-suppressed occurrence moves only
`payload_json.ts_last` — the same slot `_occurrence_mark()` already reads to
reopen the row. So the row reopened on the fresh `ts_last` and was quiet-resolved
in the same pass "since 2026-09-20 14:17" on 09-22 and 09-23 (item 1135, six
times). `_quiet_anchor()` now takes the latest of all three ISO clocks and `ts_last`.
`watchdog-poll.py`'s `sweep_stale_grouped()` has the same blind spot on its 7-day
TTL and is left alone here (the owner of that column; recorded, not fixed).

Snapshot check (`VACUUM INTO`, `--dry-run`): clean pass. Chronic by today's
ledger: 846 and 932 (`vps`, 7 reopens each in 7 d) — both escalate on their
next recurrence; 1135/1136 are unmapped and stay digest-only until mapped.

Tests: `tests/test_triage.py` 289/289 (eight new: investigated once per window; the quiet anchor takes the latest ISO clock; chronic escalates with the
brief line; two reopens still quiet; reopens outside the window ignored;
unmapped never chronic; a chronic `uk` row survives `apply_resolutions()`; the
quiet timer honours `ts_last`).

What this does **not** fix, and the owner's full review is in the session's
report: the loudest family (`MacMini Dev Host`, 87 of 264) is research-gateway's
own mini deploys (`launchctl` restart, exit 0) graded as crashes by
`devhost-health-check.sh`; its fix is dotfiles PR #7, parked `merge_blocked` on
a review finding since 09-23 with every recurrence folding silently into item 543.

## 92. A blocked fix goes back to the implementer; a parked item counts its recurrences (2026-09-28)

The owner, on this morning's report: fixes rot in draft PRs nobody hears about,
and the chain has to run through review → merge → deploy → verify on its own.
Two of the gaps it named are closed here; the last mile is §93.

**Revision (`maybe_revise_blocked()`).** `poll_validation_jobs()` sent any
blocking review finding straight to `merge_blocked`, where it waited for a human
— dotfiles#7 sat five days on one concrete finding while its alert paged 16
times. Now the first step of `advance_implement_chain()` (so both the 600 s loop
and the 300 s sweep run it) picks up a `merge_blocked`/`needs_human` item with a
*revisable* reason — the independent review blocked it (`validation_status =
'blocked'`, findings from the review job's own verdict) or the repo's checks
failed before push (`checks_failed`) — and opens a fresh implement episode:
start from the previous branch, fix every finding, keep scope. Claim is a
compare-and-set into `implementing` with `revision_count+1` before the dispatch;
a submit that definitely failed hands the item back unchanged. The superseded
PR is closed with a pointer, so revisions do not leave dead drafts. Cap:
`revisionMaxAttempts` (2) per item — an attempt count, like
`hostVerbMaxAttempts`, never a turn or time limit on the episode. A needs-human
review, a merge-gate refusal or a failed deploy is a question, not a finding,
and still goes to a human.

**Parked recurrences (`track_parked_recurrences()`).** A parked row absorbs its
signal's recurrences (`reopen_if_needed()` only touches terminal rows). Each pass
now compares the event's `_occurrence_mark()` to the one seen last pass and
counts the moves per parking; the card's *Action required* block says
`Recurred N× since it parked here`, `/board` (and so Argo's snapshot) carries
`parked_recurrences` and `revision_count`, and at `parkedRecurrenceReminder` (5)
one reminder threads under the card regardless of `REMINDER_MAX_COUNT`. Leaving
the parked states resets the count.

Schema 11 (`ledger.py` migration 11): `triage_items.revision_count`,
`parked_mark`, `parked_recurrences`, `recurrence_reminded_at`, all additive.

**It went live before the commit.** The LaunchAgents run this working tree, so
the 09:51 UTC sweep ran the new code: it migrated the live ledger to 11 and
revised item 543 for real (job `fc480bd3`, PR dotfiles#7 closed with the
superseded note). The episode found master already carrying another session's
marker-based fix (dotfiles `0fcf58d`/`668c47c`, research-gateway `0cab0ed`),
changed nothing and said so — the right answer; 543 closed with those commits.
`warden-api` was kickstarted onto schema 11 (`/health` ok). Lesson recorded:
in this repo "uncommitted" is not "undeployed".

Tests: `test_triage.py` 297/297 (+5 revision, +3 parked), `test_api.py` 42/42
(board item shape), `test_ledger.py` 28/28 (adoption column set).

## 93. The last mile: homelab, weatherorb and research-gateway merge, deploy and verify (2026-09-28)

Before this, only `vps/observability/**` could go review → merge → deploy →
verify. Every other alert fix ended as a draft PR plus `needs_human` —
homelab#9 (item 1170) even passed review and was refused for "no
autoMergePaths declared for 'homelab'". The owner's call (REVIEW.md C3
disposition update, DESIGN.md case 4 narrowed): reviewed, merged, deployed and
verified without him, except where the loop would merge its own executor.

| Repo | Scope (`autoMergePaths`) | Deploy | Liveness (`fixed` needs) |
|-|-|-|-|
| homelab | `uptime-kuma/monitors.yaml` | `uk-sync` — `ssh homelab`, `git pull --ff-only`, `op run … sync.py` on the server (argv verified headless with `--dry-run`, exit 0) | `kuma-push-fresh`: the item's own monitor UP after the deploy |
| weatherorb | `src/weatherorb/watchdog/**`, `tests/**`, `docs/**` | `weatherorb-pull` — `git -C ~/SourceRoot/weatherorb pull --ff-only` (periodic LaunchAgents exec the checkout) | `kuma-push-fresh` |
| research-gateway | `src/**`, `bin/**`, `docs/**`, `evals/**`, `README.md`, `AGENTS.md` | `deployByPoller` — merge is deploy via the CI-gated mini poller | `mini-checkout-live`: `~/.research-gateway/app` contains the merge sha and `:7780/health` says ok |

Deploy definitions (`sync.py`, `Makefile`, `scripts/**`, `launchd/**`) and
dependency files stay outside every scope — warden must not write what it runs.
A Kuma-liveness repo whose item did not come from a Kuma monitor (e.g. a GitHub
issue) lands `merged` after a good deploy instead of a window that can only
time out. A liveness that never confirms reopens the item to `new` with the PR
in its history — the loop keeps going until the fault is actually gone.

**The review is now a gate, not a code read.** `_open_validation_dispatch()`
passed `context=None`: the reviewer never saw the goal. It now gets
`_validation_context()` — the investigation's recommendation plus
`VALIDATION_GATE_QUESTIONS` (goal met, safe, and for monitor/threshold changes:
evidence the old setting misfired, else blocking). Reviewer and implementer are
different families (`GET /api/routing`: review `claude-sonnet-5[1m]` + adversary
`gpt-5.6-terra`; dispatch `deepseek-v4.1-flash`). Caveat: dispatch's fallback is
Sonnet on Max, so a fallback-run implement shares a family with its reviewer.

**Policy-refused merges retry once per policy change**
(`retry_policy_refused_merges()`): a `merge_blocked` item parked on `merge
refused:` with a confirmed, unmerged PR is re-attempted when
`config/triage-policy.json` is newer than the item — `_merge_and_rollout()` is
the old tail of `poll_validation_jobs()`, extracted so both share it.

Tests: `test_triage.py` 305/305 (+8). An edit briefly left `triage.py` with an
IndentationError for about a minute; no agent ran in that window (all five
`.err` logs clean) — edits to live scripts now go through a backup-and-parse
guard.

## 94. What waits on the owner is one list, and a gated merge is one click (2026-09-28)

**The approval path for the gated repos never worked.** `plan_or_land()` runs
`merge_gate_check()`, which refuses any repo without `autoMergePaths` — and
`warden`, `sideclaw`, `dotfiles` have none by design. So the `warden merge …
--confirm` every gated card told the owner to run, and Argo's Merge, refused
every time; and a gated fix waited in `needs_human`, where Argo offered no Merge
button at all. Now: `OWNER_AUTHORIZERS = ("owner:argo", "cli:confirm")` —
an owner merge skips exactly the unattended stand-ins (path scope, confirmed
review, zero-CI acknowledgement); a failing check, CI-definition paths, the
size ceilings, forks and PR-required repos still refuse him. `_apply_argo_merge()`
accepts `needs_human` with a PR and goes through `_merge_and_rollout()`, so an
owner merge deploys and verifies like an automatic one (a successful Argo
merge used to leave the item sitting in `merge_blocked`). `availableActions`
offers `merge` on a `needs_human` item that carries a PR. The hard gate on the
loop's own repos is unchanged — it is still his click; it is now a click that
works.

**`awaiting_owner` on `/board`** (and so in every Argo snapshot): every parked
item with `age_days` in its state, the reason (its note), `parked_recurrences`,
`revision_count` and its actions, plus every **stranded PR** — a PR warden
opened whose item ended while the PR stayed open — oldest first.
`reconcile_stranded_prs()` (loop, at most hourly, one GitHub read per unmerged
implement PR of the last 60 days whose item is parked or terminal) writes the
stranded ones to the `stranded_prs` cursor, and records a PR merged by hand:
`dispatches.merged_at` stamped, a parked item moved to `merged` ("merged
outside the loop" — item 1170, homelab#9, parked on "already merged").
Nothing is auto-closed: dotfiles#6's item was closed by hand around a PR that
is exactly the fix still wanted. Argo renders the list at the top of
`/warden` (argo commit in the next §/STATE).

Found open today: rollhook#26 (review confirmed; rollhook is PR-required —
branch protection, a human merges), dotfiles#6 (finding fixed by hand on the
branch; gated), research-gateway#27 (its item closed "rejected/superseded"),
plus research-gateway#8 and weatherorb#5, which no ledger row owns (opened by
another lane) and so appear in no list — named here instead.

Tests: `test_triage.py` 308/308 (+3), `test_merge.py` 64/64 (+1).

Argo side of §94: argo `b138ce1` — `board.awaiting_owner` accepted by the
snapshot schema (loose, optional: an older snapshot never 422s) and rendered
first on `/warden` as "Waiting on you" (age, stale ≥ 3 d, reason, recurrences,
revisions, PR link, the same action buttons). Dashboard 277 tests, typecheck and
lint green; deployed (`Deploy` run success, `/api/health` reports `b138ce1`).

## 95. Episodes see the live state the verdicts kept asking a human for (2026-09-28)

21 of 36 alert verdicts before today were `nextAction=human`, most of them
"the state lives outside this checkout". Three read-only gatherers join
`EVIDENCE_ALLOWLIST`, each bounded by `EVIDENCE_TIMEOUT` and returning a line
instead of raising, prioritised by the loudest families:

| Key | Reads | Wired to |
|-|-|-|
| `launchd-restarts` | `launchctl list` (only com.jkrumm.* jobs with a non-zero last status; -15 is deliberate), deliberate-restart markers of the last 24 h (dotfiles `lib/launchd-restarts.sh` contract), research-gateway deploy log tail | `uk:macmini-dev-host-push` |
| `beszel-alerts` | homelab `/mnt/hdd/beszel/data.db` read-only over `ssh homelab`: rules (threshold, minutes), last six firings, latest 1 m sample incl. temperatures | homelab temperature / CPU / 5 m load / disk |
| `kuma-monitor-config` | the monitor's block in homelab's **public** `monitors.yaml` + its last 25 heartbeats (down/up, longest gap) | `uk:macmini-*`, Dev Host, Brain Sync, Home Line (`uk:207`) |

homelab-private's monitors are deliberately never read — its details must not
travel into another repo's brief. Exercised live: Beszel shows Temperature
`> 90 for 15 min` fired twice on 09-26 for three minutes each; Brain Sync's and
Home Line's last 25 beats are all up; Dev Host's single recent DOWN (09:35Z)
predates the marker fix and carries no FAIL text.

Not built, named: a HyperDX/ClickHouse window gatherer (the vps investigations
already query ClickHouse themselves, 847's did), docker state on homelab/vps
(raw `docker` from warden is against the house rule; a read-only Makefile
target would be the way).

Tests: `test_triage.py` 313/313 (+5).

## 96. The review of §91–§95, and what it changed (2026-09-28)

A multi-angle sideclaw review of `16aab36..1927d8c` (security, concurrency,
resilience, backend; job `2c7fe918`) returned three blocking findings, all real,
all in the merge sharing §93/§94 introduced, plus a fourth this session found
first:

- **`cli:confirm` is not the owner.** §94 counted `warden merge --confirm` as an
  owner merge. An episode's Bash can run that command — `require_no_recursion()`
  checks env markers an `env -u` removes, and AGENTS.md already says an episode is
  not contained — so a prompt-injected episode could have merged any open dispatch
  PR, the gated repos included. `OWNER_AUTHORIZERS = ("owner:argo",)`: only the
  tailnet-bounded Argo click (the owner's 2026-09-15 decision) skips the unattended
  gate; gated cards now say "click Merge in Argo".
- **An ambiguous Argo merge was reported refused.** `_merge_and_rollout()` now
  returns `merged | refused | ambiguous`; `_apply_argo_merge()` acks an ambiguous
  outcome `applied` with the reconcile note, as the pre-§94 handler did.
- **A losing concurrent merge could clobber the winner.** Every refusal write in
  `_merge_and_rollout()` carries `expect_state=<the state the caller found>`, and
  `retry_policy_refused_merges()` claims the row (CAS on `updated_at`) before the
  slow merge — the 300 s sweep and 600 s loop both reach it.
- **The owner bypass was wider than stated.** It now skips exactly the path scope
  and the zero-CI acknowledgement. A confirmed step-7 review is required of the
  owner too, and a failing check refuses him. (The review's claim that CI-definition
  paths and size ceilings were bypassed does not hold: `plan_or_land()` enforces
  both before `merge_gate_check()` runs.) Argo offers Merge on a `needs_human` item
  only when its PR's review confirmed.

Also taken: `awaiting_owner` sorts an unknown age last instead of as newest;
`reconcile_stranded_prs()` joins on the item's *current* `implement_job`, so a PR
a revision superseded is not re-read hourly; a stale comment; a nested generator.
Not taken, named: `_apply_argo_merge()` still loads the policy itself; retry
eligibility keys off the `merge refused:` note prefix (now a shared constant, not
a structured column); a failed `close_pr()` on a superseded PR is stderr-only;
revision briefs carry reviewer text unsanitised (bounded by the 2-attempt cap and
the same step-7 review); `triage.py` is ~8k lines — extracting the revision and
reconcile code into `scripts/lifecycle/` is a deliberate next-wave call.

Tests: `test_triage.py` 315/315 (+2: ambiguous Argo merge; the loser never
clobbers), `test_merge.py` 64/64 (owner gate rewritten to the narrowed rule).

## 97. A gate is what GitHub enforces, not what a list says (2026-09-28)

The owner found it: §94's report put rollhook#26 on his "waiting on you" list as
"needs a human review on GitHub". The repository ruleset `protect-default-branch`
requires **0** approving reviews; the PR was `MERGEABLE`/`CLEAN` with green CI and
merged with one command. The claim came from warden's own code:
`plan_or_land()` called `policy.merge_precheck_repo()`, which refused any repo in
`~/.claude/pr-required-repos.json` "because it requires a human pull-request
review". That file means something else — Claude may not *push* to master there
(the `protect-branches` hook) — and a dispatch PR is exactly the workflow it asks
for. Invented friction, parked on the human.

Now `plan_or_land()` reads GitHub itself: `github.branch_rules()`
(`GET /repos/{o}/{r}/rules/branches/{default}`) → `required_approving_reviews()`;
a refusal names the count *read from the ruleset*. `pick_merge_method()` honours
the same rules (`required_linear_history` rules out a merge commit;
`allowed_merge_methods` narrows the choice) instead of discovering it as a
GitHub 405. `merge_precheck_repo()`/`pr_required_path()` are deleted — the
property they claimed to serve ("never merge where a human review is
required") is now served by the thing that actually enforces it. Mergeability
and CI were already read live (`mergeable`/`mergeable_state`, check-runs).

Tests: the four list-precheck tests (`test_lifecycle.py`) became four real-rule
tests (`test_clients.py`: review count, linear history, allowed methods, a
non-200 raises); `test_merge.py` +1 (a zero-review ruleset merges, and never by
merge commit under linear history). Numbered cases 825 → 826.

## 98. Less surface, same behaviour: policy dedup, a dead script, a stale worktree (2026-09-28)

- `config/triage-policy.json`: 166 rules → 76, 52 ignore entries → 15 (367 lines,
  −127). Only later duplicates of an already-listed `match` were dropped; first
  match wins, so they were inert. Proven, not assumed: every one of the 1263
  events in the live ledger classifies identically (rule and ignore) under the
  old and new file — 0 differences. §76's proposal dedup keeps it from regrowing.
- `scripts/validate-dispatch-policy.py` deleted (90 lines): no Makefile target or
  script calls it since `make status` dropped it; `dispatch-repos.json` is
  validated where it is used (`policy.resolve_repo()`/`resolve_tier()`) and
  checked against sideclaw by `make check-policy`. No property depended on it.
- The merged `.claude/worktrees/advance-on-completion` worktree and its branch
  removed — a full second copy of every script that every grep hit.

## 99. Most fixes merge unattended; what never may is a list in code (2026-09-28)

The owner, twice today: most fixes should be reviewed, merged, deployed and
verified without him. Before this, an unattended merge needed an explicit
`autoMergePaths` per repo — three repos had one — so every other correct fix
parked as a draft PR.

- **Default scope.** `merge.effective_repo_entry()`: a repo whose triage-policy
  entry declares no `autoMergePaths` and that is not merge-approval gated gets
  `DEFAULT_REPO_ENTRY = {"autoMergePaths": ["**"], "noCiRequired": True}`. Explicit
  entries win unchanged (argo keeps its one-file canary scope, vps its
  `observability/**`). The gate for everything else: the step-7 review with the
  goal and gate questions (§93), the implement tier's pre-push checks, CI where a
  repo runs PR checks, GitHub's own rules (§97), mergeability — then deploy and
  liveness where declared, else `merged`, and a recurrence reopens the item.
- **`NEVER_AUTO_MERGE` — DESIGN.md § Self-concealing change made executable.**
  "Deploy-target definitions stay outside every `autoMergePaths`" was a
  convention the policy file could break. It is now a tuple in `merge.py` checked
  by `merge_gate_check()` for every unattended merge whatever the scope says:
  `.github/**`, Makefiles, `scripts/**`, launchd/plists, Dockerfiles, compose
  files, package manifests and lockfiles, `.env*`/`*.tpl`. Only the owner's Argo
  merge passes it.
- **"The loop never merges its own executor" moved into the merge itself.**
  Before, only triage's routing kept `warden`/`sideclaw`/`dotfiles` from an
  unattended merge, and `_merge_needs_approval()` failed *open* on an unreadable
  `dispatch-repos.json`. `effective_repo_entry()` fails closed: a gated repo, or
  an unreadable policy, gets no default scope, so the gate refuses there too.
- **Test harness.** One triage test reached the real GitHub API (a
  `markPullRequestReadyForReview` with a fake node id, refused NOT_FOUND) the
  moment the gate it relied on moved. `_triage_env()` now fakes every GitHub
  write a merge can reach (`mark_ready_for_review`, `merge_pr`,
  `delete_branch`) with a loud throw.

Tests: `test_merge.py` 65 → 69 (gated repo refuses; unreadable policy fails
closed; ungated repo gets the default; eight NEVER paths refuse inside `**`;
the owner passes). Numbered cases 826 → 830.

## 100. The loop audits itself: named invariants, and its own wrong answers become work (2026-09-28)

The owner: the loop should notice its own gaps and make work of them, and every
rule should be readable somewhere by name. `run_self_audit()` (first step of
`run()` after the intent drain, at most hourly) runs `check_invariants()` — seven
named invariants (DESIGN.md § Executable invariants, one test each) — and
`self_audit_findings()`: a review that blocks every PR in a repo, a liveness
probe that never confirms, a `fixed` that reopened, a revision budget used up.
Each finding is one `warden_self` event (a new `INGEST_SOURCE`), kept in step
with the finding — inserted or re-opened while it holds, resolved the pass it
stops holding — and routed by `warden_self:*` → `warden` (first rule in the
policy). From there it is an ordinary item: investigate, implement, and the
owner's Argo merge (warden is merge-approval gated; the loop never merges its
own executor). The summary is `/health.self_audit` (null before the first run).

INV-6 (dead draft) and INV-7 (owner queue > 3 d) report only: a self-item about
item 750 (the macOS update only the owner can apply) would have been a second
entry for one thing that needs him. The dry run on a live snapshot found exactly
that one violation, and it is what drew the line.

Tests: `test_triage.py` 315 → 325, `test_api.py` 42 → 43. Numbered cases 830 → 841.

## 101. The approval buttons nobody could click, and the Hermes side of the integration (2026-09-28)

**A dead end since 2026-09-11, verified in source.** `warden dispatch --tier
implement` (no item) mints an approval and posts Approve/Deny buttons.
`approvals.post_buttons()` resolved `slack.resolve_slack_token()` — Warden's own
app since §61/§63 (`slack/app-manifest.json`: `chat:write` only, no socket mode,
no interactivity). A click on a message that app posted cannot reach Hermes's
`plugins/dispatch-approval/`, the only thing that turns a click into a
signature. The ledger agrees: 6 approval rows ever, the last (2026-09-20,
weatherorb) never decided. Fix: `slack.resolve_interactive_token()` — Hermes's
token, the app that owns interactivity — for buttons only; cards, receipts and
reminders stay under Warden. Test: `test_interactive_token_is_hermes_never_the_chat_write_only_warden_app`.

Considered and not done: deleting the signed-approval path (plugin 763 lines,
`approvals.py`/`intents.py`/`signer.py`/`approval-spec.json` ~920 lines). `run
--tier implement` and Argo's actions cover what it serves, but deleting it takes
~90 numbered test cases with it and needs a change in the live Hermes gateway —
left as a named candidate, with the path now at least working.

**Hermes side (hermes-agent `6db191b`, plus two uncommitted-by-design edits):**
- `skills/warden/SKILL.md` claimed `POST /items/:id/intent` and `/note` exist;
  they never did — owner actions go through Argo's queue. It now points at
  `/board.awaiting_owner` and `/health.self_audit`.
- `skills/claude-dispatch/SKILL.md` still taught the daily budgets removed on
  2026-09-15 and a "3/day" merge cap; "fix it" now routes to `run --tier
  implement` (tracked in Argo), and the merge section names the two real refusals
  (merge-approval repos → Argo click; GitHub rules requiring a review).
- `skills/devops/warden-hand-fixes/SKILL.md` (not tracked in git; live via
  `external_dirs`) opened the ledger read-write for `VACUUM INTO`, breaking the
  one-writer rule — now `mode=ro`.
- Hermes cron `fd2fa108e0cc` (homelab PR #6 watcher) removed with `hermes cron
  remove`: PR #6 merged 2026-09-23, the follow-up it existed for ran once (item
  1169); its monitor script `scripts/homelab-pr6-state.sh` deleted.

Measured and left alone: the poller's #watchdog digest posted **0** messages in
the last 7 days, so it duplicates nothing in practice.

## 102. The review of §97–§101 (2026-09-28)

A second multi-angle review (job `d8f90a15`; concurrency and backend found
nothing, the adversary and OCR passes each caught a real issue) returned five
blocking findings, all fixed:

- **Executor gate ordering.** `effective_repo_entry()` returned an entry with
  its own `autoMergePaths` before asking `merge_needs_approval()` — safe today only
  because no gated repo has one. The gate is now checked first and
  unconditionally; a gated repo's `autoMergePaths` is stripped.
- **`NEVER_AUTO_MERGE` gaps and dead entries.** `Dockerfile.dev`, `go.mod`/`go.sum`
  etc. passed; half the list were `**/` forms that `fnmatch` (whose `*` spans `/`)
  already covered. Rewritten as `*X` = "X at any depth", plus Containerfile, Cargo,
  pnpm/yarn/npm lockfiles, Gemfile/gemspec.
- **INV-3 missed the `implementing` leg** (with 10 min of grace for the
  claim-before-dispatch window).
- **An unguarded self-audit could take the loop dark.** `run()` now wraps
  `run_self_audit()` like every other tick-critical step: rollback, one stderr
  line, the pass continues.
- **Two self-audit queries lacked the self-source filter**, so a `warden_self`
  item could have spawned findings about itself. Both now exclude it; the
  revision threshold reads `revisionMaxAttempts` from the policy.

The open question — does `GET /rules/branches/{b}` report classic branch
protection? — is answered by not depending on it: after ready-for-review,
`plan_or_land()` now refuses on GitHub's own `mergeable_state == "blocked"`, which
covers any unmet required review or check, classic protection included.

Tests: `test_triage.py` 325 → 327, `test_merge.py` 69 → 72. Numbered cases
842 → 847.

## 103. The synthetic trip: a fixed Kuma push monitor must prove it can still go DOWN (2026-09-28)

The last gap named in §§93–102. A fix that *silently removes* detection (a push
window stretched past any real outage, a watchdog that stops pushing on failure)
passes review, comes back UP, and is never heard from again — no recurrence, so
`reopen_if_needed()` has nothing to see. DESIGN.md's C3 mitigation required a
synthetic trip; it did not exist.

**How it works.** A Kuma-verified deploy (homelab, weatherorb) now enters
`liveness_pending` with `{"trip": {"status": "pending"}}`. When the item's own
monitor confirms UP, `_advance_trip()` runs instead of setting `fixed`:
`scripts/kuma-trip.py` (warden-owned, piped over `ssh homelab` into
`uptime-kuma/.venv/bin/python -` inside homelab's `op run`, the `uk-sync` door)
clones the monitor's **live** detection config — interval, retry interval,
retries — into a shadow `warden-trip:<event>` push monitor with
`notificationIDList=[]`, re-reads it and deletes it unarmed if any provider stuck,
then arms it with one UP push and leaves it silent. Each loop pass checks it:
DOWN inside `interval + retries × retryInterval + 180 s` → shadow deleted, item
`fixed` with "detection still fires"; no DOWN in the window → shadow deleted, item
back to `new` with `detection no longer fires: …` and the PR — a finding, not a
fix. A read error retries until the item's own liveness deadline (Kuma's socket
API timed out once during the proof). Non-push types return a named gap and the
item reaches `fixed` with the gap on its note.

**Never pages, leaves nothing.** The productive monitor is never touched. The
shadow has no provider — checked, because "Slack - Alerts" is `isDefault=True`
(API-created monitors do not inherit it; verified). `watchdog-poll.py`'s
`poll_uk()` drops `warden-trip:` names, so a DOWN shadow never becomes an event.
`sweep_trip_residue()` deletes, hourly, every shadow no armed trip owns.

**Proven live, twice-and-a-half.** (1) A 20 s prototype: DOWN after 30 s,
deleted, zero #alerts messages. (2) Through warden's own `_kuma_trip()`, a
shadow of the live `Brain Sync - Push` (interval 600, retries 0): DOWN after
≤ 626 s, `stop` → removed, `sweep` → nothing left, 0 messages in #alerts since
arming, 0 events for any shadow. The first attempt of (2), shadow 238, was
deleted 2 s after arming by the residue sweep — run for real by a test pass that
had started *before* `_triage_env()` faked `_kuma_trip`. The sweep works; and
that test run reaching production is exactly what the fake (added in this
commit, with a module-level `ORIGINAL_KUMA_TRIP` only the argument-validation
test calls) now prevents. The live sweep cursor was held for the proof window by
one cursor upsert.

**Named gaps.** Non-push Kuma types (HTTP, keyword, docker, ping): a
residue-free violation needs a controlled failing target, which would test the
retry window but not the status/keyword matching a fix actually changes — a
proof of the wrong thing. HyperDX alerts: evaluated inside HyperDX over
production ClickStack data, delivered by a webhook bound to #alerts; a violation
means writing synthetic telemetry into the production store and either paging
#alerts or re-pointing the alert, itself a production change. For both, `fixed`
still rests on review (`VALIDATION_GATE_QUESTIONS`), liveness and recurrence, and
the item's note says so.

Tests: `test_triage.py` 327 → 338 (armed on deploy; arms instead of fixing;
DOWN fixes; waits in window; never-DOWN reopens as a finding; gap; unarmed
waits; read errors retry to the liveness deadline; sweep keeps armed shadows;
argument validation before any ssh; the poller never ingests a shadow).
Numbered cases 847 → 858.

## 104. The ledger's way back, drilled instead of assumed (2026-09-28)

DESIGN.md said it plainly: backups ship nightly (`VACUUM INTO` →
`homelab:/mnt/hdd/backups/warden/` → restic → B2) and "there is no restore path
today". A backup nobody has restored from is an assumption — and warden is the
machine that watches everything else.

**`scripts/warden-restore.sh` → `scripts/restore.py`.** Source `latest` (default)
is the newest snapshot on homelab — the off-box copy, what is left if the mini is
gone; `local-latest` and a path also work. It copies the snapshot and the repo
bundle into a fresh `mktemp` dir and proves: `PRAGMA integrity_check` is ok; the
stamped schema is ≤ this warden's and the one migrator brings the copy to current;
events and dispatches are present and the newest write is ≤ 2 h before the
snapshot's own timestamp and not after it; the real loop runs one `--dry-run`
pass on the copy; and the repo bundle clones into a tree that contains
`scripts/triage.py` and `scripts/ledger.py`. The temp dir is removed on every
path. One line on stdout, exit 0/1, and `~/.warden/restore-drill.json`.

**It cannot touch the live ledger.** `assert_safe_target()` resolves every path
it writes (symlinks followed) and raises `UnsafeTarget` for the live `WARDEN_DB`,
its `-wal`/`-shm` siblings, or anything under `WARDEN_HOME` — a refusal, not a
warning, checked per write. Tests prove the live file, its siblings, the home,
a symlink to the live file and a symlinked directory into the home are all
refused, and that a drill pointed straight at the live ledger as its *source*
leaves its bytes identical.

**Self-checking.** `com.jkrumm.warden-restore-drill`, monthly on the 1st at 04:10
(after backup 03:10 and restic 03:30, so it restores what just went off-box;
monthly because the format, transport and migration path it proves change
rarely, while the daily backup already has its own Kuma push). The self-audit
reads the result file: a failed drill → `restore-drill-failed`, none successful
for 35 days → `restore-drill-stale`, no result at all → `restore-drill-missing`,
each a `warden_self` item routed to warden like any alert. Proven on a copy of
the live ledger: a failure record became the item "the ledger restore drill
FAILED on homelab:…", repo `warden`, state `new`.

**It found its own bug on the first agent run.** Interactively the drill passed;
under launchd it failed with `git bundle verify failed: … ein Repository
benötigt` — `bundle verify` needs a repository as its cwd, and launchd's is `/`.
That is what a drill is for. It now *clones* the bundle instead, the stronger
claim anyway. Real outputs:

    restore-drill: OK homelab:/mnt/hdd/backups/warden/backups/warden-20260928T011005Z.db integrity=ok schema=10->11 events=1255 dispatches=249 write_lag=4.3m loop_dry_run=ok repo_bundle=ok@721eb45 cleaned_up=True 24.4s

**Not proven, named:** retrieval from Backblaze B2 through restic (the drill
restores homelab's copy; a restic restore needs the B2 credentials held in the
homelab container and writes a full repository snapshot — its own drill);
putting a snapshot back over a lost live ledger (a deliberate human step:
`make unload`, copy, `make setup`); the loop's *outbound* behaviour on a restored
ledger (the dry-run makes no Slack/sideclaw/Argo writes by contract).

Tests: new `tests/test_restore.py` 11/11 (guard ×4, a good snapshot, a real
schema-(N-1) snapshot migrated, corrupt, newer schema, stale data, empty, missing);
`test_triage.py` 338 → 340 (failed drill → item; stale/missing/fresh). Numbered
cases 858 → 871.

## 105. weatherorb fully unattended; the NEVER_AUTO_MERGE widening prepared, not landed (2026-09-28)

The owner's words, as the episode recorded them out of the weatherorb-podcast
conversation: the limits on what warden may change, implement and merge — the
manual merges, the issue tiers — are invented friction; weatherorb is private,
only he files its issues, and warden should fix, review, merge, deploy and verify
there without a click, "auch für die Makefiles". **Carried as the episode's
record, not as a verified quote:** Hermes searched its own store (every user
message in `state.db`, plus this pane's transcript) and cannot re-find that
conversation, so the `autoMergePaths: ["**"]` landing below rests on the owner's
standing 2026-09-28 directive (everything "automatisch reviewed, automatisch
gemerged, deployt und verifiziert") — which is unambiguous — while the Makefile
half still waits for one word.

**What was actually in the way, verified in the checkout, not believed.** The
brief named four suspects; one was real.

1. `config/triage-policy.json` `repos.weatherorb.autoMergePaths` was
   `["src/weatherorb/watchdog/**", "tests/**", "docs/**"]` (§93). Because the
   entry *declared* a scope, `effective_repo_entry()` never fell through to
   §99's `DEFAULT_REPO_ENTRY` (`["**"]`) — weatherorb was **narrower** than a
   repo with no entry at all. Every fix outside watchdog/tests/docs ended as a
   draft PR. **Real. Fixed:** the entry now declares `["**"]` explicitly rather
   than dropping the key — the two are equivalent today, but a declared scope
   says so in the file the operator reads and cannot be narrowed by a later
   change to the default. `retry_policy_refused_merges()` re-attempts any
   `merge refused:` item on the policy file's mtime, so nothing parked on the
   old scope needs a hand.
2. `NEVER_AUTO_MERGE` (`scripts/lifecycle/merge.py`, code): Makefile, `.mk`,
   `.github/*`, `scripts/*`, `launchd/*`, plists, Dockerfiles, compose, manifests,
   lockfiles, `.env*`, `.tpl`. Still stops a weatherorb PR touching those at a
   draft. **Deliberately not changed** — it is the "warden must not write the
   code it then runs" invariant, and the owner's "auch für die Makefiles" is a
   widening of it. The diff, what each path class actually executes in
   weatherorb (no LaunchAgent runs `make`; plists are inert until
   `make launchd-install`; `deploy-edge.yml` is the one path that runs on the
   merge itself; lockfiles install on the next `uv run`), the tests it needs and
   the one question are in `docs/never-auto-merge-widening.md`. Recommended shape:
   a code-level `FULL_AUTONOMY_REPOS = {"weatherorb"}` exemption, never a
   narrower global tuple — that keeps `test_never_auto_merge_covers_variants…`
   byte-identical and the executor repos untouched. Found on the way:
   `ops/run-sync.sh`/`ops/run-blendfield.sh` are exec'd by launchd and were never
   in the tuple (`scripts/*` does not match `ops/`), so for weatherorb the
   invariant already reads "CI, service definitions, manifests", not "anything
   launchd runs".
3. Liveness (the brief's M9: `kuma-push-fresh` reads a `monitorTitle` only the
   host-verb path writes). **Stale since §93/§103.** `_confirm_and_merge()`
   derives the title from the item's own event — `_kuma_monitor_title()` takes a
   `uk` event's title verbatim (batch suffix stripped) or a `[Name]` prefix from
   `slack_alert` — and enters `liveness_pending` with `{"trip": "pending"}`; an
   item with no monitor of its own (a GitHub issue) lands `merged` saying so.
   Nothing in `triage.py` needed changing, which mattered: that file was dirty in
   a parallel session (§104) and was not touched.
4. Issue tier by authorship: `ingest_github_issues()` gives `implement` to
   `_github.GH_OWNER`'s issues and caps everyone else at `investigate`. The mini's
   GitHub identity IS the owner's fine-grained PAT (`gh api user` → the owner),
   so an issue an `author`-tier episode files is owner-authored and reaches
   `implement` with no click. **No cap to lift.** Third-party bodies are fenced
   (`_UNTRUSTED_BLOCK_*`) and capped inside the fence; owner bodies are trusted
   verbatim under `MAX_BRIEF_CHARS`. weatherorb is private (`gh api repos …` →
   `private`), so GitHub rulesets — the §97 review gate — cannot apply to it.

**"Merge is deploy" was half true.** `weatherorb-pull` was `git pull --ff-only`.
The periodic jobs (`watchdog`, `obs`, `fcstlog`, `backfill`, `blendfield`) exec
the checkout on every run, so a pull rolls them out; `tileserver` (uvicorn over
`src/`) and `sync` (`ops/run-sync.sh`) are `KeepAlive` daemons that keep their
loaded process. Under the old scope that was fine — `src/weatherorb/tileserver`
was outside it. Under `**` it is a fabricated `fixed`: a merged tileserver fix
would confirm against a watchdog push that never ran the new code. The verb
(`scripts/clients/rollout.py`, code owns the argv) now pulls and then
`launchctl kickstart -k`s tileserver and sync, `|| exit 1` — a daemon left on
old code is a failed deploy and the item is `needs_human`, never `merged`.
`serve` is the vendored open-meteo binary: nothing a merge changes without a
Swift rebuild, so it is not bounced. `kickstart -k` never re-reads a plist;
`ops/*.plist` is `NEVER_AUTO_MERGE` anyway. The web side (`apps/web/**`,
`packages/**`) deploys itself: `deploy-edge.yml` runs on the merge to master.

**Liveness proven on weatherorb's own monitor, live.** Read-only first:
`_gather_kuma_push_fresh([{"monitorTitle": "WeatherOrb Watchdog - Push",
"since": now-2h}])` → `(True, "WeatherOrb Watchdog - Push heartbeat OK at
2026-09-28 14:33 UTC (> since 12:36 UTC)")`, and `_kuma_monitor_title()` on a
`uk` row titled `WeatherOrb Watchdog - Push (×3 in batch)` → the bare title.
Then the synthetic trip through warden's own `_kuma_trip()`: shadow 240 of the
live monitor (interval 2100, retries 0, so a 2280 s window) armed at 14:36:50Z
with one UP beat and no notification provider; the hourly residue sweep's cursor
was held for the window by one upsert (as in §103).
Result: `check` at 15:10:56Z still UP (1 beat); at **15:12:03Z `down: true`**
(2 beats), 2113 s after arming, inside the window that closed at 15:14:50Z;
`stop` → `removed: true`; Kuma lists no `warden-trip:` monitor afterwards; 0
events ingested for the shadow or for `WeatherOrb Watchdog - Push` since arming.
The monitor weatherorb's fixes are verified against can still go DOWN, and the
whole chain — title from the item's own event, UP read through hermes-ops,
shadow trip, cleanup — ran on the live monitor with no code change.

Tests: `test_clients.py` 112 → 113 (the closed `weatherorb-pull` argv: pull
first, both kickstarts chained after it, no bootout/bootstrap, `serve`
untouched). `test_triage.py` stays at the §104 count, 340/340. Files changed:
`config/triage-policy.json`, `scripts/clients/rollout.py`,
`tests/test_clients.py`, `docs/never-auto-merge-widening.md` (new), this log,
`STATE.md`. Nothing in the parallel session's files.

## 106. A vanished trip shadow is unproven, never a finding (2026-09-28)

Found while re-running §105's proof by hand, in the window between that section's
arm (14:36:50Z) and its verdict: a second shadow was armed through the same
`kuma-trip.py` door and reported `{"ok": true, "exists": true, "down": false,
"beats": 1}` for 11 minutes, then `{"ok": true, "exists": false, "down": false}` at
701 s. It had been deleted — the hourly residue sweep removes every `warden-trip:`
monitor no `liveness_pending` item owns, and a shadow built by hand is owned by
nothing. §105's own shadow (240) tripped at 2113 s; a third arm (242) tripped at
2109 s, so the proof itself stands.

**The defect was not the sweep.** `_advance_trip`'s `armed` branch fell through to
its last line — "a silent shadow did not go DOWN within the window" → `reopen`
with `TRIP_FAILED_NOTE_PREFIX` ("detection no longer fires"). A shadow that has
been *deleted* is not a shadow that stayed up: that reading would have accused the
fix, in the strongest wording this system has, while the probe itself was gone.
It is the same class as the §64 verdict-less verdict — our own instrument's
silence rendered as a fact about the world.

**Fix.** An explicit branch for `exists is False`, placed before the read-error
branch: retry until the item's own `liveness_deadline`, then `reopen` with the
shadow-missing reason and "unproven, not fixed". The wording never claims detection
stopped firing, because that was never observed. For a real item the sweep keeps
its shadow (it reads `liveness_pending` items with an armed trip), so this door is
reached when the shadow disappears some other way — a manual sweep, a hand in the
Kuma UI, a restore.

Tests: `tests/test_triage.py` 340 → 341 (the vanished-shadow case: retried, never
`fixed`, note carries "gone before" + "unproven, not fixed"). Numbered cases
871 → 872. `make test` green. Files changed: `scripts/triage.py`,
`tests/test_triage.py`, this log, `STATE.md`.

## 107. NEVER_AUTO_MERGE withdrawn on the owner's word; the executor gate moves into code (2026-09-29)

§105 prepared the widening and asked one question. The answer, relayed in the
owner's words: "mach es. Es soll effektiv sein, es soll funktionieren." —
Makefiles, `ops/`, `.github/`, plists, manifests, lockfiles, `pyproject.toml`,
everything that stops a fix at a draft PR, with one exception he named and did
not withdraw: `warden`, `sideclaw` and `dotfiles`, the loop's own executor.

**What changed.** `NEVER_AUTO_MERGE` (§99) is deleted from
`scripts/lifecycle/merge.py`, and with it the path-class refusal in
`merge_gate_check()`. The default unattended scope is now literally any path.
The CI-definition refusal in `plan_or_land()` (`.github/workflows`,
`.github/actions`, older than §99 and applied even to the owner's click) is kept
for the executor repos only. What he kept is now code, not policy:
`EXECUTOR_REPOS = frozenset({"warden", "sideclaw", "dotfiles"})`, checked in
`effective_repo_entry()` before the dispatch policy's `merge_approval` is even
read — an edit to that policy file can add a gated repo, never remove one of
these three. A gated repo gets no scope, so nothing unattended lands there
whatever the path.

**Why global, not weatherorb-only.** §105's diff proposed a per-repo exemption
to keep the tuple and its pin test intact. The owner's answer was not "yes for
weatherorb", it was "everything but the executor", so a tuple that applies to
no repo would have been dead code guarding nothing. The one thing it protected
that he did not withdraw — the executor — is what stayed, and it stayed in a
stronger form (code, independent of the policy file).

**Tests re-pinned, not weakened.** `test_never_auto_merge_paths_refuse_even_inside_an_explicit_scope`
→ `test_deploy_definitions_merge_unattended_in_an_ungated_repo`: the same eight
paths plus `.github/workflows/ci.yml`, `pyproject.toml` and an `ops/*.plist`
now assert a landed merge with `merge_pr` called.
`test_never_auto_merge_covers_variants_and_other_ecosystems` →
`test_executor_repos_are_gated_in_code_not_only_in_the_dispatch_policy`: with
the fake dispatch policy gating nothing and an explicit `**` in the triage
entry, each of the three still gets no scope and refuses; `gamma` gets `**`.
`test_touches_ci_definitions_refuses` → `…_only_for_the_executor_repos`: refuses
for each of the three, merges for `gamma`. `test_owner_merge_passes_never_auto_merge_paths`
→ `test_owner_merge_passes_any_path` (unchanged body). In `test_triage.py`,
`test_merge_precheck_no_longer_refuses_on_a_stale_implement_row` needed a real
refusal that was not the stale-status one; it used a `Makefile` path, which now
merges, so it uses a failed check-run instead — its point (refused for the real
reason, never the false one) is intact. `test_clients.py`'s `weatherorb-pull`
test asserts the new order pull → `launchd-install` → kickstarts and that
`FORCE` is never passed (only a changed plist reloads).

**`weatherorb-pull` runs the Makefile it may now merge.** That is the point:
a merged `ops/*.plist` that nothing reloads would sit "merged, not deployed"
while the watchdog push confirmed liveness of the old definition. The repo's
own `make launchd-install` renders all eight plists and bootout+bootstraps only
the ones whose rendered form changed (byte-identical + loaded → left alone),
which is also the one reload that re-reads a plist; `kickstart -k` for
tileserver/sync follows as in §105. Verified `make -n launchd-install` under a
launchd-shaped `PATH` (`/usr/bin:/bin:/usr/sbin:/sbin`) resolves `make`,
`plutil`, `launchctl`, `cmp`. Not run for real: a live `launchd-install` with
nothing changed is a no-op by construction, and one with a change is the
deploy itself — the next merged plist is its proof.

**What still stops at a draft PR, everywhere.** `warden`, `sideclaw`,
`dotfiles` (no scope, Argo click only, CI definitions refused even then); a PR
over `MAX_MERGE_FILES` (40) or `MAX_MERGE_LINES` (2000); a failed or missing
check-run without `noCiRequired`; a step-7 review that did not confirm; a
GitHub ruleset the token cannot satisfy; a repo whose declared `autoMergePaths`
is narrower than `**` (homelab: `uptime-kuma/monitors.yaml`; vps:
`observability/**`; argo: the one canary file; research-gateway: its list) —
those scopes are the repos' own entries and were not touched here.

Docs: DESIGN.md § Self-concealing change (the path-list bullet struck through
with the withdrawal recorded), § Executable invariants; REVIEW.md C3's
disposition paragraph; `config/triage-policy.json` weatherorb note;
`docs/never-auto-merge-widening.md` now headed "Landed". Tests: `make test`
green — `test_merge.py` 72/72 (three tests replaced, one added, one renamed),
`test_clients.py` 113/113, `test_triage.py` at §106's count, 341/341.

## 108. A plan-gated rules read is "no rules", not a refusal — weatherorb could not merge by construction (2026-09-29)

The `revisions-exhausted-1277` self-audit card said weatherorb#7's implementer
"cannot satisfy the review". That was false, and the truth was one HTTP body
away. Items 1276 (PR #11) and 1277 (PR #14) both parked in `merge_blocked` with
the note `merge refused: GitHub returned HTTP 403 reading the rules on
jkrumm/weatherorb:master` — *after* their step-7 reviews had confirmed
(`actionable` with an empty `blocking` list maps to `confirmed`; that is
`poll_validation_jobs()`'s own table). Nothing was wrong with either diff.

**The mechanism.** `lifecycle/merge.py`'s `plan_or_land()` reads GitHub's own
rules first (§97) and refuses before any other merge check when that read
fails; `clients/github.py`'s `branch_rules()` raised on any non-200 without
capturing the body, so the reason never reached the note. The body is:

    {"message": "Upgrade to GitHub Pro or make this repository public to enable this feature."}

`weatherorb` is private (§105's own policy note says so) and **rulesets are a
paid feature on private repositories**, so this repo cannot carry one: `[]` is
the true answer, and no token grant changes it. The investigation's
recommendation — grant the fine-grained PAT the rules read — would have bought
nothing. Raising instead made every weatherorb merge impossible by
construction, in the one repo whose own entry declares `autoMergePaths: ["**"]`,
`noCiRequired: true`, `autoDeploy: true`: the fully unattended repo was the one
that could never merge.

**The change.** `branch_rules()` returns `[]` for exactly that 403, matched on
GitHub's own sentence, only on a 403. Every other non-200 stays a loud
`RemoteError` — a token that cannot read a repository's rules must never read as
"no rules" — and an unreadable *classic* protection still cannot be ridden,
because the enforcement point was never this list: `plan_or_land()` refuses
`mergeable_state == "blocked"`, GitHub's own verdict, added in §102 precisely
for the protections the rulesets endpoint does not report, and the merge call
pins the head SHA.

**Verified.** `test_clients.py` gains
`test_branch_rules_plan_gated_403_is_no_rules_not_a_refusal` (written first, RED
on the old code: `RemoteError: GitHub returned HTTP 403 reading the rules on
jkrumm/weatherorb:master`); `make test` green — `test_clients.py` 114/114,
`test_triage.py` 341/341, `test_merge.py` 72/72, `test_lifecycle.py` 102/102.
Live through this repo's venv against the real API with the loop's own
credential: `branch_rules("jkrumm", "weatherorb", "master") == []` →
`required_approving_reviews == 0`, `pick_merge_method` → `rebase`; the public
control `jkrumm/warden` still returns its three ruleset rules
(`non_fast_forward`, `deletion`, `required_linear_history`), so the exemption
hides nothing that exists. The two parked items were re-driven straight after
this commit, each attempt going through this same gate — through the loop's own
retry rather than `warden merge`, for the reason §109 records.

## 109. A refused merge is retried when the *gate* changed, not only when the policy file did (2026-09-29)

§108 fixed the rules read and, on its own, unblocked nothing.
`retry_policy_refused_merges()` compares the policy file's mtime against the
item's `updated_at` (§93) — the reference was the file, not the gate — so "two
confirmed PRs, one fixed defect, no pending policy edit" is precisely the state
that never retries. The fix sat in the checkout while items 1276 and 1277 stayed
parked.

**The change.** `_merge_gate_mtime()`: the newest mtime among `POLICY_PATH`,
`lifecycle/merge.py` and `clients/github.py`. The eligibility rule is otherwise
untouched (`merge_blocked` state, a `merge refused:` note, an implement dispatch
`confirmed` and unmerged, `_merge_needs_approval()` false, CAS-claimed before the
slow merge). Deliberately **not** a timer: a refusal the changed gate still makes
re-lands the same note with a fresh `updated_at`, so an item whose scope never
arrives (homelab before §93's policy entry) still waits for a real change instead
of re-attempting on every 300 s pass — the property the original design chose
over a retry loop, kept.

**Verified.** Two tests.
`test_merge_gate_mtime_reads_the_gate_modules_not_only_the_policy_file` points
`_merge.__file__` at a tmp file stamped newer than a back-dated policy file and
asserts the module is the reference (no real file's mtime is touched).
`test_a_refused_merge_retries_when_the_gate_changed_and_never_on_a_timer`:
gate older than the refusal → no attempt and the item stays `merge_blocked`,
however long it has parked; gate newer → exactly one attempt, item `merged`.
`make test` green — `test_triage.py` 343/343, every other suite at §108's counts.
Live, nothing is asserted yet: the next pass after this commit is the test, and
1276/1277's refusals are 11 h and 1 h older than this fix, so the first sweep
after it is expected to re-attempt both through the ordinary loop path (`warden
merge` by hand was deliberately not used — it lands the PR without moving the
item, leaving a merged PR behind a stale `merge_blocked` card).

## 110. An unreadable CI read is the credential's limit, and only `noCiRequired` may waive it (2026-09-29)

§108 and §109 got the retry through the rules read and onto the next gate, which
refused differently: `merge refused: GitHub returned HTTP 403 reading check-runs
for jkrumm/weatherorb@ffdd22ef…`. Probed directly, with the loop's own
credential:

    GET /repos/jkrumm/weatherorb/commits/<sha>/check-runs
    403  x-accepted-github-permissions: checks=read
         {"message": "Resource not accessible by personal access token"}

The PAT has `admin` on the repo — it is granted on `weatherorb` — and simply
carries no `Checks: read`. That gap only bites on a private repository: the same
call on the public `jkrumm/warden` returns 200. It is the second half of §73's
recorded PAT gap ("it still 403s on Checks and Actions read"), and it closed the
owner's own Argo Merge click too.

**The change.** `check_runs()` raises a typed `CheckRunsUnreadable` for exactly
that body — a fact about the credential, not about the commit — and
`plan_or_land()` catches it in one place: the read degrades to `[]` **only when
the repo's own policy declares `noCiRequired`**, and otherwise re-raises
unchanged. So the decision stays with the repo's declaration rather than the
token's convenience, a private repo that does have a CI gate still refuses
loudly, GitHub's own `mergeable_state == "blocked"` still refuses an unmet
required check (it is read a few lines below the gate), and the fact that the
gate was waived is written into the merge operation's receipt as
`checkRunsUnreadable` — a skipped gate is recorded, never silent.

**Verified.** `test_clients.py`: the 403 is `CheckRunsUnreadable`, a 404 is not
(115/115). `test_merge.py`: with `noCiRequired` declared the merge lands and the
receipt carries the reason; without it the same error refuses and nothing is
marked ready (74/74). `make test` green — `test_triage.py` 343/343,
`test_lifecycle.py` 102/102. The four `noCiRequired` repos in the policy
(weatherorb, homelab, vps, research-gateway) are the only ones this can affect,
and only weatherorb is private, so nothing public changes behaviour.

## 111. The revisions-exhausted finding reads the park note instead of blaming the review (2026-09-29)

The card that started this session said `github_go:jkrumm/weatherorb#7 still
blocked after 2 revisions`, with the detail *"the implementer cannot satisfy the
review in weatherorb; the brief or the gate is wrong"*. Both halves were
wrong: #7's second revision had cleared step-7 review (`actionable` with an empty
`blocking` list folds to `confirmed`), and the item was parked on a merge-time
403. The finding keys on `revision_count >= revisionMaxAttempts` plus a parked
state and hardcoded that sentence without ever reading why the item parked — so
it read as evidence of a review failure that did not exist.

**The change.** `_revision_exhaustion_detail(state, note, repo)` — the detail is
now derived from the item's own park note: a `step-7 validation (blocked):` park
keeps the review wording (that is the case it was written for), a
`merge refused:` park says the merge gate refused the confirmed PR and that
revisions cannot change that, a `step-7 validation (needs-human):` park names the
review pipeline, and anything else says which state it parked in and to read the
note. The title is unchanged.

**Verified.** `test_revisions_exhausted_reads_the_park_note_instead_of_blaming_the_review`
covers the three note shapes (written first, RED on the old code). `make test`
green — `test_triage.py` 344/344, everything else at §110's counts.


## 112. The op-refs probe ran in an environment no cron uses (2026-09-29)

`op_refs_homelab:raw:error-too-many-requests-…` paged as *"1Password refs
unresolved on homelab (.env.tpl)"* while **all six op-wrapped crons on that host
were green** — vpn-watchdog, auto-update, garmin-auto-relogin,
koinsight-stats-push, mam-seedbox-sync, mam-account-sync, wishlist-sync, last
runs minutes earlier. `OP_REF_HOSTS` sent

    cd ~/homelab && op run --env-file=.env.tpl -- true

over ssh without the `. ~/.profile;` every op-wrapped crontab line begins with,
and `OP_SOCK` is pinned in that profile. Unpinned, the client derived its socket
from an unset `XDG_RUNTIME_DIR`, dialled `/var/run/user/1000/op-daemon.sock`
(absent — the daemon listens at `~/.config/op/`), missed the daemon cache, and
spent network requests, then reported the shared service-account budget's `Too
many requests` as a broken template. The probe was answering a different
question than the one it was built for: *would the crons survive this template?*

Measured A/B, 2026-09-29: the unfaithful command returns 429; the profile-sourced
one returns 0 having made no network requests; `OP_SOCK` alone reproduces the
pass, so the socket pin is the whole difference.

Fix: `OP_REF_PROFILE = "[ -r ~/.profile ] && . ~/.profile; "`, prefixed to both
hosts. The `[ -r ]` guard is load-bearing, not decoration (`docs/decisions.md`:
`.` is a POSIX special builtin, so dash aborts the whole line on an absent
profile). Faithfulness costs no verdict — a missing item is still named.

Warden's own `triage.py` trip path had it too. The sweep is hourly by cursor,
but the cursor is written only on success, so a failing sweep re-attempts every
600s tick — and under an exhausted budget `op run` does not fail fast, so it
surfaced as `trip residue sweep failed: TimeoutError` (the 90s
TRIP_SSH_TIMEOUT) next to a direct 429 in `warden-loop.err`. So did
`hermes-ops.sh`'s `env-check` — the verb this card ran to confirm the failure —
with the uk-sync and both deploy paths. One profile-source constant now serves
each repo. `hermes-agent` has no implement lane (`investigate` ceiling), so that
half landed by hand, as §75's did.

**Verified.** `tests/test_watchdog_op_refs_env.py` written first: 5 assertions
RED on the unfixed probe (both hosts' guard + order, the argv ssh actually
receives, and the trip path's argv), green after; the missing-item case is green
throughout, which is what shows the fix does not cost detection. `make test` green — `test_triage.py`
344/344 unchanged, every other suite at §110/§111 counts. Live, through the
shipped `poll_op_refs`: both hosts `reachable=True` with **0 events**, where the
unprofiled command still 429s. Twenty-four hours on: event 1087 resolved itself
on the next poll (2026-09-29T03:20:17), that signature has fired exactly once in
the ledger's whole history, no `Too many requests` line has reached
`warden-loop.err` since the edit, and the sweep cursor reads
`{"ok": true, "removed": []}`.

## 113. A corrected rule heals the alert row it already mapped (2026-09-29)

`uk:226` (MyAnonamouse Session - Push) was auto-proposed to `warden` by
`_propose_mapping_candidates()` on an earlier day. Nothing in `warden`
implements the MAM session — the sync scripts and the account state live in
`homelab-private` — so the rule was corrected to `homelab` and committed on
2026-09-23. On 2026-09-29 the monitor fired again, `reopen_if_needed()` moved
the *same* row back to `new` with `repo='warden'` intact, `classify()` skipped
rule matching for any row that already carried a mapping, and the item
escalated to `warden` a second time: the episode ran in a checkout that cannot
see the MAM code by construction and folded a `medium` verdict guessing at an
expired session cookie. The correction was not late — it was inert, for
exactly the signature it was written for.

The guard exists to keep a *mapped* row out of the prose filter, and that
purpose is unchanged; what it also did, silently, was make the policy's own
corrections unreachable for every recurring signature. `classify()` now asks
the rules again for a row in `new` that already carries a mapping **when
`origin='alert'`**: that repo is a rule outcome, so the policy owns it. A
`human` row keeps the repo its caller chose and a `github_issue` row the
issue's own — neither is a rule outcome, and neither is re-resolved. A rule
that still says what the row already carries is not a rewrite (no `updated_at`
churn), and a rule that now names a `verb` clears `repo` (and vice versa), so a
correction can move a signature between the two lanes, not only between two
repos.

Item 843's implement lane was closed to the loop for a second reason: its
first round's failed implement episode (2026-09-23, `warden` had no `origin`
then) left `implement_job` set, and `maybe_auto_implement()` requires NULL, so
no second round could fire on the corrected verdict. The change landed by hand
on the live checkout, the §76 shape, rather than unsticking the row.

**Verified.** `tests/test_triage.py::test_corrected_rule_re_maps_a_reopened_alert_row_only`
written first — RED on the unfixed `classify()` with the row stuck at `warden`,
green after; it pins the heal, the `human` row that must NOT move, the
unchanged-rule no-op (a later `now` must not restamp `updated_at`), and
`state='new'` — a re-map is not an escalation. `make test` green, 345/345 in
`test_triage.py`. On a copy of the live ledger (`VACUUM INTO`), reopening every
mapped alert row — 81 of them — and running the fixed `classify()` re-points
**exactly one**: item 843, `uk:226`, `warden` → `homelab`. No collateral
re-points is the number that shows the guard is narrow.

## 114. A PR-wrapper finding is not a revision's job, and a revision brief carries the closing instruction (2026-09-30)

Item 1286 (weatherorb#20, the edge-of-domain tile 500s) parked at the 2-attempt
revision cap with a mergeable fix on the branch and a card reading `step-7
validation (blocked):`. The `revisions-exhausted-1286` self-audit finding told
its reader "the implementer cannot satisfy the review in weatherorb; the brief
or the gate is wrong". Three rounds, three different findings: the guard test
covered only the synthetic fixture (`tests/serve/test_app.py:395`); then the new
guard ran before the `local_index`/`time_count` bounds check and masked
TimeOutOfRange (`src/weatherorb/serve/cache.py:310`); then two findings at once —
the zero-byte bands the guard caches with no eviction path, and a PR-wrapper
finding: *"PR's stated goal is 'Closes #20' but the diff only adds non-closing
'Issue #20' source comments — issue won't auto-close on merge. Add 'Closes #20'
to the PR description or commit trailer."*

That last finding was false — the description's own final line was `Closes #20.`,
unchanged since the push (the PR's `updatedAt` 03:28, the review ran 03:33–03:37)
— and it was unsatisfiable by construction. The step-7 review reads the diff and
the commit messages; the pull request's description is not in front of it. And
nothing in a revision brief asked for the text either: `_origin_item_brief()`
carries `'Closes #<issue number>'` for the *origin* brief only, while the
revision brief — the one that has to satisfy the review — was assembled from the
findings alone. One of two attempts could therefore only ever come back with the
same finding, and the item parked at the cap.

**The change, three parts.**

1. `_is_process_only_finding()`: a step-7 blocking finding about the pull
   request's *wrapper* — its body, its trailer, whether the issue auto-closes —
   is no longer read as `blocked`. `blocked` is what spends a revision, and no
   implement episode can satisfy this class: it cuts a fresh worktree and opens
   its own PR, and the previous body is not its to edit. The class routes to
   `needs_human` with the finding on the card, where a human edits one line or
   merges without it. Recognition is narrow on purpose — a closing/auto-close
   phrase **and** a wrapper subject **and** an add-it instruction — so a finding
   that merely mentions `Closes #20` while pointing at a doc that overstates the
   code, or at a diff that does not match the body, keeps its revision. The same
   filter guards `_revision_findings()`, so an item parked `blocked` by an older
   round cannot spend its last attempt on text no episode can write either.
2. `ISSUE_CLOSING_INSTRUCTION` is one constant now, used by `_origin_item_brief()`
   and by `maybe_revise_blocked()`'s brief, for the same trusted-issue items only
   (an `alert` origin has no issue to close; an untrusted issue is
   investigate-only and never reaches the revision path). "The revision cannot
   satisfy 'add Closes #N'" is then false by construction.
3. `_revision_exhaustion_detail()` describes the rounds it can read: when no file
   was blocked on twice, the card says *"N step-7 rounds, each blocking a
   different file (…) — every round found something new, so read the last head
   before blaming the implementer"* instead of asserting an implementer failure
   it cannot evidence (§111's rule, applied to the other direction).

**What was deliberately not changed.** The obvious reading of "non-convergent" —
park when a round's blocking set is disjoint from the previous round's — was
built, measured against this item's own history, and dropped: it would have
parked attempt 2, whose finding was a regression **attempt 1 introduced**, which
is exactly the round the loop must spend. Every round in this incident held a
real defect, and a converging review is disjoint by nature — it re-flags what is
still broken before it finds the next thing. Disjointness stays a description of
a history, never a parking decision, which is where part 3 puts it. The hard form
is one early return in `maybe_revise_blocked()` if the owner wants it anyway.

**Verified.** Six tests written first, RED for the stated reason, green after;
`make test` green with `test_triage.py` at **351/351** (345 before, +6). Against
a `VACUUM INTO` copy of the live ledger the classifier is narrow exactly where it
matters: of **47** blocking findings in the whole ledger, **one** classifies
process-only — 78f7cf25's `Closes #20` finding above — and no code finding is
diverted. One live item sits at the cap (1273, dotfiles swap-gate): its two
rounds blocked on lines 62 and 63 of one script, so it keeps the old wording —
which is why the round comparison is per file, not per `file:line`.

## 115. A needs-human review is a question, not a finding, so it never spends a revision (2026-09-30)

Item 1294's whole reason to exist was one line of switch order. A step-7 review that
reports `outcome: "needs-human"` and *also* carries findings folded to the revisable
`blocked`, because `poll_validation_jobs()` tested `elif code_blocking:` before
`elif outcome == "needs-human":` — so `maybe_revise_blocked()` handed those findings
straight back to the implementer as a fresh episode. The fold contradicted the code's
own docstring a few hundred lines below it (`_revision_findings()`: "a needs-human
review … is a question, not a finding, and stays with a human") and §92, which makes
`validation_status='blocked'` and `checks_failed` the revisable reasons and nothing
else. `assert_outcome()`/`REVIEW_OUTCOMES` never caught it: the outcome was known and
handled, just handled after the findings had already won.

**What it cost, measured.** Of the **56** `review` dispatches in the live ledger that
carry a stored verdict, **19** are `needs-human` reviews that also carry findings, and
every one of them folded `blocked` — one review round in three. They belong to 15
items; 14 of the 15 were eventually closed **by hand** rather than by the loop — the fifteenth
(1273, dotfiles swap-gate) is still parked — and 1289 (weatherorb #9's flake guard) spent both of its attempts that way, its second
round's own summary naming a failed architect reviewer ("this reviewer did not examine
the diff at all"). §111's `revisions-exhausted` card is what surfaced it.

**The change is one precedence, and it runs in both directions.**
`elif outcome == "needs-human"` now sits above `elif code_blocking`, and when that
branch does carry findings they go on the card via `_format_blocking_findings()`: the
human is the reader now, so the findings must not be dropped — the same rule §114
applies to the wrapper class, which keeps its own branch immediately below. `blocked`
stays what it was for `actionable` rounds with findings; a needs-human round with an
empty `blocking` list is unchanged.

**Deliberately not done, so it is not re-derived:** the same investigation's second
recommendation — a disjoint-set guard in `maybe_revise_blocked()` — is the decision
§114 already made against, and for a better reason than the guard had: attempt 2's
finding was a regression **attempt 1 had introduced**, which is precisely the round
the loop must spend. Disjointness stays a description of a history (§114), never a
parking decision. This section does not touch it.

**Also corrected here:** six comments cited the PR-wrapper class as §113 — the log's
§113 is the `classify()` correction and the wrapper class is §114. And `STATE.md`'s
test row still read 345/345 (§114 added six tests and moved it to 351); it now carries
the true number, as does `AGENTS.md`'s gate line.

**Verified.** Test first, RED for the stated reason
(`test_validation_needs_human_with_blocking_stays_with_a_human: merge_blocked` — the
item parked `merge_blocked` on a review that had said a human had to look), green
after; `make test` green with `test_triage.py` at **352/352** (351 before, +1). The
ledger numbers above are the shipped classifier (`_is_process_only_finding()`) run
against a `VACUUM INTO` copy: 56 rows in, 19 re-classified, no code finding diverted,
no `actionable` round changed. One live item still sits parked from the old order —
1273 (dotfiles swap-gate) — left where it is, per the rule that a fixed mechanism does
not move the rows it already parked.

**The artifact.** The fix arrived as a draft PR (jkrumm/warden#1, branch
`dispatch/a-prior-read-only-investigation-of-this-31918607`) and sat in `needs_human`
for 24h. The control plane's own repo is merge-approval-gated and this is the hand-fix
lane — a saved edit to `scripts/triage.py` *is* the next tick's behaviour — so the
change lands here, direct to `master`, and #1 is closed as superseded. Item 1294
closes with it.

## 116. The self-audit reads the review's own verdict, and counts PRs, not rows (2026-09-30)

The self-audit's `review-always-blocks-<repo>` finding answers one question — is a
repo's step-7 review gate refusing *every* PR — and it answered it off
`dispatches.validation_status='blocked'`. §115 moved a `needs-human` review that
carries findings off that column and onto `needs_human`, so the audit went blind to
exactly the reviews that keep raising code findings: they were the class §115 was
written for. The audit silently rode a column whose meaning changed for a different
reason — no test covered a needs-human review, because every test seeded three
`blocked` rows.

**And it counted rows where it meant items.** The query was `COUNT(*)` over
`tier='implement'` rows, so one PR that took two revisions contributed two. In the live
14-day window `weatherorb` read **23** (11 distinct items, 10 of them revised) and the
card said "all 23 PRs". The number is not cosmetic: the finding fires on `n >= 3`, so
inflated rows lower the bar too.

**The change keys on the review verdict's content and counts distinct items.** For each
implement row in the window, join its review dispatch (`implement.validation_job_id ->
review.job_id`), read the top-level `verdict_json`, and count an item as blocking when
its `blocking[]` holds at least one finding `_is_process_only_finding()` does not call
process-only. `COUNT(DISTINCT origin_event_id)` is both the denominator (items with a
completed review — a `verdict_json` is required, so a review that errored on
infrastructure cannot make the gate look non-blocking) and the numerator. A genuine
"every PR blocked" still fires; `validation_status='needs_human'` from a process-only
finding or an unknown outcome no longer silently counts, and a PR's second revision no
longer counts twice.

**Verified.** Test first: the existing `review-always-blocks-demo-repo` case now seeds
implement+review pairs, and two regression cases were added — a needs-human review
carrying a code finding still fires, and two items (one with two blocked rows) is two,
not three. `make test` green with `test_triage.py` at **354/354** (352 before, +2). On
the live ledger the old query fires only for `research-gateway` (6 rows / 6 blocked)
and the new one still fires there — now honestly, **6 items / 6 code-blocked** — while
`weatherorb` reads 11 / 9, `dotfiles` 4 / 3, `warden` 2 / 1, `homelab` 7 / 4.

## 117. The self-audit reads an item by its LATEST review, not any earlier one (2026-10-01)

§116 keyed `review-always-blocks-<repo>` on the review verdict's own `blocking[]` and counted
distinct items, but its per-item status was still an ACCUMULATION across that item's
revisions: an event entered `code_blocked` on any review of it that carried a code finding and
was never removed, so a PR whose first revision was blocked and whose second was accepted still
read as blocked. The finding fires on `len(code_blocked) == n`, so a repo that accepts its PRs
after one revision round — the healthy shape — fired "the review blocked all N PRs" on items
that were ultimately accepted. It shipped as §116's deliberate judgment call ("any revision
counts", flagged for the reviewer rather than decided); the step-7 review on PR #7's diff
(sideclaw job `5233ae3b`, raised by the adversary angle alone) rejected it as a blocking
correctness bug, and the owner's disposition was to fix it rather than settle it.

**The change: the last completed review wins.** The implement→review join is ordered by the
review's own `created_at`, then its row id (insertion order — the ordering the folded
`validation_status` column never exposed), so each item ends on its LAST review: a blocking one
counts it, a clean one does not. Only a COMPLETED review may do either — a verdict whose
`outcome` is outside sideclaw's published `REVIEW_OUTCOMES` (the set `poll_validation_jobs()`
refuses a verdict outside of) is not a review at all: it neither clears an earlier blocked round
nor joins the denominator, because `_safe_json()` reduces a corrupt payload to `{}` and an
absent `blocking` key must not read as "passed". The `n >= 3` bar and every other self-audit
count are unchanged.

**Second round.** The step-7 review on the first revision of this fix (`b18deaee`, the same
adversary angle) blocked it on exactly that second half: "any non-`NULL` `verdict_json` is
treated as a completed, clean review whenever it lacks a non-process `blocking` entry", so an
errored or partially-serialised verdict would clear a code-blocked item — a false all-clear on
the one check whose job is to notice a broken review gate. The `outcome` gate above is that
finding's fix, and "an item nobody has judged is not counted" is now pinned by its own case.

**Third round.** The review on the second revision (`4f48fc15`) blocked it once more, one step
further in: the `outcome` gate alone still let a parseable but PARTIAL stored payload through —
`{"outcome": "actionable"}` with no `blocking` key — whose `(verdict.get("blocking") or [])`
collapses to `[]` and reads as a clean review, again clearing a code-blocked round. The gate is
now the whole published shape, `_is_completed_review()`: sideclaw's `schemaVersion`, its
published `outcome`, and a `blocking` list — the same three `assert_result_schema()` /
`assert_outcome()` insist on for a live response, applied to the stored payload because that is
what this audit reads. A skipped row is counted and printed
(`self-audit: N stored review verdict(s) in <repo> are not a complete review … skipped, never
read as a pass`), so "no usable verdict" is visible in the loop's log instead of
indistinguishable from "nothing to report". The same round asked for the wrapper-finding filter
that had become triplicated to be extracted: `_code_blocking_findings()` now serves
`poll_validation_jobs()`, `_revision_findings()` and this audit, so §114's rule has one home.

**Verified.** Test first, all three rounds:
`test_self_audit_reads_an_item_by_its_latest_review_not_any_earlier_one` fails on the §116 code
for the stated reason — three items, each blocked on round one and accepted on round two, fire
the finding — and pins the positive direction (three items blocked on their LATEST round still
fire, two of them having come back clean earlier);
`test_self_audit_does_not_let_an_unusable_review_clear_a_blocked_item` fails on this section's
first revision — three items blocked on round one, each then carrying an `{"outcome": "error"}`
review, went silent — and `test_self_audit_does_not_let_a_partial_verdict_clear_a_blocked_item`
fails on its second — the same shape with `{"outcome": "actionable"}`. Both pass with the shape
gate, which also pins that an item whose only review is unusable stays out of the denominator.
`tests/test_triage.py` at **357/357** (354 before, +3); all other suites unchanged. On a
`VACUUM INTO` copy of the live ledger **0** of the window's implement→review rows fail the gate —
it costs nothing on today's data — and the 14-day firing set is unchanged (`research-gateway` 6
items / 6 code-blocked in every reading), while the per-item status moves where it should:
`weatherorb` 10 any-review-blocked → 6 latest-review-blocked, `warden` 1 → 0 — the false
positive is a live hazard, not a theory.

**Landing.** §116 and §117 both ride PR #7 (branch
`dispatch/a-prior-read-only-investigation-of-this-69b1b7bb`): the control plane's own repo is
merge-approval gated, so the owner's Argo Merge click is the outside. Item 1364 — the alert
carrying the review's finding — was closed rather than carried: its investigate verdict was
`implement`/high, but the auto-implement round is cut from the live checkout's `HEAD`, i.e.
`master`, where `code_blocked` does not exist, so the correction was applied on the PR branch by
hand instead of spending an episode on a tree that cannot show the defect.
