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

warden is a deterministic control plane over one SQLite ledger. **No LLM call
decides a state transition** — the dispatched episodes each run one, the loop
never does. Pollers feed it, sideclaw executes for it, Slack and Argo render it.

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
| `com.jkrumm.warden-restore-drill` | `scripts/warden-restore.sh` (restore the newest off-box snapshot into a temp dir, verify, clean up) | monthly, 1st 04:10 |
| `com.jkrumm.warden-api` | `scripts/api.py --serve` (GET /metrics, /health) | long-running, `KeepAlive` |

`warden-api` is the odd shape: a long-running server, not a periodic job — see
its own plist template for why that changes `KeepAlive`/`StartInterval` and adds
a `ThrottleInterval`. It binds `127.0.0.1:7735` only, loopback and no auth — see
`docs/api.md` for the endpoints, the six funnel numbers' exact definitions, the
honesty rules (`null` + reason, never a fabricated `0`), and what is deliberately
not built yet.

Slack delivery from the loop is a **plain HTTP client** (`chat.postMessage` /
`chat.update` with a token from `resolve_slack_token()`), never the gateway's live
`slack_bolt` connection. That is what makes a gateway-independent agent safe, and
it is not an implementation detail — it is the property. It posts under warden's
own Slack app identity, falling back to Hermes's token until that app is seeded —
see `slack/README.md` for creating and seeding it.

## Running it

```bash
make setup     # venv + plists + load the agents
make test      # every tests/*.py
make status    # what is loaded, what ran last, is the ledger reachable
make restore-drill  # restore the newest off-box snapshot into a temp dir, verify, clean up
make unload    # stop the agents
```

**Never start a second loop.** Two loops against one ledger double every card and
every dispatch. Before loading an agent, check nothing else is already running the
same script — during the extraction that means `com.jkrumm.warden-loop` in
particular.

## Python

Pinned to **3.11.15** via `python3.11` (uv-managed). The box's default `python3` is
3.14 — do not use it. `make venv` fails loudly rather than falling back, because
the extracted loop was written and proven against 3.11 and an interpreter change
is its own verifiable step.

**The loop is pure stdlib.** `requirements.txt` has one entry, `cryptography`, for
the Ed25519 verifier. Keep it that way: a dependency here is a dependency in the
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

`tests/test_triage.py` is the regression gate at **352/352**. Any other number is a
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
truth; Slack cards and Argo pages are projections.

- **One migrator.** `scripts/ledger.py` owns the schema and `schema_version`;
  only the loop migrates, at boot. Everything else asserts the version and
  refuses on mismatch. Four processes racing unversioned `CREATE TABLE` /
  `ALTER TABLE` is what this replaced.
- **Read-only means read-only**: `sqlite3.connect(f"file:{p}?mode=ro", uri=True)`.
- Backup is `VACUUM INTO` (never a bare `cp` or `rsync` of an open database —
  that captures the main file and its `-wal` at different instants and restores
  as either stale or corrupt with nothing saying which), shipped to
  `homelab:/mnt/hdd/backups/warden/`, which the existing restic container already
  walks on its way to B2. The restore is **drilled, not assumed**:
  `make restore-drill` (and monthly, `com.jkrumm.warden-restore-drill`) restores
  the newest off-box snapshot into a temp dir and verifies it; it can never write
  `~/.warden/warden.db`. A failed drill becomes a `warden_self` item (§104).

## Talking to sideclaw

Warden depends on exactly two things: `submit(tier, repo, brief, model) -> jobId`
and `get(jobId) -> {status, result}`. Everything else about sideclaw is its own.

- **The verdict schema is published by sideclaw**, not copied here. Copying it
  guarantees drift, and drift presents as "verdict silently ignored" — the exact
  failure warden exists to fix. A version mismatch is a loud refusal, never a
  best-effort parse.
- **A dispatch names a repo, never a path.** That is what keeps a composed path
  out of the interface.
- **The repo allowlist is enforced inside sideclaw**
  (`sideclaw/server/lib/dispatch-policy.ts`, `GET /api/dispatch-policy`). Warden's
  copy is defence in depth; sideclaw's is the boundary. If the two disagree, the
  boundary is quietly allowing something the control plane forbids — they must be
  checked against each other, not assumed to agree.
- **Warden no longer picks the worker model.** `triage.py`'s `AUTO_DISPATCH_MODEL` /
  `AUTO_IMPLEMENT_MODEL` default to `None` — no `model` key is sent, and sideclaw
  routes each tier per its own table (`server/lib/routing.ts`, live at
  `GET /api/routing`): investigate/author to DeepSeek-V4-Flash, implement to
  DeepSeek-V4-Pro. The env vars (`TRIAGE_AUTO_DISPATCH_MODEL`,
  `TRIAGE_AUTO_IMPLEMENT_MODEL`) are the operator's escape hatch, and
  `make check-routing` verifies any such override against the live table the same
  way `check-schema-versions.py`/`check-dispatch-policy.py` do for the verdict
  schema and the allowlist.
- **Warden may never auto-merge on `sideclaw`, `warden` or `dotfiles`.**
  Implement is allowed (a draft PR), but neither repo ever gets
  `autoMergePaths`, and all three are now also in
  `config/dispatch-repos.json`'s `merge_approval` — a clean step-7 validation
  routes the item to `needs_human` carrying the PR URL instead of calling
  `merge`, and the owner lands it with `warden merge <job> --confirm`.
  Auto-merging PRs against your own executor closes a loop that has no
  outside. The owner's Argo Merge click is that outside.
- **An episode is not contained.** `readOnly` is three tool names on a CLI flag
  under `--dangerously-skip-permissions`; `Bash` is unrestricted and the brief is
  attacker-influenceable (public issues, alert text, log lines all reach it). A
  bearer token on this host is not an authorization boundary against an episode.

## Things that are load-bearing and look like they are not

- **A signal going quiet may cancel the need to *start* work. It may never
  discharge a verdict, a pending approval, or an in-flight operation.**
  Silence-resolve applies to `new` and to nothing else.
- **Overflow waits, never drops.** Cluster members past the cap stay `new`.
- **The dry-run contract**: never touches Slack, never shells out, everything else
  real. With no staging environment it is the only pre-production surface there is.
- **A policy file may name and parameterise, never express.** Config carries
  validated values; code owns the argv array. The five closed allowlists are one
  principle in five instances (a fifth, `HOST_VERB_ALLOWLIST`, added 2026-09-11
  for the owner's host-restart decision — DESIGN.md § The host-verb carve-out).
- **Deferral must be visible.** A budget hit that only reaches a `.err` file is
  indistinguishable from a broken loop.
- **A dispatch that ends terminal with no verdict is not a verdict.** It folds
  to `needs_human` carrying `dispatches.error`, never into `verdict` — a
  verdict-less `verdict` row is the same invisibility in a different column
  (§64). It is never retried automatically.
- **An action pulled from Argo's queue is the owner, full stop** (owner
  decision, 2026-09-15 — DESIGN.md § *2026-09-15 override*, REVIEW.md's C1
  disposition update, §71). `apply_argo_actions()` passes
  `authorized_by="owner:argo"` into the same plain truthy-string gate a
  signed Slack approval satisfies — no signing key touches Argo, and
  `require_signed_approval()`/`execute_approved()` are unchanged and remain
  the only path for anything reachable off the tailnet. This is not a second
  signing oracle; it is because Argo is reachable only over his own tailnet.

## Cross-repo facts

`hermes-ops.sh`, `agents-overview.py`, `plugins/dispatch-approval/` and a
six-line dispatch-shim script (which `exec`s into this repo's `scripts/warden`)
live in `hermes-agent` and are reached, not vendored.
`~/.hermes/{scripts,config}` are **whole-directory symlinks** into that repo, which
is why the extraction was never a `git mv`. The Ed25519 signing key exists only in
the gateway's RAM, is minted fresh at every start, and is never serialized — only
a Slack interaction payload can cause a signature to exist. Prompt injection
reaching a brief produces words, and words cannot mint a signature.

## Git

Direct-to-master. `/commit` per logical concern, no attribution footers.
Append a § to `docs/history/state-log.md` and rewrite `STATE.md` in the same
commit as the work it describes — it is the memory, and the next session is a
stranger.
