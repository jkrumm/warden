# warden — design

**warden turns a signal into a verified outcome, and it is the only thing in the
estate that holds that state.** Everything else either feeds it (pollers),
executes for it (sideclaw), or renders it (Argo, Slack).

STATUS: design, not built. Extraction from `hermes-agent` is Wave 0.

---

## Why this exists

The system warden replaces reasons correctly and then drops the answer. Measured
over 14 days to 2026-09-09, on the live `watchdog.db`:

| | |
|-|-|
| Distinct signatures entered the loop | 49 |
| `investigate` episodes run | 15 |
| → produced a shipped fix | **1** |
| Items marked `resolved` | 28 |
| → liveness-verified fix | **0** |
| → closed because the signal went quiet for 2h | 26 |
| Items holding a written fix and no way to say yes | 3 |

Nine of the last ten investigate verdicts were read and dropped. The diagnoses
were not wrong — each of the three stuck items carries concrete, correct
instructions in its `note` field, and one of them has been sitting there since
2026-09-07. The defect is that a correct verdict has nowhere to go.

There is a second, structural failure. Ingest (`watchdog-poll.py`) and
verdict-folding (`dispatch-sweep.py`) run as cron jobs **inside the Hermes
gateway process**; the act-loop runs on its own LaunchAgent. When the gateway
crash-looped on 2026-09-07 — seven restarts in three and a half minutes — the
loop kept ticking against a ledger that had stopped receiving signals and a
dispatch table whose verdicts were never folded back. It had no way to notice it
was blind. The item still open about that crash is `uk:229`.

A control plane cannot live inside the thing it supervises. That is the whole
argument for a separate repo, and it is the same argument that already moved the
act-loop out of the gateway's scheduler once.

---

## What "done" means

Five numbers, which warden reports about itself. Not a feeling.

| Metric | Today | Target |
|-|-|-|
| Verdicts reaching a **recorded** decision | 1 / 15 | 15 / 15 — `implement`, `dismissed` with a reason, or `needs_human`. Silence is never an outcome. |
| Closes that are verified fixes vs. silence | 0 / 28 | verified is the majority of closes **for mapped signatures** |
| Median time from `needs_human` to a human decision | no answer path exists | < 4h |
| Verified unattended fixes per week | 0 | ≥ 2, sustained over a month |
| Minutes with no poller running | unmeasured | 0, and alarmed |

The first row is the one that matters. A fix rate is not the target — some
verdicts genuinely conclude "nothing to do here." **Dropping a verdict on the
floor is the defect**, and it is measurable.

---

## Non-goals

- **Not an LLM coordinator.** No supervisor agent, no agent-to-agent messaging.
  The published failure taxonomy for that shape (MAST, 200+ traces across seven
  multi-agent systems) is 41.8% specification failures, 36.9% inter-agent
  misalignment, 21.3% verification failures. Anthropic's own multi-agent writeup
  says it plainly: poor fit when steps share context or have dependencies, and
  coding parallelizes worse than research. This pipeline is sequential and
  dependent. Code coordinates it.
- **Not a workflow engine.** SQLite plus a 10-minute tick. The one thing that
  would justify Temporal or Restate is a durable wait for a human, and a signed
  `approvals` row already is one.
- **Not a chat interface.** Slack and Argo are surfaces over this, never state.
- **Not a product.** Single operator, single tenant, no auth model beyond a
  bearer token and a signed decision.

---

## Principles

1. **One ledger.** `warden.db` is the source of truth. Slack cards and Argo
   pages are projections, re-rendered from rows. A button click is not the
   event — it writes to the ledger and the ledger re-renders the surface.
2. **Deterministic control plane; LLMs only inside bounded steps.** No LLM call
   decides a state transition. Episodes return typed output; warden validates it
   against a schema and a policy before acting on it.
3. **Every surface is optional; the ledger and the loop are not.** If Argo is
   down, Slack works. If Slack is down, the loop runs. If the Hermes gateway is
   down, ingest keeps running — that is the bug this design fixes.
4. **Policy names a key, code decides the argv.** `triage-policy.json` and
   `deploy-targets.json` are machine-writable (warden's own mapping proposer
   commits to the first one). A policy edit must never be able to express a
   command.
5. **Honest states.** A signal going quiet is not a fix. `quiet` and `fixed` are
   different terminal states and the difference is visible everywhere.
6. **No step trusts a previous step's memory.** Every stage re-derives its own
   eligibility from the ledger on every pass. Restarting warden mid-chain loses
   nothing.
7. **Ask a human only where a human is essential.** Defined below — this one has
   a test, because "ask when unsure" is how a system becomes friction.

### When a human is essential

Exactly three cases. Everything else runs unattended.

- **Irreversible and unscoped.** The action falls outside a declared
  `autoMergePaths` / `deploy-targets` scope, or has no tested compensation.
- **Only a human can supply it.** Biometric `op`, a tailnet ACL push, a
  console-only action, a decision about intent rather than fact.
- **The evidence is genuinely ambiguous and the wrong branch is expensive.**
  Two plausible root causes with materially different fixes.

**Approval fatigue is a failure mode, not a safety feature.** An approval the
operator grants every time is a bug: either widen the scope so it runs
unattended, or delete the gate. Warden reports its own approve/deny ratio per
category for exactly this reason — a category running at 100% approve gets
promoted to automatic.

---

## The model

Four nouns. Everything in the schema is one of them.

| Noun | What | Owner |
|-|-|-|
| **Signal** | One deduplicated observation from a source. Append-only. | pollers |
| **Item** | One unit of work, 1:1 with a signal, carrying a lifecycle. | the loop |
| **Episode** | One bounded sideclaw run against one repo. Disposable. | sideclaw |
| **Decision** | One signed human answer. Durable, single-use. | the decision API |

An item's **origin** is what generalizes this beyond alerts:

| Origin | Source | Notes |
|-|-|-|
| `alert` | Slack, UptimeKuma, docker, hermes logs, op refs | today's only origin |
| `github_issue` | `gh` poll | the operator's own handover mechanism — currently ingested and then ignored |
| `github_pr` | `gh` poll | review needed |
| `human` | Slack or Argo | file work directly |
| `agent` | an episode | follow-up work an episode discovered |

Same ledger, same lifecycle, same surfaces for all five.

### Lifecycle

```
new -> investigating -> verdict -+-> implementing -> validating -+-> merged
                                 |                               |
                                 +-> needs_human                 +-> merge_blocked
                                 +-> dismissed
                                                merged -> deploying -> verifying -+-> fixed
                                                                                  +-> new (reopened)
terminal: fixed | quiet | closed | dismissed | ignored | note
```

Two changes from the current machine, both about honesty:

- **`resolved` splits.** `fixed` means a deploy landed and a positive signal
  confirmed it. `quiet` means the signal stopped and nothing shipped. `closed`
  means a human said done. Today all three render as `resolved`, which is why
  the board reads 28 successes and means 26 silences.
- **`dismissed` is new.** A verdict that concludes "nothing to do" must record
  that, with the reason, as a terminal state. Right now it has nowhere to go,
  which is precisely how nine of ten verdicts vanished.

Every non-terminal state also goes to `quiet` when its underlying signal
resolves — that already falls out of the machine and should stay that way.

---

## Boundaries

| System | Responsibility | Holds state? | Failure domain |
|-|-|-|-|
| **warden** | ingest, dedupe, decide, drive the lifecycle, ask a human | **yes — the ledger** | mini, own LaunchAgents |
| **sideclaw** | execute one bounded Claude Code episode | job store only, disposable | mini, own daemon |
| **hermes** | conversational agent; Slack surface; also a signal source | its own, unrelated | mini, gateway process |
| **Argo** | operator console: board, timeline, approvals, system state, cost | display cache only | **VPS — separate domain** |

The two rules that fall out and must not be broken:

- **warden never runs inside sideclaw.** The loop decides whether the actuator
  worked; put them in one process and the thing that would notice sideclaw
  wedged is inside sideclaw.
- **warden never runs inside hermes.** Same argument, already learned once the
  hard way, and still half-violated today by the two gateway cron jobs.

Argo living on the VPS is a feature, not an accident: the console that tells you
the mini is broken should not need the mini. Slack buttons need the gateway's
Socket Mode and will degrade when it doesn't; that is the honest ranking. Slack
is the convenience path, Argo is the one that works when it matters.

---

## Contracts

### HTTP API (mini, bearer-authenticated, tailnet-only)

Argo is the primary client. Everything is derived from the ledger; nothing here
holds state of its own.

| Method | Path | Purpose |
|-|-|-|
| `GET` | `/board` | every item, current state, one row each — the kanban |
| `GET` | `/items/:id` | full timeline: signal, brief, verdict, PR, validation, deploy, verification, with artifact URLs |
| `GET` | `/health` | heartbeat: last completed pass, state census, poller ages |
| `GET` | `/metrics` | the five "done" numbers above, computed |
| `POST` | `/items/:id/decide` | `{action, actor, note}` → the decision primitive |
| `POST` | `/items` | file work by hand (`human` origin) |

Argo caches `GET` responses in its own Postgres purely so the page renders when
the mini is unreachable, stamped with the fetch time. That is an HTTP cache, not
a mirror: Argo never writes item state, and there is no sync protocol to get
wrong.

`POST /items/:id/decide` is never cached, never queued, and fails loudly when
the mini is unreachable. An approval is a transaction.

### The decision primitive

Today the cryptographic gate is already surface-agnostic —
`require_signed_approval()` trusts only the row's Ed25519 signature and never the
caller. What is missing is a public entrypoint: the only wired orchestration is a
Slack Bolt handler with the approver allowlist inlined in it.

Extract exactly one function, and make every surface a client of it:

```
decide(item_id, action, actor, note) -> Decision
  action ∈ { approve, deny, snooze, dismiss, escalate }
```

It records the decision, signs it, and lets the existing verify-and-spend path
run unchanged. Slack buttons call it. Argo calls it. A CLI calls it. Build it
once here, or build it twice later.

### `deploy-targets.json`

The current `deploy_argv()` is a bash `case` with one arm, so adding a repo means
editing shell. That friction is real and the fix does not weaken anything:

```json
{ "vps":     { "host": "vps",     "dir": "~/vps",     "target": "hyperdx-apply", "env": "ENV=prod" },
  "homelab": { "host": "homelab", "dir": "~/homelab", "target": "deploy" } }
```

argv is always `ssh <host> "cd <dir> && make <target> <env>"`. The only free
variable is a **Makefile target name**, itself declared in the target repo. N
repos, zero code edits, and `rm -rf` stays inexpressible.

The mini already has keyless Tailscale SSH to `homelab` and `vps`, so reach is
not the missing piece — the allowlist is.

### Autonomy tiers

Per repo, in `triage-policy.json`. Deliberately conservative and deliberately
promotable.

| Tier | Warden may | Gate |
|-|-|-|
| 0 | investigate, report | none |
| 1 | + open a draft PR | path scope |
| 2 | + merge | path scope, validation by a different model, CI green or `noCiRequired` |
| 3 | + deploy | tier 2 plus a declared `deploy-targets` entry |
| 4 | + close on verified signal | tier 3 plus a declared `liveness` predicate |

A repo with no entry is tier 0. There is no implicit allow. Promotion is a
policy edit, made after watching the tier below run clean — and warden's own
approve/deny ratio is the evidence for when to promote.

### Repos that should not need a deploy tier at all

`meteo`, `research-gateway`, `image-share` and `argo` already deploy through
GitHub Actions → RollHook. For those, **merge is deploy** and warden needs no
shell reach whatsoever. Prefer that. The deploy tier exists for what genuinely
cannot have CI — `vps/observability/`, `homelab`, `homelab-private` — not as the
default answer.

---

## Observability

Warden's own health is a first-class output, because the failure this design
exists to fix was invisible for eleven days.

- **Heartbeat.** One row per completed pass, written unconditionally, carrying
  the state census and open-cluster count. A stalled loop is a stale timestamp;
  an idle loop is a fresh timestamp with an unchanged census. (Shipped already,
  in the pre-extraction code.)
- **Poller age.** Per source, the age of the last successful poll. The
  gateway-crash failure mode becomes an alarm instead of a silence.
- **Funnel metrics.** The five numbers from "What done means", computed on
  demand at `/metrics` and rendered in Argo. Weekly delta in the digest.
- **Cost per item.** Episodes carry token cost; attribute it to the item so
  "what did this fix cost" is answerable. Note the current baseline: the entire
  real-money LLM spend across the estate is ~$82 / 14 days, and the agentic path
  runs on the flat Max subscription. **Cost is not a constraint on this design**
  — route the agentic path for quality.
- **Uptime Kuma push.** Warden joins the existing composite heartbeat rather
  than inventing a monitoring stack.

---

## Migration

| Wave | What | Why this order |
|-|-|-|
| **0** | Extract to `warden`. `git mv` with history: the three scripts, `hermes-cc.sh`, both config files, tests, docs, the DB, the LaunchAgent. Promote the two gateway cron jobs to LaunchAgents. | The seams are already cut. Doing this first means every later wave lands in a clean repo instead of being grafted onto a fork overlay and moved afterwards. |
| **1** | Decision API + `decide()`; Slack buttons repointed at it. | The bottleneck. Everything downstream is worth less until a verdict can be answered. |
| **2** | Honest states (`fixed` / `quiet` / `closed` / `dismissed`); `/metrics`. | Makes wave 1's effect measurable. Cheap, and it must land before more volume arrives. |
| **3** | New origins: `github_issue`, `github_pr`, `human`. | The operator's handover mechanism starts working. Gated per origin. |
| **4** | `deploy-targets.json`; CI-where-possible; `verify` tier. | The last mile, for the repos that genuinely cannot have CI. |
| **5** | Argo operator console. | Needs waves 1–3 to have something worth rendering. |
| **6** | `review` tier for any PR, off CodeRabbit's quota. | Independent; last because it is additive, not blocking. |

Wave 0 is roughly a day — the code is already file-separated, the DB is already
its own file, the LaunchAgent already exists, and the tests already run standalone.

---

## Open questions

1. **Does warden keep Python?** The extraction is trivial in Python and a rewrite
   is not on the critical path. But sideclaw and Argo are Bun/TS, and the HTTP
   API is the one new surface. Recommendation: keep Python for the loop, and
   write the API in Python too rather than splitting the repo across runtimes for
   aesthetics.
2. **Slack interactivity after extraction.** Buttons need Socket Mode, which is
   the gateway. Either the gateway plugin becomes a thin client of `/decide`
   (simple, but buttons die with the gateway), or warden runs its own minimal
   Slack app (independent, more moving parts). Leaning thin client, because Argo
   is the surface that must survive.
3. **Three unmapped UptimeKuma group monitors** — `uk:95` ("VPS"), `uk:179`
   ("Services"), `uk:186` ("Local"). Needs the operator to say which repo owns
   each; guessing routes real alerts into the wrong repo permanently.
4. **`stray_skill` 849** — an agent-created skill with 86 patches living outside
   `skills.external_dirs`, flagged eight days, never triaged. Is `stray_skill` a
   warden origin or a hermes concern?
5. **Cluster feedback.** Episodes are returning "re-triage these two together as
   one" — a shape the loop cannot parse. Today it only understands
   `UNRELATED SIGNATURES`. Worth an inverse marker, or is that over-fitting?
