# warden — flows

Six real scenarios end to end. For each: what triggers it, what warden does, what
the operator sees in Slack and Argo, where a decision is genuinely needed, and
where the friction is.

**The default is no approval.** A gate appears only under one of the four
human-essential cases in `DESIGN.md`. If a flow below asks for a decision, the
case number is named. If it doesn't, that is deliberate.

Legend: **W** warden · **S** sideclaw episode · **You** the operator.

---

## 1. HyperDX alert → threshold fix → deploy → verify

The path that has worked once (`vps#8`), and the one carrying the sharpest trap.

| | |
|-|-|
| **Trigger** | HyperDX posts to `#alerts`; `watchdog-poll` dedupes into a signal |
| **Gate to start** | 3 occurrences **or** 30 min open — not the first firing |

```
W  cluster by repo -> one investigate episode (vps)
S  reads observability/alerts/*.json, returns typed verdict
   nextAction=implement, confidence=high
W  auto-implement (no human: case 1 fails - scoped; case 4 FIRES - see below)
S  opens draft PR
W  validate: a sideclaw review job, a typed verdict from a second session (model per sideclaw's routing)
W  merge if every changed path is inside observability/**
W  deploy: ssh vps -- make hyperdx-apply ENV=prod
W  verify: re-read live thresholds; must match the merged diff
W  -> fixed
```

**Slack** — one card, updated in place, never a new message per stage. Stages
appear as a checklist; the PR link lands when it exists.
**Argo** — the item's timeline with the actual brief, the diff, the validation
verdict, deploy stdout, and the verification read.

**Decision needed: yes, case 4.** This is monitoring config — a change to the
system's ability to observe itself. The trap: raising a threshold to silence an
alert *is* the fix warden will propose, it will verify the threshold is live, and
the alert will never fire again. So:

- monitoring config never exceeds tier 2 (merge, no auto-deploy)
- you get **one** approval, on the merge, with the diff and the old→new threshold
  shown inline
- `fixed` requires a synthetic trip proving the alert can still fire

**Friction:** one approval per monitoring change. Accepted deliberately — it is
the only category where a successful-looking outcome hides the failure.
**Everything non-monitoring in `vps` runs to `fixed` with no approval at all.**

---

## 2. Container unhealthy on homelab → no CI, no deploy target

| | |
|-|-|
| **Trigger** | `docker_homelab` poller sees `unhealthy:garmin-*` |
| **Mapped to** | `homelab` |

```
W  investigate episode (homelab)
S  verdict: nextAction=implement, confidence=high
W  implement -> draft PR -> validate -> merge
W  deploy? homelab has `make deploy` but no deploy-targets entry -> tier 2
W  -> merged, then closed after 1h with no deploy
```

**What you see:** the card says *merged, not deployed — `homelab` has no deploy
target*. Argo shows a one-click **Add deploy target** that pre-fills
`{host: homelab, dir: ~/homelab, target: deploy}` for you to confirm.

**Decision needed: no** for the code change. **Yes, once, ever** to add the deploy
target — case 1, because the first deploy into an environment is the unscoped
one. After that the repo runs at tier 3 unattended.

**Friction, named:** today this is where work dies. `homelab`, `homelab-private`,
`image-share` and `vps/observability` are the four repos with no CI, and they are
exactly the ones alerts fire about most. Until each has a deploy target this flow
stops at `merged` — visibly, on the board, not silently.

---

## 3. You file a GitHub issue as a handover

The origin you asked for, and the one with the most different completion
semantics.

| | |
|-|-|
| **Trigger** | you `gh issue create`, or Hermes files one via `capture` |
| **Gate to start** | label `warden:go` — **not** every open issue |

```
W  loop tick polls `warden:go` issues (ingest_github_go) -> item, origin=github_issue
W  investigate episode against the named repo
S  verdict + a plan comment posted back on the issue
W  implement -> PR that closes the issue -> validate -> merge
W  -> fixed on CI green (these repos have CI) or closed
```

**Slack** — a card, same as an alert. **Argo** — same timeline, plus the issue
body as the brief's source.

**Decision needed: no**, if the repo is at tier ≥ 2 and the issue is yours. The
label *is* the approval — you already decided when you typed it.

**Third-party issues are different.** Every repo is public, so anyone can open
one. Those get `investigate` only, verdict scanned, never auto-implemented,
regardless of label. That rule exists today and stays.

**Friction:** one label. Deliberate — without it every stale idea in your issue
tracker becomes an episode. `warden:go` is you saying "this is ready", which is a
thing only you know.

---

## 4. A PR opens → review without CodeRabbit's quota

| | |
|-|-|
| **Trigger** | `github_pr` poller sees a PR opened by you |

```
W  review episode (the existing validate primitive, generalized)
S  reads the diff, returns typed findings
W  posts one review comment, updated in place on new commits
```

**Decision needed: no.** A review is a comment; it changes nothing.

This runs on Max, so it has no quota and no monthly cliff. CodeRabbit stays as a
second opinion when it has budget. **Friction: none** — and it removes the one
you have now, which is CodeRabbit going quiet mid-month.

---

## 5. Hermes itself is broken

The recursive case, and the reason warden is a separate repo.

| | |
|-|-|
| **Trigger** | `hermes_log` signals, or the gateway heartbeat going stale |

```
W  (own LaunchAgent, own pollers - unaffected by the gateway being down)
W  investigate episode against hermes-agent
S  verdict: patch corruption in chat_completions.py, concrete fix
W  hermes-agent is NOT auto-mergeable -> needs_human
```

**Slack** — degraded. Socket Mode is the gateway, so buttons don't render. The
card still posts: warden's Slack client is a plain HTTP call, never the gateway's
live app.
**Argo** — full function. It is on the VPS. **This is the flow that justifies
Argo existing.**

**Decision needed: yes, case 2** — reviving a wedged gateway needs a human at a
machine.

**Corrected 2026-09-09 (Wave 1).** This paragraph used to say *"You approve in
Argo, or at a TTY."* Both halves were false. Argo **records intents and cannot
approve** — this document's own surface table says so two sections down. And the
TTY path was unimplementable: the signing key is RAM-only in the gateway, so with
the gateway stopped no signature can exist, and this estate cannot hold a
human-only secret to build a second signer with (`dotfiles-private/headless.refs`
refuses `op://Private/*` unconditionally, and the mini's cache resolves headless
— an episode reads whatever an operator stores). See DESIGN.md § The decision
primitive for the full withdrawal.

**What you actually do:** restart the gateway —
`launchctl kickstart -k gui/$UID/ai.hermes.gateway`, ~8s measured, the plugin
republishes its public key on boot — then approve in Slack as normal. You are
already at a machine in this flow; that is what "case 2" means.

**Friction, named honestly:** if the gateway cannot be restarted at all, no
approval can be minted and **warden fails closed** — it keeps triaging and
carding, and nothing merges. That is the right way to be broken, and it is why
this is a degradation rather than an outage.

---

## 6. A storm — 40 correlated alerts

| | |
|-|-|
| **Trigger** | a VPS outage lights up every dependent monitor |

```
W  dedupe -> cluster by repo -> at most ONE episode per repo per pass
W  MAX_OPEN_INVESTIGATIONS=3, DAILY_INVESTIGATE_BUDGET=8
W  overflow stays `new` and waits - never dropped
W  MAX_IMPLEMENT_PER_DAY=5, MAX_MERGES_PER_DAY=3
```

**What you see:** the board shows *3 investigating, 12 deferred (budget)* as a
first-class state, not a line in a log file. This is the fix for the one real
friction budgets have today: they defer silently, in a `.err` file nobody reads.

**Decision needed: no.** But **`warden pause`** exists — one command, or one
button, that stops all escalation while leaving ingest running. During a real
outage you want the ledger recording and the robot still.

**Friction:** a storm delays low-priority work by design. Acceptable; the
alternative is 40 merges.

---

## What the surfaces are for

| | Slack | Argo |
|-|-|-|
| **Role** | trigger, notification, one-tap decision | the console |
| **Shows** | one card per problem, updated in place | board, item timeline, approvals, system state, cost |
| **Survives a mini outage** | no (Socket Mode) | **yes** (VPS) |
| **Can approve** | yes — Slack payloads mint signatures | no — records intent; confirmation in Slack |
| **Steering** | reply to the card thread | comment on the item |

**Steering is not the same as approving**, and both are needed. Approving answers
a yes/no warden asked. Steering is you saying *"don't fix the threshold, fix the
probe"* — free text that becomes an additional constraint on the next episode's
brief. Warden has no primitive for it today. Add it as `POST /items/:id/note`,
appended to the brief on re-dispatch, with no authority attached — a note cannot
approve anything, so it needs no signature.

---

## Observability, per flow

Every item emits one trace; every stage is a span. HyperDX already ingests OTel,
so this is a client, not a stack.

| Signal | Where | Answers |
|-|-|-|
| Trace, one per item | HyperDX | why did this take four hours |
| Span per stage | HyperDX | which stage is slow — episode, validation, or waiting on me |
| Episode transcript | linked from the item | what did the agent actually do |
| Token cost | on the item | what did this fix cost |
| Poller age | `/health`, Kuma | am I blind right now |
| Funnel counts | `/metrics`, Argo | is any of this working |
| Heartbeat | `cursors`, Kuma | did the loop run, or just find nothing |

The distinction that matters: **duration split by stage.** "Median time to fix"
is useless if it hides that 90% of it was waiting for a human — which is exactly
the number this whole design exists to move.

---

## Where friction genuinely remains

Four places, all deliberate, none accidental:

1. **One approval per monitoring-config change** (flow 1, case 4). The only
   category where success and failure look identical.
2. **One approval, once per repo ever**, to add a deploy target (flow 2, case 1).
3. **One label** to hand a GitHub issue over (flow 3). Without it, your backlog
   becomes a work queue.
4. **A terminal**, if both Slack and Argo are down (flow 5).

Everything else — alert to verified fix, PR review, storm handling, dismissal of
a verdict that concludes nothing-to-do — runs with no human in it at all.
