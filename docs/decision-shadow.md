# Decision-model shadow lane

An IU decision model (typed answers with calibrated confidence, no text, no search) answers the
same triage questions as the LLM `triage` job and the fixed-by sweep, in **shadow**: both answers
land in `decision_shadow` (ledger v21) and nothing reads the table to decide anything. Code:
`scripts/lifecycle/shadow.py`; agent-gateway side: its AGENTS.md §Decide.

## What the model sees

Only the `state` text `shadow.build_triage` builds: the incoming event, each candidate repo's
one-line purpose (300 chars), every same-repo open item (title, summary, root cause, state, PR,
last activity), and items fixed in the last 14 days with PR titles. Budget 24 000 chars
(`WARDEN_SHADOW_BUDGET_CHARS`); the oldest fixed items are cut first, then the oldest open items,
and the cut counts are stored in `context_json`. agent-gateway adds its own per-model hard cap and
reports `truncatedChars`. A `-private` repo never reaches the model (`intake.may_reach_model`).

Questions: `action` (attach | fixed_by | new | ignore), `attach_target` (a choice over the shown
open item ids + `none`), `fixed_by_target` (over the shown fixed ids + `none`). Sweep: one boolean
per waiting item (cap 16).

## Measured: replay of labelled ledger history (2026-10-15)

`scripts/shadow-replay.py` on a copy of the ledger. Ground truth: `closed(duplicate)` → attach
(target = `duplicate_of`), `closed(fixed_by)`, `closed(ignored)`, and a sample of 17 items that went on to
be worked → new. 33 labelled items (2 skipped as private). Each item is rebuilt as it stood at its
creation, limited to its own repo's candidates (favourable to the model). Event text is the
current payload.

| model | n | action agreement | attach target | cost / 31 calls | avg latency |
|-|-|-|-|-|-|
| pplx-decider-v1-27b | 31 | 61% | 1/7 | $0.0051 | 0.6 s |
| decision-1-eu | 31 | 52% | 0/7 | $0.0023 | 0.3 s |
| decision-1 (first run, 28) | 28 | 57% | 0/7 | $0.0022 | 0.6 s |
| clef-eu (first run, 28, 5k-char budget) | 28 | 64% | 1/7 | free | 0.3 s |

`pplx-decider-v1-27b` confusion (truth → decision): new→new 16, new→ignore 1; attach→attach 3,
attach→new 3, attach→ignore 1; **ignore→new 6 of 8**; fixed_by→new 1. Agreement at confidence ≥ 0.95
is 11/16 (69%), so confidence is not yet calibrated against this ledger's labels.

Read it as: the model is good at "this is a real new defect" and weak at the two things triage
exists for — recognising a duplicate (target 1/7) and noise (0/8 ignored). The sample is small and
biased (labels are the LLM's and the owner's past calls, not an independent truth), so this is a
direction, not a verdict. The default stays `pplx-decider-v1-27b` (best action agreement, still
under $0.001 a call).

## Wave 11: context iteration on a faithful 101-item set (2026-10-15)

**The Wave 6 set was skewed.** It required `repo IS NOT NULL`, which dropped 26 of the 29 alerts closed
as noise (they are repo-less), kept the human-aborted tasks as "ignore" truth, and offered only the
item's true repo as candidates. The new default set (`scripts/shadow-replay.py`, `--legacy` keeps the old
one) uses the live candidate rule (label route, else every known repo), drops human-origin ignores and
takes `new` truth from alert/issue items that were worked to a fix: 101 items = 60 new, 28 ignore, 3
ignore* (closed by the policy ignore list before triage, so never seen live), 10 attach, 2 fixed_by.
A truth that is only 10 attaches and 2 fixed_by is far too thin to tune on; read the attach and fixed_by
columns as anecdotes.

**Signals added** (`shadow._signals`, `build_triage(signals=True)`), all as-of-safe (an outcome counts
only when it was known at the item's creation) and never from a `-private` repo: the same alert
signature's earlier items with their outcome and close note; how items from the source ended in the last
90 days; the last six alerts of the source that were ignored as noise. The action options now carry the
LLM triage's own rules (a recovery notice is noise, a topical match is not fixed_by, "prefer attach"),
and target options carry the root cause.

| model | before action | after action | attach target before / after | acc. at conf ≥ 0.95 before / after | cost / 101 |
|-|-|-|-|-|-|
| pplx-decider-v1-27b | 76/101 = 75% | 78/101 = 77% | 5/10 / 5/10 | 47/51 = 92% / 53/61 = 87% | $0.055 |
| decision-1-eu | 70/101 = 69% | 70/101 = 69% | 2/10 / 2/10 | 52/69 = 75% / 46/65 = 71% | $0.023 |
| decision-1 | 72/101 = 71% | 71/101 = 70% | 2/10 / 2/10 | 52/69 = 75% / 46/63 = 73% | $0.023 |
| clef-eu (5k-char budget) | 83/101 = 82% | 83/101 = 82% | 0/10 / 1/10 | 22/23 / 26/26 | free |

**Reading.** The signals did not move the lane: +2 points for the default model, nothing for the
others, and the confidence ≥ 0.95 slice got no more reliable (87% right, the promotion rule needs
≥ 95%). The 23 misses of `pplx-decider-v1-27b` split into (a) 10 repeated `hermes_log` WARNING lines
(`tools.checkpoint_manager`, `agent.tool_executor`) labelled `new` because each was worked, which the
model reads as duplicates of each other, a defensible call and a label artifact; (b) 6 noise alerts
(watchdog pushes, a MAM session notice) the model calls `new` at confidence ≥ 0.95; (c) attach and
fixed_by items it calls `new`. Fix (b) belongs in the policy ignore list, not in the model. clef-eu's
82% is mostly "everything is new" (60/60 new, 1/10 attach). **Verdict: lane stays shadow; no promotion
by hand.** The live rows (`shadow.py report`) remain the only evidence the promotion rule reads; this
replay just bounds what context can buy: little, on this ledger.

## Promotion rule

The decision model decides nothing until ALL hold, measured by `python3 scripts/lifecycle/shadow.py
report` on **live shadow rows** (not the replay):

1. at least 50 settled triage rows with both answers;
2. on rows where the decision confidence is ≥ 0.95: action agreement with the LLM ≥ 95% and, where
   both chose attach or fixed_by, target agreement ≥ 90%;
3. no disagreement on that slice where the LLM said `new` and the decision model said `ignore`
   (an ignored real defect is the costly error).

Then it decides **only** above confidence 0.95 and only for `attach`/`new`; everything below, every
`ignore` and every `fixed_by` stays with the LLM triage. The rule is re-checked by the
`warden-improve` loop (Wave 8); today's replay fails condition 2, so the lane stays shadow.

## Operating notes

- The agw job queue has a global concurrency cap (3). A decide job queues behind 10-minute reviews,
  so a shadow row can lag its triage by minutes; `settle` simply waits (rows older than 24 h fail).
- `python3 scripts/lifecycle/shadow.py report` prints agreement by confidence bucket, the sweep
  totals, summed cost and rows by status.
- `settle` prunes settled rows (`recorded` / `failed`) whose `settled_at` is older than
  `SETTLED_RETENTION_DAYS` (90). `submitted` rows are never pruned. Rows older than 90 days
  therefore drop out of `report` and out of the promotion rule's sample.
