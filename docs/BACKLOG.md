# Backlog — improvements pending

Owner-approved items waiting for a structured improvement wave. Each row says what, why, and who
consumes it. A wave that takes one moves it into `docs/waves/PLAN.md` and deletes it here.

## Authored `question` on `needs_decision` items

Approved 2026-10-18. Every item that lands `needs_decision` carries an optional `question`: one
authored sentence ending in `?`, at most 80 characters, naming the decision itself ("Revert the
rb deploy that broke /health?"), not the item title.

- **Why:** argo's Warden page and Home inbox derive the card title today
  (`apps/dashboard/src/lib/question-copy.ts`: the reason's first sentence if it is a question, else
  `{stem}: {subject}?`). Design reviews across argo Waves 26–29 read the derived form as an
  instruction with a question mark; the derivation has hit its ceiling and only the producer knows
  the real question.
- **Consumer:** argo reads it already, `authoredQuestion(source.question)` in
  `apps/dashboard/src/features/warden/page-model.ts`; absent or blank falls back to the derived
  rule, so shipping it is additive.
- **Shape:** write it where the decision is made (triage / lifecycle, the LLM that sets
  `needs_decision` already has the context), expose it on the item in the API (`docs/api.md`), and
  keep the reason as the evidence. Pair with agent-gateway's `question` on `needs_you`
  (its `docs/BACKLOG.md`) so both producers follow one contract.
- **Bigger frame:** the owner wants this folded into a real structure-and-maturity pass on warden,
  not a one-field patch.
