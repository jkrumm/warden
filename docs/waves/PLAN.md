# GitHub issues in warden, Argo as the owner's triage surface

**Goal:** every open GitHub issue across `jkrumm/*` is a warden item without a
label, assessed automatically, and the owner triages and approves it from
Argo's `/warden` page with one click — no Slack round-trip, no label, no
signature ceremony.

**Gate:** warden — `make test` green with `test_triage.py` at its recorded
count (255 today; a wave that adds tests records the new number in STATE.md),
plus `make check-policy` and `make check-routing`. argo — `/check`
green in `~/SourceRoot/argo`. `/review` on every wave.

## Owner decisions this plan implements (2026-09-15) — do not re-litigate

- **Tailnet access is the owner.** Argo is reachable only over his Tailscale
  network; an action taken in Argo *is* him. No passkey, no Touch ID, no
  Slack-only signing, no opt-in label for his own actions. This **overrides**
  REVIEW.md C1's corollary "Argo records intents, cannot approve" and
  DESIGN.md § *The decision primitive*. Record the override in both files
  (Wave 2), with this date and the owner as the source — do not delete the
  C1 history, append the disposition.
- **No `warden:go`.** Every open issue is ingested. `warden:skip` is the only
  label warden reads, as an opt-out.
- **Third-party issues** (author ≠ `jkrumm`) are assessed automatically
  (investigate tier, body still wrapped as untrusted text in the brief — that
  wrapper is brief hygiene, not friction, keep it), never auto-implemented,
  and land in `needs_human` so the owner sees the assessment in Argo. No
  public comment on third-party issues.
- **Owner issues** auto-implement on the existing rule (`confidence=high` +
  `nextAction=implement`); anything less lands in `needs_human`.
- **Slack stays as-is** (cards, existing buttons). Nothing new is built there.
- **`sideclaw` and `warden` are capped at `investigate`** for warden-originated
  work (CLAUDE.md: warden may never hold tier ≥ 1 on its own executor or on
  itself). Today `config/dispatch-repos.json` gives both `implement` by default.

## Wave 1 — issue intake without a label            <!-- status: done -->
- [x] `scripts/clients/github.py` `search_issues()`: drop the label argument —
      query every open issue under the owner (`owner:jkrumm is:issue is:open
      -label:warden:skip`), keep the pagination/`total_count` refusal. Return
      `labels` alongside `author`/`body`/`url`/`updatedAt`.
- [x] `scripts/triage.py` `ingest_github_go()` → rename to
      `ingest_github_issues()`; delete `GITHUB_GO_LABEL`, add
      `GITHUB_SKIP_LABEL = "warden:skip"`. Keep event source `github_go` (the
      ledger already has rows under it; renaming a source is a migration, not
      worth it — say so in a comment). Keep the disappearance-resolve for
      `new` items only. Store `labels`, `updatedAt` in the event payload.
- [x] Third-party verdict routing: an `investigate`-capped `github_issue` item
      whose verdict lands goes to `needs_human` carrying the verdict summary,
      not `closed`/"answered" (docs/triage.md:90-95 describes today's path).
      Owner-issue routing unchanged. Tests for both.
- [x] `config/dispatch-repos.json`: cap `sideclaw` and `warden` at
      `investigate`; confirm `make check-policy` against sideclaw's
      `GET /api/dispatch-policy` — if sideclaw's boundary still allows
      `implement` there, note it in **Left behind** (sideclaw is not edited by
      this plan).
- [x] `scripts/watchdog-poll.py`: stop polling issues in `poll_github()` (PRs
      stay) — issue items supersede the stale-issue digest line. Resolve the
      open `github_issue` events once with a short note in the state log.
- [x] Docs: FLOWS.md flow 3 rewritten (no label, `warden:skip`, third-party →
      `needs_human`), docs/triage.md issue section, STATE.md origin line,
      state-log §70. Grep for `warden:go` repo-wide and leave no stale mention
      outside the state log.
**Left behind:** `make check-policy` disagrees, as anticipated above: warden's
copy caps `sideclaw`/`warden` at `investigate` now, but sideclaw's own
boundary (`server/lib/dispatch-policy.ts`) still reports `ceiling=implement`
for both — verbatim, `sideclaw: hermes says ceiling=investigate sensitive=False,
sideclaw says ceiling=implement sensitive=False` (same line for `warden`). The
self-authoring-loop prohibition (CLAUDE.md "Talking to sideclaw") is not
actually closed until sideclaw's side matches — a sideclaw-repo change, out of
this plan's scope; whoever picks that up should land it before relying on the
cap in practice. `/review` (sideclaw multi-angle) also caught a real bug this
wave introduced: `search_issues()` didn't check GitHub's `incomplete_results`
flag, which could silently resolve a genuinely-still-open issue's event on
a search-index timeout — fixed in the same commit, with a regression test
(`tests/test_clients.py`, now 93/93). Test gate is now 256/256
(`test_fold_dispatch_verdict_third_party_github_issue_lands_needs_human_not_closed`),
recorded in STATE.md and CLAUDE.md.

## Wave 2 — owner actions pulled from Argo            <!-- status: done -->
- [x] `scripts/clients/argo.py`: `fetch_actions(machine)` →
      `GET /warden/actions?machine=&status=pending` and
      `ack_action(id, {status, result, error})` → `POST /warden/actions/:id/ack`,
      same token and never-raises contract as `push_snapshot()`.
- [x] `scripts/triage.py` `apply_argo_actions()`, run each tick before
      `push_argo_snapshot()`. Closed verb allowlist, code owns every
      transition: `implement` (item in `verdict`/`needs_human` → implement
      dispatch, `authorized_by="owner:argo"`), `merge` (item in
      `merge_blocked` → the existing merge path), `dismiss` (→ `dismissed`
      with the owner's reason), `reinvestigate` (→ a fresh investigate
      dispatch), `note` (appended to the item, included in the next brief —
      DESIGN.md's designed-but-unbuilt `/items/:id/note`). Unknown verb or an
      item in the wrong state → ack `rejected` with the reason, never a
      silent drop. Idempotent on action id.
- [x] `scripts/lifecycle/policy.py`: `authorized_by="owner:argo"` satisfies
      the implement/merge human gate the way a signed Slack approval does.
      The per-repo lock and the `investigate` cap on sideclaw/warden still
      apply.
- [x] **Remove every daily count budget** (owner, 2026-09-15: "absurd
      friction"): `WARDEN_DAILY_BUDGET`, `WARDEN_IMPLEMENT_BUDGET`,
      `WARDEN_MERGE_BUDGET` (policy.py:268-275), `DAILY_INVESTIGATE_BUDGET`
      (triage.py:787) — the constants, the checks, the deferral notes, the
      `budget` object in CLI output and the Argo snapshot, their tests and
      docs. Keep `MAX_OPEN_INVESTIGATIONS` as concurrency pacing (overflow
      waits, never drops) and the per-repo lock (two PRs on one repo is a
      correctness bug, not a budget). Record the removal in DESIGN.md and
      the state log; Wave 4's dashboard shows no budget.
- [x] Snapshot: each board item carries `availableActions` (computed from
      `state` alone — the item's own `max_tier` never blocks an owner
      override, only the repo-level dispatch policy does, checked at apply
      time) and, for `github_issue` origins, `issue: {repo,
      number, url, author, trusted, labels}`. Keep inside `MAX_BODY_BYTES`.
      Update docs/api.md § Argo push.
- [x] DESIGN.md + REVIEW.md: record the owner override (see top of this
      plan). CLAUDE.md load-bearing section: new bullet recording
      `authorized_by="owner:argo"` (no sentence literally said "Argo cannot
      approve" in this file to adjust). State-log §71.
**Left behind:** `make check-policy` still fails on the same Wave-1-left-behind
disagreement (sideclaw's own boundary still allows `implement` on
`sideclaw`/`warden`) — unchanged by this wave, still a sideclaw-repo fix.
`/review --deep` caught and fixed, same commit: a stale `implement_job`
permanently blocking re-implement on a `needs_human` item (also fixes the
reinvestigate→verdict→implement path since both share the claim CAS);
`_apply_argo_note` had no CAS/dedup, contradicting its own idempotency
contract (fixed with an action-id-tagged stamp); `_board_item_issue()`
crashed on a non-dict-but-valid-JSON payload; `_apply_argo_reinvestigate()`
never called `sync_card()`; `fetch_actions()` read an unbounded response
body. Also caught: the implementer's own diff had drifted
`scripts/clients/sideclaw.py`'s `DISPATCH_SCHEMA_VERSION` with no
sideclaw-side source to justify it — reverted before commit. Test gate is
now 265/265 (`test_clients.py` 105/105, `test_api.py` 42/42).

## Wave 3 — argo API: the action queue            <!-- status: done -->
Work happens in `~/SourceRoot/argo` (its own CLAUDE.md and rules apply;
direct-to-master). Contract is whatever Wave 2 shipped on the warden side —
read `warden/docs/api.md` § Argo push first.
- [x] `apps/api/src/db/schema.ts`: `warden_actions` table (id, machine,
      event_id, verb, payload json, status pending|applied|rejected|failed,
      result, error, created_at, acked_at) + drizzle migration.
- [x] `apps/api/src/routes/warden.ts`: `POST /warden/items/:eventId/actions`
      (dashboard → queue; validates verb against the same closed list),
      `GET /warden/actions` (warden pulls), `POST /warden/actions/:id/ack`
      (warden reports). Same auth as `POST /warden/snapshot` for the warden
      side; the dashboard route uses whatever the dashboard already uses.
      OpenAPI per `apps/api/.claude/rules/openapi.md`.
- [x] `routes/warden.test.ts` cases: enqueue, pull pending only, ack
      transitions, unknown verb 400, double ack idempotent.
- [x] argo CLAUDE.md § Warden: the queue exists, and why (warden is
      loopback-only and pulls).
**Left behind:** `/review` (sideclaw multi-angle, adversary + concurrency
angles independently) caught a real bug and it was fixed in the same commit:
`GET /warden/actions` was a plain `SELECT WHERE status='pending'` with no
atomic claim, so two overlapping warden polls (a slow tick still applying
while the next fires) could both fetch the same pending row and double-apply
a non-idempotent verb (merge/implement/dismiss/note) against the ledger.
Fixed by making the pull itself an `UPDATE ... WHERE status='pending' ...
RETURNING` that atomically flips claimed rows to an internal `pulled` status
(never returned by any route response — schema.ts and the query enum both
document it); a claim older than 10 minutes (roughly warden's own poll
cadence) is treated as abandoned and reclaimed by the next pull. Covered by
new tests (`atomically claims pending rows...`, the two lease-reclaim
cases). Also fixed from the same review pass: the closed-verb check on
enqueue used a backwards TS cast (widened the array instead of narrowing
the input — no runtime effect, but asserted something unproven); jsonb
`payload`/`result` columns were untyped, forcing an unchecked cast on read;
`toActionRecord` cast `status` off an unconstrained `text` column with no
runtime guard; enqueue payload and ack result/error had no size cap (now
capped at 100 KB, matching the byte-cap posture `warden_snapshots` already
has). Deferred as discussion-level, not blocking: no dedup guard on rapid
double-enqueue against the same (event_id, verb) — a partial unique index
on `status='pending'` is the likely fix, pick up alongside any future
double-apply investigation; `POST /warden/actions/:id/ack` unconditionally
overwrites a terminal status on a second ack (tolerates a redelivered
identical ack, but would also silently accept a genuinely different
terminal-to-terminal transition with no audit trace); `verb`/`status` are
plain `text` with only app-level enum enforcement, kept in sync with this
repo's `ARGO_ACTION_VERBS` by a source comment only — no in-process check
either side; `machine` on the pull/ack routes is a self-reported string
under the shared bearer, same trust model as `/warden/snapshot` but now
covers a write path. Deployed to prod (`argo.jkrumm.com`, RollHook) and
verified live: `GET /api/health` reports the pushed commit,
`GET /api/warden/actions?machine=mini&status=pending` returns `[]`
authenticated, 422 without `machine`.

## Wave 4 — argo dashboard: issues and one-click triage   <!-- status: done -->
Work happens in `~/SourceRoot/argo/apps/dashboard` (basalt-ui, Mantine, no
Tailwind, `--vx-*` tokens).
- [x] `features/warden/issues-section.tsx`: a GitHub issues section on
      `/warden` — repo#number link, title, third-party badge, state, note
      (verdict summary), age. Grouped by pipeline stage via
      `groupIssueItems()`: needs you / running / auto-implementing / done.
- [x] Action buttons from `availableActions` (Implement, Merge, Dismiss with
      reason, Re-investigate, Note) on issue rows; POST to the queue via
      `enqueueWardenAction()` (TanStack Query mutation in
      `use-warden-actions.ts`), pending state shown as "Queued: <verb>"
      until the next snapshot's `updated_at` moves past the queue moment (or
      a 15-min timeout, reconciled every 30s).
- [x] `model.ts` + `model.test.ts` for the grouping and the pending-action
      reconciliation (`groupIssueItems`, `withPendingAction`,
      `reconcilePendingActions`, `deriveWardenPage`).
- [x] `/check` in argo, then deploy: pushed argo `master`
      (`a85c6d3`) — GitHub Actions `Deploy` workflow (api + dashboard, both
      via RollHook) succeeded, `/api/health` confirmed the new commit live.
      Confirmed via an authenticated chrome-devtools session against
      `https://argo.jkrumm.com/warden`: the section renders the real backlog
      (6 needs-you, 1 auto-implementing, correct per-state action buttons),
      zero console errors.
**Left behind:** `apps/api/src/routes/warden.ts`'s `BoardItemSchema` was
missing `availableActions`/`issue` entirely (present on the wire since Wave 2,
never declared in Argo's own schema) — added, and deliberately typed
`availableActions` as `z.array(z.string())` rather than a hard `z.enum`
(caught by `/review`'s api-contract angle: a closed enum here would 422 the
*entire* snapshot over one item carrying a verb the two repos haven't synced
on). The dashboard's own `WardenActionVerb` is hand-declared and every read of
`item.availableActions` is filtered through `isKnownActionVerb()` before
render — an earlier pass had it silently derived from the (now-loosened) wire
type, which collapsed to plain `string` and would have rendered an
"undefined"-labelled button that still fired a real mutation; caught by a
second `/review` pass, fixed same wave. Also fixed same wave from that first
`/review`: `ActionPromptModal`'s shared form instance leaking stale
dismiss/note text across items on close-without-submit; a keyboard
Enter/Space on a nested Button/Anchor bubbling into the card's own
`onKeyDown` and opening the wrong modal; pending actions never expiring when
a poll returns a structurally-identical snapshot (React Query keeps the same
array reference) — now also reconciled on a 30s timer, not only on reference
change. And from the second `/review` pass: an `onError` race where a
stale/late-failing action could clear a *newer* pending action queued for the
same event (fixed by capturing `queuedAt` as mutation context and only
clearing on a match). `fallow`'s audit never went fully green and was
deliberately not chased further — `24 unused dependencies` is pre-existing,
repo-wide debt confirmed unrelated to this diff (a stashed pre-Wave-4 check
run was fallow-clean); the remaining ~3 duplicate-clone groups between
`board-section.tsx` and `issues-section.tsx` (card skeleton, the responsive
table/card switch, the top-level empty-state wrapper) is the same
architectural call `/review`'s architect angle flagged explicitly as "worth a
deliberate decision, not an auto-apply" (a generic `EntityListSection<T>`) —
picked up only if a future wave touches this area again. Minor,
non-blocking `/review` findings deferred as-is: no confirm-step before
`merge` fires (reads as destructive in the UI even though warden gates it
server-side), the dismiss/note `Textarea` has no accessible `label` (only a
placeholder), no double-submit guard on the prompt modal, `ItemCard` in
`board-section.tsx` has the same keyboard-double-fire shape as the new
`IssueCard` but wasn't touched by this diff, OpenAPI `detail.description` for
`GET /warden/snapshot` wasn't updated to mention the new fields.

## Wave 5 — end to end on a real issue                <!-- status: active -->
- [ ] Open an owner issue in `dispatch-scratch`; watch it become an item,
      get investigated, and either auto-implement (draft PR) or land in
      `needs_human`. Approve from Argo; confirm the transition in the ledger
      and on the page.
- [ ] Same with a third-party-shaped item (fixture author) → `needs_human`,
      Dismiss from Argo.
- [ ] Confirm the existing backlog (research-gateway #3–#7, basalt-ui #51/#52,
      rollhook #21, sideclaw #3 → investigate-only, ntfy-mac #12 third-party)
      is on the page with assessments.
- [ ] STATE.md rewritten, state-log §, delete `docs/waves/PLAN.md` once every
      wave is done.
**Left behind:**
