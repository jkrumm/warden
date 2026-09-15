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

## Wave 3 — argo API: the action queue            <!-- status: active -->
Work happens in `~/SourceRoot/argo` (its own CLAUDE.md and rules apply;
direct-to-master). Contract is whatever Wave 2 shipped on the warden side —
read `warden/docs/api.md` § Argo push first.
- [ ] `apps/api/src/db/schema.ts`: `warden_actions` table (id, machine,
      event_id, verb, payload json, status pending|applied|rejected|failed,
      result, error, created_at, acked_at) + drizzle migration.
- [ ] `apps/api/src/routes/warden.ts`: `POST /warden/items/:eventId/actions`
      (dashboard → queue; validates verb against the same closed list),
      `GET /warden/actions` (warden pulls), `POST /warden/actions/:id/ack`
      (warden reports). Same auth as `POST /warden/snapshot` for the warden
      side; the dashboard route uses whatever the dashboard already uses.
      OpenAPI per `apps/api/.claude/rules/openapi.md`.
- [ ] `routes/warden.test.ts` cases: enqueue, pull pending only, ack
      transitions, unknown verb 400, double ack idempotent.
- [ ] argo CLAUDE.md § Warden: the queue exists, and why (warden is
      loopback-only and pulls).
**Left behind:**

## Wave 4 — argo dashboard: issues and one-click triage   <!-- status: pending -->
Work happens in `~/SourceRoot/argo/apps/dashboard` (basalt-ui, Mantine, no
Tailwind, `--vx-*` tokens).
- [ ] `features/warden/`: a GitHub issues section on `/warden` — repo#number
      link, title, author with a third-party badge, item state, verdict
      summary/recommendation/confidence, age. Grouped by state: needs you /
      running / auto-implementing / done.
- [ ] Action buttons from `availableActions` (Implement, Merge, Dismiss with
      reason, Re-investigate, Note) on issue rows and in the existing item
      modal; POST to the queue via `lib/queries/warden.ts` (TanStack Query
      mutation), show "queued → applied/rejected" from the action status
      until the next snapshot reflects the transition.
- [ ] `model.ts` + `model.test.ts` for the grouping and the pending-action
      reconciliation.
- [ ] `/check` in argo. Deploying argo is the owner's call — stop here with
      the deploy command in **Left behind**; do not spawn Wave 5 until he has
      deployed.
**Left behind:**

## Wave 5 — end to end on a real issue                <!-- status: pending -->
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
