# Field review — handover

Prompt for **Wave 9** (`~/SourceRoot/dotfiles/docs/waves/PLAN.md`). Run it
after warden has run unattended for **at least 3 days** with nobody watching.
Produces three artifacts, implements nothing: a dated `§` appended to
`docs/history/state-log.md`, a brain Inbox note `Warden Field Review
<date>.md`, and a proposed next `PLAN.md`. Authority order: `DESIGN.md` →
`FLOWS.md` → `REVIEW.md` → `STATE.md` → `docs/history/state-log.md` → this file.

## Before you start

- `make status` — expect the agents block all `✓`, `api (/health)` reachable
  and `ok`, `policy` agreeing, `sideclaw schemas` matching. A green baseline
  from a live run:

  ```
  warden
    venv                     Python 3.11.15
    agents:
      ✓ com.jkrumm.warden-loop  [pid -, last exit 0]
      ✓ com.jkrumm.warden-poll  [pid -, last exit 0]
      ✓ com.jkrumm.warden-sweep  [pid -, last exit 0]
      ✓ com.jkrumm.warden-backup  [pid -, last exit 0]
      ✗ com.jkrumm.warden-api  [pid 52538, LAST EXIT -15] — read .../warden-api.err
    api (/health)            ✓ reachable, ok
    policy                   ✓ both copies agree on all N repos
    sideclaw schemas         ✓ dispatch=2 review=1
    ledger                   <size> <date>
  ```

  The `warden-api` `LAST EXIT -15` line is a known cosmetic artifact of the
  last manual `kickstart -k` — cross-check `api (/health)`, which independently
  reports `ok`.

- Open the ledger with a plain path, not `file:…?mode=ro` and not
  `-readonly`: this box's `/usr/bin/sqlite3` 3.51 cannot open a WAL database
  read-only while no other connection holds it (no `-shm` file to attach to)
  and fails with "unable to open database file (14)"; it worked in earlier
  sessions only because warden-api happened to hold the file. A plain open
  with SELECT-only statements is safe under WAL. Python's `sqlite3` module
  (`ledger.py`, `api.py`) keeps `mode=ro`, which works there.
- The loop's last tick: `sqlite3 "$HOME/.warden/warden.db" "SELECT
  value, updated_at FROM cursors WHERE key='triage_last_run';"` — `value` is
  the last pass's state histogram, `updated_at` its timestamp. Compare against
  `600s` (the LaunchAgent interval) and `/health`'s own `poller_ages.loop`.
- `git status` clean in `warden`, `sideclaw`, `hermes-agent`, `argo`.
- **Did argo PR #19 land?** If not, every tick has logged `triage: argo push —
  http-error:404 (… items)` in `warden-loop.err` and the Argo `/warden` board
  has never received a snapshot — it will show its empty state, not stale
  data. Say so explicitly in the review rather than reading the board as
  "nothing happened."

## What to measure

### (a) The six funnel metrics — `GET /metrics`

```bash
curl -s http://127.0.0.1:7735/metrics | python3 -m json.tool
```

Name each exactly as `docs/api.md` does, and restate its honesty rule: `value`
is `null` only paired with a non-empty `unavailable` reason — **never** read a
`null` as a fabricated `0`, and never read a `0` as "not tracked" (metric 6's
`reverts` leaf is a real `0` from the moment schema 7 landed).

| # | Key | Good | Bad |
|-|-|-|-|
| 1 | `verdicts_recorded_disposition` | rising toward 1.0, large denominator | `excluded_interactive` growing faster than the denominator (Hermes doors bypassing the funnel) |
| 2 | `verified_fixes_vs_silence` | `fixed/(fixed+quiet)` rising | flat near 0 while `quiet` grows (nothing lands, only expires) |
| 3 | `median_needs_human_to_decision_hours` | falling, or `null` (window predates `history_since`) | rising, or many `excluded_dismissed_pairs` (timing out, not decided) |
| 4 | `verified_unattended_fixes_per_week` | `unattended_in_window` > 0 at all (never happened yet) | `null`/`0` for weeks with `fixed_in_window` > 0 |
| 5 | `poller_ages` | every poller well under its threshold (loop 30m, watchdog_poll 90m, dispatch_sweep 15m) | any `stale: true` |
| 6 | `reverts_and_reopens` | both leaves at `0` | `reverts` > 0, or `reopen_after_fixed` > 0 once it stops reading `null` |

### (b) `needs_human` queue — count and age

```bash
sqlite3 "$HOME/.warden/warden.db" \
  "SELECT count(*) FROM triage_items WHERE state='needs_human';"

sqlite3 "$HOME/.warden/warden.db" "
SELECT ti.event_id,
       ROUND((julianday('now') - julianday(t.at)) * 24, 1) AS hours_in_state
FROM triage_items ti
JOIN (
  SELECT event_id, MAX(at) AS at
  FROM item_transitions
  WHERE to_state = 'needs_human'
  GROUP BY event_id
) t ON t.event_id = ti.event_id
WHERE ti.state = 'needs_human'
ORDER BY hours_in_state DESC;"
```

The join only covers items whose entry into `needs_human` is recorded in
`item_transitions` (empty until schema migration 4 — see `docs/api.md`'s
history guard); an item that entered earlier won't appear in the age list even
though it counts in the total — a gap, not a bug. Good: queue small, ages
under the 7-day `needs_human` deadline (`STATE_DEADLINES`, `docs/triage.md`);
bad: ages clustering near 7d (dismissed by expiry, not decided).

### (c) Reverts and reopen-after-`fixed`

```bash
sqlite3 "$HOME/.warden/warden.db" \
  "SELECT count(*) FROM item_transitions WHERE from_state='fixed';"

sqlite3 "$HOME/.warden/warden.db" \
  "SELECT count(*) FROM triage_items WHERE revert_pr IS NOT NULL OR state='reverted';"
```

**Fact worth surfacing, not a bug:** `warden revert` (`cmd_revert` in
`scripts/warden.py`) only writes an `item_transitions` row (to `reverted`) and
stamps `triage_items.revert_pr` — it does **not** write an `operations` row
of a `revert` kind (`operations.kind` today only ever holds `implement`/
`merge`). If Wave 9 wants "revert" as its own operations fact, that's a gap
to build, not a query to fix.

### (d) False `fixed` — reached `fixed` and reopened, or the signal recurred

```bash
sqlite3 "$HOME/.warden/warden.db" \
  "SELECT event_id, from_state, to_state, at, note FROM item_transitions
   WHERE from_state='fixed' ORDER BY at DESC;"
```

Any row here (`to_state` back to `new` per the `liveness_pending` reopen path
in `docs/triage.md`'s state machine, or otherwise) is a false `fixed` by
definition — `fixed` is meant to be terminal. Good: zero rows, ever.

### (e) Budget deferrals

`docs/triage.md`'s step 6 (`maybe_auto_implement()`) writes the marker
`deferred: ` as the prefix of `triage_items.note` when a verdict-state item is
held back by policy/budget, syncing the card immediately — DESIGN.md's "a
deferral must be visible" rule.

```bash
sqlite3 "$HOME/.warden/warden.db" \
  "SELECT count(*) FROM triage_items WHERE note LIKE 'deferred:%';"
```

Good: deferrals appear and clear (item later reaches `implementing`); bad: an
item stuck in `verdict` past its own 24h deadline with a `deferred:` note (it
should have expired to `needs_human`, not sat deferred forever).

### (f) Cost per item — **not built**

Honestly: there is no join today. Two sides exist, unwired: **ledger side** —
`dispatches.job_id`/`tier`/`repo`/`status`/`created_at`/`finished_at`;
**usage side** — sideclaw's usage-tracker records lanes `sideclaw:dispatch`
and `sideclaw:review` (`CLAUDE.md` § Talking to sideclaw, `docs/history/state-log.md` §56's
"Warden requests no model" note) but has no column keyed on `job_id`. Wave 9
decides whether building that join (usage row → job id → `dispatches` →
`triage_items`) is worth it before answering "what did this fix cost."

### (g) Dispatch volume by origin and tier

```bash
sqlite3 "$HOME/.warden/warden.db" \
  "SELECT ti.origin, d.tier, count(*) FROM dispatches d
   JOIN triage_items ti ON ti.event_id = d.origin_event_id
   GROUP BY ti.origin, d.tier ORDER BY 3 DESC;"
sqlite3 "$HOME/.warden/warden.db" \
  "SELECT origin, max_tier, count(*) FROM triage_items GROUP BY origin, max_tier;"
```

Read against `FLOWS.md`'s six scenarios: is `github_issue` volume tracking
real `warden:go` usage, or near-zero? Is `alert` volume dominated by one repo
(worth tuning `minOccurrences`/`cooldownHours` over a code fix)?

## What to read

- `STATE.md` (two pages), then `docs/history/state-log.md`'s last dated `§`.
- `~/Library/Logs/warden-{loop,poll,sweep,backup,api}.{log,err}` — grep
  `warden-loop.err` for `argo push —` (see Before you start), `deferred`,
  `budget`, and any Python traceback.
- `#agents` — each item is one card, updated in place (never a new message per
  stage), a stage checklist, a PR link once one exists. Does a card exist for
  everything the ledger says should have one, and does its state match `/board`?
- The Argo `/warden` board (once PR #19 is live) — six funnel tiles, buckets
  by state (`deferred (budget)` and `unknown` are first-class), per-item
  timeline, banner "Recorded intents — not approvals." Could you answer "what
  happened to item X" from the board alone, without the ledger?
- hermes-agent's `make agent-overview`, or sideclaw's `GET /api/overview.txt`
  — the `warden` block (`warden · N open · needs_human … · merge_blocked … ·
  in flight …` plus prioritised item lines), the herdr-facing surface.
- `scripts/warden list` (`open`/`today`/`all`) and `scripts/warden status
  <job-id>` for the raw record behind any card or board row.

## Where was the friction

**Herdr.** Did `rd wave`/`rd bg` on Sonnet get the job done, or did anything
genuinely need Fable? Did the overview pane surface what mattered?

**Hermes.** Did the door — `warden run` from a Slack thread, the `warden:go`
label, an approval click — actually get used, or did real fixes still go
through a human typing by hand? Did any approval wait on a human longer than
felt right (compare against metric 3)?

**Argo.** Was the board opened at all this week? Did the per-item timeline
answer "what happened to item X" without falling back to a direct ledger query?

**Slack.** Card noise (`sync_card()`'s hash short-circuit should prevent
re-renders with no real change — did it hold?) or cards that stopped updating.

**Where it was too fast.** Any auto-merge that, in hindsight, should have
waited (only `argo`'s canary scope and `vps`'s `observability/**` can
auto-merge today — did that boundary hold?). Any repo/tier combination that
reached a tier you didn't expect unattended. Any `state_deadline` that fired
and dismissed something that deserved more time.

## Decisions this review must surface

Each as a question, the evidence, and where the knob lives:

| Question | Evidence | Knob |
|-|-|-|
| Widen `autoMergePaths` beyond the current canary scope? | (b),(d),(g), FLOWS.md flow 1/2 friction | `config/triage-policy.json` per-repo `autoMergePaths`, validated by `scripts/validate-dispatch-policy.py`, enforced by `scripts/lifecycle/merge.py`'s `merge_gate_check()` |
| Promote or demote a tier ceiling per origin? | (g), FLOWS.md's third-party-issues rule | `config/dispatch-repos.json` per-repo `maxTier`/`defaultTier`, `triage_items.max_tier` |
| Retire a surface (Slack cards, overview block, Argo board)? | "Where was the friction" above — the digest was already retired this way in Wave 7 (`docs/history/state-log.md` §56), it read sideclaw, never the ledger | the surface's own LaunchAgent/cron entry |
| Change `quietResolveHours` or a state deadline? | (b),(c),(d) | `config/triage-policy.json`'s `quietResolveHours`, or `scripts/triage.py`'s `STATE_DEADLINES` |
| Keep validation as a sideclaw `review` job? | false-`fixed`/reopen counts (c)/(d), `/board`'s `merge_blocked` rate | `scripts/lifecycle/dispatch.py`'s `open_review()` |
| Build the cost join? | (f) | none yet — this is the decision to build one |

Carried owner items, check each against `git log`/`gh`: argo PR #19
(`warden-board`) merged? the loop's GitHub PAT (`op://mini/github/token`)
granted Issues read/write (`github_issue` origin is dead under the
LaunchAgent without it)? the 4.3 "human types in Slack" acceptance
(`docs/history/state-log.md` §55/§56)?

## Output

**1. `docs/history/state-log.md`** — append (never edit a past section) a
dated `§` with a header table, then the findings:

```markdown
## <N>. Field review (2026-MM-DD, window <start> → <end>)

| | |
|-|-|
| Window reviewed | <days> days, <start ISO> → <end ISO> |
| Loop ticks | <count> |
| Items opened / closed | <opened> / <closed over window> |
| verdicts_recorded_disposition | <value> (<numerator>/<denominator>) |
| verified_fixes_vs_silence | <value> |
| median_needs_human_to_decision_hours | <value or null+reason> |
| verified_unattended_fixes_per_week | <value or null+reason> |
| poller_ages (worst) | <minutes> |
| reverts_and_reopens | reverts=<n>, reopen_after_fixed=<n or null+reason> |
```

**2. A brain Inbox note**, `Inbox/Warden Field Review <date>.md` — Inbox's
capture schema is `title`, `date`, `tags` only, no MOC discipline
(`brain/CLAUDE.md`).

**3. A proposed next `PLAN.md`**, in the `/wave` skill's format
(`~/.claude/skills/wave/SKILL.md`): numbered `## Wave N — <name>` sections
with a status comment, checklist items, and a "Left behind" line.

## Do not

Implement anything in this wave (a review produces a plan, not code). Edit
`docs/history/state-log.md`'s past sections (append only). Re-litigate
`REVIEW.md` without new evidence — and say so explicitly if you have some.
