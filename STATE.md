# STATE — warden

**Read this before anything else. It is the memory; the conversation is not.**
Authority order: `DESIGN.md` → `FLOWS.md` → `REVIEW.md` → this file →
`docs/history/state-log.md`.

| | |
|-|-|
| Last updated | 2026-09-11 (§58) |
| Current wave | Estate chain Wave 8 done (§57); first field look §58. Wave 9, the field review, is the owner's to start — authority `~/SourceRoot/dotfiles/docs/waves/PLAN.md` |
| Repo state | `master`, five LaunchAgents on the mini |
| Ledger | `~/.warden/warden.db`, schema 8 |
| Tests | `tests/test_triage.py` 213/213 is the gate; `make test` runs all suites |
| Next action | see § Next action (bottom) |

---

## What is live

Five LaunchAgents run the whole control plane; no `hermes cron` job is in the
loop.

| Agent | Runs | Interval |
|-|-|-|
| `com.jkrumm.warden-loop` | `scripts/triage.py --run` | 600s |
| `com.jkrumm.warden-poll` | ingest | 1800s |
| `com.jkrumm.warden-sweep` | `scripts/dispatch-sweep.py` | 300s |
| `com.jkrumm.warden-backup` | `scripts/warden-backup.sh` | daily 03:10 |
| `com.jkrumm.warden-api` | `scripts/api.py --serve` (GET /metrics, /health) | long-running, `KeepAlive` |

- The `warden` CLI (`run`, `dispatch`, `status`, `list`, `merge`, `abort`,
  `revert`) replaces the old bash verbs; the loop calls `scripts/lifecycle/`
  as functions, not shell scripts.
- Every origin opens an item. `triage_items.origin` is one of `alert` (the
  pollers), `github_issue` (the staleness poller and the `warden:go` label,
  event source `github_go`), or `human` (`warden run`, typed in herdr or
  through Hermes's door with `--origin-channel`/`--origin-thread`). See
  `docs/history/state-log.md` §55.
- Automatic investigate/implement dispatches pass `AUTO_DISPATCH_MODEL`
  (`glm-5.3-flash`, env `TRIAGE_AUTO_DISPATCH_MODEL`) and run on the IU
  backend, never Max. Validation is a sideclaw `review` job on the pull
  request and still runs on sideclaw's JUDGE route (no per-call knob).
- `needs_human` / `merge_blocked` cards carry an `Action required` section:
  `Do this: <note>` plus a day-granularity auto-dismiss countdown.
- `push_argo_snapshot()` runs as step 10 of `run()`, after every tick.
- Slack delivery is a plain HTTP client (`chat.postMessage`/`chat.update`),
  never the gateway's live `slack_bolt` connection.
- The `#agents` overview digest is retired.
- sideclaw's published verdict schemas are pinned in
  `scripts/clients/sideclaw.py` (`DISPATCH_SCHEMA_VERSION=2`,
  `REVIEW_SCHEMA_VERSION=1`) and checked by `make check-schemas`.

## Open — owner actions

- Mark argo PR #19 ready and merge it — it is a **draft**, which is why it
  never landed; until then every tick logs `argo push — http-error:404`.
- Six `needs_human` cards in `#agents` (hermes gateway wedged, sideclaw crash,
  meteo probe, hermes patch corruption, research-gateway OOM) need a hand
  action or a dismissal; the 168h clock dismisses them 2026-09-16.
- Grant the loop's PAT (`op://mini/github/token`) Issues read/write — the
  `github_issue` origin cannot poll or comment under the LaunchAgent until
  then.
- The §55 4.3 "human types in Slack" acceptance is still open.
- `warden-api`'s "LAST EXIT -15" is the §55 kickstart; cosmetic.

## Carried debt

- sideclaw `fallow` fails at HEAD — pre-existing, not this repo's.
- Cost per Warden item needs a join on ledger job ids; sideclaw sets no
  `USAGE_LANE`, so usage-tracker cannot attribute its spend at all (§58).
- The 1-day `needs_human` reminder from `docs/api.md` is not built; a card
  lands once, then silence until the 168h dismissal.
- `#agents` carries both warden cards and Hermes's narrative digest; approval
  buttons post to `#hermes`. Three `warden_canary` merge_blocked items still
  sit on the board.
- No ledger restore path yet.

## History

- Wave 0 — `docs/history/state-log.md` §§13–25
- Wave 1 — §§26–36
- Wave 2 — §§37–43
- Wave 3 — §§44–51
- Estate chain Wave 4 — §§52–53
- Wave 5 — §54
- Wave 6 — §55
- Wave 7 — §56
- Wave 8 — §57
- First field look — §58

Every wave appends a § to the log and rewrites this file; never edit the
log's past sections.

### Next action

Owner: merge argo PR #19, clear the six `needs_human` cards. Loop: the
1-day `needs_human` reminder, then `USAGE_LANE` tagging in sideclaw. Wave 9,
the field review, from `docs/handover-field-review.md` after that.
