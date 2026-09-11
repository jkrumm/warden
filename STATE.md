# STATE — warden

**Read this before anything else. It is the memory; the conversation is not.**
Authority order: `DESIGN.md` → `FLOWS.md` → `REVIEW.md` → this file →
`docs/history/state-log.md`.

| | |
|-|-|
| Last updated | 2026-09-11 |
| Current wave | Estate chain Wave 8 done (§57). Wave 9, the field review, is the owner's to start — authority `~/SourceRoot/dotfiles/docs/waves/PLAN.md` |
| Repo state | `master`, five LaunchAgents on the mini |
| Ledger | `~/.warden/warden.db`, schema 8 |
| Tests | `tests/test_triage.py` 211/211 is the gate; `make test` runs all suites |
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
- Validation is a sideclaw `review` job on the pull request; automatic
  dispatches pass `model=None` and run on sideclaw's JUDGE route.
- `push_argo_snapshot()` runs as step 10 of `run()`, after every tick.
- Slack delivery is a plain HTTP client (`chat.postMessage`/`chat.update`),
  never the gateway's live `slack_bolt` connection.
- The `#agents` overview digest is retired.
- sideclaw's published verdict schemas are pinned in
  `scripts/clients/sideclaw.py` (`DISPATCH_SCHEMA_VERSION=2`,
  `REVIEW_SCHEMA_VERSION=1`) and checked by `make check-schemas`.

## Open — owner actions

- Merge argo PR #19 — until then every tick logs `argo push —
  http-error:404`.
- Grant the loop's PAT (`op://mini/github/token`) Issues read/write — the
  `github_issue` origin cannot poll or comment under the LaunchAgent until
  then.
- The §55 4.3 "human types in Slack" acceptance is still open.
- `warden-api`'s "LAST EXIT -15" is the §55 kickstart; cosmetic.

## Carried debt

- sideclaw `fallow` fails at HEAD — pre-existing, not this repo's.
- Cost per Warden item needs a join on ledger job ids; the usage lanes only
  give `sideclaw:dispatch`/`sideclaw:review` today.
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

Every wave appends a § to the log and rewrites this file; never edit the
log's past sections.

### Next action

Wave 9, the field review, run by the owner from
`docs/handover-field-review.md`.
