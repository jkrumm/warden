# STATE — warden

**Read this before anything else. It is the memory; the conversation is not.**
Authority order: `DESIGN.md` → `FLOWS.md` → `REVIEW.md` → this file →
`docs/history/state-log.md`.

| | |
|-|-|
| Last updated | 2026-09-11 (§60) |
| Current wave | Estate chain Wave 8 done (§57); field look §58; autonomy §59. Wave 9, the field review, is the owner's to start — authority `~/SourceRoot/dotfiles/docs/waves/PLAN.md` |
| Repo state | `master`, five LaunchAgents on the mini |
| Ledger | `~/.warden/warden.db`, schema 9 |
| Tests | `tests/test_triage.py` 242/242 is the gate; `make test` runs all suites |
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
| `com.jkrumm.warden-api` | `scripts/api.py --serve` (GET /metrics, /health) on `127.0.0.1:7735` | long-running, `KeepAlive` |

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
- `push_argo_snapshot()` runs as step 10 of `run()`, after every tick; argo
  PR #19 merged 2026-09-11 (62d9633), the first `argo push — ok` landed at
  11:06Z, the `/warden` board is live.
- `maybe_auto_remediate()` runs between `run_verbs()` and
  `maybe_auto_implement()`: a `hostVerbs` policy match plus a folded verdict
  at or above `hostVerbMinConfidence` (medium) runs one `HOST_VERB_ALLOWLIST`
  argv per verb per pass, cooldown and attempt cap keyed per verb, receipt in
  `operations` (`kind=host`), then `liveness_pending` verified by
  `kuma-push-fresh`. First real run 11:36Z: `restart-hermes-gateway`, three
  items discharged (§59).
- sideclaw routes `review` and `dispatch` to `glm-5.3-flash` on IU
  (`SIDECLAW_MODEL_REVIEW`/`SIDECLAW_MODEL_DISPATCH` in sideclaw's `.env`).
- Slack delivery is a plain HTTP client (`chat.postMessage`/`chat.update`),
  never the gateway's live `slack_bolt` connection.
- The `#agents` overview digest is retired.
- sideclaw's published verdict schemas are pinned in
  `scripts/clients/sideclaw.py` (`DISPATCH_SCHEMA_VERSION=2`,
  `REVIEW_SCHEMA_VERSION=1`) and checked by `make check-schemas`.

## Open — owner actions

- One `needs_human` card remains (meteo probe uk:220), closed by item 996's
  PR when it merges. The other eight were closed 2026-09-11 with reasons in
  the ledger (§60).
- Grant the loop's PAT (`op://mini/github/token`) Issues read/write — the
  `github_issue` origin cannot poll or comment under the LaunchAgent until
  then.
- The §55 4.3 "human types in Slack" acceptance is still open.
- `warden-api`'s "LAST EXIT -15" is the §55 kickstart; cosmetic.

## Carried debt

- sideclaw `fallow` fails at HEAD — pre-existing, not this repo's.
- Cost per Warden item needs a join on ledger job ids; sideclaw sets no
  `USAGE_LANE`, so usage-tracker cannot attribute its spend at all (§58).
- `#agents` is warden-only since 2026-09-11 (Hermes's narratives cron moved to
  `#hermes`); approval buttons still post to `#hermes`. Three `warden_canary`
  merge_blocked items still sit on the board.
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
- Autonomy: host verbs, Argo live, GLM routing — §59
- Closing the queue: reminders, the heartbeat probe, Kuma sync — §60

Every wave appends a § to the log and rewrites this file; never edit the
log's past sections.

### Next action

Watch item 996 (meteo watchdog heartbeat gate, `implementing` on GLM) land
its PR and merge; then close uk:220. The three hermes items resolved as
`quiet` before the Kuma-heartbeat probe landed, so the first `fixed` from a
host verb is still ahead. Loop: a host verb for uk:204 (sideclaw kickstart,
guarded by no dispatch in flight). Wave 9, the field review, from
`docs/handover-field-review.md` after a few days of this.
