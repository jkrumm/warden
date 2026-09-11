# STATE — warden

**Read this before anything else. It is the memory; the conversation is not.**
Authority order: `DESIGN.md` → `FLOWS.md` → `REVIEW.md` → this file →
`docs/history/state-log.md`.

| | |
|-|-|
| Last updated | 2026-09-11 (§62) |
| Current wave | Estate chain Wave 8 done (§57); field look §58; autonomy §59. Wave 9, the field review, is the owner's to start — authority `~/SourceRoot/dotfiles/docs/waves/PLAN.md` |
| Repo state | `master`, five LaunchAgents on the mini |
| Ledger | `~/.warden/warden.db`, schema 9 |
| Tests | `tests/test_triage.py` 244/244 is the gate; `make test` runs all suites |
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
- Cards, receipts and reminders post under warden's own Slack app identity
  (`resolve_slack_token()` tries `op://common/slack/WARDEN_BOT_TOKEN` first),
  falling back to Hermes's bot user until that app is created and seeded —
  `slack/README.md` has the owner steps. The read path (`#alerts`/`#updates`
  history in `watchdog-poll.py`) stays pinned to Hermes's token regardless
  (`resolve_alerts_read_token()`) — a `chat:write`-only app cannot read.
- The `#agents` overview digest is retired.
- sideclaw's published verdict schemas are pinned in
  `scripts/clients/sideclaw.py` (`DISPATCH_SCHEMA_VERSION=2`,
  `REVIEW_SCHEMA_VERSION=1`) and checked by `make check-schemas`.

## Open — owner actions

- Create the Warden Slack app: mint a config token at api.slack.com/apps,
  `make slack-app-create SLACK_CONFIG_TOKEN=…`, install, store the bot token
  at `op://common/slack/WARDEN_BOT_TOKEN`, and only THEN add the ref to
  `dotfiles-private/headless.refs` — `secrets-seed.sh` is `set -e` and an
  unresolvable ref breaks the next reseal for every consumer (§62). Until
  then warden posts as Hermes and says so once per run on stderr.
- The §55 4.3 "human types in Slack" acceptance is still open.
- `warden-api`'s "LAST EXIT -15" is the §55 kickstart; cosmetic.

## Carried debt

- sideclaw `fallow` fails at HEAD — pre-existing, not this repo's.
- Cost per Warden item is a usage-tracker query on `sub_tool` now that
  sideclaw tags every session (`sideclaw:<tool>`, §60); the ledger join is
  still not built.
- `#agents` is warden-only since 2026-09-11 (Hermes's narratives cron moved to
  `#hermes`); approval buttons still post to `#hermes`.
- No ledger restore path yet. The repo itself has no remote; since §62 the
  daily backup ships a `git bundle` of every ref next to the ledger snapshots.

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
- Warden's own Slack identity — §61
- MacBook field report folded in: PAT was never gated, repo bundle — §62

Every wave appends a § to the log and rewrites this file; never edit the
log's past sections.

### Next action

`needs_human` is 0; item 996 landed (meteo master 95d3e3b, watchdog
105/105, Kuma 220 UP with `degraded: true`). Owner: the Warden Slack app
(above). Loop: the first host-verb `fixed` is still ahead — the three hermes
items resolved as `quiet` before the heartbeat probe landed; a sideclaw host
verb for uk:204-shaped crashes, guarded by no dispatch in flight; the ledger
restore path. Wave 9, the field review, from `docs/handover-field-review.md`
after a few days of this.
