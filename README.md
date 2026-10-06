# warden

**warden turns a signal into a verified outcome, and it is the only thing that
holds that state.** Pollers feed it, sideclaw executes for it, Argo and Slack
render it. What it is: `DESIGN.md`. Where it is: `STATE.md`. How to work on it:
`AGENTS.md`.

## Running it

Five LaunchAgents on the mini, never `hermes cron` jobs — the loop that notices
Hermes is broken cannot depend on Hermes to run.

```bash
make setup     # venv + plists + load the agents + warden on PATH
make check     # compile + every test suite
make status    # what is loaded, what ran last, is the ledger reachable
make verify    # /health ok and every agent's last exit 0
make logs      # tail every agent's logs
```

## Using it

Unattended work goes in through `warden run` or a GitHub issue; the item rides
triage → investigate → implement → merge train → deploy → verify, visible in Argo
`/warden`, `warden list` and one Slack line when it is `fixed` or needs a decision.

| Verb | Syntax | Does |
|-|-|-|
| `run` | `warden run <repo> [--tier investigate\|implement] [--wait] [--json] <<'BRIEF' … BRIEF` | opens an item; implements when the verdict says so, `--tier investigate` = answer only |
| `dispatch` | `warden dispatch <repo> [--tier …] [--model …] <<'BRIEF' … BRIEF` | one bare episode, a verdict, no item |
| `status` / `list` | `warden status <job-id> [--json]` · `warden list [open\|today\|all]` | see it |
| `merge` | `warden merge <job-id> --why … --confirm` | lands a PR through the same merge gate |
| `abort` / `revert` / `close` | `warden abort <event-id> --why …` · `warden revert <event-id> --pr <n> --why …` · `warden close <event-id> --why … [--reason resolved\|ignored]` | stop, record a revert, close by hand |
| `retry` | `warden retry <event-id> [--why …]` | puts a `failed` item back where it failed, with a fresh re-drive budget |
| `reinvestigate` | `warden reinvestigate <event-id> --why …` | sends an item back to `triaged` for a fresh investigation |

`warden help` has the exact flags.

| Symptom | Look here |
|-|-|
| Nothing seems to be running | `make status`, then `make logs` |
| A poller might be dead | `GET http://127.0.0.1:7735/health` — each poller's heartbeat age |
| An item won't move | `warden status <id>`'s `note`, or the item's Argo timeline |
| An item is `failed` | its `note` carries the error and `failure_class` says whether the loop re-drives it (`infra`, `policy`) or waits for you (`work`); `warden retry` or Argo's retry puts it back |
