# warden

**warden turns a signal into a verified outcome, and it is the only thing in the
estate that holds that state.** Everything else feeds it (pollers), executes for it
(sideclaw), or renders it (Argo, Slack).

| | |
|-|-|
| `DESIGN.md` | what warden is, why it exists, and what "done" means. Authoritative. |
| `FLOWS.md` | six scenarios end to end, and where a human is genuinely needed. |
| `REVIEW.md` | four reviews of the design, and what was rejected. Settled arguments stay settled. |
| `STATE.md` | where the implementation actually is. Read this before working. |

## Running it

warden is four LaunchAgents on the mini, never `hermes cron` jobs. The reason is the
whole reason this repo is separate: the loop that notices Hermes is broken cannot
depend on Hermes being up to run it, and its Slack delivery is a plain HTTP client
rather than the gateway's live connection.

```bash
make setup     # venv + plists + load the agents
make test      # every tests/*.py — hand-rolled runners, not pytest
make status    # what is loaded, what ran last, is the ledger reachable
```

The ledger is `~/.warden/warden.db` — SQLite, WAL, one writer, and **not in git**.
It backs up to homelab and rides the existing restic → B2 path from there.
