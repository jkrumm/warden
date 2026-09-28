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

warden is six LaunchAgents on the mini, never `hermes cron` jobs. The reason is the
whole reason this repo is separate: the loop that notices Hermes is broken cannot
depend on Hermes being up to run it, and its Slack delivery is a plain HTTP client
rather than the gateway's live connection, posting under warden's own Slack app
identity once it's created and seeded — see `slack/README.md`.

```bash
make setup     # venv + plists + load the agents
make test      # every tests/*.py — hand-rolled runners, not pytest
make status    # what is loaded, what ran last, is the ledger reachable
```

The ledger is `~/.warden/warden.db` — SQLite, WAL, one writer, and **not in git**.
It backs up to homelab and rides the existing restic → B2 path from there.

## How to use it

**Three lanes** — unattended work goes through the lifecycle lane; the other
two are a human's:

| Lane | Command | What you get |
|-|-|-|
| executor | sideclaw `dispatch` (MCP) | one bare episode, a typed verdict, no item |
| lifecycle | `warden run <repo> <<'BRIEF' ... BRIEF` | an item on the ledger riding investigate → verdict → implement → review → merge, gated by policy, visible in `#agents`, Argo and `warden list` |
| colleague | `rd bg <repo> '<task>'` / `agent-dispatch bg` | a durable Claude you steer with `rd say` |

**Four verbs**, exact syntax from `scripts/warden.py`:

| Verb | Syntax | Does |
|-|-|-|
| `run` | `warden run <repo> [--tier investigate\|implement] [--wait] [--json] <<'BRIEF' ... BRIEF` | opens an item |
| `status` / `list` | `warden status <job-id> [--json]` · `warden list [open\|today\|all]` | see it |
| `merge` | `warden merge <job-id> --why "<reason>" --confirm [--json]` | lands it when the loop is blocked on a human |
| `abort` / `revert` | `warden abort <event-id> --why "<reason>"` · `warden revert <event-id> --pr <number> --why "<reason>"` | stops an in-flight item; `revert` undoes a landed one |

**When something is stuck:**

| Symptom | Look here |
|-|-|
| Nothing seems to be running | `make status` — five agents `✓`, `api (/health)` `ok` |
| A specific pass errored | `~/Library/Logs/warden-{loop,poll,sweep,backup,api}.err` |
| An item won't move | `warden status <id>`'s `note` field, or the item's `#agents` card |
| Unclear what happened to an item | Argo `/warden` board's per-item timeline |
| A poller might be dead | `GET /health` — each poller's heartbeat age vs. its own threshold |
| A verdict looks silently ignored | `make check-schemas` — a sideclaw schema mismatch |
| `needs_human` vs `merge_blocked` | `needs_human` = a human's turn to decide; `merge_blocked` = read the item's `note` for why the merge gate refused |

Run this periodically, not just when stuck: `docs/handover-field-review.md`.
