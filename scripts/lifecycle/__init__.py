"""lifecycle — the origin-independent half of warden's control plane.

Policy resolution, gates, episode opening, merge, rollout (deploy + verify), and the
operations ledger those write. Everything here is called as a function by the
loop (triage.py) and by the `warden` CLI; nothing here shells out except
through `clients/`.
"""
