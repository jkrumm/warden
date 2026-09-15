"""lifecycle — the origin-independent half of warden's control plane.

Policy resolution, gates, signed-approval spend, episode opening, merge and
rollout, and the operations ledger those write. Everything here is called as
a function by the loop (triage.py), by intents.py's drain and by the
`warden` CLI; nothing here shells out except through `clients/`.
"""
