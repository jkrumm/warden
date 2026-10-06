# Improvement journal

One line per iteration of the improvement loop (`docs/improve/LOOP.md`). Newest last.

2026-10-06 · baseline: needs-you 23→3→0 after wave 7 + owner decisions; 4 items in flight (sideclaw op:// scan, research-gateway alert, sideclaw escalation rules, warden needs_decision guard) · loop started
2026-10-06 09:45 · needs-you 0→13 (6 hermes checkpoint-skip duplicates, 1 homelab dozzle, 4 sideclaw/dotfiles, 1 research-gateway, 1 weatherorb), working 1, failed 0; sideclaw check route degraded (13 fails in a row, iu-5xx-after-output) · friction: checkpoint-skip alerts split by hash pre-fingerprint fix (98d7798, no new splits since) and each asks owner a question the standing policy answers · warden run hermes-agent item 1461 (downgrade missing-workdir skip to INFO) · previous 4 in-flight items now sit in needs_decision (1458, 1452, 1389, 1390), not fixed — recheck next tick; DECISION: none
