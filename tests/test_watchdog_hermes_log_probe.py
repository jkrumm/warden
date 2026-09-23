#!/usr/bin/env python3
"""A deliberate fallback probe must not become a Warden card.

Run: .venv/bin/python3 tests/test_watchdog_hermes_log_probe.py

A post-rollout check validates the brain's fallback chain by overriding the model
to a sentinel id ending in `-does-not-exist`, so the IU endpoint is *guaranteed*
to answer 404 — the 404 is the probe's expected result, and the fallback then
serves the turn (`Fallback activated: <sentinel> -> gpt-6-luna`). Hermes retries
`api_max_retries` times, so ONE probe writes exactly three ERROR lines to
`logs/errors.log` and lands on the poller's `minOccurrences: 3`: a card, a triage
item and an investigate episode per probe run (2026-09-23, item 676).

`PROBE_SENTINEL_RE` filters those lines out of the hermes-log poller instead of
`triage-policy.json`'s `ignore` list, which keys on `external_id` — the signature
is truncated before the model id, so an ignore entry there could not separate the
sentinel from a genuine "No suitable backend server found" for the real brain
model. The cases below hold both halves: sentinel lines are dropped, a real 404
and an ordinary ERROR are still reported exactly as before.
"""

import datetime as dt
import importlib.util
import sqlite3
import sys
import tempfile
from pathlib import Path
from zoneinfo import ZoneInfo

REPO = Path(__file__).resolve().parent.parent
spec = importlib.util.spec_from_file_location("watchdog_poll", REPO / "scripts" / "watchdog-poll.py")
wp = importlib.util.module_from_spec(spec)
spec.loader.exec_module(wp)

failures: list[str] = []


def check(name: str, got, want) -> None:
    if got == want:
        print(f"  ok   {name}")
    else:
        failures.append(f"{name}: got {got!r}, want {want!r}")
        print(f"  FAIL {name}: got {got!r}, want {want!r}")


LOG_TZ = ZoneInfo("Europe/Berlin")
NOW = dt.datetime.now(LOG_TZ).replace(microsecond=0)


def ts(seconds_ago: int) -> str:
    return (NOW - dt.timedelta(seconds=seconds_ago)).strftime("%Y-%m-%d %H:%M:%S")


SENTINEL = "deepseek-v4.1-flash-does-not-exist"
# The probe's three retry attempts, byte-identical apart from their timestamps.
PROBE_LINES = [
    f"{ts(10)},122 ERROR agent.chat_completion_helpers: Streaming failed before delivery: "
    f"Error code: 404 - No suitable backend server found for model '{SENTINEL}'.",
    f"{ts(7)},601 ERROR agent.chat_completion_helpers: Streaming failed before delivery: "
    f"Error code: 404 - No suitable backend server found for model '{SENTINEL}'.",
    f"{ts(3)},559 ERROR agent.chat_completion_helpers: Streaming failed before delivery: "
    f"Error code: 404 - No suitable backend server found for model '{SENTINEL}'.",
    # The retry WARNING and the traceback ride along in the same window; neither
    # carries a leading ` ERROR `, so the poller never grouped them.
    f"{ts(3)},560 WARNING [20260923_144746_ea474d] agent.conversation_loop: API call failed "
    f"(attempt 3/3) error_type=NotFoundError provider=custom model={SENTINEL}",
    f"openai.NotFoundError: Error code: 404 - No suitable backend server found for model '{SENTINEL}'.",
]
# A genuine 404 for the model the brain is actually configured with — the alert
# that must survive this filter, and the reason `ignore` was not the place for it.
REAL_LINE = (
    f"{ts(2)},500 ERROR agent.chat_completion_helpers: Streaming failed before delivery: "
    "Error code: 404 - No suitable backend server found for model 'deepseek-v4.1-flash'."
)
OTHER_LINE = f"{ts(1)},600 ERROR tools.checkpoint_manager: Git command failed: git add -A (rc=128)"


def poll(lines: list[str], name: str) -> dict[str, dict]:
    tmp = Path(tempfile.mkdtemp(prefix=f"wd-probe-{name}-"))
    home = tmp / "hermes-home"
    (home / "logs").mkdir(parents=True)
    (home / "logs" / "errors.log").write_text("\n".join(lines) + "\n")
    wp.HERMES_HOME = home
    wp.HERMES_LOG_FILES = [(f"test_offset_{name}", "logs/errors.log")]
    wp.DB_PATH = tmp / "watchdog.db"
    wp._ledger.connect(wp.DB_PATH, migrate=True).close()
    conn: sqlite3.Connection = wp.db_connect()
    try:
        return {g["external_id"]: g for g in wp.poll_hermes_logs(conn, NOW, NOW.isoformat())}
    finally:
        conn.close()


print("\n1. a probe's three ERROR lines mint no signature at all")
got = poll(PROBE_LINES, "probe")
check("no signature", got, {})

print("\n2. a real 404 for the configured brain model is still reported")
got = poll([REAL_LINE], "real")
check("one signature", len(got), 1)
check("keyed on the streaming line", list(got)[0].startswith(
    "agent-chat-completion-helpers-streaming-failed-before-delivery-error-code-404-no-suitable-backend"), True)
check("carries the real model id", "deepseek-v4.1-flash'" in list(got.values())[0]["payload"]["first_line"], True)

print("\n3. an unrelated ERROR is untouched")
got = poll([OTHER_LINE], "other")
check("one signature", len(got), 1)
check("names the checkpoint manager", "checkpoint-manager-git-command-failed" in list(got)[0], True)

print("\n4. probe and real 404 in one batch: exactly one signature, the real one")
got = poll(PROBE_LINES + [REAL_LINE, OTHER_LINE], "mixed")
check("two signatures", len(got), 2)
check("no sentinel leaked", any("does-not-exist" in k or "does-not-exist" in g["payload"]["first_line"]
                                for k, g in got.items()), False)

print()
if failures:
    print(f"{len(failures)} failure(s):")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
print("all cases as expected")
