#!/usr/bin/env python3
"""The event fingerprint is the identity of every grouped/derived source.

Run: .venv/bin/python3 tests/test_watchdog_fingerprint.py

`fingerprint(title)` is the title minus timestamps, UUIDs, hex ids, paths,
log-file names and numbers — so the same event arriving twice (from two log files,
or from Slack an hour apart) is ONE event, not two. Covers each strip class, the
never-empty guarantee, and the two dedupe paths that matter: hermes_log across
HERMES_LOG_FILES, and aggregate_slack_batch().
"""

from __future__ import annotations

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


fp = wp.fingerprint

print("\n1. each strip class leaves the same fingerprint as the bare message")
BASE = "gateway: connection reset"
check("bare message", fp(BASE), "gateway-connection-reset")
variants = {
    "iso timestamp": "2026-09-23T14:47:46.122Z gateway: connection reset",
    "iso timestamp, offset": "2026-09-23 14:47:46,122+02:00 gateway: connection reset",
    "slash timestamp": "2026/09/01 15:00:34 gateway: connection reset",
    "rfc timestamp": "Tue, 01 Sep 2026 15:00:34 GMT gateway: connection reset",
    "bare date": "gateway: connection reset 01.09.2026",
    "bare time": "gateway: connection reset 15:00:34",
    "uuid": "gateway: connection reset 550e8400-e29b-41d4-a716-446655440000",
    "hex id": "gateway: connection reset a1b2c3d4e5f6",
    "git sha": "gateway: connection reset 9fceb02d0ae598e95dc970b74767f19372d61af8",
    "0x literal": "gateway: connection reset 0xDEADBEEF",
    "absolute path": "gateway: connection reset /Users/jkrumm/.hermes/logs/errors.log",
    "home path": "gateway: connection reset ~/.hermes/state.db",
    "relative path": "gateway: connection reset ./logs/x.txt",
    "log file name": "gateway.error.log gateway: connection reset",
    "rotated log file": "gateway: connection reset (errors.log.3)",
    "number": "gateway: connection reset 604",
    "decimal": "gateway: connection reset 3.14",
}
for name, title in variants.items():
    check(name, fp(title), fp(BASE))

check("duration keeps only its unit", fp("gateway: connection reset 12ms 604s"), "gateway-connection-reset-ms-s")

print("\n2. strip order: a UUID or timestamp is not half-eaten by the number rule")
check("uuid whole", fp("job 550e8400-e29b-41d4-a716-446655440000 failed"), "job-failed")
check("timestamp whole", fp("[ERROR] 2026/09/01 15:00:34 (504) Unknown: An unknown error occurred."),
      "error-unknown-an-unknown-error-occurred")
check("url keeps host, drops path and query",
      fp("GET https://api.example.com:8443/v1/items?id=3 failed"), "get-https-api-example-com-failed")

print("\n3. words are not eaten")
check("and/or survives", fp("read and/or write failed"), "read-and-or-write-failed")
check("pure-letter hex-ish word survives", fp("decade facade failed"), "decade-facade-failed")

print("\n4. never empty when the title has any alphanumerics")
check("all numbers", fp("12345"), "12345")
check("timestamp only", fp("2026-09-23 14:47:46"), "2026-09-23-14-47-46")
check("cap 120", len(fp("x" * 300)), 120)
check("nothing to keep stays empty", fp("---"), "")

print("\n5. the same hermes_log line from two log files is ONE event")
LOG_TZ = ZoneInfo("Europe/Berlin")
NOW = dt.datetime.now(LOG_TZ).replace(microsecond=0)


def ts(seconds_ago: int) -> str:
    return (NOW - dt.timedelta(seconds=seconds_ago)).strftime("%Y-%m-%d %H:%M:%S")


tmp = Path(tempfile.mkdtemp(prefix="wd-fp-"))
home = tmp / "hermes-home"
(home / "logs").mkdir(parents=True)
(home / "logs" / "errors.log").write_text(
    f"{ts(30)},101 ERROR gateway.run: Session 20260923_144746_ea474d aborted after 604s (/Users/x/a.log)\n")
(home / "logs" / "gateway.error.log").write_text(
    f"{ts(20)},202 ERROR gateway.run: Session 20260924_090000_bb12cc aborted after 12s (/var/log/b.log)\n"
    f"{ts(10)},303 ERROR tools.git: Git command failed (rc=128)\n")
wp.HERMES_HOME = home
wp.HERMES_LOG_FILES = [("fp_offset_errors", "logs/errors.log"), ("fp_offset_gw", "logs/gateway.error.log")]
wp.DB_PATH = tmp / "watchdog.db"
wp._ledger.connect(wp.DB_PATH, migrate=True).close()
conn: sqlite3.Connection = wp.db_connect()
try:
    groups = wp.poll_hermes_logs(conn, NOW, NOW.isoformat())
finally:
    conn.close()
by_id = {g["external_id"]: g for g in groups}
check("two signatures, not three", len(by_id), 2)
session = by_id.get("gateway-run-session-aborted-after-s")
check("session line keyed on its fingerprint", session is not None, True)
check("counted once per file, in one group", session and session["count"], 2)

print("\n6. the same Slack line differing only in timestamp / SHA / number is ONE event")
msgs = [
    {"external_id": "1790000001.000100",
     "title": "Deploy failed for abc1234def (attempt 1) at 2026-09-23 14:47:46 in 604s"},
    {"external_id": "1790000999.000200",
     "title": "Deploy failed for 0fe9876cba (attempt 2) at 2026-09-24 09:00:01 in 12s"},
    {"external_id": "1790001500.000300", "title": "Something else entirely"},
]
slack = {g["external_id"]: g for g in wp.aggregate_slack_batch(msgs)}
check("two groups", len(slack), 2)
deploy = slack.get("deploy-failed-for-attempt-at-in-s")
check("deploy key is the fingerprint", deploy is not None, True)
check("deploy counted twice", deploy and deploy["count"], 2)
check("ts range spans both", deploy and (deploy["payload"]["ts_first"], deploy["payload"]["ts_last"]),
      ("1790000001.000100", "1790000999.000200"))

print()
if failures:
    print(f"{len(failures)} failure(s):")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
print("all cases as expected")
