#!/usr/bin/env python3
"""Regression guard for the GitHub poll that resolved every open issue.

Run: .venv/bin/python3 tests/test_watchdog_github_blindness.py

From 2026-09-09 the poller ran under a LaunchAgent whose PATH had no `gh`.
`poll_github()` caught the FileNotFoundError and returned `[]` per kind —
byte-identical to "no open issues" — so reconcile() resolved all six open
`github_issue` events on the first run and never saw them again.

The fix is `GH_BIN` (absolute) plus a None-per-kind failure that makes the
reconcile step skip. Checked here: a missing binary and a non-zero exit are
None, a real empty result is still `[]`, and a failed search leaves open
events open.

`poll_github()` stopped polling issues entirely in docs/waves/PLAN.md Wave 1
(`github_pr` is the only surviving kind, see `poll_github()`'s own
docstring) — every case below now checks `github_pr` only, but the
regression this file guards is unchanged: a `gh` failure must never be
mistaken for an empty result, for whichever kind this poller still covers.
"""

import datetime as dt
import importlib.util
import subprocess
import sys
import tempfile
from pathlib import Path

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


def fake_run(returncode: int, stdout: str = "[]", raises: Exception | None = None):
    def run(*a, **k):
        if raises is not None:
            raise raises
        return subprocess.CompletedProcess(a[0], returncode, stdout=stdout, stderr="boom")
    return run


original_run = wp.subprocess.run

print("\n1. a missing gh binary is a failure, not an empty result")
wp.subprocess.run = fake_run(0, raises=FileNotFoundError(2, "No such file or directory"))
check("github_pr None", wp.poll_github({}), {"github_pr": None})

print("\n2. a non-zero gh exit is a failure")
wp.subprocess.run = fake_run(1)
check("github_pr None", wp.poll_github({}), {"github_pr": None})

print("\n3. a successful empty search is still []")
wp.subprocess.run = fake_run(0, stdout="[]")
check("github_pr []", wp.poll_github({}), {"github_pr": []})

print("\n4. the argv names the absolute GH_BIN, never a bare `gh`")
seen: list[list[str]] = []
wp.subprocess.run = lambda argv, **k: (seen.append(argv), subprocess.CompletedProcess(argv, 0, stdout="[]", stderr=""))[1]
wp.poll_github({})
check("argv[0] is GH_BIN", {argv[0] for argv in seen}, {str(wp.GH_BIN)})
wp.subprocess.run = original_run

print("\n5. a failed search leaves an open github_pr event open")
tmp = Path(tempfile.mkdtemp(prefix="wd-gh-test-")) / "watchdog.db"
wp.DB_PATH = tmp
wp._ledger.connect(tmp, migrate=True).close()
conn = wp.db_connect()
now = dt.datetime(2026, 9, 9, 12, 5, tzinfo=dt.timezone.utc)
stale = [{"external_id": "jkrumm/basalt-ui#51", "title": "t", "url": "",
          "payload": {"repo": "jkrumm/basalt-ui", "author": "jkrumm"}}]
wp.reconcile(conn, "github_pr", stale, now, 0, wp.REM_HOURS["github_pr"], deliver=False)
conn.commit()

for name in ("poll_uk", "poll_docker", "poll_hermes_cron", "poll_stray_skills"):
    setattr(wp, name, lambda *a, **k: [])
wp.poll_op_refs = lambda *a, **k: ([], False)
wp.poll_hermes_logs = lambda *a, **k: []
wp.poll_slack_messages = lambda *a, **k: ([], None, True)
wp.poll_github = lambda *a, **k: {"github_pr": None}
wp._run_poll(conn, now + dt.timedelta(minutes=30), {}, deliver=False)

row = conn.execute("SELECT resolved_at FROM events WHERE source='github_pr'").fetchone()
check("still unresolved", row["resolved_at"], None)
conn.close()

print()
if failures:
    print(f"{len(failures)} failure(s):")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
print("all cases as expected")
