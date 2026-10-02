#!/usr/bin/env python3
"""Regression guard for the two ways the watchdog silently lost an alert, and for what its
`main()` still does now that it posts nothing to Slack (the New/Resolved digest is gone: only
the blindness alarm and the heartbeat remain).

Run: ~/.hermes/hermes-agent/venv/bin/python3 tests/test_watchdog_delivery.py

BUG 1 — a notification stamped during quiet hours was BURNED, not deferred.
`_run_poll` marked rows notified unconditionally, then `compose_slack_body()`
returned "" for quiet/vacation. With a cooldown at or near 24h that is not a
delayed message but a permanently silent one: the next eligible emit lands at the
same wall-clock hour, back inside the same window, forever.

BUG 2 — `upsert_grouped` never cleared `resolved_at`. Once `sweep_stale_grouped()`
retired a signature after 7 idle days, a recurrence kept ticking
`last_reminder_at` on a row that every `resolved_at IS NULL` reader ignores.

Both fired together on 2026-08-24: a Slack socket died at 03:44, its `hermes_log`
signature (24h cooldown, resolved back in June) produced a 48-hour ~17,300-line
reconnect flood, and the digest never mentioned it once.
"""

import contextlib
import datetime as dt
import importlib.util
import io
import shutil
import sqlite3
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


def fresh_db() -> sqlite3.Connection:
    tmp = Path(tempfile.mkdtemp(prefix="wd-test-")) / "watchdog.db"
    wp.DB_PATH = tmp
    # watchdog-poll.py is assert-only (it is not the migrator — see
    # ledger.py); stand in for the loop's own boot-time migrate() so
    # wp.db_connect() below has a schema to assert against.
    wp._ledger.connect(tmp, migrate=True).close()
    return wp.db_connect()


def observed(external_id: str = "sig-a", title: str = "something broke") -> list[dict]:
    return [{"external_id": external_id, "title": title, "url": "", "payload": {}}]


def grouped(external_id: str = "sig-a", count: int = 5) -> list[dict]:
    return [{"external_id": external_id, "title": "slack_bolt: Failed to connect",
             "url": "", "payload": {}, "count": count}]


def notified_at(conn, external_id: str = "sig-a"):
    row = conn.execute("SELECT notified_at FROM events WHERE external_id=?", (external_id,)).fetchone()
    return row["notified_at"] if row else None


# --- --post fixture --------------------------------------------------------
#
# `main(["--post"])` runs the full poll pipeline (UptimeKuma, Docker, GitHub,
# Slack, op-refs ssh probes, …) before it ever reaches the heartbeat — none of
# that belongs in this test and none of it may touch the network here.
# `_run_poll` is stubbed wholesale so main() never gets that far; resolve_secret
# and the UptimeKuma heartbeat are stubbed so nothing reaches 1Password or
# uptime.jkrumm.com. This poller posts nothing to Slack at all (the New/Resolved
# digest is gone), so there is no Slack stub: the module no longer has a way to post.
@contextlib.contextmanager
def post_env(*, run_poll_result=None):
    tmp_dir = Path(tempfile.mkdtemp(prefix="wd-post-test-"))
    tmp_db = tmp_dir / "watchdog.db"
    wp._ledger.connect(tmp_db, migrate=True).close()

    saved = {
        "DB_PATH": wp.DB_PATH,
        "_run_poll": wp._run_poll,
        "resolve_secret": wp.resolve_secret,
        "_push_uptime_heartbeat": wp._push_uptime_heartbeat,
    }

    heartbeats = {"n": 0}
    secrets = {"UPTIME_PUSH_WATCHDOG": "https://push.example/x"}
    result = run_poll_result if run_poll_result is not None else ([], [])

    def _fake_run_poll(conn, now, env, deliver=True):
        return result

    def _fake_resolve_secret(key):
        return secrets.get(key) or ""

    def _fake_heartbeat():
        heartbeats["n"] += 1

    try:
        wp.DB_PATH = tmp_db
        wp._run_poll = _fake_run_poll
        wp.resolve_secret = _fake_resolve_secret
        wp._push_uptime_heartbeat = _fake_heartbeat

        class Ctx:
            def __init__(self):
                self.heartbeats = heartbeats
                self.db_path = tmp_db

        yield Ctx()
    finally:
        for k, v in saved.items():
            setattr(wp, k, v)
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _seed_slack_blind(db_path: Path) -> None:
    """Mark Slack polling as blind (three consecutive failures) so main()
    returns rc=1 — used to prove the heartbeat fires only on a clean run.
    db_path must already be wp.DB_PATH."""
    conn = wp.db_connect()
    wp.cursor_set(conn, "slack_alert_fail_streak", "3", dt.datetime.now(dt.timezone.utc).isoformat())
    conn.commit()
    conn.close()


NEWS = ([{"source": "uk", "title": "monitor down", "url": ""}], [])

print("\n1. reconcile — quiet hours defer rather than burn")
conn = fresh_db()
t0 = dt.datetime(2026, 8, 24, 3, 44, tzinfo=dt.timezone.utc)

new, res = wp.reconcile(conn, "uk", observed(), t0, 0, deliver=False)
check("no NEW emitted while suppressed", len(new), 0)
check("row exists anyway", conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"], 1)
check("notified_at NOT stamped", notified_at(conn), None)

# 07:00, quiet window over — the backlog must fire now.
t1 = t0 + dt.timedelta(hours=3, minutes=16)
new, res = wp.reconcile(conn, "uk", observed(), t1, 0, deliver=True)
check("fires as NEW on first delivering poll", len(new), 1)
check("notified_at stamped once delivered", notified_at(conn) is not None, True)

conn.close()

print("\n2. upsert_grouped — quiet hours defer rather than burn")
conn = fresh_db()
out = wp.upsert_grouped(conn, "hermes_log", grouped(), t0, flap_threshold=1, cooldown_hours=24,
                        deliver=False)
check("no emission while suppressed", len(out), 0)
check("row recorded anyway", conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"], 1)
check("notified_at NOT stamped", notified_at(conn), None)

out = wp.upsert_grouped(conn, "hermes_log", grouped(), t1, flap_threshold=1, cooldown_hours=24,
                        deliver=True)
check("emits on the first delivering poll", len(out), 1)
conn.close()

print("\n3. upsert_grouped — a swept signature re-opens when it recurs")
conn = fresh_db()
june = dt.datetime(2026, 6, 8, 23, 30, tzinfo=dt.timezone.utc)
wp.upsert_grouped(conn, "hermes_log", grouped(), june, flap_threshold=1, cooldown_hours=24)

# 7+ idle days → the sweeper retires it, exactly as it did on 2026-06-23.
swept = wp.sweep_stale_grouped(conn, june + dt.timedelta(days=10))
check("sweeper resolved the idle row", swept, 1)

# The identical error returns two months later.
aug = dt.datetime(2026, 8, 24, 12, 0, tzinfo=dt.timezone.utc)
out = wp.upsert_grouped(conn, "hermes_log", grouped(), aug, flap_threshold=1, cooldown_hours=24)
row = conn.execute("SELECT resolved_at, reminder_count, first_seen FROM events "
                   "WHERE external_id='sig-a'").fetchone()
check("resolved_at cleared on recurrence", row["resolved_at"], None)
check("visible to `resolved_at IS NULL` readers", row["resolved_at"] is None, True)
check("re-emitted", len(out), 1)
check("first_seen re-anchored to the recurrence", row["first_seen"], aug.isoformat())
check("reminder_count reset", row["reminder_count"], 0)
conn.close()

print("\n4. delivering polls are unchanged (the default path)")
conn = fresh_db()
new, res = wp.reconcile(conn, "uk", observed(), t0, 0)
check("reconcile still emits NEW by default", len(new), 1)
out = wp.upsert_grouped(conn, "hermes_log", grouped(), t0, flap_threshold=1, cooldown_hours=24)
check("upsert_grouped still emits by default", len(out), 1)
conn.close()

print("\n5. resolution still tracked while suppressed")
conn = fresh_db()
wp.reconcile(conn, "uk", observed(), t0, 0, deliver=True)
# The condition clears during quiet hours — the row must still close.
wp.reconcile(conn, "uk", [], t0 + dt.timedelta(hours=1), 0, deliver=False)
row = conn.execute("SELECT resolved_at FROM events WHERE external_id='sig-a'").fetchone()
check("disappearance still resolves under suppression", row["resolved_at"] is not None, True)
conn.close()

print("\n6. a poll with news posts nothing to Slack and still heartbeats")
check("no digest composer left", hasattr(wp, "compose_slack_body"), False)
check("no Slack post path left", hasattr(wp, "post_text"), False)
with post_env(run_poll_result=NEWS) as ctx:
    rc = wp.main(["--post"])
    check("rc == 0", rc, 0)
    # The monitor this feeds answers "is the poller still running", not "did it have
    # news": a quiet half hour is the normal case and must still ping.
    check("heartbeat fires on a clean run", ctx.heartbeats["n"], 1)

print("\n7. no --post, no heartbeat")
with post_env(run_poll_result=NEWS) as ctx:
    rc = wp.main([])
    check("rc == 0", rc, 0)
    check("zero heartbeat calls", ctx.heartbeats["n"], 0)

print("\n8. --post --dry-run makes zero heartbeat calls")
with post_env(run_poll_result=NEWS) as ctx:
    # main()'s --dry-run branch still runs the full temp-copy-then-poll path
    # (see main(): "Run poll against a temp copy of the DB…") — a clean rc here is
    # evidence that path completed.
    rc = wp.main(["--post", "--dry-run"])
    check("rc == 0", rc, 0)
    check("zero heartbeat calls under dry-run", ctx.heartbeats["n"], 0)

print("\n9. the heartbeat fires on rc == 0 and not while Slack polling is blind")
with post_env(run_poll_result=NEWS) as ctx:
    _seed_slack_blind(ctx.db_path)
    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        rc = wp.main(["--post"])
    check("rc != 0 (slack polling blind)", rc == 0, False)
    check("the blindness alarm is still reported", "Slack polling is blind" in stderr.getvalue(), True)
    check("no heartbeat on a non-zero rc", ctx.heartbeats["n"], 0)

print("\n10. ledger behind this process's schema, --post — pass skipped, heartbeat still fires")
with post_env(run_poll_result=NEWS) as ctx:
    behind_conn = sqlite3.connect(ctx.db_path)
    behind_conn.execute("UPDATE schema_version SET version = ?", (wp._ledger.LEDGER_SCHEMA_VERSION - 1,))
    behind_conn.commit()
    behind_conn.close()

    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        rc = wp.main(["--post"])
    check("rc == 0", rc, 0)
    # The Kuma monitor measures "is the poller alive", not "did the ledger open" —
    # a pass deliberately skipped during a migration window is a live poller.
    check("heartbeat still fires", ctx.heartbeats["n"], 1)
    check("stderr names the skip", "ledger behind this process's schema" in stderr.getvalue(), True)

print("\n11. ledger behind this process's schema, no --post — heartbeat NOT pushed")
with post_env(run_poll_result=NEWS) as ctx:
    behind_conn = sqlite3.connect(ctx.db_path)
    behind_conn.execute("UPDATE schema_version SET version = ?", (wp._ledger.LEDGER_SCHEMA_VERSION - 1,))
    behind_conn.commit()
    behind_conn.close()

    rc = wp.main([])
    check("rc == 0", rc, 0)
    check("heartbeat NOT pushed without --post", ctx.heartbeats["n"], 0)

print()
if failures:
    print(f"{len(failures)} failure(s):")
    for f in failures:
        print(f"  - {f}")
    sys.exit(1)
print("all cases as expected")
