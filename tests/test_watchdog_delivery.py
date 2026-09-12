#!/usr/bin/env python3
"""Regression guard for the two ways the watchdog silently lost an alert.

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
# Slack, op-refs ssh probes, …) before it ever reaches delivery — none of
# that belongs in a delivery-path test and none of it may touch the network
# here. `_run_poll` is stubbed wholesale so main() never gets that far;
# in_quiet_hours/vacation_active are stubbed too so the suppression decision
# is deterministic regardless of wall-clock time; post_text, resolve_secret,
# and the UptimeKuma heartbeat are stubbed so nothing reaches Slack, 1Password,
# or uptime.jkrumm.com.
@contextlib.contextmanager
def post_env(*, run_poll_result=None, quiet: bool = False, vacation: bool = False,
             token: str | None = "test-slack-token", post_ok: bool = True):
    tmp_dir = Path(tempfile.mkdtemp(prefix="wd-post-test-"))
    tmp_db = tmp_dir / "watchdog.db"
    wp._ledger.connect(tmp_db, migrate=True).close()

    saved = {
        "DB_PATH": wp.DB_PATH,
        "_run_poll": wp._run_poll,
        "in_quiet_hours": wp.in_quiet_hours,
        "vacation_active": wp.vacation_active,
        "post_text": wp.post_text,
        "resolve_secret": wp.resolve_secret,
        "_push_uptime_heartbeat": wp._push_uptime_heartbeat,
    }

    posts: list[dict] = []
    heartbeats = {"n": 0}
    secrets = {"SLACK_BOT_TOKEN": token, "UPTIME_PUSH_WATCHDOG": "https://push.example/x"}
    result = run_poll_result if run_poll_result is not None else ([], [], [])

    def _fake_run_poll(conn, now, env, deliver=True):
        return result

    def _fake_post_text(channel, text, tok):
        posts.append({"channel": channel, "text": text, "token": tok})
        return post_ok, ("1000.000001" if post_ok else None)

    def _fake_resolve_secret(key):
        return secrets.get(key) or ""

    def _fake_heartbeat():
        heartbeats["n"] += 1

    try:
        wp.DB_PATH = tmp_db
        wp._run_poll = _fake_run_poll
        wp.in_quiet_hours = lambda state: quiet
        wp.vacation_active = lambda state: vacation
        wp.post_text = _fake_post_text
        wp.resolve_secret = _fake_resolve_secret
        wp._push_uptime_heartbeat = _fake_heartbeat

        class Ctx:
            def __init__(self):
                self.posts = posts
                self.heartbeats = heartbeats
                self.db_path = tmp_db

        yield Ctx()
    finally:
        for k, v in saved.items():
            setattr(wp, k, v)
        shutil.rmtree(tmp_dir, ignore_errors=True)


def _seed_slack_blind(db_path: Path) -> None:
    """Mark Slack polling as blind (three consecutive failures) so main()
    returns rc=1 without any post_text failure — used to prove the heartbeat
    fires only on a clean run. db_path must already be wp.DB_PATH."""
    conn = wp.db_connect()
    wp.cursor_set(conn, "slack_alert_fail_streak", "3", dt.datetime.now(dt.timezone.utc).isoformat())
    conn.commit()
    conn.close()


print("\n1. reconcile — quiet hours defer rather than burn")
conn = fresh_db()
t0 = dt.datetime(2026, 8, 24, 3, 44, tzinfo=dt.timezone.utc)

new, rem, res = wp.reconcile(conn, "uk", observed(), t0, 0, 6, deliver=False)
check("no NEW emitted while suppressed", len(new), 0)
check("row exists anyway", conn.execute("SELECT COUNT(*) c FROM events").fetchone()["c"], 1)
check("notified_at NOT stamped", notified_at(conn), None)

# 07:00, quiet window over — the backlog must fire now.
t1 = t0 + dt.timedelta(hours=3, minutes=16)
new, rem, res = wp.reconcile(conn, "uk", observed(), t1, 0, 6, deliver=True)
check("fires as NEW on first delivering poll", len(new), 1)
check("notified_at stamped once delivered", notified_at(conn) is not None, True)

# A reminder must likewise not consume its anchor while suppressed.
t2 = t1 + dt.timedelta(hours=7)
new, rem, res = wp.reconcile(conn, "uk", observed(), t2, 0, 6, deliver=False)
check("no reminder emitted while suppressed", len(rem), 0)
anchor = conn.execute("SELECT last_reminder_at FROM events WHERE external_id='sig-a'").fetchone()
check("reminder anchor untouched", anchor["last_reminder_at"], None)
new, rem, res = wp.reconcile(conn, "uk", observed(), t2, 0, 6, deliver=True)
check("reminder fires on the next delivering poll", len(rem), 1)
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
new, rem, res = wp.reconcile(conn, "uk", observed(), t0, 0, 6)
check("reconcile still emits NEW by default", len(new), 1)
out = wp.upsert_grouped(conn, "hermes_log", grouped(), t0, flap_threshold=1, cooldown_hours=24)
check("upsert_grouped still emits by default", len(out), 1)
conn.close()

print("\n5. resolution still tracked while suppressed")
conn = fresh_db()
wp.reconcile(conn, "uk", observed(), t0, 0, 6, deliver=True)
# The condition clears during quiet hours — the row must still close.
wp.reconcile(conn, "uk", [], t0 + dt.timedelta(hours=1), 0, 6, deliver=False)
row = conn.execute("SELECT resolved_at FROM events WHERE external_id='sig-a'").fetchone()
check("disappearance still resolves under suppression", row["resolved_at"] is not None, True)
conn.close()

print("\n6. --post with a non-empty body posts once, right channel and body")
with post_env(run_poll_result=([{"source": "uk", "title": "monitor down", "url": ""}], [], [])) as ctx:
    rc = wp.main(["--post"])
    check("rc == 0", rc, 0)
    check("post_text called exactly once", len(ctx.posts), 1)
    check("posted to WATCHDOG_CHANNEL", ctx.posts[0]["channel"], wp.WATCHDOG_CHANNEL)
    check("posted body is non-empty", bool(ctx.posts[0]["text"]), True)
    check("no heartbeat call skipped on clean run", ctx.heartbeats["n"], 1)

print("\n7. --post with an empty body (quiet hours) posts nothing")
with post_env(run_poll_result=([{"source": "uk", "title": "monitor down", "url": ""}], [], []),
              quiet=True) as ctx:
    rc = wp.main(["--post"])
    check("rc == 0", rc, 0)
    check("zero Slack calls", len(ctx.posts), 0)
    # NOT zero. The monitor this feeds answers "is the poller still running", not
    # "did it have news", and a quiet half hour is the normal case. Skipping the
    # ping here pages on every silent poll and trains the alert to be ignored —
    # which is how eleven days of blindness go unnoticed. The retiring wrapper
    # pinged on rc == 0 regardless of whether a body was printed.
    check("heartbeat STILL fires on a quiet poll", ctx.heartbeats["n"], 1)

print("\n8. --post --dry-run makes zero Slack calls and zero heartbeat calls")
with post_env(run_poll_result=([{"source": "uk", "title": "monitor down", "url": ""}], [], [])) as ctx:
    # main()'s --dry-run branch still runs the full temp-copy-then-poll path
    # (see main(): "Run poll against a temp copy of the DB…") before it ever
    # reaches delivery — a clean rc here is evidence that path completed.
    rc = wp.main(["--post", "--dry-run"])
    check("rc == 0", rc, 0)
    check("zero Slack calls under dry-run", len(ctx.posts), 0)
    check("zero heartbeat calls under dry-run", ctx.heartbeats["n"], 0)

print("\n9. an unresolvable token returns non-zero and posts nothing")
with post_env(run_poll_result=([{"source": "uk", "title": "monitor down", "url": ""}], [], []),
              token=None) as ctx:
    rc = wp.main(["--post"])
    check("rc != 0", rc == 0, False)
    check("zero Slack calls", len(ctx.posts), 0)
    check("zero heartbeat calls", ctx.heartbeats["n"], 0)

print("\n10. a failing post_text returns non-zero")
with post_env(run_poll_result=([{"source": "uk", "title": "monitor down", "url": ""}], [], []),
              post_ok=False) as ctx:
    rc = wp.main(["--post"])
    check("rc != 0", rc == 0, False)
    check("post_text was still called once", len(ctx.posts), 1)
    check("no heartbeat on a failed post", ctx.heartbeats["n"], 0)

print("\n11. the heartbeat fires on rc == 0 and not on a non-zero rc")
with post_env(run_poll_result=([{"source": "uk", "title": "monitor down", "url": ""}], [], [])) as ctx:
    _seed_slack_blind(ctx.db_path)
    rc = wp.main(["--post"])
    check("rc != 0 (slack polling blind)", rc == 0, False)
    check("post still delivered (digest itself unaffected)", len(ctx.posts), 1)
    check("no heartbeat on a non-zero rc", ctx.heartbeats["n"], 0)

print("\n12. --slack-body alone still prints to stdout and posts nothing")
with post_env(run_poll_result=([{"source": "uk", "title": "monitor down", "url": ""}], [], [])) as ctx:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        rc = wp.main(["--slack-body"])
    check("rc == 0", rc, 0)
    check("body printed to stdout", bool(buf.getvalue().strip()), True)
    check("zero Slack calls", len(ctx.posts), 0)
    check("zero heartbeat calls", ctx.heartbeats["n"], 0)

print("\n13. ledger behind this process's schema, --post — pass skipped, heartbeat still fires")
with post_env(run_poll_result=([{"source": "uk", "title": "monitor down", "url": ""}], [], [])) as ctx:
    behind_conn = sqlite3.connect(ctx.db_path)
    behind_conn.execute("UPDATE schema_version SET version = ?", (wp._ledger.LEDGER_SCHEMA_VERSION - 1,))
    behind_conn.commit()
    behind_conn.close()

    stderr = io.StringIO()
    with contextlib.redirect_stderr(stderr):
        rc = wp.main(["--post"])
    check("rc == 0", rc, 0)
    check("zero Slack calls (pass skipped before delivery)", len(ctx.posts), 0)
    # The Kuma monitor measures "is the poller alive", not "did the ledger open" —
    # a pass deliberately skipped during a migration window is a live poller.
    check("heartbeat still fires", ctx.heartbeats["n"], 1)
    check("stderr names the skip", "ledger behind this process's schema" in stderr.getvalue(), True)

print("\n14. ledger behind this process's schema, no --post — heartbeat NOT pushed")
with post_env(run_poll_result=([{"source": "uk", "title": "monitor down", "url": ""}], [], [])) as ctx:
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
