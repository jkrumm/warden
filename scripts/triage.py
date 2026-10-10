"""The act-loop entry point: `scripts/triage.py --run` is the `com.jkrumm.warden-loop`
LaunchAgent. run() is one pass over the stages in scripts/loop/ (core, intake, triaging,
work, train, verify, notify). DESIGN.md says what the loop is."""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
import sys
from pathlib import Path

# scripts/ on sys.path so `clients`, `lifecycle` and `loop` import as packages.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from clients import kuma  # noqa: E402
from loop import core, intake, notify, triaging, verify, work  # noqa: E402


def run(conn: sqlite3.Connection, *, dry_run: bool) -> int:
    now = dt.datetime.now(dt.timezone.utc)
    policy = core.load_policy()

    # Runs first, before any poller can retry the same external call: an item under an in-flight
    # operation (a crashed process, an ambiguous submit) is reconciled before anything else.
    work.reconcile_operations(conn, policy, now, dry_run=dry_run)

    # `failed` items that were never the work's fault re-enter their stage here, so this pass's
    # pollers pick them up.
    work.redrive_failed(conn, now, dry_run=dry_run)

    intake.ingest(conn, now)
    intake.ingest_github_issues(conn, now)
    intake.reopen_if_needed(conn, now, policy)
    intake.classify(conn, policy, now)
    intake.apply_resolutions(conn, now, policy)
    intake.resolve_recovery_paired(conn, policy, now, dry_run=dry_run)
    intake.resolve_quiet_grouped(conn, policy, now)
    work.maybe_dissolve_clusters(conn, now, dry_run=dry_run)

    # The triage step: fold last tick's finished jobs, submit the ready `new` items, then wait
    # briefly for this tick's own so most are `triaged` before the escalation below.
    triaging.poll_triage_jobs(conn, now, dry_run=dry_run)
    submitted = triaging.submit_triage_jobs(conn, policy, now, dry_run=dry_run)
    triaging.settle_triage_jobs(conn, now, dry_run=dry_run, job_ids=submitted)

    work.escalate_origin_items(conn, now, dry_run=dry_run)
    work.escalate(conn, policy, now, dry_run=dry_run)

    # Before the implement chain: a host-verb row carries a claim in `implement_job`, which
    # maybe_auto_implement() treats as taken, so one pass never picks the same row up twice.
    work.maybe_auto_remediate(conn, policy, now, dry_run=dry_run)

    # Verdict -> implement -> validate -> merge -> deploy -> verify. Each stage polls once per run
    # over its own state; this order lets an item that crossed a stage this run be picked up by the
    # next. Correctness never depends on it: every stage re-derives eligibility from the DB.
    # dispatch-sweep.py also runs the implement chain, on its own 300s cadence.
    work.advance_implement_chain(conn, policy, now, dry_run=dry_run)
    verify.maybe_verify(conn, policy, now, dry_run=dry_run)

    for _key, members in work.cluster_groups(conn).items():
        members = sorted(members, key=lambda r: r["event_id"])
        event_rows = [core.get_event(conn, m["event_id"]) for m in members]
        if any(er is None for er in event_rows):
            continue
        notify.notify_cluster(conn, members, event_rows, policy, dry_run=dry_run)

    notify.maybe_post_daily_digest(conn, policy, now, dry_run=dry_run)

    # No timestamp argument, on purpose: see record_heartbeat().
    record_heartbeat(conn, dry_run=dry_run)

    if not dry_run:
        kuma.ping_loop()

    # Pulls the owner's queued Argo actions before this tick's snapshot reflects their outcome.
    notify.apply_argo_actions(conn, now, dry_run=dry_run)

    # Last step of every pass: the Argo snapshot.
    notify.push_argo_snapshot(conn, now, dry_run=dry_run)
    return 0


HEARTBEAT_CURSOR_KEY = "triage_last_run"


def record_heartbeat(conn: sqlite3.Connection, *, dry_run: bool) -> None:
    """Write one `cursors` row per completed pass, unconditionally.

    Every other write is conditional on a change, so an idle pass leaves no trace and "ran with
    nothing to do" is indistinguishable from "did not run". `updated_at` is the wall-clock end of
    the last COMPLETED pass and `value` is that pass's state census; api.py judges loop
    freshness from it. Skipped under --dry-run, which completes no real pass."""
    if dry_run:
        return
    census = {
        row["state"]: row["n"]
        for row in conn.execute(
            "SELECT state, COUNT(*) AS n FROM triage_items GROUP BY state"
        )
    }
    open_clusters = work.count_open_investigation_clusters(conn)
    value = json.dumps({"states": census, "open_clusters": open_clusters}, sort_keys=True)
    conn.execute(
        "INSERT INTO cursors(key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        # Stamped here from a clock read at write time, and the function takes no timestamp: callers
        # hold the pass's START time, while `updated_at` must mean "when the pass COMPLETED" (api.py
        # checks it against 3x the StartInterval). Without the parameter the wrong stamp cannot be
        # expressed.
        (HEARTBEAT_CURSOR_KEY, value, core.now_iso(dt.datetime.now(dt.timezone.utc))),
    )
    conn.commit()


def _items_for_signature(conn: sqlite3.Connection, signature: str) -> list[sqlite3.Row]:
    """The CLI verbs address a SIGNATURE, not an event: resolve it to rows, then transition each
    through set_state(), the one writer of `state`."""
    return conn.execute("SELECT event_id FROM triage_items WHERE signature=?", (signature,)).fetchall()


def _arg_value(argv: list[str], flag: str) -> str | None:
    if flag not in argv:
        return None
    idx = argv.index(flag)
    return argv[idx + 1] if idx + 1 < len(argv) else None


def cmd_ignore(conn: sqlite3.Connection, argv: list[str], now: dt.datetime) -> int:
    signature = _arg_value(argv, "--ignore")
    if not signature:
        print("triage: --ignore needs a signature", file=sys.stderr)
        return 2
    rows = _items_for_signature(conn, signature)
    for row in rows:
        core.set_state(conn, row["event_id"], core.STATE_CLOSED, now, close_reason=core.CLOSE_IGNORED)
    conn.commit()
    if not rows:
        print(f"triage: no triage item for signature {signature!r}", file=sys.stderr)
        return 1
    print(f"ignored {signature}")
    return 0


def cmd_reopen(conn: sqlite3.Connection, argv: list[str], now: dt.datetime) -> int:
    signature = _arg_value(argv, "--reopen")
    if not signature:
        print("triage: --reopen needs a signature", file=sys.stderr)
        return 2
    rows = _items_for_signature(conn, signature)
    for row in rows:
        core.set_state(conn, row["event_id"], core.STATE_NEW, now)
    conn.commit()
    if not rows:
        print(f"triage: no triage item for signature {signature!r}", file=sys.stderr)
        return 1
    print(f"reopened {signature}")
    return 0


def cmd_close(conn: sqlite3.Connection, argv: list[str], now: dt.datetime) -> int:
    """`closed(resolved)` by hand. A reason is required: a close without one is indistinguishable
    from a bug, and the reason is the whole content of the state."""
    signature = _arg_value(argv, "--close")
    reason = _arg_value(argv, "--reason")
    if not signature:
        print("triage: --close needs a signature", file=sys.stderr)
        return 2
    if not reason or not reason.strip():
        print("triage: --close needs --reason <text>", file=sys.stderr)
        return 2
    rows = _items_for_signature(conn, signature)
    for row in rows:
        core.set_state(conn, row["event_id"], core.STATE_CLOSED, now, note=reason.strip(), close_reason=core.CLOSE_RESOLVED)
    conn.commit()
    if not rows:
        print(f"triage: no triage item for signature {signature!r}", file=sys.stderr)
        return 1
    print(f"closed {signature}: {reason.strip()}")
    return 0


def cmd_list(conn: sqlite3.Connection) -> int:
    rows = conn.execute(
        "SELECT signature, state, repo, verb, occurrences, first_seen FROM triage_items "
        "WHERE state NOT IN (?, ?, ?) ORDER BY updated_at DESC",
        core.TERMINAL_STATES,
    ).fetchall()
    if not rows:
        print("no open triage items")
        return 0
    for r in rows:
        print(f"{r['state']:<15} {r['signature']:<70} repo={r['repo'] or '-'} "
              f"occurrences={r['occurrences']} since={r['first_seen']}")
    return 0


def main(argv: list[str] | None = None) -> int:
    argv = list(argv if argv is not None else sys.argv[1:])
    core.apply_db_override(argv)
    now = dt.datetime.now(dt.timezone.utc)
    conn = core.db_connect()
    try:
        if "--ignore" in argv:
            return cmd_ignore(conn, argv, now)
        if "--reopen" in argv:
            return cmd_reopen(conn, argv, now)
        if "--close" in argv:
            return cmd_close(conn, argv, now)
        if "--list" in argv:
            return cmd_list(conn)
        return run(conn, dry_run="--dry-run" in argv)
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
