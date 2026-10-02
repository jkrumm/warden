#!/usr/bin/env python3
"""Regression suite for the ledger-integration behavior docs/history/state-log.md
§87 added to scripts/dispatch-sweep.py:

  1. `process_dispatch()` stamps `dispatches.finished_at` from sideclaw's own
     job envelope (`job["finishedAt"]`), not from the moment this sweep pass
     happened to observe the terminal status — see clients/sideclaw.py's
     `finished_at_iso()` and its own dedicated tests in test_clients.py.

  2. `main()` calls `triage.advance_implement_chain()` — the same
     verdict -> implement -> review -> merge code
     triage.py's own 600s loop tick calls — once per pass, unconditionally,
     so an item does not wait for that tick to cross its next stage
     boundary. `advance_implement_chain()`'s own behaviour (does it
     correctly dispatch/poll/merge) is test_triage.py's job; this file only
     pins that dispatch-sweep.py's `main()` actually calls it, every pass,
     with `dry_run` threaded through, and that a failure inside it cannot
     take the sweep itself down.

HOUSE CONVENTION, not pytest — see test_lifecycle.py's own docstring for why:
a plain `def test_*(): assert ...` per case, discovered reflectively by this
file's own main().

Run: .venv/bin/python3 tests/test_dispatch_sweep_pipeline.py
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _load(name: str, relpath: str):
    spec = importlib.util.spec_from_file_location(name, REPO / relpath)
    assert spec is not None and spec.loader is not None
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# A SEPARATE load from test_dispatch_sweep.py's own — by-path loading gives
# each test file its own module object, so patching an attribute here can
# never leak into that file's run in the same `make test` pass.
dispatch_sweep = _load("dispatch_sweep_pipeline_under_test", "scripts/dispatch-sweep.py")
_ledger = dispatch_sweep._ledger


def _fresh_db() -> Path:
    path = Path(tempfile.mkdtemp(prefix="dispatch-sweep-pipeline-")) / "warden.db"
    conn = _ledger.connect(path, migrate=True)
    conn.close()
    return path


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class _patch:
    """Swap one attribute on a module object for the duration of a `with`
    block, restored afterward — same shape test_lifecycle.py's `_patch` uses."""

    def __init__(self, obj, name, value):
        self.obj, self.name, self.value = obj, name, value

    def __enter__(self):
        self.original = getattr(self.obj, self.name)
        setattr(self.obj, self.name, self.value)
        return self.value

    def __exit__(self, *exc):
        setattr(self.obj, self.name, self.original)


# --- process_dispatch(): finished_at from sideclaw's own envelope -----------

def test_process_dispatch_finished_at_uses_sideclaws_own_timestamp_not_poll_time():
    db_path = _fresh_db()
    conn = _ledger.connect(db_path)
    try:
        now = _now()
        # sideclaw finished this job hours ago; this sweep pass is only NOW
        # observing it — the exact §79 shape ("a poll suspended overnight").
        finished = now - dt.timedelta(hours=10)
        conn.execute(
            "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at) "
            "VALUES(?,?,?,?,?,?)",
            ("job-late-poll", "investigate", "demo-repo", "b", "running", finished.isoformat()),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM dispatches WHERE job_id='job-late-poll'").fetchone()

        job = {
            "id": "job-late-poll",
            "status": "done",
            "result": {"nextAction": "none", "confidence": "low", "summary": "nothing to do"},
            "finishedAt": int(finished.timestamp() * 1000),
        }
        with _patch(dispatch_sweep, "poll_job", lambda job_id: job):
            dispatch_sweep.process_dispatch(conn, row, dry_run=False)

        stored = conn.execute(
            "SELECT finished_at FROM dispatches WHERE job_id='job-late-poll'"
        ).fetchone()
        recorded = dt.datetime.fromisoformat(stored["finished_at"])
        assert abs((recorded - finished).total_seconds()) < 1, stored["finished_at"]
        assert recorded < now - dt.timedelta(hours=1), (
            "finished_at must not fall back to this late poll's own wall clock")
    finally:
        conn.close()


def test_process_dispatch_finished_at_falls_back_to_now_when_sideclaw_omits_it():
    db_path = _fresh_db()
    conn = _ledger.connect(db_path)
    try:
        conn.execute(
            "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at) "
            "VALUES(?,?,?,?,?,?)",
            ("job-no-finishedat", "investigate", "demo-repo", "b", "running", _now().isoformat()),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM dispatches WHERE job_id='job-no-finishedat'").fetchone()

        job = {"id": "job-no-finishedat", "status": "failed", "result": None, "error": "boom"}
        before = _now()
        with _patch(dispatch_sweep, "poll_job", lambda job_id: job):
            dispatch_sweep.process_dispatch(conn, row, dry_run=False)
        after = _now()

        stored = conn.execute(
            "SELECT finished_at FROM dispatches WHERE job_id='job-no-finishedat'"
        ).fetchone()
        recorded = dt.datetime.fromisoformat(stored["finished_at"])
        assert before <= recorded <= after, stored["finished_at"]
    finally:
        conn.close()


# --- main(): advance_implement_chain() runs once, every pass -----------------

def test_main_calls_advance_implement_chain_once_per_pass_even_with_nothing_to_report():
    db_path = _fresh_db()
    calls: list[dict] = []

    def _fake_advance(conn, policy, now, *, dry_run):
        calls.append({"now": now, "dry_run": dry_run})

    with _patch(dispatch_sweep._triage, "advance_implement_chain", _fake_advance):
        rc = dispatch_sweep.main(["--db", str(db_path)])

    assert rc == 0
    assert len(calls) == 1, "advance_implement_chain must run exactly once per sweep pass"
    assert calls[0]["dry_run"] is False


def test_main_dry_run_passes_dry_run_through_to_advance_implement_chain():
    db_path = _fresh_db()
    calls: list[bool] = []

    def _fake_advance(conn, policy, now, *, dry_run):
        calls.append(dry_run)

    with _patch(dispatch_sweep._triage, "advance_implement_chain", _fake_advance):
        rc = dispatch_sweep.main(["--db", str(db_path), "--dry-run"])

    assert rc == 0
    assert calls == [True]


def test_advance_implement_chain_failure_does_not_crash_the_sweep():
    db_path = _fresh_db()

    def _raise(conn, policy, now, *, dry_run):
        raise RuntimeError("boom")

    with _patch(dispatch_sweep._triage, "advance_implement_chain", _raise):
        rc = dispatch_sweep.main(["--db", str(db_path)])

    assert rc == 0, "one bad pipeline pass must not take the whole sweep down"


def test_advance_implement_chain_runs_after_the_per_row_fold_in_the_same_pass():
    """The whole point: an investigate verdict folded by THIS pass's own
    row loop (fold_dispatch_verdict(), inside process_dispatch()) must be
    visible to advance_implement_chain() in the SAME call, not the next one
    — that ordering is what lets an item cross verdict -> implement
    without waiting for anything else. A `human` verdict is used here because it
    changes the state (working -> needs_decision), which is what the chain's own
    first look has to observe."""
    db_path = _fresh_db()
    conn = _ledger.connect(db_path)
    try:
        now = _now().isoformat()
        conn.execute(
            "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at,origin_event_id) "
            "VALUES(?,?,?,?,?,?,?)",
            ("job-fold-then-chain", "investigate", "demo-repo", "b", "running", now, 1),
        )
        conn.execute(
            "INSERT INTO triage_items(event_id,signature,repo,state,dispatch_job,occurrences,"
            "created_at,updated_at,max_tier) VALUES(?,?,?,?,?,?,?,?,?)",
            (1, "sig-fold-then-chain", "demo-repo", "working", "job-fold-then-chain", 1,
             now, now, "implement"),
        )
        conn.commit()
        row = conn.execute("SELECT * FROM dispatches WHERE job_id='job-fold-then-chain'").fetchone()

        job = {
            "id": "job-fold-then-chain",
            "status": "done",
            "result": {"nextAction": "human", "confidence": "low", "summary": "needs the owner"},
        }
        seen_states: list[str] = []

        def _fake_advance(conn, policy, now, *, dry_run):
            item = conn.execute(
                "SELECT state FROM triage_items WHERE event_id=1"
            ).fetchone()
            seen_states.append(item["state"])

        with _patch(dispatch_sweep, "poll_job", lambda job_id: job), \
             _patch(dispatch_sweep._triage, "advance_implement_chain", _fake_advance):
            dispatch_sweep.main(["--db", str(db_path)])

        assert seen_states == ["needs_decision"], (
            f"advance_implement_chain must observe the fold this SAME pass already made, got {seen_states}")
    finally:
        conn.close()


def test_a_pruned_item_backed_dispatch_is_folded_so_its_item_strikes_instead_of_staying_working():
    """sideclaw pruned an investigation that a triage item was waiting on: the dispatch closes
    ITEM_TRACKED, and the item takes the no-verdict path (a strike back to `triaged` with its
    dispatch handle cleared) rather than sitting `working` forever."""
    db_path = _fresh_db()
    conn = _ledger.connect(db_path)
    try:
        now = _now()
        conn.execute("INSERT INTO events(id, source, external_id, title, first_seen) VALUES (1,'s','e1','t',?)",
                     (now.isoformat(),))
        conn.execute(
            "INSERT INTO triage_items(event_id, signature, repo, state, dispatch_job, occurrences, first_seen, "
            "last_seen, created_at, updated_at) VALUES (1,'s:e1','demo-repo','working','job-pruned',1,?,?,?,?)",
            (now.isoformat(),) * 4)
        conn.execute(
            "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at,origin_event_id,poll_misses) "
            "VALUES('job-pruned','investigate','demo-repo','b','running',?,1,?)",
            (now.isoformat(), dispatch_sweep.LOST_AFTER_MISSES - 1))
        conn.commit()
        row = conn.execute("SELECT * FROM dispatches WHERE job_id='job-pruned'").fetchone()

        with _patch(dispatch_sweep, "poll_job", lambda job_id: dispatch_sweep.NOT_FOUND):
            dispatch_sweep.process_dispatch(conn, row, dry_run=False)

        d = conn.execute("SELECT status, delivery_status, reported_at FROM dispatches "
                         "WHERE job_id='job-pruned'").fetchone()
        assert d["delivery_status"] == dispatch_sweep.ITEM_TRACKED and d["reported_at"], dict(d)
        item = conn.execute("SELECT state, strikes, dispatch_job, retry_at FROM triage_items "
                            "WHERE event_id=1").fetchone()
        assert item["state"] == "triaged" and item["strikes"] == 1, dict(item)
        assert item["dispatch_job"] is None and item["retry_at"], dict(item)
    finally:
        conn.close()


def main() -> int:
    tests = [(name, fn) for name, fn in sorted(globals().items())
             if name.startswith("test_") and callable(fn)]
    passed = 0
    failures: list[str] = []
    for name, fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as e:
            failures.append(f"{name}: {e}")
        except Exception:
            failures.append(f"{name}: unexpected exception\n{traceback.format_exc()}")

    print(f"{passed}/{len(tests)} passed")
    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
