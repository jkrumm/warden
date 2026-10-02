#!/usr/bin/env python3
"""Regression suite for scripts/lifecycle/{policy,dispatch}.py —
the Wave 5.2 port of the retired bash CLI's policy/dispatch half into
Python modules the loop calls as functions.

Every sideclaw call is faked by assigning `clients.sideclaw.submit` (the
import-style the brief specifies: `from clients import sideclaw`, called as
`sideclaw.submit(...)` at call time, so a test can inject a fake without any
HTTP stub).

Run: .venv/bin/python3 tests/test_lifecycle.py  (or: make test, from warden/)
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import sys
import tempfile
import traceback
import importlib.util
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from lifecycle import dispatch, operations, policy  # noqa: E402
from clients import sideclaw  # noqa: E402
from clients.errors import PolicyError, PreconditionError, RemoteError, SubmitRefused, UsageError  # noqa: E402

_ledger_spec = importlib.util.spec_from_file_location("ledger", REPO / "scripts" / "ledger.py")
ledger = importlib.util.module_from_spec(_ledger_spec)
_ledger_spec.loader.exec_module(ledger)


# --- fixtures & helpers -------------------------------------------------------

def _tmp_dir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix))


def _fresh_ledger():
    path = _tmp_dir("lifecycle-db-") / "warden.db"
    return ledger.connect(path, migrate=True), path


def _now() -> dt.datetime:
    return dt.datetime.now(dt.timezone.utc)


class _patch:
    """Swap one attribute on a module object for the duration of a `with`
    block — the monkeypatch shape the brief specifies: `sideclaw.submit =
    fake`, restored afterward so tests cannot leak into one another."""

    def __init__(self, obj, name: str, value):
        self.obj, self.name, self.value = obj, name, value

    def __enter__(self):
        self.original = getattr(self.obj, self.name)
        setattr(self.obj, self.name, self.value)
        return self.value

    def __exit__(self, *exc):
        setattr(self.obj, self.name, self.original)


class _env:
    """Set (or, with `None`, force-unset) environment variables for one
    `with` block, restoring exactly what was there before."""

    def __init__(self, **kv):
        self.kv = kv

    def __enter__(self):
        self.original = {k: os.environ.get(k) for k in self.kv}
        for k, v in self.kv.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v
        return self

    def __exit__(self, *exc):
        for k, v in self.original.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _raiser(exc: Exception):
    def _f(**kwargs):
        raise exc
    return _f


def _write_json(data) -> Path:
    p = _tmp_dir("lifecycle-json-") / "f.json"
    p.write_text(json.dumps(data), encoding="utf-8")
    return p


def _seed_triage_item(conn, event_id, *, repo, state, dispatch_job=None, max_tier=None):
    now = _now().isoformat()
    if max_tier is None:
        conn.execute(
            "INSERT INTO triage_items(event_id, signature, repo, state, dispatch_job, occurrences, "
            "created_at, updated_at) VALUES (?,?,?,?,?,?,?,?)",
            (event_id, f"sig-{event_id}", repo, state, dispatch_job, 0, now, now),
        )
    else:
        conn.execute(
            "INSERT INTO triage_items(event_id, signature, repo, state, dispatch_job, occurrences, "
            "created_at, updated_at, max_tier) VALUES (?,?,?,?,?,?,?,?,?)",
            (event_id, f"sig-{event_id}", repo, state, dispatch_job, 0, now, now, max_tier),
        )
    conn.commit()


def _seed_dispatch_row(conn, job_id, *, status="done", verdict=None, repo="warden", tier="implement"):
    now = _now().isoformat()
    conn.execute(
        "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at,verdict_json) "
        "VALUES (?,?,?,?,?,?,?)",
        (job_id, tier, repo, "b", status, now, json.dumps(verdict) if verdict is not None else None),
    )
    conn.commit()


def _seed_dispatch(conn, job_id, *, status="running", tier="investigate", repo="warden",
                    created_at=None, reported_at=None, verdict_json=None):
    now = created_at or _now().isoformat()
    conn.execute(
        "INSERT INTO dispatches(job_id,tier,repo,brief,status,created_at,reported_at,verdict_json) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (job_id, tier, repo, "some brief", status, now, reported_at, verdict_json),
    )
    conn.commit()


def _expect(exc_type, fn, *args, **kwargs):
    try:
        fn(*args, **kwargs)
    except exc_type:
        return
    raise AssertionError(f"expected {exc_type.__name__} from {fn}")


# --- policy: repo_cwd() ---------------------------------------
# No repo/tier policy lives here any more: sideclaw is the only boundary. These
# only pin the one path the wire protocol still needs and its name-shape guard.

def test_repo_cwd_is_root_slash_name():
    with _env(WARDEN_REPOS_ROOT="/tmp/repos-root"):
        assert policy.repo_cwd("warden") == Path("/tmp/repos-root/warden")


def test_repo_cwd_default_root_is_source_root():
    with _env(WARDEN_REPOS_ROOT=None):
        assert policy.repo_cwd("warden") == Path(os.path.expanduser("~/SourceRoot")) / "warden"


def test_repo_cwd_does_not_check_existence_or_allowlist():
    """Whether the repo exists / may be dispatched is sideclaw's call (a 4xx)."""
    with _env(WARDEN_REPOS_ROOT="/nonexistent-root"):
        assert policy.repo_cwd("homelab-private") == Path("/nonexistent-root/homelab-private")


def test_repo_cwd_rejects_dot_dotdot_hidden_and_traversal_names():
    for bad in ("", ".", "..", ".hidden", "a/b", "../x", "a b", "x\n"):
        _expect(UsageError, policy.repo_cwd, bad)


# --- policy: valid_origin() ----------------------------------------------------

def test_valid_origin_accepts_good_shapes_and_no_origin_at_all():
    policy.valid_origin(channel="C1234ABCD", thread_ts="1234567890.123456", event_id=42)
    policy.valid_origin()


def test_valid_origin_rejects_lowercase_channel():
    _expect(UsageError, policy.valid_origin, channel="clower")


def test_valid_origin_rejects_channel_not_starting_with_c():
    _expect(UsageError, policy.valid_origin, channel="D1234")


def test_valid_origin_rejects_bad_thread_ts():
    _expect(UsageError, policy.valid_origin, channel="C123", thread_ts="not-a-ts")


def test_valid_origin_thread_needs_channel():
    try:
        policy.valid_origin(thread_ts="123.456")
    except UsageError as e:
        assert "needs --origin-channel" in str(e), e
    else:
        raise AssertionError("expected UsageError")


def test_valid_origin_rejects_non_digit_event_id_string():
    _expect(UsageError, policy.valid_origin, event_id="abc")


def test_valid_origin_accepts_int_event_id():
    policy.valid_origin(event_id=7)


def test_valid_origin_accepts_digit_string_event_id():
    policy.valid_origin(event_id="7")


# --- policy: require_auto_from_item() ------------------------------------------

def test_require_auto_from_item_rejects_non_implement_tier():
    conn, _ = _fresh_ledger()
    try:
        policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="investigate")
    except UsageError as e:
        assert "only valid with --tier implement" in str(e), e
    else:
        raise AssertionError("expected UsageError")


def test_require_auto_from_item_rejects_non_integer_event_id():
    conn, _ = _fresh_ledger()
    try:
        policy.require_auto_from_item(conn, event_id="abc", repo="warden", tier="implement")
    except UsageError as e:
        assert "integer" in str(e), e
    else:
        raise AssertionError("expected UsageError")


def test_require_auto_from_item_norow():
    conn, _ = _fresh_ledger()
    try:
        policy.require_auto_from_item(conn, event_id=999, repo="warden", tier="implement")
    except PolicyError as e:
        assert "no triage_items row" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_require_auto_from_item_wrong_state():
    conn, _ = _fresh_ledger()
    _seed_triage_item(conn, 1, repo="warden", state="needs_decision")
    try:
        policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="implement")
    except PolicyError as e:
        assert "not 'working'" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_require_auto_from_item_repo_mismatch():
    conn, _ = _fresh_ledger()
    _seed_triage_item(conn, 1, repo="other", state="working")
    try:
        policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="implement")
    except PolicyError as e:
        assert "own recorded repo" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_require_auto_from_item_rejects_investigate_ceiling():
    conn, _ = _fresh_ledger()
    _seed_triage_item(conn, 1, repo="warden", state="working", max_tier="investigate")
    try:
        policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="implement")
    except PolicyError as e:
        assert "max_tier='investigate'" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_require_auto_from_item_no_dispatch_job():
    conn, _ = _fresh_ledger()
    _seed_triage_item(conn, 1, repo="warden", state="working", dispatch_job=None)
    try:
        policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="implement")
    except PolicyError as e:
        assert "no linked dispatch_job" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_require_auto_from_item_no_dispatch_record():
    conn, _ = _fresh_ledger()
    _seed_triage_item(conn, 1, repo="warden", state="working", dispatch_job="job-ghost")
    try:
        policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="implement")
    except PolicyError as e:
        assert "no record in" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_require_auto_from_item_not_done():
    conn, _ = _fresh_ledger()
    _seed_dispatch_row(conn, "job-1", status="running")
    _seed_triage_item(conn, 1, repo="warden", state="working", dispatch_job="job-1")
    try:
        policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="implement")
    except PolicyError as e:
        assert "not 'done'" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_require_auto_from_item_no_verdict():
    conn, _ = _fresh_ledger()
    _seed_dispatch_row(conn, "job-1", status="done", verdict=None)
    _seed_triage_item(conn, 1, repo="warden", state="working", dispatch_job="job-1")
    try:
        policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="implement")
    except PolicyError as e:
        assert "no parseable verdict" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_require_auto_from_item_next_action_not_implement():
    conn, _ = _fresh_ledger()
    _seed_dispatch_row(conn, "job-1", status="done", verdict={"nextAction": "human", "confidence": "high"})
    _seed_triage_item(conn, 1, repo="warden", state="working", dispatch_job="job-1")
    try:
        policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="implement")
    except PolicyError as e:
        assert "nextAction='human'" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_require_auto_from_item_accepts_implement_and_issue_at_any_confidence():
    for confidence in ("low", "medium", "high"):
        conn, _ = _fresh_ledger()
        _seed_dispatch_row(conn, "job-1", status="done", verdict={"nextAction": "implement", "confidence": confidence})
        _seed_triage_item(conn, 1, repo="warden", state="working", dispatch_job="job-1")
        assert policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="implement") == "job-1"
    conn, _ = _fresh_ledger()
    _seed_dispatch_row(conn, "job-1", status="done", verdict={"nextAction": "issue", "confidence": "low"})
    _seed_triage_item(conn, 1, repo="warden", state="working", dispatch_job="job-1")
    assert policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="implement") == "job-1"


def test_require_auto_from_item_positive():
    conn, _ = _fresh_ledger()
    _seed_dispatch_row(conn, "job-1", status="done", verdict={"nextAction": "implement", "confidence": "high"})
    _seed_triage_item(conn, 1, repo="warden", state="working", dispatch_job="job-1")
    assert policy.require_auto_from_item(conn, event_id=1, repo="warden", tier="implement") == "job-1"


# --- policy: check_repo_not_in_flight() -----------------------------------------

def test_check_repo_not_in_flight_passes_when_clear():
    conn, _ = _fresh_ledger()
    policy.check_repo_not_in_flight(conn, repo="warden")


def test_check_repo_not_in_flight_via_a_working_item_with_an_implement_job():
    conn, _ = _fresh_ledger()
    _seed_triage_item(conn, 1, repo="warden", state="working")
    policy.check_repo_not_in_flight(conn, repo="warden")   # an investigation is not an implement episode
    conn.execute("UPDATE triage_items SET implement_job='job-impl' WHERE event_id=1")
    conn.commit()
    try:
        policy.check_repo_not_in_flight(conn, repo="warden")
    except PolicyError as e:
        assert "already has an implement episode in flight" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_check_repo_not_in_flight_via_merging_state():
    conn, _ = _fresh_ledger()
    _seed_triage_item(conn, 1, repo="warden", state="merging")
    _expect(PolicyError, policy.check_repo_not_in_flight, conn, repo="warden")
    policy.check_repo_not_in_flight(conn, repo="warden", exclude_event_id=1)


def test_check_repo_not_in_flight_via_open_operation():
    conn, _ = _fresh_ledger()
    operations.record(conn, event_id=None, kind="implement", repo="warden", authorized_by="test")
    try:
        policy.check_repo_not_in_flight(conn, repo="warden")
    except PolicyError as e:
        assert "operation" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_check_repo_not_in_flight_ignores_completed_operation():
    conn, _ = _fresh_ledger()
    op_id = operations.record(conn, event_id=None, kind="implement", repo="warden", authorized_by="test")
    operations.complete(conn, op_id, outcome="done")
    policy.check_repo_not_in_flight(conn, repo="warden")


# --- policy: triage_repo_entry() ---------------------------------------------


def test_triage_repo_entry_returns_entry():
    p = _write_json({"repos": {"vps": {"autoDeploy": True}}})
    assert policy.triage_repo_entry("vps", p) == {"autoDeploy": True}


def test_triage_repo_entry_returns_empty_for_unknown_repo():
    p = _write_json({"repos": {"vps": {}}})
    assert policy.triage_repo_entry("other", p) == {}


def test_triage_repo_entry_unreadable_raises_precondition():
    missing = _tmp_dir("lifecycle-missing2-") / "nope.json"
    _expect(PreconditionError, policy.triage_repo_entry, "vps", missing)


# --- dispatch: normalize_brief() / check_context() ------------------------------

def test_normalize_brief_strips_trailing_whitespace():
    # Per-line trailing whitespace is stripped (matching the bash `sed -e
    # 's/[[:space:]]*$//'`), but a trailing newline in the input still
    # produces a trailing empty line in the joined output — the same shape
    # the shell version produces, since sed never deletes the newline byte
    # itself.
    out = dispatch.normalize_brief("line one   \nline two\t\n")
    assert out == "line one\nline two\n", repr(out)


def test_normalize_brief_empty_raises():
    try:
        dispatch.normalize_brief("   \n  \n")
    except UsageError as e:
        assert "empty" in str(e), e
    else:
        raise AssertionError("expected UsageError")


def test_normalize_brief_oversize_raises():
    try:
        dispatch.normalize_brief("x" * (dispatch.MAX_BRIEF_CHARS + 1))
    except UsageError as e:
        assert "limit" in str(e), e
    else:
        raise AssertionError("expected UsageError")


def test_check_context_none_passthrough():
    assert dispatch.check_context(None) is None


def test_check_context_within_limit_passthrough():
    assert dispatch.check_context("hello") == "hello"


def test_check_context_oversize_raises():
    try:
        dispatch.check_context("x" * (dispatch.MAX_CONTEXT_CHARS + 1))
    except UsageError as e:
        assert "limit" in str(e), e
    else:
        raise AssertionError("expected UsageError")


# --- dispatch: open_episode() ----------------------------------------------------

def test_open_episode_ungated_has_no_operation_row():
    conn, _ = _fresh_ledger()
    with _patch(sideclaw, "submit", lambda **kw: {"id": "j-invest", "status": "running"}):
        opened = dispatch.open_episode(
            conn, repo="warden", tier="investigate", brief="do it", context=None,
            why=None, model=None, origin=dispatch.Origin(), authorized_by=None,
        )
    assert opened.op_id is None
    assert conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 0


def test_open_episode_per_repo_lock_race_two_connections():
    """check_repo_not_in_flight() is a bare SELECT; the operations row that
    IS the lock is written after it returns — two connections could both
    pass the check before either committed. open_episode()'s BEGIN IMMEDIATE
    closes that: while the first connection holds the write lock
    (uncommitted), a second connection's open_episode() for the same repo
    must fail loudly rather than record a second in-flight operation. Once
    the first commits, the second refuses on the ordinary PolicyError path
    instead (the lock is gone, but the operations row it left behind is now
    visible)."""
    conn1, path = _fresh_ledger()
    conn1.execute("BEGIN IMMEDIATE")
    policy.check_repo_not_in_flight(conn1, repo="warden")
    operations.record(conn1, event_id=None, kind="implement", repo="warden", authorized_by="U1", commit=False)
    # conn1 now holds sqlite's write lock, uncommitted — the exact window
    # the old bare-SELECT check could race through.

    conn2 = ledger.connect(path, migrate=False)
    conn2.execute("PRAGMA busy_timeout=200")  # fail fast rather than hang the suite
    try:
        dispatch.open_episode(
            conn2, repo="warden", tier="implement", brief="do it", context=None, why="w",
            model=None, origin=dispatch.Origin(), authorized_by="U2",
        )
    except sqlite3.OperationalError as e:
        assert "locked" in str(e).lower(), e
    else:
        raise AssertionError("expected sqlite3.OperationalError: database is locked")
    finally:
        conn2.close()

    conn1.commit()
    assert conn1.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 1, (
        "the first connection's operation must have landed")

    # Second attempt, after the first committed: the lock is gone, but the
    # operations row it left behind now makes the ordinary check refuse.
    conn3 = ledger.connect(path, migrate=False)
    try:
        try:
            dispatch.open_episode(
                conn3, repo="warden", tier="implement", brief="do it", context=None, why="w",
                model=None, origin=dispatch.Origin(), authorized_by="U3",
            )
        except PolicyError as e:
            assert "in flight" in str(e), e
        else:
            raise AssertionError("expected PolicyError: already in flight")
    finally:
        conn3.close()
    assert conn1.execute("SELECT COUNT(*) FROM operations").fetchone()[0] == 1, (
        "a second connection must never record a second in-flight operation for the same repo"
    )


def test_open_episode_gated_requires_authorized_by():
    conn, _ = _fresh_ledger()
    try:
        dispatch.open_episode(
            conn, repo="warden", tier="implement", brief="do it", context=None, why="w",
            model=None, origin=dispatch.Origin(), authorized_by=None,
        )
    except ValueError as e:
        assert "authorized_by" in str(e), e
    else:
        raise AssertionError("expected ValueError")


def test_open_episode_gated_records_operation_before_submit():
    conn, _ = _fresh_ledger()
    seen = {}

    def fake_submit(**kwargs):
        seen["ops"] = conn.execute("SELECT COUNT(*) FROM operations").fetchone()[0]
        return {"id": "j-impl", "status": "running"}

    with _patch(sideclaw, "submit", fake_submit):
        opened = dispatch.open_episode(
            conn, repo="warden", tier="implement", brief="do it", context=None, why="w",
            model=None, origin=dispatch.Origin(), authorized_by="U1",
        )
    assert seen["ops"] == 1, "the operation row must exist before submit() is called"
    op = conn.execute("SELECT * FROM operations WHERE op_id=?", (opened.op_id,)).fetchone()
    assert op["outcome"] == "done"
    assert json.loads(op["receipt_json"])["jobId"] == "j-impl"


def test_open_episode_remote_error_marks_operation_failed():
    conn, _ = _fresh_ledger()
    with _patch(sideclaw, "submit", _raiser(RemoteError("boom"))):
        try:
            dispatch.open_episode(
                conn, repo="warden", tier="implement", brief="do it", context=None, why="w",
                model=None, origin=dispatch.Origin(), authorized_by="U1",
            )
        except RemoteError:
            pass
        else:
            raise AssertionError("expected RemoteError")
    op = conn.execute("SELECT * FROM operations WHERE repo='warden'").fetchone()
    assert op["outcome"] == "failed", dict(op)


def test_open_episode_remote_error_maybe_mutated_leaves_the_operation_open():
    conn, _ = _fresh_ledger()
    with _patch(sideclaw, "submit", _raiser(RemoteError("boom", maybe_mutated=True))):
        try:
            dispatch.open_episode(
                conn, repo="warden", tier="implement", brief="do it", context=None, why="w",
                model=None, origin=dispatch.Origin(), authorized_by="U1",
            )
        except RemoteError:
            pass
        else:
            raise AssertionError("expected RemoteError")
    op = conn.execute("SELECT * FROM operations WHERE repo='warden'").fetchone()
    assert op["outcome"] is None, ("an ambiguous submit must stay open for reconcile_operations()", dict(op))
    assert json.loads(op["receipt_json"])["maybeMutated"] is True, dict(op)


def test_open_episode_submits_the_composed_cwd_and_no_sensitive_or_model_key():
    """A dispatch names the repo; warden composes only the cwd and sends no
    `sensitive` (sideclaw derives it) and no `model` unless the owner passed one."""
    conn, _ = _fresh_ledger()
    sent: list[dict] = []

    def _submit(**kw):
        sent.append(kw)
        return {"id": "j-cwd", "status": "running"}

    with _env(WARDEN_REPOS_ROOT="/tmp/repos-root"), _patch(sideclaw, "submit", _submit):
        dispatch.open_episode(
            conn, repo="warden", tier="investigate", brief="do it", context=None, why=None,
            origin=dispatch.Origin(), authorized_by=None,
        )
    assert sent == [{"cwd": "/tmp/repos-root/warden", "tier": "investigate", "brief": "do it",
                     "context": None, "model": None}], sent


def test_open_episode_submit_refused_marks_operation_failed_not_unknown():
    conn, _ = _fresh_ledger()
    with _patch(sideclaw, "submit", _raiser(SubmitRefused("sideclaw refused the job (HTTP 400): nope", status=400))):
        try:
            dispatch.open_episode(
                conn, repo="warden", tier="implement", brief="do it", context=None, why="w",
                origin=dispatch.Origin(), authorized_by="U1",
            )
        except SubmitRefused as e:
            assert e.status == 400 and "nope" in str(e), e
        else:
            raise AssertionError("expected SubmitRefused")
    op = conn.execute("SELECT * FROM operations WHERE repo='warden'").fetchone()
    assert op["outcome"] == "failed", dict(op)
    assert conn.execute("SELECT COUNT(*) FROM dispatches").fetchone()[0] == 0


def test_open_episode_status_comes_from_job_not_a_literal():
    conn, _ = _fresh_ledger()
    with _patch(sideclaw, "submit", lambda **kw: {"id": "j-status", "status": "pending"}):
        dispatch.open_episode(
            conn, repo="warden", tier="investigate", brief="do it", context=None,
            why=None, model=None, origin=dispatch.Origin(), authorized_by=None,
        )
    row = conn.execute("SELECT status FROM dispatches WHERE job_id='j-status'").fetchone()
    assert row["status"] == "pending", dict(row)


# --- dispatch: sync_record() / list_dispatches() ---------------------------------

def test_sync_record_reported_stamps_delivered():
    conn, _ = _fresh_ledger()
    _seed_dispatch(conn, "job-1")
    job = {"id": "job-1", "status": "done", "result": {"artifactUrl": "https://x"}}
    dispatch.sync_record(conn, job, reported=True)
    row = conn.execute("SELECT * FROM dispatches WHERE job_id='job-1'").fetchone()
    assert row["status"] == "done"
    assert row["artifact_url"] == "https://x"
    assert row["delivery_status"] == "delivered"
    assert row["reported_at"] is not None


def test_sync_record_not_reported_leaves_reported_at_null():
    conn, _ = _fresh_ledger()
    _seed_dispatch(conn, "job-2")
    job = {"id": "job-2", "status": "done", "result": None}
    dispatch.sync_record(conn, job, reported=False)
    row = conn.execute("SELECT * FROM dispatches WHERE job_id='job-2'").fetchone()
    assert row["reported_at"] is None
    assert row["delivery_status"] is None


def test_sync_record_finished_at_uses_sideclaws_own_timestamp():
    """docs/history/state-log.md §87: `finished_at` must read when sideclaw
    itself finished the job (`job["finishedAt"]`, epoch ms), not when this
    process happened to poll it — a poll suspended for hours must not
    misreport how long the episode actually ran (§79's 614-minute dispatch
    that took 20)."""
    conn, _ = _fresh_ledger()
    _seed_dispatch(conn, "job-finished-at")
    observed_late = _now() + dt.timedelta(hours=10)
    finished_epoch_ms = int((_now() + dt.timedelta(minutes=5)).timestamp() * 1000)
    job = {"id": "job-finished-at", "status": "done", "result": None, "finishedAt": finished_epoch_ms}
    dispatch.sync_record(conn, job, reported=False, now=observed_late)
    row = conn.execute("SELECT finished_at FROM dispatches WHERE job_id='job-finished-at'").fetchone()
    recorded = dt.datetime.fromisoformat(row["finished_at"])
    expected = dt.datetime.fromtimestamp(finished_epoch_ms / 1000, tz=dt.timezone.utc)
    assert abs((recorded - expected).total_seconds()) < 1, row["finished_at"]
    assert recorded < observed_late - dt.timedelta(hours=1), (
        "finished_at must not fall back to the late observation time when sideclaw's own value is present")


def test_sync_record_finished_at_falls_back_to_now_when_sideclaw_omits_it():
    conn, _ = _fresh_ledger()
    _seed_dispatch(conn, "job-no-finished-at")
    now = _now()
    job = {"id": "job-no-finished-at", "status": "failed", "result": None}
    dispatch.sync_record(conn, job, reported=False, now=now)
    row = conn.execute("SELECT finished_at FROM dispatches WHERE job_id='job-no-finished-at'").fetchone()
    assert row["finished_at"] == now.isoformat(), row["finished_at"]


def test_list_dispatches_unknown_scope_raises():
    conn, _ = _fresh_ledger()
    try:
        dispatch.list_dispatches(conn, "bogus", _now())
    except UsageError as e:
        assert "unknown list scope" in str(e), e
    else:
        raise AssertionError("expected UsageError")


def test_list_dispatches_open_scope():
    conn, _ = _fresh_ledger()
    _seed_dispatch(conn, "j-open-running", status="running")
    _seed_dispatch(conn, "j-open-done-unreported", status="done")
    _seed_dispatch(conn, "j-closed", status="done", reported_at=_now().isoformat())
    rows = dispatch.list_dispatches(conn, "open", _now())
    ids = {r["job_id"] for r in rows}
    assert ids == {"j-open-running", "j-open-done-unreported"}, ids


def test_list_dispatches_today_scope():
    conn, _ = _fresh_ledger()
    yesterday = (_now() - dt.timedelta(days=1)).isoformat()
    _seed_dispatch(conn, "j-today", created_at=_now().isoformat())
    _seed_dispatch(conn, "j-yesterday", created_at=yesterday)
    rows = dispatch.list_dispatches(conn, "today", _now())
    assert {r["job_id"] for r in rows} == {"j-today"}


def test_list_dispatches_all_scope():
    conn, _ = _fresh_ledger()
    _seed_dispatch(conn, "j1")
    _seed_dispatch(conn, "j2")
    rows = dispatch.list_dispatches(conn, "all", _now())
    assert {r["job_id"] for r in rows} == {"j1", "j2"}


def test_list_dispatches_pops_brief_and_verdict():
    conn, _ = _fresh_ledger()
    _seed_dispatch(conn, "j1", verdict_json=json.dumps({"nextAction": "none"}))
    rows = dispatch.list_dispatches(conn, "all", _now())
    assert "brief" not in rows[0]
    assert "verdict_json" not in rows[0]


# --- runner ------------------------------------------------------------------

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
