#!/usr/bin/env python3
"""Regression suite for scripts/clients/ — the Python port of the transport
half of the retired bash dispatch bridge (Wave 5.1).

Every HTTP-facing client (sideclaw, github) is exercised against an
in-process `ThreadingHTTPServer` whose handler records every request
(method, path, headers, parsed body) and replies from a per-test route
table — never a real network call. `github.token()` is exercised against a
stub `secrets-run` executable, never the real one.

Run: .venv/bin/python3 tests/test_clients.py
"""

from __future__ import annotations

import contextlib
import datetime as dt
import io
import json
import os
import stat
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from clients import argo, github, sideclaw  # noqa: E402
from clients import slack as clients_slack  # noqa: E402
from clients.errors import HeadMoved, PolicyError, PreconditionError, RemoteError, SubmitRefused  # noqa: E402


# --- stub HTTP server ----------------------------------------------------------
# Extracted to tests/stubs.py (Wave 5.3) so tests/test_warden_cli.py can drive
# the same recording, per-test route table without a second implementation.
# Kept as `_StubServer`/`_closed_port` aliases here rather than renaming every
# call site below.

from stubs import StubServer as _StubServer  # noqa: E402
from stubs import closed_port as _closed_port  # noqa: E402


def _tmp_secrets_run(token: str, *, exit_code: int = 0) -> Path:
    d = Path(tempfile.mkdtemp(prefix="clients-secrets-"))
    script = d / "secrets-run"
    script.write_text(
        "#!/bin/sh\n"
        f"echo '{token}'\n"
        f"exit {exit_code}\n",
        encoding="utf-8",
    )
    script.chmod(script.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return script


def _tmp_secrets_run_by_ref(by_ref: dict[str, str]) -> Path:
    """Like `_tmp_secrets_run()` but branches on the `read <ref>` argument
    (called as `[secrets-run, "read", ref]` — `$2` in the script below) so a
    single stub can answer differently per op:// ref, the shape
    `resolve_slack_token()`'s two-tier resolution needs to be exercised for
    real rather than through a monkeypatched `resolve_secret`. A ref with no
    entry exits non-zero, matching `resolve_secret()`'s own "" on failure."""
    d = Path(tempfile.mkdtemp(prefix="clients-secrets-multi-"))
    script = d / "secrets-run"
    lines = ["#!/bin/sh", 'ref="$2"']
    for ref, token in by_ref.items():
        lines.append(f'if [ "$ref" = "{ref}" ]; then echo "{token}"; exit 0; fi')
    lines.append("exit 1")
    script.write_text("\n".join(lines) + "\n", encoding="utf-8")
    script.chmod(script.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
    return script


def _reset_github_token() -> None:
    github._token_cache = None  # noqa: SLF001


# --- sideclaw ------------------------------------------------------------------

def test_valid_job_id():
    assert sideclaw.valid_job_id("abc-123")
    assert not sideclaw.valid_job_id("abc/123")
    assert not sideclaw.valid_job_id("")


def test_submit_body_shape_minimal():
    srv = _StubServer({("POST", "/api/jobs"): (200, {"ok": True, "job": {"id": "j1", "status": "running"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        job = sideclaw.submit(cwd="/repo", tier="investigate", brief="do the thing")
        assert job == {"id": "j1", "status": "running"}, job
        req = srv.requests[0]
        assert req["body"] == {"tool": "dispatch", "params": {"cwd": "/repo", "tier": "investigate", "brief": "do the thing"}}, req["body"]
    finally:
        srv.stop()


def test_submit_body_shape_with_optional_keys_and_order():
    srv = _StubServer({("POST", "/api/jobs"): (200, {"ok": True, "job": {"id": "j2"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        sideclaw.submit(cwd="/repo", tier="implement", brief="b", context="ctx", model="haiku")
        params = srv.requests[0]["body"]["params"]
        assert list(params.keys()) == ["cwd", "tier", "brief", "context", "model"], params
        assert params["context"] == "ctx" and params["model"] == "haiku"
    finally:
        srv.stop()


def test_submit_4xx_raises_submit_refused_with_sideclaws_message():
    """A 4xx is sideclaw REFUSING (allowlist, tier ceiling, bad model): its own
    `error` text, the status, and a type callers can tell from a 5xx."""
    srv = _StubServer({("POST", "/api/jobs"): (
        400, {"ok": False, "error": "dispatch refused: cwd is not a repo directly under a dispatch root: /x"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit(cwd="/x", tier="implement", brief="b")
        except SubmitRefused as e:
            assert e.status == 400 and e.maybe_mutated is False, (e.status, e.maybe_mutated)
            assert "dispatch refused: cwd is not a repo directly under a dispatch root: /x" in str(e), e
        else:
            raise AssertionError("expected SubmitRefused")
    finally:
        srv.stop()


def test_submit_4xx_without_a_json_error_still_carries_the_body():
    srv = _StubServer({("POST", "/api/jobs"): (422, "params invalid")})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit(cwd="/x", tier="implement", brief="b")
        except SubmitRefused as e:
            assert e.status == 422 and "params invalid" in str(e), e
        else:
            raise AssertionError("expected SubmitRefused")
    finally:
        srv.stop()


def test_submit_5xx_is_a_plain_remote_error_not_a_refusal():
    srv = _StubServer({("POST", "/api/jobs"): (503, {"error": "overloaded"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit(cwd="/x", tier="implement", brief="b")
        except SubmitRefused:
            raise AssertionError("a 5xx must keep its retry behaviour, not read as a refusal")
        except RemoteError as e:
            assert "503" in str(e), e
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()


def test_submit_500_raises_remote_error():
    srv = _StubServer({("POST", "/api/jobs"): (500, {"error": "boom"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit(cwd="/repo", tier="investigate", brief="x")
        except RemoteError as e:
            assert "500" in str(e), e
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()


def test_submit_connection_refused_raises_remote_error():
    os.environ["WARDEN_SIDECLAW_BASE"] = f"http://127.0.0.1:{_closed_port()}"
    try:
        sideclaw.submit(cwd="/repo", tier="investigate", brief="x")
    except RemoteError as e:
        assert e.maybe_mutated is False, "connection refused is definitive: nothing was sent, a retry is safe"
    else:
        raise AssertionError("expected RemoteError")


def test_submit_timeout_is_ambiguous_and_flagged_maybe_mutated():
    real = sideclaw._request
    def _timeout(*a, **kw):
        raise TimeoutError("timed out")
    sideclaw._request = _timeout
    try:
        sideclaw.submit(cwd="/repo", tier="investigate", brief="x")
    except RemoteError as e:
        assert e.maybe_mutated is True, "a timeout may have landed — must be flagged"
    else:
        raise AssertionError("expected RemoteError")
    finally:
        sideclaw._request = real


def test_submit_5xx_is_definitive_and_not_maybe_mutated():
    srv = _StubServer({("POST", "/api/jobs"): (503, {"error": "busy"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit(cwd="/repo", tier="investigate", brief="x")
        except RemoteError as e:
            assert e.maybe_mutated is False, e.maybe_mutated
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()


def test_submit_no_id_raises_remote_error():
    srv = _StubServer({("POST", "/api/jobs"): (200, {"ok": True, "job": {"status": "running"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit(cwd="/repo", tier="investigate", brief="x")
        except RemoteError as e:
            assert "no id" in str(e), e
            assert e.maybe_mutated is True, "a 200 means the job exists — a retry would duplicate it"
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()


def test_get_returns_job_dict():
    srv = _StubServer({("GET", "/api/jobs/j1"): (200, {"job": {"id": "j1", "status": "done"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        assert sideclaw.get("j1") == {"id": "j1", "status": "done"}
    finally:
        srv.stop()


def test_get_404_returns_none():
    srv = _StubServer({("GET", "/api/jobs/missing"): (404, {"error": "not found"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        assert sideclaw.get("missing") is None
    finally:
        srv.stop()


def test_get_non200_raises_remote_error():
    srv = _StubServer({("GET", "/api/jobs/j1"): (500, {"error": "boom"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.get("j1")
        except RemoteError as e:
            assert "j1" in str(e), e
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()


def test_wait_returns_terminal_job():
    srv = _StubServer({("GET", "/api/jobs/j1"): (200, {"job": {"id": "j1", "status": "done"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        job = sideclaw.wait("j1", timeout_s=5, interval_s=1, sleep=lambda s: None, clock=lambda: 0.0)
        assert job == {"id": "j1", "status": "done"}
    finally:
        srv.stop()


def test_wait_times_out_returns_none():
    srv = _StubServer({("GET", "/api/jobs/j1"): (200, {"job": {"id": "j1", "status": "running"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        clock = _FakeClock()
        job = sideclaw.wait("j1", timeout_s=1, interval_s=1, sleep=lambda s: None, clock=clock)
        assert job is None
    finally:
        srv.stop()


class _FakeClock:
    def __init__(self):
        self.t = 0.0

    def __call__(self) -> float:
        self.t += 1000
        return self.t


def test_wait_404_mid_poll_raises_remote_error():
    calls = {"n": 0}

    def route(_req):
        calls["n"] += 1
        if calls["n"] == 1:
            return 200, {"job": {"id": "j1", "status": "running"}}
        return 404, {"error": "gone"}

    srv = _StubServer({("GET", "/api/jobs/j1"): route})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.wait("j1", timeout_s=5, interval_s=0.01, sleep=time.sleep, clock=time.monotonic)
        except RemoteError as e:
            assert "j1" in str(e), e
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()


def test_cancel_200_returns_job():
    srv = _StubServer({("POST", "/api/jobs/j1/cancel"): (200, {"job": {"id": "j1", "status": "cancelled"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        job = sideclaw.cancel("j1")
        assert job == {"id": "j1", "status": "cancelled"}
        assert srv.requests[0]["body"] == {}
    finally:
        srv.stop()


def test_cancel_404_raises_remote_error():
    srv = _StubServer({("POST", "/api/jobs/j1/cancel"): (404, {"error": "no such job"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.cancel("j1")
        except RemoteError:
            pass
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()


def test_cancel_409_raises_policy_error():
    srv = _StubServer({("POST", "/api/jobs/j1/cancel"): (409, {"error": "already terminal"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.cancel("j1")
        except PolicyError as e:
            assert "already terminal" in str(e), e
        else:
            raise AssertionError("expected PolicyError")
    finally:
        srv.stop()


def test_submit_review_body_shape():
    srv = _StubServer({("POST", "/api/jobs"): (200, {"ok": True, "job": {"id": "r1", "status": "running"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        job = sideclaw.submit_review(cwd=Path("/repo"), pr=17, context="ctx")
        assert job == {"id": "r1", "status": "running"}, job
        assert srv.requests[0]["body"] == {
            "tool": "review", "params": {"cwd": "/repo", "pr": 17, "context": "ctx"},
        }, srv.requests[0]["body"]
    finally:
        srv.stop()


def test_submit_review_body_shape_with_model():
    srv = _StubServer({("POST", "/api/jobs"): (200, {"ok": True, "job": {"id": "r3"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        sideclaw.submit_review(cwd=Path("/repo"), pr=17, context="ctx", model="glm-5.3-flash")
        assert srv.requests[0]["body"] == {
            "tool": "review", "params": {"cwd": "/repo", "pr": 17, "context": "ctx", "model": "glm-5.3-flash"},
        }, srv.requests[0]["body"]
    finally:
        srv.stop()


def test_submit_review_omits_context_when_absent():
    srv = _StubServer({("POST", "/api/jobs"): (200, {"ok": True, "job": {"id": "r2"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        sideclaw.submit_review(cwd=Path("/repo"), pr=5)
        assert srv.requests[0]["body"]["params"] == {"cwd": "/repo", "pr": 5}
    finally:
        srv.stop()


def test_submit_review_4xx_raises_submit_refused():
    srv = _StubServer({("POST", "/api/jobs"): (400, {"ok": False, "error": "dispatch refused: nope"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit_review(cwd=Path("/repo"), pr=1)
        except SubmitRefused as e:
            assert e.status == 400 and "dispatch refused: nope" in str(e), e
        else:
            raise AssertionError("expected SubmitRefused")
    finally:
        srv.stop()


def test_submit_review_500_raises_remote_error():
    srv = _StubServer({("POST", "/api/jobs"): (500, {"error": "boom"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit_review(cwd=Path("/repo"), pr=1)
        except RemoteError as e:
            assert "500" in str(e), e
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()


def test_submit_update_pr_body_shape():
    srv = _StubServer({("POST", "/api/jobs"): (200, {"ok": True, "job": {"id": "u1", "status": "queued"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        job = sideclaw.submit_update_pr(cwd=Path("/repo"), pr=17)
        assert job == {"id": "u1", "status": "queued"}, job
        assert srv.requests[0]["body"] == {"tool": "update_pr", "params": {"cwd": "/repo", "pr": 17}}, \
            srv.requests[0]["body"]
    finally:
        srv.stop()


def test_submit_update_pr_4xx_is_a_refusal_and_500_or_unreachable_a_remote_error():
    srv = _StubServer({("POST", "/api/jobs"): (400, {"ok": False, "error": "update_pr refused: not allowed"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit_update_pr(cwd="/repo", pr=1)
        except SubmitRefused as e:
            assert e.status == 400 and "update_pr refused: not allowed" in str(e), e
        else:
            raise AssertionError("expected SubmitRefused")
    finally:
        srv.stop()
    srv = _StubServer({("POST", "/api/jobs"): (500, {"error": "boom"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit_update_pr(cwd="/repo", pr=1)
        except SubmitRefused:
            raise AssertionError("a 5xx is not a refusal")
        except RemoteError as e:
            assert "500" in str(e), e
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()
    os.environ["WARDEN_SIDECLAW_BASE"] = f"http://127.0.0.1:{_closed_port()}"
    try:
        sideclaw.submit_update_pr(cwd="/repo", pr=1)
    except RemoteError as e:
        assert "update_pr submit failed" in str(e), e
    else:
        raise AssertionError("expected RemoteError")


_SHA_A, _SHA_B = "a" * 40, "b" * 40


def _update_pr_job(**result) -> dict:
    base = {"status": "updated", "headSha": _SHA_B, "previousHeadSha": _SHA_A, "baseSha": "c" * 40,
            "checks": {"passed": True, "summary": "all green"}, "prUrl": "https://github.com/jkrumm/r/pull/1"}
    return {"id": "u1", "status": "done", "result": {**base, **result}}


def test_update_pr_result_accepts_every_published_status():
    assert sideclaw.update_pr_result(_update_pr_job())["status"] == "updated"
    up = _update_pr_job(status="up_to_date", headSha=_SHA_A, checks=None)
    up["result"].pop("checks")
    assert sideclaw.update_pr_result(up)["headSha"] == _SHA_A
    conflict = _update_pr_job(status="conflict", headSha=_SHA_A, note="rebase onto master failed: x")
    conflict["result"].pop("checks")
    assert sideclaw.update_pr_result(conflict)["note"] == "rebase onto master failed: x"
    failed = _update_pr_job(checks={"passed": False, "summary": "1 failed", "failed": "lint"})
    assert sideclaw.update_pr_result(failed)["checks"]["passed"] is False


def test_update_pr_result_refuses_any_shape_it_does_not_know():
    no_checks = _update_pr_job()
    no_checks["result"].pop("checks")
    bad = [
        ("unknown status", _update_pr_job(status="rebased")),
        ("short sha", _update_pr_job(headSha="abc123")),
        ("missing previous", _update_pr_job(previousHeadSha=None)),
        ("no prUrl", _update_pr_job(prUrl=None)),
        ("updated without checks", no_checks),
        ("checks.passed not a bool", _update_pr_job(checks={"passed": "yes", "summary": "s"})),
        ("not done", {"id": "u1", "status": "failed", "error": "boom"}),
        ("no result", {"id": "u1", "status": "done", "result": None}),
    ]
    for why, job in bad:
        try:
            sideclaw.update_pr_result(job)
        except RemoteError:
            pass
        else:
            raise AssertionError(f"{why}: a malformed update_pr result must be a loud refusal")


def test_submit_triage_body_shape_has_no_model():
    srv = _StubServer({("POST", "/api/jobs"): (200, {"ok": True, "job": {"id": "t1", "status": "queued"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        schema = {"type": "object", "properties": {"action": {"type": "string"}}}
        job = sideclaw.submit_triage(prompt="decide", schema=schema)
        assert job == {"id": "t1", "status": "queued"}, job
        assert srv.requests[0]["body"] == {
            "tool": "triage", "params": {"prompt": "decide", "schema": schema},
        }, srv.requests[0]["body"]
    finally:
        srv.stop()


def test_submit_triage_4xx_raises_submit_refused():
    srv = _StubServer({("POST", "/api/jobs"): (400, {"ok": False, "error": "invalid params: prompt too long"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit_triage(prompt="x", schema={"type": "object"})
        except SubmitRefused as e:
            assert e.status == 400 and "invalid params: prompt too long" in str(e), e
        else:
            raise AssertionError("expected SubmitRefused")
    finally:
        srv.stop()


def test_submit_triage_500_and_unreachable_are_remote_errors():
    srv = _StubServer({("POST", "/api/jobs"): (500, {"error": "boom"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit_triage(prompt="x", schema={"type": "object"})
        except RemoteError as e:
            assert "500" in str(e), e
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()
    os.environ["WARDEN_SIDECLAW_BASE"] = f"http://127.0.0.1:{_closed_port()}"
    try:
        sideclaw.submit_triage(prompt="x", schema={"type": "object"})
    except RemoteError as e:
        assert "triage submit failed" in str(e), e
    else:
        raise AssertionError("expected RemoteError")


def test_assert_result_schema_ok_on_matching_version():
    sideclaw.assert_result_schema(
        {"status": "done", "result": {"schemaVersion": 2}}, 2, "implement"
    )  # must not raise


def test_assert_result_schema_raises_on_mismatch():
    try:
        sideclaw.assert_result_schema({"status": "done", "result": {"schemaVersion": 1}}, 2, "implement")
    except RemoteError as e:
        assert str(e) == (
            "sideclaw implement result schemaVersion 1, warden expects 2 — refusing to parse"
        ), e
    else:
        raise AssertionError("expected RemoteError")


def test_assert_result_schema_skips_non_done_jobs():
    sideclaw.assert_result_schema({"status": "failed", "result": None}, 2, "implement")  # must not raise
    sideclaw.assert_result_schema({"status": "cancelled"}, 2, "review")  # must not raise


def test_assert_outcome_ok_on_known_outcome():
    sideclaw.assert_outcome({"status": "done", "result": {"outcome": "clean"}},
                             sideclaw.REVIEW_OUTCOMES, "review")  # must not raise


def test_assert_outcome_raises_on_unrecognized_outcome():
    try:
        sideclaw.assert_outcome(
            {"status": "done", "result": {"outcome": "a_future_outcome"}},
            sideclaw.REVIEW_OUTCOMES, "review",
        )
    except RemoteError as e:
        assert "'a_future_outcome'" in str(e) and "refusing to parse" in str(e), e
    else:
        raise AssertionError("expected RemoteError")


def test_assert_outcome_raises_on_missing_outcome():
    try:
        sideclaw.assert_outcome({"status": "done", "result": {}}, sideclaw.DISPATCH_OUTCOMES, "implement")
    except RemoteError as e:
        assert "None" in str(e) and "refusing to parse" in str(e), e
    else:
        raise AssertionError("expected RemoteError")


def test_finished_at_iso_prefers_sideclaws_own_timestamp():
    fallback = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    finished = dt.datetime(2026, 1, 1, 0, 5, tzinfo=dt.timezone.utc)
    job = {"status": "done", "finishedAt": int(finished.timestamp() * 1000)}
    assert sideclaw.finished_at_iso(job, fallback=fallback) == finished.isoformat()


def test_finished_at_iso_falls_back_when_missing_or_not_a_number():
    fallback = dt.datetime(2026, 1, 1, tzinfo=dt.timezone.utc)
    assert sideclaw.finished_at_iso({"status": "failed"}, fallback=fallback) == fallback.isoformat()
    assert sideclaw.finished_at_iso({"finishedAt": None}, fallback=fallback) == fallback.isoformat()
    assert sideclaw.finished_at_iso({"finishedAt": "not-a-number"}, fallback=fallback) == fallback.isoformat()
    # bool is an int subclass in Python — must not be read as an epoch-ms timestamp.
    assert sideclaw.finished_at_iso({"finishedAt": True}, fallback=fallback) == fallback.isoformat()


def test_assert_outcome_skips_non_done_jobs():
    sideclaw.assert_outcome({"status": "failed", "result": None}, sideclaw.DISPATCH_OUTCOMES, "implement")
    sideclaw.assert_outcome({"status": "cancelled"}, sideclaw.REVIEW_OUTCOMES, "review")


# --- github ----------------------------------------------------------------

def test_token_memoized_and_resolved_via_secrets_run():
    _reset_github_token()
    stub = _tmp_secrets_run("ghs_faketoken123")
    os.environ["WARDEN_SECRETS_RUN"] = str(stub)
    try:
        t1 = github.token()
        # Change the script so a second read would return something else —
        # memoization means it is never re-read.
        stub.write_text("#!/bin/sh\necho 'different-token'\nexit 0\n", encoding="utf-8")
        t2 = github.token()
        assert t1 == t2 == "ghs_faketoken123", (t1, t2)
    finally:
        del os.environ["WARDEN_SECRETS_RUN"]
        _reset_github_token()


def test_token_empty_raises_precondition_error():
    _reset_github_token()
    stub = _tmp_secrets_run("", exit_code=0)
    os.environ["WARDEN_SECRETS_RUN"] = str(stub)
    try:
        try:
            github.token()
        except PreconditionError as e:
            assert "resolved empty" in str(e), e
        else:
            raise AssertionError("expected PreconditionError")
    finally:
        del os.environ["WARDEN_SECRETS_RUN"]
        _reset_github_token()


def _gh_env(srv: "_StubServer", token: str = "ghs_test") -> None:
    _reset_github_token()
    stub = _tmp_secrets_run(token)
    os.environ["WARDEN_SECRETS_RUN"] = str(stub)
    os.environ["WARDEN_GH_API"] = srv.base


def _gh_cleanup() -> None:
    del os.environ["WARDEN_SECRETS_RUN"]
    del os.environ["WARDEN_GH_API"]
    _reset_github_token()


def test_api_sets_headers_accept_version_bearer():
    srv = _StubServer({("GET", "/repos/jkrumm/gamma"): (200, {"full_name": "jkrumm/gamma"})})
    _gh_env(srv, token="ghs_secretvalue")
    try:
        status, body = github.api("GET", "/repos/jkrumm/gamma")
        assert status == 200 and body == {"full_name": "jkrumm/gamma"}
        headers = srv.requests[0]["headers"]
        assert headers.get("Accept") == "application/vnd.github+json"
        assert headers.get("X-Github-Api-Version") == "2022-11-28"
        assert headers.get("Authorization") == "Bearer ghs_secretvalue"
    finally:
        srv.stop()
        _gh_cleanup()


def test_api_never_raises_on_http_status():
    srv = _StubServer({("GET", "/repos/jkrumm/gamma"): (500, {"message": "server error"})})
    _gh_env(srv)
    try:
        status, body = github.api("GET", "/repos/jkrumm/gamma")
        assert status == 500 and body == {"message": "server error"}
    finally:
        srv.stop()
        _gh_cleanup()


def test_api_connection_refused_raises_remote_error():
    _reset_github_token()
    stub = _tmp_secrets_run("ghs_test")
    os.environ["WARDEN_SECRETS_RUN"] = str(stub)
    os.environ["WARDEN_GH_API"] = f"http://127.0.0.1:{_closed_port()}"
    try:
        try:
            github.api("GET", "/repos/jkrumm/gamma")
        except RemoteError:
            pass
        else:
            raise AssertionError("expected RemoteError")
    finally:
        del os.environ["WARDEN_SECRETS_RUN"]
        del os.environ["WARDEN_GH_API"]
        _reset_github_token()


def test_read_pr_non200_raises_remote_error():
    srv = _StubServer({("GET", "/repos/jkrumm/gamma/pulls/1"): (404, {"message": "not found"})})
    _gh_env(srv)
    try:
        try:
            github.read_pr("jkrumm", "gamma", 1)
        except RemoteError:
            pass
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()
        _gh_cleanup()


def test_read_repo_ok():
    srv = _StubServer({("GET", "/repos/jkrumm/gamma"): (200, {"default_branch": "master"})})
    _gh_env(srv)
    try:
        assert github.read_repo("jkrumm", "gamma") == {"default_branch": "master"}
    finally:
        srv.stop()
        _gh_cleanup()


def test_pr_files_ok():
    srv = _StubServer({("GET", "/repos/jkrumm/gamma/pulls/1/files?per_page=100"): (200, [{"filename": "a.py"}])})
    _gh_env(srv)
    try:
        assert github.pr_files("jkrumm", "gamma", 1) == [{"filename": "a.py"}]
    finally:
        srv.stop()
        _gh_cleanup()


_FULL_SHA = "d" * 40


def test_check_runs_ok():
    srv = _StubServer({("GET", f"/repos/jkrumm/gamma/commits/{_FULL_SHA}/check-runs"): (200, {"check_runs": [{"conclusion": "success"}]})})
    _gh_env(srv)
    try:
        assert github.check_runs("jkrumm", "gamma", _FULL_SHA) == [{"conclusion": "success"}]
    finally:
        srv.stop()
        _gh_cleanup()


def test_check_runs_invalid_sha_raises_precondition_error_before_any_request():
    srv = _StubServer({})
    _gh_env(srv)
    try:
        try:
            github.check_runs("jkrumm", "gamma", "../../etc/passwd")
        except PreconditionError:
            pass
        else:
            raise AssertionError("expected PreconditionError")
        assert srv.requests == [], "a malformed sha must never reach a request"
    finally:
        srv.stop()
        _gh_cleanup()


def test_workflow_runs_maps_to_check_run_shape_keeping_the_latest_per_workflow():
    """The Actions-runs fallback reader maps runs to the check-run shape the merge
    gate consumes, keeping only the latest attempt per workflow so a stale green
    re-run cannot mask the run that matters."""
    srv = _StubServer({("GET", f"/repos/jkrumm/gamma/actions/runs?head_sha={_FULL_SHA}&per_page=100"): (
        200,
        {"workflow_runs": [
            {"workflow_id": 1, "name": "CI", "status": "completed", "conclusion": "failure",
             "run_attempt": 1, "created_at": "2026-01-01T00:00:00Z"},
            {"workflow_id": 1, "name": "CI", "status": "completed", "conclusion": "success",
             "run_attempt": 2, "created_at": "2026-01-01T01:00:00Z"},
            {"workflow_id": 2, "name": "lint", "status": "in_progress", "conclusion": None,
             "run_attempt": 1, "created_at": "2026-01-01T00:00:00Z"},
        ]},
    )})
    _gh_env(srv)
    try:
        runs = github.workflow_runs("jkrumm", "gamma", _FULL_SHA)
        assert sorted(runs, key=lambda r: r["name"]) == [
            {"name": "CI", "status": "completed", "conclusion": "success"},
            {"name": "lint", "status": "in_progress", "conclusion": None},
        ], runs
    finally:
        srv.stop()
        _gh_cleanup()


def test_workflow_runs_non_200_raises_remote_error():
    srv = _StubServer({("GET", f"/repos/jkrumm/gamma/actions/runs?head_sha={_FULL_SHA}&per_page=100"): (
        403, {"message": "Resource not accessible by personal access token"})})
    _gh_env(srv)
    try:
        try:
            github.workflow_runs("jkrumm", "gamma", _FULL_SHA)
        except RemoteError as e:
            assert "403" in str(e)
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()
        _gh_cleanup()


def test_workflow_runs_invalid_sha_raises_precondition_error_before_any_request():
    srv = _StubServer({})
    _gh_env(srv)
    try:
        try:
            github.workflow_runs("jkrumm", "gamma", "../../etc/passwd")
        except PreconditionError:
            pass
        else:
            raise AssertionError("expected PreconditionError")
        assert srv.requests == [], "a malformed sha must never reach a request"
    finally:
        srv.stop()
        _gh_cleanup()


def test_mark_ready_for_review_non200_raises():
    srv = _StubServer({("POST", "/graphql"): (500, {"message": "boom"})})
    _gh_env(srv)
    try:
        try:
            github.mark_ready_for_review("PR_node")
        except RemoteError as e:
            assert "Nothing was merged" in str(e), e
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()
        _gh_cleanup()


def test_mark_ready_for_review_graphql_errors_raises():
    srv = _StubServer({("POST", "/graphql"): (200, {"errors": [{"message": "nope"}]})})
    _gh_env(srv)
    try:
        try:
            github.mark_ready_for_review("PR_node")
        except RemoteError as e:
            assert "rejected" in str(e), e
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()
        _gh_cleanup()


def test_mark_ready_for_review_ok():
    srv = _StubServer({("POST", "/graphql"): (200, {"data": {"markPullRequestReadyForReview": {"pullRequest": {"isDraft": False}}}})})
    _gh_env(srv)
    try:
        github.mark_ready_for_review("PR_node")  # must not raise
        assert srv.requests[0]["body"]["variables"] == {"id": "PR_node"}
    finally:
        srv.stop()
        _gh_cleanup()


def test_merge_pr_200_returns_body():
    srv = _StubServer({("PUT", "/repos/jkrumm/gamma/pulls/1/merge"): (200, {"sha": "abc123", "merged": True})})
    _gh_env(srv)
    try:
        body = github.merge_pr("jkrumm", "gamma", 1, sha="head-sha", method="squash")
        assert body == {"sha": "abc123", "merged": True}
        req_body = srv.requests[0]["body"]
        assert req_body == {"sha": "head-sha", "merge_method": "squash"}
    finally:
        srv.stop()
        _gh_cleanup()


def test_merge_pr_409_raises_head_moved():
    """GitHub's 409 on the merge endpoint is "Head branch was modified": the pinned sha is not
    the head any more. HeadMoved (a PolicyError) — the merge train brings the PR up to date again."""
    srv = _StubServer({("PUT", "/repos/jkrumm/gamma/pulls/1/merge"): (409, {"message": "Head branch was modified"})})
    _gh_env(srv)
    try:
        try:
            github.merge_pr("jkrumm", "gamma", 1, sha="s", method="squash")
        except HeadMoved as e:
            assert isinstance(e, PolicyError), "a moved head is still a refusal for every other caller"
        else:
            raise AssertionError("expected HeadMoved")
    finally:
        srv.stop()
        _gh_cleanup()


def test_merge_pr_405_raises_policy_error():
    srv = _StubServer({("PUT", "/repos/jkrumm/gamma/pulls/1/merge"): (405, {"message": "not mergeable"})})
    _gh_env(srv)
    try:
        try:
            github.merge_pr("jkrumm", "gamma", 1, sha="s", method="squash")
        except PolicyError:
            pass
        else:
            raise AssertionError("expected PolicyError")
    finally:
        srv.stop()
        _gh_cleanup()


def test_merge_pr_500_raises_remote_error_maybe_mutated():
    srv = _StubServer({("PUT", "/repos/jkrumm/gamma/pulls/1/merge"): (500, {"message": "boom"})})
    _gh_env(srv)
    try:
        try:
            github.merge_pr("jkrumm", "gamma", 1, sha="s", method="squash")
        except RemoteError as e:
            assert e.maybe_mutated is True, "the PUT was sent, so the outcome is unknown, not clean"
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()
        _gh_cleanup()


def test_delete_branch_204_and_422_true_else_false():
    srv = _StubServer({
        ("DELETE", "/repos/jkrumm/gamma/git/refs/heads/dispatch/a"): (204, None),
        ("DELETE", "/repos/jkrumm/gamma/git/refs/heads/dispatch/b"): (422, {"message": "gone"}),
        ("DELETE", "/repos/jkrumm/gamma/git/refs/heads/dispatch/c"): (500, {"message": "boom"}),
    })
    _gh_env(srv)
    try:
        assert github.delete_branch("jkrumm", "gamma", "dispatch/a") is True
        assert github.delete_branch("jkrumm", "gamma", "dispatch/b") is True
        assert github.delete_branch("jkrumm", "gamma", "dispatch/c") is False
    finally:
        srv.stop()
        _gh_cleanup()


def test_search_issues_parses_a_search_response():
    body = {
        "items": [
            {
                "repository_url": "https://api.github.com/repos/jkrumm/argo",
                "number": 42,
                "title": "fix the thing",
                "body": "please fix it",
                "html_url": "https://github.com/jkrumm/argo/issues/42",
                "user": {"login": "jkrumm"},
                "updated_at": "2026-09-10T00:00:00Z",
            },
        ],
    }
    encoded_path = '/search/issues?q=owner:jkrumm+is:issue+is:open+-label:warden:skip&per_page=50'
    srv = _StubServer({("GET", encoded_path): (200, body)})
    _gh_env(srv)
    try:
        hits = github.search_issues(owner="jkrumm", skip_label="warden:skip")
    finally:
        srv.stop()
        _gh_cleanup()
    assert hits == [{
        "repo": "argo", "number": 42, "title": "fix the thing", "body": "please fix it",
        "url": "https://github.com/jkrumm/argo/issues/42", "author": "jkrumm",
        "updated_at": "2026-09-10T00:00:00Z", "labels": [],
    }], hits
    # The request path the stub actually saw — pins that `+`/`:` stay literal
    # (GitHub's own search syntax) while the quotes around the skip label are
    # percent-encoded, not sent raw.
    assert srv.requests[-1]["path"] == encoded_path, srv.requests[-1]["path"]


def test_search_issues_url_encodes_a_skip_label_with_special_characters():
    """`skip_label` reaches the query string only through `urllib.parse.quote`
    — a skip label carrying `&`/`=`/a space must not be able to smuggle extra
    query parameters into the GitHub request (`api()` sends whatever path it
    is given verbatim, with no encoding of its own)."""
    srv = _StubServer({"default": (200, {"items": []})})
    _gh_env(srv)
    try:
        github.search_issues(owner="jkrumm", skip_label="a&b=c d")
    finally:
        srv.stop()
        _gh_cleanup()
    path = srv.requests[-1]["path"]
    assert path == '/search/issues?q=owner:jkrumm+is:issue+is:open+-label:a%26b%3Dc%20d&per_page=50', path
    assert "&b=" not in path and " " not in path


def test_search_issues_pages_through_total_count():
    """A `total_count` bigger than one page of 50 must be paged through, not
    silently truncated at page 1 — see `ingest_github_issues()`'s reliance on
    the FULL result set to decide what to resolve."""
    def _hit(n: int) -> dict[str, Any]:
        return {
            "repository_url": "https://api.github.com/repos/jkrumm/argo",
            "number": n, "title": "t", "body": "b",
            "html_url": f"https://github.com/jkrumm/argo/issues/{n}",
            "user": {"login": "jkrumm"}, "updated_at": "2026-09-10T00:00:00Z",
        }
    page1_path = "/search/issues?q=owner:jkrumm+is:issue+is:open+-label:warden:skip&per_page=50"
    page2_path = page1_path + "&page=2"
    srv = _StubServer({
        ("GET", page1_path): (200, {"total_count": 60, "items": [_hit(n) for n in range(50)]}),
        ("GET", page2_path): (200, {"total_count": 60, "items": [_hit(n) for n in range(50, 60)]}),
    })
    _gh_env(srv)
    try:
        hits = github.search_issues(owner="jkrumm", skip_label="warden:skip")
    finally:
        srv.stop()
        _gh_cleanup()
    assert len(hits) == 60, len(hits)
    assert [h["number"] for h in hits] == list(range(60))


def test_search_issues_total_count_exceeding_max_pages_raises_remote_error():
    """`total_count` still bigger than everything `_SEARCH_MAX_PAGES` pages
    could fetch must refuse outright — DESIGN.md's "overflow waits, never
    drops" means a caller that cannot see the whole result set must not
    resolve anything out of it, not silently act on a partial one."""
    def _hit(n: int) -> dict[str, Any]:
        return {
            "repository_url": "https://api.github.com/repos/jkrumm/argo",
            "number": n, "title": "t", "body": "b",
            "html_url": f"https://github.com/jkrumm/argo/issues/{n}",
            "user": {"login": "jkrumm"}, "updated_at": "2026-09-10T00:00:00Z",
        }
    srv = _StubServer({"default": (200, {"total_count": 1_000_000, "items": [_hit(n) for n in range(50)]})})
    _gh_env(srv)
    try:
        try:
            github.search_issues(owner="jkrumm", skip_label="warden:skip")
        except RemoteError as e:
            assert "1000000" in str(e), e
            assert "refusing to resolve" in str(e), e
        else:
            raise AssertionError("expected RemoteError")
        # Exactly _SEARCH_MAX_PAGES requests, never an unbounded loop.
        assert len(srv.requests) == github._SEARCH_MAX_PAGES, len(srv.requests)
    finally:
        srv.stop()
        _gh_cleanup()


def test_search_issues_non_200_raises_remote_error():
    srv = _StubServer({"default": (500, {"error": "boom"})})
    _gh_env(srv)
    try:
        try:
            github.search_issues(owner="jkrumm", skip_label="warden:skip")
        except RemoteError:
            pass
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()
        _gh_cleanup()


def test_search_issues_incomplete_results_raises_remote_error():
    """GitHub's search index can time out and still return HTTP 200 with
    `incomplete_results: true` — trusting that page would let
    `ingest_github_issues()` read a genuinely-still-open issue as "not in
    this result set" and resolve its event out from under a human still
    waiting on it. Must refuse the same way a truncated `total_count` does."""
    srv = _StubServer({"default": (200, {"total_count": 1, "incomplete_results": True, "items": []})})
    _gh_env(srv)
    try:
        try:
            github.search_issues(owner="jkrumm", skip_label="warden:skip")
        except RemoteError as e:
            assert "incomplete_results" in str(e), e
        else:
            raise AssertionError("expected RemoteError")
    finally:
        srv.stop()
        _gh_cleanup()


def test_create_issue_comment_posts_to_the_right_url():
    srv = _StubServer({
        ("POST", "/repos/jkrumm/argo/issues/42/comments"): (201, {"id": 1, "body": "hi"}),
    })
    _gh_env(srv)
    try:
        resp = github.create_issue_comment("jkrumm/argo", 42, "hi")
    finally:
        srv.stop()
        _gh_cleanup()
    assert resp == {"id": 1, "body": "hi"}, resp
    assert srv.requests[0]["body"] == {"body": "hi"}, srv.requests[0]


def test_create_issue_comment_bad_repo_full_raises_precondition_error():
    srv = _StubServer({})
    _gh_env(srv)
    try:
        try:
            github.create_issue_comment("not-owner-slash-repo", 1, "hi")
        except PreconditionError:
            pass
        else:
            raise AssertionError("expected PreconditionError")
        assert srv.requests == []
    finally:
        srv.stop()
        _gh_cleanup()


def test_parse_pr_url_good_and_bad():
    assert github.parse_pr_url("https://github.com/jkrumm/gamma/pull/42") == ("jkrumm", "gamma", 42)
    assert github.parse_pr_url("https://gitlab.com/jkrumm/gamma/pull/42") is None
    assert github.parse_pr_url("https://github.com/jkrumm/gamma/issues/42") is None
    assert github.parse_pr_url("not a url") is None


def test_pick_merge_method_honours_linear_history():
    repo = {"allow_merge_commit": True}
    assert github.pick_merge_method(repo) == "merge"
    assert github.pick_merge_method(repo, [{"type": "required_linear_history", "parameters": None}]) is None


def test_pick_merge_method_honours_allowed_merge_methods():
    repo = {"allow_squash_merge": True, "allow_rebase_merge": True}
    rules = [{"type": "pull_request", "parameters": {"allowed_merge_methods": ["rebase"]}}]
    assert github.pick_merge_method(repo, rules) == "rebase"


def test_branch_rules_non_200_raises():
    saved = github.api
    github.api = lambda method, path, body=None: (404, {"message": "Not Found"})
    try:
        github.branch_rules("jkrumm", "gamma", "master")
    except RemoteError as e:
        assert "404" in str(e)
    else:
        raise AssertionError("expected RemoteError")
    finally:
        github.api = saved


def test_branch_rules_plan_gated_403_is_no_rules_not_a_refusal():
    """§108 — rulesets are a paid feature on private repositories, so GitHub
    answers the rules read with 403 "Upgrade to GitHub Pro or make this
    repository public to enable this feature." on a private repo on a plan
    without them. The repo then cannot have a ruleset at all: `[]` is the
    true answer, and refusing made weatherorb (private, `autoDeploy`)
    permanently unmergeable — items 1276 and 1277 parked on this 403 while
    their reviews had confirmed. The merge call stays the enforcement point,
    so an unreadable-but-real protection still cannot be ridden."""
    saved = github.api
    try:
        github.api = lambda method, path, body=None: (
            403,
            {"message": "Upgrade to GitHub Pro or make this repository public to enable this feature."},
        )
        assert github.branch_rules("jkrumm", "weatherorb", "master") == []

        github.api = lambda method, path, body=None: (
            403, {"message": "Resource not accessible by personal access token"},
        )
        try:
            github.branch_rules("jkrumm", "weatherorb", "master")
        except RemoteError as e:
            assert "403" in str(e)
        else:
            raise AssertionError("a 403 that is not the plan gate must stay a refusal")
    finally:
        github.api = saved


def test_check_runs_token_403_is_its_own_refusal():
    """§110 — a fine-grained PAT without `Checks: read` 403s this read on a
    private repository ("Resource not accessible by personal access token";
    the response's `x-accepted-github-permissions: checks=read`), which is a
    fact about the credential, not about the commit. It is its own exception
    so the *caller* decides what an unreadable CI read means; every other
    non-200 — and any 403 that is not this one — stays a plain RemoteError."""
    saved = github.api
    try:
        github.api = lambda method, path, body=None: (
            403, {"message": "Resource not accessible by personal access token"})
        try:
            github.check_runs("jkrumm", "weatherorb", "a" * 40)
        except github.CheckRunsUnreadable as e:
            assert "403" in str(e)
        else:
            raise AssertionError("expected CheckRunsUnreadable")

        github.api = lambda method, path, body=None: (404, {"message": "Not Found"})
        try:
            github.check_runs("jkrumm", "weatherorb", "a" * 40)
        except github.CheckRunsUnreadable:
            raise AssertionError("a 404 is not the token's permission gap")
        except RemoteError:
            pass
        else:
            raise AssertionError("expected RemoteError")
    finally:
        github.api = saved


def test_pick_merge_method_order():
    assert github.pick_merge_method({"allow_squash_merge": True, "allow_rebase_merge": True}) == "squash"
    assert github.pick_merge_method({"allow_squash_merge": False, "allow_rebase_merge": True}) == "rebase"
    assert github.pick_merge_method({"allow_merge_commit": True}) == "merge"
    assert github.pick_merge_method({}) is None


# --- the slack move ------------------------------------------------------------

def test_slack_module_importable_from_clients():
    assert hasattr(clients_slack, "resolve_slack_token")
    assert hasattr(clients_slack, "slack_post_message")


def test_shim_reexports_same_objects():
    """The by-path loaders in triage.py and dispatch-sweep.py load
    scripts/slack_client.py by file path, not by package import — prove the
    shim hands back the SAME function objects clients.slack defines, so a
    caller monkeypatching the loaded shim module sees its own patch."""
    import importlib.util

    shim_path = REPO / "scripts" / "slack_client.py"
    spec = importlib.util.spec_from_file_location("slack_client_shim_test", shim_path)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert module.resolve_slack_token is clients_slack.resolve_slack_token
    assert module.slack_post_message is clients_slack.slack_post_message

    # Patching the loaded shim instance must not leak into clients.slack —
    # each by-path load gets its own module object.
    original = module.resolve_slack_token
    module.resolve_slack_token = lambda: "patched"
    assert clients_slack.resolve_slack_token is original
    assert module.resolve_slack_token() == "patched"


# --- argo (the loop's own push to the Argo dashboard) --------------------------

class _FakeArgoResp:
    def __init__(self, status: int, body: bytes = b"{}"):
        self.status = status
        self._body = body

    def read(self, amt: int | None = None):
        return self._body if amt is None else self._body[:amt]

    def __enter__(self):
        return self

    def __exit__(self, *_a):
        return False


def _with_fake_urlopen(fn):
    """Patch `argo.urllib.request.urlopen` for the duration of the `with`
    block, restoring it on exit — no real network in any of these tests."""

    class _Ctx:
        def __enter__(self):
            self.saved = argo.urllib.request.urlopen
            argo.urllib.request.urlopen = fn
            return self

        def __exit__(self, *_a):
            argo.urllib.request.urlopen = self.saved
            return False

    return _Ctx()


def test_push_snapshot_201_is_ok_and_carries_bearer_and_body():
    captured: dict[str, Any] = {}

    def _fake_urlopen(req, timeout=None):
        captured["req"] = req
        return _FakeArgoResp(201)

    with _with_fake_urlopen(_fake_urlopen):
        status = argo.push_snapshot({"machine": "mini"}, token="test-token")

    assert status == "ok", status
    req = captured["req"]
    assert req.get_header("Authorization") == "Bearer test-token", req.headers
    assert req.get_header("Content-type") == "application/json", req.headers
    assert json.loads(req.data.decode()) == {"machine": "mini"}


def test_push_snapshot_http_error_404():
    def _fake_urlopen(req, timeout=None):
        raise argo.urllib.error.HTTPError(req.full_url, 404, "not found", None, None)

    with _with_fake_urlopen(_fake_urlopen):
        status = argo.push_snapshot({}, token="test-token")
    assert status == "http-error:404", status


def test_push_snapshot_url_error_is_network_error():
    def _fake_urlopen(req, timeout=None):
        raise argo.urllib.error.URLError("connection refused")

    with _with_fake_urlopen(_fake_urlopen):
        status = argo.push_snapshot({}, token="test-token")
    assert status == "network-error", status


def test_push_snapshot_empty_token_is_no_secret_with_no_network_call():
    called = {"n": 0}

    def _fake_urlopen(req, timeout=None):
        called["n"] += 1
        return _FakeArgoResp(201)

    with _with_fake_urlopen(_fake_urlopen):
        status = argo.push_snapshot({}, token="")
    assert status == "no-secret", status
    assert called["n"] == 0


def test_push_snapshot_oversize_is_too_large_with_no_network_call():
    called = {"n": 0}

    def _fake_urlopen(req, timeout=None):
        called["n"] += 1
        return _FakeArgoResp(201)

    with _with_fake_urlopen(_fake_urlopen):
        status = argo.push_snapshot({"data": "x" * (argo.MAX_BODY_BYTES + 10)}, token="test-token")
    assert status == "too-large", status
    assert called["n"] == 0


def test_push_snapshot_non_serializable_payload_is_encode_error_with_no_network_call():
    called = {"n": 0}

    def _fake_urlopen(req, timeout=None):
        called["n"] += 1
        return _FakeArgoResp(201)

    with _with_fake_urlopen(_fake_urlopen):
        status = argo.push_snapshot({"bad": object()}, token="test-token")
    assert status == "encode-error", status
    assert called["n"] == 0


def test_fetch_actions_ok_returns_status_and_list():
    captured: dict[str, Any] = {}

    def _fake_urlopen(req, timeout=None):
        captured["req"] = req
        return _FakeArgoResp(200, json.dumps([{"id": "a1", "event_id": 1, "verb": "note",
                                                "payload": {"text": "hi"}}]).encode())

    with _with_fake_urlopen(_fake_urlopen):
        status, actions = argo.fetch_actions("mini", token="test-token")

    assert status == "ok", status
    assert actions == [{"id": "a1", "event_id": 1, "verb": "note", "payload": {"text": "hi"}}]
    req = captured["req"]
    assert req.get_header("Authorization") == "Bearer test-token", req.headers
    assert "machine=mini" in req.full_url and "status=pending" in req.full_url, req.full_url


def test_fetch_actions_empty_token_is_no_secret_with_no_network_call():
    called = {"n": 0}

    def _fake_urlopen(req, timeout=None):
        called["n"] += 1
        return _FakeArgoResp(200, b"[]")

    with _with_fake_urlopen(_fake_urlopen):
        status, actions = argo.fetch_actions("mini", token="")
    assert status == "no-secret", status
    assert actions == []
    assert called["n"] == 0


def test_fetch_actions_http_error_404():
    def _fake_urlopen(req, timeout=None):
        raise argo.urllib.error.HTTPError(req.full_url, 404, "not found", None, None)

    with _with_fake_urlopen(_fake_urlopen):
        status, actions = argo.fetch_actions("mini", token="test-token")
    assert status == "http-error:404", status
    assert actions == []


def test_fetch_actions_url_error_is_network_error():
    def _fake_urlopen(req, timeout=None):
        raise argo.urllib.error.URLError("connection refused")

    with _with_fake_urlopen(_fake_urlopen):
        status, actions = argo.fetch_actions("mini", token="test-token")
    assert status == "network-error", status
    assert actions == []


def test_fetch_actions_non_list_body_is_decode_error():
    def _fake_urlopen(req, timeout=None):
        return _FakeArgoResp(200, b'{"not": "a list"}')

    with _with_fake_urlopen(_fake_urlopen):
        status, actions = argo.fetch_actions("mini", token="test-token")
    assert status == "decode-error", status
    assert actions == []


def test_fetch_actions_invalid_json_is_decode_error():
    def _fake_urlopen(req, timeout=None):
        return _FakeArgoResp(200, b"not json at all")

    with _with_fake_urlopen(_fake_urlopen):
        status, actions = argo.fetch_actions("mini", token="test-token")
    assert status == "decode-error", status
    assert actions == []


def test_fetch_actions_oversized_body_is_decode_error_never_fully_buffered():
    # A list of one huge string, well past MAX_BODY_BYTES once encoded.
    oversized = json.dumps([{"id": "a1", "event_id": 1, "verb": "note",
                              "payload": {"text": "x" * (argo.MAX_BODY_BYTES + 1000)}}]).encode()

    def _fake_urlopen(req, timeout=None):
        return _FakeArgoResp(200, oversized)

    with _with_fake_urlopen(_fake_urlopen):
        status, actions = argo.fetch_actions("mini", token="test-token")
    assert status == "decode-error", status
    assert actions == []


def test_ack_action_ok_carries_status_and_omits_none_fields():
    captured: dict[str, Any] = {}

    def _fake_urlopen(req, timeout=None):
        captured["req"] = req
        return _FakeArgoResp(200)

    with _with_fake_urlopen(_fake_urlopen):
        status = argo.ack_action("a1", status="applied", token="test-token")

    assert status == "ok", status
    req = captured["req"]
    assert req.get_header("Authorization") == "Bearer test-token", req.headers
    body = json.loads(req.data.decode())
    assert body == {"status": "applied"}, body


def test_ack_action_carries_result_and_error_when_given():
    captured: dict[str, Any] = {}

    def _fake_urlopen(req, timeout=None):
        captured["req"] = req
        return _FakeArgoResp(200)

    with _with_fake_urlopen(_fake_urlopen):
        status = argo.ack_action("a1", status="failed", result={"jobId": "j1"}, error="boom",
                                  token="test-token")

    assert status == "ok", status
    body = json.loads(captured["req"].data.decode())
    assert body == {"status": "failed", "result": {"jobId": "j1"}, "error": "boom"}, body


def test_ack_action_http_error_404():
    def _fake_urlopen(req, timeout=None):
        raise argo.urllib.error.HTTPError(req.full_url, 404, "not found", None, None)

    with _with_fake_urlopen(_fake_urlopen):
        status = argo.ack_action("a1", status="applied", token="test-token")
    assert status == "http-error:404", status


def test_ack_action_url_error_is_network_error():
    def _fake_urlopen(req, timeout=None):
        raise argo.urllib.error.URLError("connection refused")

    with _with_fake_urlopen(_fake_urlopen):
        status = argo.ack_action("a1", status="applied", token="test-token")
    assert status == "network-error", status


def test_ack_action_empty_token_is_no_secret_with_no_network_call():
    called = {"n": 0}

    def _fake_urlopen(req, timeout=None):
        called["n"] += 1
        return _FakeArgoResp(200)

    with _with_fake_urlopen(_fake_urlopen):
        status = argo.ack_action("a1", status="applied", token="")
    assert status == "no-secret", status
    assert called["n"] == 0


# --- clients.secrets — the shared env-then-secrets-run resolver ----------------

def test_secrets_resolve_secret_env_var_first():
    from clients import secrets as clients_secrets
    os.environ["TEST_WARDEN_SECRET_ENV"] = "from-env"
    try:
        assert clients_secrets.resolve_secret("TEST_WARDEN_SECRET_ENV", "op://x/y/z") == "from-env"
    finally:
        os.environ.pop("TEST_WARDEN_SECRET_ENV", None)


def test_secrets_resolve_secret_falls_back_to_secrets_run():
    from clients import secrets as clients_secrets
    stub = _tmp_secrets_run("from-secrets-run")
    saved = clients_secrets._SECRETS_RUN
    clients_secrets._SECRETS_RUN = stub
    os.environ.pop("TEST_WARDEN_SECRET_ENV_2", None)
    try:
        assert clients_secrets.resolve_secret("TEST_WARDEN_SECRET_ENV_2", "op://x/y/z") == "from-secrets-run"
    finally:
        clients_secrets._SECRETS_RUN = saved


def test_secrets_resolve_secret_empty_on_failure():
    from clients import secrets as clients_secrets
    stub = _tmp_secrets_run("ignored", exit_code=1)
    saved = clients_secrets._SECRETS_RUN
    clients_secrets._SECRETS_RUN = stub
    os.environ.pop("TEST_WARDEN_SECRET_ENV_3", None)
    try:
        assert clients_secrets.resolve_secret("TEST_WARDEN_SECRET_ENV_3", "op://x/y/z") == ""
    finally:
        clients_secrets._SECRETS_RUN = saved


def test_slack_and_argo_resolvers_delegate_to_shared_secrets_module():
    from clients import secrets as clients_secrets
    assert argo._ARGO_TOKEN_REF == "op://common/api/SECRET"
    assert clients_slack._SLACK_TOKEN_REF == "op://hermes/slack/bot-token"
    assert clients_slack._WARDEN_SLACK_TOKEN_REF == "op://common/slack/WARDEN_BOT_TOKEN"

    calls: list[tuple[str, str]] = []

    def _fake_resolve(env_var, ref, *, timeout=15.0):
        calls.append((env_var, ref))
        # Warden's own pair resolves empty here so resolve_slack_token() falls
        # through to the Hermes pair — exercising both legs of its own
        # resolution order through this one shared fake, same as before.
        return "" if env_var == "WARDEN_SLACK_BOT_TOKEN" else "stub-token"

    saved = clients_secrets.resolve_secret
    saved_warned = clients_slack._warned_hermes_fallback
    argo.resolve_secret = _fake_resolve
    clients_slack.resolve_secret = _fake_resolve
    try:
        assert argo.resolve_argo_token() == "stub-token"
        assert clients_slack.resolve_slack_token() == "stub-token"
    finally:
        argo.resolve_secret = saved
        clients_slack.resolve_secret = saved
        clients_slack._warned_hermes_fallback = saved_warned
    assert ("ARGO_API_SECRET", "op://common/api/SECRET") in calls
    assert ("WARDEN_SLACK_BOT_TOKEN", "op://common/slack/WARDEN_BOT_TOKEN") in calls
    assert ("SLACK_BOT_TOKEN", "op://hermes/slack/bot-token") in calls


def test_resolve_slack_token_prefers_warden_env_over_everything():
    """`WARDEN_SLACK_BOT_TOKEN` in the env short-circuits before secrets-run
    is even consulted for either identity — the cheapest, most direct override
    a caller (or a test) can give it."""
    os.environ["WARDEN_SLACK_BOT_TOKEN"] = "warden-env-token"
    os.environ.pop("SLACK_BOT_TOKEN", None)
    try:
        assert clients_slack.resolve_slack_token() == "warden-env-token"
    finally:
        os.environ.pop("WARDEN_SLACK_BOT_TOKEN", None)


def test_resolve_slack_token_prefers_warden_ref_over_hermes_fallback():
    """Neither env var set: secrets-run resolves Warden's own ref
    (`op://common/slack/WARDEN_BOT_TOKEN`) before the Hermes fallback ref is
    ever tried — a seeded Warden app always wins, unconditionally."""
    from clients import secrets as clients_secrets
    stub = _tmp_secrets_run_by_ref({
        clients_slack._WARDEN_SLACK_TOKEN_REF: "warden-ref-token",
        clients_slack._SLACK_TOKEN_REF: "hermes-ref-token",
    })
    saved = clients_secrets._SECRETS_RUN
    clients_secrets._SECRETS_RUN = stub
    os.environ.pop("WARDEN_SLACK_BOT_TOKEN", None)
    os.environ.pop("SLACK_BOT_TOKEN", None)
    try:
        assert clients_slack.resolve_slack_token() == "warden-ref-token"
    finally:
        clients_secrets._SECRETS_RUN = saved


def test_resolve_slack_token_falls_back_to_hermes_and_warns_once():
    """Warden's identity unresolved on both env and ref: falls back to the
    Hermes token unchanged, and prints exactly one stderr line for it no
    matter how many times resolve_slack_token() is called in this process —
    the module-level `_warned_hermes_fallback` guard, not a per-call print."""
    from clients import secrets as clients_secrets
    stub = _tmp_secrets_run_by_ref({clients_slack._SLACK_TOKEN_REF: "hermes-ref-token"})
    saved_bin = clients_secrets._SECRETS_RUN
    saved_warned = clients_slack._warned_hermes_fallback
    clients_secrets._SECRETS_RUN = stub
    clients_slack._warned_hermes_fallback = False
    os.environ.pop("WARDEN_SLACK_BOT_TOKEN", None)
    os.environ.pop("SLACK_BOT_TOKEN", None)
    buf = io.StringIO()
    try:
        with contextlib.redirect_stderr(buf):
            assert clients_slack.resolve_slack_token() == "hermes-ref-token"
            assert clients_slack.resolve_slack_token() == "hermes-ref-token"
    finally:
        clients_secrets._SECRETS_RUN = saved_bin
        clients_slack._warned_hermes_fallback = saved_warned
    lines = [l for l in buf.getvalue().splitlines() if "Slack posting as Hermes" in l]
    assert len(lines) == 1, buf.getvalue()


def test_submit_revision_of_goes_out_as_params_revisionOf_only_when_set():
    srv = _StubServer({("POST", "/api/jobs"): (200, {"ok": True, "job": {"id": "j-rev"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        sideclaw.submit(cwd="/repo", tier="implement", brief="b", revision_of="dispatch/fix-1")
        sideclaw.submit(cwd="/repo", tier="implement", brief="b")
        assert srv.requests[0]["body"]["params"]["revisionOf"] == "dispatch/fix-1", srv.requests[0]["body"]
        assert "revisionOf" not in srv.requests[1]["body"]["params"], srv.requests[1]["body"]
    finally:
        srv.stop()


def test_dispatch_outcomes_include_pr_updated_and_conflict():
    for outcome in ("pr_updated", "conflict"):
        sideclaw.assert_outcome({"status": "done", "result": {"outcome": outcome}}, sideclaw.DISPATCH_OUTCOMES,
                                "implement")


def test_is_lease_refusal_matches_only_a_failed_job_with_the_lease_text():
    # sideclaw server/lib/repo-lease.ts repoLeaseRefusal(holder, tool), for both tools that take the lease.
    tail = ("an implement episode is already running in this repo (job abc) — implement episodes serialize "
            "per repo because their edits and pushes would interleave. Re-submit once it finishes.")
    lease = f"dispatch refused: {tail}"
    assert sideclaw.is_lease_refusal({"status": "failed", "error": lease})
    assert sideclaw.is_lease_refusal({"status": "failed", "error": f"update_pr refused: {tail}"})
    assert sideclaw.is_lease_refusal({"status": "failed", "error": f"Error: update_pr refused: {tail}"})
    assert not sideclaw.is_lease_refusal({"status": "failed", "error": "update_pr refused: PR #3 is closed, not open"})
    assert not sideclaw.is_lease_refusal({"status": "failed", "error": "worker crashed"})
    assert not sideclaw.is_lease_refusal({"status": "failed", "error": None})
    assert not sideclaw.is_lease_refusal({"status": "done", "error": lease})
    assert not sideclaw.is_lease_refusal({"status": "failed"})


def test_conflict_bundle_path_is_read_from_the_verdict_prose():
    path = "/tmp/dispatch-bundles/fix-1.bundle"
    assert sideclaw.conflict_bundle_path(
        {"verdict": f"Rebase conflicted. The episode's commits were bundled at {path}."}) == path
    assert sideclaw.conflict_bundle_path({"verdict": f"bundled at {path}"}) == path
    assert sideclaw.conflict_bundle_path({"verdict": f"bundled at {path}. More text follows."}) == path
    assert sideclaw.conflict_bundle_path({"verdict": "Rebase conflicted, nothing bundled."}) is None
    assert sideclaw.conflict_bundle_path({"summary": "no verdict key"}) is None


def _routing(routes: dict) -> dict:
    return {("GET", "/api/routing"): (200, {"routes": routes, "models": []})}


def test_escalation_model_reads_the_route_and_caches_it():
    sideclaw._escalation_cache.clear()
    srv = _StubServer(_routing({"dispatch_implement": {"model": "default-m", "backend": "x"},
                                "dispatch_implement_escalation": {"model": "strong-m", "backend": "x"}}))
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        assert sideclaw.escalation_model() == "strong-m"
        assert sideclaw.escalation_model() == "strong-m"
        assert len(srv.requests) == 1, "cached for the life of the process"
    finally:
        srv.stop()
        sideclaw._escalation_cache.clear()


def test_escalation_model_is_none_when_the_route_is_absent_or_the_call_fails():
    sideclaw._escalation_cache.clear()
    srv = _StubServer(_routing({"dispatch_implement": {"model": "default-m"}}))
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        with contextlib.redirect_stderr(io.StringIO()) as err:
            assert sideclaw.escalation_model() is None
        assert "dispatch_implement_escalation" in err.getvalue(), err.getvalue()
    finally:
        srv.stop()

    srv = _StubServer({("GET", "/api/routing"): (500, {"error": "boom"})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        with contextlib.redirect_stderr(io.StringIO()):
            assert sideclaw.escalation_model() is None
    finally:
        srv.stop()

    os.environ["WARDEN_SIDECLAW_BASE"] = f"http://127.0.0.1:{_closed_port()}"
    with contextlib.redirect_stderr(io.StringIO()):
        assert sideclaw.escalation_model() is None, "an unreachable sideclaw means no model key, not an error"
    assert sideclaw._escalation_cache == {}, "a failure is never cached"


# --- runner --------------------------------------------------------------------

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
    if len(tests) == 0:
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
