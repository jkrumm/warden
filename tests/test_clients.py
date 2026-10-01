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

from typing import Any

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

from clients import argo, github, rollout, signer, sideclaw  # noqa: E402
from clients import slack as clients_slack  # noqa: E402
from clients.errors import PolicyError, PreconditionError, RemoteError  # noqa: E402

from cryptography.hazmat.primitives.asymmetric.ed25519 import (  # noqa: E402
    Ed25519PrivateKey,
)

SPEC_PATH = REPO / "config" / "approval-spec.json"


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
        sideclaw.submit(cwd="/repo", tier="implement", brief="b", context="ctx", sensitive=True, model="haiku")
        params = srv.requests[0]["body"]["params"]
        assert list(params.keys()) == ["cwd", "tier", "brief", "context", "sensitive", "model"], params
        assert params["context"] == "ctx" and params["sensitive"] is True and params["model"] == "haiku"
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
        assert e.maybe_mutated is True, "a submit failure is ambiguous, must be flagged"
    else:
        raise AssertionError("expected RemoteError")


def test_submit_no_id_raises_remote_error():
    srv = _StubServer({("POST", "/api/jobs"): (200, {"ok": True, "job": {"status": "running"}})})
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        try:
            sideclaw.submit(cwd="/repo", tier="investigate", brief="x")
        except RemoteError as e:
            assert "no id" in str(e), e
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


# `/api/review-schema`'s real body, abridged to what the check reads: the published finding
# object lives at `output.properties.blocking.items`, and the producer requires `angle` while
# warden deliberately does not (see `REVIEW_FINDING_REQUIRED`).
_REVIEW_SCHEMA_OUTPUT = {
    "type": "object",
    "properties": {
        "outcome": {"type": "string", "enum": list(sideclaw.REVIEW_OUTCOMES)},
        "blocking": {"type": "array", "items": {
            "type": "object",
            "properties": {"file": {"type": "string"}, "line": {"type": "number"},
                           "message": {"type": "string"}, "angle": {"type": "string"}},
            "required": ["file", "message", "angle"],
            "additionalProperties": False,
        }},
    },
}


def test_check_schema_versions_ok():
    srv = _StubServer({
        ("GET", "/api/dispatch-schema"): (200, {
            "ok": True, "version": sideclaw.DISPATCH_SCHEMA_VERSION,
            "outcomes": list(sideclaw.DISPATCH_OUTCOMES),
        }),
        ("GET", "/api/review-schema"): (200, {
            "ok": True, "version": sideclaw.REVIEW_SCHEMA_VERSION,
            "outcomes": list(sideclaw.REVIEW_OUTCOMES),
            "output": _REVIEW_SCHEMA_OUTPUT,
        }),
    })
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        results = sideclaw.check_schema_versions()
        assert results["dispatch"]["reachable"] is True and results["dispatch"]["ok"] is True, results
        assert results["review"]["reachable"] is True and results["review"]["ok"] is True, results
    finally:
        srv.stop()


def test_check_schema_versions_version_mismatch():
    srv = _StubServer({
        ("GET", "/api/dispatch-schema"): (200, {
            "ok": True, "version": sideclaw.DISPATCH_SCHEMA_VERSION + 1,
            "outcomes": list(sideclaw.DISPATCH_OUTCOMES),
        }),
        ("GET", "/api/review-schema"): (200, {
            "ok": True, "version": sideclaw.REVIEW_SCHEMA_VERSION,
            "outcomes": list(sideclaw.REVIEW_OUTCOMES),
            "output": _REVIEW_SCHEMA_OUTPUT,
        }),
    })
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        results = sideclaw.check_schema_versions()
        assert results["dispatch"]["ok"] is False, results
        assert results["review"]["ok"] is True, results
    finally:
        srv.stop()


def test_check_schema_versions_outcome_set_mismatch():
    """A version match with a DIFFERENT outcome set is still a mismatch —
    sideclaw could add/rename an outcome without bumping the version, and a
    consumer that only compared `version` would silently miss it."""
    srv = _StubServer({
        ("GET", "/api/dispatch-schema"): (200, {
            "ok": True, "version": sideclaw.DISPATCH_SCHEMA_VERSION,
            "outcomes": [*sideclaw.DISPATCH_OUTCOMES, "a_new_outcome"],
        }),
        ("GET", "/api/review-schema"): (200, {
            "ok": True, "version": sideclaw.REVIEW_SCHEMA_VERSION,
            "outcomes": list(sideclaw.REVIEW_OUTCOMES),
            "output": _REVIEW_SCHEMA_OUTPUT,
        }),
    })
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        results = sideclaw.check_schema_versions()
        assert results["dispatch"]["ok"] is False, results
    finally:
        srv.stop()


def test_check_schema_versions_catches_a_finding_shape_that_lost_a_required_field():
    """§131: version and outcomes can agree while the FINDING shape differs, and that was the one
    piece of the published contract nothing compared — `is_review_finding()` mirrors it in Python,
    so a producer that renamed `message` would leave warden reading a shape nobody emits, with
    every other check green and the missing field only visible as an unusable verdict later."""
    output = json.loads(json.dumps(_REVIEW_SCHEMA_OUTPUT))  # deep copy, no shared mutation
    items = output["properties"]["blocking"]["items"]
    items["required"] = ["file", "angle"]
    items["properties"].pop("message")
    srv = _StubServer({
        ("GET", "/api/dispatch-schema"): (200, {
            "ok": True, "version": sideclaw.DISPATCH_SCHEMA_VERSION,
            "outcomes": list(sideclaw.DISPATCH_OUTCOMES),
        }),
        ("GET", "/api/review-schema"): (200, {
            "ok": True, "version": sideclaw.REVIEW_SCHEMA_VERSION,
            "outcomes": list(sideclaw.REVIEW_OUTCOMES), "output": output,
        }),
    })
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        review = sideclaw.check_schema_versions()["review"]
        assert review["ok"] is False, review
        shape = review["findingShape"]
        assert shape["missingFromProperties"] == ["message"], shape
        assert shape["missingFromRequired"] == ["message"], shape
        assert set(shape["wardenRequires"]) == {"file", "message"}, shape
    finally:
        srv.stop()


def test_check_schema_versions_treats_an_unpublished_finding_shape_as_a_disagreement():
    """No readable shape is a finding, not a reason to skip: the alternative is warden's copy
    being unverifiable, which is exactly the drift the comparison exists to catch."""
    srv = _StubServer({
        ("GET", "/api/dispatch-schema"): (200, {
            "ok": True, "version": sideclaw.DISPATCH_SCHEMA_VERSION,
            "outcomes": list(sideclaw.DISPATCH_OUTCOMES),
        }),
        ("GET", "/api/review-schema"): (200, {
            "ok": True, "version": sideclaw.REVIEW_SCHEMA_VERSION,
            "outcomes": list(sideclaw.REVIEW_OUTCOMES),
        }),
    })
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        review = sideclaw.check_schema_versions()["review"]
        assert review["ok"] is False, review
        assert review["findingShape"]["published"] is False, review
        # And it says WHICH requirement the body failed, not only that it failed one. This stub
        # publishes no `output` at all, so the reason names that level.
        assert "output" in review["findingShape"]["reason"], review["findingShape"]
    finally:
        srv.stop()


def test_a_blocking_container_that_is_no_longer_an_array_fails_the_schema_check():
    """§135 — the field comparison this check already had, run against a producer that keeps
    every field NAME and changes only the container around them. `output.properties.blocking`
    becomes an object (or admits `null`), the `items` object stays exactly as it was, and the
    check used to report success — while `_review_verdict_problems()` iterates `blocking` as a
    list and would reject every verdict the producer emits, silently."""
    for blocking_type in ("object", ["array", "null"]):
        output = json.loads(json.dumps(_REVIEW_SCHEMA_OUTPUT))
        output["properties"]["blocking"]["type"] = blocking_type
        srv = _StubServer({
            ("GET", "/api/dispatch-schema"): (200, {
                "ok": True, "version": sideclaw.DISPATCH_SCHEMA_VERSION,
                "outcomes": list(sideclaw.DISPATCH_OUTCOMES),
            }),
            ("GET", "/api/review-schema"): (200, {
                "ok": True, "version": sideclaw.REVIEW_SCHEMA_VERSION,
                "outcomes": list(sideclaw.REVIEW_OUTCOMES), "output": output,
            }),
        })
        os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
        try:
            review = sideclaw.check_schema_versions()["review"]
        finally:
            srv.stop()
        assert review["ok"] is False, (blocking_type, review)
        shape = review["findingShape"]
        assert shape["published"] is False, (blocking_type, shape)
        assert "blocking" in shape["reason"], shape


def test_the_warden_required_finding_fields_are_the_ones_the_live_schema_publishes():
    """The comparison is only meaningful if both sides name the same fields, so this pins
    warden's side against the stub's published shape rather than against prose."""
    published = set(_REVIEW_SCHEMA_OUTPUT["properties"]["blocking"]["items"]["properties"])
    assert set(sideclaw.REVIEW_FINDING_REQUIRED) <= published, sideclaw.REVIEW_FINDING_REQUIRED
    # And the deliberately-lenient direction is explicit: the producer requires a field warden
    # does not, and that is tolerated (reported, never a mismatch).
    extra = set(_REVIEW_SCHEMA_OUTPUT["properties"]["blocking"]["items"]["required"]) \
        - set(sideclaw.REVIEW_FINDING_REQUIRED)
    assert extra == {"angle"}, extra


def test_the_published_finding_shape_never_raises_on_a_truthy_non_object():
    """§132 — `parsed.get("output") or {}` survives `None` and `{}` but not a truthy non-dict,
    and this helper sits OUTSIDE `check_schema_versions()`'s try/except on the operator-facing
    `make status` path: an `AttributeError` there would turn a drifted schema into a traceback
    instead of the "unreadable shape" refusal it is documented to produce."""
    # The refusal states WHICH requirement the body failed, so this reads the reason instead of
    # only asserting a refusal: a body with no `blocking` at all and one whose `blocking` stopped
    # being an array are both refused, and telling them apart is the difference between a check
    # that explains a red and one that only reports it (§135).
    def reason(parsed: Any) -> str:
        shape = sideclaw._published_finding_shape(parsed)
        assert isinstance(shape, sideclaw.UnreadableFindingShape), shape
        return shape.reason

    assert "`output`" in reason({"output": "yes"})
    assert "`output`" in reason({"output": ["x"]})
    assert "`output`" in reason({"output": 7})
    assert "`output`" in reason({"output": None})
    assert "`output`" in reason({})
    # One level down, the same case the first guard covers: `properties` reached with `or {}`
    # raises on a truthy non-dict, and it is the level a drifted body is most likely to break.
    assert "properties" in reason({"output": {"properties": "yes"}})
    assert "blocking" in reason({"output": {"properties": {"blocking": ["x"]}}})
    # `blocking` typed but its `items` unreadable: the guards reach the level below too.
    assert "items" in reason(
        {"output": {"properties": {"blocking": {"type": "array", "items": "x"}}}})

    def body(blocking_type: Any, items_type: Any) -> Any:
        """A published body whose FIELD NAMES never change — only the containers around them."""
        return {"output": {"properties": {"blocking": {
            "type": blocking_type,
            "items": {"type": items_type,
                      "properties": {"file": {"type": "string"}, "message": {"type": "string"}},
                      "required": ["file", "message"]}}}}}

    # The container is part of the contract (§135). Every field name stays green in these bodies,
    # which is exactly why comparing names alone passed them while the runtime rejected every
    # verdict: `_review_verdict_problems()` iterates `blocking` as a list, and reads `items`'
    # fields by name. `["array", "null"]` admits the null it would then iterate.
    assert "blocking" in reason(body("object", "object"))
    assert "blocking" in reason(body(["array", "null"], "object"))
    assert "blocking" in reason(body(None, "object"))
    assert "items" in reason(body("array", "string"))
    assert "items" in reason(body("array", ["object", "null"]))
    assert "items" in reason(body("array", None))
    # …while the containers the producer actually publishes read as a comparison, so this stays a
    # check rather than a refusal of the live shape.
    live = sideclaw._published_finding_shape(body("array", "object"))
    assert isinstance(live, sideclaw.PublishedFindingShape), live
    assert live.required == frozenset({"file", "message"})
    # `required` is read as a collection of names or not at all: a bare string would become a
    # set of its own characters (silently passing a name check it should fail) and a number
    # would raise TypeError out of a function that is documented never to raise.
    for bad in ("file", 7, {"file": 1}, [7], ["file", 7]):
        why = reason({"output": {"properties": {"blocking": {"type": "array", "items": {
            "type": "object",
            "properties": {"file": {}, "message": {}}, "required": bad}}}}})
        assert "required" in why or "items" in why, (bad, why)
    # An empty `properties` carries no shape either — that is "not published", not "nothing
    # required".
    assert "publishes no fields" in reason(
        {"output": {"properties": {"blocking": {"type": "array", "items": {
            "type": "object", "properties": {}, "required": []}}}}})
    # …and the same shape read from a well-formed body still works, types included.
    shape = sideclaw._published_finding_shape({"output": _REVIEW_SCHEMA_OUTPUT})
    assert isinstance(shape, sideclaw.PublishedFindingShape), shape
    assert shape.required == frozenset({"file", "message", "angle"})
    assert shape.properties >= {"file", "message", "line", "angle"}
    assert shape.types["file"] == "string" and shape.types["line"] == "number"


def test_check_schema_versions_catches_a_field_that_was_retyped_but_not_renamed():
    """§133 — names are not the whole contract. A producer that keeps `file` and `message` and
    changes either type leaves every name in place: the version still matches, the outcome list
    still matches, every field is still published, and `is_review_finding()` rejects every
    finding the producer emits — the verdict silently unreadable, with the check that exists to
    catch exactly this reporting success. Types are compared, and a property with no readable
    type is not assumed to be a string."""
    retyped = json.loads(json.dumps(_REVIEW_SCHEMA_OUTPUT))
    retyped["properties"]["blocking"]["items"]["properties"]["file"] = {"type": "array"}
    srv = _StubServer({
        ("GET", "/api/dispatch-schema"): (200, {
            "ok": True, "version": sideclaw.DISPATCH_SCHEMA_VERSION,
            "outcomes": list(sideclaw.DISPATCH_OUTCOMES),
        }),
        ("GET", "/api/review-schema"): (200, {
            "ok": True, "version": sideclaw.REVIEW_SCHEMA_VERSION,
            "outcomes": list(sideclaw.REVIEW_OUTCOMES), "output": retyped,
        }),
    })
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        review = sideclaw.check_schema_versions()["review"]
        assert review["ok"] is False, review
        assert review["findingShape"]["mistyped"] == ["file"], review["findingShape"]
        assert review["findingShape"]["missingFromProperties"] == []
    finally:
        srv.stop()

    # A published property with NO readable type is a disagreement too: warden reads a string,
    # and "any type" is not a promise it can rely on.
    untyped = json.loads(json.dumps(_REVIEW_SCHEMA_OUTPUT))
    untyped["properties"]["blocking"]["items"]["properties"]["message"] = {}
    srv = _StubServer({
        ("GET", "/api/dispatch-schema"): (200, {
            "ok": True, "version": sideclaw.DISPATCH_SCHEMA_VERSION,
            "outcomes": list(sideclaw.DISPATCH_OUTCOMES),
        }),
        ("GET", "/api/review-schema"): (200, {
            "ok": True, "version": sideclaw.REVIEW_SCHEMA_VERSION,
            "outcomes": list(sideclaw.REVIEW_OUTCOMES), "output": untyped,
        }),
    })
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        review = sideclaw.check_schema_versions()["review"]
        assert review["ok"] is False and review["findingShape"]["mistyped"] == ["message"]
    finally:
        srv.stop()


def test_check_schema_versions_unreachable_never_raises():
    os.environ["WARDEN_SIDECLAW_BASE"] = f"http://127.0.0.1:{_closed_port()}"
    results = sideclaw.check_schema_versions()
    assert results["dispatch"] == {"reachable": False, "ok": False}, results
    assert results["review"] == {"reachable": False, "ok": False}, results


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


def test_merge_pr_409_raises_policy_error():
    srv = _StubServer({("PUT", "/repos/jkrumm/gamma/pulls/1/merge"): (409, {"message": "conflict"})})
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


def test_contents_base64_decodes():
    import base64
    encoded = base64.b64encode(b'{"name": "alert"}').decode()
    srv = _StubServer({("GET", "/repos/jkrumm/gamma/contents/a.json?ref=deadbeef"): (200, {"content": encoded})})
    _gh_env(srv)
    try:
        raw = github.contents("jkrumm", "gamma", "a.json", ref="deadbeef")
        assert raw == b'{"name": "alert"}'
    finally:
        srv.stop()
        _gh_cleanup()


def test_contents_non200_returns_none():
    srv = _StubServer({("GET", "/repos/jkrumm/gamma/contents/missing.json?ref=deadbeef"): (404, {"message": "not found"})})
    _gh_env(srv)
    try:
        assert github.contents("jkrumm", "gamma", "missing.json", ref="deadbeef") is None
    finally:
        srv.stop()
        _gh_cleanup()


def test_contents_quotes_path_and_ref_with_question_hash_traversal_and_space():
    """A `path`/`ref` containing '?', '#', '../', or a space must reach the
    request URL-encoded — never break the path segment or the query string,
    or reach an unintended resource. `path` is quoted with `safe='/'`
    (legitimate embedded slashes survive); `ref` is quoted fully."""
    import base64
    import urllib.parse
    quoted_path = urllib.parse.quote("dir with space/a?b#c../d.json", safe="/")
    quoted_ref = urllib.parse.quote("weird ref#1", safe="")
    srv = _StubServer({
        ("GET", f"/repos/jkrumm/gamma/contents/{quoted_path}?ref={quoted_ref}"):
            (200, {"content": base64.b64encode(b"ok").decode()}),
    })
    _gh_env(srv)
    try:
        raw = github.contents("jkrumm", "gamma", "dir with space/a?b#c../d.json", ref="weird ref#1")
        assert raw == b"ok", "the quoted request must reach the exact stubbed route"
    finally:
        srv.stop()
        _gh_cleanup()


def test_actions_runs_ok():
    srv = _StubServer({("GET", f"/repos/jkrumm/gamma/actions/runs?head_sha={_FULL_SHA}&per_page=20"): (200, {"workflow_runs": [{"id": 1}]})})
    _gh_env(srv)
    try:
        assert github.actions_runs("jkrumm", "gamma", head_sha=_FULL_SHA) == [{"id": 1}]
    finally:
        srv.stop()
        _gh_cleanup()


def test_actions_runs_invalid_sha_raises_precondition_error_before_any_request():
    srv = _StubServer({})
    _gh_env(srv)
    try:
        try:
            github.actions_runs("jkrumm", "gamma", head_sha="not-a-sha")
        except PreconditionError:
            pass
        else:
            raise AssertionError("expected PreconditionError")
        assert srv.requests == [], "a malformed sha must never reach a request"
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


def test_required_approving_reviews_reads_the_ruleset():
    assert github.required_approving_reviews([]) == 0
    assert github.required_approving_reviews(
        [{"type": "pull_request", "parameters": {"required_approving_review_count": 0}}]) == 0
    assert github.required_approving_reviews(
        [{"type": "deletion", "parameters": None},
         {"type": "pull_request", "parameters": {"required_approving_review_count": 2}}]) == 2


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
    true answer, and refusing made weatherorb (private, `autoMergePaths:
    ["**"]`, `autoDeploy`) permanently unmergeable — items 1276 and 1277
    parked on this 403 while their reviews had confirmed. GitHub's own
    `mergeable_state == "blocked"` and the merge call stay the enforcement
    point, so an unreadable-but-real protection still cannot be ridden."""
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
    so the *caller* can weigh the repo's own `noCiRequired`; every other
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


# --- rollout -----------------------------------------------------------------

def test_rollout_unknown_key_raises_policy_error():
    try:
        rollout.run("not-a-real-key")
    except PolicyError as e:
        assert "not-a-real-key" in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_rollout_known_key_argv():
    assert rollout.argv_for("hyperdx-apply") == ("ssh", "vps", "cd ~/vps && make hyperdx-apply ENV=prod")
    assert rollout.argv_for("unknown") is None


def test_rollout_weatherorb_pull_installs_plists_then_restarts_the_daemons():
    """§105/§107: a fast-forward rolls out the periodic jobs; ops/*.plist may
    now merge unattended, so the repo's own idempotent `make launchd-install`
    (bootout+bootstrap only for a changed plist — the one reload that re-reads
    one) runs next; then tileserver and sync, KeepAlive daemons that keep their
    loaded process, are kickstarted, or a merged tileserver fix would read
    `fixed` against a watchdog that never ran it. Closed argv: one /bin/sh -c
    string in that order, every step chained on the previous one, and serve —
    the vendored binary a merge cannot change — is not touched."""
    argv = rollout.argv_for("weatherorb-pull")
    assert argv[:2] == ("/bin/sh", "-c") and len(argv) == 3
    script = argv[2]
    pull, install, kick = script.index("pull --ff-only"), script.index("launchd-install"), script.index("kickstart -k")
    assert pull < install < kick
    assert script.count(" && ") >= 2 and "|| exit 1" in script
    for job in ("tileserver", "sync"):
        assert job in script, job
    assert "serve" not in script.replace("tileserver", "")
    assert "FORCE" not in script  # never bounce all eight; only a changed plist reloads


def test_rollout_run_success():
    def fake_runner(argv, **kwargs):
        assert argv == list(rollout.ROLLOUTS["hyperdx-apply"])
        assert kwargs.get("capture_output") is True and kwargs.get("text") is True
        return subprocess.CompletedProcess(argv, 0, stdout="ok\n", stderr="")

    result = rollout.run("hyperdx-apply", runner=fake_runner)
    assert result.ok is True and result.exit_code == 0 and "ok" in result.output


def test_rollout_run_timeout():
    def fake_runner(argv, **kwargs):
        raise subprocess.TimeoutExpired(cmd=argv, timeout=kwargs.get("timeout"))

    result = rollout.run("hyperdx-apply", timeout_s=5, runner=fake_runner)
    assert result.ok is False and result.exit_code == 124 and "5" in result.output


def test_rollout_run_oserror_never_raises_out_of_the_actuator():
    def fake_runner(argv, **kwargs):
        raise FileNotFoundError("ssh: no such file or directory")

    result = rollout.run("hyperdx-apply", runner=fake_runner)
    assert result.ok is False and result.exit_code == 127
    assert "no such file" in result.output


def test_rollout_run_failure_output_truncated_to_2000():
    def fake_runner(argv, **kwargs):
        return subprocess.CompletedProcess(argv, 1, stdout="x" * 3000, stderr="y" * 3000)

    result = rollout.run("hyperdx-apply", runner=fake_runner)
    assert result.ok is False and result.exit_code == 1 and len(result.output) == 2000


# --- signer / approval spec ---------------------------------------------------

def test_spec_payload_hash_vectors():
    spec = json.loads(SPEC_PATH.read_text())
    for v in spec["vectors"]["payloadHash"]:
        got = signer.payload_hash(v["verb"], v["repo"], v["tier"], v["body"], v["why"], v["context"])
        assert got == v["expected"], (v, got)


def test_spec_signed_message_vectors():
    spec = json.loads(SPEC_PATH.read_text())
    for v in spec["vectors"]["signedMessage"]:
        got = signer.canonical_message(v["nonce"], v["payload_hash"], v["decision"], v["decided_by"], v["expires_at"])
        assert got.hex() == v["expectedHex"], (v, got.hex())


def test_spec_key_id_vector():
    spec = json.loads(SPEC_PATH.read_text())
    for v in spec["vectors"]["keyId"]:
        assert signer.key_id(v["publicKeyHex"]) == v["expected"], v


def test_signer_round_trip():
    priv = Ed25519PrivateKey.generate()
    from cryptography.hazmat.primitives import serialization
    pub_hex = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw,
    ).hex()
    msg = signer.canonical_message("nonce123", "hash456", "approve", "U1", "2026-09-10T12:00:00+00:00")
    sig_hex = priv.sign(msg).hex()
    assert signer.verify(pub_hex, sig_hex, msg) is True


def test_signer_wrong_key_fails():
    from cryptography.hazmat.primitives import serialization
    priv = Ed25519PrivateKey.generate()
    other = Ed25519PrivateKey.generate()
    wrong_pub_hex = other.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw,
    ).hex()
    msg = signer.canonical_message("nonce123", "hash456", "approve", "U1", "2026-09-10T12:00:00+00:00")
    sig_hex = priv.sign(msg).hex()
    assert signer.verify(wrong_pub_hex, sig_hex, msg) is False


def test_signer_garbage_hex_fails():
    assert signer.verify("not-hex", "also-not-hex", b"whatever") is False
    assert signer.verify("aa" * 32, "not-hex", b"whatever") is False


def test_load_pubkey_missing_raises_policy_error():
    missing = Path(tempfile.mkdtemp(prefix="clients-pubkey-")) / "no-such-file.pub"
    try:
        signer.load_pubkey(missing)
    except PolicyError as e:
        assert str(missing) in str(e), e
    else:
        raise AssertionError("expected PolicyError")


def test_load_pubkey_malformed_raises_precondition_error():
    bad = Path(tempfile.mkdtemp(prefix="clients-pubkey-")) / "bad.pub"
    bad.write_text("not-hex-at-all", encoding="utf-8")
    try:
        signer.load_pubkey(bad)
    except PreconditionError:
        pass
    else:
        raise AssertionError("expected PreconditionError")


def test_load_pubkey_valid_round_trip():
    from cryptography.hazmat.primitives import serialization
    priv = Ed25519PrivateKey.generate()
    pub_hex = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw,
    ).hex()
    path = Path(tempfile.mkdtemp(prefix="clients-pubkey-")) / "good.pub"
    path.write_text(pub_hex + "\n", encoding="utf-8")
    assert signer.load_pubkey(path) == pub_hex


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



def test_interactive_token_is_hermes_never_the_chat_write_only_warden_app():
    """§101: buttons posted under the Warden app (chat:write only, no
    interactivity) could never be clicked through to Hermes's plugin."""
    calls: list[tuple[str, str]] = []
    saved = clients_slack.resolve_secret
    clients_slack.resolve_secret = lambda env_var, ref: calls.append((env_var, ref)) or "hermes-token"
    try:
        assert clients_slack.resolve_interactive_token() == "hermes-token"
    finally:
        clients_slack.resolve_secret = saved
    assert calls == [("SLACK_BOT_TOKEN", "op://hermes/slack/bot-token")]


if __name__ == "__main__":
    sys.exit(main())
