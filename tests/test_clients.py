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

from clients import github, rollout, signer, sideclaw  # noqa: E402
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


def test_parse_pr_url_good_and_bad():
    assert github.parse_pr_url("https://github.com/jkrumm/gamma/pull/42") == ("jkrumm", "gamma", 42)
    assert github.parse_pr_url("https://gitlab.com/jkrumm/gamma/pull/42") is None
    assert github.parse_pr_url("https://github.com/jkrumm/gamma/issues/42") is None
    assert github.parse_pr_url("not a url") is None


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
