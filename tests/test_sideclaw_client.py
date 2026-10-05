#!/usr/bin/env python3
"""Regression suite for the shared `_submit_job()` behind clients/sideclaw.py's
`submit_review`, `submit_triage` and `submit_update_pr`.

`submit_review` is covered in tests/test_clients.py; this pins the same contract for the
other two and the failure shapes all three share. Against an in-process stub server,
never a real network call.

Run: .venv/bin/python3 tests/test_sideclaw_client.py  (or: make test, from warden/)
"""

from __future__ import annotations

import os
import sys
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))
sys.path.insert(0, str(REPO / "tests"))

from clients import sideclaw  # noqa: E402
from clients.errors import RemoteError, SubmitRefused  # noqa: E402
from stubs import StubServer as _StubServer  # noqa: E402
from stubs import closed_port as _closed_port  # noqa: E402

_SUBMITS = (
    ("review", lambda: sideclaw.submit_review(cwd=Path("/repo"), pr=3)),
    ("triage", lambda: sideclaw.submit_triage(prompt="p", schema={"type": "object"})),
    ("update_pr", lambda: sideclaw.submit_update_pr(cwd="/repo", pr=3)),
)


def _with_stub(routes, fn):
    srv = _StubServer(routes)
    os.environ["WARDEN_SIDECLAW_BASE"] = srv.base
    try:
        return fn(srv)
    finally:
        srv.stop()


def test_triage_and_update_pr_body_shapes():
    def run(srv):
        assert sideclaw.submit_triage(prompt="p", schema={"type": "object"}) == {"id": "j1"}
        assert sideclaw.submit_update_pr(cwd=Path("/repo"), pr=9) == {"id": "j1"}
        assert srv.requests[0]["body"] == {
            "tool": "triage", "params": {"prompt": "p", "schema": {"type": "object"}}}, srv.requests[0]["body"]
        assert srv.requests[1]["body"] == {
            "tool": "update_pr", "params": {"cwd": "/repo", "pr": 9}}, srv.requests[1]["body"]
    _with_stub({("POST", "/api/jobs"): (200, {"ok": True, "job": {"id": "j1"}})}, run)


def test_a_4xx_is_submit_refused_for_every_submit():
    for tool, call in _SUBMITS:
        def run(srv):
            try:
                call()
            except SubmitRefused as e:
                assert e.status == 422 and "nope" in str(e), (tool, e)
            else:
                raise AssertionError(f"{tool}: expected SubmitRefused")
        _with_stub({("POST", "/api/jobs"): (422, {"ok": False, "error": "nope"})}, run)


def test_a_5xx_is_a_remote_error_for_every_submit():
    for tool, call in _SUBMITS:
        def run(srv):
            try:
                call()
            except RemoteError as e:
                assert "500" in str(e), (tool, e)
            else:
                raise AssertionError(f"{tool}: expected RemoteError")
        _with_stub({("POST", "/api/jobs"): (500, {"error": "boom"})}, run)


def test_a_200_without_a_job_id_names_the_tool():
    for tool, call in _SUBMITS:
        def run(srv):
            try:
                call()
            except RemoteError as e:
                assert f"accepted the {tool} job but returned no id" in str(e), (tool, e)
            else:
                raise AssertionError(f"{tool}: expected RemoteError")
        _with_stub({("POST", "/api/jobs"): (200, {"ok": True, "job": {}})}, run)


def test_an_unreachable_sideclaw_may_have_mutated_and_names_the_tool():
    os.environ["WARDEN_SIDECLAW_BASE"] = f"http://127.0.0.1:{_closed_port()}"
    for tool, call in _SUBMITS:
        try:
            call()
        except RemoteError as e:
            assert f"sideclaw {tool} submit failed" in str(e) and e.maybe_mutated is True, (tool, e)
        else:
            raise AssertionError(f"{tool}: expected RemoteError")


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
