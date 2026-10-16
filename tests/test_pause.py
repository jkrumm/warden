#!/usr/bin/env python3
"""Regression suite for scripts/lifecycle/pause.py — the kill-switch's flag file.

The flag is one JSON file (`reason`, `since`); the loop, the sweep, the CLI and the API all ask
this module and nothing else. What matters, and what is pinned here: a missing file is the ONLY
"running" answer (a file that cannot be read still pauses), the write is atomic, `pause` is
idempotent and keeps its original `since`, and the backstop under the dispatch functions refuses with
a `PolicyError` (exit 4) before any network call or ledger row.

The gating at every dispatch site is tests/test_triage.py's (it owns the loop fixtures); the CLI verbs
are tests/test_warden_cli.py's; `/health` is tests/test_api.py's.

Run: .venv/bin/python3 tests/test_pause.py  (or: make test, from warden/)
"""

from __future__ import annotations

import contextlib
import datetime as dt
import json
import os
import stat
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from clients import agent_gateway  # noqa: E402
from clients.errors import PolicyError  # noqa: E402
from lifecycle import dispatch, pause  # noqa: E402

NOW = dt.datetime(2030, 1, 2, 3, 4, 5, tzinfo=dt.timezone.utc)
_ENV = ("WARDEN_PAUSE_FILE", "WARDEN_HOME")


@contextlib.contextmanager
def _flag(**env):
    """A fresh flag path via WARDEN_PAUSE_FILE (or whatever `env` sets instead)."""
    tmp = Path(tempfile.mkdtemp(prefix="pause-test-"))
    saved = {k: os.environ.get(k) for k in _ENV}
    for k in _ENV:
        os.environ.pop(k, None)
    os.environ.update(env or {"WARDEN_PAUSE_FILE": str(tmp / "paused")})
    try:
        yield tmp
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def test_no_flag_file_means_running():
    with _flag():
        assert pause.state() is None and pause.is_paused() is False
        assert pause.describe() == "running"
        pause.require_running("anything")   # does not raise


def test_pause_writes_reason_and_since_and_state_reads_them_back():
    with _flag() as tmp:
        written = pause.pause("canary rollback", NOW)
        assert written == {"reason": "canary rollback", "since": "2030-01-02T03:04:05+00:00"}
        assert json.loads((tmp / "paused").read_text()) == written
        assert pause.state() == written and pause.is_paused() is True
        assert pause.describe() == "paused since 2030-01-02T03:04:05+00:00: canary rollback"


def test_pause_without_a_reason_is_valid():
    with _flag():
        assert pause.pause(None, NOW)["reason"] is None
        assert pause.pause("   ", NOW)["reason"] is None, "blank is no reason"
        assert pause.is_paused() and pause.describe().startswith("paused since ")


def test_pausing_a_paused_warden_keeps_its_since_and_takes_a_new_reason_only_when_given():
    with _flag():
        pause.pause("first", NOW)
        later = NOW + dt.timedelta(hours=5)
        kept = pause.pause(None, later)
        assert kept == {"reason": "first", "since": NOW.isoformat()}, kept
        swapped = pause.pause("second", later)
        assert swapped == {"reason": "second", "since": NOW.isoformat()}, swapped


def test_resume_removes_the_flag_and_reports_what_it_lifted():
    with _flag() as tmp:
        assert pause.resume() is None, "resuming a running warden is a no-op"
        pause.pause("x", NOW)
        lifted = pause.resume()
        assert lifted == {"reason": "x", "since": NOW.isoformat()}
        assert not (tmp / "paused").exists() and pause.is_paused() is False
        assert pause.resume() is None


def test_the_write_is_atomic_and_leaves_no_temp_file_behind():
    with _flag() as tmp:
        pause.pause("a", NOW)
        pause.pause("b", NOW)
        pause.resume()
        pause.pause("c", NOW)
        assert sorted(p.name for p in tmp.iterdir()) == ["paused"], list(tmp.iterdir())


def test_a_flag_that_cannot_be_parsed_still_pauses():
    """Fail closed: the kill-switch must not open because the file is half-written."""
    for body in ("", "not json", "[]", '"paused"', "null", "{"):
        with _flag() as tmp:
            (tmp / "paused").write_text(body)
            state = pause.state()
            assert state is not None and state["reason"] == pause.UNREADABLE_REASON and state["since"] is None, (body, state)
            assert pause.is_paused()


def test_a_flag_of_non_utf8_bytes_still_pauses():
    for body in (b"\xff\xfe\x00", b'{"reason": "\xe9"}', b"\x80"):
        with _flag() as tmp:
            (tmp / "paused").write_bytes(body)
            state = pause.state()
            assert state == {"reason": pause.UNREADABLE_REASON, "since": None}, (body, state)
            assert pause.is_paused()


def test_a_flag_that_cannot_be_read_still_pauses():
    with _flag() as tmp:
        (tmp / "paused").mkdir()   # a directory where the file should be: read_text raises OSError
        assert pause.is_paused() and pause.state()["reason"] == pause.UNREADABLE_REASON
    if os.geteuid() != 0:
        with _flag() as tmp:
            path = tmp / "paused"
            path.write_text("{}")
            path.chmod(0)
            try:
                assert pause.is_paused()
            finally:
                path.chmod(stat.S_IRUSR | stat.S_IWUSR)


def test_pausing_over_an_unreadable_flag_does_not_inherit_its_placeholder_reason():
    with _flag() as tmp:
        (tmp / "paused").write_text("garbage")
        assert pause.pause(None, NOW) == {"reason": None, "since": NOW.isoformat()}
        (tmp / "paused").write_text("garbage")
        assert pause.pause("real", NOW)["reason"] == "real"


def test_a_flag_with_odd_field_types_reads_as_no_reason_not_a_crash():
    with _flag() as tmp:
        (tmp / "paused").write_text(json.dumps({"reason": 5, "since": ["x"]}))
        assert pause.state() == {"reason": None, "since": None} and pause.is_paused()


def test_the_flag_path_is_env_first_then_warden_home_then_the_default():
    with _flag(WARDEN_PAUSE_FILE="/tmp/explicit-flag"):
        assert pause.flag_path() == Path("/tmp/explicit-flag")
    with _flag(WARDEN_HOME="/tmp/some-home"):
        assert pause.flag_path() == Path("/tmp/some-home/paused")
    with _flag(WARDEN_PAUSE_FILE="/tmp/explicit-flag", WARDEN_HOME="/tmp/some-home"):
        assert pause.flag_path() == Path("/tmp/explicit-flag"), "the specific override wins"
    with _flag(OTHER="1"):
        assert pause.flag_path() == Path.home() / ".warden" / "paused"


def test_pause_creates_the_missing_parent_directory():
    with _flag() as tmp:
        os.environ["WARDEN_PAUSE_FILE"] = str(tmp / "deep" / "er" / "paused")
        pause.pause("x", NOW)
        assert pause.is_paused()


def test_require_running_raises_a_policy_refusal_that_names_the_reason():
    with _flag():
        pause.pause("rolling back", NOW)
        try:
            pause.require_running("a review of demo#7")
        except pause.PausedError as e:
            assert isinstance(e, PolicyError) and e.exit_code == 4
            assert "rolling back" in str(e) and "a review of demo#7 refused" in str(e) and "warden resume" in str(e), str(e)
        else:
            raise AssertionError("require_running did not raise while paused")


def test_the_dispatch_backstop_refuses_before_any_network_call_or_row():
    import sqlite3
    conn = sqlite3.connect(":memory:")
    saved = (agent_gateway.submit, agent_gateway.submit_review)
    agent_gateway.submit = lambda **kw: (_ for _ in ()).throw(AssertionError("reached agent-gateway"))
    agent_gateway.submit_review = lambda **kw: (_ for _ in ()).throw(AssertionError("reached agent-gateway"))
    try:
        with _flag():
            pause.pause("backstop", NOW)
            for call in (
                lambda: dispatch.open_episode(conn, repo="demo", tier="implement", brief="b", context=None, why=None,
                                              origin=dispatch.Origin(), authorized_by="cli:dispatch"),
                lambda: dispatch.open_review(conn, repo="demo", pr=1, context=None, origin=dispatch.Origin()),
            ):
                try:
                    call()
                except pause.PausedError:
                    pass
                else:
                    raise AssertionError("dispatch went through while paused")
    finally:
        agent_gateway.submit, agent_gateway.submit_review = saved
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
