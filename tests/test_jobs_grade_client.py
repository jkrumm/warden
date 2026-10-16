#!/usr/bin/env python3
"""Regression suite for scripts/clients/jobs_grade.py — the boundary to `jobs-grade`.

The contract is dotfiles' `scripts/lib/jobs-grade.ts` default output: a JSON array of
`{label, owner, grade, reason, rc, consecutive_fail, fails_24h, last_ok, last_started, every}`.
Failure is a return value, never an exception: None means "could not grade", and the loop reads it
as blind (opens nothing, resolves nothing). A partly malformed array is None too — dropping one row
would read as that job's recovery.

Real subprocesses run a stub script (never the installed `jobs-grade`); the injectable `runner`
covers the shapes a script cannot produce cheaply.

Run: .venv/bin/python3 tests/test_jobs_grade_client.py  (or: make test, from warden/)
"""

from __future__ import annotations

import contextlib
import io
import json
import os
import socket
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from clients import jobs_grade  # noqa: E402

# A row as `jobs-grade` prints it today.
SAMPLE = {
    "label": "com.jkrumm.brain-sync", "owner": "dotfiles", "rc": 0, "consecutive_fail": 0, "fails_24h": 0,
    "last_ok": "2026-10-16T11:14:20.196Z", "last_started": "2026-10-16T11:14:19.040Z", "every": 300,
    "grade": "ok", "reason": "ok",
}


@contextlib.contextmanager
def _stub(script: str, **env):
    """`WARDEN_JOBS_GRADE_BIN` pointing at an executable `script` (a /bin/sh body)."""
    tmp = Path(tempfile.mkdtemp(prefix="jobs-grade-test-"))
    binary = tmp / "jobs-grade"
    binary.write_text(f"#!/bin/sh\n{script}\n")
    binary.chmod(0o755)
    keys = ("WARDEN_JOBS_GRADE_BIN", "WARDEN_MACHINE")
    saved = {k: os.environ.get(k) for k in keys}
    os.environ["WARDEN_JOBS_GRADE_BIN"] = str(binary)
    os.environ.update(env)
    try:
        yield binary
    finally:
        for k, v in saved.items():
            if v is None:
                os.environ.pop(k, None)
            else:
                os.environ[k] = v


def _fetch_quietly(**kw):
    err = io.StringIO()
    with contextlib.redirect_stderr(err):
        rows = jobs_grade.fetch(**kw)
    return rows, err.getvalue()


def test_a_well_formed_array_comes_back_whole_with_extra_fields_untouched():
    rows = [SAMPLE, {**SAMPLE, "label": "com.jkrumm.x", "grade": "failing", "tail": ["boom"], "owner_sha": "abc"}]
    with _stub(f"cat <<'EOF'\n{json.dumps(rows)}\nEOF"):
        got, err = _fetch_quietly()
    assert got == rows and err == "", (got, err)


def test_an_empty_array_is_nothing_graded_not_a_failure():
    with _stub("echo '[]'"):
        got, err = _fetch_quietly()
    assert got == [] and err == ""


def test_a_missing_binary_is_none_with_one_log_line():
    with _stub("true") as binary:
        binary.unlink()
        got, err = _fetch_quietly()
    assert got is None and "could not run" in err and err.count("\n") == 1, err


def test_a_non_zero_exit_is_none_and_names_the_last_stderr_line():
    with _stub("echo 'first' >&2; echo 'bun: not found' >&2; exit 127"):
        got, err = _fetch_quietly()
    assert got is None and "exited 127" in err and "bun: not found" in err and "first" not in err, err


def test_a_hung_grader_is_cut_off_at_the_timeout():
    with _stub("sleep 5"):
        got, err = _fetch_quietly(timeout_s=0.3)
    assert got is None and "did not answer within" in err, err


def test_output_that_is_not_json_is_none():
    with _stub("echo 'jobs ok (6)'"):
        got, err = _fetch_quietly()
    assert got is None and "not JSON" in err, err


def test_output_that_is_not_an_array_of_rows_is_none():
    for body in ('{"label": "x"}', '"ok"', "null", "[1, 2]"):
        with _stub(f"echo '{body}'"):
            got, err = _fetch_quietly()
        assert got is None and "not an array" in err, (body, err)


def test_one_malformed_row_makes_the_whole_pass_blind():
    bad_rows = [
        {**SAMPLE, "grade": "great"},
        {**SAMPLE, "grade": None},
        {k: v for k, v in SAMPLE.items() if k != "label"},
        {k: v for k, v in SAMPLE.items() if k != "owner"},
        {**SAMPLE, "label": ""},
        {**SAMPLE, "owner": None},
    ]
    for bad in bad_rows:
        with _stub(f"echo '{json.dumps([SAMPLE, bad])}'"):
            got, err = _fetch_quietly()
        assert got is None, ("a malformed row was dropped, which would read as a recovery", bad)


def test_every_grade_the_grader_can_print_is_accepted():
    rows = [{**SAMPLE, "label": f"com.jkrumm.{g}", "grade": g} for g in jobs_grade.GRADES]
    with _stub(f"echo '{json.dumps(rows)}'"):
        got, _ = _fetch_quietly()
    assert got == rows
    assert jobs_grade.BAD_GRADES == ("failing", "stale", "flapping", "unloaded") and "ok" not in jobs_grade.BAD_GRADES


def test_the_runner_and_env_are_injectable_and_nothing_is_read_from_stdin():
    seen: dict = {}

    def runner(argv, **kw):
        seen.update(argv=argv, **kw)
        return subprocess.CompletedProcess(argv, 0, stdout="[]", stderr="")

    with _stub("exit 1") as binary:
        got = jobs_grade.fetch(runner=runner, env={"PATH": "/x"}, timeout_s=7)
    assert got == []
    assert seen["argv"] == [str(binary)] and seen["env"] == {"PATH": "/x"} and seen["timeout"] == 7
    assert seen["stdin"] == subprocess.DEVNULL and seen["capture_output"] and seen["text"]


def test_an_oserror_or_timeout_from_the_runner_is_none_not_a_raise():
    def boom(exc):
        def runner(argv, **kw):
            raise exc
        return runner

    for exc in (PermissionError("denied"), FileNotFoundError("gone"), subprocess.TimeoutExpired("x", 1)):
        got, err = _fetch_quietly(runner=boom(exc))
        assert got is None and "jobs-grade:" in err, (exc, err)


def test_the_binary_defaults_to_local_bin_and_the_env_overrides_it():
    saved = os.environ.pop("WARDEN_JOBS_GRADE_BIN", None)
    try:
        assert jobs_grade.bin_path() == Path.home() / ".local" / "bin" / "jobs-grade"
    finally:
        if saved is not None:
            os.environ["WARDEN_JOBS_GRADE_BIN"] = saved
    with _stub("true") as binary:
        assert jobs_grade.bin_path() == binary


def test_the_machine_is_the_short_lowercased_hostname_unless_the_env_names_one():
    with _stub("true", WARDEN_MACHINE="Mini"):
        assert jobs_grade.machine_name() == "mini"
    with _stub("true"):
        os.environ.pop("WARDEN_MACHINE", None)
        assert jobs_grade.machine_name() == (socket.gethostname().split(".")[0].strip().lower() or "unknown")


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
