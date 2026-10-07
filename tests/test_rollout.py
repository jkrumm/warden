#!/usr/bin/env python3
"""Regression suite for scripts/lifecycle/rollout.py — deploy and verify through the repo's own
Makefile: has-target detection, the checkout fast-forward precondition, the fixed argv, timeouts
and the bounded output tail.

Every subprocess goes through the module's injectable `runner`; nothing here touches git or make.

Run: .venv/bin/python3 tests/test_rollout.py  (or: make test, from warden/)
"""

from __future__ import annotations

import os
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from lifecycle import rollout  # noqa: E402

CWD = Path("/repos/demo")


class Script:
    """A runner answering by the command's tail: `answers` maps a substring of the joined argv to
    (returncode, stdout, stderr); every argv is recorded. An unmatched command exits 0, empty."""

    def __init__(self, answers: dict[str, tuple[int, str, str] | Exception]):
        self.answers = answers
        self.argvs: list[list[str]] = []
        self.kwargs: list[dict[str, Any]] = []

    def __call__(self, argv, **kwargs):
        self.argvs.append(list(argv))
        self.kwargs.append(kwargs)
        joined = " ".join(argv)
        for needle, answer in self.answers.items():
            if needle in joined:
                if isinstance(answer, Exception):
                    raise answer
                code, out, err = answer
                return subprocess.CompletedProcess(argv, code, stdout=out, stderr=err)
        return subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    def ran(self, needle: str) -> bool:
        return any(needle in " ".join(a) for a in self.argvs)


_CLEAN_ON_MAIN = {
    "symbolic-ref": (0, "origin/main\n", ""),
    "rev-parse --abbrev-ref HEAD": (0, "main\n", ""),
    "status --porcelain": (0, "", ""),
    "rev-parse HEAD origin/": (0, "c0ffee\nc0ffee\n", ""),
}


def test_has_target_true_when_make_resolves_the_target():
    run = Script({"-n deploy": (0, "echo deploying\n", "")})
    assert rollout.has_target(CWD, "deploy", runner=run) is True
    assert run.argvs == [["make", "-C", str(CWD), "-n", "deploy"]]
    assert run.kwargs[0]["env"]["LC_ALL"] == "C" and run.kwargs[0]["timeout"] == rollout.PROBE_TIMEOUT_S


def test_has_target_false_on_no_rule_to_make_target():
    run = Script({"-n verify": (2, "", "make: *** No rule to make target `verify'.  Stop.\n")})
    assert rollout.has_target(CWD, "verify", runner=run) is False


def test_has_target_false_when_there_is_no_makefile_at_all():
    run = Script({"-n deploy": (2, "", "make: *** No targets specified and no makefile found.  Stop.\n")})
    assert rollout.has_target(CWD, "deploy", runner=run) is False


def test_has_target_false_on_make_381_quoting_of_the_target_itself():
    run = Script({"-n deploy": (2, "", "make: *** No rule to make target `deploy'.  Stop.\n")})
    assert rollout.has_target(CWD, "deploy", runner=run) is False


def test_a_missing_prerequisite_of_the_target_reads_as_present_never_as_no_target():
    for err in ("make: *** No rule to make target 'build/app', needed by 'deploy'.  Stop.\n",
                "make: *** No rule to make target `build/app', needed by `deploy'.  Stop.\n"):
        run = Script({"-n deploy": (2, "", err)})
        assert rollout.has_target(CWD, "deploy", runner=run) is True, err


def test_has_target_reads_any_other_failure_as_present_so_the_real_run_reports_it():
    run = Script({"-n deploy": (2, "", "Makefile:3: *** missing separator.  Stop.\n")})
    assert rollout.has_target(CWD, "deploy", runner=run) is True
    assert rollout.has_target(CWD, "deploy", runner=Script({"-n": subprocess.TimeoutExpired("make", 1)})) is True


def test_has_target_refuses_a_target_that_is_not_deploy_or_verify():
    try:
        rollout.has_target(CWD, "rm-rf", runner=Script({}))
    except ValueError:
        return
    raise AssertionError("expected ValueError")


def test_sync_fast_forwards_to_origin_and_checks_it_landed_there():
    run = Script(_CLEAN_ON_MAIN)
    assert rollout.sync_checkout(CWD, runner=run) == rollout.Synced("c0ffee", "c0ffee")  # nothing merged
    cmds = [" ".join(a) for a in run.argvs]
    assert cmds[0] == f"git -C {CWD} fetch --quiet origin"
    assert cmds[-2] == f"git -C {CWD} merge --ff-only origin/main"
    assert cmds[-1] == f"git -C {CWD} rev-parse HEAD origin/main"


def test_sync_is_deferred_when_the_checkout_is_ahead_of_origin():
    # `merge --ff-only` answers "Already up to date" for a checkout with unpushed commits: those
    # must never be deployed.
    run = Script({**_CLEAN_ON_MAIN, "rev-parse HEAD origin/": (0, "1111111111111111\n2222222222222222\n", "")})
    result = rollout.sync_checkout(CWD, runner=run)
    assert isinstance(result, rollout.Deferred) and "not at origin/main" in result.reason, result
    assert "111111111111" in result.reason and "222222222222" in result.reason, result.reason


def test_deploy_runs_only_make_deploy_with_the_deploy_timeout():
    run = Script({"make": (0, "deployed ok\n", "")})
    result = rollout.deploy(CWD, runner=run)
    assert result == rollout.Ran(True, 0, "deployed ok\n"), result
    assert run.argvs == [["make", "-C", str(CWD), "deploy"]], "the caller syncs; deploy() only runs make"
    assert run.kwargs[-1]["timeout"] == rollout.DEPLOY_TIMEOUT_S == 900


def test_sync_is_deferred_when_the_checkout_is_on_another_branch():
    run = Script({**_CLEAN_ON_MAIN, "rev-parse --abbrev-ref HEAD": (0, "feature/x\n", "")})
    result = rollout.sync_checkout(CWD, runner=run)
    assert isinstance(result, rollout.Deferred), result
    assert "checkout not clean/on main" in result.reason and "feature/x" in result.reason, result.reason
    assert not run.ran("merge"), "nothing may run against a checkout we did not touch"


def test_sync_is_deferred_when_the_checkout_has_tracked_changes():
    run = Script({**_CLEAN_ON_MAIN, "status --porcelain": (0, " M scripts/app.py\n", "")})
    result = rollout.sync_checkout(CWD, runner=run)
    assert isinstance(result, rollout.Deferred) and "checkout not clean/on main" in result.reason, result
    assert "--untracked-files=no" in " ".join(next(a for a in run.argvs if "status" in a))
    assert not run.ran("merge")


def test_sync_is_deferred_when_the_fast_forward_is_refused():
    run = Script({**_CLEAN_ON_MAIN, "merge --ff-only": (128, "", "fatal: Not possible to fast-forward, aborting.")})
    result = rollout.sync_checkout(CWD, runner=run)
    assert isinstance(result, rollout.Deferred) and "cannot fast-forward" in result.reason, result


def test_sync_is_deferred_when_fetch_fails():
    run = Script({"fetch": (128, "", "fatal: unable to access remote")})
    result = rollout.sync_checkout(CWD, runner=run)
    assert isinstance(result, rollout.Deferred) and "git fetch failed" in result.reason, result


def test_the_default_branch_falls_back_to_master_when_origin_head_is_unset():
    run = Script({**_CLEAN_ON_MAIN, "symbolic-ref": (128, "", "fatal: ref refs/remotes/origin/HEAD is not a symbolic ref"),
                  "refs/remotes/origin/main": (1, "", ""),
                  "rev-parse --abbrev-ref HEAD": (0, "master\n", "")})
    assert isinstance(rollout.sync_checkout(CWD, runner=run), rollout.Synced)
    assert run.ran("merge --ff-only origin/master") and run.ran("rev-parse HEAD origin/master")


def test_a_default_branch_that_cannot_be_determined_defers():
    run = Script({"symbolic-ref": (128, "", "x"), "refs/remotes/origin": (1, "", "")})
    result = rollout.sync_checkout(CWD, runner=run)
    assert isinstance(result, rollout.Deferred) and "default branch" in result.reason, result


def test_a_failing_deploy_reports_the_exit_code_and_a_bounded_tail():
    run = Script({"make": (3, "x" * 3000, "y" * 3000)})
    result = rollout.deploy(CWD, runner=run)
    assert isinstance(result, rollout.Ran) and not result.ok and result.exit_code == 3
    assert len(result.tail) == rollout.OUTPUT_TAIL_CHARS and result.tail.endswith("y"), len(result.tail)


def test_a_deploy_timeout_and_a_missing_binary_never_raise():
    timeout = Script({"make": subprocess.TimeoutExpired("make", 5)})
    assert rollout.deploy(CWD, runner=timeout, timeout_s=5) == rollout.Ran(False, 124, "timed out after 5s")
    missing = Script({"make": FileNotFoundError("make: not found")})
    result = rollout.deploy(CWD, runner=missing)
    assert result.ok is False and result.exit_code == 127 and "not found" in result.tail, result


def test_verify_runs_only_make_verify_with_its_own_timeout():
    run = Script({"make": (0, "healthy\n", "")})
    result = rollout.verify(CWD, runner=run)
    assert result == rollout.Ran(True, 0, "healthy\n")
    assert run.argvs == [["make", "-C", str(CWD), "verify"]]
    assert run.kwargs[0]["timeout"] == rollout.VERIFY_TIMEOUT_S


def test_a_timeout_kills_the_whole_process_group_not_just_its_leader():
    # The default runner: a recipe's background child must die with it, or a timed-out deploy
    # keeps running while warden strikes and runs it again.
    with tempfile.TemporaryDirectory() as tmp:
        pidfile = Path(tmp) / "child.pid"
        script = f"sleep 30 & echo $! > {pidfile}; wait"
        started = time.monotonic()
        try:
            rollout._run_in_group(["sh", "-c", script], capture_output=True, text=True, timeout=1,
                                  env=dict(os.environ))
        except subprocess.TimeoutExpired:
            pass
        else:
            raise AssertionError("expected TimeoutExpired")
        assert time.monotonic() - started < 10, "the timeout must not wait on the orphaned child"
        child = int(pidfile.read_text().strip())
        for _ in range(50):
            try:
                os.kill(child, 0)
            except ProcessLookupError:
                break
            time.sleep(0.05)
        else:
            os.kill(child, 9)
            raise AssertionError(f"the recipe's child {child} outlived the timeout")


def test_a_timeout_does_not_hang_on_a_descendant_that_left_the_group_holding_the_pipes():
    # A grandchild in its own session survives killpg and keeps stdout open: the post-kill drain
    # must be bounded, and the run still reads as a timeout.
    with tempfile.TemporaryDirectory() as tmp:
        pidfile = Path(tmp) / "escaped.pid"
        script = ("import os, subprocess, time\n"
                  f"p = subprocess.Popen(['sleep', '30'], start_new_session=True)\n"
                  f"open({str(pidfile)!r}, 'w').write(str(p.pid))\n"
                  "time.sleep(30)\n")
        original = rollout.KILL_DRAIN_S
        rollout.KILL_DRAIN_S = 0.5
        started = time.monotonic()
        try:
            res = rollout._run([sys.executable, "-c", script], rollout._run_in_group, 1)
        finally:
            rollout.KILL_DRAIN_S = original
            if pidfile.exists():
                try:
                    os.kill(int(pidfile.read_text()), 9)
                except ProcessLookupError:
                    pass
        assert time.monotonic() - started < 10, "the drain after killpg must be bounded"
        assert res == rollout.Ran(False, 124, "timed out after 1s"), res


class _EnvLock:
    """Point the deploy lock at a temp file and make warden's checkout CWD for one `with`."""

    def __enter__(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.saved = (os.environ.get("WARDEN_DEPLOY_LOCK"), rollout.WARDEN_CHECKOUT)
        os.environ["WARDEN_DEPLOY_LOCK"] = str(Path(self.tmp.name) / "deploy.lock")
        rollout.WARDEN_CHECKOUT = CWD.resolve()
        return self

    def __exit__(self, *exc):
        old, rollout.WARDEN_CHECKOUT = self.saved
        if old is None:
            os.environ.pop("WARDEN_DEPLOY_LOCK", None)
        else:
            os.environ["WARDEN_DEPLOY_LOCK"] = old
        self.tmp.cleanup()


def test_the_deploy_lock_is_exclusive_and_released_on_exit():
    with _EnvLock():
        with rollout.deploy_lock(wait_s=0):
            try:
                with rollout.deploy_lock(wait_s=0.2):
                    raise AssertionError("a second holder got the lock")
            except rollout.LockBusy:
                pass
        with rollout.deploy_lock(wait_s=0):
            pass


def test_syncing_warden_own_checkout_is_deferred_while_a_deploy_holds_the_lock():
    with _EnvLock():
        run = Script(_CLEAN_ON_MAIN)
        with rollout.deploy_lock(wait_s=0):
            original = rollout.LOCK_WAIT_S
            rollout.LOCK_WAIT_S = 0.2
            try:
                result = rollout.sync_checkout(CWD, runner=run)
            finally:
                rollout.LOCK_WAIT_S = original
        assert isinstance(result, rollout.Deferred) and "busy" in result.reason, result
        assert run.argvs == [], "nothing may touch git while the deploy holds the lock"
        assert isinstance(rollout.sync_checkout(CWD, runner=run), rollout.Synced)


def test_syncing_another_repo_never_takes_the_deploy_lock():
    with _EnvLock():
        with rollout.deploy_lock(wait_s=0):
            assert isinstance(rollout.sync_checkout(Path("/repos/other"), runner=Script(_CLEAN_ON_MAIN)), rollout.Synced)


def test_sync_reports_the_shas_before_and_after_a_fast_forward():
    answers = {**_CLEAN_ON_MAIN, "rev-parse HEAD origin/": (0, "bbbb\nbbbb\n", ""),
               "rev-parse HEAD": (0, "aaaa\n", "")}
    assert rollout.sync_checkout(CWD, runner=Script(answers)) == rollout.Synced("aaaa", "bbbb")


def test_deploy_hands_the_synced_shas_to_make_as_rollback_target_and_expected_head():
    run = Script({})
    rollout.deploy(CWD, prev_sha="aaaa", head_sha="bbbb", runner=run)
    env = run.kwargs[-1]["env"]
    assert env["WARDEN_DEPLOY_PREV"] == "aaaa" and env["WARDEN_DEPLOY_HEAD"] == "bbbb", env


def test_deploy_without_shas_sets_neither_variable():
    # Nothing is remembered between calls: a SHA reaches make only when the caller passes it.
    run = Script({})
    prior = {k: os.environ.pop(k, None) for k in ("WARDEN_DEPLOY_PREV", "WARDEN_DEPLOY_HEAD")}
    try:
        rollout.deploy(CWD, runner=run)
    finally:
        os.environ.update({k: v for k, v in prior.items() if v is not None})
    env = run.kwargs[-1]["env"]
    assert "WARDEN_DEPLOY_PREV" not in env and "WARDEN_DEPLOY_HEAD" not in env, env
    assert not hasattr(rollout, "_pre_merge_head")


def test_the_default_runner_returns_like_subprocess_run():
    res = rollout._run(["sh", "-c", "echo out; echo err >&2; exit 3"], rollout._run_in_group, 10)
    assert res.ok is False and res.exit_code == 3 and "out" in res.tail and "err" in res.tail, res


def test_the_runner_env_prepends_the_existing_host_tool_dirs_to_path():
    # launchd's minimal PATH does not include Homebrew or /usr/local/bin, so a repo's
    # `make deploy` would not find `op` or `brew` without this widening.
    with tempfile.TemporaryDirectory() as tmp:
        present = Path(tmp) / "homebrew" / "bin"
        present.mkdir(parents=True)
        absent = Path(tmp) / "nope" / "bin"
        saved = rollout.HOST_TOOL_DIRS
        rollout.HOST_TOOL_DIRS = (str(present), str(absent))
        try:
            run = Script({})
            rollout.deploy(CWD, runner=run)
        finally:
            rollout.HOST_TOOL_DIRS = saved
        env_path = run.kwargs[-1]["env"]["PATH"]
        entries = env_path.split(os.pathsep)
        assert entries[0] == str(present), entries
        assert str(absent) not in entries, "a dir not on this host must not be added"


def test_a_host_tool_dir_already_on_the_inherited_path_is_not_duplicated():
    with tempfile.TemporaryDirectory() as tmp:
        present = Path(tmp) / "bin"
        present.mkdir()
        saved = rollout.HOST_TOOL_DIRS
        rollout.HOST_TOOL_DIRS = (str(present),)
        try:
            entries = rollout.host_path(base=f"/usr/bin:{present}:/bin").split(os.pathsep)
        finally:
            rollout.HOST_TOOL_DIRS = saved
        assert entries[0] == str(present), entries
        assert entries.count(str(present)) == 1, entries
        assert entries[1:] == ["/usr/bin", "/bin"], entries


def test_host_path_keeps_the_inherited_path_when_no_host_tool_dir_exists():
    saved = rollout.HOST_TOOL_DIRS
    rollout.HOST_TOOL_DIRS = ("/nonexistent/warden-test-dir",)
    try:
        assert rollout.host_path(base="/usr/bin:/bin") == "/usr/bin:/bin"
    finally:
        rollout.HOST_TOOL_DIRS = saved


def test_a_caller_supplied_path_in_extra_env_still_wins():
    run = Script({})
    rollout._run(["make", "-C", str(CWD), "deploy"], run, 5, extra_env={"PATH": "/custom/bin:/bin"})
    assert run.kwargs[-1]["env"]["PATH"] == "/custom/bin:/bin", run.kwargs[-1]["env"]["PATH"]


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
