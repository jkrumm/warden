#!/usr/bin/env python3
"""Regression suite for scripts/deploy.sh — the deploy lock, the validated rollback target and the
/health 503 schema comparison.

deploy.sh is sourceable: the function tests source it into `bash -c` and call the functions, with
`launchctl` and `curl` replaced by stubs first on PATH (and REPO/LIVE_REPO pointed at a throwaway
git repo for the rollback cases), so nothing reaches launchd, the live checkout or the ledger.
The lock tests run the script itself against a lock file in a temp dir.

Run: .venv/bin/python3 tests/test_deploy_sh.py  (or: make test, from warden/)
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

from lifecycle import rollout  # noqa: E402

DEPLOY_SH = REPO / "scripts" / "deploy.sh"
PY = REPO / ".venv" / "bin" / "python3"

_GIT_ENV = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"}


def _schema_body(ledger: int | None, code: int) -> str:
    return json.dumps({"error": f"warden ledger at /x/warden.db is at schema_version={ledger}, this process "
                                f"expects LEDGER_SCHEMA_VERSION={code}. Only the loop may migrate this file."})


class Sandbox:
    """A temp dir with stub `launchctl` / `curl` first on PATH. The curl stub prints $CURL_BODY (then, given -w,
    the status $CURL_CODE on its own line — health()'s shape) and logs each call."""

    def __enter__(self):
        self._tmp = tempfile.TemporaryDirectory()
        self.dir = Path(self._tmp.name)
        self.bin = self.dir / "bin"
        self.bin.mkdir()
        self.calls = self.dir / "calls.log"
        (self.bin / "launchctl").write_text(f'#!/bin/sh\necho "launchctl $*" >> "{self.calls}"\n')
        (self.bin / "curl").write_text(
            f'#!/bin/sh\necho "curl $*" >> "{self.calls}"\n'
            'case "$*" in *-w*) printf "%s\\n%s" "${CURL_BODY:-}" "${CURL_CODE:-200}" ;; *) printf "%s" "${CURL_BODY:-}" ;; esac\n'
            '[ -z "${CURL_SLEEP:-}" ] || { echo started > "${CURL_MARK:-/dev/null}"; sleep "$CURL_SLEEP"; }\n')
        for stub in self.bin.iterdir():
            stub.chmod(0o755)
        self.env = {**os.environ, "PATH": f"{self.bin}:{os.environ['PATH']}", **_GIT_ENV,
                    "WARDEN_DEPLOY_LOCK": str(self.dir / "deploy.lock"), "WARDEN_DEPLOY_LOCK_WAIT": "0.3"}
        self.env.pop("WARDEN_DEPLOY_LOCKED", None)
        self.env.pop("WARDEN_DEPLOY_PREV", None)
        return self

    def __exit__(self, *exc):
        self._tmp.cleanup()

    def logged(self) -> str:
        return self.calls.read_text() if self.calls.exists() else ""

    def bash(self, script: str, **env: str) -> subprocess.CompletedProcess:
        """Source deploy.sh, then run `script`."""
        return subprocess.run(["bash", "-c", f'source "{DEPLOY_SH}"\n{script}'], capture_output=True, text=True,
                              env={**self.env, **env}, timeout=60)

    def git(self, repo: Path, *args: str) -> str:
        res = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, env=self.env)
        assert res.returncode == 0, (args, res.stderr)
        return res.stdout.strip()

    def make_repo(self) -> tuple[Path, dict[str, str]]:
        """A repo with A <- B <- C on master (HEAD@{1} = B after the last ff) and a side commit D."""
        repo = self.dir / "repo"
        repo.mkdir()
        self.git(repo, "init", "-q", "-b", "master")
        shas: dict[str, str] = {}
        for name in "ABC":
            (repo / "f").write_text(name)
            self.git(repo, "add", "f")
            self.git(repo, "commit", "-q", "-m", name)
            shas[name] = self.git(repo, "rev-parse", "HEAD")
        self.git(repo, "checkout", "-q", "-b", "side", shas["A"])
        (repo / "f").write_text("D")
        self.git(repo, "commit", "-q", "-am", "D")
        shas["D"] = self.git(repo, "rev-parse", "HEAD")
        self.git(repo, "checkout", "-q", "master")  # HEAD@{1} is now D, not an ancestor of C
        return repo, shas


def _point_at(repo: Path) -> str:
    return f'REPO="{repo}"; LIVE_REPO="{repo}"; PY="{PY}"; IMPORT_SMOKE=pass\n'


# --- /health 503 schema comparison -------------------------------------------------------------

def test_schema_migratable_accepts_only_a_ledger_older_than_the_code():
    with Sandbox() as sb:
        cases = [(_schema_body(3, 5), 0), (_schema_body(5, 5), 1), (_schema_body(6, 5), 1),
                 (_schema_body(None, 5), 1), ('{"error": "ledger unreachable at /x: unable to open"}', 1),
                 ("not json", 1), ("", 1), ('["schema_version=1 LEDGER_SCHEMA_VERSION=2"]', 1)]
        for body, want in cases:
            res = sb.bash('schema_migratable "$BODY"', BODY=body)
            assert res.returncode == want, (body, res.returncode, res.stderr)


def _health(sb: Sandbox, body: str, code: str) -> subprocess.CompletedProcess:
    return sb.bash(f'LA="{sb.dir}"; API_URL=http://stub.invalid; PY="{PY}"; IMPORT_SMOKE=pass\nhealth',
                   CURL_BODY=body, CURL_CODE=code)


def test_health_passes_on_200():
    with Sandbox() as sb:
        res = _health(sb, "{}", "200")
        assert res.returncode == 0 and "healthy" in res.stdout, (res.stdout, res.stderr)
        assert "launchctl kickstart -k" in sb.logged(), "the stub, never the real launchctl, was called"


def test_health_503_passes_when_the_ledger_is_behind_the_code():
    with Sandbox() as sb:
        res = _health(sb, _schema_body(3, 5), "503")
        assert res.returncode == 0 and "migrates on the loop's next boot" in res.stdout, (res.stdout, res.stderr)


def test_health_503_fails_when_the_ledger_is_ahead_of_the_code():
    with Sandbox() as sb:
        res = _health(sb, _schema_body(6, 5), "503")
        assert res.returncode == 1 and "503" in res.stdout, (res.stdout, res.stderr)


def test_health_503_fails_on_an_unreachable_ledger_or_a_crash():
    with Sandbox() as sb:
        for body in ('{"error": "ledger unreachable at /x: unable to open database file"}', "Traceback..."):
            res = _health(sb, body, "503")
            assert res.returncode == 1, (body, res.stdout)


# --- rollback target ----------------------------------------------------------------------------

def test_rollback_target_prefers_the_passed_pre_merge_sha():
    with Sandbox() as sb:
        repo, sha = sb.make_repo()
        res = sb.bash(_point_at(repo) + "rollback_target", WARDEN_DEPLOY_PREV=sha["B"])
        assert res.stdout.strip() == sha["B"], (res.stdout, res.stderr)


def test_rollback_target_rejects_a_non_ancestor_sha_and_a_head_at_1_that_is_not_one_either():
    with Sandbox() as sb:
        repo, sha = sb.make_repo()  # HEAD@{1} == D, which is not an ancestor of C
        res = sb.bash(_point_at(repo) + "rollback_target; echo rc=$?", WARDEN_DEPLOY_PREV=sha["D"])
        assert res.stdout.strip() == "rc=1", (res.stdout, res.stderr)


def test_rollback_target_falls_back_to_a_validated_head_at_1():
    with Sandbox() as sb:
        repo, sha = sb.make_repo()
        sb.git(repo, "reset", "-q", "--hard", sha["A"])
        sb.git(repo, "merge", "-q", "--ff-only", sha["C"])  # HEAD@{1} == A, an ancestor
        for prev in ("", sha["D"], "--hard", "zzzz", sha["C"]):  # unset, non-ancestor, option-like, junk, HEAD itself
            res = sb.bash(_point_at(repo) + "rollback_target", WARDEN_DEPLOY_PREV=prev)
            assert res.stdout.strip() == sha["A"], (prev, res.stdout, res.stderr)


def _deploy_with_health(sb: Sandbox, repo: Path, **env: str) -> subprocess.CompletedProcess:
    # health() fails on the first call (the new code) and passes after (the rolled-back code).
    counter = sb.dir / "health.count"
    return sb.bash(_point_at(repo) +
                   f'health() {{ [ -e "{counter}" ] && return 0; touch "{counter}"; return 1; }}\ndeploy; echo rc=$?', **env)


def test_a_failed_deploy_rolls_back_to_the_validated_target():
    with Sandbox() as sb:
        repo, sha = sb.make_repo()
        res = _deploy_with_health(sb, repo, WARDEN_DEPLOY_PREV=sha["B"])
        assert "rolled back" in res.stdout and res.stdout.strip().endswith("rc=1"), (res.stdout, res.stderr)
        assert sb.git(repo, "rev-parse", "HEAD") == sha["B"]


def test_a_failed_deploy_refuses_to_roll_back_to_an_invalid_target():
    with Sandbox() as sb:
        repo, sha = sb.make_repo()
        res = _deploy_with_health(sb, repo, WARDEN_DEPLOY_PREV=sha["D"])
        assert "no valid previous commit" in res.stdout and res.stdout.strip().endswith("rc=1"), (res.stdout, res.stderr)
        assert sb.git(repo, "rev-parse", "HEAD") == sha["C"], "HEAD must not move"


def test_deploy_still_refuses_outside_the_live_checkout():
    with Sandbox() as sb:
        res = sb.bash(f'PY="{PY}"\ndeploy; echo rc=$?')
        assert "deploy runs only in" in res.stdout and res.stdout.strip().endswith("rc=1"), res.stdout


# --- the lock -----------------------------------------------------------------------------------

_GOOD_HEALTH = json.dumps({"schema_version": 5, "schema_version_expected": 5,
                           "pollers": {"loop": {"ok": True, "age_minutes": 1, "threshold_minutes": 30}}})


def _run_script(sb: Sandbox, *args: str, **env: str) -> subprocess.Popen:
    return subprocess.Popen(["bash", str(DEPLOY_SH), *args], stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            text=True, env={**sb.env, "WARDEN_API_URL": "http://stub.invalid", **env})


def test_verify_runs_the_script_under_the_lock_when_it_is_free():
    with Sandbox() as sb:
        proc = _run_script(sb, "verify", CURL_BODY=_GOOD_HEALTH)
        out, err = proc.communicate(timeout=30)
        assert proc.returncode == 0 and "schema 5/5" in out, (out, err)
        assert "curl -fsS" in sb.logged()


def test_a_held_lock_makes_deploy_and_verify_back_off_without_touching_anything():
    with Sandbox() as sb:
        os.environ["WARDEN_DEPLOY_LOCK"] = sb.env["WARDEN_DEPLOY_LOCK"]
        try:
            with rollout.deploy_lock(wait_s=0):
                for target in ("verify", "deploy"):
                    proc = _run_script(sb, target, CURL_BODY=_GOOD_HEALTH)
                    out, err = proc.communicate(timeout=30)
                    assert proc.returncode == rollout.LOCK_BUSY_EXIT and "busy" in err, (target, proc.returncode, out, err)
        finally:
            os.environ.pop("WARDEN_DEPLOY_LOCK", None)
        assert sb.logged() == "", "no curl/launchctl may run while another deploy holds the lock"


def test_the_lock_is_held_for_the_whole_run_and_released_after():
    with Sandbox() as sb:
        mark = sb.dir / "mark"
        proc = _run_script(sb, "verify", CURL_BODY=_GOOD_HEALTH, CURL_SLEEP="1.5", CURL_MARK=str(mark))
        for _ in range(100):
            if mark.exists():
                break
            time.sleep(0.1)
        assert mark.exists(), "verify never reached curl"
        os.environ["WARDEN_DEPLOY_LOCK"] = sb.env["WARDEN_DEPLOY_LOCK"]
        try:
            try:
                with rollout.deploy_lock(wait_s=0):
                    raise AssertionError("the lock was free while verify ran")
            except rollout.LockBusy:
                pass
            out, err = proc.communicate(timeout=30)
            assert proc.returncode == 0, (out, err)
            with rollout.deploy_lock(wait_s=0):
                pass
        finally:
            os.environ.pop("WARDEN_DEPLOY_LOCK", None)


def test_an_unknown_target_is_a_usage_error_before_any_lock():
    with Sandbox() as sb:
        proc = _run_script(sb, "nope")
        out, err = proc.communicate(timeout=30)
        assert proc.returncode == 64 and "usage" in err, (proc.returncode, err)


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
