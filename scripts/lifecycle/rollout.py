"""rollout — deploy and verify a merged change through the repo's own Makefile.

`make -C <cwd> deploy` and `make -C <cwd> verify` are the repo's contract: warden
owns the argv (two fixed targets, a path, nothing else) and the repo owns what
they do. `deploy` must be idempotent — a crash mid-deploy is answered by running
it again. A repo without the target has nothing to deploy / verifies by signal only.

Three questions live here and nowhere else:

  sync_checkout()  fast-forward the checkout to `origin/<default>` — only when it is on
                the default branch with no tracked changes and ends exactly AT
                `origin/<default>` (not ahead of it: unpushed commits are never deployed),
                else a typed `Deferred` (never a surprise merge into someone's work tree).
                The caller syncs once, before asking has_target(): a merge that adds the
                first `deploy` target must be seen, and `make verify` must judge the
                merged tree.
  has_target()  does the Makefile define the target? `make -n <target>` answers with
                make's own semantics (includes, pattern rules, variables) where a text
                match over the Makefile would not. A recipe line marked `+` runs even
                under -n; the only other failure to tell apart is "no rule to make the
                target ITSELF" — a missing prerequisite ("..., needed by 'deploy'") is a
                broken target, not an absent one — so ANY other non-zero exit reads as
                "present" and the real run reports it loudly (a broken Makefile must not
                silently skip a deploy).
  deploy()      `make deploy`.
  verify()      `make verify`.

Every subprocess goes through an injectable `runner` (tests) and is bounded by a
timeout; the default runner starts it in its own process group and kills the whole
group on timeout (a `make` recipe's children must not outlive it). Output is returned
as a bounded tail. Dry-run is the caller's contract: it prints what it would do and
never calls into this module.
"""

from __future__ import annotations

import os
import re
import signal
import subprocess
from pathlib import Path
from typing import Callable, NamedTuple

Runner = Callable[..., "subprocess.CompletedProcess[str]"]

DEPLOY_TIMEOUT_S = int(os.environ.get("WARDEN_DEPLOY_TIMEOUT", "900"))
VERIFY_TIMEOUT_S = int(os.environ.get("WARDEN_VERIFY_TIMEOUT", "600"))
PROBE_TIMEOUT_S = 60
GIT_TIMEOUT_S = 120
OUTPUT_TAIL_CHARS = 2000

_NO_MAKEFILE_RE = re.compile(r"No targets specified and no makefile found")
_FALLBACK_DEFAULT_BRANCHES = ("main", "master")


class Ran(NamedTuple):
    """A command that ran (or could not even start: exit 127, or timed out: exit 124)."""
    ok: bool
    exit_code: int
    tail: str


class Deferred(NamedTuple):
    """The checkout is not in a state a deploy may start from — nothing ran."""
    reason: str


def _tail(text: str) -> str:
    return text[-OUTPUT_TAIL_CHARS:]


def _run_in_group(argv: list[str], *, capture_output: bool, text: bool, timeout: int,
                  env: dict[str, str]) -> "subprocess.CompletedProcess[str]":
    """`subprocess.run()` for one command in its own session: on timeout the whole process
    group is killed, not just its leader — `make deploy` timing out must not leave the
    recipe's children running."""
    pipe = subprocess.PIPE if capture_output else None
    with subprocess.Popen(argv, stdout=pipe, stderr=pipe, text=text, env=env, start_new_session=True) as proc:
        try:
            stdout, stderr = proc.communicate(timeout=timeout)
        except subprocess.TimeoutExpired:
            try:
                os.killpg(proc.pid, signal.SIGKILL)
            except ProcessLookupError:
                pass
            proc.communicate()
            raise
    return subprocess.CompletedProcess(argv, proc.returncode, stdout, stderr)


def _run(argv: list[str], runner: Runner, timeout_s: int) -> Ran:
    # LC_ALL=C: has_target() matches make's own English message.
    env = {**os.environ, "LC_ALL": "C"}
    try:
        proc = runner(argv, capture_output=True, text=True, timeout=timeout_s, env=env)
    except subprocess.TimeoutExpired:
        return Ran(False, 124, f"timed out after {timeout_s}s")
    except OSError as exc:
        return Ran(False, 127, str(exc))
    return Ran(proc.returncode == 0, proc.returncode, _tail((proc.stdout or "") + (proc.stderr or "")))


def _no_rule_for(target: str) -> re.Pattern[str]:
    """make's "no rule" for the target itself — GNU make 4 quotes it 'deploy', make 3.81
    `deploy' — and never the missing-prerequisite form, "No rule to make target 'x', needed
    by 'deploy'", whose target is followed by a comma, not the full stop."""
    return re.compile(rf"No rule to make target [`']{re.escape(target)}'\.")


def has_target(cwd: Path, target: str, *, runner: Runner = _run_in_group) -> bool:
    if target not in ("deploy", "verify"):
        raise ValueError(f"{target!r} is not a rollout target")
    res = _run(["make", "-C", str(cwd), "-n", target], runner, PROBE_TIMEOUT_S)
    return res.ok or not (_no_rule_for(target).search(res.tail) or _NO_MAKEFILE_RE.search(res.tail))


def _git(cwd: Path, runner: Runner, *args: str) -> Ran:
    return _run(["git", "-C", str(cwd), *args], runner, GIT_TIMEOUT_S)


def _default_branch(cwd: Path, runner: Runner) -> str | None:
    head = _git(cwd, runner, "symbolic-ref", "--short", "refs/remotes/origin/HEAD")
    if head.ok and head.tail.strip().startswith("origin/"):
        return head.tail.strip().removeprefix("origin/")
    for name in _FALLBACK_DEFAULT_BRANCHES:
        if _git(cwd, runner, "rev-parse", "--verify", "--quiet", f"refs/remotes/origin/{name}").ok:
            return name
    return None


def sync_checkout(cwd: Path, *, runner: Runner = _run_in_group) -> Deferred | None:
    """Fast-forward `cwd` to `origin/<default>`; a `Deferred` says why it is not there.
    Untracked files do not count as dirty — git itself refuses a merge they would collide with.
    `merge --ff-only` also succeeds on a checkout AHEAD of origin ("Already up to date"), so
    the result is checked: HEAD must be origin/<default> exactly."""
    fetched = _git(cwd, runner, "fetch", "--quiet", "origin")
    if not fetched.ok:
        return Deferred(f"git fetch failed in {cwd.name}: {fetched.tail.strip()[-200:]}")
    default = _default_branch(cwd, runner)
    if default is None:
        return Deferred(f"cannot determine {cwd.name}'s default branch")
    branch = _git(cwd, runner, "rev-parse", "--abbrev-ref", "HEAD").tail.strip()
    if branch != default:
        return Deferred(f"checkout not clean/on {default}: {cwd.name} is on {branch or '(unknown)'}")
    dirty = _git(cwd, runner, "status", "--porcelain", "--untracked-files=no")
    if not dirty.ok or dirty.tail.strip():
        return Deferred(f"checkout not clean/on {default}: {cwd.name} has uncommitted changes")
    merged = _git(cwd, runner, "merge", "--ff-only", f"origin/{default}")
    if not merged.ok:
        return Deferred(f"cannot fast-forward {cwd.name} to origin/{default}: {merged.tail.strip()[-200:]}")
    heads = _git(cwd, runner, "rev-parse", "HEAD", f"origin/{default}")
    shas = heads.tail.split()
    if not heads.ok or len(shas) != 2 or shas[0] != shas[1]:
        return Deferred(f"checkout not at origin/{default}: {cwd.name} has commits origin does not "
                        f"({' vs '.join(s[:12] for s in shas) or 'unreadable'})")
    return None


def deploy(cwd: Path, *, runner: Runner = _run_in_group, timeout_s: int | None = None) -> Ran:
    """`make deploy` — the caller synced the checkout first (sync_checkout())."""
    return _run(["make", "-C", str(cwd), "deploy"], runner, timeout_s or DEPLOY_TIMEOUT_S)


def verify(cwd: Path, *, runner: Runner = _run_in_group, timeout_s: int | None = None) -> Ran:
    return _run(["make", "-C", str(cwd), "verify"], runner, timeout_s or VERIFY_TIMEOUT_S)
