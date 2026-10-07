"""rollout — deploy and verify a merged change through the repo's own Makefile.

`make -C <cwd> deploy` and `make -C <cwd> verify` are the repo's contract: warden
owns the argv (two fixed targets, a path, nothing else) and the repo owns what
they do. `deploy` must be idempotent — a crash mid-deploy is answered by running
it again. A repo without the target has nothing to deploy / verifies by signal only.

Three questions live here and nowhere else:

  sync_checkout()  fast-forward the checkout to `origin/<default>` — only when it is on
                the default branch with no tracked changes and ends exactly AT
                `origin/<default>` (not ahead of it: unpushed commits are never deployed),
                else a typed `Deferred` (never a surprise merge into someone's work tree);
                on success a `Synced(before, head)` that the caller hands to deploy().
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

One lock, `deploy_lock()` (~/.warden/deploy.lock, override WARDEN_DEPLOY_LOCK), serialises
scripts/deploy.sh's deploy/verify (which re-execs itself under `exec_locked`) with
sync_checkout() on warden's own checkout: a fast-forward must not land mid-deploy or
mid-rollback. It is an fcntl.flock — macOS has no flock(1) — so a crash releases it.

Every subprocess goes through an injectable `runner` (tests) and is bounded by a
timeout; the default runner starts it in its own process group and kills the whole
group on timeout (a `make` recipe's children must not outlive it). Output is returned
as a bounded tail. PATH is widened for launchd's minimal environment (HOST_TOOL_DIRS) so
a repo's `make deploy` can reach `op`, `brew` or a `~/.local/bin` tool; a caller's own
PATH in extra_env still wins. Dry-run is the caller's contract: it prints what it would
do and never calls into this module.
"""

from __future__ import annotations

import fcntl
import os
import re
import signal
import subprocess
import sys
import time
from contextlib import contextmanager
from pathlib import Path
from typing import Callable, Iterator, NamedTuple

Runner = Callable[..., "subprocess.CompletedProcess[str]"]

DEPLOY_TIMEOUT_S = int(os.environ.get("WARDEN_DEPLOY_TIMEOUT", "900"))
VERIFY_TIMEOUT_S = int(os.environ.get("WARDEN_VERIFY_TIMEOUT", "600"))
PROBE_TIMEOUT_S = 60
GIT_TIMEOUT_S = 120
KILL_DRAIN_S = 5      # after killpg, how long the pipes get to close before the run is given up on
LOCK_WAIT_S = float(os.environ.get("WARDEN_DEPLOY_LOCK_WAIT", "120"))
LOCK_BUSY_EXIT = 75   # EX_TEMPFAIL
WARDEN_CHECKOUT = Path(__file__).resolve().parents[2]
OUTPUT_TAIL_CHARS = 2000

# launchd hands a LaunchAgent a minimal PATH (/usr/bin:/bin), so a repo's `make deploy`
# cannot see a tool installed by Homebrew or into /usr/local/bin — `op`, `brew`, a
# `~/.local/bin` shim. Every _run call widens PATH with these existing host tool dirs
# first; extra_env is applied after, so a caller that needs its own PATH still wins.
HOST_TOOL_DIRS = (
    "/opt/homebrew/bin",
    "/opt/homebrew/sbin",
    "/usr/local/bin",
    str(Path.home() / ".local" / "bin"),
    str(Path.home() / ".bun" / "bin"),
)
PATH_FALLBACK = "/usr/bin:/bin"

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


class Synced(NamedTuple):
    """The checkout sits at origin's default branch. `before` is where HEAD was when the sync
    began (== `head` when nothing merged), `head` where it is now: the hand-off to deploy()."""
    before: str
    head: str


class LockBusy(Exception):
    """The deploy lock stayed held past the wait."""


def deploy_lock_path() -> Path:
    return Path(os.environ.get("WARDEN_DEPLOY_LOCK") or Path.home() / ".warden" / "deploy.lock")


def _acquire_lock(wait_s: float) -> int:
    path = deploy_lock_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
    deadline = time.monotonic() + wait_s
    while True:
        try:
            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            return fd
        except BlockingIOError:
            if time.monotonic() >= deadline:
                os.close(fd)
                raise LockBusy(f"deploy lock {path} busy after {wait_s:g}s") from None
            time.sleep(0.1)


@contextmanager
def deploy_lock(wait_s: float | None = None) -> Iterator[None]:
    fd = _acquire_lock(LOCK_WAIT_S if wait_s is None else wait_s)
    try:
        yield
    finally:
        os.close(fd)  # closing the descriptor releases the flock


def exec_locked(argv: list[str]) -> None:
    """Take the deploy lock, then replace this process with `argv`. The lock's descriptor is
    inherited across the exec, so it is held exactly as long as the command runs."""
    try:
        fd = _acquire_lock(LOCK_WAIT_S)
    except LockBusy as exc:
        print(f"warden: {exc}", file=sys.stderr)
        sys.exit(LOCK_BUSY_EXIT)
    os.set_inheritable(fd, True)
    os.execvpe(argv[0], argv, {**os.environ, "WARDEN_DEPLOY_LOCKED": "1"})


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
            try:
                proc.communicate(timeout=KILL_DRAIN_S)
            except subprocess.TimeoutExpired:
                pass  # a descendant that left the group still holds the pipes: give up, don't hang
            raise
    return subprocess.CompletedProcess(argv, proc.returncode, stdout, stderr)


def host_path(base: str | None = None) -> str:
    """`base` (default the inherited PATH) with every existing HOST_TOOL_DIRS entry moved
    to the front and de-duplicated — a repo's `make deploy` runs under launchd's minimal
    PATH and must still find `/usr/local/bin/op` or Homebrew's `brew`. A dir that is not
    on this host is left out rather than shadowing nothing.

    An empty PATH component means the current working directory under POSIX lookup, so it
    is kept (de-duplicated like any other entry), and an *unset* PATH falls back to
    PATH_FALLBACK while an explicitly empty PATH stays empty (CWD only)."""
    seen: set[str] = set()
    entries: list[str] = []
    for d in HOST_TOOL_DIRS:
        if os.path.isdir(d) and d not in seen:
            entries.append(d)
            seen.add(d)
    if base is None:
        raw = os.environ.get("PATH")
        if raw is None:
            raw = PATH_FALLBACK
    else:
        raw = base
    for d in raw.split(os.pathsep):
        if d not in seen:
            entries.append(d)
            seen.add(d)
    return os.pathsep.join(entries)


def _run(argv: list[str], runner: Runner, timeout_s: int, extra_env: dict[str, str] | None = None) -> Ran:
    # LC_ALL=C: has_target() matches make's own English message.
    # PATH: widened for launchd's minimal env (host_path); extra_env last, so a caller's
    # own PATH still wins.
    env = {**os.environ, "PATH": host_path(), "LC_ALL": "C", **(extra_env or {})}
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


def sync_checkout(cwd: Path, *, runner: Runner = _run_in_group) -> Synced | Deferred:
    """Fast-forward `cwd` to `origin/<default>`; a `Deferred` says why it is not there, a
    `Synced` carries the SHAs before and after for deploy().
    Warden's own checkout is synced under the deploy lock — see the module docstring.
    Untracked files do not count as dirty — git itself refuses a merge they would collide with.
    `merge --ff-only` also succeeds on a checkout AHEAD of origin ("Already up to date"), so
    the result is checked: HEAD must be origin/<default> exactly."""
    if cwd.resolve() != WARDEN_CHECKOUT:
        return _sync_checkout(cwd, runner)
    try:
        with deploy_lock():
            return _sync_checkout(cwd, runner)
    except LockBusy as exc:
        return Deferred(f"{exc} — a deploy is running in {cwd.name}")


def _sync_checkout(cwd: Path, runner: Runner) -> Synced | Deferred:
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
    before = _git(cwd, runner, "rev-parse", "HEAD").tail.strip()
    merged = _git(cwd, runner, "merge", "--ff-only", f"origin/{default}")
    if not merged.ok:
        return Deferred(f"cannot fast-forward {cwd.name} to origin/{default}: {merged.tail.strip()[-200:]}")
    heads = _git(cwd, runner, "rev-parse", "HEAD", f"origin/{default}")
    shas = heads.tail.split()
    if not heads.ok or len(shas) != 2 or shas[0] != shas[1]:
        return Deferred(f"checkout not at origin/{default}: {cwd.name} has commits origin does not "
                        f"({' vs '.join(s[:12] for s in shas) or 'unreadable'})")
    return Synced(before or shas[0], shas[0])


def deploy(cwd: Path, *, prev_sha: str | None = None, head_sha: str | None = None,
           runner: Runner = _run_in_group, timeout_s: int | None = None) -> Ran:
    """`make deploy` — the caller synced the checkout first (sync_checkout()) and passes its
    `Synced` SHAs on: `prev_sha` is scripts/deploy.sh's rollback target (WARDEN_DEPLOY_PREV),
    `head_sha` the commit it is meant to deploy (WARDEN_DEPLOY_HEAD) — the lock is released
    between sync and deploy, so deploy.sh refuses to roll back when HEAD is no longer it."""
    extra = {name: sha for name, sha in (("WARDEN_DEPLOY_PREV", prev_sha), ("WARDEN_DEPLOY_HEAD", head_sha)) if sha}
    return _run(["make", "-C", str(cwd), "deploy"], runner, timeout_s or DEPLOY_TIMEOUT_S, extra or None)


def verify(cwd: Path, *, runner: Runner = _run_in_group, timeout_s: int | None = None) -> Ran:
    return _run(["make", "-C", str(cwd), "verify"], runner, timeout_s or VERIFY_TIMEOUT_S)
