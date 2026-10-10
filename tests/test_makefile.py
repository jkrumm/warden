#!/usr/bin/env python3
"""Regression suite for the worktree guard in the Makefile's `link`/`setup` targets.

`make link` writes ~/.local/bin/warden from WARDEN_REPO := $(shell pwd); run inside a linked
git worktree it would repoint every shell at a checkout that is deleted when the worktree goes
(an episode did exactly that). `assert-main-checkout` refuses unless `git rev-parse --git-dir`
equals `--git-common-dir`, i.e. unless this is the main checkout.

The suite builds a throwaway main repo + linked worktree under a temp HOME (so the real
~/.local/bin and the real repo are never touched) and runs the real Makefile with `make -C`.
The failure case must refuse before venv/render/agents run at all.

Run: .venv/bin/python3 tests/test_makefile.py  (or: make test, from warden/)
"""

from __future__ import annotations

import os
import re
import subprocess
import sys
import tempfile
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
MAKEFILE = REPO / "Makefile"

_GIT_ENV = {"GIT_AUTHOR_NAME": "t", "GIT_AUTHOR_EMAIL": "t@example.com",
            "GIT_COMMITTER_NAME": "t", "GIT_COMMITTER_EMAIL": "t@example.com"}


class Fixture:
    """A temp main repo with one commit and a linked worktree, under a temp HOME."""

    def __enter__(self) -> "Fixture":
        self._tmp = tempfile.TemporaryDirectory()
        self.root = Path(self._tmp.name).resolve()
        self.home = self.root / "home"
        self.home.mkdir()
        self.env = {**os.environ, "HOME": str(self.home), **_GIT_ENV}
        self.main = self.root / "repo"
        self.main.mkdir()
        self._git(self.main, "init", "-q", "-b", "master")
        (self.main / "f").write_text("a")
        self._git(self.main, "add", "f")
        self._git(self.main, "commit", "-q", "-m", "a")
        self.worktree = self.root / "wt"
        self._git(self.main, "worktree", "add", "-q", str(self.worktree), "-b", "wt")
        return self

    def __exit__(self, *exc) -> None:
        self._tmp.cleanup()

    def _git(self, repo: Path, *args: str) -> str:
        res = subprocess.run(["git", "-C", str(repo), *args], capture_output=True, text=True, env=self.env)
        assert res.returncode == 0, (args, res.stderr)
        return res.stdout.strip()

    def make(self, cwd: Path, *targets: str) -> subprocess.CompletedProcess:
        return subprocess.run(["make", "-C", str(cwd), "-f", str(MAKEFILE), *targets],
                              capture_output=True, text=True, env=self.env, timeout=60)

    def wrapper(self) -> Path:
        return self.home / ".local" / "bin" / "warden"


def test_link_refuses_from_a_linked_worktree_without_writing_the_wrapper():
    with Fixture() as f:
        res = f.make(f.worktree, "link")
        assert res.returncode != 0, (res.stdout, res.stderr)
        assert not f.wrapper().exists(), "the guard must not write the global wrapper"
        assert "worktree" in res.stdout + res.stderr, res.stdout


def test_setup_refuses_from_a_linked_worktree_before_any_other_target_runs():
    with Fixture() as f:
        res = f.make(f.worktree, "setup")
        assert res.returncode != 0, (res.stdout, res.stderr)
        assert not f.wrapper().exists()
        assert not (f.worktree / ".venv").exists(), "the guard must fail before venv/render/agents run"


def test_venv_is_not_gated_from_a_linked_worktree():
    # venv writes only the checkout's own .venv; `make check` in a worktree needs it. With a
    # missing BASE_PY the recipe fails on its own "not found" line — proof the worktree gate
    # did not refuse first.
    with Fixture() as f:
        res = f.make(f.worktree, "venv", "BASE_PY=no-such-python-for-test")
        out = res.stdout + res.stderr
        assert "no-such-python-for-test not found" in out, out
        assert "worktree" not in out, out

def test_link_refuses_from_a_worktree_even_with_ignore_errors():
    # `make -i` skips a failed prerequisite; the write must still not happen.
    with Fixture() as f:
        res = f.make(f.worktree, "-i", "link")
        assert "refusing" in res.stdout + res.stderr, res.stdout + res.stderr
        assert not f.wrapper().exists(), "-i must not let link write the global wrapper"


def test_ignore_errors_is_refused_for_every_global_target():
    # -i would skip any failed guard; the Makefile refuses it before considering a target.
    with Fixture() as f:
        for target in ("render-plists", "agents", "setup"):
            res = f.make(f.worktree, "-i", target)
            assert res.returncode != 0, (target, res.stdout, res.stderr)
            assert "make -i" in res.stdout + res.stderr, (target, res.stdout + res.stderr)
            assert not (f.home / "Library" / "LaunchAgents").exists(), target

def test_link_refuses_when_git_cannot_read_the_checkout():
    # Fail closed: with no git repo here, both rev-parse substitutions are empty and a bare
    # `[ "" = "" ]` would treat the unknown checkout as the main one. The guard must refuse
    # and must not touch the global wrapper.
    with tempfile.TemporaryDirectory() as tmp:
        root = Path(tmp).resolve()
        home = root / "home"
        home.mkdir()
        plain = root / "plain"
        plain.mkdir()
        env = {**os.environ, "HOME": str(home), **_GIT_ENV}
        res = subprocess.run(["make", "-C", str(plain), "-f", str(MAKEFILE), "link"],
                             capture_output=True, text=True, env=env, timeout=60)
        assert res.returncode != 0, (res.stdout, res.stderr)
        assert not (home / ".local" / "bin" / "warden").exists(), \
            "an unreadable checkout must fail closed, not overwrite the wrapper"


def test_link_refuses_when_git_dir_is_inherited_from_the_main_checkout():
    # From a linked worktree, inherited GIT_DIR / GIT_COMMON_DIR / GIT_WORK_TREE pointing at
    # the main checkout would make git report the main dir for both rev-parse answers, so the
    # equality test wrongly passes. The guard must clear those overrides first.
    with Fixture() as f:
        env = {**f.env, "GIT_DIR": str(f.main / ".git"),
               "GIT_COMMON_DIR": str(f.main / ".git"),
               "GIT_WORK_TREE": str(f.main)}
        res = subprocess.run(["make", "-C", str(f.worktree), "-f", str(MAKEFILE), "link"],
                             capture_output=True, text=True, env=env, timeout=60)
        assert res.returncode != 0, (res.stdout, res.stderr)
        assert not f.wrapper().exists(), "the guard must ignore inherited git overrides"


def test_setup_parallel_refuses_from_a_linked_worktree_before_any_side_effect():
    # Under `make -j setup` the assertion must be a completed gate, not a parallel sibling:
    # none of venv/render-plists/agents/link may run at all when the checkout is a worktree.
    with Fixture() as f:
        res = subprocess.run(["make", "-j", "-C", str(f.worktree), "-f", str(MAKEFILE), "setup"],
                             capture_output=True, text=True, env=f.env, timeout=60)
        assert res.returncode != 0, (res.stdout, res.stderr)
        assert not f.wrapper().exists()
        assert not (f.worktree / ".venv").exists()
        assert not (f.home / "Library" / "LaunchAgents").exists(), \
            "render-plists must not run before the assertion gate fails"


def test_link_writes_the_wrapper_from_the_main_checkout():
    with Fixture() as f:
        res = f.make(f.main, "link")
        assert res.returncode == 0, (res.stdout, res.stderr)
        wrapper = f.wrapper()
        assert wrapper.exists(), res.stdout
        match = re.search(r'exec "([^"]+)/scripts/warden"', wrapper.read_text())
        assert match, wrapper.read_text()
        assert Path(match.group(1)).resolve() == f.main, (match.group(1), f.main)


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
