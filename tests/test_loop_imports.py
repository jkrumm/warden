#!/usr/bin/env python3
"""Import-order smoke test for scripts/loop/*.py.

The loop modules (core, intake, triaging, work, train, verify, notify) import each other at
the top of the file (`from loop import core, work, ...`) and form import cycles: work imports
train, train imports work, and so on. That is safe only under one rule: a module references
another loop module's attributes (`core.set_state`, `work.notify_item`, ...) inside function
bodies, where the lookup happens at call time, never at import time (no module-level
`x = other.y`, no decorator, default argument or class body that reads another loop module).

Whichever module the process imports first sees the others half-initialised, so a violation
shows up only for some entry points: triage.py starts at core, dispatch-sweep.py at core and
work, the CLI at core and intake. This test imports each loop module FIRST in a fresh
interpreter, with the same sys.path setup as scripts/triage.py, and asserts a clean exit, so
the violation fails here instead of in a LaunchAgent.

Run: .venv/bin/python3 tests/test_loop_imports.py  (or: make test, from warden/)
"""

from __future__ import annotations

import subprocess
import sys
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
SCRIPTS = REPO / "scripts"
LOOP_MODULES = sorted(p.stem for p in (SCRIPTS / "loop").glob("*.py") if p.stem != "__init__")

# What scripts/triage.py does before `from loop import ...`: scripts/ on sys.path, so
# `clients`, `lifecycle` and `loop` import as packages.
_SNIPPET = "import sys; sys.path.insert(0, {scripts!r}); import loop.{module}"


def _import_first(module: str) -> subprocess.CompletedProcess:
    code = _SNIPPET.format(scripts=str(SCRIPTS), module=module)
    return subprocess.run([sys.executable, "-c", code], capture_output=True, text=True, timeout=60)


def test_the_loop_modules_are_found():
    assert {"core", "intake", "triaging", "work", "train", "verify", "notify"} <= set(LOOP_MODULES), LOOP_MODULES


def test_every_loop_module_imports_first_in_a_fresh_interpreter():
    failures = []
    for module in LOOP_MODULES:
        result = _import_first(module)
        if result.returncode != 0:
            failures.append(f"loop/{module}.py imported first exited {result.returncode}:\n{result.stderr}")
    assert not failures, "\n".join(failures)


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
