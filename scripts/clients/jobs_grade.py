"""jobs-grade — the local boundary to `~/.local/bin/jobs-grade` (dotfiles `scripts/lib/jobs-grade.ts`),
the only grader of background jobs. One subprocess per loop pass, JSON on stdout: an array with one
row per job label.

Row shape (dotfiles' `Graded`): `label`, `owner` (the owner repo's name), `grade`
(ok | failing | stale | flapping | unloaded), `reason`, `rc`, `consecutive_fail`, `fails_24h`,
`last_ok`, `last_started`, `every`. Any further field the grader adds rides along untouched.

Failure is never an exception: a missing binary, a timeout, a non-zero exit or output that is not
the contract returns None and logs one stderr line. The caller treats None as "blind this pass" —
it must neither open nor resolve anything on it, the same rule as an unreachable op_refs host.
A partly malformed array is also None: dropping one row would read as that job's recovery.
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
from pathlib import Path
from typing import Any, Callable

Runner = Callable[..., "subprocess.CompletedProcess[str]"]

GRADES = ("ok", "failing", "stale", "flapping", "unloaded")
# The grades that open (and hold open) an item. `ok` is the recovery.
BAD_GRADES = ("failing", "stale", "flapping", "unloaded")

TIMEOUT_S = int(os.environ.get("WARDEN_JOBS_GRADE_TIMEOUT", "30"))


def bin_path() -> Path:
    return Path(os.environ.get("WARDEN_JOBS_GRADE_BIN") or Path.home() / ".local" / "bin" / "jobs-grade").expanduser()


def machine_name() -> str:
    """The short host name that scopes a job's identity (`job:<machine>:<label>`)."""
    return (os.environ.get("WARDEN_MACHINE") or socket.gethostname().split(".")[0]).strip().lower() or "unknown"


def _valid(row: Any) -> bool:
    return (isinstance(row, dict) and isinstance(row.get("label"), str) and bool(row["label"])
            and isinstance(row.get("owner"), str) and row.get("grade") in GRADES)


def fetch(*, runner: Runner = subprocess.run, env: dict[str, str] | None = None,
          timeout_s: float | None = None) -> list[dict[str, Any]] | None:
    """The graded jobs of this machine, or None when they could not be graded. `runner` and `env`
    are the injection points (tests; the loop passes a PATH widened for launchd, because the grader
    is a `bun` script)."""
    binary = bin_path()
    try:
        done = runner([str(binary)], capture_output=True, text=True, timeout=timeout_s or TIMEOUT_S,
                      env=env, stdin=subprocess.DEVNULL)
    except subprocess.TimeoutExpired:
        print(f"jobs-grade: {binary} did not answer within {timeout_s or TIMEOUT_S}s — skipping the job source this pass",
              file=sys.stderr)
        return None
    except OSError as e:
        print(f"jobs-grade: could not run {binary}: {e} — skipping the job source this pass", file=sys.stderr)
        return None
    if done.returncode != 0:
        detail = (done.stderr or "").strip().splitlines()[-1:] or ["no stderr"]
        print(f"jobs-grade: {binary} exited {done.returncode} ({detail[0][:200]}) — skipping the job source this pass",
              file=sys.stderr)
        return None
    try:
        rows = json.loads(done.stdout)
    except json.JSONDecodeError as e:
        print(f"jobs-grade: output is not JSON ({e}) — skipping the job source this pass", file=sys.stderr)
        return None
    if not isinstance(rows, list) or not all(_valid(r) for r in rows):
        print("jobs-grade: output is not an array of {label, owner, grade} rows — skipping the job source this pass",
              file=sys.stderr)
        return None
    return rows
