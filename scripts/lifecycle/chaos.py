"""chaos — the stop-condition exercise's one hook (DESIGN.md § Migration Wave 3).

Proving that an obligation (an implement operation, a merge, a deploy) is
neither dropped nor repeated under a crash means actually crashing the loop
at every boundary that matters, not reasoning about it. `crash_point(name)`
is that hook: a no-op unless `WARDEN_KILL_AT` names this exact point, in
which case it exits the process hard (`os._exit`, no cleanup, no atexit,
no traceback) right there — the same shape a real `kill -9` or a power loss
would leave behind, so `reconcile_operations()` has to resolve exactly what
it would resolve after a genuine crash.

The point names are a closed list (`POINTS`) for the same reason every other
closed allowlist in this file exists: a caller passing a name this module
does not recognize is a typo, not a new point, and must fail loudly rather
than silently never firing.
"""

from __future__ import annotations

import os
import sys

POINTS = (
    "before-implement-open",
    "after-implement-op",
    "after-implement-submit",
    "before-merge",
    "after-merge-op",
    "after-merge-put",
    "after-merged-at",
    "after-deploy-op",
    "after-merge-before-state",
    "before-fixed",
    "before-host-verb",
)


def crash_point(name: str) -> None:
    if name not in POINTS:
        raise ValueError(f"{name!r} not in POINTS={POINTS} — chaos points are a closed list")
    if os.environ.get("WARDEN_KILL_AT") == name:
        sys.stderr.write(f"warden: WARDEN_KILL_AT={name} — exiting hard\n")
        sys.stderr.flush()
        os._exit(137)
