#!/usr/bin/env python3
"""Does the running sideclaw's live routing table still agree with the model
warden pins for automatic episodes (`scripts/triage.py`'s
`AUTO_DISPATCH_MODEL`, default `glm-5.3-flash`) and, when the operator has set
one, for review validation (`TRIAGE_VALIDATION_DISPATCH_MODEL`)?

sideclaw owns the routing table (`server/lib/routing.ts`) and publishes it
live at `GET /api/routing`. This is the third cross-repo copy with no drift
check, built in the same mould as `check-schema-versions.py` (the verdict
schema) and `check-dispatch-policy.py` (the repo allowlist): sideclaw being
unreachable is not this script's problem to fail loudly over — only a genuine
MODEL mismatch is.

Exit 0 = warden's pinned model(s) agree with sideclaw's live route(s).
Exit 1 = a genuine mismatch.
Exit 2 = sideclaw unreachable — printed honestly, never a fabricated ✓.

    python3 scripts/check-routing.py [--json]
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# scripts/ (this file's own directory) onto sys.path, same pattern
# check-schema-versions.py and check-dispatch-policy.py use.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

SIDECLAW_URL = os.environ.get("SIDECLAW_URL", "http://127.0.0.1:7705")

# triage.py is a top-level script, not a package (no __init__.py in scripts/),
# so it is loaded by path — the same shape tests/test_triage.py already uses —
# rather than reimplemented here. That is what keeps this check reading the
# SAME AUTO_DISPATCH_MODEL / TRIAGE_VALIDATION_DISPATCH_MODEL the loop
# actually runs with (including TRIAGE_AUTO_DISPATCH_MODEL env overrides the
# LaunchAgent may carry), instead of a hand-copied guess that itself could
# drift.
_TRIAGE_PATH = _SCRIPTS_DIR / "triage.py"
_spec = importlib.util.spec_from_file_location("triage", _TRIAGE_PATH)
assert _spec is not None and _spec.loader is not None
triage = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(triage)


def fetch_routing() -> dict[str, Any]:
    req = urllib.request.Request(f"{SIDECLAW_URL}/api/routing")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def _checks() -> list[tuple[str, str]]:
    """(sideclaw route key, warden-pinned model) pairs to compare. `review` is
    only in scope once the operator has actually set
    TRIAGE_VALIDATION_DISPATCH_MODEL — its default (None) means "take
    sideclaw's own JUDGE route", which is not a model id to compare against."""
    checks = [("dispatch", triage.AUTO_DISPATCH_MODEL)]
    if triage.TRIAGE_VALIDATION_DISPATCH_MODEL:
        checks.append(("review", triage.TRIAGE_VALIDATION_DISPATCH_MODEL))
    return checks


def main(argv: list[str]) -> int:
    as_json = "--json" in argv

    try:
        routing = fetch_routing()
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as err:
        if as_json:
            print(json.dumps({"reachable": False, "error": str(err)}, indent=2))
        else:
            print("routing ? sideclaw unreachable")
        return 2

    routes: dict[str, Any] = routing.get("routes", {})
    checks = _checks()

    rows = []
    mismatched = []
    for tool, pinned in checks:
        live = (routes.get(tool) or {}).get("model")
        agree = live == pinned
        rows.append({"tool": tool, "pinned": pinned, "live": live, "agree": agree})
        if not agree:
            mismatched.append((tool, pinned, live))

    if as_json:
        print(json.dumps({"reachable": True, "ok": not mismatched, "rows": rows}, indent=2))
        return 1 if mismatched else 0

    if mismatched:
        for tool, pinned, live in mismatched:
            print(f"routing ✗ warden pins {tool}={pinned}, sideclaw routes {tool} to {live}")
        return 1

    print(f"routing ✓ {' '.join(f'{tool}={pinned}' for tool, pinned in checks)} (both)")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
