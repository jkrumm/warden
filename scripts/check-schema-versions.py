#!/usr/bin/env python3
"""Does the running sideclaw still agree with the dispatch/review verdict
schema versions this warden pins (`clients/sideclaw.py`'s
`DISPATCH_SCHEMA_VERSION`/`REVIEW_SCHEMA_VERSION`/`DISPATCH_OUTCOMES`/
`REVIEW_OUTCOMES`)?

Distinct from `assert_result_schema()`, which is the per-job runtime guard
inside the loop — this is the operator-facing probe `make status` runs on
every `make status` (never `make check-policy`'s own exit-1-on-disagreement
shape): sideclaw being unreachable is not this script's problem to fail
loudly over, only a genuine version/outcome-set MISMATCH is.

Exit 0 = versions agree, or sideclaw is unreachable (printed as
`unreachable`, never a failure — a connection failure must not make `make
status` itself fail). Exit 1 = an actual mismatch.

    python3 scripts/check-schema-versions.py [--json]
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

# scripts/ (this file's own directory) onto sys.path so `clients` is
# importable as a real package, same pattern check-dispatch-policy.py uses.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from clients import sideclaw  # noqa: E402


def main(argv: list[str]) -> int:
    as_json = "--json" in argv
    results = sideclaw.check_schema_versions()
    mismatched = [tool for tool, r in results.items() if r.get("reachable") and not r.get("ok")]

    if as_json:
        print(json.dumps(results, indent=2))
        return 1 if mismatched else 0

    if not any(r.get("reachable") for r in results.values()):
        print("unreachable — could not GET dispatch-schema/review-schema from sideclaw")
        return 0

    if mismatched:
        details = []
        for tool in mismatched:
            r = results[tool]
            details.append(
                f"{tool}: sideclaw serves version={r.get('remoteVersion')} "
                f"outcomes={sorted(r.get('remoteOutcomes') or ())}, warden pins "
                f"version={r.get('expectedVersion')} outcomes={sorted(r.get('expectedOutcomes') or ())}"
            )
            shape: sideclaw.FindingShapeReport | None = r.get("findingShape")
            # `published` discriminates the union: below this line the other keys are proven
            # present, which is why the report is a union of two TypedDicts and not one with
            # optional fields.
            if shape and not shape["published"]:
                details.append(
                    "  review: sideclaw publishes no readable finding shape — warden's "
                    "is_review_finding() would be an unverifiable copy of it")
            elif shape:
                # Both shortfalls are reported together: they are independent sets, and printing
                # one of them when both are non-empty sends the operator back for a second run to
                # learn the other — the diagnostic is the only thing this check produces.
                missing = sorted(set(shape.get("missingFromRequired") or ())
                                 | set(shape.get("missingFromProperties") or ()))
                mistyped = shape.get("mistyped") or []
                details.append(
                    f"  review findings: sideclaw requires {shape['required']} "
                    f"(types {shape.get('types')}) with properties {shape['properties']}; "
                    f"warden requires {shape['wardenRequires']}"
                    + (f" — MISSING {missing}" if missing else "")
                    + (f" — WRONG TYPE {mistyped} (warden reads a string)"
                       if mistyped else ""))
        print("✗ sideclaw schemas DISAGREE with warden's pinned versions:")
        for line in details:
            print(f"  {line}")
        return 1

    parts = []
    for tool in ("dispatch", "review"):
        r = results.get(tool) or {}
        parts.append(f"{tool}={r['remoteVersion']}" if r.get("reachable") else f"{tool}=unreachable")
        # A difference warden tolerates on purpose is still worth naming: the producer
        # requiring more than warden does is safe, and hiding it would make the next real
        # difference indistinguishable from this one.
        shape = r.get("findingShape")
        if shape and shape["published"]:
            extra = sorted(set(shape["required"]) - set(shape["wardenRequires"]))
            if extra:
                parts.append(f"(producer also requires: {', '.join(extra)})")
    print(f"✓ {' '.join(parts)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
