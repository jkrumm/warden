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
from typing import Any

# scripts/ (this file's own directory) onto sys.path so `clients` is
# importable as a real package, same pattern check-dispatch-policy.py uses.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from clients import sideclaw  # noqa: E402


def _finding_shape_notes(result: dict[str, Any]) -> tuple[list[str], list[str]]:
    """One tool's finding-shape report as (refusals, tolerated differences, …).

    A single reader for both branches below, because the alternative was the same narrowing written
    twice in one function — once for the mismatch detail and once for the `(producer also …)` suffix
    — and two copies of a union narrowing drift the moment the comparison gains a field (§144).
    `is True`/`is False` on the DISCRIMINATOR, never truthiness: a TypedDict union does not narrow
    on `if shape:`, so the other arm's keys would be reads the checker cannot prove (§136, §139).
    """
    shape: sideclaw.FindingShapeReport | None = result.get("findingShape")
    if shape is None:
        return [], []
    if shape["published"] is False:
        # The reason is printed because the two causes it distinguishes send an operator to
        # different places: no finding object at all is a producer that stopped publishing one,
        # while a `blocking` that is no longer an array is a container the runtime reads
        # differently (§135).
        return [f"no readable finding shape — warden's is_review_finding() would be an "
                f"unverifiable copy of it ({shape['reason']})"], []
    # Both shortfalls are reported together: they are independent sets, and printing one of them
    # when both are non-empty sends the operator back for a second run to learn the other — the
    # diagnostic is the only thing this check produces.
    missing = sorted(set(shape.get("missingFromRequired") or ())
                     | set(shape.get("missingFromProperties") or ()))
    mistyped = shape.get("mistyped") or []
    line = (f"findings: sideclaw requires {shape['required']} (types {shape.get('types')}) "
            f"with properties {shape['properties']}; warden requires {shape['wardenRequires']}"
            + (f" — MISSING {missing}" if missing else "")
            + (f" — WRONG TYPE {mistyped} (warden reads a string)" if mistyped else ""))
    # A difference warden tolerates on purpose is still worth naming: the producer requiring more
    # than warden does is safe, and hiding it would make the next real difference
    # indistinguishable from this one.
    extra = sorted(set(shape["required"]) - set(shape["wardenRequires"]))
    return [line], [f"producer also requires: {', '.join(extra)}"] if extra else []


def _envelope_notes(result: dict[str, Any]) -> tuple[list[str], list[str]]:
    """The same, for the result envelope: the fields the runtime reads by VALUE (§143).

    Kept apart from `_finding_shape_notes()` because the two refusals send an operator to different
    fixes — one is a shape warden reads as a noun, the other is a contract `assert_result_schema()`
    / `assert_outcome()` enforce per job — and merged into one line they read as one problem."""
    envelope: sideclaw.EnvelopeShapeReport | None = result.get("envelopeShape")
    if envelope is None:
        return [], []
    if envelope["published"] is False:
        return [f"no readable result envelope — warden's assert_result_schema()/assert_outcome() "
                f"would be unverifiable ({envelope['reason']})"], []
    problems = []
    if not envelope["versionTypeOk"]:
        problems.append(f"schemaVersion is typed {envelope['versionTypes']}, while warden compares "
                        f"an integer ({envelope['wardenVersion']})")
    if not envelope["versionConstOk"]:
        problems.append(f"schemaVersion const {envelope['versionConst']!r}, while warden pins "
                        f"{envelope['wardenVersion']}")
    if envelope["unknownOutcomes"]:
        problems.append(f"outcome(s) {envelope['unknownOutcomes']} outside warden's "
                        f"{envelope['wardenOutcomes']} — every review carrying one would be parked")
    # The other direction: a warden outcome the producer can no longer emit is a branch of ours
    # gone unreachable. Safe, so tolerated — and named, for the reason the suffix above exists.
    omitted = sorted(set(envelope["wardenOutcomes"]) - set(envelope["outcomes"]))
    return problems, [f"producer also omits: {', '.join(omitted)}"] if omitted else []


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
            # No leading indent and no `review:` label on these: `details` is printed one level
            # in already, and the tool line above already says which tool it is.
            details.extend(_finding_shape_notes(r)[0])
            details.extend(_envelope_notes(r)[0])
        print("✗ sideclaw schemas DISAGREE with warden's pinned versions:")
        for line in details:
            print(f"  {line}")
        return 1

    parts = []
    for tool in ("dispatch", "review"):
        r = results.get(tool) or {}
        parts.append(f"{tool}={r['remoteVersion']}" if r.get("reachable") else f"{tool}=unreachable")
        # A difference warden tolerates on purpose is still worth naming: the producer requiring
        # more than warden does is safe, and hiding it would make the next real difference
        # indistinguishable from this one. Both reports are read through the same two helpers the
        # mismatch branch uses, so a tolerated difference cannot be printed here and dropped there.
        for tolerated in _finding_shape_notes(r)[1] + _envelope_notes(r)[1]:
            parts.append(f"({tolerated})")
    print(f"✓ {' '.join(parts)}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
