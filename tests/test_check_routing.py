#!/usr/bin/env python3
"""Regression suite for scripts/check-routing.py — does warden's pinned
dispatch/review model still agree with what sideclaw's live routing table
(`GET /api/routing`) actually routes those tools to?

HOUSE CONVENTION, not pytest: same hand-rolled shape as every other
tests/test_*.py in this repo — a plain `def test_*(): assert ...` function
with no arguments and no fixtures, discovered and called by main() via
reflection. Also valid standalone pytest input if pytest is ever installed.

No network: `check_routing.fetch_routing` is monkeypatched per-case, exactly
like `_triage_env()`'s client-boundary monkeypatching in test_triage.py.

Run:

    .venv/bin/python3 tests/test_check_routing.py
"""

from __future__ import annotations

import contextlib
import importlib.util
import io
import sys
import traceback
import urllib.error
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
CHECK_ROUTING_PATH = REPO_ROOT / "scripts" / "check-routing.py"

_spec = importlib.util.spec_from_file_location("check_routing", CHECK_ROUTING_PATH)
assert _spec is not None and _spec.loader is not None
check_routing = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(check_routing)

triage = check_routing.triage


def _run(argv: list[str] | None = None) -> tuple[int, str]:
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        code = check_routing.main(argv or [])
    return code, buf.getvalue()


def test_nothing_pinned_exits_zero_and_says_so():
    """With no overrides set (both knobs default None), there is no model id
    to compare — the check is vacuous and must report that honestly (exit 0),
    never fabricate a mismatch against sideclaw's own default routes."""
    original_dispatch = triage.AUTO_DISPATCH_MODEL
    original_review = triage.TRIAGE_VALIDATION_DISPATCH_MODEL
    try:
        triage.AUTO_DISPATCH_MODEL = None
        triage.TRIAGE_VALIDATION_DISPATCH_MODEL = None
        check_routing.fetch_routing = lambda: {
            "routes": {"dispatch": {"model": "DeepSeek-V4-Flash"}}
        }
        code, out = _run()
        assert code == 0, out
        assert "nothing pinned" in out, out
    finally:
        triage.AUTO_DISPATCH_MODEL = original_dispatch
        triage.TRIAGE_VALIDATION_DISPATCH_MODEL = original_review


def test_agreement_exits_zero_and_prints_check():
    """Live route matches an operator-set AUTO_DISPATCH_MODEL override — the
    exact shape `make check-routing` prints against the real daemon."""
    original = triage.AUTO_DISPATCH_MODEL
    try:
        triage.AUTO_DISPATCH_MODEL = "DeepSeek-V4-Flash"
        check_routing.fetch_routing = lambda: {
            "routes": {"dispatch": {"model": "DeepSeek-V4-Flash"}}
        }
        code, out = _run()
        assert code == 0, out
        assert out.strip() == "routing ✓ dispatch=DeepSeek-V4-Flash (both)", out
    finally:
        triage.AUTO_DISPATCH_MODEL = original


def test_drift_exits_one_and_names_both_models():
    """sideclaw routes dispatch somewhere the override does not expect — loud, exit 1."""
    original = triage.AUTO_DISPATCH_MODEL
    try:
        triage.AUTO_DISPATCH_MODEL = "DeepSeek-V4-Flash"
        check_routing.fetch_routing = lambda: {
            "routes": {"dispatch": {"model": "some-other-model"}}
        }
        code, out = _run()
        assert code == 1, out
        assert "✗" in out
        assert "DeepSeek-V4-Flash" in out
        assert "some-other-model" in out
    finally:
        triage.AUTO_DISPATCH_MODEL = original


def test_implement_override_checked_against_dispatch_implement_route():
    """AUTO_IMPLEMENT_MODEL defaults to None (sideclaw routes implement), so it
    only enters scope once set — then it is compared against sideclaw's
    `dispatch_implement` route, not the investigate `dispatch` route."""
    original_dispatch = triage.AUTO_DISPATCH_MODEL
    original_implement = triage.AUTO_IMPLEMENT_MODEL
    try:
        triage.AUTO_DISPATCH_MODEL = None
        triage.AUTO_IMPLEMENT_MODEL = "DeepSeek-V4-Pro"
        check_routing.fetch_routing = lambda: {
            "routes": {
                "dispatch": {"model": "DeepSeek-V4-Flash"},
                "dispatch_implement": {"model": "DeepSeek-V4-Pro"},
            }
        }
        code, out = _run()
        assert code == 0, out
        assert "dispatch_implement=DeepSeek-V4-Pro" in out, out
        assert "dispatch=" not in out, out  # investigate stays out of scope while None

        check_routing.fetch_routing = lambda: {
            "routes": {
                "dispatch": {"model": "DeepSeek-V4-Flash"},
                "dispatch_implement": {"model": "some-other-implement"},
            }
        }
        code, out = _run()
        assert code == 1, out
        assert "dispatch_implement" in out and "✗" in out, out
    finally:
        triage.AUTO_DISPATCH_MODEL = original_dispatch
        triage.AUTO_IMPLEMENT_MODEL = original_implement


def test_unreachable_exits_two_never_fabricates_ok():
    """sideclaw down is honestly reported, never a fabricated ✓ — distinct exit
    code from a genuine mismatch so a caller can tell the two apart."""

    def _raise():
        raise urllib.error.URLError("connection refused")

    check_routing.fetch_routing = _raise
    code, out = _run()
    assert code == 2, out
    assert out.strip() == "routing ? sideclaw unreachable", out


def test_review_checked_only_when_validation_model_set():
    """TRIAGE_VALIDATION_DISPATCH_MODEL defaults to None (sideclaw's own JUDGE
    route, not a model id) — review must stay out of scope until the operator
    actually sets it, then both tools are compared."""
    original = triage.TRIAGE_VALIDATION_DISPATCH_MODEL
    try:
        triage.TRIAGE_VALIDATION_DISPATCH_MODEL = None
        check_routing.fetch_routing = lambda: {
            "routes": {
                "dispatch": {"model": "DeepSeek-V4-Flash"},
                "review": {"model": "claude-sonnet-5[1m]"},
            }
        }
        code, out = _run()
        assert code == 0, out
        assert "review" not in out, out

        triage.TRIAGE_VALIDATION_DISPATCH_MODEL = "claude-sonnet-5[1m]"
        code, out = _run()
        assert code == 0, out
        assert "review=claude-sonnet-5[1m]" in out, out

        triage.TRIAGE_VALIDATION_DISPATCH_MODEL = "claude-opus-4"
        code, out = _run()
        assert code == 1, out
        assert "review" in out and "✗" in out, out
    finally:
        triage.TRIAGE_VALIDATION_DISPATCH_MODEL = original


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
