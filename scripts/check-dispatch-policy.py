#!/usr/bin/env python3
"""Do the two copies of the dispatch policy still agree?

There are deliberately two. DESIGN.md § Security model asks for exactly that:
sideclaw's copy is THE BOUNDARY (`server/lib/dispatch-policy.ts`, projected at
`GET /api/dispatch-policy`), because `POST /api/jobs` has no auth and any local
process can reach it; this repo's own `config/dispatch-repos.json` (read by
the warden CLI, which lives here too) is defence in depth for the one caller that
goes through it.

Duplication asked for on purpose is still duplication, and this is the same drift
shape DESIGN.md warns about for the verdict schema. Drift here does not present as
an error — it presents as **the boundary quietly allowing something the control
plane believes it forbids**, which is indistinguishable from working right up
until the day it matters. So the two get compared, and disagreement is loud.

Exit 0 = they agree. Exit 1 = they disagree (or a copy could not be read).

    python3 scripts/check-dispatch-policy.py [--json]
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# scripts/ (this file's own directory) onto sys.path so `lifecycle` is
# importable as a real package — TIER_RANK used to be redefined here,
# drifting from lifecycle/policy.py's copy (0/1/2 vs 1/2/3); one definition,
# imported, so the two can never disagree again.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from lifecycle.policy import TIER_RANK  # noqa: E402

SIDECLAW_URL = os.environ.get("SIDECLAW_URL", "http://127.0.0.1:7705")
# The CLI's own copy of this file moved here with the script (2026-09-10).
REPOS_JSON = Path(
    os.environ.get("WARDEN_DISPATCH_REPOS")
    or Path(__file__).resolve().parent.parent / "config" / "dispatch-repos.json"
).expanduser()

REFUSED = "refused"  # denied at every tier — no tier name covers that


def _rank(tier: str) -> int:
    """Unknown ranks above implement so it fails closed, mirroring both copies."""
    return TIER_RANK.get(tier, 99)


def hermes_ceiling(policy: dict[str, Any], repo: str) -> tuple[str, bool]:
    """The effective (ceiling, sensitive) the warden CLI's resolver would apply.

    Mirrors resolve_repo/resolve_tier, including the one carve-out that is easy to
    get wrong: a name in `sensitive` MUST also be in `deny`, and that pair means
    `investigate` ONLY — not a denial, and not a free pass.
    """
    deny = set(policy.get("deny", []))
    sensitive = set(policy.get("sensitive", []))
    if repo in deny:
        return ("investigate", True) if repo in sensitive else (REFUSED, False)
    for tier, names in (policy.get("tiers") or {}).items():
        if repo in names:
            return tier, False
    return policy.get("defaultTier", "implement"), False


def fetch_sideclaw() -> dict[str, Any]:
    req = urllib.request.Request(f"{SIDECLAW_URL}/api/dispatch-policy")
    with urllib.request.urlopen(req, timeout=10) as resp:
        return json.loads(resp.read().decode())


def main(argv: list[str]) -> int:
    as_json = "--json" in argv

    try:
        hermes = json.loads(REPOS_JSON.read_text())
    except (OSError, json.JSONDecodeError) as err:
        print(f"policy-check: cannot read {REPOS_JSON}: {err}", file=sys.stderr)
        return 1
    try:
        sc = fetch_sideclaw()
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as err:
        print(f"policy-check: cannot reach {SIDECLAW_URL}/api/dispatch-policy: {err}", file=sys.stderr)
        return 1

    sc_rules: dict[str, Any] = sc.get("rules", {})
    sc_roots: list[str] = sc.get("roots", [])
    root = Path(os.path.expanduser(hermes.get("root", "~/SourceRoot")))

    problems: list[str] = []
    notes: list[str] = []  # populated inside the loop below too
    rows: list[dict[str, Any]] = []

    # Enumerate from DISK, not from either config. A repo neither side has an
    # opinion about still resolves to a default on both, and the interesting
    # question is whether those defaults agree for something real.
    repos = sorted(p.name for p in root.iterdir() if (p / ".git").exists()) if root.is_dir() else []

    for repo in repos:
        h_ceiling, h_sensitive = hermes_ceiling(hermes, repo)
        # sideclaw keys its table lowercase and looks up case-insensitively.
        rule = sc_rules.get(repo) or sc_rules.get(repo.lower()) or {}
        s_ceiling = rule.get("ceiling", "implement")
        s_sensitive = bool(rule.get("sensitive", False))

        # A repo hermes refuses outright has no tier for sideclaw to match, so the
        # honest comparison is "is sideclaw at least as strict".
        # Two directions, and they are NOT the same severity.
        #
        # sideclaw LOOSER than hermes is the dangerous one and the reason this
        # script exists: the boundary admits something the control plane believes
        # it forbids, and nothing anywhere says so — it reads as working.
        #
        # sideclaw STRICTER is drift too, but it fails visibly: the control plane
        # tries, the boundary refuses, someone reads the refusal. Reported as a
        # note rather than a failure, because calling it a failure would train the
        # reader to ignore this check — and reporting nothing at all would let
        # this script claim "agree" about two files that plainly do not.
        want = "investigate" if h_ceiling == REFUSED else h_ceiling
        detail = "hermes denies outright, so investigate-or-stricter is the match" if h_ceiling == REFUSED else ""
        looser = _rank(s_ceiling) > _rank(want) or (h_sensitive and not s_sensitive)
        stricter = _rank(s_ceiling) < _rank(want) or (s_sensitive and not h_sensitive)
        agree = not looser
        if stricter and not looser:
            notes.append(
                f"  {repo}: sideclaw is STRICTER than the warden CLI "
                f"(boundary={s_ceiling}/sensitive={s_sensitive}, "
                f"control plane={h_ceiling}/sensitive={h_sensitive}) — safe, but the two "
                "files disagree and a dispatch the control plane allows will be refused"
            )

        rows.append({
            "repo": repo, "hermes": h_ceiling, "hermesSensitive": h_sensitive,
            "sideclaw": s_ceiling, "sideclawSensitive": s_sensitive, "agree": agree,
        })
        if not agree:
            problems.append(
                f"  {repo}: hermes says ceiling={h_ceiling} sensitive={h_sensitive}, "
                f"sideclaw says ceiling={s_ceiling} sensitive={s_sensitive}"
                + (f"  ({detail})" if detail else "")
            )

        # The fail-open condition sideclaw's lowercased lookup exists to survive:
        # its keys are lowercase, and a repo DIRECTORY carrying a capital is what
        # would otherwise stop matching its own rule.
        if repo != repo.lower() and repo.lower() in sc_rules:
            notes.append(f"  {repo}: on-disk name is not lowercase but its rule key is — "
                         "matching relies on the case-insensitive lookup, keep it")

    # Root sets. sideclaw is deliberately WIDER: it also serves interactive
    # dispatch into ~/IuRoot, which the warden CLI never reaches. That is expected,
    # and worth printing rather than silently tolerating.
    extra_roots = [r for r in sc_roots if Path(r).resolve() != root.resolve()]
    if extra_roots:
        notes.append(f"  sideclaw also admits roots the warden CLI never uses: {extra_roots} "
                     "(expected — interactive dispatch into work repos)")
    if not any(Path(r).resolve() == root.resolve() for r in sc_roots):
        problems.append(f"  sideclaw does not admit the warden CLI's own root {root} at all")

    if as_json:
        print(json.dumps({"ok": not problems, "rows": rows, "notes": notes}, indent=2))
        return 1 if problems else 0

    print(f"dispatch policy — {len(repos)} repos under {root}")
    if problems:
        print(f"\n✗ {len(problems)} disagreement(s) between the boundary and the control plane:")
        print("\n".join(problems))
        print("\n  sideclaw (server/lib/dispatch-policy.ts) is the boundary; warden's own")
        print("  config/dispatch-repos.json is defence in depth. Fix BOTH — a change to one")
        print("  is a change to the other, and drift here reads as working until it matters.")
    else:
        drifted = sum(1 for n in notes if "STRICTER" in n)
        if drifted:
            print(f"\n✓ the boundary is at least as strict as the control plane on all {len(repos)} repos")
            print(f"  ({drifted} where it is STRICTER — see notes; safe, but they disagree)")
        else:
            print(f"\n✓ both copies agree on all {len(repos)} repos")
    if notes:
        print("\nnotes:")
        print("\n".join(notes))
    return 1 if problems else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
