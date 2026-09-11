"""policy — repo/tier resolution, origin shape checks and the spend budgets.

The Python port of the retired bash CLI's `resolve_repo` (457-583), `resolve_tier`
(589-606), `tier_rank` (624-631), `require_auto_from_item` (639-705),
`valid_origin` (745-766), `budget_counts`/`check_budget`/`budget_json`
(849-915), `merge_precheck_repo` (1659-1682) and the `repos.<repo>` half of
`config/triage-policy.json` that `run_deploy_if_enabled` reads (1842-1856).

Two simplifications versus the bash version, both because this is now the
ONE implementation instead of a shell script embedding a second one in
Python for every call: `BUILT_TIERS` is gone (it always equalled
`VALID_TIERS` by the time this was ported — there is no "not implemented
yet" tier left to refuse), and `resolve_repo()` no longer re-validates the
resolved tier against `VALID_TIERS` a second time after `load_dispatch_policy()`
already refused a malformed one at load — one validating implementation,
not two copies that could disagree.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from clients.errors import PolicyError, PreconditionError, UsageError

from . import operations

REPO = Path(__file__).resolve().parents[2]

VALID_TIERS = ("investigate", "author", "implement")
GATED_TIERS = ("implement",)

TIER_RANK = {"investigate": 1, "author": 2, "implement": 3}

BUDGET_WARN_REMAINING = 3
IMPLEMENT_WARN_REMAINING = 1


def require_no_recursion() -> None:
    """A dispatched episode may never dispatch. Moved here from warden.py so
    the two places that actually mutate — `lifecycle.dispatch.open_episode()`
    and the LAND step of `lifecycle.merge.plan_or_land()` — refuse on their
    own, not only when reached through the CLI. Planning (a dry-run or an
    unconfirmed merge) stays allowed; only the write is guarded."""
    for marker in ("CLAUDE_CODE_SESSION", "CLAUDECODE", "CLAUDE_SESSION_ID"):
        if os.environ.get(marker):
            raise PolicyError(
                f"refusing to run inside a Claude Code session ({marker} is set): "
                "a dispatched episode may never dispatch"
            )
    if os.environ.get("CLAUDE_ENTRYPOINT") == "worker":
        raise PolicyError(
            "refusing to run inside a sideclaw worker session (CLAUDE_ENTRYPOINT=worker): "
            "a dispatched episode may never dispatch"
        )


def tier_rank(tier: str) -> int:
    """99 for anything unrecognized — deliberately ABOVE every real tier, so
    an unrecognized *request* is refused. See the retired bash CLI's own comment on
    why this default is safe only on the left-hand side of the comparison,
    which is why the ceiling itself is validated at policy-load time rather
    than trusted here."""
    return TIER_RANK.get(tier, 99)


def dispatch_policy_path() -> Path:
    if os.environ.get("WARDEN_DISPATCH_REPOS"):
        return Path(os.environ["WARDEN_DISPATCH_REPOS"]).expanduser()
    return REPO / "config" / "dispatch-repos.json"


def triage_policy_path() -> Path:
    if os.environ.get("WARDEN_TRIAGE_POLICY"):
        return Path(os.environ["WARDEN_TRIAGE_POLICY"]).expanduser()
    return REPO / "config" / "triage-policy.json"


def pr_required_path() -> Path:
    if os.environ.get("WARDEN_PR_REQUIRED_JSON"):
        return Path(os.environ["WARDEN_PR_REQUIRED_JSON"]).expanduser()
    return Path.home() / ".claude" / "pr-required-repos.json"


@dataclass(frozen=True)
class RepoTarget:
    name: str
    path: Path
    max_tier: str
    sensitive: bool


def load_dispatch_policy(path: Path | None = None) -> dict[str, Any]:
    """Parse `dispatch-repos.json` and run the five contradiction/shape
    checks the retired bash CLI's embedded `resolve_repo` python ran inline — see
    that block's own comments for why each one refuses rather than picks a
    winner. Returns a normalized dict: `root` (resolved Path), `default_tier`,
    `deny`/`sensitive` (sets), `overrides` (name -> tier)."""
    p = path or dispatch_policy_path()
    try:
        raw = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError) as err:
        raise PreconditionError(f"dispatch policy at {p} could not parse: {err}")

    root = Path(os.path.realpath(os.path.expanduser(raw.get("root", "~/SourceRoot"))))
    default_tier = raw.get("defaultTier", "investigate")
    deny = set(raw.get("deny") or [])
    sensitive = set(raw.get("sensitive") or [])

    overrides: dict[str, str] = {}
    for tier, names in (raw.get("tiers") or {}).items():
        if tier not in VALID_TIERS:
            raise PreconditionError(
                f"dispatch policy at {p}: unknown tier {tier!r} in `tiers` "
                f"(must be one of: {', '.join(VALID_TIERS)})"
            )
        for n in names:
            overrides[n] = tier

    if default_tier not in VALID_TIERS:
        raise PreconditionError(
            f"dispatch policy at {p}: unrecognized defaultTier {default_tier!r} "
            f"(must be one of: {', '.join(VALID_TIERS)})"
        )

    both = deny & set(overrides)
    if both:
        raise PreconditionError(
            f"dispatch policy at {p}: named in both `deny` and `tiers`: {', '.join(sorted(both))}"
        )

    not_denied = sensitive - deny
    if not_denied:
        raise PreconditionError(
            f"dispatch policy at {p}: named in `sensitive` but not in `deny`: {', '.join(sorted(not_denied))}"
        )

    both_sensitive = sensitive & set(overrides)
    if both_sensitive:
        raise PreconditionError(
            f"dispatch policy at {p}: named in both `sensitive` and `tiers`: {', '.join(sorted(both_sensitive))}"
        )

    return {
        "root": root,
        "default_tier": default_tier,
        "deny": deny,
        "sensitive": sensitive,
        "overrides": overrides,
    }


_NAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")


def _validate_repo_name(name: str) -> None:
    if not name or name in (".", "..") or name.startswith(".") or not set(name) <= _NAME_CHARS:
        raise UsageError(f"not a repo name: {name}")


def discoverable(root: Path, deny: set[str]) -> list[str]:
    try:
        entries = sorted(os.listdir(root))
    except OSError:
        return []
    out = []
    for n in entries:
        if n.startswith(".") or n in deny:
            continue
        p = root / n
        if p.is_dir() and (p / ".git").exists():
            out.append(n)
    return out


def resolve_repo(name: str, policy: dict[str, Any] | None = None) -> RepoTarget:
    _validate_repo_name(name)

    p = dispatch_policy_path()
    if policy is None:
        if not p.is_file():
            raise PreconditionError(f"dispatch policy not found at {p}")
        policy = load_dispatch_policy(p)

    root: Path = policy["root"]
    deny: set[str] = policy["deny"]
    sensitive: set[str] = policy["sensitive"]
    overrides: dict[str, str] = policy["overrides"]
    default_tier: str = policy["default_tier"]

    if name in deny and name not in sensitive:
        raise PolicyError(f"repo '{name}' is not dispatchable: denied by policy")

    path = root / name
    real = Path(os.path.realpath(path))
    if real.parent != root:
        raise PolicyError(f"repo '{name}' resolves outside {root}")

    if not real.is_dir() or not (real / ".git").exists():
        if name in overrides:
            raise PreconditionError(
                f"repo '{name}' carries a tier in {p} but has no checkout under the dispatch root — "
                "the policy names a repo this machine does not have"
            )
        raise UsageError(
            f"repo '{name}' has no git checkout under the dispatch root "
            f"(dispatchable: {', '.join(discoverable(root, deny))})"
        )

    tier = "investigate" if name in sensitive else overrides.get(name, default_tier)
    return RepoTarget(name=name, path=real, max_tier=tier, sensitive=name in sensitive)


def resolve_tier(requested: str, target: RepoTarget) -> str:
    if requested not in VALID_TIERS:
        raise UsageError(f"unknown tier: {requested} (must be one of: {', '.join(VALID_TIERS)})")

    if target.sensitive and requested != "investigate":
        raise PolicyError(
            f"repo '{target.name}' is sensitive (secret-bearing) and only ever permits 'investigate'; "
            f"'{requested}' was requested. A filed issue or a pushed branch has no safe artifact path "
            "in a secret-bearing repo."
        )

    if tier_rank(requested) > tier_rank(target.max_tier):
        raise PolicyError(
            f"repo '{target.name}' is capped at tier '{target.max_tier}' by the dispatch policy; "
            f"'{requested}' was requested"
        )

    return requested


def valid_origin(*, channel: str | None = None, thread_ts: str | None = None,
                  event_id: str | int | None = None) -> None:
    if channel:
        if not channel.startswith("C") or not set(channel) <= set(
            "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
        ):
            raise UsageError(f"not a Slack channel id: {channel} (expected C…)")

    if thread_ts:
        if not thread_ts or not set(thread_ts) <= set("0123456789."):
            raise UsageError(f"not a Slack thread ts: {thread_ts} (expected 1234567890.123456)")
        if not channel:
            raise UsageError("--origin-thread needs --origin-channel: a thread ts alone cannot be delivered to")

    if event_id is not None and event_id != "":
        ok = (isinstance(event_id, int) and not isinstance(event_id, bool)) or (
            isinstance(event_id, str) and event_id.isdigit()
        )
        if not ok:
            raise UsageError(f"--origin-event must be a watchdog events.id integer (got: {event_id})")


@dataclass
class BudgetLimits:
    daily: int
    implement: int
    merge: int


def limits_from_env() -> BudgetLimits:
    return BudgetLimits(
        daily=int(os.environ.get("WARDEN_DAILY_BUDGET", "20")),
        implement=int(os.environ.get("WARDEN_IMPLEMENT_BUDGET", "5")),
        merge=int(os.environ.get("WARDEN_MERGE_BUDGET", "3")),
    )


@dataclass
class BudgetCounts:
    used_today: int
    implement_today: int
    merges_today: int


def budget_counts(conn: sqlite3.Connection, now: dt.datetime) -> BudgetCounts:
    utc_now = now.astimezone(dt.timezone.utc) if now.tzinfo else now
    today = utc_now.date().isoformat()
    used = conn.execute("SELECT COUNT(*) FROM dispatches WHERE created_at >= ?", (today,)).fetchone()[0]
    implement = conn.execute(
        "SELECT COUNT(*) FROM dispatches WHERE created_at >= ? AND tier = ?", (today, "implement")
    ).fetchone()[0]
    merges = conn.execute("SELECT COUNT(*) FROM dispatches WHERE merged_at >= ?", (today,)).fetchone()[0]
    return BudgetCounts(used_today=used, implement_today=implement, merges_today=merges)


def check_dispatch_budget(counts: BudgetCounts, tier: str, limits: BudgetLimits) -> None:
    if counts.used_today >= limits.daily:
        raise PolicyError(
            f"daily dispatch budget exhausted ({counts.used_today}/{limits.daily} opened today, UTC — "
            "resets at 00:00 UTC). This is a structural ceiling on unattended Max spend, not a rate limit. "
            "To proceed now, raise it deliberately: WARDEN_DAILY_BUDGET=<n>"
        )
    if tier == "implement" and counts.implement_today >= limits.implement:
        remaining = max(limits.daily - counts.used_today, 0)
        raise PolicyError(
            f"daily implement budget exhausted ({counts.implement_today}/{limits.implement} opened today, "
            "UTC — resets at 00:00 UTC). Implement is the expensive tier and each episode produces a PR a "
            f"human has to read, so it has its own ceiling; the shared budget still has {remaining} slot(s) "
            "left for the read-only tiers, which is where triage should go. To proceed now, raise it "
            "deliberately: WARDEN_IMPLEMENT_BUDGET=<n>"
        )


def check_merge_budget(counts: BudgetCounts, limits: BudgetLimits) -> None:
    if counts.merges_today >= limits.merge:
        raise PolicyError(
            f"daily merge budget exhausted ({counts.merges_today}/{limits.merge} landed today, UTC — "
            "resets at 00:00 UTC). This is the tightest ceiling in the script because a merge is the only "
            "act here that changes what runs. To proceed now, raise it deliberately: WARDEN_MERGE_BUDGET=<n>"
        )


def budget_json(counts: BudgetCounts, limits: BudgetLimits) -> dict[str, Any]:
    remaining = max(limits.daily - counts.used_today, 0)
    implement_remaining = max(limits.implement - counts.implement_today, 0)
    out: dict[str, Any] = {
        "usedToday": counts.used_today,
        "max": limits.daily,
        "remaining": remaining,
        "implementToday": counts.implement_today,
        "implementMax": limits.implement,
        "implementRemaining": implement_remaining,
    }
    warn: list[str] = []
    if remaining <= BUDGET_WARN_REMAINING:
        warn.append(
            f"{remaining} of {limits.daily} dispatches left today (UTC day, resets 00:00). "
            + ("The next dispatch will be REFUSED. " if remaining == 0 else "")
            + "This is a spend ceiling, not a rate limit — raise it deliberately with "
              "WARDEN_DAILY_BUDGET=<n> if it is wrong, do not retry into it."
        )
    if implement_remaining <= IMPLEMENT_WARN_REMAINING:
        warn.append(
            f"{implement_remaining} of {limits.implement} implement dispatches left today. "
            + (
                "The next implement will be REFUSED (read-only tiers are unaffected). "
                if implement_remaining == 0
                else ""
            )
            + "Raise it deliberately with WARDEN_IMPLEMENT_BUDGET=<n> if it is wrong."
        )
    if warn:
        out["warning"] = " ".join(warn)
    return out


def require_auto_from_item(conn: sqlite3.Connection, *, event_id: int | str, repo: str, tier: str) -> str:
    """Port of the retired bash CLI's `require_auto_from_item` (639-705). Returns the
    linked done investigate job id, or raises one of the nine refusals it
    names verbatim."""
    if tier != "implement":
        raise UsageError(f"--auto-from-item is only valid with --tier implement (got '{tier}')")

    if isinstance(event_id, bool) or not (
        isinstance(event_id, int) or (isinstance(event_id, str) and event_id.isdigit())
    ):
        raise UsageError(f"--auto-from-item must be a triage_items.event_id integer (got: {event_id})")
    event_id_int = int(event_id)

    row = conn.execute(
        "SELECT state, repo, dispatch_job, max_tier FROM triage_items WHERE event_id=?", (event_id_int,)
    ).fetchone()
    if row is None:
        raise PolicyError(
            f"no triage_items row for event_id {event_id_int} — --auto-from-item names a "
            "triage_items.event_id, not a dispatch job id or a bare events.id from another table"
        )
    if row["state"] != "verdict":
        raise PolicyError(
            f"triage item {event_id_int} is in state '{row['state']}', not 'verdict' — --auto-from-item "
            "only fires off a completed investigation"
        )
    if row["max_tier"] != "implement":
        raise PolicyError(
            f"triage item {event_id_int} has max_tier='{row['max_tier']}', not 'implement' — its origin "
            "capped it below auto-implement (Wave 6: a human or a third-party GitHub issue may ask for "
            "investigate-only) and --auto-from-item may not exceed that ceiling"
        )
    if row["repo"] != repo:
        raise PolicyError(
            f"triage item {event_id_int}'s own recorded repo is '{row['repo']}', not '{repo}' — the repo "
            "on this dispatch must match the repo the verdict was actually about"
        )
    job_id = row["dispatch_job"]
    if not job_id:
        raise PolicyError(
            f"triage item {event_id_int} has no linked dispatch_job — nothing was ever investigated for it"
        )
    d = conn.execute("SELECT status, verdict_json FROM dispatches WHERE job_id=?", (job_id,)).fetchone()
    if d is None:
        raise PolicyError(
            f"triage item {event_id_int} points at dispatch job '{job_id}', which has no record in the ledger"
        )
    if d["status"] != "done":
        raise PolicyError(
            f"triage item {event_id_int}'s investigation finished as '{d['status']}', not 'done' — a "
            "failed or still-running episode is not a verdict"
        )
    try:
        verdict_obj = json.loads(d["verdict_json"]) if d["verdict_json"] else None
    except ValueError:
        verdict_obj = None
    if not isinstance(verdict_obj, dict):
        raise PolicyError(f"triage item {event_id_int}'s dispatch recorded no parseable verdict")
    next_action = str(verdict_obj.get("nextAction") or "")
    confidence = str(verdict_obj.get("confidence") or "")
    if next_action != "implement":
        raise PolicyError(
            f"triage item {event_id_int}'s verdict says nextAction='{next_action}', not 'implement' — "
            "--auto-from-item only fires on an investigation that concluded implement is warranted"
        )
    if confidence != "high":
        raise PolicyError(
            f"triage item {event_id_int}'s verdict says confidence='{confidence}', not 'high' — a "
            "medium/low-confidence verdict needs a human, not an unattended implement"
        )
    return job_id


# States that mean "an implement episode is already running against this
# repo" — mirrored from triage.py's own state vocabulary rather than
# imported from it (ledger.py is the schema owner and does not yet mirror
# these two back; see ledger.py's own STATE_* comment for the pattern this
# follows for the states it DOES mirror).
_IN_FLIGHT_STATES = ("implementing", "validating")


def check_repo_not_in_flight(conn: sqlite3.Connection, *, repo: str,
                              exclude_event_id: int | None = None) -> None:
    """DESIGN.md § per-repo in-flight lock. One implement episode per repo at
    a time, checked two ways: the triage item driving it (if any) and the
    operations ledger (which also covers an approval-spend-opened episode
    that has no triage_items row at all).

    `exclude_event_id` is the caller's OWN item — e.g. maybe_auto_implement()
    claims its item to `implementing` (an in-flight state) BEFORE calling
    open_episode(), which runs this check; without the exclusion, that claim
    would make the item refuse itself the moment open_episode() re-checks."""
    placeholders = ",".join("?" for _ in _IN_FLIGHT_STATES)
    query = f"SELECT event_id FROM triage_items WHERE repo=? AND state IN ({placeholders})"
    params: list[Any] = [repo, *_IN_FLIGHT_STATES]
    if exclude_event_id is not None:
        query += " AND event_id != ?"
        params.append(exclude_event_id)
    row = conn.execute(f"{query} LIMIT 1", params).fetchone()
    if row is not None:
        raise PolicyError(
            f"repo '{repo}' already has an implement episode in flight (item {row['event_id']}) — "
            "one at a time per repo"
        )

    ops = operations.open_for_repo(conn, repo=repo, kind="implement")
    if ops:
        raise PolicyError(
            f"repo '{repo}' already has an implement episode in flight (operation {ops[0]['op_id']}) — "
            "one at a time per repo"
        )


def merge_precheck_repo(repo: str, path: Path | None = None) -> None:
    p = path or pr_required_path()
    if not p.is_file():
        raise PreconditionError(
            f"cannot verify merge eligibility: {p} is missing. That file decides which repos require a "
            "human review, so an unreadable one is a refusal, never an assumption."
        )
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError) as err:
        raise PreconditionError(f"could not read {p}: {err}")
    repos = data.get("repos")
    if not isinstance(repos, list) or not all(isinstance(r, str) for r in repos):
        raise PreconditionError(f"could not read merge eligibility from {p}: `repos` is not a list of strings")
    if repo in repos:
        raise PolicyError(
            f"{repo} requires a human pull-request review (it is listed in {p}, the same file the "
            "branch-protection hook enforces). This verb will not merge there — say the PR is ready "
            "and let Johannes merge it."
        )


def triage_repo_entry(repo: str, path: Path | None = None) -> dict[str, Any]:
    p = path or triage_policy_path()
    try:
        data = json.loads(p.read_text(encoding="utf-8"))
    except (OSError, ValueError) as err:
        raise PreconditionError(f"could not read {p}: {err}")
    return (data.get("repos") or {}).get(repo) or {}
