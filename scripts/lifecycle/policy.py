"""policy — origin shape checks, the per-repo in-flight lock.

The Python port of the retired bash CLI's `require_auto_from_item` (639-705) and
`valid_origin` (745-766).

There is no repo/tier policy here: sideclaw is the only boundary (it enforces
its own repo allowlist and tier ceilings and answers a refusal with a 4xx,
which `clients.sideclaw` raises as `SubmitRefused`). A dispatch names the repo
string; `repo_cwd()` composes the one path sideclaw's wire protocol still
requires and decides nothing.
"""

from __future__ import annotations

import json
import os
import sqlite3
from pathlib import Path
from typing import Any

from clients.errors import PolicyError, UsageError

from . import operations

REPO = Path(__file__).resolve().parents[2]

VALID_TIERS = ("investigate", "author", "implement")
GATED_TIERS = ("implement",)


def triage_policy_path() -> Path:
    if os.environ.get("WARDEN_TRIAGE_POLICY"):
        return Path(os.environ["WARDEN_TRIAGE_POLICY"]).expanduser()
    return REPO / "config" / "triage-policy.json"


_NAME_CHARS = set("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_.-")


def repos_root() -> Path:
    """Where the repo checkouts live: `WARDEN_REPOS_ROOT`, else `~/SourceRoot`."""
    return Path(os.path.expanduser(os.environ.get("WARDEN_REPOS_ROOT") or "~/SourceRoot"))


def repo_cwd(name: str) -> Path:
    """`<root>/<name>` — the absolute `cwd` sideclaw's submit wire protocol
    requires. Only the shape of the name is checked (no traversal out of the
    root); whether the repo exists or may be dispatched into is sideclaw's call,
    answered as a 4xx."""
    if not name or name in (".", "..") or name.startswith(".") or not set(name) <= _NAME_CHARS:
        raise UsageError(f"not a repo name: {name}")
    return repos_root() / name


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


def require_auto_from_item(conn: sqlite3.Connection, *, event_id: int | str, repo: str, tier: str) -> str:
    """Port of the retired bash CLI's `require_auto_from_item` (639-705). Returns the
    linked done investigate job id, or raises one of the refusals it
    names verbatim.

    A note on `authorized_by` (the gate `dispatch.open_episode()`/
    `merge.plan_or_land()` actually enforce, `if not authorized_by: raise
    ValueError(...)`): it is a plain truthy-string check, not a validated
    allowlist. `"auto-remediate"`, `"auto-from-item"`, `"cli:confirm"` and
    `"cli:dispatch"` are conventional string shapes a human reader relies on,
    never values this module parses or checks. `"owner:argo"`
    (`notify.apply_argo_actions()`) is another such convention: an action the
    owner pulled off Argo's own pending-actions queue is the owner himself —
    tailnet access to Argo IS him. Nothing here checks the string."""
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
    if row["state"] != "working":
        raise PolicyError(
            f"triage item {event_id_int} is in state '{row['state']}', not 'working' — --auto-from-item "
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
    if next_action not in ("implement", "issue"):
        raise PolicyError(
            f"triage item {event_id_int}'s verdict says nextAction='{next_action}', not 'implement' or "
            "'issue' — --auto-from-item only fires on an investigation that concluded implement is warranted"
        )
    return job_id


def _in_flight_sql(count_merging: bool) -> str:
    """An implement episode is "already running against this repo": an item in `merging` (its PR
    is being reviewed and merged), or a `working` item with an implement_job on record (a claim,
    an episode, or a revision waiting for its next episode).

    `count_merging=False` (a revert): a `merging` item counts only while its train's `update_pr`
    runs (or is being submitted) — a PR waiting on CI or review holds no episode, and a revert
    must not starve behind it. sideclaw's per-repo lease is the backstop for the race (a lease
    retry).

    The state names come from loop/core.py. It is imported here, in the function body, not at
    the top: lifecycle/ stays importable without the loop, and core imports lifecycle/items.py."""
    from loop import core

    working = f"(state='{core.STATE_WORKING}' AND implement_job IS NOT NULL)"
    if count_merging:
        return f"(state='{core.STATE_MERGING}' OR {working})"
    return f"((state='{core.STATE_MERGING}' AND train_job IS NOT NULL) OR {working})"


def check_repo_not_in_flight(conn: sqlite3.Connection, *, repo: str,
                              exclude_event_id: int | None = None, count_merging: bool = True) -> None:
    """DESIGN.md § per-repo in-flight lock. One implement episode per repo at
    a time, checked two ways: the triage item driving it (if any) and the
    operations ledger (which also covers an episode that has no
    triage_items row at all).

    `exclude_event_id` is the caller's OWN item — e.g. maybe_auto_implement()
    claims its item (an in-flight shape) BEFORE calling
    open_episode(), which runs this check; without the exclusion, that claim
    would make the item refuse itself the moment open_episode() re-checks.

    `count_merging=False` (a revert): a `merging` item counts only while an `update_pr` episode
    runs on its train, not while its PR waits."""
    query = f"SELECT event_id FROM triage_items WHERE repo=? AND {_in_flight_sql(count_merging)}"
    params: list[Any] = [repo]
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
