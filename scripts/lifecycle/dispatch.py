"""dispatch — brief/context validation, episode opening, and the dispatch
record (the Python port of the retired bash CLI's `read_brief`/`read_context`
715-738, `record_dispatch` 1195-1211, `sync_record` 1470-1498, `cmd_list`
1557-1585, and the submit half of `cmd_dispatch` 1395-1414).

`open_episode()` is the one function in this module that did not exist in
the bash script in this shape: the old `--confirm` re-invocation is gone
(docs/history/state-log.md §46), so opening the sideclaw episode for a signed approval now
happens in-process, in `lifecycle/approvals.py`'s `execute_approved()`,
which calls this function rather than re-running a CLI.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
from dataclasses import dataclass

from clients import sideclaw
from clients.errors import PolicyError, RemoteError, UsageError

from . import chaos, operations, policy

MAX_BRIEF_CHARS = 8000
MAX_CONTEXT_CHARS = 16000


def normalize_brief(text: str) -> str:
    """Trailing whitespace stripped per line, same as the bash `sed -e
    's/[[:space:]]*$//'`. Empty (after stripping) or oversize is a
    UsageError — a caller mistake, never a remote effect."""
    normalized = "\n".join(line.rstrip() for line in text.split("\n"))
    if not normalized.strip():
        raise UsageError("brief is empty")
    if len(normalized) > MAX_BRIEF_CHARS:
        raise UsageError(
            f"brief is {len(normalized)} chars, over the {MAX_BRIEF_CHARS} limit — summarize it, or "
            "attach the bulk with --context-file"
        )
    return normalized


def check_context(text: str | None) -> str | None:
    if text is None:
        return None
    if len(text) > MAX_CONTEXT_CHARS:
        raise UsageError(f"context is {len(text)} chars, over the {MAX_CONTEXT_CHARS} limit")
    return text


@dataclass(frozen=True)
class Origin:
    channel: str | None = None
    thread_ts: str | None = None
    event_id: int | None = None


@dataclass
class Opened:
    job: dict
    job_id: str
    op_id: str | None


def _insert_dispatch_row(
    conn: sqlite3.Connection,
    *,
    job_id: str,
    tier: str,
    repo: str,
    brief: str,
    why: str | None,
    origin: Origin,
    status: str,
    now: dt.datetime,
) -> None:
    """The one `dispatches` INSERT shape — `open_episode()` and
    `open_review()` share it rather than each carrying its own copy, so this
    table never grows a THIRD hand-written INSERT with its own drift."""
    conn.execute(
        "INSERT INTO dispatches(job_id,tier,repo,brief,why,origin_channel,origin_thread_ts,"
        "origin_event_id,status,created_at) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (
            job_id,
            tier,
            repo,
            brief,
            why or None,
            origin.channel or None,
            origin.thread_ts or None,
            origin.event_id,
            status,
            now.isoformat(),
        ),
    )


def open_episode(
    conn: sqlite3.Connection,
    *,
    target: policy.RepoTarget,
    tier: str,
    brief: str,
    context: str | None,
    why: str | None,
    model: str | None,
    origin: Origin,
    authorized_by: str | None,
    op_id: str | None = None,
    now: dt.datetime | None = None,
) -> Opened:
    """Submit one sideclaw episode and record it.

    A gated tier (`implement`) is covered by an `operations` row committed
    BEFORE the submit — DESIGN.md § Crash recovery. If the caller has not
    already recorded one (the signed-approval spend records its own, in the
    same transaction as `spent_at`, and passes `op_id` in), one is minted
    here; `authorized_by` is required in that case."""
    policy.require_no_recursion()
    now = now or dt.datetime.now(dt.timezone.utc)
    gated = tier in policy.GATED_TIERS

    opened_op_id = op_id
    if gated and opened_op_id is None:
        if not authorized_by:
            raise ValueError("authorized_by is required to open a gated episode")
        # check_repo_not_in_flight() is a bare SELECT; the operations row
        # that actually IS the lock is written after it returns. Two
        # concurrent opens could both pass the check before either had
        # committed that row. BEGIN IMMEDIATE takes the write lock before
        # the check, so a second connection blocks (or times out) on this
        # same statement rather than racing past a check whose answer the
        # first caller's own commit hasn't landed yet.
        if conn.in_transaction:
            raise RuntimeError(
                "open_episode() was called with an already-open transaction on `conn` — "
                "BEGIN IMMEDIATE cannot nest; the caller must not hold one open across this call"
            )
        conn.execute("BEGIN IMMEDIATE")
        try:
            # The loop's own claimed item (already flipped to `implementing`
            # by the caller, e.g. maybe_auto_implement()'s compare-and-set,
            # BEFORE it calls this function) must not refuse itself.
            policy.check_repo_not_in_flight(conn, repo=target.name, exclude_event_id=origin.event_id)
            opened_op_id = operations.record(
                conn,
                event_id=origin.event_id,
                kind="implement",
                repo=target.name,
                authorized_by=authorized_by,
                commit=False,
            )
        except PolicyError:
            conn.rollback()
            raise
        conn.commit()
        chaos.crash_point("after-implement-op")

    try:
        job = sideclaw.submit(
            cwd=str(target.path),
            tier=tier,
            brief=brief,
            context=context,
            sensitive=target.sensitive,
            model=model,
        )
    except RemoteError as exc:
        if gated and opened_op_id is not None:
            operations.complete(
                conn,
                opened_op_id,
                outcome="unknown" if exc.maybe_mutated else "failed",
                receipt=json.dumps({"error": str(exc)}),
            )
        raise

    chaos.crash_point("after-implement-submit")

    job_id = job["id"]
    status = job.get("status") or "unknown"
    _insert_dispatch_row(conn, job_id=job_id, tier=tier, repo=target.name, brief=brief, why=why,
                         origin=origin, status=status, now=now)
    conn.commit()

    if gated and opened_op_id is not None:
        operations.complete(conn, opened_op_id, outcome="done", receipt=json.dumps({"jobId": job_id}))

    return Opened(job=job, job_id=job_id, op_id=opened_op_id)


def open_review(
    conn: sqlite3.Connection,
    *,
    target: policy.RepoTarget,
    pr: int,
    context: str | None,
    origin: Origin,
    model: str | None = None,
    now: dt.datetime | None = None,
) -> Opened:
    """Submit one sideclaw `review` episode and record it in `dispatches`
    with `tier='review'` — the review counterpart to `open_episode()` above,
    sharing its exact `dispatches` INSERT shape (job_id, tier, repo, brief,
    why, origin_channel, origin_thread_ts, origin_event_id, status,
    created_at) so this table never grows a THIRD code path writing its own
    row. `brief` has no review equivalent — `dispatches.brief` is NOT NULL,
    so this stores a short description of what was reviewed instead.

    Not gated: `review` runs read-only on sideclaw's side (no write tool
    profile, no branch, no PR), so unlike `open_episode()`'s `implement`
    path there is no `operations` row to cover a crash between submit and
    record — a review job with no matching `dispatches` row is, at worst, an
    orphaned read-only sideclaw session, not an unaccounted mutation.

    `model` mirrors `open_episode()`'s own parameter — `None` leaves the job
    on sideclaw's own routing, a non-Claude IU model id pins it there
    instead."""
    policy.require_no_recursion()
    now = now or dt.datetime.now(dt.timezone.utc)
    job = sideclaw.submit_review(cwd=target.path, pr=pr, context=context, model=model)
    job_id = job["id"]
    status = job.get("status") or "unknown"
    _insert_dispatch_row(conn, job_id=job_id, tier="review", repo=target.name,
                         brief=f"review PR #{pr}", why=None, origin=origin, status=status, now=now)
    conn.commit()
    return Opened(job=job, job_id=job_id, op_id=None)


def sync_record(conn: sqlite3.Connection, job: dict, *, reported: bool, now: dt.datetime | None = None) -> None:
    """Fold a terminal job's outcome back into its dispatch row — the Python
    port of the retired bash CLI's `sync_record` (1470-1498), schema-6
    `delivery_status` included."""
    now = now or dt.datetime.now(dt.timezone.utc)
    now_iso = now.isoformat()
    result = job.get("result")
    artifact = (result.get("artifactUrl") or None) if isinstance(result, dict) else None
    reported_at = now_iso if reported else None
    conn.execute(
        "UPDATE dispatches SET status=?, verdict_json=?, artifact_url=?, finished_at=?, "
        "reported_at=COALESCE(reported_at, ?), "
        "delivery_status=COALESCE(delivery_status, CASE WHEN ? IS NOT NULL THEN ? END) "
        "WHERE job_id=?",
        (
            job.get("status"),
            json.dumps(result) if result is not None else None,
            artifact,
            now_iso,
            reported_at,
            reported_at,
            "delivered",
            job.get("id"),
        ),
    )
    conn.commit()


def list_dispatches(conn: sqlite3.Connection, scope: str, now: dt.datetime) -> list[dict]:
    if scope not in ("open", "today", "all"):
        raise UsageError(f"unknown list scope: {scope} (must be one of: open today all)")

    if scope == "open":
        terminal = tuple(sideclaw.TERMINAL)
        placeholders = ",".join("?" for _ in terminal)
        rows = conn.execute(
            f"SELECT * FROM dispatches WHERE status NOT IN ({placeholders}) OR reported_at IS NULL "
            "ORDER BY id DESC LIMIT 50",
            terminal,
        ).fetchall()
    elif scope == "today":
        today = now.date().isoformat()
        rows = conn.execute(
            "SELECT * FROM dispatches WHERE created_at >= ? ORDER BY id DESC", (today,)
        ).fetchall()
    else:
        rows = conn.execute("SELECT * FROM dispatches ORDER BY id DESC LIMIT 50").fetchall()

    out = []
    for r in rows:
        d = dict(r)
        d.pop("brief", None)
        d.pop("verdict_json", None)
        out.append(d)
    return out
