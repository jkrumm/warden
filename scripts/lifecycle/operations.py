"""The operations ledger — the crash-recovery unit (DESIGN.md § Crash recovery).

An operation row is committed BEFORE the external call it covers, and that
commit is the whole contract: a crash inside the call leaves a row with
`outcome IS NULL` that `reconcile_operations()` (triage.py) resolves on the
next pass, before anything could retry.

Four kinds, each one external mutation:

  implement  — a sideclaw implement episode (branch + draft PR)
  merge      — ready-for-review + PUT /merge + branch delete
  deploy     — the rollout after a merge: either the closed-allowlist argv
               (`autoDeploy`) or the GitHub Actions run a push to the default
               branch triggers (`deployOnMerge`). Its own write-point since
               Wave 5 — docs/history/state-log.md §48's "one operation, not two" limitation
               existed only because of the subprocess boundary.
  host       — an idempotent HOST_VERB_ALLOWLIST argv (triage.py's
               maybe_auto_remediate(), STATE.md's 2026-09-11 owner decision) —
               a process restart run BY warden itself, on the same
               crash-recovery contract as the other three: recorded before
               the subprocess runs, resolved after it returns. Has no remote
               receipt to reconcile from (see reconcile_operations()'s own
               `host` branch) — a crashed run always resolves `unknown`.

`investigate` and validation episodes deliberately get none: they run
read-only in their own worktree and dispatch-sweep.py already covers a
forgotten job.
"""
from __future__ import annotations

import sqlite3
import uuid

import ledger

KINDS = ("implement", "merge", "deploy", "host")
OUTCOMES = ("done", "failed", "unknown")

_now_iso = ledger.now_iso


def record(conn: sqlite3.Connection, *, event_id: int | None, kind: str, repo: str,
           authorized_by: str, note: str | None = None, commit: bool = True) -> str:
    """Mint and durably record an operation before the external call.

    `commit=False` lets a caller fold this INSERT into a transaction of its
    own; that caller then owns the commit and must make it before the call.
    `kind` reaches SQL and is closed on purpose."""
    if kind not in KINDS:
        raise ValueError(f"{kind!r} not in KINDS={KINDS} — kind reaches SQL, closed on purpose")
    op_id = str(uuid.uuid4())
    conn.execute(
        "INSERT INTO operations(op_id, event_id, kind, repo, authorized_by, started_at, note) "
        "VALUES (?,?,?,?,?,?,?)",
        (op_id, event_id, kind, repo, authorized_by, _now_iso(), note),
    )
    if commit:
        conn.commit()
    return op_id


def complete(conn: sqlite3.Connection, op_id: str, *, outcome: str,
             receipt: str | None = None, note: str | None = None) -> None:
    """Resolve an operation after its call returned. Never call this for an
    ambiguous return — leave `outcome` NULL and let reconciliation decide.
    `receipt` is already-serialised JSON text; this function has no opinion
    about its shape."""
    if outcome not in OUTCOMES:
        raise ValueError(f"{outcome!r} not in OUTCOMES={OUTCOMES} — outcome reaches SQL, closed on purpose")
    conn.execute(
        "UPDATE operations SET outcome=?, outcome_at=?, receipt_json=COALESCE(?, receipt_json), "
        "note=COALESCE(?, note) WHERE op_id=?",
        (outcome, _now_iso(), receipt, note, op_id),
    )
    conn.commit()


def open_for_repo(conn: sqlite3.Connection, *, repo: str, kind: str) -> list[sqlite3.Row]:
    """Operations of `kind` on `repo` whose outcome is still NULL — the
    per-repo in-flight lock reads this."""
    return conn.execute(
        "SELECT * FROM operations WHERE repo=? AND kind=? AND outcome IS NULL ORDER BY started_at",
        (repo, kind),
    ).fetchall()
