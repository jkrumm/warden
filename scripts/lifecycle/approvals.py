"""approvals — mint a signed-approval request, and spend one once it is
decided.

The Python port of the retired bash CLI's `approval_hash` (963-972),
`post_approval_buttons` (977-1038), `mint_approval` (1081-1114) and
`require_signed_approval` (1119-1191) — reshaped for docs/history/state-log.md §46's Shape
note: the bash `--confirm` re-invocation is gone. `intents.py`'s `drain()`
is what lands a signed Slack decision onto the `dispatch_approvals` row
(`decision`, `decided_at`, `decided_by`, `signature`); `execute_approved()`
below is what turns a decided-and-unspent row into a running episode, in one
transaction that commits `spent_at` and the `operations` row together
(DESIGN.md § Crash recovery) before the sideclaw submit is ever attempted —
a crash after that commit leaves an open operation reconciliation resolves,
never a burned approval with nothing recorded.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import secrets as _secrets
import sqlite3
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from clients import signer, slack
from clients.errors import PolicyError, PreconditionError, RemoteError

from . import dispatch, operations, policy

# The closed parameter dict a mint carries and a spend replays. Anything
# else is a caller mistake — see `mint()`.
_ALLOWED_PARAMS = frozenset({"why", "model", "origin_channel", "origin_thread_ts", "origin_event_id"})

# Every outcome `execute_approved()` can return.
_STATUSES = frozenset(
    {"opened", "refused", "invalid", "superseded", "expired", "denied", "already", "pending", "failed"}
)


def ttl_minutes() -> int:
    return int(os.environ.get("WARDEN_APPROVAL_TTL", "30"))


def _hermes_home() -> Path:
    return Path(os.environ.get("HERMES_HOME", str(Path.home() / ".hermes"))).expanduser()


def pubkey_path() -> Path:
    if os.environ.get("WARDEN_APPROVAL_PUBKEY"):
        return Path(os.environ["WARDEN_APPROVAL_PUBKEY"]).expanduser()
    return _hermes_home() / "dispatch-approval.pub"


def post_buttons(*, nonce: str, verb: str, repo: str, tier: str, channel: str | None,
                  why: str | None, thread_ts: str | None = None) -> None:
    """Best-effort, exactly like the bash `post_approval_buttons`: a Slack
    failure must never look like a refusal. The row is already on file by
    the time this runs, so the worst outcome of anything in here failing is
    that nobody sees a button — never raises."""
    if not channel:
        print("note: no --origin-channel, so no buttons could be posted", file=sys.stderr)
        return
    token = slack.resolve_interactive_token()
    if not token:
        print("note: no SLACK_BOT_TOKEN, so no buttons could be posted", file=sys.stderr)
        return

    ttl = ttl_minutes()
    text = "\n".join(
        [
            f":lock: *Approval needed* — `{verb}` {tier} on `{repo}`",
            f"*Why:* {why or '(none given)'}",
            f"_Expires in {ttl} min._",
        ]
    )
    blocks = [
        {"type": "section", "text": {"type": "mrkdwn", "text": text}},
        {
            "type": "actions",
            "elements": [
                {
                    "type": "button",
                    "style": "primary",
                    "text": {"type": "plain_text", "text": "Approve"},
                    "action_id": "hermes_cc_approve",
                    "value": nonce,
                },
                {
                    "type": "button",
                    "style": "danger",
                    "text": {"type": "plain_text", "text": "Deny"},
                    "action_id": "hermes_cc_deny",
                    "value": nonce,
                },
            ],
        },
    ]
    resp = slack.slack_post_blocks(token, channel, text, blocks, thread_ts=thread_ts)
    if not resp.get("ok"):
        print(f"note: Slack rejected the button message: {json.dumps(resp)[:200]}", file=sys.stderr)


def mint(
    conn: sqlite3.Connection,
    *,
    verb: str,
    repo: str,
    tier: str,
    body: str,
    why: str | None,
    context: str | None,
    channel: str | None,
    params: dict[str, Any],
    argv: list[str],
    now: dt.datetime | None = None,
) -> str:
    unknown = set(params) - _ALLOWED_PARAMS
    if unknown:
        raise ValueError(
            f"unknown approval param(s) {sorted(unknown)} — params is a closed dict: {sorted(_ALLOWED_PARAMS)}"
        )

    # Loaded FIRST — nobody could click if there is no key to verify a
    # decision against, so a mint with no verifier available refuses rather
    # than posting buttons nobody can ever satisfy.
    pub = signer.load_pubkey(pubkey_path())

    now = now or dt.datetime.now(dt.timezone.utc)
    nonce = _secrets.token_hex(16)
    payload_hash = signer.payload_hash(verb, repo, tier, body, why or "", context or "")
    expires_at = now + dt.timedelta(minutes=ttl_minutes())

    # Supersede any older pending request for the identical payload — a
    # caller that re-plans the same thing twice must not leave two live
    # buttons that both work.
    conn.execute(
        "DELETE FROM dispatch_approvals WHERE payload_hash=? AND decision IS NULL", (payload_hash,)
    )
    conn.execute(
        "INSERT INTO dispatch_approvals(nonce,verb,repo,tier,payload_hash,created_at,expires_at,channel,"
        "argv_json,stdin_text,context_text,key_id,params_json) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            nonce,
            verb,
            repo,
            tier,
            payload_hash,
            now.isoformat(),
            expires_at.isoformat(),
            channel or None,
            json.dumps(argv),
            body if verb == "dispatch" else None,
            context or None,
            signer.key_id(pub),
            json.dumps(params, sort_keys=True),
        ),
    )
    conn.commit()

    post_buttons(
        nonce=nonce, verb=verb, repo=repo, tier=tier, channel=channel, why=why,
        thread_ts=params.get("origin_thread_ts"),
    )
    return nonce


def pending_approved(conn: sqlite3.Connection, now: dt.datetime) -> list[sqlite3.Row]:
    """Decided-approve, unspent, unexpired — the loop's retry sweep for a
    spend that refused on an in-flight lock last time."""
    return conn.execute(
        "SELECT * FROM dispatch_approvals WHERE decision='approve' AND spent_at IS NULL AND expires_at > ?",
        (now.isoformat(),),
    ).fetchall()


@dataclass
class SpendResult:
    status: str
    nonce: str
    job_id: str | None = None
    op_id: str | None = None
    reason: str | None = None


def _parse_dt(text: str) -> dt.datetime:
    parsed = dt.datetime.fromisoformat(text)
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=dt.timezone.utc)


def _record_spend_error(conn: sqlite3.Connection, nonce: str, reason: str) -> None:
    conn.execute("UPDATE dispatch_approvals SET spend_error=? WHERE nonce=?", (reason, nonce))
    conn.commit()


def execute_approved(conn: sqlite3.Connection, nonce: str, *, now: dt.datetime | None = None) -> SpendResult:
    now = now or dt.datetime.now(dt.timezone.utc)
    row = conn.execute("SELECT * FROM dispatch_approvals WHERE nonce=?", (nonce,)).fetchone()
    if row is None:
        raise ValueError(f"no approval row for nonce {nonce}")

    if row["spent_at"] is not None:
        return SpendResult(status="already", nonce=nonce, job_id=row["spent_job_id"])

    if row["decision"] is None:
        return SpendResult(status="pending", nonce=nonce)

    if row["decision"] != "approve":
        return SpendResult(status="denied", nonce=nonce)

    if _parse_dt(row["expires_at"]) < now:
        _record_spend_error(conn, nonce, "expired")
        return SpendResult(status="expired", nonce=nonce)

    pub = signer.load_pubkey(pubkey_path())
    current_key_id = signer.key_id(pub)
    message = signer.canonical_message(
        nonce, row["payload_hash"], row["decision"], row["decided_by"] or "", row["expires_at"]
    )
    verified = bool(row["signature"]) and signer.verify(pub, row["signature"], message)
    if not verified:
        if row["key_id"] and row["key_id"] != current_key_id:
            reason = f"signed under key {row['key_id']}, current is {current_key_id}"
            _record_spend_error(conn, nonce, reason)
            return SpendResult(status="superseded", nonce=nonce, reason=reason)
        reason = "signature does not verify"
        _record_spend_error(conn, nonce, reason)
        return SpendResult(status="invalid", nonce=nonce, reason=reason)

    if row["params_json"] is None:
        # Minted by the retired bash CLI, before schema 7 added params_json —
        # distinguishable from a tampered row (which would fail the
        # signature check above, not this one). There is nothing here to
        # replay `why`/`model`/origin from, so this can never safely spend;
        # a fresh mint carries the column.
        reason = "minted before schema 7 — re-plan to get a fresh approval"
        _record_spend_error(conn, nonce, reason)
        return SpendResult(status="invalid", nonce=nonce, reason=reason)

    params = json.loads(row["params_json"])

    # The signature covers `payload_hash`, and `payload_hash` covers the
    # bytes that will be dispatched. Recompute it from the row's OWN stored
    # payload at spend time — the retired bash verifier did this from what
    # it held in argv+stdin, and dropping it would let anyone with write
    # access to the ledger swap `stdin_text` under a valid signature.
    recomputed = signer.payload_hash(
        row["verb"], row["repo"], row["tier"], row["stdin_text"] or "",
        params.get("why") or "", row["context_text"] or "",
    )
    if recomputed != row["payload_hash"]:
        reason = "stored payload does not match the signed payload_hash (brief, why or context changed after mint)"
        _record_spend_error(conn, nonce, reason)
        return SpendResult(status="invalid", nonce=nonce, reason=reason)

    # Re-checked at spend time, against the CURRENT policy and CURRENT
    # standing counts — the plan-time check is stale by the time a human
    # clicks, sometimes by hours. BEGIN IMMEDIATE takes the write lock
    # BEFORE the checks run, so the checks, `spent_at`, and the operations
    # row commit as one write-locked transaction — check_repo_not_in_flight()
    # alone is a bare SELECT and cannot close this race on its own (mirrors
    # lifecycle/dispatch.py's open_episode(), which does the same for the
    # auto-from-item path).
    if conn.in_transaction:
        raise RuntimeError(
            "execute_approved() was called with an already-open transaction on `conn` — "
            "BEGIN IMMEDIATE cannot nest; the caller must not hold one open across this call"
        )
    conn.execute("BEGIN IMMEDIATE")
    try:
        target = policy.resolve_repo(row["repo"])
        policy.resolve_tier(row["tier"], target)
        policy.check_repo_not_in_flight(conn, repo=row["repo"])
    except (PolicyError, PreconditionError) as exc:
        conn.rollback()
        # Refused, approval stays UNSPENT — retryable on a later
        # `pending_approved()` pass, until it expires.
        _record_spend_error(conn, nonce, str(exc))
        return SpendResult(status="refused", nonce=nonce, reason=str(exc))

    # THE SPEND. `spent_at` and the `operations` row commit in one
    # transaction, before the episode is ever submitted.
    cur = conn.execute(
        "UPDATE dispatch_approvals SET spent_at=?, spend_error=NULL WHERE nonce=? AND spent_at IS NULL",
        (now.isoformat(), nonce),
    )
    if cur.rowcount == 0:
        conn.rollback()
        return SpendResult(status="already", nonce=nonce)

    op_id = operations.record(
        conn,
        event_id=params.get("origin_event_id"),
        kind="implement",
        repo=row["repo"],
        authorized_by=f"signed:{row['decided_by']}",
        commit=False,
    )
    conn.commit()

    origin = dispatch.Origin(
        channel=params.get("origin_channel"),
        thread_ts=params.get("origin_thread_ts"),
        event_id=params.get("origin_event_id"),
    )
    try:
        opened = dispatch.open_episode(
            conn,
            target=target,
            tier=row["tier"],
            brief=row["stdin_text"] or "",
            context=row["context_text"],
            why=params.get("why"),
            model=params.get("model"),
            origin=origin,
            authorized_by=f"signed:{row['decided_by']}",
            op_id=op_id,
            now=now,
        )
    except RemoteError as exc:
        # The approval IS spent (single-use, consumed above) and
        # open_episode() has already resolved the operation as
        # failed/unknown — this is not a "refused", it is a spend that
        # could not complete its submit.
        _record_spend_error(conn, nonce, str(exc))
        return SpendResult(status="failed", nonce=nonce, op_id=op_id, reason=str(exc))

    conn.execute("UPDATE dispatch_approvals SET spent_job_id=? WHERE nonce=?", (opened.job_id, nonce))
    conn.commit()
    return SpendResult(status="opened", nonce=nonce, job_id=opened.job_id, op_id=op_id)
