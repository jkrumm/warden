"""Slack lines, the daily failed digest, and the Argo projection (snapshot push, owner actions).
Slack hears one line when an item is `fixed` or `needs_decision`; everything else lives in Argo.
Dry-run never touches Slack or Argo."""

from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import sys
from typing import Any

from clients import argo as _argo
from clients.errors import PolicyError, PreconditionError, RemoteError, SubmitRefused, UsageError
from lifecycle import dispatch as _dispatch, items as _items
from loop import core, work, train


def _escape(text: str) -> str:
    return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")


def argo_warden_url() -> str:
    """Argo's /warden page (no per-item deep link exists): the configured Argo base
    (clients/argo.py `argo_api_base()`, `ARGO_URL`) without its trailing `/api`."""
    return f"{_argo.argo_api_base().removesuffix('/api')}/warden"


def argo_link() -> str:
    return f"<{argo_warden_url()}|Argo>"


def format_notification(state: str, primary: sqlite3.Row, event_row: sqlite3.Row, member_count: int) -> str:
    """The one Slack line: `<icon> <repo>: <summary> — <state> <Argo link>`. The summary
    is the item's note (for `needs_decision` the decision question), else the event's
    title, as one line of at most 200 characters."""
    fallback = f"{member_count} related alerts" if member_count > 1 else (event_row["title"] or primary["signature"])
    summary = _items.cap_note(primary["note"]) or _items.cap_note(fallback) or primary["signature"]
    return f"{core.NOTIFY_ICON[state]} {primary['repo'] or 'warden'}: {_escape(summary)} — {state} {argo_link()}"


def _announce_state(item: sqlite3.Row) -> str | None:
    """The state a Slack line is owed for, or None. Besides NOTIFY_STATES, an item that
    asked for an answer (`closed(resolved)`, max_tier=investigate) and has its own origin
    channel is owed NOTIFY_ANSWERED — in that thread, never in the shared channel."""
    if item["state"] in core.NOTIFY_STATES:
        return item["state"]
    if (item["state"] == core.STATE_CLOSED and item["close_reason"] == core.CLOSE_RESOLVED
            and item["max_tier"] == "investigate" and item["origin_channel"]):
        return core.NOTIFY_ANSWERED
    return None


def _notify_claim_live(card_hash: str | None, state: str, now: dt.datetime) -> bool:
    """True while another process holds the posting claim for `state` — a stale claim
    (older than NOTIFY_CLAIM_STALE_S, or unreadable) is a dead poster's and is retaken."""
    prefix = f"{core.NOTIFY_CLAIM_PREFIX}{state}:"
    if not card_hash or not card_hash.startswith(prefix):
        return False
    claimed_at = core._parse_ts(card_hash[len(prefix):])
    return claimed_at is not None and (now - claimed_at).total_seconds() < core.NOTIFY_CLAIM_STALE_S


def notify_cluster(conn: sqlite3.Connection, members: list[sqlite3.Row], event_rows: list[sqlite3.Row],
                   policy: dict[str, Any], *, dry_run: bool) -> None:
    """Post ONE plain line to Slack for each NOTIFY_STATES state (and NOTIFY_ANSWERED) `members`
    has entered and not yet announced; everything else is silent (it lives in Argo). A cluster
    (several members, one state) posts once, for its first member. Dedupe is `card_hash` = the
    state posted for, copied onto every member of that state: _set_state() clears it when an item
    leaves the state, so a re-entry posts again and a re-run of the same pass never posts twice.
    Only Slack's own `ok: true` stamps it; a failed post is retried on the next pass.

    The loop and the sweep both call this, so the post is CLAIMED first: a compare-and-set swaps
    `card_hash` for `posting:<state>:<now>`, only the winner posts, and on a Slack failure the
    claim is handed back. A claim older than NOTIFY_CLAIM_STALE_S belongs to a dead poster and is
    retaken.

    An item with its own origin thread (`warden run`, Hermes) is answered there instead of in the
    shared channel. `dry_run` never touches Slack."""
    now = dt.datetime.now(dt.timezone.utc)
    for state in (*core.NOTIFY_STATES, core.NOTIFY_ANSWERED):
        pairs = [(m, e) for m, e in zip(members, event_rows)
                 if _announce_state(m) == state and m["card_hash"] != state
                 and not _notify_claim_live(m["card_hash"], state, now)]
        if not pairs:
            continue
        primary, event_row = pairs[0]
        channel = primary["origin_channel"] or core._card_channel(policy)
        thread_ts = primary["origin_thread_ts"] if primary["origin_channel"] else None
        if dry_run:
            print(f"[dry-run] would post to {channel}: {format_notification(state, primary, event_row, len(pairs))}")
            continue
        token = core.resolve_slack_token()
        if not token:
            print(f"triage: no Slack token, cannot post for {[m['signature'] for m, _ in pairs]}", file=sys.stderr)
            continue
        claim = f"{core.NOTIFY_CLAIM_PREFIX}{state}:{core._now_iso(now)}"
        won = [(m, e) for m, e in pairs
               if conn.execute("UPDATE triage_items SET card_hash=? WHERE event_id=? AND state=? AND card_hash IS ?",
                               (claim, m["event_id"], m["state"], m["card_hash"])).rowcount]
        conn.commit()
        if not won:
            continue
        primary, event_row = won[0]
        ok, ts = core.post_line(channel, format_notification(state, primary, event_row, len(won)), token,
                                thread_ts=thread_ts)
        if not ok:
            for m, _e in won:
                conn.execute("UPDATE triage_items SET card_hash=? WHERE event_id=? AND card_hash=?",
                             (m["card_hash"], m["event_id"], claim))
            conn.commit()
            continue
        for m, _e in won:
            conn.execute("UPDATE triage_items SET card_channel=?, card_ts=?, card_hash=? "
                         "WHERE event_id=? AND card_hash=?", (channel, ts, state, m["event_id"], claim))
        conn.commit()


def maybe_post_daily_digest(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                             *, dry_run: bool) -> None:
    """`failed` never posts when an item enters it; instead one line, at most once per UTC
    day, says how many are failed: `:x: <n> failed — <Argo link>`. Silent at zero."""
    failed = conn.execute("SELECT count(*) c FROM triage_items WHERE state=?", (core.STATE_FAILED,)).fetchone()["c"]
    if not failed:
        return
    today = now.date().isoformat()
    row = conn.execute("SELECT value FROM cursors WHERE key=?", (core.DAILY_DIGEST_CURSOR_KEY,)).fetchone()
    if row and row["value"] == today:
        return

    text = f":x: {failed} failed — {argo_link()}"
    channel = core._card_channel(policy)
    if dry_run:
        print(f"[dry-run] would post to {channel}: {text}")
        return
    token = core.resolve_slack_token()
    if not token:
        print("triage: no Slack token, cannot post daily digest", file=sys.stderr)
        return
    ok, _ts = core.post_line(channel, text, token)
    if not ok:
        return
    conn.execute(
        "INSERT INTO cursors(key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (core.DAILY_DIGEST_CURSOR_KEY, today, core._now_iso(now)),
    )
    conn.commit()


# Argo (the VPS dashboard) cannot reach this box: warden-api binds loopback only. So the mini
# pushes its projection to Argo. A non-200 push is logged as a plain status line, never a failed
# tick.

ARGO_SNAPSHOT_ITEMS_CAP = 50

# Passed as item_payload()'s history_limit for every embedded item: a flapping item's
# transitions/operations otherwise grow without bound.
ARGO_SNAPSHOT_HISTORY_LIMIT = 50

# The closed set of verbs Argo's action queue may name, like HOST_VERB_ALLOWLIST: the string
# reaches a dispatcher that branches on it, so an unrecognized value is a loud, acked rejection,
# never a silent drop or a guess.
ARGO_ACTION_VERBS = frozenset({"implement", "merge", "dismiss", "reinvestigate", "note"})

# Owner actions are gated on the same state sets api.py's `availableActions` offers (mirrored
# there by literal value, kept in sync by hand). An owner dismissal is the same terminal call as
# a human `warden close`, so it may end anything that has not started work (`new`, `triaged`),
# anything waiting on the owner (`needs_decision`, `failed`) and a `quiet` item.
_ARGO_DISMISS_ALLOWED_STATES = (
    core.STATE_NEW, core.STATE_TRIAGED, core.STATE_NEEDS_DECISION, core.STATE_FAILED, core.STATE_QUIET,
)
# The same without `new`/`triaged`: an item not yet investigated has nothing to re-investigate;
# escalate()/escalate_origin_items() pick it up on their own.
_ARGO_REINVESTIGATE_ALLOWED_STATES = (core.STATE_NEEDS_DECISION, core.STATE_FAILED, core.STATE_QUIET)
_ARGO_IMPLEMENT_ALLOWED_STATES = (core.STATE_NEEDS_DECISION, core.STATE_FAILED)
_ARGO_MERGE_ALLOWED_STATES = (core.STATE_NEEDS_DECISION, core.STATE_FAILED)
# An item with `revert_pr` set was merged and rolled back by hand: implementing or merging it
# again would redo the reverted change. api.py's `availableActions` offers neither.
_ARGO_REVERTED_REFUSAL = "this item was reverted (revert_pr is set) — implement/merge would redo the reverted change"


def _apply_argo_implement(conn: sqlite3.Connection, item: sqlite3.Row, event_id: int,
                           now: dt.datetime) -> tuple[str, dict[str, Any] | None, str | None]:
    if item["state"] not in _ARGO_IMPLEMENT_ALLOWED_STATES:
        return "rejected", None, f"item is in state {item['state']!r}, not needs_decision/failed"
    if item["revert_pr"] is not None:
        return "rejected", None, _ARGO_REVERTED_REFUSAL

    # No `expect_null=("implement_job",)` here, unlike maybe_auto_implement()'s claim: this handler
    # accepts `needs_decision`/`failed` items carrying a STALE implement_job from a prior attempt, and
    # refusing on that column would make retrying from Argo impossible for exactly the item the action
    # exists to unstick. Overwritten below on success. The owner's attempt replaces whatever automatic
    # revert the item was on (maybe_submit_reverts()).
    claimed = core._set_state(conn, event_id, core.STATE_WORKING, now, expect_state=item["state"],
                              implement_job=core.IMPLEMENT_CLAIM, validation_job=None, pr_url=None,
                              reverting_sha=None, revert_json=None)
    conn.commit()
    if not claimed:
        return "rejected", None, "item state changed before this action could be applied — retry from Argo"

    if item["dispatch_job"]:
        d = conn.execute(
            "SELECT verdict_json FROM dispatches WHERE job_id=?", (item["dispatch_job"],)
        ).fetchone()
        verdict = core._safe_json(d["verdict_json"]) if d else {}
        context = work._verdict_as_context(item["dispatch_job"], verdict)
        brief = (
            "The owner reviewed this item in Argo and asked for it to be implemented directly. "
            "Re-read the prior investigation's own verdict and evidence yourself (it ran against "
            "this exact repo) before writing anything, then implement the fix it describes. If "
            "what you find on re-reading no longer supports that conclusion, say so in your own "
            "verdict and stop rather than forcing a change."
        )
    else:
        context = None
        brief = "The owner asked, via Argo, for this to be implemented directly. " + (item["brief"] or "")

    def _hand_back(note: str) -> None:
        core._set_state(conn, event_id, item["state"], now, expect_state=core.STATE_WORKING, note=note,
                        implement_job=item["implement_job"], validation_job=item["validation_job"],
                        pr_url=item["pr_url"], reverting_sha=item["reverting_sha"], revert_json=item["revert_json"])
        conn.commit()

    try:
        opened = _dispatch.open_episode(
            conn, repo=item["repo"], tier="implement", brief=brief, context=context,
            why="argo owner action: implement",
            origin=_dispatch.Origin(event_id=event_id), authorized_by="owner:argo",
        )
    except SubmitRefused as exc:
        work._end_on_refusal(conn, [item], exc, tier="implement", now=now, policy=core.load_policy(),
                             implement_job=None)
        return "failed", None, str(exc)
    except RemoteError as exc:
        if exc.maybe_mutated:
            work._hold_ambiguous_submit(item, exc)
            return "applied", {"note": "implement submit may have reached sideclaw, outcome ambiguous — "
                                       "left for reconcile_operations()"}, None
        _hand_back(f"deferred: {exc}")
        return "failed", None, str(exc)
    except (PolicyError, PreconditionError, UsageError) as exc:
        _hand_back(f"deferred: {exc}")
        return "failed", None, str(exc)

    conn.execute(
        "UPDATE triage_items SET implement_job=?, updated_at=? WHERE event_id=?",
        (opened.job_id, core._now_iso(now), event_id),
    )
    conn.commit()
    return "applied", {"jobId": opened.job_id}, None


def _apply_argo_merge(conn: sqlite3.Connection, item: sqlite3.Row, event_id: int,
                       now: dt.datetime) -> tuple[str, dict[str, Any] | None, str | None]:
    """The owner's one-click merge, accepted from `failed` and from `needs_decision` carrying a PR.
    Goes through _merge_and_rollout() with `authorized_by="owner:argo"` and the same gate (PR open,
    checks green on the head, review confirmed, GitHub allows), so an owner merge deploys and
    verifies like an automatic one. Outside `merging` there is no train SHA: the merge pins the head
    a review confirmed (`reviewed_sha`), since `dispatches.validation_status` only says that a
    review confirmed, not which head. No reviewed head on record, or a PR head that moved off it:
    nothing is merged and the item rejoins its repo's merge train, which reviews the current head
    and lands it."""
    if item["state"] not in _ARGO_MERGE_ALLOWED_STATES:
        return "rejected", None, f"item is in state {item['state']!r}, not needs_decision/failed"
    if item["revert_pr"] is not None:
        return "rejected", None, _ARGO_REVERTED_REFUSAL
    if not item["implement_job"] or not item["pr_url"]:
        return "rejected", None, "no pull request on this item to merge"

    def _rejoin_train(note: str) -> tuple[str, dict[str, Any] | None, str | None]:
        # The item joins its repo's merge train (advance_merge_trains()), which brings it up to date,
        # reviews it unless `reviewed_sha` is already its head, and lands it once the checks settle.
        # Parked here it would never be asked again.
        moved = core._set_state(conn, event_id, core.STATE_MERGING, now, expect_state=item["state"],
                                train_stage=train.TRAIN_UPDATE, train_sha=None, train_job=None, validation_job=None)
        conn.commit()
        if not moved:
            return "rejected", None, "item state changed before this action could be applied — retry from Argo"
        return "applied", {"merging": True, "note": note}, None

    reviewed = item["reviewed_sha"]
    if not reviewed:
        return _rejoin_train("no review of this PR's head on record — review the current head first; "
                             "the item rejoins the merge train")
    outcome = train._merge_and_rollout(conn, core.load_policy(), item, now, expected_sha=reviewed,
                                       authorized_by="owner:argo", why="owner approved via Argo")
    fresh = core._get_item(conn, event_id)
    if outcome == "merged":
        return "applied", {"merged": True, "state": fresh["state"] if fresh else None}, None
    if outcome == "pending":
        # The owner said merge and the checks are still running: a merge in progress, not a refusal.
        return _rejoin_train((fresh["note"] if fresh is not None else None) or "waiting for checks")
    if outcome == "ambiguous":
        return "applied", {"note": "merge may have reached GitHub, outcome ambiguous — left for "
                                   "reconcile_operations()"}, None
    if outcome == "head_moved":
        return _rejoin_train(f"the PR head is not the {reviewed[:12]} a review confirmed — review the current "
                             f"head first; the item rejoins the merge train")
    return "rejected", None, (fresh["note"] if fresh is not None else None) or "merge refused"


def _apply_argo_dismiss(conn: sqlite3.Connection, item: sqlite3.Row, event_id: int, now: dt.datetime,
                         payload: dict[str, Any]) -> tuple[str, dict[str, Any] | None, str | None]:
    if item["state"] not in _ARGO_DISMISS_ALLOWED_STATES:
        return "rejected", None, f"item is in state {item['state']!r}, dismiss not allowed"
    reason = str(payload.get("reason") or "").strip()
    if not reason:
        return "rejected", None, "dismiss requires a reason"
    rowcount = core._set_state(conn, event_id, core.STATE_CLOSED, now, expect_state=item["state"], note=reason,
                               close_reason=core.CLOSE_IGNORED)
    conn.commit()
    if rowcount == 0:
        return "rejected", None, "item state changed before this action could be applied — retry from Argo"
    return "applied", None, None


def _apply_argo_reinvestigate(conn: sqlite3.Connection, item: sqlite3.Row, event_id: int,
                               now: dt.datetime) -> tuple[str, dict[str, Any] | None, str | None]:
    """Back to `triaged`, whose pollers (escalate()/escalate_origin_items()) open the fresh
    investigation. The old investigation, review and PR handles are cleared so the new `working`
    phase starts clean, `dispatch_job` included: it would otherwise hold the cooldown anchor
    against the re-run just asked for."""
    if item["state"] not in _ARGO_REINVESTIGATE_ALLOWED_STATES:
        return "rejected", None, f"item is in state {item['state']!r}, reinvestigate not allowed"
    rowcount = core._set_state(conn, event_id, core.STATE_TRIAGED, now, expect_state=item["state"],
                               note="re-investigation requested by the owner via Argo",
                               dispatch_job=None, implement_job=None, validation_job=None)
    conn.commit()
    if rowcount == 0:
        return "rejected", None, "item state changed before this action could be applied — retry from Argo"
    return "applied", None, None


def _apply_argo_note(conn: sqlite3.Connection, item: sqlite3.Row, event_id: int, now: dt.datetime,
                      payload: dict[str, Any], action_id: str | int) -> tuple[str, dict[str, Any] | None, str | None]:
    if item["state"] in core.TERMINAL_STATES:
        return "rejected", None, "item is terminal, cannot attach a note"
    text = str(payload.get("text") or "").strip()
    if not text:
        return "rejected", None, "note requires text"
    # The action id rides in the stamp itself, not a separate dedup table: a redelivered action
    # (its ack failed last tick) finds its own tag already present and no-ops. Every other verb gets
    # that from its state compare-and-swap (see _apply_one_argo_action()); a note has no state to
    # compare-and-swap on.
    tag = f"[owner note via Argo #{action_id}, {now.strftime('%Y-%m-%d')}]"
    if item["note"] and tag in item["note"]:
        return "applied", None, None
    # The tag must survive the note cap (items.py NOTE_MAX, applied again by _set_state): appended after
    # a long note it would be cut off, the check above would miss it, and a redelivery would append twice.
    merged = _items.append_to_note(item["note"], f"{tag}: {text}")
    rowcount = core._set_state(conn, event_id, item["state"], now, expect_state=item["state"], note=merged)
    conn.commit()
    if rowcount == 0:
        return "rejected", None, "item state changed before this action could be applied — retry from Argo"
    return "applied", None, None


def _apply_one_argo_action(conn: sqlite3.Connection, action: dict[str, Any], now: dt.datetime) -> None:
    """Validate the shape of one action Argo handed back, dispatch it to the verb handler that
    owns its state gate, and ack the outcome. Never raises: every expected failure is folded into an
    acked `rejected`/`failed`; the caller's own try/except is a second line of defense.

    IDEMPOTENCY CONTRACT: every verb handler re-checks the item's CURRENT state before mutating.
    A second delivery of the same action (its ack failed, so Argo still shows it pending) finds
    the item already moved on and rejects with a state-mismatch reason instead of double-applying.
    No separate dedup table is needed."""
    action_id = action.get("id")
    event_id = action.get("event_id")
    verb = action.get("verb")
    payload = action.get("payload") or {}

    # `is None`/`== ""`, never bare truthiness: a falsy-but-real id (integer `0`) must still be
    # ackable, or an id-0 action is re-delivered forever with nothing resolving it.
    if action_id is None or action_id == "":
        print(f"triage: argo action with no id, nothing to ack against — dropped ({action!r})",
              file=sys.stderr)
        return

    if verb not in ARGO_ACTION_VERBS:
        status, result, error = "rejected", None, f"unknown verb: {verb!r}"
    else:
        item = core._get_item(conn, event_id)
        if item is None:
            status, result, error = "rejected", None, f"no triage_items row for event_id {event_id}"
        elif verb == "implement":
            status, result, error = _apply_argo_implement(conn, item, event_id, now)
        elif verb == "merge":
            status, result, error = _apply_argo_merge(conn, item, event_id, now)
        elif verb == "dismiss":
            status, result, error = _apply_argo_dismiss(conn, item, event_id, now, payload)
        elif verb == "reinvestigate":
            status, result, error = _apply_argo_reinvestigate(conn, item, event_id, now)
        else:
            status, result, error = _apply_argo_note(conn, item, event_id, now, payload, action_id)

    ack_status = _argo.ack_action(action_id, status=status, result=result, error=error)
    if ack_status != "ok":
        print(f"triage: argo ack for action {action_id} (outcome {status!r}) — {ack_status}",
              file=sys.stderr)


def apply_argo_actions(conn: sqlite3.Connection, now: dt.datetime, *, dry_run: bool) -> None:
    """Pulls the owner's queued actions off Argo (`implement`/`merge`/`dismiss`/`reinvestigate`/
    `note`, see ARGO_ACTION_VERBS) and applies each one, immediately before this pass's
    push_argo_snapshot() reflects the outcome. Idempotency: see _apply_one_argo_action()."""
    if dry_run:
        print("[dry-run] would poll Argo for pending owner actions")
        return

    status, actions = _argo.fetch_actions(os.environ.get("WARDEN_MACHINE", "mini"))
    if status != "ok":
        # "no-secret" is expected before the token is seeded, not an error; logged like push_argo_snapshot()'s own.
        print(f"triage: argo fetch-actions — {status}", file=sys.stderr)
        return

    for action in actions:
        try:
            _apply_one_argo_action(conn, action, now)
        except Exception as e:  # noqa: BLE001 — one bad action must never take down the tick
            print(f"triage: argo action {action.get('id')} raised: {e}", file=sys.stderr)


def build_argo_snapshot(conn: sqlite3.Connection, now: dt.datetime) -> dict[str, Any]:
    """The whole payload POSTed to Argo's `/warden/snapshot` after every pass. Every section reuses
    api.py's builders (health_payload/metrics_payload/board_payload/item_payload) rather than
    re-deriving them, so a second definition of "what /board counts" cannot drift. `items` carries
    full detail for only the first `ARGO_SNAPSHOT_ITEMS_CAP` board items (already ORDER BY
    updated_at DESC), keyed by event_id as a string (JSON object keys are strings)."""
    board = core._api.board_payload(conn)
    board_items = board["items"][:ARGO_SNAPSHOT_ITEMS_CAP]
    items = {
        str(item["event_id"]): core._api.item_payload(
            conn, item["event_id"], history_limit=ARGO_SNAPSHOT_HISTORY_LIMIT
        )
        for item in board_items
    }

    return {
        "machine": os.environ.get("WARDEN_MACHINE", "mini"),
        "generatedAt": core._now_iso(now),
        "health": core._api.health_payload(conn),
        "metrics": core._api.metrics_payload(conn),
        "board": board,
        "items": items,
        "itemsTruncated": len(board["items"]) > ARGO_SNAPSHOT_ITEMS_CAP,
    }


def push_argo_snapshot(conn: sqlite3.Connection, now: dt.datetime, *, dry_run: bool) -> str:
    """The last step of every pass. Builds the snapshot fresh, then either reports what it WOULD push
    (dry-run: no network) or pushes it, logging exactly one stderr line either way. Building AND
    encoding sit in one try (a non-serializable field fails as `"build-failed"`, logged, never
    raised, even under --dry-run, which still needs a real byte count); the push is wrapped too, so
    the tick does not depend on `clients.argo.push_snapshot()`'s never-raise contract."""
    try:
        snapshot = build_argo_snapshot(conn, now)
        body = json.dumps(snapshot).encode()
    except Exception as e:  # noqa: BLE001 — building/encoding the projection must never fail the tick
        print(f"triage: argo push — build failed: {e}", file=sys.stderr)
        return "build-failed"

    item_count = len(snapshot["items"])
    if dry_run:
        print(f"triage: argo push — dry-run, would push {len(body)} bytes ({item_count} items)",
              file=sys.stderr)
        return "dry-run"

    try:
        status = _argo.push_snapshot(snapshot)
    except Exception as e:  # noqa: BLE001 — defensive: the client contract is never-raise
        print(f"triage: argo push — client raised: {e}", file=sys.stderr)
        return "client-error"
    print(f"triage: argo push — {status} ({len(body)} bytes, {item_count} items)", file=sys.stderr)
    return status
