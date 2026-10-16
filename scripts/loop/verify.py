"""Deploy and verify, the fixed-by sweep after a fix merge, and the revert after a failed
verification. A merged item waits in `verifying`; maybe_verify() walks it once per loop tick."""

from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

from clients import github as _github, agent_gateway as _agent_gateway, jobs_grade as _jobs_grade
from clients.errors import PolicyError, PreconditionError, RemoteError, SubmitRefused, UsageError, WardenError
from lifecycle import dispatch as _dispatch, intake as _intake, policy as _policy, rollout as _rollout, shadow as _shadow
from loop import core, triaging, work


# A merged item waits in `verifying`, and maybe_verify() walks it:
#
#   1. DEPLOY (`verify_started_at` NULL): fast-forward the repo's checkout to origin/<default>
#      first, before its Makefile is read (lifecycle/rollout.py: a typed deferral when the checkout
#      is not clean, not on it, or ahead of it), then `make deploy` if the Makefile has the target,
#      not again when a deploy of the same merged commit was interrupted (it may have restarted
#      warden itself: _interrupted_deploy()). A deferral or a non-zero exit is an infrastructure
#      strike (backoff, third -> `failed` carrying the output tail); it never reverts, since it
#      does not prove the change is bad. Success, or no target, opens the window.
#   2. VERIFY: `make verify` when the repo has the target and, for an alert item, the item's own
#      signal quiet for VERIFY_WINDOW_HOURS: its event's occurrence mark unchanged since the window
#      opened (the same mark reopen_if_needed() reads, so an occurrence reaches both the same way),
#      a state-source event no longer open, and the item's own Kuma monitor UP since. An item with
#      no signal (issue, `warden run`) is `fixed` as soon as `make verify` passes. A host-verb
#      remediation (maybe_auto_remediate()) enters here with its window already open and its
#      verb's monitor in `deploy_expect_json`.
#   3. FAILURE: the signal recurred, or verification fails VERIFY_FAILURE_LIMIT passes in a row
#      -> _on_verify_failure(): the merged change is reverted (see the revert block below).
#
# A `make verify` that was ALREADY red before the merge is not the merge's doing. The merge train
# probes it once before it lands the pull request (take_baseline(), stored in `verify_baseline`);
# when that probe was red and `make verify` is red again here, the red is pre-existing: it is not a
# failing pass (no strike toward VERIFY_FAILURE_LIMIT, no revert), `verify_result` names it, and the
# item is judged on its own signal as before (or goes `fixed` at once, with the red named in the note,
# when it has none). A green or missing baseline keeps today's behaviour. The residual: while verify
# stays red, a second regression is not attributed to this merge — the red is its own alert.

VERIFIED_NOTE_PREFIX = "verified: "
VERIFY_FAILED_NOTE_PREFIX = "verification failed: "
VERIFY_RESULT_MAX = 300
BASELINE_TAIL_MAX = 300


def _repo_checkout(item: sqlite3.Row) -> Path | None:
    try:
        return _policy.repo_cwd(item["repo"] or "")
    except UsageError:
        return None


def _tail_for_note(text: str, limit: int = 120) -> str:
    collapsed = " ".join(text.split())
    return collapsed[-limit:] if collapsed else "(no output)"


def take_baseline(conn: sqlite3.Connection, event_id: int, cwd: Path, now: dt.datetime) -> None:
    """Probe `make verify` once, before the item's pull request merges, and store the result on the
    item (`verify_baseline`, JSON `{ok, exit, tail, at}`) — or clear it when the repo has no verify
    target. A timeout, a failure to start or any error is stored as a red probe with its tail: this
    never raises and never blocks or strikes the merge. A production probe: it describes production
    as it is before this change lands. Not a state transition, so a plain column write."""
    baseline: dict[str, Any] | None = None
    try:
        if _rollout.has_target(cwd, "verify"):
            res = _rollout.verify(cwd)
            baseline = {"ok": bool(res.ok), "exit": res.exit_code, "tail": _tail_for_note(res.tail, BASELINE_TAIL_MAX),
                        "at": core.now_iso(now)}
    except Exception as e:  # noqa: BLE001 — a probe must never decide the merge
        baseline = {"ok": False, "exit": -1, "tail": _tail_for_note(f"verify baseline error: {e}", BASELINE_TAIL_MAX),
                    "at": core.now_iso(now)}
    conn.execute("UPDATE triage_items SET verify_baseline=? WHERE event_id=?",
                 (json.dumps(baseline) if baseline is not None else None, event_id))
    conn.commit()


def _red_baseline(item: sqlite3.Row) -> dict[str, Any] | None:
    """The stored baseline when it says `make verify` was red before the merge, else None (green,
    not taken, or unreadable — all of which keep the plain behaviour)."""
    baseline = core.safe_json(item["verify_baseline"])
    return baseline if baseline.get("ok") is False else None


def _on_verify_failure(conn: sqlite3.Connection, item: sqlite3.Row, evidence: str, now: dt.datetime) -> None:
    """The one door every verification failure goes through. A merged change is reverted: the
    item goes back to `working` carrying the evidence, marked reverting the commit its merge
    landed as (`reverting_sha`, with the record in `revert_json`), and maybe_submit_reverts()
    opens the revert episode. An item with no merge on record (a host verb's restart) has
    nothing to revert and goes back to `triaged` with the evidence.

    Only a merge that landed as ONE commit is reverted mechanically: a squash, or a merge commit
    (`git revert -m 1`). A rebase merge's `merged_sha` is only the tip of the commits it replayed,
    and a merge whose commit or method is not on record names nothing safe to revert — the item
    is `failed` with the evidence and the reason, the change still deployed, for the owner."""
    event_id, sha, method = item["event_id"], item["merged_sha"], item["merge_method"]
    if method is None:
        core.set_state(conn, event_id, core.STATE_TRIAGED, now, expect_state=core.STATE_VERIFYING,
                        note=f"{VERIFY_FAILED_NOTE_PREFIX}{evidence}", **core.VERIFY_RESET)
        conn.commit()
        return
    if not sha or method not in REVERTIBLE_MERGE_METHODS:
        why = ("its merge commit is not on record" if not sha else
               f"{sha[:12]} landed as a {method} merge, not one commit")
        core.set_state(conn, event_id, core.STATE_FAILED, now, expect_state=core.STATE_VERIFYING,
                        expect_eq={"merged_sha": sha} if sha else None, verify_result=evidence[:VERIFY_RESULT_MAX],
                        failure_class=core.FAILURE_WORK,
                        note=f"{VERIFY_FAILED_NOTE_PREFIX}cannot auto-revert ({why}): {evidence}")
        conn.commit()
        return
    record = {"sha": sha, "pr": item["pr_url"], "title": _merged_title(conn, event_id, sha), "evidence": evidence,
              "method": method}
    core.set_state(conn, event_id, core.STATE_WORKING, now, expect_state=core.STATE_VERIFYING, expect_eq={"merged_sha": sha},
                    note=f"{VERIFY_FAILED_NOTE_PREFIX}{evidence} — reverting {sha[:12]}",
                    reverting_sha=sha, revert_json=json.dumps(record), implement_job=None, validation_job=None,
                    pr_url=None, reviewed_sha=None, blocked_sha=None, strikes=0, retry_at=None, **core.VERIFY_RESET)
    conn.commit()


def _start_verify(conn: sqlite3.Connection, item: sqlite3.Row, now: dt.datetime, *, note: str) -> None:
    """Open the verify window: stamp its start and, for an alert item, the event's occurrence
    mark every later occurrence is compared against."""
    mark = core.occurrence_mark(core.get_event(conn, item["event_id"])) if item["origin"] == "alert" else None
    core.set_state(conn, item["event_id"], core.STATE_VERIFYING, now, expect_state=core.STATE_VERIFYING, note=note,
                    verify_started_at=core.now_iso(now), verify_mark=mark, verify_failures=0, verify_result=None,
                    strikes=0, retry_at=None)
    conn.commit()


def _interrupted_deploy(conn: sqlite3.Connection, item: sqlite3.Row) -> bool:
    """A deploy of this item's merged commit already started and never reported back (open, or
    resolved `unknown` by reconcile_operations()): most likely it restarted warden's own process
    — a self-hosting repo — and running it again would restart it again, forever."""
    if not item["merged_sha"]:
        return False
    return conn.execute(
        "SELECT 1 FROM operations WHERE event_id=? AND kind='deploy' AND note=? "
        "AND (outcome IS NULL OR outcome='unknown') LIMIT 1",
        (item["event_id"], f"sha:{item['merged_sha']}")).fetchone() is not None


def _deploy_item(conn: sqlite3.Connection, item: sqlite3.Row, now: dt.datetime) -> sqlite3.Row | None:
    """The deploy stage. Returns the item with its verify window open, or None when it has to
    wait (a strike's backoff) or just struck.

    The checkout is fast-forwarded first, once, before anything reads its Makefile: a merge that
    adds the first `deploy` target is deployed, and `make verify` (every later pass) runs on the
    merged tree, not the one before it. A checkout that cannot be synced strikes."""
    event_id = item["event_id"]
    retry_at = core.parse_ts(item["retry_at"])
    if retry_at is not None and now < retry_at:
        return None
    cwd = _repo_checkout(item)
    if cwd is None:
        _start_verify(conn, item, now, note="no checkout to deploy; verifying")
        return core.get_item(conn, event_id)
    busy = _rollout.checkout_in_use(cwd)
    if busy is not None:
        core.park(conn, event_id, now, f"deploy waits: {busy}", state=core.STATE_VERIFYING)
        return None
    synced = _rollout.sync_checkout(cwd)
    if isinstance(synced, _rollout.Deferred):
        if synced.parked:
            core.park(conn, event_id, now, f"deploy waits: {synced.reason}", state=core.STATE_VERIFYING)
            return None
        core.strike(conn, event_id, now, f"deploy deferred: {synced.reason}",
                     retry_state=core.STATE_VERIFYING, expect_state=core.STATE_VERIFYING)
        conn.commit()
        return None
    if not _rollout.has_target(cwd, "deploy"):
        _start_verify(conn, item, now, note="no deploy target; verifying")
        return core.get_item(conn, event_id)
    if _interrupted_deploy(conn, item):
        _start_verify(conn, item, now, note=f"the deploy of {item['merged_sha'][:12]} was interrupted (it may "
                                            f"have restarted warden itself) — not run again; make verify judges")
        return core.get_item(conn, event_id)

    # Recorded before the deploy runs (DESIGN.md § Crash recovery), keyed to the merged commit:
    # a crash leaves it open, reconcile_operations() resolves it `unknown`, and the next pass
    # goes on to verify (_interrupted_deploy()) instead of deploying again.
    op_id = work.record_operation(conn, event_id=event_id, kind="deploy", repo=item["repo"],
                                   authorized_by="auto-verify",
                                   note=f"sha:{item['merged_sha']}" if item["merged_sha"] else None)
    result = _rollout.deploy(cwd, prev_sha=synced.before, head_sha=synced.head)
    receipt = json.dumps({"exitCode": result.exit_code, "output": result.tail})
    if not result.ok:
        work.complete_operation(conn, op_id, outcome="failed", receipt=receipt)
        # Not infra: the same merged commit's deploy would fail the same way.
        core.strike(conn, event_id, now, f"deploy failed (exit {result.exit_code}): {_tail_for_note(result.tail)}",
                     retry_state=core.STATE_VERIFYING, expect_state=core.STATE_VERIFYING,
                     failure_class=core.FAILURE_WORK)
        conn.commit()
        return None
    work.complete_operation(conn, op_id, outcome="done", receipt=receipt)
    _start_verify(conn, item, now, note="deployed via make deploy; verifying")
    return core.get_item(conn, event_id)


def _signal_not_quiet(item: sqlite3.Row, event: sqlite3.Row) -> list[str]:
    """Why the item's own signal is not quiet at the end of its window, empty when it is."""
    problems: list[str] = []
    if event["source"] not in core.GROUPED_TRIAGE_SOURCES and event["resolved_at"] is None:
        problems.append(f"{event['title']} is still firing")
    records = work.safe_json_list(item["deploy_expect_json"])
    title = (records[0].get("monitorTitle") if records else None) or work.kuma_monitor_title(event)
    if title:
        ran_ok, result = work.run_bounded(work.gather_kuma_push_fresh,
                                           [{"monitorTitle": title, "since": item["verify_started_at"]}],
                                           timeout=core.EVIDENCE_TIMEOUT)
        live_ok, detail = result if ran_ok else (False, f"monitor probe error: {result}")
        if not live_ok:
            problems.append(f"own monitor not UP: {detail}")
    return problems


def _job_closure(event: sqlite3.Row | None, started: dt.datetime, now: dt.datetime) -> tuple[str, str]:
    """Verified closure of a `job` item (ops-signals.md §6): its fix is `fixed` only when the job's own
    run record shows a success AFTER the deploy (`verify_started_at`, stamped once `make deploy`
    returned). Returns (`done` | `pending` | `expired`, evidence).

    The record is read through jobs-grade, the only grader, once per call. jobs-grade unreadable or the
    label absent from its output is `pending` — the job may simply not have run yet — until the window
    is over: twice the job's cadence plus JOB_VERIFY_MARGIN_MINUTES, never under JOB_VERIFY_MIN_MINUTES.
    An expired window is the caller's `work` strike; a revert would punish a change nobody proved wrong."""
    payload = core.safe_json(event["payload_json"]) if event is not None else {}
    label = str(payload.get("label") or "")
    rows = _jobs_grade.fetch(env={**os.environ, "PATH": _rollout.host_path()})
    row = next((r for r in rows or [] if r["label"] == label), None)
    every = (row or payload).get("every")
    cadence_minutes = 2 * float(every) / 60 if isinstance(every, (int, float)) else 0.0
    window = max(core.JOB_VERIFY_MIN_MINUTES, cadence_minutes + core.JOB_VERIFY_MARGIN_MINUTES)
    last_ok = core.parse_ts(row.get("last_ok")) if row else None
    # A success alone is not enough: the LATEST run must be a success too (consecutive_fail == 0), or a job
    # that ran once and failed again would be `fixed` while it is red.
    if last_ok is not None and last_ok > started and row.get("consecutive_fail") == 0:
        return "done", f"job {label} succeeded at {core.now_iso(last_ok)}, after the deploy at {core.now_iso(started)}"
    if rows is None:
        seen = "jobs-grade could not be read"
    elif row is None:
        seen = f"job {label} is not in the jobs-grade output"
    elif last_ok is not None and last_ok > started:
        seen = f"job {label} succeeded at {core.now_iso(last_ok)} but its latest run failed again ({row['grade']})"
    else:
        seen = f"job {label} is {row['grade']}, last success {row.get('last_ok') or 'never'}"
    if now - started >= dt.timedelta(minutes=window):
        return "expired", f"no success after the deploy at {core.now_iso(started)} within {window:g} min — {seen}"
    return "pending", f"waiting for a success of {label} after the deploy ({window:g} min window) — {seen}"


JOB_CLOSURE_EXPIRED_NOTE = f"{VERIFY_FAILED_NOTE_PREFIX}no success after the deploy"


def _on_job_closure_expired(conn: sqlite3.Connection, item: sqlite3.Row, evidence: str, now: dt.datetime) -> None:
    """A merged, deployed job fix that never produced a success: a `work` strike back to `triaged` with
    every handle of the failed attempt cleared, so the next investigation reads the merged change in the
    owner repo's `git log` and tries again.

    `strikes` alone cannot bound the attempts: it is given back each time the item advances to `merging`,
    which every retry does. The bound is the number of earlier expiries in the item's own transition
    history: the STRIKE_LIMIT-th ends it `failed` (class `work`, the change left deployed) for the owner."""
    event_id = item["event_id"]
    earlier = conn.execute("SELECT COUNT(*) FROM item_transitions WHERE event_id=? AND note LIKE ?",
                           (event_id, f"{JOB_CLOSURE_EXPIRED_NOTE}%")).fetchone()[0]
    if earlier + 1 >= core.STRIKE_LIMIT:
        core.set_state(conn, event_id, core.STATE_FAILED, now, expect_state=core.STATE_VERIFYING,
                       failure_class=core.FAILURE_WORK, verify_result=evidence[:VERIFY_RESULT_MAX],
                       note=f"{VERIFY_FAILED_NOTE_PREFIX}{evidence} — {earlier + 1} deployed fixes in a row did not make the "
                            f"job succeed; the last change is still deployed")
        conn.commit()
        return
    core.strike(conn, event_id, now, f"{VERIFY_FAILED_NOTE_PREFIX}{evidence}", retry_state=core.STATE_TRIAGED,
                failure_class=core.FAILURE_WORK, expect_state=core.STATE_VERIFYING,
                dispatch_job=None, implement_job=None, validation_job=None, pr_url=None, reviewed_sha=None,
                blocked_sha=None, revision_count=0, stalled_rounds=0, **core.VERIFY_RESET)
    conn.commit()


def _verify_item(conn: sqlite3.Connection, policy: dict[str, Any], item: sqlite3.Row, now: dt.datetime) -> None:
    if item["fixed_by_pr"]:
        _verify_swept(conn, item, now)
        return
    if core.is_revert(item):
        _verify_revert(conn, item, now)
        return
    event_id = item["event_id"]
    event = core.get_event(conn, event_id)
    signal = item["origin"] == "alert" and event is not None
    started = core.parse_ts(item["verify_started_at"]) or now
    window_over = now - started >= dt.timedelta(hours=core.VERIFY_WINDOW_HOURS)

    if signal and item["verify_mark"] is not None and core.occurrence_mark(event) != item["verify_mark"]:
        _on_verify_failure(conn, item, f"own signal recurred while verifying: {event['title']}", now)
        return

    problems: list[str] = []
    verify_note = "no make verify target"
    cwd = _repo_checkout(item)
    if cwd is not None and _rollout.has_target(cwd, "verify"):
        res = _rollout.verify(cwd)
        before = _red_baseline(item)
        if res.ok:
            verify_note = "make verify passed"
        elif before is not None:
            verify_note = (f"make verify red before the merge too (pre-existing, exit {before.get('exit')}): "
                           f"{_tail_for_note(before.get('tail') or '')}")
        else:
            problems.append(f"make verify failed (exit {res.exit_code}): {_tail_for_note(res.tail)}")
    if signal and window_over:
        problems += _signal_not_quiet(item, event)

    if problems:
        failures = item["verify_failures"] + 1
        evidence = "; ".join(problems)
        if failures >= core.VERIFY_FAILURE_LIMIT:
            _on_verify_failure(conn, item, f"{failures} consecutive failing passes — {evidence}", now)
            return
        core.set_state(conn, event_id, core.STATE_VERIFYING, now, expect_state=core.STATE_VERIFYING,
                        verify_failures=failures, verify_result=evidence[:VERIFY_RESULT_MAX])
        conn.commit()
        return
    if item["origin"] == core.JOB_ORIGIN:
        outcome, evidence = _job_closure(event, started, now)
        if outcome == "expired":
            _on_job_closure_expired(conn, item, evidence, now)
            return
        if outcome == "pending":
            core.set_state(conn, event_id, core.STATE_VERIFYING, now, expect_state=core.STATE_VERIFYING,
                            verify_failures=0, verify_result=f"{verify_note}; {evidence}"[:VERIFY_RESULT_MAX])
            conn.commit()
            return
        verify_note = f"{verify_note}; {evidence}"
    if signal and not window_over:
        core.set_state(conn, event_id, core.STATE_VERIFYING, now, expect_state=core.STATE_VERIFYING, verify_failures=0,
                        verify_result=f"{verify_note}; signal window open ({core.VERIFY_WINDOW_HOURS:g}h)")
        conn.commit()
        return

    note = f"{VERIFIED_NOTE_PREFIX}{verify_note}" + (f"; own signal quiet {core.VERIFY_WINDOW_HOURS:g}h" if signal else "")
    moved = core.set_state(conn, event_id, core.STATE_FIXED, now, expect_state=core.STATE_VERIFYING, note=note,
                            verify_failures=0, verify_result=note)
    conn.commit()
    if moved:
        work.notify_item(conn, policy, event_id)


def maybe_verify(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime, *, dry_run: bool) -> None:
    """Step 10, see the block comment above. Dry-run prints what each item would run and shells out
    to nothing.

    Paused (`warden pause`): an item not yet deployed waits for its `make deploy` (a production
    action); one already deployed is still verified, which is observation, and its revert, should
    verification fail, is held by maybe_submit_reverts()."""
    for item in conn.execute("SELECT * FROM triage_items WHERE state=? ORDER BY event_id",
                             (core.STATE_VERIFYING,)).fetchall():
        # Asked per item, not once per pass: a pause set while one deploy runs holds the next.
        if item["verify_started_at"] is None and core.is_paused():
            continue
        if dry_run:
            what = (f"signal-only verification (swept by {item['fixed_by_pr']})" if item["fixed_by_pr"]
                    else "make deploy, then make verify" if item["verify_started_at"] is None else "make verify")
            print(f"[dry-run] would run {what} for {item['signature']} ({item['repo']})")
            continue
        if item["verify_started_at"] is None:
            item = _deploy_item(conn, item, now)
            if item is None:
                continue
        _verify_item(conn, policy, item, now)
    if not dry_run:
        # A verification that just failed opens its revert now, not a sweep later.
        maybe_submit_reverts(conn, policy, now, dry_run=False)


# A merged fix may also fix other items waiting in its repo. When a fix's merge lands (a revert's
# never does: is_revert()) merged_entry() queues the sweep on the merged item (`sweep_pr`), and
# advance_fixed_by_sweeps(), in the implement chain so both crons run it, turns that into one
# agent-gateway `triage` job: the merged PR's title, body and diff against the repo's `triaged` items
# and its `working` items with no episode in flight, answering `{matches: [{item, reason}]}`. It
# follows the intake triage pattern: a claim sentinel, then the job id, then a compare-and-set
# fold. A match is validated against the ledger (an id the prompt showed, still in the state it was
# shown in, still no episode in flight); a valid one enters `verifying` on signal alone
# (`fixed_by_pr`, _verify_swept()): quiet for the window -> `closed(fixed_by)`, recurrence -> back
# to `triaged`. The sweep never moves the merged item: a submit failure or a bad answer is a
# stderr line and another attempt, and after SWEEP_ATTEMPT_LIMIT the sweep is dropped. A `-private`
# repo is never swept: nothing in it may reach the model. Dry-run prints, never submits.

SWEEP_ATTEMPT_LIMIT = 3
# `triaged`, or `working` with no implement/review episode, no revert in progress and no
# investigation still running: the items a fix may pre-empt. Also re-checked at fold time.
_SWEEPABLE_SQL = (
    "(state='triaged' OR (state='working' AND implement_job IS NULL AND validation_job IS NULL "
    "AND reverting_sha IS NULL AND NOT (dispatch_job IS NOT NULL AND NOT EXISTS "
    "(SELECT 1 FROM dispatches d WHERE d.job_id = triage_items.dispatch_job AND d.finished_at IS NOT NULL))))"
)
_SWEEP_COLUMNS = ("sweep_pr", "sweep_job", "sweep_job_at", "sweep_attempts", "sweep_candidates")
_SWEEP_DONE: dict[str, Any] = {"sweep_pr": None, "sweep_job": None, "sweep_job_at": None,
                               "sweep_attempts": 0, "sweep_candidates": None}
SWEPT_BACK_NOTE_PREFIX = "fixed-by sweep did not hold: "


def _sweep_cas(conn: sqlite3.Connection, event_id: int, expect_job: str | None, **columns: Any) -> bool:
    """Write the sweep's bookkeeping on the merged item, compare-and-set on `sweep_job` being
    `expect_job` (NULL included) while a sweep is still queued. Not a state transition: the merged
    item's own state is never the sweep's to touch."""
    unknown = tuple(c for c in columns if c not in _SWEEP_COLUMNS)
    if unknown:
        raise ValueError(f"{unknown} are not sweep columns")
    sql = f"UPDATE triage_items SET {', '.join(f'{c}=?' for c in columns)} WHERE event_id=? AND sweep_pr IS NOT NULL"
    params: list[Any] = [*columns.values(), event_id]
    if expect_job is None:
        sql += " AND sweep_job IS NULL"
    else:
        sql += " AND sweep_job=?"
        params.append(expect_job)
    won = conn.execute(sql, params).rowcount > 0
    conn.commit()
    return won


def _sweep_drop(conn: sqlite3.Connection, row: sqlite3.Row, expect_job: str | None, why: str) -> bool:
    """End a sweep without running it (or after its last failure): quiet, a stderr line."""
    won = _sweep_cas(conn, row["event_id"], expect_job, **_SWEEP_DONE)
    if won:
        print(f"triage: fixed-by sweep of {row['sweep_pr']} dropped: {why}", file=sys.stderr)
    return won


def _sweep_failed(conn: sqlite3.Connection, row: sqlite3.Row, expect_job: str | None, why: str) -> None:
    """One failed attempt: another next pass, until SWEEP_ATTEMPT_LIMIT, then the sweep is dropped."""
    attempts = row["sweep_attempts"] + 1
    if attempts >= SWEEP_ATTEMPT_LIMIT:
        _sweep_drop(conn, row, expect_job, f"{why} (attempt {attempts} of {SWEEP_ATTEMPT_LIMIT})")
        return
    if _sweep_cas(conn, row["event_id"], expect_job, sweep_job=None, sweep_job_at=None, sweep_attempts=attempts):
        print(f"triage: fixed-by sweep of {row['sweep_pr']} failed, will retry: {why} "
              f"(attempt {attempts} of {SWEEP_ATTEMPT_LIMIT})", file=sys.stderr)


def _sweep_candidates(conn: sqlite3.Connection, row: sqlite3.Row) -> list[dict[str, Any]]:
    """The items of the merged item's repo a fix may pre-empt (_SWEEPABLE_SQL), newest first. Never
    the merged item itself and never the owner's own request (`human`): nothing owner-asked closes
    on a model's say-so."""
    out = []
    for r in conn.execute(
            f"SELECT event_id, state, root_cause, note FROM triage_items WHERE repo=? AND event_id != ? "
            f"AND origin != 'human' AND {_SWEEPABLE_SQL} ORDER BY created_at DESC, event_id DESC LIMIT ?",
            (row["repo"], row["event_id"], _intake.MAX_OPEN_ITEMS)):
        event = core.get_event(conn, r["event_id"])
        out.append({"id": r["event_id"], "state": r["state"], "title": event["title"] if event else "",
                    "root_cause": r["root_cause"], "note": r["note"]})
    return out


def _submit_sweep(conn: sqlite3.Connection, row: sqlite3.Row, now: dt.datetime) -> dict[str, Any] | None:
    """Claim one queued sweep, submit its job, record the job id. Returns the job, or None when
    nothing is in flight afterwards (dropped, the claim lost, or a failed attempt)."""
    event_id, pr_url = row["event_id"], row["sweep_pr"]
    if not row["repo"] or _intake.is_private(row["repo"]):
        _sweep_drop(conn, row, None, "private or unknown repo, nothing safe to show the model")
        return None
    ref = _github.parse_pr_url(pr_url)
    if ref is None:
        _sweep_drop(conn, row, None, "not a pull request URL")
        return None
    candidates = _sweep_candidates(conn, row)
    if not candidates:
        _sweep_drop(conn, row, None, "no item waiting in the repo")
        return None
    claim = f"{triaging.TRIAGE_CLAIM_PREFIX}{core.now_iso(now)}"
    if not _sweep_cas(conn, event_id, None, sweep_job=claim, sweep_job_at=core.now_iso(now),
                      sweep_candidates=json.dumps({str(c["id"]): c["state"] for c in candidates})):
        return None
    row = core.get_item(conn, event_id)
    try:
        pr = _github.read_pr(*ref)
    except WardenError as e:
        _sweep_failed(conn, row, claim, f"could not read {pr_url}: {e}")
        return None
    diff = _pr_diff(pr_url, _intake.MAX_SWEEP_DIFF_CHARS)
    if diff is None:
        _sweep_failed(conn, row, claim, f"no diff of {pr_url}")
        return None
    prompt = _intake.build_sweep_prompt(repo=row["repo"], pr_title=str(pr.get("title") or ""),
                                        pr_body=str(pr.get("body") or ""), diff=diff, candidates=candidates)
    try:
        job = _agent_gateway.submit_triage(prompt=prompt, schema=_intake.SWEEP_SCHEMA)
    except WardenError as e:
        _sweep_failed(conn, row, claim, f"submit failed: {e}")
        return None
    if not _sweep_cas(conn, event_id, claim, sweep_job=job["id"], sweep_job_at=core.now_iso(now)):
        print(f"triage: fixed-by sweep job {job['id']} for {pr_url} lost its claim (released as stale "
              f"while submitting) — its answer is dropped", file=sys.stderr)
        return None
    try:
        _shadow.submit(conn, _shadow.KIND_SWEEP, event_id, job["id"],
                       _shadow.build_sweep(repo=row["repo"], pr_title=str(pr.get("title") or ""),
                                           pr_body=str(pr.get("body") or ""), diff=diff, candidates=candidates), now)
    except Exception as e:  # noqa: BLE001 — the shadow lane must never touch the real one
        print(f"shadow: sweep context for {pr_url} failed: {e}", file=sys.stderr)
    return job


def _fold_sweep_job(conn: sqlite3.Connection, event_id: int, job_id: str, job: dict[str, Any],
                    now: dt.datetime) -> str | None:
    """Settle one finished sweep job. The queued sweep is cleared first, compare-and-set on the job
    id, so a second pass finding the same finished job does nothing; then every match is validated
    and applied on its own (see the block comment). Returns what happened in a few words, or None
    when another pass already folded it or the job failed (a failed attempt)."""
    row = core.get_item(conn, event_id)
    if row is None or row["sweep_job"] != job_id:
        return None
    result = job.get("result")
    answer = result.get("result") if job.get("status") == "done" and isinstance(result, dict) else None
    matches = answer.get("matches") if isinstance(answer, dict) else None
    if not isinstance(matches, list):
        _sweep_failed(conn, row, job_id, f"job {job_id} {job.get('status')}: "
                      f"{job.get('error') or 'no usable answer'}")
        return None
    pr_url = row["sweep_pr"]
    _shadow.record_llm(conn, _shadow.KIND_SWEEP, event_id, job_id,
                       [m["item"] for m in matches if isinstance(m, dict) and isinstance(m.get("item"), int)])
    shown = core.safe_json(row["sweep_candidates"])
    if not _sweep_cas(conn, event_id, job_id, **_SWEEP_DONE):
        return None
    swept: list[int] = []
    for match in matches:
        target_id = match.get("item") if isinstance(match, dict) else None
        if not isinstance(target_id, int) or isinstance(target_id, bool) or target_id in swept:
            continue
        shown_state = shown.get(str(target_id))
        target = core.get_item(conn, target_id)
        if shown_state is None or target is None or target["repo"] != row["repo"] or target_id == event_id:
            continue
        if not conn.execute(f"SELECT 1 FROM triage_items WHERE event_id=? AND state=? AND {_SWEEPABLE_SQL}",
                            (target_id, shown_state)).fetchone():
            continue
        event = core.get_event(conn, target_id)
        reason = " ".join(str(match.get("reason") or "").split())[:200] or "no reason given"
        mark = core.occurrence_mark(event) if target["origin"] == "alert" and event is not None else None
        moved = core.set_state(
            conn, target_id, core.STATE_VERIFYING, now, expect_state=shown_state,
            expect_null=("implement_job", "validation_job"), note=f"fixed by {pr_url}: {reason}",
            **{**core.VERIFY_RESET, "verify_started_at": core.now_iso(now), "verify_mark": mark, "fixed_by_pr": pr_url})
        conn.commit()
        if moved:
            swept.append(target_id)
    return f"swept {swept} by {pr_url}" if swept else "swept nothing"


def _verify_swept(conn: sqlite3.Connection, item: sqlite3.Row, now: dt.datetime) -> None:
    """Verification of an item a fixed-by sweep put in `verifying`: its own signal only. No
    deploy and no `make verify` — the merging item already ran both. The signal recurring, or
    not quiet at the end of the window VERIFY_FAILURE_LIMIT passes in a row, sends it back to
    `triaged` with the evidence (never `_on_verify_failure()`: it was not this item's merge, so
    there is nothing to revert); the window over and the signal quiet closes it
    `closed(fixed_by)`, with no Slack line. An item with no signal (an issue) closes at the end of
    the window; an owner's issue is told, like any other issue verdict."""
    event_id, pr_url = item["event_id"], item["fixed_by_pr"]
    event = core.get_event(conn, event_id)
    signal = item["origin"] == "alert" and event is not None

    def back(evidence: str) -> None:
        core.set_state(conn, event_id, core.STATE_TRIAGED, now, expect_state=core.STATE_VERIFYING,
                        expect_eq={"fixed_by_pr": pr_url}, note=f"{SWEPT_BACK_NOTE_PREFIX}{evidence}", **core.VERIFY_RESET)
        conn.commit()

    if signal and item["verify_mark"] is not None and core.occurrence_mark(event) != item["verify_mark"]:
        back(f"own signal recurred after {pr_url}: {event['title']}")
        return
    started = core.parse_ts(item["verify_started_at"]) or now
    if now - started < dt.timedelta(hours=core.VERIFY_WINDOW_HOURS):
        return
    problems = _signal_not_quiet(item, event) if signal else []
    if problems:
        failures = item["verify_failures"] + 1
        evidence = "; ".join(problems)
        if failures >= core.VERIFY_FAILURE_LIMIT:
            back(f"{failures} consecutive failing passes after {pr_url} — {evidence}")
            return
        core.set_state(conn, event_id, core.STATE_VERIFYING, now, expect_state=core.STATE_VERIFYING,
                        verify_failures=failures, verify_result=evidence[:VERIFY_RESULT_MAX])
        conn.commit()
        return
    note = item["note"] or f"fixed by {pr_url}"
    won = core.set_state(conn, event_id, core.STATE_CLOSED, now, expect_state=core.STATE_VERIFYING,
                          expect_eq={"fixed_by_pr": pr_url}, close_reason=core.CLOSE_FIXED_BY, note=note,
                          verify_failures=0, verify_result=f"quiet {core.VERIFY_WINDOW_HOURS:g}h")
    conn.commit()
    if won and event is not None:
        work.maybe_comment_back_on_issue(conn, item, event, {}, now, state="fixed",
                                          note=note.removeprefix("fixed by "), dry_run=False)


def advance_fixed_by_sweeps(conn: sqlite3.Connection, now: dt.datetime, *, dry_run: bool) -> None:
    """Move every queued sweep (`sweep_pr` set on a merged item) one step: submit its job, release a
    stale claim (TRIAGE_CLAIM_STALE_MINUTES), fold a finished job, and cancel a job still not
    terminal TRIAGE_JOB_STALE_MINUTES after its submit. Called from the implement chain, so the loop
    and the sweep cron both run it; every write is a compare-and-set. A failure never touches the
    merged item (see the block comment). Dry-run prints what it would submit and nothing else."""
    for row in conn.execute("SELECT * FROM triage_items WHERE sweep_pr IS NOT NULL ORDER BY event_id").fetchall():
        job_id = row["sweep_job"]
        if dry_run:
            if job_id is None:
                private = not row["repo"] or _intake.is_private(row["repo"])
                print(f"[dry-run] would {'skip the private' if private else 'submit a'} fixed-by sweep "
                      f"for {row['sweep_pr']} ({len(_sweep_candidates(conn, row))} candidate item(s))")
            continue
        if job_id is None:
            job = _submit_sweep(conn, row, now)
            if job is not None and job.get("status") in _agent_gateway.TERMINAL:
                _fold_sweep_job(conn, row["event_id"], job["id"], job, now)
            continue
        if triaging.is_triage_claim(job_id):
            claimed_at = core.parse_ts(job_id[len(triaging.TRIAGE_CLAIM_PREFIX):])
            if claimed_at is None or claimed_at < now - dt.timedelta(minutes=triaging.TRIAGE_CLAIM_STALE_MINUTES):
                _sweep_failed(conn, row, job_id, "a stale claim (the submitter died)")
            continue
        try:
            job = _agent_gateway.get(job_id)
        except RemoteError as e:
            print(f"triage: could not poll fixed-by sweep job {job_id}: {e}", file=sys.stderr)
            continue
        if job is None:
            _sweep_failed(conn, row, job_id, f"agent-gateway has no record of job {job_id}")
            continue
        if job.get("status") in _agent_gateway.TERMINAL:
            _fold_sweep_job(conn, row["event_id"], job_id, job, now)
            continue
        submitted_at = core.parse_ts(row["sweep_job_at"])
        if submitted_at is None or now - submitted_at < dt.timedelta(minutes=triaging.TRIAGE_JOB_STALE_MINUTES):
            continue
        try:
            _agent_gateway.cancel(job_id)
        except WardenError as e:
            print(f"triage: could not cancel stuck fixed-by sweep job {job_id}: {e}", file=sys.stderr)
        _sweep_failed(conn, row, job_id, f"job {job_id} still {job.get('status')} after "
                      f"{triaging.TRIAGE_JOB_STALE_MINUTES} min — cancelled")


#   1. _on_verify_failure(): `verifying` -> `working`, `reverting_sha` = the merged commit,
#      `revert_json` = {sha, pr, title, evidence}, the PR and jobs of the failed fix cleared.
#   2. maybe_submit_reverts(): an implement episode whose brief is `git revert --no-edit <sha>`
#      and nothing else. It goes through the implement path unchanged: the lease, a refusal
#      (`failed`), a strike, an ambiguous submit. Not an attempt: `revision_count` stays.
#   3. Its PR is a `dispatch/*` PR: poll_implement_jobs() hands it to the merge train like any
#      other, the review is told it is a mechanical revert (revert_review_context()), and a
#      block, failing checks or a conflict end it `failed` with the PR left open, never a
#      revision (revisable()). A revert merge keeps `reverting_sha`: that is is_revert().
#   4. Deploy as usual, then _verify_revert(): `make verify` only, no signal window. Passing
#      clears `reverting_sha`, counts the next attempt (the failed fix was one) and sends the
#      item to `working`, where maybe_submit_reverts() opens a fresh attempt from the latest
#      base with the evidence and the reverted change as its context; attempts spent ->
#      `failed`. Failing VERIFY_FAILURE_LIMIT passes in a row -> `failed`, production
#      unhealthy after the revert.
#
# `revert_pr` is not written: it is the owner's own `warden revert` record, and Argo refuses to
# implement an item carrying it. The automatic path ends in exactly that fresh attempt.

REVERT_WHY = "triage revert: the merged change failed verification"
# How a merge must have landed for `git revert` of its one commit to undo it (see _on_verify_failure()).
REVERTIBLE_MERGE_METHODS = ("squash", "merge")
AFTER_REVERT_WHY_PREFIX = "triage attempt after revert "
REVERT_EVIDENCE_MAX = 1500
REVERTED_DIFF_MAX = 8000


def _merged_title(conn: sqlite3.Connection, event_id: int, sha: str) -> str | None:
    """The title of the pull request that merged as `sha`, from its merge operation's receipt."""
    for op in conn.execute("SELECT receipt_json FROM operations WHERE event_id=? AND kind='merge' "
                           "AND outcome='done' ORDER BY rowid DESC", (event_id,)):
        receipt = core.safe_json(op["receipt_json"])
        if receipt.get("mergeCommit") == sha:
            return receipt.get("title")
    return None


def _revert_record(item: sqlite3.Row) -> dict[str, Any]:
    record = core.safe_json(item["revert_json"])
    record["evidence"] = str(record.get("evidence") or "no evidence on record")[:REVERT_EVIDENCE_MAX]
    return record


def _revert_command(record: dict[str, Any]) -> str:
    """`git revert` of the merged commit: `-m 1` (keep the default branch's side) for a merge commit."""
    mainline = "-m 1 " if record.get("method") == "merge" else ""
    return f"git revert --no-edit {mainline}{record.get('sha')}"


def _revert_brief(record: dict[str, Any]) -> str:
    sha, pr = record.get("sha"), record.get("pr") or "a pull request"
    title = (f'titled exactly `Revert "{record["title"]}"`' if record.get("title")
             else "titled with the revert commit's own subject line")
    kind = "the merge commit" if record.get("method") == "merge" else "the squashed commit"
    return (
        f"Mechanical revert. {pr} was merged into the default branch as {kind} {sha}, and the change "
        f"failed verification in production afterwards:\n{record['evidence']}\n\n"
        f"Do exactly this and nothing else: on the latest default branch run `{_revert_command(record)}` "
        f"and open a pull request {title} carrying that one revert commit. No other edit, no fix, no "
        f"formatting, no follow-up — the real fix is a separate attempt once this revert has landed. If the revert does not "
        f"apply cleanly, do not resolve the conflict: say so in your verdict and stop."
    )


def revert_review_context(item: sqlite3.Row) -> str:
    record = _revert_record(item)
    sha = record.get("sha") or item["reverting_sha"]
    title = f" ({record['title']})" if record.get("title") else ""
    return (
        f"This pull request is a MECHANICAL REVERT of commit {sha} — {record.get('pr') or 'a merged pull request'}"
        f"{title} — opened by warden after that change failed verification in production:\n{record['evidence']}"
        f"\n\nYou are the merge gate. Confirm the diff is exactly the inverse of {sha} "
        f"(`{_revert_command({**record, 'sha': sha})}`) and nothing else: any other change is a blocking "
        f"finding. Block also if reverting would itself break a running service or lose data (e.g. it undoes a migration that already ran). Do not "
        f"judge whether the reverted change was right — its fix is a separate attempt after this revert."
    )[: _dispatch.MAX_CONTEXT_CHARS]


def _reverted_diff(pr_url: str | None) -> str | None:
    """The reverted pull request's diff, capped — or None when it cannot be read (the episode is
    then told to read it with `git show`)."""
    return _pr_diff(pr_url, REVERTED_DIFF_MAX)


def _pr_diff(pr_url: str | None, limit: int) -> str | None:
    """A pull request's diff, file by file from GitHub, capped at `limit` chars — or None when it
    cannot be read."""
    ref = _github.parse_pr_url(pr_url or "")
    if ref is None:
        return None
    try:
        files = _github.pr_files(*ref)
    except RemoteError as e:
        print(f"triage: could not read the files of {pr_url}: {e}", file=sys.stderr)
        return None
    parts = [f"--- {f.get('filename') or '?'}\n{f.get('patch') or '(no textual diff)'}"
             for f in files if isinstance(f, dict)]
    return "\n".join(parts)[:limit] if parts else None


def _after_revert_episode(conn: sqlite3.Connection, item: sqlite3.Row, record: dict[str, Any],
                          attempt: int) -> tuple[str, str]:
    """The brief and context of the fresh attempt that follows a landed revert."""
    sha, pr = record.get("sha"), record.get("pr") or "(not on record)"
    brief = (
        f"Attempt {attempt} of {core.MAX_IMPLEMENT_ATTEMPTS} of a fix the alert triage loop already shipped once: "
        f"{pr} was merged as {sha}, failed verification in production and has been reverted. Your worktree "
        f"starts from the latest default branch, without that change. The context below has the "
        f"verification failure and the reverted change: work out why it did not hold, then implement a fix "
        f"that does — do not re-apply the reverted change unchanged. If the evidence shows the problem "
        f"cannot be fixed from inside this repo, say so and stop.\n\n"
        f"{work.issue_closing_instruction(conn, item)}"
    )
    title = f" ({record['title']})" if record.get("title") else ""
    diff = _reverted_diff(record.get("pr"))
    parts = [f"Verification failure after {sha} was merged:\n{record['evidence']}",
             f"Reverted pull request: {pr}{title}. Read the reverted change with `git show {sha}`."]
    if diff:
        parts.append(f"The reverted change, as GitHub shows it (capped):\n{diff}")
    if item["dispatch_job"]:
        inv = conn.execute("SELECT verdict_json FROM dispatches WHERE job_id=?", (item["dispatch_job"],)).fetchone()
        if inv is not None:
            parts.append(work.verdict_as_context(item["dispatch_job"], core.safe_json(inv["verdict_json"])))
    elif item["brief"]:
        parts.append(f"The owner's brief: {item['brief']}")
    return brief, "\n\n".join(parts)[: _dispatch.MAX_CONTEXT_CHARS]


def maybe_submit_reverts(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                         *, dry_run: bool) -> None:
    """Open the episode a `working` item with a revert record (`revert_json`) and no implement job
    waits for: the revert itself while `reverting_sha` is set, else the fresh attempt after it
    (see the block comment above). Claim before dispatch, a compare-and-set on `implement_job`
    and `reverting_sha`, the same shape as maybe_auto_implement(), with the same failure
    handling: a refusal ends the item, an infrastructure failure strikes, an ambiguous submit
    stays claimed for reconcile_operations(), a busy repo defers. A lease refusal, a struck
    episode or a reconciled one hands `implement_job` back to NULL, and this submits again.

    "Busy" for the revert itself is a running episode only (an implement, or a train's
    `update_pr`): a PR merely waiting on its train never holds it back — the revert would starve
    behind a PR waiting on CI — and its own PR then goes first on the train (advance_merge_trains()).

    Paused (`warden pause`): the revert waits, its claim untaken."""
    if core.is_paused():
        return
    ready_sql, ready_params = core.retry_ready_sql(now)
    candidates = conn.execute(
        f"SELECT * FROM triage_items WHERE state=? AND implement_job IS NULL AND revert_json IS NOT NULL "
        f"AND {ready_sql} ORDER BY event_id", (core.STATE_WORKING, *ready_params)).fetchall()
    for item in candidates:
        if item["repo"] is None:
            continue
        event_id, record = item["event_id"], _revert_record(item)
        if core.is_revert(item):
            what, model, why = f"the revert of {item['reverting_sha'][:12]}", None, REVERT_WHY
            brief, context = _revert_brief(record), None
        else:
            attempt = item["revision_count"] + 1
            what, model = f"attempt {attempt}/{core.MAX_IMPLEMENT_ATTEMPTS} after a revert", work.implement_model(attempt)
            why = f"{AFTER_REVERT_WHY_PREFIX}{attempt}: the reverted change failed verification"
            brief = context = None
        if dry_run:
            print(f"[dry-run] would submit {what} for {item['signature']} in {item['repo']}")
            continue
        claimed = core.set_state(conn, event_id, core.STATE_WORKING, now, expect_state=core.STATE_WORKING,
                                  expect_null=("implement_job",), expect_eq={"reverting_sha": item["reverting_sha"]},
                                  implement_job=core.IMPLEMENT_CLAIM)
        conn.commit()
        if not claimed:
            continue
        if brief is None:
            brief, context = _after_revert_episode(conn, item, record, item["revision_count"] + 1)
        try:
            opened = work.open_implement_episode(
                conn, repo=item["repo"], tier="implement", brief=brief, context=context, why=why,
                origin=_dispatch.Origin(event_id=event_id), authorized_by="auto-from-item", model=model,
                count_merging=not core.is_revert(item))
        except SubmitRefused as exc:
            work.end_on_refusal(conn, [item], exc, tier="implement", now=now, policy=policy, implement_job=None)
            continue
        except RemoteError as exc:
            if exc.maybe_mutated:
                work.hold_ambiguous_submit(item, exc)
                continue
            core.strike(conn, event_id, now, f"{what} could not be submitted: {exc}", retry_state=core.STATE_WORKING,
                         expect_state=core.STATE_WORKING, implement_job=None)
            conn.commit()
            continue
        except (PolicyError, PreconditionError, UsageError) as e:
            core.set_state(conn, event_id, core.STATE_WORKING, now, expect_state=core.STATE_WORKING, implement_job=None,
                            note=f"deferred: {e}")
            conn.commit()
            continue
        conn.execute("UPDATE triage_items SET implement_job=?, updated_at=? WHERE event_id=?",
                     (opened.job_id, core.now_iso(now), event_id))
        conn.commit()


def _verify_revert(conn: sqlite3.Connection, item: sqlite3.Row, now: dt.datetime) -> None:
    """Verify a landed revert: `make verify` only (no signal window — the signal is expected back
    until the real fix lands). See the block comment above for where each outcome goes."""
    event_id, sha = item["event_id"], item["reverting_sha"]
    verify_note = "no make verify target"
    cwd = _repo_checkout(item)
    if cwd is not None and _rollout.has_target(cwd, "verify"):
        res = _rollout.verify(cwd)
        if not res.ok:
            failures = item["verify_failures"] + 1
            evidence = f"make verify failed (exit {res.exit_code}): {_tail_for_note(res.tail)}"
            if failures >= core.VERIFY_FAILURE_LIMIT:
                core.set_state(conn, event_id, core.STATE_FAILED, now, expect_state=core.STATE_VERIFYING,
                                expect_eq={"reverting_sha": sha}, verify_failures=failures,
                                verify_result=evidence[:VERIFY_RESULT_MAX], failure_class=core.FAILURE_WORK,
                                note=f"production unhealthy after revert of {sha[:12]}: {failures} consecutive "
                                     f"failing passes — {evidence}")
            else:
                core.set_state(conn, event_id, core.STATE_VERIFYING, now, expect_state=core.STATE_VERIFYING,
                                expect_eq={"reverting_sha": sha}, verify_failures=failures,
                                verify_result=evidence[:VERIFY_RESULT_MAX])
            conn.commit()
            return
        verify_note = "make verify passed"

    record = _revert_record(item)
    # A revert is an attempt that did not land: it counts as a stalled round.
    stalled = item["stalled_rounds"] + 1
    if not work.revisions_left(item, stalled):
        core.set_state(conn, event_id, core.STATE_FAILED, now, expect_state=core.STATE_VERIFYING,
                        expect_eq={"reverting_sha": sha}, reverting_sha=None, verify_failures=0, verify_result=verify_note,
                        failure_class=core.FAILURE_WORK, stalled_rounds=stalled,
                        note=f"reverted {sha[:12]} ({verify_note}); no implement attempt left — "
                             f"verification failure: {record['evidence']}")
        conn.commit()
        return
    attempt = item["revision_count"] + 2
    core.set_state(conn, event_id, core.STATE_WORKING, now, expect_state=core.STATE_VERIFYING, expect_eq={"reverting_sha": sha},
                    note=f"reverted {sha[:12]} ({verify_note}); attempt {attempt}/{core.MAX_IMPLEMENT_ATTEMPTS} next",
                    reverting_sha=None, revision_count=item["revision_count"] + 1, stalled_rounds=stalled, implement_job=None,
                    validation_job=None, pr_url=None, reviewed_sha=None, blocked_sha=None, strikes=0, retry_at=None, **core.VERIFY_RESET)
    conn.commit()
