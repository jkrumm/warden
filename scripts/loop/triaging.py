"""The triage step: one single-shot agent-gateway `triage` job per `new` item decides where it goes
(attach | new | fixed_by | ignore). Submit, fold, poll and settle; the answer is validated
against the ledger before any item moves."""

from __future__ import annotations

import datetime as dt
import sqlite3
import sys
import time
from typing import Any

from clients import agent_gateway as _agent_gateway
from clients.errors import RemoteError, WardenError
from lifecycle import intake as _intake
from loop import core, intake, work


# Issues, alerts and `warden run` share one pool: every `new` item that is ready (an alert
# once debounce-eligible, an issue or `warden run` immediately) gets one single-shot agent-gateway
# `triage` job that answers attach | new(repo, title) | fixed_by | ignore. The signal's own
# label picks the candidate repos first (intake.route_by_label()); with no label every known
# repo is a candidate. The submit is a compare-and-set claim on `triage_job`; the fold is a
# compare-and-set on the job id, so a second pass finding the same finished job writes nothing.

# `triage_job` while one process is submitting: `claiming:<iso claimed-at>`. A claim older than
# TRIAGE_CLAIM_STALE_MINUTES is a crashed submitter's and is released.
TRIAGE_CLAIM_PREFIX = "claiming:"
TRIAGE_CLAIM_STALE_MINUTES = 5
# Submissions per run: the rest stay `new` for the next run (overflow waits, never drops).
MAX_TRIAGE_SUBMITS_PER_RUN = 20
# After submitting, the loop re-polls its own jobs this long (they take 1-10 s) so a fast job
# folds in the same tick; whatever is still running folds on the next tick. Nothing fails on expiry.
TRIAGE_SETTLE_S = 45
# A triage job still not terminal this long after it was submitted is stuck (a single-shot job takes
# seconds): cancelled best-effort and struck, so the item is submitted again instead of waiting forever.
TRIAGE_JOB_STALE_MINUTES = 30
# `warden run` waits for its one triage job; this is a hang guard on a single non-agentic
# request, never a budget. On expiry the job stays recorded and the loop's poll_triage_jobs()
# folds it.
TRIAGE_WAIT_GUARD_S = 1800


def is_triage_claim(value: str | None) -> bool:
    return bool(value) and value.startswith(TRIAGE_CLAIM_PREFIX)


def _triage_candidates(item: sqlite3.Row, event: sqlite3.Row) -> list[str]:
    label = intake.label_route(item, event)
    return [label] if label else _intake.known_repos()


def _submit_triage(conn: sqlite3.Connection, item: sqlite3.Row, now: dt.datetime,
                   policy: dict[str, Any]) -> dict[str, Any] | None:
    """Claim one `new` item, submit its triage job, record the job id. Returns the job, or None
    when nothing is in flight afterwards: the claim was lost, or the submit failed and the item
    was struck. EVERY submit failure strikes (retry after backoff, `failed` on the third) — a
    agent-gateway 4xx included: a refused triage prompt is agent-gateway's or the prompt's problem, never
    the item's, so it must not end the item on the first refusal. The strike is a compare-and-set
    on the claim."""
    event_id = item["event_id"]
    event = core.get_event(conn, event_id)
    if event is None:
        return None
    claim = f"{TRIAGE_CLAIM_PREFIX}{core.now_iso(now)}"
    if not core.set_state(conn, event_id, core.STATE_NEW, now, expect_state=core.STATE_NEW, expect_null=("triage_job",),
                           triage_job=claim, triage_job_at=core.now_iso(now)):
        return None
    conn.commit()
    claimed = {"triage_job": claim}
    item = core.get_item(conn, event_id)
    prompt = _intake.build_triage_prompt(conn, item, event, _triage_candidates(item, event), now)
    try:
        job = _agent_gateway.submit_triage(prompt=prompt, schema=_intake.TRIAGE_SCHEMA)
    except WardenError as e:
        print(f"triage: triage submit failed for {item['signature']}: {e}", file=sys.stderr)
        core.strike(conn, event_id, now, f"triage submit failed: {e}", retry_state=core.STATE_NEW,
                     expect_state=core.STATE_NEW, expect_eq=claimed, triage_job=None)
        conn.commit()
        return None
    recorded = core.set_state(conn, event_id, core.STATE_NEW, now, expect_state=core.STATE_NEW, expect_eq=claimed,
                               triage_job=job["id"], triage_job_at=core.now_iso(now))
    conn.commit()
    if not recorded:
        print(f"triage: triage job {job['id']} for {item['signature']} lost its claim (released as stale "
              f"while submitting) — its answer is dropped", file=sys.stderr)
        return None
    return job


def _fold_triage_job(conn: sqlite3.Connection, event_id: int, job_id: str, job: dict[str, Any],
                     now: dt.datetime) -> str | None:
    """Settle one finished triage job onto its item, compare-and-set on `state=new` and
    `triage_job=<job_id>`. Returns what happened in a few words (`attached to #4`, `fixed by #7`,
    `ignored`, `triaged to <repo>`, `retrying: <why>`), or None when another pass already settled
    the item.

    A failed job or an unusable answer strikes (retry after backoff, `failed` on the third). Then
    per action: `attach` closes the item `closed(duplicate)` onto an OPEN target and adds its
    occurrences to it; `fixed_by` closes it `closed(fixed_by)` onto a fixed item or a PR on record;
    `ignore` closes it `closed(ignored)`; `new` sets the repo and moves it to `triaged`.

    Guards: a human item (the owner asked explicitly) is never attached, fixed_by'd or ignored, and
    a GitHub issue is never ignored. An attached issue gets the comment-back ("duplicate: tracked
    as #N: <reason>") like any other issue verdict. An attach or fixed_by naming nothing real is
    treated as `new`, which needs a repo: an issue or `warden run` keeps its own, a labelled alert
    takes its label's, any other alert takes the answer's, which must be a known repo; an
    unlabelled alert whose answer names none strikes instead.

    Every outcome but `ignore` clears `triage_job` in the same compare-and-set: a row keeps its
    triage job only when the MODEL ignored it, which is what reopen_if_needed() reads to tell a
    model's ignore (revisited after the cooldown) from an owner's dismiss or `--ignore` (never
    reopened)."""
    item = core.get_item(conn, event_id)
    event = core.get_event(conn, event_id)
    if item is None or event is None or item["state"] != core.STATE_NEW or item["triage_job"] != job_id:
        return None
    cas = {"expect_state": core.STATE_NEW, "expect_eq": {"triage_job": job_id}}

    def strike(reason: str) -> str:
        core.strike(conn, event_id, now, reason, retry_state=core.STATE_NEW, triage_job=None, **cas)
        conn.commit()
        return f"retrying: {reason}"

    def close(close_reason: str, note: str, outcome: str, **columns: Any) -> str | None:
        won = core.set_state(conn, event_id, core.STATE_CLOSED, now, close_reason=close_reason, note=note,
                              **cas, **columns)
        conn.commit()
        return outcome if won else None

    if job.get("status") != "done":
        return strike(f"triage job {job_id} {job.get('status')}: {job.get('error') or 'no detail'}")
    result = job.get("result")
    answer = result.get("result") if isinstance(result, dict) else None
    action = answer.get("action") if isinstance(answer, dict) else None
    if action not in _intake.TRIAGE_ACTIONS:
        return strike(f"triage job {job_id} returned no usable answer")
    reason = str(answer.get("reason") or "").strip()
    origin = item["origin"]

    if action == "attach" and origin != "human":
        target = open_item(conn, answer.get("item"), exclude=event_id)
        if target is not None:
            outcome = close(core.CLOSE_DUPLICATE, f"attached to #{target['event_id']}: {reason}",
                            f"attached to #{target['event_id']}", duplicate_of=target["event_id"],
                            triage_job=None)
            if outcome:
                intake.bump_open_target(conn, target["event_id"], occurrences=item["occurrences"],
                                         last_seen=item["last_seen"])
                conn.commit()
                # An issue closed as a duplicate would otherwise vanish without a word to whoever
                # filed it (the owner's own issues only — see maybe_comment_back_on_issue()).
                work.maybe_comment_back_on_issue(conn, item, event, {}, now, state="duplicate",
                                                  note=f"tracked as #{target['event_id']}: {reason}", dry_run=False)
            return outcome

    if action == "fixed_by" and origin != "human":
        ref = _fixed_reference(conn, answer, exclude=event_id)
        if ref is not None:
            return close(core.CLOSE_FIXED_BY, f"fixed by {ref}: {reason}", f"fixed by {ref}", triage_job=None)

    if action == "ignore" and origin == "alert":
        return close(core.CLOSE_IGNORED, f"ignored: {reason}", "ignored")

    label = intake.label_route(item, event)
    if origin != "alert":
        repo = item["repo"]
    elif label:
        repo = label
    else:
        repo = answer.get("repo") if answer.get("repo") in _intake.known_repos() else None
    if repo is None:
        return strike(f"triage job {job_id} named no candidate repo (answer: {action}, "
                      f"repo {answer.get('repo')!r})")
    won = core.set_state(conn, event_id, core.STATE_TRIAGED, now, repo=repo, note=str(answer.get("title") or reason),
                          strikes=0, retry_at=None, triage_job=None, **cas)
    conn.commit()
    return f"triaged to {repo}" if won else None


def open_item(conn: sqlite3.Connection, event_id: Any, *, exclude: int) -> sqlite3.Row | None:
    """The item `event_id` names when it exists, is not `exclude`, and is still open."""
    if not isinstance(event_id, int) or isinstance(event_id, bool) or event_id == exclude:
        return None
    target = core.get_item(conn, event_id)
    return None if target is None or target["state"] in core.NOT_OPEN_STATES else target


def _fixed_reference(conn: sqlite3.Connection, answer: dict[str, Any], *, exclude: int) -> str | None:
    """What a `fixed_by` answer points at, as text for the note, or None when it points at nothing
    real: an item that is `fixed` or carries a PR, or a PR URL on some item's record."""
    target_id = answer.get("item")
    if isinstance(target_id, int) and not isinstance(target_id, bool) and target_id != exclude:
        target = core.get_item(conn, target_id)
        if target is not None and (target["state"] == core.STATE_FIXED or target["pr_url"]):
            return f"#{target_id}" + (f" ({target['pr_url']})" if target["pr_url"] else "")
    pr = answer.get("pr")
    if isinstance(pr, str) and pr and conn.execute(
            "SELECT 1 FROM triage_items WHERE pr_url=? AND event_id != ?", (pr, exclude)).fetchone():
        return pr
    return None


def poll_triage_jobs(conn: sqlite3.Connection, now: dt.datetime, *, dry_run: bool) -> None:
    """Fold every finished triage job onto its item (see _fold_triage_job()). A claim older than
    TRIAGE_CLAIM_STALE_MINUTES is a submitter that died between claiming and recording the job:
    released, so the item is submitted again. A job agent-gateway no longer knows is lost, and strikes,
    and so does one still not terminal TRIAGE_JOB_STALE_MINUTES after it was submitted
    (`triage_job_at`): cancelled best-effort first, so a job that is merely slow cannot also fold."""
    if dry_run:
        return
    stale_before = now - dt.timedelta(minutes=TRIAGE_CLAIM_STALE_MINUTES)
    rows = conn.execute(
        "SELECT event_id, signature, triage_job, triage_job_at FROM triage_items "
        "WHERE state=? AND triage_job IS NOT NULL",
        (core.STATE_NEW,),
    ).fetchall()
    for row in rows:
        event_id, job_id = row["event_id"], row["triage_job"]
        if is_triage_claim(job_id):
            claimed_at = core.parse_ts(job_id[len(TRIAGE_CLAIM_PREFIX):])
            if claimed_at is None or claimed_at < stale_before:
                released = core.set_state(conn, event_id, core.STATE_NEW, now, expect_state=core.STATE_NEW,
                                           expect_eq={"triage_job": job_id}, triage_job=None)
                conn.commit()
                if released:
                    print(f"triage: released a stale triage claim on {row['signature']} (event {event_id})",
                          file=sys.stderr)
            continue
        try:
            job = _agent_gateway.get(job_id)
        except RemoteError as e:
            print(f"triage: could not poll agent-gateway triage job {job_id} for {row['signature']}: {e}",
                  file=sys.stderr)
            continue
        if job is None:
            core.strike(conn, event_id, now, f"agent-gateway has no record of triage job {job_id} (pruned or lost)",
                         retry_state=core.STATE_NEW, expect_state=core.STATE_NEW, expect_eq={"triage_job": job_id},
                         triage_job=None)
            conn.commit()
            continue
        if job.get("status") not in _agent_gateway.TERMINAL:
            submitted_at = core.parse_ts(row["triage_job_at"])
            if submitted_at is None or now - submitted_at < dt.timedelta(minutes=TRIAGE_JOB_STALE_MINUTES):
                continue
            try:
                _agent_gateway.cancel(job_id)
            except WardenError as e:
                print(f"triage: could not cancel stuck triage job {job_id}: {e}", file=sys.stderr)
            core.strike(conn, event_id, now, f"triage job {job_id} still {job.get('status')} after "
                         f"{TRIAGE_JOB_STALE_MINUTES} min — cancelled", retry_state=core.STATE_NEW,
                         expect_state=core.STATE_NEW, expect_eq={"triage_job": job_id}, triage_job=None)
            conn.commit()
            continue
        _fold_triage_job(conn, event_id, job_id, job, now)


def submit_triage_jobs(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                       *, dry_run: bool) -> list[str]:
    """Open a triage job for every ready `new` item without one: `warden run` and issues first,
    then alerts that passed the debounce (is_escalation_eligible()) and the cooldown
    (cooldown_ok()), at most MAX_TRIAGE_SUBMITS_PER_RUN per run. A job that is already finished
    when the submit returns is folded on the spot; the rest fold in poll_triage_jobs(). Returns the
    ids of the jobs submitted by THIS call that are still running (what settle_triage_jobs() waits on)."""
    ready_sql, ready_params = core.retry_ready_sql(now)
    rows = conn.execute(
        f"SELECT * FROM triage_items WHERE state=? AND triage_job IS NULL AND {ready_sql} "
        f"ORDER BY origin = 'alert', event_id",
        (core.STATE_NEW, *ready_params),
    ).fetchall()
    ready = []
    for item in rows:
        if item["origin"] == "alert":
            if not work.is_escalation_eligible(item, policy, now):
                continue
            if not work.cooldown_ok(conn, item, policy, now):
                print(f"triage: {item['signature']} recurred inside cooldownHours, not re-triaging yet",
                      file=sys.stderr)
                continue
        ready.append(item)
    batch, overflow = ready[:MAX_TRIAGE_SUBMITS_PER_RUN], ready[MAX_TRIAGE_SUBMITS_PER_RUN:]
    if overflow:
        print(f"triage: {len(overflow)} more item(s) wait for the next run (cap {MAX_TRIAGE_SUBMITS_PER_RUN} "
              f"triage submissions per run): {[i['signature'] for i in overflow]}", file=sys.stderr)
    if dry_run:
        if batch:
            print(f"[dry-run] would submit {len(batch)} triage job(s): {[i['signature'] for i in batch]}")
        return []
    running: list[str] = []
    for item in batch:
        job = _submit_triage(conn, item, now, policy)
        if job is None:
            continue
        if job.get("status") in _agent_gateway.TERMINAL:
            _fold_triage_job(conn, item["event_id"], job["id"], job, now)
        else:
            running.append(job["id"])
    return running


def settle_triage_jobs(conn: sqlite3.Connection, now: dt.datetime, *, dry_run: bool,
                       job_ids: list[str]) -> None:
    """Poll the triage jobs this run submitted (`job_ids`, from submit_triage_jobs()) until none is
    left in flight or TRIAGE_SETTLE_S has passed (they take 1-10 s, so most fold in this tick). Jobs
    still running are not failed: the next tick's poll_triage_jobs() folds them. Only this run's
    jobs are waited on — an older job that is stuck would otherwise cost the full window every tick."""
    deadline = time.monotonic() + TRIAGE_SETTLE_S
    while True:
        poll_triage_jobs(conn, now, dry_run=dry_run)
        marks = ",".join("?" * len(job_ids))
        pending = conn.execute(
            f"SELECT 1 FROM triage_items WHERE state=? AND triage_job IN ({marks})", (core.STATE_NEW, *job_ids),
        ).fetchone() if job_ids else None
        if dry_run or pending is None or time.monotonic() >= deadline:
            return
        time.sleep(2)


def triage_item_now(conn: sqlite3.Connection, event_id: int, now: dt.datetime) -> str | None:
    """Triage one item synchronously — `warden run`'s intake: submit, wait for the job, fold it.
    Returns the outcome (see _fold_triage_job()), or None when the item was not triaged here (not
    `new`, already has a job, or agent-gateway could not be reached — the loop retries those)."""
    item = core.get_item(conn, event_id)
    if item is None or item["state"] != core.STATE_NEW or item["triage_job"] is not None:
        return None
    job = _submit_triage(conn, item, now, core.load_policy())
    if job is None:
        return None
    try:
        if job.get("status") not in _agent_gateway.TERMINAL:
            job = _agent_gateway.wait(job["id"], timeout_s=TRIAGE_WAIT_GUARD_S, interval_s=2)
    except RemoteError as e:
        print(f"triage: could not wait for triage job {job['id']}: {e}", file=sys.stderr)
        return None
    if job is None:
        return None
    return _fold_triage_job(conn, event_id, job["id"], job, now)
