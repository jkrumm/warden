"""Intake: ingest alerts, GitHub issues and `warden run` into `triage_items`; reopen, resolve
(silence only ever cancels a `new` item), recover-pair and quiet-resolve grouped alerts; classify
(the cheap no-model filters) and route by label."""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import sqlite3
import sys
from typing import Any

from clients import github as _github, jobs_grade as _jobs_grade
from clients.errors import RemoteError
from lifecycle import intake as _intake
from lifecycle import items as _items
from lifecycle import rollout as _rollout
from loop import core, triaging


def ingest(conn: sqlite3.Connection, now: dt.datetime) -> None:
    """Upsert one triage_items row per open ingest-source event. Occurrences and last_seen refresh
    every run; repo/state are left alone on an existing row, since classify() owns those, so a
    re-run never clobbers an escalation in progress."""
    placeholders = ",".join("?" * len(core.INGEST_SOURCES))
    rows = conn.execute(
        f"SELECT * FROM events WHERE resolved_at IS NULL AND source IN ({placeholders})",
        core.INGEST_SOURCES,
    ).fetchall()
    now_iso = core.now_iso(now)
    for ev in rows:
        event_id = ev["id"]
        payload = core.safe_json(ev["payload_json"])
        occurrences = payload.get("batch_count")
        if not isinstance(occurrences, int):
            occurrences = int(ev["reminder_count"] or 0) + 1
        last_seen = ev["last_reminder_at"] or ev["notified_at"] or ev["first_seen"] or now_iso
        existing = core.get_item(conn, event_id)
        if existing is None:
            conn.execute(
                "INSERT INTO triage_items(event_id, signature, repo, state, occurrences, "
                "first_seen, last_seen, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?)",
                (event_id, core.signature(ev), None, core.STATE_NEW, occurrences,
                 ev["first_seen"], last_seen, now_iso, now_iso),
            )
            core.record_created_transition(conn, event_id, core.STATE_NEW, now_iso)
        else:
            conn.execute(
                "UPDATE triage_items SET occurrences=?, last_seen=?, updated_at=? WHERE event_id=?",
                (occurrences, last_seen, now_iso, event_id),
            )
    conn.commit()


# The event source `human` items are ingested under, and the source GitHub-issue items use:
# `github_go`, not `github_issue`. watchdog-poll.py's stale-issue digest writes `github_issue`
# events under the same `repo#num` external_id with staleness semantics (resolved when the issue
# goes quiet), so sharing that source would fold an issue item into that reconcile() cycle and
# resolve it falsely. The name is a leftover of the `warden:go` label; renaming a source is a
# migration, so it stays.
ORIGIN_EVENT_SOURCE = {"human": "human", "github_issue": "github_go"}

# The one opt-out label: an issue carrying it never gets an item. Every open issue is ingested
# by default; this label keeps one out.
GITHUB_SKIP_LABEL = "warden:skip"


BRIEF_OVERLAP_MIN = 0.8
_TOKEN_RE = re.compile(r"[a-z0-9_./-]{3,}")


def _brief_tokens(text: str) -> frozenset[str]:
    return frozenset(_TOKEN_RE.findall(text.lower()))


def brief_overlap(a: str, b: str) -> float:
    """Jaccard similarity of the two briefs' word sets: 1.0 for the same words in any order."""
    ta, tb = _brief_tokens(a), _brief_tokens(b)
    return len(ta & tb) / len(ta | tb) if ta and tb else 0.0


def overlapping_open_item(conn: sqlite3.Connection, *, repo: str, origin: str, brief: str,
                           thread_ts: str | None) -> int | None:
    """The `event_id` of an open item in `repo` of the same origin whose brief says (nearly) the same
    thing, or None. Only a `human` request: an issue is keyed on its number already. Duplicate work starts at the
    door: a second `warden run` for a request still in flight must not open a second investigation and a second pull request. `failed` and terminal
    items do not count (a re-run after a failure is a decision), and a request that arrives with its
    own Slack thread only matches an item answering into that same thread, so nobody's answer is
    dropped."""
    placeholders = ",".join("?" * len(core.NOT_OPEN_STATES))
    rows = conn.execute(
        f"SELECT event_id, brief, origin_thread_ts FROM triage_items WHERE repo=? AND origin=? "
        f"AND brief IS NOT NULL AND state NOT IN ({placeholders}) ORDER BY event_id",
        (repo, origin, *core.NOT_OPEN_STATES),
    ).fetchall()
    for row in rows:
        if thread_ts and row["origin_thread_ts"] != thread_ts:
            continue
        if brief_overlap(brief, row["brief"]) >= BRIEF_OVERLAP_MIN:
            return row["event_id"]
    return None


def open_origin_item(conn: sqlite3.Connection, *, origin: str, repo: str, brief: str, max_tier: str,
                      external_id: str, title: str, url: str | None = None,
                      payload: dict[str, Any] | None = None, now: dt.datetime | None = None,
                      origin_channel: str | None = None, origin_thread_ts: str | None = None) -> int | None:
    """Open one `triage_items` row for a `human` or `github_issue` origin: the non-alert counterpart
    to `ingest()`, which only handles `INGEST_SOURCES`. Inserts (or reuses) an `events` row keyed on
    `(source, external_id)`; `source` is `human` for a human origin and `github_go` for a GitHub
    issue (see ORIGIN_EVENT_SOURCE).

    One row per open signature: if a NON-TERMINAL item already exists for this `(origin, repo,
    external_id)` this returns its `event_id` and inserts nothing, so a caller never opens a second
    item for the same handover. If a TERMINAL item exists (fixed/quiet/closed) this does nothing and
    returns None: an issue whose work finished, then dropped out of the open-issue poll (closed, or
    `warden:skip` applied), or a repeat `warden run` for something already `closed`, is not a new
    handover.

    `origin_channel`/`origin_thread_ts` are the Slack thread this item's verdict must answer into (a
    `human` origin from `warden run`'s `--origin-channel`/`--origin-thread`); NULL for a GitHub
    issue, which has no thread."""
    now = now or dt.datetime.now(dt.timezone.utc)
    now_iso = core.now_iso(now)
    source = ORIGIN_EVENT_SOURCE[origin]

    event_row = conn.execute(
        "SELECT * FROM events WHERE source=? AND external_id=?", (source, external_id)
    ).fetchone()
    if event_row is None:
        twin = (overlapping_open_item(conn, repo=repo, origin=origin, brief=brief, thread_ts=origin_thread_ts)
                if origin == "human" else None)
        if twin is not None:
            print(f"triage: {origin} request for {repo} overlaps open item #{twin}, not opening a second one",
                  file=sys.stderr)
            return twin

        conn.execute(
            "INSERT INTO events(source, external_id, title, url, payload_json, first_seen) "
            "VALUES (?,?,?,?,?,?)",
            (source, external_id, title, url or "", json.dumps(payload or {}), now_iso),
        )
        conn.commit()
        event_row = conn.execute(
            "SELECT * FROM events WHERE source=? AND external_id=?", (source, external_id)
        ).fetchone()
    event_id = event_row["id"]

    existing_item = core.get_item(conn, event_id)
    if existing_item is not None:
        if existing_item["state"] in core.TERMINAL_STATES:
            return None
        return event_id

    conn.execute(
        "INSERT INTO triage_items(event_id, signature, repo, state, origin, max_tier, brief, "
        "origin_channel, origin_thread_ts, occurrences, first_seen, last_seen, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (event_id, core.signature(event_row), repo, core.STATE_NEW, origin, max_tier, brief,
         origin_channel or None, origin_thread_ts or None, 1, now_iso, now_iso, now_iso, now_iso),
    )
    core.record_created_transition(conn, event_id, core.STATE_NEW, now_iso)
    conn.commit()
    return event_id


def ingest_github_issues(conn: sqlite3.Connection, now: dt.datetime) -> None:
    """Called once per loop tick, same cadence as `ingest()`. Polls every open issue under
    `_github.GH_OWNER`, minus any carrying `GITHUB_SKIP_LABEL`, and opens (or reuses, via
    `open_origin_item()`) one item per hit. A still-open `github_go` event whose issue no longer
    appears in the result set, AND that a direct per-issue check confirms is closed, is marked
    resolved (`resolved_at=now`); apply_resolutions() then closes a still-`new` item on that
    resolution like a disappeared alert (silence cancels the need to START, never discharges an
    obligation already in flight; a `new` item carries none).

    Missing from the result set is NOT proof the issue closed: `search_issues()` silently omits a
    repo the token cannot search (a PAT without `Issues: read` gets a 422 from `/search/issues`
    while `GET /repos/.../issues/{n}` 403s, both indistinguishable from "no open issues" at the
    search layer). So this calls `_github.read_issue()` on that one issue and resolves only if it
    reports `state == "closed"`; any error leaves the event alone (fail-closed).

    Trust is fail-closed, like watchdog-poll.py's `_github_author()`/`TRUSTED_GH_LOGIN`: only
    `_github.GH_OWNER` is trusted, so a missing or unparseable author is third-party. `max_tier`
    follows: `implement` for the owner's own issues, `investigate` always for everyone else,
    whatever the labels.

    Never raises: a GitHub outage must not take down the rest of the tick."""
    try:
        hits = _github.search_issues(owner=_github.GH_OWNER, skip_label=GITHUB_SKIP_LABEL)
    except RemoteError as e:
        print(f"triage: could not poll GitHub issues for owner {_github.GH_OWNER!r}: {e}", file=sys.stderr)
        return

    seen_external_ids: set[str] = set()
    for hit in hits:
        repo = hit.get("repo")
        number = hit.get("number")
        if not repo or not isinstance(number, int):
            continue
        external_id = f"{_github.GH_OWNER}/{repo}#{number}"
        seen_external_ids.add(external_id)
        author = hit.get("author")
        max_tier = "implement" if author == _github.GH_OWNER else "investigate"
        open_origin_item(
            conn, origin="github_issue", repo=repo, brief=hit.get("body") or "", max_tier=max_tier,
            external_id=external_id, title=hit.get("title") or "?", url=hit.get("url"),
            payload={"repo": repo, "number": number, "author": author,
                     "labels": hit.get("labels") or [], "updated_at": hit.get("updated_at")},
            now=now,
        )

    now_iso = core.now_iso(now)
    for row in conn.execute(
        "SELECT id, external_id FROM events WHERE source='github_go' AND resolved_at IS NULL"
    ).fetchall():
        if row["external_id"] in seen_external_ids:
            continue
        owner_part, _, rest = row["external_id"].partition("/")
        repo_part, _, number_part = rest.partition("#")
        try:
            number = int(number_part)
        except ValueError:
            continue
        try:
            issue = _github.read_issue(owner_part, repo_part, number)
        except RemoteError:
            continue
        if issue.get("state") != "closed":
            continue
        conn.execute("UPDATE events SET resolved_at=? WHERE id=?", (now_iso, row["id"]))
    conn.commit()


def ingest_jobs(conn: sqlite3.Connection, now: dt.datetime, *, dry_run: bool = False) -> None:
    """The `job` source. Once per loop tick: `jobs-grade` (clients/jobs_grade.py) grades this machine's
    background jobs; every label in a bad grade (failing | stale | flapping | unloaded) is one
    `events` row `job:<machine>:<label>`, folded in by watchdog-poll.py's reconcile() exactly as a
    `uk` monitor is. That makes the source LEVEL-based: the event stays one row while the grade stays
    bad (its payload refreshes every tick), it resolves when the label grades `ok` or leaves the
    output, and it re-arms (a new occurrence mark, so reopen_if_needed() sees a recurrence) when the
    label goes bad again.

    A job event opens a `triage_items` row of origin `job`, state `new`, `max_tier=implement`, with
    the brief stored on the row (job_brief(): the grade, the last runs and output tail, the owner repo's
    commits since `last_ok`). Its repo is NOT chosen here or by a triage job: route_job_items()
    sends it to the `owner` the job's own state file names. An item still `new`/`triaged` keeps its
    brief current (the grade text moves); one past that is not touched, so its claim is not disturbed.

    Blind is not recovered: when jobs-grade cannot answer (client returns None) nothing is opened and
    nothing is resolved this pass. Under --dry-run nothing runs (the contract is "never shells out")."""
    if dry_run:
        print("[dry-run] would run jobs-grade for the job source (skipped under --dry-run)")
        return
    rows = _jobs_grade.fetch(env={**os.environ, "PATH": _rollout.host_path()})
    if rows is None:
        return
    machine = _jobs_grade.machine_name()
    observed = [
        {"external_id": f"{machine}:{row['label']}", "title": f"job {row['label']} is {row['grade']}", "url": "",
         "payload": {**row, "machine": machine}}
        for row in rows if row["grade"] in _jobs_grade.BAD_GRADES
    ]
    open_events = conn.execute("SELECT external_id FROM events WHERE source=? AND resolved_at IS NULL",
                                (core.JOB_SOURCE,)).fetchall()
    graded = {f"{machine}:{row['label']}" for row in rows}
    for ev in open_events:
        if ev["external_id"] not in graded:   # an ok label resolves silently; a vanished one is said
            print(f"triage: job {ev['external_id']} left the jobs-grade output (plist removed or state file "
                  f"deleted) — resolving its event", file=sys.stderr)
    if observed or open_events:   # nothing observed and nothing open: all-ok passes touch no table
        wp = core.wp_module()
        if wp is None:
            print("triage: watchdog-poll.py did not load — the job source is skipped this pass", file=sys.stderr)
            return
        wp.reconcile(conn, core.JOB_SOURCE, observed, now, 0)
        conn.commit()

    now_iso = core.now_iso(now)
    for ev in conn.execute("SELECT * FROM events WHERE source=? AND resolved_at IS NULL",
                            (core.JOB_SOURCE,)).fetchall():
        item = core.get_item(conn, ev["id"])
        if item is not None and item["state"] not in (core.STATE_NEW, core.STATE_TRIAGED):
            continue   # past triage the brief is fixed: its claim is not disturbed, and no host fact is read for it
        payload = core.safe_json(ev["payload_json"])
        brief = _intake.job_brief(payload, machine, _intake.gather_job_context(payload))
        if item is None:
            conn.execute(
                "INSERT INTO triage_items(event_id, signature, repo, state, origin, max_tier, brief, "
                "occurrences, first_seen, last_seen, created_at, updated_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                (ev["id"], core.signature(ev), None, core.STATE_NEW, core.JOB_ORIGIN, core.JOB_MAX_TIER, brief,
                 1, ev["first_seen"], now_iso, now_iso, now_iso),
            )
            core.record_created_transition(conn, ev["id"], core.STATE_NEW, now_iso)
        elif item["brief"] != brief or item["max_tier"] != core.JOB_MAX_TIER:
            # max_tier too: an item opened before the implement tier (Wave 6's) must not keep the old ceiling
            # when it is reopened or re-investigated.
            conn.execute("UPDATE triage_items SET brief=?, max_tier=? WHERE event_id=?",
                         (brief, core.JOB_MAX_TIER, ev["id"]))
    conn.commit()


def job_chronic_reason(conn: sqlite3.Connection, item: sqlite3.Row, policy: dict[str, Any],
                       label: str) -> str | None:
    """Why a REOPENED `job` item goes to the owner instead of another dispatch, or None. Reads the
    reopen that reopen_if_needed() just made (the item's latest terminal -> `new` transition) and the
    terminal transition it reopened from; `label` (the job's, for the note) comes from the event payload. Two rules, both only for an item warden had already acted on
    (it carries a `dispatch_job`; an item that healed `quiet` before any dispatch is simply new work):

    - REGRESSION: the item was `fixed` and its job failed again within `jobRegressionHours` — the fix
      did not hold a day. Counts whatever `jobChronicRecurrences` says.
    - CHRONIC: it reopened `jobChronicRecurrences` times within `jobChronicWindowDays` (default one
      reopen within 7 days of the terminal state it left).

    A first-ever item has no reopen transition and is never chronic, and neither is one the owner closed
    by hand (`warden close`, core.HAND_CLOSE_NOTE_PREFIX): that close is the owner's answer that the failure
    was not real or needed nothing, so its recurrence is new work, not a fix that did not hold."""
    if not item["dispatch_job"]:
        return None
    states = (core.STATE_FIXED, core.STATE_QUIET, core.STATE_CLOSED)
    reopen = conn.execute(
        "SELECT id, from_state, at FROM item_transitions WHERE event_id=? AND to_state=? "
        f"AND from_state IN ({','.join('?' * len(states))}) ORDER BY id DESC LIMIT 1",
        (item["event_id"], core.STATE_NEW, *states),
    ).fetchone()
    if reopen is None:
        return None
    reopened_at = core.parse_ts(reopen["at"])
    left = conn.execute(
        "SELECT at, note FROM item_transitions WHERE event_id=? AND to_state=? AND id < ? ORDER BY id DESC LIMIT 1",
        (item["event_id"], reopen["from_state"], reopen["id"]),
    ).fetchone()
    if left is not None and str(left["note"] or "").startswith(core.HAND_CLOSE_NOTE_PREFIX):
        return None   # the owner closed it by hand: a verdict that it needed nothing, not a fix that failed to hold
    left_at = core.parse_ts(left["at"]) if left else None
    if reopened_at is None or left_at is None:
        return None
    gap = reopened_at - left_at
    if reopen["from_state"] == core.STATE_FIXED and gap < dt.timedelta(hours=policy["jobRegressionHours"]):
        return (f"the fix did not hold: {label} failed again {gap.total_seconds() / 3600:.1f} h after it was "
                f"marked fixed (limit {policy['jobRegressionHours']:g} h)")
    window = policy["jobChronicWindowDays"]
    if gap < dt.timedelta(days=window):
        recurrences = _recurrence_count(conn, item["event_id"], reopened_at, window)
        if recurrences >= policy["jobChronicRecurrences"]:
            return f"{label} came back {recurrences}x within {window:g} days of warden acting on it"
    return None


def route_job_items(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime) -> None:
    """The `job` item's whole triage: no model call. Every `new` job item goes, in this order, to

    1. `needs_decision` when its owner is no repo warden knows (a state file the grader could not
       attribute, `?`, or a repo outside the repos root) — nothing can be dispatched to it;
    2. `needs_decision` when it is CHRONIC (job_chronic_reason()): a fix that did not hold, or a
       reopen inside the window. The owner repo is recorded so the card names it;
    3. `triaged` in the owner repo, where escalate_origin_items() dispatches it (unless paused: then it
       waits here, which is the point — ingest and routing continue, dispatch does not).

    A reopened item re-enters `triaged` with its old `dispatch_job` cleared: that pointer is the
    alert path's cooldown anchor, and escalate_origin_items() claims only a row without one."""
    rows = conn.execute(
        "SELECT * FROM triage_items WHERE state=? AND origin=?", (core.STATE_NEW, core.JOB_ORIGIN),
    ).fetchall()
    if not rows:
        return
    known = {name.lower(): name for name in _intake.known_repos()}
    for item in rows:
        event = core.get_event(conn, item["event_id"])
        if event is None:
            continue
        payload = core.safe_json(event["payload_json"])
        owner = known.get(str(payload.get("owner") or "").strip().lower())
        if owner is None:
            core.set_state(conn, item["event_id"], core.STATE_NEEDS_DECISION, now, expect_state=core.STATE_NEW,
                           note=f"[blocker] job owner {payload.get('owner')!r} is not a repo warden can dispatch to — "
                                f"fix `jobrun --owner` or the checkout")
            continue
        chronic = job_chronic_reason(conn, item, policy, str(payload.get("label") or event["external_id"]))
        if chronic is not None:
            core.set_state(conn, item["event_id"], core.STATE_NEEDS_DECISION, now, expect_state=core.STATE_NEW,
                           repo=owner, note=f"[chronic] {chronic}")
            continue
        core.set_state(conn, item["event_id"], core.STATE_TRIAGED, now, expect_state=core.STATE_NEW, repo=owner,
                       note=str(payload.get("reason") or event["title"] or "")[:200], dispatch_job=None,
                       strikes=0, retry_at=None, max_tier=core.JOB_MAX_TIER)   # a reopen lands here AFTER ingest_jobs() ran
    conn.commit()


def reinvestigate_stuck_job_items(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                                  *, dry_run: bool = False) -> None:
    """A `job` item the loop closed (an investigation with nothing to change) whose job is still graded
    bad `jobStillRedHours` later: the level-based source never re-arms it, so nothing would ever look
    again. The first time it goes back to `triaged` for a fresh investigation (the note starts
    core.STILL_RED_NOTE_PREFIX); when that one also closes with the job still red it is a
    `needs_decision`, which is the true answer: two investigations could not move the job. An item the
    owner closed by hand (core.HAND_CLOSE_NOTE_PREFIX) is the owner's call and is left alone."""
    limit = dt.timedelta(hours=policy["jobStillRedHours"])
    reopened_prs: list[str | None] = []
    rows = conn.execute(
        "SELECT ti.event_id AS event_id FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        "WHERE ti.origin=? AND ti.state=? AND e.resolved_at IS NULL", (core.JOB_ORIGIN, core.STATE_CLOSED),
    ).fetchall()
    for row in rows:
        event_id = row["event_id"]
        closed = conn.execute(
            "SELECT at, note FROM item_transitions WHERE event_id=? AND to_state=? ORDER BY id DESC LIMIT 1",
            (event_id, core.STATE_CLOSED)).fetchone()
        closed_at = core.parse_ts(closed["at"]) if closed else None
        if closed_at is None or now - closed_at < limit:
            continue
        if str(closed["note"] or "").startswith(core.HAND_CLOSE_NOTE_PREFIX):
            continue
        # Only this incident's marker counts: a recurrence re-enters `new`, which starts a fresh occurrence.
        earlier = conn.execute(
            "SELECT count(*) FROM item_transitions WHERE event_id=? AND to_state=? AND note LIKE ? AND id > "
            "(SELECT coalesce(max(id), 0) FROM item_transitions WHERE event_id=? AND to_state=?)",
            (event_id, core.STATE_TRIAGED, f"{core.STILL_RED_NOTE_PREFIX}%", event_id, core.STATE_NEW)).fetchone()[0]
        event = core.get_event(conn, event_id)
        if event is None:
            continue
        label = event["external_id"]
        hours = (now - closed_at).total_seconds() / 3600
        if dry_run:
            print(f"[dry-run] would {'escalate' if earlier else 're-investigate'} job item {event_id} "
                  f"(still red {hours:.1f} h after its investigation closed)")
            continue
        if earlier:
            core.set_state(conn, event_id, core.STATE_NEEDS_DECISION, now, expect_state=core.STATE_CLOSED,
                           note=f"[blocker] {label} is still red {hours:.1f} h after a second investigation "
                                f"closed without moving it")
            continue
        _, old_pr = core.reinvestigate(conn, event_id, now, expect_state=core.STATE_CLOSED,
                                       note=f"{core.STILL_RED_NOTE_PREFIX}{label} stayed red {hours:.1f} h after "
                                            f"its investigation closed; investigating again")
        reopened_prs.append(old_pr)
    conn.commit()
    for pr in reopened_prs:
        core.close_reinvestigated_pr(pr)


# Sources whose event is a level (DOWN/firing until its own recovery) and restarts at each occurrence.
LEVEL_EVENT_SOURCES = ("uk", "hyperdx")


def reopen_if_needed(conn: sqlite3.Connection, now: dt.datetime, policy: dict[str, Any] | None = None,
                     *, dry_run: bool = False) -> None:
    """A grouped or state source reuses the SAME events.id across a resolve -> recur cycle
    (UNIQUE(source, external_id)), so a row closed for an event that has since produced a new
    occurrence would sit invisible forever. Reopening to `new` (never clearing
    artifact_url/dispatch_job) lets the next escalation's brief say "a PR already exists for this
    signature" instead of rediscovering it. Every implement handle the closed attempt left is
    cleared, exactly as reinvestigate() clears them: a stale `implement_job` would be re-polled and
    `revision_count`/`pr_url` would route the new occurrence's `no_changes` to `needs_decision` — a
    page with no question — instead of the fresh investigation a recurrence must get (item 543).
    The cleared `pr_url` names a pull request that is CLOSED best-effort after the commit and kept in
    the note as `superseded PR <url>`, exactly as reinvestigate() does: leaving it open would make
    the fresh run's own PR a second one beside it, and a stale URL would mis-route a later
    `work`-failure back to `merging` (item 1480). Only a still-open PR is closed: a `fixed` item's
    PR is already merged (the `fixed` transition keeps `pr_url`), so close_reopened_pr() checks its
    state and leaves a merged one alone rather than posting a false 'Superseded' comment on it.
    `dispatch_job` stays as the escalation cooldown
    anchor (cooldown_ok()), which also makes the reopened item escalate as a singleton (escalate()).

    The predicate is "the event's occurrence_mark() changed since this row's last transition", NOT
    `events.resolved_at IS NULL`: for a GROUPED source (slack_alert, hermes_log) resolved_at stays
    NULL for up to 7 idle days by design (sweep_stale_grouped()), so it is true on every pass after
    a quiet-resolve and would reopen a row just re-closed, every 10 minutes, forever. Comparing
    marks ties reopening to an actual new occurrence, whichever of the two clocks produced it (see
    occurrence_mark()).

    Per closed row, the event's CURRENT mark against the one `set_state()` stamped at this row's
    last transition:

    - differs -> a genuine new occurrence since the row closed: reopen via set_state(), as an atomic
      conditional transition (expect_state=the closed state read here, expect_mark=the stored mark):
      a concurrent triage pass that claimed this same recurrence first has already moved the row, so
      only one caller's transition succeeds, and only that caller closes the superseded PR.
    - same -> still quiet: leave it alone.
    - stored mark IS NULL -> the row closed without a stamp. Do not reopen and do not guess a
      history it does not have: write the current mark as a baseline with a plain UPDATE (not
      set_state(): it writes no `state`) and leave the state. It reopens on the next genuine
      occurrence like any other row.

    A `closed(duplicate)` item whose event recurs does not reopen while the item it was attached to
    is still open: the recurrence is counted on that target (bump_open_target()). Once the target
    is terminal or `failed`, it reopens like any other (back to `new`, `duplicate_of` cleared by
    set_state()).

    `closed(ignored)` reopens only when the triage MODEL ignored it (the row still carries the
    `triage_job` that decided it) and `cooldownHours` have passed since it closed: an LLM must never
    silence a signature forever. A human `--ignore`, the policy's ignore list and the prose filter
    are deliberate calls and stay closed (the last two would close a reopened row again in
    classify() anyway, without a model call). `fixed`, `quiet` and every other `closed` reason are
    not a judgement that a signature is benign, so a genuine recurrence is new information for all
    of them. Terminal means "this item is closed", not "this signature may never open another"."""
    cooldown = dt.timedelta(hours=(policy or {}).get("cooldownHours") or core.DEFAULT_COOLDOWN_HOURS)
    rows = conn.execute(
        "SELECT ti.event_id AS event_id, ti.occurrence_mark AS stored_mark, ti.state AS item_state, "
        "ti.close_reason AS close_reason, ti.duplicate_of AS duplicate_of, ti.triage_job AS triage_job, "
        "ti.pr_url AS pr_url, ti.note AS note, e.* "
        "FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        "WHERE ti.state IN (?, ?, ?)",
        (core.STATE_FIXED, core.STATE_QUIET, core.STATE_CLOSED),
    ).fetchall()
    superseded_prs: list[str] = []
    for row in rows:
        stored_mark = row["stored_mark"]
        current_mark = core.occurrence_mark(row)
        if stored_mark is None:
            conn.execute(
                "UPDATE triage_items SET occurrence_mark=? WHERE event_id=?",
                (current_mark, row["event_id"]),
            )
        elif current_mark != stored_mark:
            if row["close_reason"] == core.CLOSE_IGNORED:
                # Only a model's ignore (it carries the triage job that decided it) is revisited; the ignore
                # list and the prose filter close such a row again at once in classify(), and a human `--ignore`
                # has no triage job and stays closed.
                if not row["triage_job"] or not _ignored_for(conn, row["event_id"], now) >= cooldown:
                    continue
            if (row["item_state"] == core.STATE_CLOSED and row["close_reason"] == core.CLOSE_DUPLICATE
                    and bump_open_target(conn, row["duplicate_of"], occurrences=1,
                                          last_seen=row["last_reminder_at"] or row["notified_at"] or core.now_iso(now))):
                conn.execute("UPDATE triage_items SET occurrence_mark=? WHERE event_id=?",
                             (current_mark, row["event_id"]))
                continue
            old_pr = row["pr_url"]
            columns: dict[str, Any] = {
                "implement_job": None, "validation_job": None, "pr_url": None, "reviewed_sha": None,
                "blocked_sha": None, "revision_count": 0, "stalled_rounds": 0,
            }
            if row["source"] in LEVEL_EVENT_SOURCES:
                # A level source's event restarts at every DOWN/firing; the item keeps its original
                # first_seen, so without this a reopen is already "open 30 min" and skips the debounce
                # (item 1543: reopened and triaged mid-blip, the event resolved a minute later).
                columns["first_seen"] = row["first_seen"]
                columns["occurrences"] = 1
            if old_pr:
                # Keep the URL in the note; the PR itself is CLOSED after the commit (never holding
                # the write lock across the network call), exactly as reinvestigate() does.
                columns["note"] = _items.append_to_note(row["note"], f"superseded PR {old_pr}")
            # Atomic conditional transition: the reopen wins only if the row is still in the closed
            # state this SELECT read AND still carries the occurrence mark it was read with. A
            # concurrent triage pass that claimed this same recurrence first has already moved the
            # row out of that state (and stamped a fresh mark), so this UPDATE matches nothing, the
            # rowcount is 0, and the loser closes nothing.
            rowcount = core.set_state(conn, row["event_id"], core.STATE_NEW, now,
                                      expect_state=row["item_state"], expect_mark=stored_mark, **columns)
            if old_pr and rowcount:
                superseded_prs.append(old_pr)
    conn.commit()
    if dry_run:
        # --dry-run makes NO outbound call, the PR close included: a preview against a throwaway DB
        # copy must never reach GitHub. The local reopen (set_state() above, including the note's
        # `superseded PR <url>`) still runs so the preview shows the transition; only the close loop
        # is skipped, leaving the superseded PR open in the discarded copy.
        return
    for old_pr in superseded_prs:
        core.close_reopened_pr(old_pr)


def _ignored_for(conn: sqlite3.Connection, event_id: int, now: dt.datetime) -> dt.timedelta:
    """How long ago the item last entered `closed` (zero when there is no such transition)."""
    row = conn.execute("SELECT MAX(at) AS at FROM item_transitions WHERE event_id=? AND to_state=?",
                       (event_id, core.STATE_CLOSED)).fetchone()
    closed_at = core.parse_ts(row["at"]) if row else None
    return now - closed_at if closed_at else dt.timedelta(0)


def bump_open_target(conn: sqlite3.Connection, target_id: int | None, *, occurrences: int,
                      last_seen: str | None) -> bool:
    """Count `occurrences` more sightings on an OPEN item (not terminal, not `failed`) and move
    its `last_seen` forward. Returns False, writing nothing, when the target is gone or no longer
    open — the caller then treats the sighting as a new item.

    An ALERT target is open and returns True but is not written: ingest() rewrites its
    `occurrences`/`last_seen` from its own event on every run, so a bump would be overwritten, and
    its own event already counts what it sees. The counts are kept for issue and `warden run`
    targets, which ingest() never touches."""
    target = triaging.open_item(conn, target_id, exclude=-1)
    if target is None:
        return False
    if target["origin"] == "alert":
        return True
    newest = max(filter(None, (target["last_seen"], last_seen)), default=None)
    conn.execute("UPDATE triage_items SET occurrences=occurrences+?, last_seen=? WHERE event_id=?",
                 (occurrences, newest, target["event_id"]))
    return True


def apply_resolutions(conn: sqlite3.Connection, now: dt.datetime,
                      policy: dict[str, Any] | None = None) -> None:
    # `events.resolved_at` is set only by disappearance-from-observation (watchdog-poll.py's ingest
    # sweep) or its 7-idle-day housekeeping, never by a human decision. So this is a SILENCE path, and
    # only `new` is eligible (see _SILENCE_RESOLVE_ELIGIBLE_STATES); every other state, terminal ones
    # included, is covered by that allowlist rather than named here.
    #
    # -> STATE_QUIET, never STATE_FIXED: disappearance is pure silence, and nothing here confirms a
    # change shipped (see STATE_QUIET).
    placeholders = ",".join("?" * len(_SILENCE_RESOLVE_ELIGIBLE_STATES))
    rows = conn.execute(
        f"SELECT ti.event_id, ti.repo FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        f"WHERE e.resolved_at IS NOT NULL AND ti.state IN ({placeholders})",
        _SILENCE_RESOLVE_ELIGIBLE_STATES,
    ).fetchall()
    for row in rows:
        if _is_chronic(conn, row["event_id"], row["repo"], policy or {}, now):
            continue
        # note=NULL is safe BECAUSE the row is `new`: it carries no obligation and no prior-phase text
        # worth keeping. Clearing it stops a stale QUIET_RESOLVE_NOTE_PREFIX/RECOVERY_PAIRED_NOTE_PREFIX
        # note from an earlier quiet-resolve surviving a reopen -> genuine fix -> resolve cycle as if it
        # were current.
        core.set_state(conn, row["event_id"], core.STATE_QUIET, now, note=None)
    conn.commit()


def resolve_recovered_decisions(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime) -> None:
    """A `uk`/`hyperdx` item parked in `needs_decision` whose event has since RECOVERED (the source's own
    UP/OK, not silence) has nothing left to decide: the question was asked about an outage that is over
    (item 1543). Quiet, never fixed. An item holding a written fix (a PR or an implement job) keeps its
    obligation, and a chronic signature keeps its needs_decision: the recurrence is the defect."""
    rows = conn.execute(
        "SELECT ti.event_id, ti.repo FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        "WHERE ti.state=? AND e.source IN (?, ?) AND e.resolved_at IS NOT NULL "
        "AND ti.pr_url IS NULL AND ti.implement_job IS NULL",
        (core.STATE_NEEDS_DECISION, *LEVEL_EVENT_SOURCES),
    ).fetchall()
    for row in rows:
        if _is_chronic(conn, row["event_id"], row["repo"], policy, now):
            continue
        core.set_state(conn, row["event_id"], core.STATE_QUIET, now, expect_state=core.STATE_NEEDS_DECISION,
                       note="event recovered on its own; nothing left to decide")
    conn.commit()


def _quiet_resolve_hours(policy: dict[str, Any]) -> float:
    return float(policy.get("quietResolveHours") or core.DEFAULT_QUIET_RESOLVE_HOURS)


def _recurrence_count(conn: sqlite3.Connection, event_id: int, now: dt.datetime,
                      window_days: float) -> int:
    """How often this row reopened (terminal -> `new`, reopen_if_needed()'s one transition) inside
    the window. `occurrences` cannot answer this: it is the LAST poll's batch count, so a signature
    that fires once per day for a week reads `1` every time."""
    since = (now - dt.timedelta(days=window_days)).isoformat()
    return conn.execute(
        "SELECT COUNT(*) FROM item_transitions WHERE event_id=? AND to_state=? "
        "AND from_state IN (?, ?, ?) AND at >= ?",
        (event_id, core.STATE_NEW, core.STATE_FIXED, core.STATE_QUIET, core.STATE_CLOSED, since),
    ).fetchone()[0]


def chronic_policy(policy: dict[str, Any]) -> tuple[float, int]:
    return (float(policy.get("chronicWindowDays") or core.DEFAULT_CHRONIC_WINDOW_DAYS),
            int(policy.get("chronicRecurrences") or core.DEFAULT_CHRONIC_RECURRENCES))


def chronic_recurrences(conn: sqlite3.Connection, event_id: int, repo: str | None,
                         policy: dict[str, Any], now: dt.datetime) -> int:
    """The reopen count when this row is chronic, else 0 (see DEFAULT_CHRONIC_RECURRENCES). Unmapped
    rows are never chronic: nothing could escalate them, so holding them out of `quiet` would only
    park them in `new`.

    Nor is a row already investigated inside the window: one episode per chronic signature per
    window. Otherwise a signature whose fix is parked elsewhere (a draft PR, an owner decision)
    would re-dispatch every cooldownHours for as long as it keeps flapping. Inside that window the
    row silence-resolves as usual."""
    if repo is None:
        return 0
    window, threshold = chronic_policy(policy)
    since = (now - dt.timedelta(days=window)).isoformat()
    investigated = conn.execute(
        "SELECT 1 FROM triage_items ti JOIN dispatches d ON d.job_id = ti.dispatch_job "
        "WHERE ti.event_id=? AND d.created_at >= ?", (event_id, since),
    ).fetchone()
    if investigated:
        return 0
    n = _recurrence_count(conn, event_id, now, window)
    return n if n >= threshold else 0


def _is_chronic(conn: sqlite3.Connection, event_id: int, repo: str | None,
                policy: dict[str, Any], now: dt.datetime) -> bool:
    return chronic_recurrences(conn, event_id, repo, policy, now) > 0


def _slack_ts_to_dt(value: Any) -> dt.datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return dt.datetime.fromtimestamp(float(value), tz=dt.timezone.utc)
    except (ValueError, OverflowError, OSError):
        return None


def _quiet_anchor(row: sqlite3.Row) -> tuple[dt.datetime | None, str | None]:
    """The last time a grouped event was actually observed. The ISO clocks
    (`last_reminder_at`/`notified_at`/`first_seen`) move only when watchdog-poll.py's
    upsert_grouped() EMITS; a cooldown-suppressed occurrence moves `payload_json.ts_last` alone (see
    occurrence_mark()). Reading only the ISO clocks would reopen a row on the fresh ts_last and
    quiet-resolve it again in the same pass against a days-old time. Returns (anchor, raw value for
    the note)."""
    candidates = [(t, r) for r in (row["last_reminder_at"], row["notified_at"], row["first_seen"])
                  if r and (t := core.parse_ts(r)) is not None]
    ts_last = _slack_ts_to_dt(core.safe_json(row["payload_json"]).get("ts_last"))
    if ts_last is not None:
        candidates.append((ts_last, ts_last.isoformat()))
    if not candidates:
        return None, None
    return max(candidates, key=lambda c: c[0])


# The only state a SILENCE path may resolve (DESIGN.md § "The quiet rule, corrected": observation
# status and remediation obligation are different facts). All three silence paths
# (apply_resolutions(), resolve_recovery_paired(), resolve_quiet_grouped()) resolve an item
# because its signal STOPPED BEING OBSERVED, and observation ending is not a discharge: `new` is
# the one state carrying no obligation yet, which is why silence may cancel it. An item in
# `needs_decision` whose intermittent fault cleared on its own must not go terminal and abandon
# the written fix.
#
# It is an INCLUSION list of one, and that is the load-bearing part: every state past `new`
# carries an obligation. An exclusion list silently ADMITS every state added after it was
# written; an inclusion list silently EXCLUDES them. This must fail closed.
_SILENCE_RESOLVE_ELIGIBLE_STATES = (core.STATE_NEW,)


def resolve_recovery_paired(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime,
                             *, dry_run: bool) -> None:
    """The stronger of the two grouped-source resolve paths (resolve_quiet_grouped() is the
    fallback): a `✅ <same alert text>` recovery message is a positive signal that the paired `🚨`
    alert cleared, better than waiting out a quiet window. A service that is fully DOWN also stops
    emitting, so silence alone cannot tell the two apart; an explicit recovery message can.

    It is cheap because HyperDX's webhook posts the IDENTICAL alert text with only the leading glyph
    flipped, and normalize_title() strips every non-alnum character, so the `✅` message normalizes
    to the SAME string as its `🚨` counterpart. For a grouped source that string already IS the
    event's stable `external_id`, so no text-matching scheme is needed: one fresh #alerts fetch, one
    dict lookup per candidate.

    This cannot be read back out of `events`/`payload_json`: upsert_grouped() retains one title and
    no per-occurrence history, so a ✅ landing in the same 30-min batch as its 🚨 is folded into a
    bumped `batch_count` with no trace of the glyph. Reading live #alerts is what makes it possible,
    and it stays cheap: ONE fetch per triage run (poll_slack_messages's single call), shared across
    every open slack_alert candidate via one dict.

    A recovery message is still only an OBSERVATION that the alert cleared, so like every other
    silence path this one only touches a row still in `new` (see
    _SILENCE_RESOLVE_ELIGIBLE_STATES). A pending human decision, an open dispatch, an in-flight
    merge are not discharged by the thing that raised them going away.

    Resolves to STATE_QUIET, NOT STATE_FIXED. A ✅ is a positive signal, so `fixed` looks right; it
    is not. DESIGN.md § What must not be lost, item 4: "Recovery-pairing is the strong path, the 2h
    timer the fallback, and NEITHER EVER CLAIMS A FIX." Nothing shipped by this function's
    knowledge: the service recovered, by our hand or its own, and a ✅ cannot tell the two apart any
    more than silence can. `fixed` is reserved for maybe_verify()'s positive branch, the one place a
    POSITIVE PROBE confirms a change that shipped (see STATE_QUIET)."""
    if dry_run:
        # --dry-run makes NO outbound call, Slack reads included, so a preview against a throwaway DB
        # copy never depends on live credentials or network.
        placeholders = ",".join("?" * len(_SILENCE_RESOLVE_ELIGIBLE_STATES))
        candidates = conn.execute(
            f"SELECT ti.event_id FROM triage_items ti JOIN events e ON e.id = ti.event_id "
            f"WHERE e.source='slack_alert' AND e.resolved_at IS NULL AND ti.state IN ({placeholders})",
            _SILENCE_RESOLVE_ELIGIBLE_STATES,
        ).fetchall()
        if candidates:
            print(f"[dry-run] would check {len(candidates)} open slack_alert item(s) against live "
                  f"#alerts for a ✅ recovery pairing (skipped under --dry-run)")
        return

    wp = core.wp_module()
    if wp is None:
        return
    token = wp.resolve_secret("HOMELAB_API_KEY")
    if not token:
        return
    msgs, _latest, ok = wp.poll_slack_messages({"HOMELAB_API_KEY": token}, core.ALERTS_CHANNEL, None,
                                                skip_webhook_fed=True)
    if not ok or not msgs:
        return
    latest_by_key: dict[str, tuple[str, str]] = {}
    for m in msgs:
        text = (m.get("payload") or {}).get("text") or m.get("title") or ""
        key = core.fingerprint(text)
        if not key:
            continue
        ts = m.get("external_id") or "0"
        prev = latest_by_key.get(key)
        if prev is None or ts > prev[0]:
            latest_by_key[key] = (ts, text)

    placeholders = ",".join("?" * len(_SILENCE_RESOLVE_ELIGIBLE_STATES))
    rows = conn.execute(
        f"SELECT ti.event_id, ti.repo, e.external_id FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        f"WHERE e.source='slack_alert' AND e.resolved_at IS NULL AND ti.state IN ({placeholders})",
        _SILENCE_RESOLVE_ELIGIBLE_STATES,
    ).fetchall()
    for row in rows:
        match = latest_by_key.get(row["external_id"])
        if match is None:
            continue
        if _is_chronic(conn, row["event_id"], row["repo"], policy, now):
            continue
        _ts, text = match
        if not text.lstrip().startswith("✅"):
            continue
        note = f"{core.RECOVERY_PAIRED_NOTE_PREFIX}{text.strip()[:200]}"
        # STATE_QUIET, never STATE_FIXED: see the docstring.
        core.set_state(conn, row["event_id"], core.STATE_QUIET, now, note=note)
    conn.commit()


def resolve_quiet_grouped(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime) -> None:
    """The fallback half of grouped-source resolution (resolve_recovery_paired() is the stronger
    positive-signal path). GROUPED_TRIAGE_SOURCES (slack_alert, hermes_log) never
    disappearance-resolve on their own event lifecycle: watchdog-poll.py's sweep_stale_grouped()
    only clears them after 7 idle DAYS, as housekeeping rather than signal. This is the triage-side
    fix: a row whose event has produced no new occurrence in `quietResolveHours`
    (DEFAULT_QUIET_RESOLVE_HOURS) flips to `quiet`, a purely local transition that never touches
    `events.resolved_at` (that column stays owned by watchdog-poll.py).

    No new column tracks "quiet since": `events.last_reminder_at` / `notified_at` / `first_seen`
    (the idle anchor sweep_stale_grouped() uses) only ADVANCE when watchdog-poll.py re-stamps the
    row on a fresh occurrence (upsert_grouped()), so a value that has stopped changing already IS
    the quiet duration.

    Only a row still in `new` is eligible (see _SILENCE_RESOLVE_ELIGIBLE_STATES): every state past
    `new` carries an obligation, and a quiet timer is an observation about the signal, never a
    discharge of it. Such a row exits through its own transition, not through silence.

    Never claims a fix: the note only says "signal quiet since <time>"
    (QUIET_RESOLVE_NOTE_PREFIX); a service that is fully down also stops emitting. Pure local
    bookkeeping (no Slack, no dispatch), so like apply_resolutions()/classify() it runs for real
    even under --dry-run; only the eventual notification respects `dry_run`."""
    quiet_hours = _quiet_resolve_hours(policy)
    placeholders_sources = ",".join("?" * len(core.GROUPED_TRIAGE_SOURCES))
    placeholders_states = ",".join("?" * len(_SILENCE_RESOLVE_ELIGIBLE_STATES))
    rows = conn.execute(
        f"SELECT ti.event_id, ti.repo, e.last_reminder_at, e.notified_at, e.first_seen, e.payload_json "
        f"FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        f"WHERE e.source IN ({placeholders_sources}) AND e.resolved_at IS NULL "
        f"AND ti.state IN ({placeholders_states})",
        (*core.GROUPED_TRIAGE_SOURCES, *_SILENCE_RESOLVE_ELIGIBLE_STATES),
    ).fetchall()
    for row in rows:
        quiet_since, anchor_raw = _quiet_anchor(row)
        if quiet_since is None:
            continue
        if (now - quiet_since).total_seconds() < quiet_hours * 3600:
            continue
        if _is_chronic(conn, row["event_id"], row["repo"], policy, now):
            continue
        note = (f"{core.QUIET_RESOLVE_NOTE_PREFIX}{core.fmt_ts(anchor_raw)} — no new occurrence for "
                f"{quiet_hours:g}h. This closes the item on silence alone; it is NOT a confirmed "
                f"fix, and the signature reopens automatically the moment it recurs.")
        core.set_state(conn, row["event_id"], core.STATE_QUIET, now, note=note)
    conn.commit()


def label_route(item: sqlite3.Row, event: sqlite3.Row, policy: dict[str, Any] | None = None) -> str | None:
    """The repo the signal's own label names, or None. Native labels first (intake.route_by_label():
    the repo an issue or `warden run` carries, a Kuma tag, a container name, an OTel
    `service.name`), then the policy's `rules`: a rule match is exactly a label route, the
    deterministic tier that goes away when the repos' `## Verify & Monitor` sections cover the
    fleet."""
    repo = _intake.route_by_label(item, event)
    if repo:
        return repo
    rule = core.match_rule(core.match_targets(event), (policy or core.load_policy()).get("rules") or [])
    return rule["repo"] if rule else None


def classify(conn: sqlite3.Connection, policy: dict[str, Any], now: dt.datetime) -> None:
    """The cheap pre-filters in front of the triage step; a row they close never gets a model call.
    Only touches an alert still in `new`, in this order:

    1. the explicit `ignore` list (genuine recoveries / known-benign patterns, matched against both
       match targets) -> `closed(ignored)`;
    2. with `ignoreUnstructuredSlackProse`, a `slack_alert` with no label route whose title does not
       start with a recognized bot-alert shape (`looks_like_bot_alert`) is chat prose that
       watchdog-poll.py ingested from #alerts, not an alert -> `closed(ignored)` with a note.

    **The prose filter runs after the label route, and that order is load-bearing.** It is a prefix
    test on the title, and a producer that emits bare sentences (Beszel's `HomeLab CPU above
    threshold`) fails it on every occurrence, however real the alert. A row that already carries a
    repo is never the filter's to close.

    Routing itself is not decided here: label_route() and the triage job do that (see
    submit_triage_jobs())."""
    rows = conn.execute(
        "SELECT event_id, repo FROM triage_items WHERE state=? AND origin='alert' AND triage_job IS NULL",
        (core.STATE_NEW,),
    ).fetchall()
    for row in rows:
        event_row = core.get_event(conn, row["event_id"])
        item = core.get_item(conn, row["event_id"])
        if event_row is None or item is None:
            continue
        if core.fnmatch_any(core.match_targets(event_row), policy.get("ignore") or []):
            core.set_state(conn, row["event_id"], core.STATE_CLOSED, now, expect_state=core.STATE_NEW,
                            close_reason=core.CLOSE_IGNORED, note="matched the policy ignore list")
            continue
        if (policy["ignoreUnstructuredSlackProse"] and row["repo"] is None
                and event_row["source"] == "slack_alert" and not core.looks_like_bot_alert(event_row["title"])
                and label_route(item, event_row, policy) is None):
            core.set_state(conn, row["event_id"], core.STATE_CLOSED, now, expect_state=core.STATE_NEW,
                            close_reason=core.CLOSE_IGNORED, note="unstructured #alerts prose, not a bot alert")
    conn.commit()
