"""merge — the Python port of the retired bash CLI's `cmd_merge` (1896-2131) and
`merge_gate_check` (1731-1769).

Land is the one door in this whole estate that changes what runs on a
default branch, so it re-checks the pull request's CURRENT state — a stale
record is a refusal, never a trust. The gate itself is small: PR open, checks
green (or none), review `confirmed`; GitHub's own rules answer at the merge call.

`plan_or_land()` is confirm-gated: a plan (no `--confirm`) changes nothing.

Landing does not deploy: the verify pass (loop/verify.py) runs `make deploy` through
lifecycle/rollout.py once the item is `verifying`.
"""

from __future__ import annotations

import datetime as dt
import json
import sqlite3
import sys
import time
from dataclasses import dataclass
from typing import Any, Callable

from clients import github, agent_gateway
from clients.errors import (
    CheckRunsUnreadable, HeadMoved, PolicyError, PreconditionError, RemoteError, UsageError,
)
from lifecycle import operations


class ChecksPending(PolicyError):
    """CI on the head commit is still running — nothing has failed, so the caller
    waits and asks again rather than ending the item."""


class ChecksFailed(PolicyError):
    """A check run on the head commit completed and did not pass. A refusal to merge;
    the merge train reads it as a finding for the implementer (a `checks_failed`
    revision)."""


class MergeInFlight(PolicyError):
    """Another caller is already landing this very pull request."""


class AlreadyMerged(PolicyError):
    """GitHub says the dispatch's own pull request is merged while the ledger never recorded
    it (a merge call whose answer was lost, or a merge by hand): the change landed. The merge
    train lands its item from here instead of failing it; `merge_commit` is GitHub's and
    `head_sha` is the PR head that merged — the caller asks whether a review confirmed THAT head."""

    def __init__(self, message: str, *, merge_commit: str | None, head_sha: str | None = None):
        super().__init__(message)
        self.merge_commit = merge_commit
        self.head_sha = head_sha

def _nested(d: dict[str, Any], path: str) -> Any:
    cur: Any = d
    for part in path.split("."):
        if not isinstance(cur, dict) or part not in cur:
            return None
        cur = cur[part]
    return cur


@dataclass
class MergePlan:
    """The dry-run / unconfirmed result — nothing was merged and nothing
    was un-drafted. `to_json()` is the bash `emit_merge_plan` dict minus
    the `verb`/`ok` keys, which belong to a CLI layer this module does not
    own."""

    needs_confirm: bool
    repo: str
    pull_request: int
    title: str
    head: str
    base: str
    merge_method: str
    changed_files: int
    changed_lines: int

    def to_json(self) -> dict[str, Any]:
        note = "nothing was merged and nothing was un-drafted"
        if self.needs_confirm:
            note += (
                ". Re-invoke with --confirm to land it. This is the last point at "
                "which nothing has changed on GitHub."
            )
        return {
            "dryRun": True,
            "needsConfirm": self.needs_confirm,
            "repo": self.repo,
            "pullRequest": self.pull_request,
            "title": self.title,
            "head": self.head,
            "base": self.base,
            "mergeMethod": self.merge_method,
            "changedFiles": self.changed_files,
            "changedLines": self.changed_lines,
            "wouldDo": [
                "mark the draft pull request ready for review",
                f"merge it into {self.base} by {self.merge_method}",
                "delete the dispatch/… branch",
            ],
            "wouldNeverDo": [
                "never merge a pull request this bridge did not open",
                "never merge a fork branch or a retargeted base",
                "never override a failing check or a branch protection rule",
            ],
            "note": note,
        }


@dataclass
class MergeResult:
    """The landed result. `to_json()` is the bash `emit_merged` dict minus
    `verb`/`ok`. `merge_op_id` is the operations-ledger audit id — new in this
    port, not part of the external contract, so it is not serialised."""

    merged: bool
    repo_slug: str
    pull_request: int
    title: str
    merge_method: str
    merge_commit: str | None
    branch: str
    branch_deleted: bool
    merge_op_id: str

    def to_json(self) -> dict[str, Any]:
        return {
            "merged": self.merged,
            "repo": self.repo_slug,
            "pullRequest": self.pull_request,
            "title": self.title,
            "mergeMethod": self.merge_method,
            "mergeCommit": self.merge_commit,
            "branch": self.branch,
            "branchDeleted": self.branch_deleted,
            "note": (
                "This is on the default branch now. Say so plainly, with the "
                "pull request link — a merge is the one outcome here that a human "
                "cannot discover later by reading an open PR list."
            ),
        }


# --- the merge gate: PR open, checks green, review confirmed ------------------
#
# That is all warden decides. GitHub's own rules (branch protection, rulesets,
# required reviews and checks) are enforced by the merge API call itself and a
# refusal there is reported (`github.merge_pr()` -> `PolicyError`), never
# pre-empted by a local copy of the rule. No path scope, size ceiling, CI-file
# rule or per-repo carve-out exists any more.


def read_check_runs(owner: str, repo: str, sha: str) -> list[dict[str, Any]]:
    """Every check run on `sha`, mapped to the shape the gate consumes.

    A 403 (a fine-grained PAT without `Checks: read` on a private repository, §110:
    `weatherorb`) falls back to the commit's GitHub Actions workflow runs, mapped to
    the same shape — the same credential can often read those. Unreadable is still not
    "none exist": if the fallback is unreadable too, the unknown CI state refuses with
    the fallback's own `RemoteError`, and an EMPTY fallback is `ChecksPending` — only a
    readable check-runs API may conclude that no CI exists, so the green-or-none gate is
    never passed on an unknown state (workflows may simply not have registered yet)."""
    try:
        return github.check_runs(owner, repo, sha)
    except CheckRunsUnreadable as e:
        runs = github.workflow_runs(owner, repo, sha)
        print(
            f"merge: check runs unreadable on {owner}/{repo}@{sha[:12]} ({e}); gating on "
            f"{len(runs)} GitHub Actions workflow run(s) instead",
            file=sys.stderr,
        )
        if not runs:
            raise ChecksPending(
                f"check runs on {owner}/{repo}@{sha[:12]} are unreadable and the commit has no GitHub "
                f"Actions runs to gate on — the CI state is unknown, not absent."
            )
        return runs


def check_runs_gate(*, repo: str, check_runs: list[dict[str, Any]]) -> None:
    """Green — every run completed success/neutral/skipped, or none exist — returns.
    Still running and nothing failed: `ChecksPending`. Any completed run that did not
    pass: `ChecksFailed`."""
    bad = [
        r for r in check_runs
        if r.get("status") != "completed"
        or r.get("conclusion") not in ("success", "neutral", "skipped")
    ]
    if bad:
        names = ", ".join(str(r.get("name")) for r in bad)
        # Decided on the very runs that are bad, never by re-filtering on name: two
        # runs can share a name (a re-run), and a completed-green one must not
        # make a still-running one read as failed.
        if all(r.get("status") != "completed" for r in bad):
            # Every non-green run still running (queued/in progress), none failed:
            # that is waiting, not a refusal.
            raise ChecksPending(f"{repo}'s CI is still running on the head commit: {names}.")
        raise ChecksFailed(f"{repo}'s CI has not passed cleanly on the head commit: {names}.")


def merge_gate_check(*, repo: str, check_runs: list[dict[str, Any]], validation: str | None) -> None:
    """Checks green — or none exist, which passes — and the step-7 review
    `confirmed`. "PR open" is read off the pull request by `plan_or_land()`."""
    check_runs_gate(repo=repo, check_runs=check_runs)
    if validation != "confirmed":
        raise PolicyError(
            f"the step-7 validation has not confirmed (status: {validation or '(none)'}). "
            f"A missing, disagreeing, or errored validation blocks the merge."
        )


def plan_or_land(
    conn: sqlite3.Connection, *, job_id: str, why: str, confirm: bool, dry_run: bool,
    authorized_by: str, now: dt.datetime, sleep: Callable[[float], None] = time.sleep,
    expected_sha: str | None = None, require_pin: bool = False,
) -> MergePlan | MergeResult:
    """Inspect, gate and (confirmed) land one dispatch's pull request, squash first
    (`github.pick_merge_method()` falls back to rebase/merge only where the repo's rules
    allow nothing else). The merge call pins the head SHA every check was made against.
    `expected_sha` is the merge train's SHA: a PR whose head is anything else is refused
    with `HeadMoved` before any gate runs; None pins whatever the head is now — unless
    `require_pin` (the owner's `warden merge`): `validation_status` says a review confirmed, not
    of which head, so with no reviewed head to pin the merge is refused."""
    if not why:
        raise UsageError(
            'merge requires --why "<reason>". It is the audit record of why an '
            "unattended episode was allowed to land code on a default branch. "
            "There is no default."
        )
    if not agent_gateway.valid_job_id(job_id):
        raise UsageError(f"{job_id!r} is not a valid job id")

    row = conn.execute(
        "SELECT tier, repo, status, artifact_url, merged_at, validation_status, origin_event_id "
        "FROM dispatches WHERE job_id=?",
        (job_id,),
    ).fetchone()
    if row is None:
        raise PreconditionError(
            f"no dispatch recorded with job id {job_id}. This verb merges only a pull "
            f"request this bridge opened, so a job it has no record of is not mergeable by it."
        )
    tier, repo, status, artifact, merged_at, validation_status, origin_event_id = (
        row["tier"], row["repo"], row["status"], row["artifact_url"],
        row["merged_at"], row["validation_status"], row["origin_event_id"],
    )

    if merged_at:
        raise PolicyError(
            f"dispatch {job_id} was already merged at {merged_at}. Re-merging is not a "
            f"retry — if something is wrong with what landed, that is a new change, not "
            f"a second merge."
        )
    if tier != "implement":
        raise PolicyError(
            f"dispatch {job_id} ran at tier '{tier}', which produces no pull request. "
            f"Only an implement episode can be merged."
        )
    if status != "done":
        raise PolicyError(
            f"dispatch {job_id} finished as '{status}', not 'done'. A merge follows a "
            f"successful episode, never a failed or still-running one."
        )
    if not artifact:
        raise PolicyError(
            f"dispatch {job_id} recorded no artifact URL — the episode pushed a branch "
            f"but never opened a pull request."
        )
    if require_pin and expected_sha is None:
        raise PolicyError(
            f"no reviewed head on record for dispatch {job_id} — review the current head first "
            f"(its item's merge train reviews it; Argo's merge puts the item back on the train)."
        )

    parsed = github.parse_pr_url(artifact)
    if parsed is None:
        raise PolicyError(
            f"dispatch {job_id} recorded an artifact that is not a pull request URL: {artifact}"
        )
    owner, url_repo, pr_number = parsed
    if owner != github.GH_OWNER:
        raise PolicyError(
            f"the recorded pull request belongs to '{owner}', not {github.GH_OWNER}. This "
            f"bridge dispatches only into Johannes's own repos; a foreign owner means a "
            f"corrupted record."
        )
    if url_repo != repo:
        raise PolicyError(
            f"the dispatch record disagrees with itself: repo '{repo}' but a pull "
            f"request in '{url_repo}'."
        )

    pr = github.read_pr(owner, repo, pr_number)
    repo_json = github.read_repo(owner, repo)

    pr_state = pr.get("state")
    if pr_state is None:
        raise RemoteError(f"GitHub's pull request response for {owner}/{repo}#{pr_number} has no state")
    pr_merged = pr.get("merged", False)
    pr_base = _nested(pr, "base.ref")
    if pr_base is None:
        raise RemoteError(f"GitHub's pull request response for {owner}/{repo}#{pr_number} has no base ref")
    pr_head = _nested(pr, "head.ref")
    if pr_head is None:
        raise RemoteError(f"GitHub's pull request response for {owner}/{repo}#{pr_number} has no head ref")
    pr_head_sha = _nested(pr, "head.sha")
    if pr_head_sha is None:
        raise RemoteError(f"GitHub's pull request response for {owner}/{repo}#{pr_number} has no head sha")
    pr_head_repo = _nested(pr, "head.repo.full_name") or ""
    pr_files_n = pr.get("changed_files") or 0
    pr_add = pr.get("additions") or 0
    pr_del = pr.get("deletions") or 0
    pr_title = pr.get("title") or "(untitled)"
    node_id = pr.get("node_id")
    if node_id is None:
        raise RemoteError(f"GitHub's pull request response for {owner}/{repo}#{pr_number} has no node id")
    default_branch = repo_json.get("default_branch")
    if default_branch is None:
        raise RemoteError(f"GitHub's repo response for {owner}/{repo} has no default branch")

    # The rules are read only to pick a merge method GitHub will accept
    # (`pick_merge_method`). Whether they ALLOW this merge is GitHub's answer
    # to the merge call, reported as-is — never pre-empted here (§97).
    rules = github.branch_rules(owner, repo, default_branch)

    # The guards that say whether this is the pull request this bridge inspected come BEFORE the
    # merged check: a merged PR that is retargeted, from a fork or off a non-dispatch branch is not
    # the dispatch's own, and "already merged" must never vouch for it.
    if pr_base != default_branch:
        raise PolicyError(
            f"{owner}/{repo}#{pr_number} targets '{pr_base}', not the default branch "
            f"'{default_branch}'. A dispatch PR that has been retargeted is not the "
            f"thing that was inspected."
        )
    if not pr_head.startswith("dispatch/"):
        raise PolicyError(
            f"{owner}/{repo}#{pr_number} merges '{pr_head}', which is not a dispatch/… "
            f"branch. This verb merges only branches this bridge cut."
        )
    if pr_head_repo != f"{owner}/{repo}":
        raise PolicyError(
            f"{owner}/{repo}#{pr_number} is from '{pr_head_repo}', a fork. A fork branch "
            f"was never inspected by the episode and is never merged here."
        )
    if pr_merged:
        # The change landed — but "landed" is only the caller's to accept when it is the head the
        # caller pinned (checked, reviewed): a PR merged at any other head is not what was inspected.
        if expected_sha is not None and pr_head_sha != expected_sha:
            raise PolicyError(
                f"{owner}/{repo}#{pr_number} is already merged, at head {pr_head_sha[:12]} — not the "
                f"{expected_sha[:12]} that was checked and reviewed. It was merged outside this "
                f"bridge, so the ledger does not take it as the reviewed change."
            )
        sha = pr.get("merge_commit_sha")
        raise AlreadyMerged(f"{owner}/{repo}#{pr_number} is already merged.",
                            merge_commit=sha if isinstance(sha, str) and sha else None,
                            head_sha=pr_head_sha)
    if pr_state != "open":
        raise PolicyError(f"{owner}/{repo}#{pr_number} is '{pr_state}', not open.")
    if expected_sha is not None and pr_head_sha != expected_sha:
        raise HeadMoved(
            f"{owner}/{repo}#{pr_number}'s head is {pr_head_sha[:12]}, not the {expected_sha[:12]} "
            f"that was checked and reviewed. Nothing was merged."
        )
    lines = pr_add + pr_del

    runs = read_check_runs(owner, repo, pr_head_sha)
    merge_gate_check(repo=repo, check_runs=runs, validation=validation_status)

    method = github.pick_merge_method(repo_json, rules)
    if method is None:
        raise PolicyError(
            f"{owner}/{repo} allows no merge method this verb can use (squash, rebase, "
            f"merge commit all disabled)."
        )

    slug = f"{owner}/{repo}"

    if not confirm or dry_run:
        return MergePlan(
            needs_confirm=not confirm,
            repo=slug, pull_request=pr_number, title=pr_title, head=pr_head, base=pr_base,
            merge_method=method, changed_files=pr_files_n, changed_lines=lines,
        )

    # --- land it -----------------------------------------------------------
    # The double `--confirm` race: two concurrent lands of the SAME job_id
    # would both have passed the `merged_at IS NULL` check from the row this
    # function read at the top, long before either write-locked anything.
    # BEGIN IMMEDIATE takes the write lock, re-reads `merged_at` fresh, and
    # checks for an already-open merge operation for this exact job — all
    # before either records one, so a second caller refuses instead of
    # merging the same pull request twice. The operation row itself still
    # commits BEFORE the external call it covers — that commit is the whole
    # crash-recovery contract (DESIGN.md § Crash recovery).
    if conn.in_transaction:
        raise RuntimeError(
            "plan_or_land() was called with an already-open transaction on `conn` — "
            "BEGIN IMMEDIATE cannot nest; the caller must not hold one open across this call"
        )
    conn.execute("BEGIN IMMEDIATE")
    try:
        fresh = conn.execute("SELECT merged_at FROM dispatches WHERE job_id=?", (job_id,)).fetchone()
        if fresh is not None and fresh["merged_at"]:
            raise PolicyError(
                f"dispatch {job_id} was already merged at {fresh['merged_at']}. Re-merging is not a "
                f"retry — if something is wrong with what landed, that is a new change, not "
                f"a second merge."
            )
        in_flight = conn.execute(
            "SELECT 1 FROM operations WHERE kind='merge' AND outcome IS NULL AND note=?",
            (f"job:{job_id}",),
        ).fetchone()
        if in_flight is not None:
            raise MergeInFlight(f"a merge for job {job_id} is already in flight")
        merge_op = operations.record(
            conn, event_id=origin_event_id, kind="merge", repo=repo, authorized_by=authorized_by,
            note=f"job:{job_id}", commit=False,
        )
    except PolicyError:
        conn.rollback()
        raise
    conn.commit()
    # The method rides on the open operation, so a merge whose answer is lost (a crash, a
    # timeout) still says how it landed once reconcile_operations() resolves it — a revert
    # depends on it.
    operations.annotate(conn, merge_op, receipt=json.dumps({"mergeMethod": method}))

    # Ready-for-review first: a draft cannot be merged. Done AFTER every
    # check above, so a PR that fails one is never un-drafted as a side
    # effect of being refused.
    try:
        github.mark_ready_for_review(node_id)
    except RemoteError:
        operations.complete(conn, merge_op, outcome="failed")
        raise

    # mergeable is computed asynchronously and is null right after a
    # mutation, so a single read would be a coin flip. Re-read until GitHub
    # has an answer.
    tries = 0
    mergeable: Any = None
    state_now: Any = None
    while True:
        pr = github.read_pr(owner, repo, pr_number)
        mergeable = pr.get("mergeable")
        state_now = pr.get("mergeable_state")
        if mergeable is not None and state_now != "unknown":
            break
        tries += 1
        if tries >= 5:
            operations.complete(conn, merge_op, outcome="failed")
            raise RemoteError(
                f"GitHub never finished computing mergeability for {owner}/{repo}#{pr_number}. "
                f"Nothing was merged; the pull request is now marked ready for review."
            )
        sleep(2)

    if mergeable is not True:
        operations.complete(conn, merge_op, outcome="failed")
        raise PolicyError(
            f"{owner}/{repo}#{pr_number} is not mergeable (state: {state_now}) — usually "
            f"a conflict with {default_branch}."
        )
    # The head SHA is pinned: every check above was made against it, so a
    # push that lands between the inspection and this call must fail the
    # merge, not ride it.
    try:
        resp = github.merge_pr(owner, repo, pr_number, sha=pr_head_sha, method=method)
    except PolicyError:
        operations.complete(conn, merge_op, outcome="failed")
        raise
    except RemoteError as e:
        operations.complete(conn, merge_op, outcome="unknown",
                            receipt=json.dumps({"error": str(e), "mergeMethod": method}))
        raise
    merge_sha = resp.get("sha")

    # The record is stamped before the branch delete, which is cleanup and
    # is allowed to fail: a merged commit with a leftover branch is untidy,
    # a merge this table does not know about is a second merge waiting to
    # happen.
    try:
        conn.execute(
            "UPDATE dispatches SET merged_at=? WHERE job_id=?",
            (dt.datetime.now(dt.timezone.utc).isoformat(), job_id),
        )
        conn.commit()
    except sqlite3.Error as e:
        raise PreconditionError(
            f"MERGED {owner}/{repo}#{pr_number} but could not stamp the dispatch record: {e}"
        )

    try:
        deleted = github.delete_branch(owner, repo, pr_head)
    except RemoteError:
        # Cleanup, allowed to fail — see the comment above: a merged commit
        # with a leftover branch is untidy, not a defect worth raising out
        # of an otherwise-successful merge.
        deleted = False

    operations.complete(conn, merge_op, outcome="done", receipt=json.dumps({
        "pullRequest": pr_number, "mergeCommit": merge_sha, "branch": pr_head,
        "branchDeleted": deleted, "mergeMethod": method, "title": pr_title,
    }))

    return MergeResult(
        merged=True, repo_slug=slug, pull_request=pr_number, title=pr_title,
        merge_method=method, merge_commit=merge_sha, branch=pr_head, branch_deleted=deleted,
        merge_op_id=merge_op,
    )
