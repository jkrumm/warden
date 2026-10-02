#!/usr/bin/env python3
"""Regression suite for scripts/lifecycle/merge.py — the Python port of
the retired bash CLI's `cmd_merge`, `merge_gate_check`, `collect_expected_alerts` and
`run_deploy_if_enabled`.

Every `clients.github`/`clients.rollout` call and every `lifecycle.policy`
call is faked by assignment for the duration of a `with fakes(...):` block —
this suite never touches the network and never depends on the sibling
worker's real `policy.py` landing first. If the real module is not on disk
yet, a throwaway stand-in is registered in `sys.modules["lifecycle.policy"]`
so `merge.py`'s own `from lifecycle import policy` still resolves; every
function on it is then overridden per test regardless.

Run: .venv/bin/python3 tests/test_merge.py
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import sqlite3
import sys
import tempfile
import traceback
import types
from contextlib import contextmanager
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import lifecycle  # noqa: E402 — the package; used to patch in a stand-in policy module if absent

try:
    import lifecycle.policy  # noqa: F401, E402
except ModuleNotFoundError:
    _stub = types.ModuleType("lifecycle.policy")
    for _name in ("triage_repo_entry", "triage_policy_path"):
        def _unset(*_a, _n=_name, **_k):
            raise NotImplementedError(f"lifecycle.policy.{_n} stub called without a test override")
        setattr(_stub, _name, _unset)
    sys.modules["lifecycle.policy"] = _stub
    lifecycle.policy = _stub

from lifecycle import merge, operations  # noqa: E402
from clients import github, rollout  # noqa: E402
from clients.errors import PolicyError, PreconditionError, RemoteError, UsageError  # noqa: E402

_ledger_spec = importlib.util.spec_from_file_location("ledger", REPO / "scripts" / "ledger.py")
ledger = importlib.util.module_from_spec(_ledger_spec)
_ledger_spec.loader.exec_module(ledger)


# --- fixtures -----------------------------------------------------------------

JOB_ID = "merge-job-0001"
PR_URL = "https://github.com/jkrumm/gamma/pull/7"
_NOW = dt.datetime(2026, 9, 10, 12, 0, 0, tzinfo=dt.timezone.utc)
_FULL_SHA = "a" * 40


def _no_sleep(_seconds: float) -> None:
    return None


def _now_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).isoformat()


def _tmp_dir(prefix: str) -> Path:
    return Path(tempfile.mkdtemp(prefix=prefix))


def _fresh_ledger():
    return ledger.connect(_tmp_dir("merge-db-") / "warden.db", migrate=True)


def _seed_pr_dispatch(conn, *, job_id=JOB_ID, tier="implement", repo="gamma", status="done",
                       artifact=PR_URL, validation_status="confirmed", merged_at=None,
                       origin_event_id=None) -> None:
    conn.execute(
        "INSERT INTO dispatches(job_id,tier,repo,brief,status,artifact_url,created_at,"
        "validation_status,merged_at,origin_event_id) VALUES(?,?,?,?,?,?,?,?,?,?)",
        (job_id, tier, repo, "seed brief", status, artifact, _now_iso(), validation_status,
         merged_at, origin_event_id),
    )
    conn.commit()


def _merged_at(conn, job_id=JOB_ID):
    row = conn.execute("SELECT merged_at FROM dispatches WHERE job_id=?", (job_id,)).fetchone()
    return row["merged_at"] if row else None


def _op_row(conn, *, kind="merge"):
    return conn.execute(
        "SELECT * FROM operations WHERE kind=? ORDER BY started_at DESC LIMIT 1", (kind,)
    ).fetchone()


def _op_count(conn, *, kind="merge") -> int:
    return conn.execute("SELECT COUNT(*) FROM operations WHERE kind=?", (kind,)).fetchone()[0]


_DEFAULT_PR = {
    "state": "open", "merged": False, "base": {"ref": "master"},
    "head": {"ref": "dispatch/stub-branch", "sha": "deadbeef" * 5,
             "repo": {"full_name": "jkrumm/gamma"}},
    "changed_files": 2, "additions": 4, "deletions": 4,
    "mergeable": True, "mergeable_state": "clean",
    "node_id": "PR_kwstub", "title": "stub pr title",
}
_DEFAULT_REPO = {
    "default_branch": "master",
    "allow_squash_merge": True, "allow_rebase_merge": True, "allow_merge_commit": True,
}


@contextmanager
def fakes(**overrides):
    """Patches every clients.github / clients.rollout / lifecycle.policy
    function `merge.py` calls, for the duration of the block, restoring the
    originals on exit — real or stub `policy` module, either way."""
    calls: dict[str, list] = {}
    state = {
        "pr": dict(_DEFAULT_PR),
        "repo": dict(_DEFAULT_REPO),
        "files": [],
        "check_runs": [{"name": "build", "status": "completed", "conclusion": "success"}],
        "check_runs_error": None,
        "merge_resp": {"sha": "mergedsha001"},
        "delete_ok": True,
        "contents": {},
        "actions_runs": [],
        "actions_runs_error": None,
        "entry": {},
        "branch_rules": [],
        "mark_ready_error": None,
        "mark_ready_fn": None,
        "merge_pr_error": None,
        "rollout_result": rollout.RolloutResult(True, 0, "ok"),
        "rollout_run_fn": None,
    }
    state.update(overrides)

    def record(name, *a, **kw):
        calls.setdefault(name, []).append((a, kw))

    def _read_pr(owner, repo, n):
        record("read_pr", owner, repo, n)
        return dict(state["pr"])

    def _read_repo(owner, repo):
        record("read_repo", owner, repo)
        return dict(state["repo"])

    def _pr_files(owner, repo, n):
        record("pr_files", owner, repo, n)
        return list(state["files"])

    def _check_runs(owner, repo, sha):
        record("check_runs", owner, repo, sha)
        if state["check_runs_error"]:
            raise state["check_runs_error"]
        return list(state["check_runs"])

    def _mark_ready(node_id):
        record("mark_ready_for_review", node_id)
        if state["mark_ready_fn"]:
            state["mark_ready_fn"](node_id)
        if state["mark_ready_error"]:
            raise state["mark_ready_error"]

    def _merge_pr(owner, repo, n, *, sha, method):
        record("merge_pr", owner, repo, n, sha=sha, method=method)
        if state["merge_pr_error"]:
            raise state["merge_pr_error"]
        return dict(state["merge_resp"])

    def _delete_branch(owner, repo, branch):
        record("delete_branch", owner, repo, branch)
        return state["delete_ok"]

    def _contents(owner, repo, path, *, ref):
        record("contents", owner, repo, path, ref=ref)
        return state["contents"].get(path)

    def _actions_runs(owner, repo, *, head_sha):
        record("actions_runs", owner, repo, head_sha=head_sha)
        if state["actions_runs_error"]:
            raise state["actions_runs_error"]
        return list(state["actions_runs"])

    def _rollout_run(key, *, timeout_s=180, runner=None):
        record("rollout_run", key, timeout_s)
        if state["rollout_run_fn"]:
            return state["rollout_run_fn"](key, timeout_s=timeout_s)
        return state["rollout_result"]

    def _branch_rules(owner, repo, branch):
        record("branch_rules", owner, repo, branch)
        return list(state["branch_rules"])

    def _triage_repo_entry(repo):
        record("triage_repo_entry", repo)
        return dict(state["entry"])

    def _triage_policy_path():
        return Path("/dev/null")

    patches = [
        (github, "read_pr", _read_pr),
        (github, "read_repo", _read_repo),
        (github, "pr_files", _pr_files),
        (github, "check_runs", _check_runs),
        (github, "mark_ready_for_review", _mark_ready),
        (github, "merge_pr", _merge_pr),
        (github, "delete_branch", _delete_branch),
        (github, "contents", _contents),
        (github, "actions_runs", _actions_runs),
        (rollout, "run", _rollout_run),
        (github, "branch_rules", _branch_rules),
        (lifecycle.policy, "triage_repo_entry", _triage_repo_entry),
        (lifecycle.policy, "triage_policy_path", _triage_policy_path),
    ]
    saved = [(m, n, getattr(m, n)) for m, n, _ in patches]
    for m, n, fn in patches:
        setattr(m, n, fn)
    try:
        yield types.SimpleNamespace(state=state, calls=calls)
    finally:
        for m, n, orig in saved:
            setattr(m, n, orig)


def _land(conn, *, job_id=JOB_ID, why="test", confirm=True, dry_run=False, authorized_by="U123"):
    return merge.plan_or_land(conn, job_id=job_id, why=why, confirm=confirm, dry_run=dry_run,
                               authorized_by=authorized_by, now=_NOW, sleep=_no_sleep)


def _assert_refuses(*, exc_type, msg, job_id=JOB_ID, seed=None, fake=None):
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn, **(seed or {}))
        with fakes(**(fake or {})) as fx:
            try:
                _land(conn, job_id=job_id)
            except exc_type as e:
                assert msg in str(e), f"{msg!r} not in {e!r}"
            else:
                raise AssertionError(f"expected {exc_type.__name__} containing {msg!r}")
            assert "mark_ready_for_review" not in fx.calls
            assert "merge_pr" not in fx.calls
    finally:
        conn.close()


def _assert_refuses_as(authorized_by, *, msg, seed=None, fake=None):
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn, **(seed or {}))
        with fakes(**(fake or {})) as fx:
            try:
                _land(conn, authorized_by=authorized_by)
            except PolicyError as e:
                assert msg in str(e), f"{msg!r} not in {e!r}"
            else:
                raise AssertionError(f"expected PolicyError containing {msg!r}")
            assert "merge_pr" not in fx.calls
    finally:
        conn.close()


# --- (a) the one-condition refusals, each inert -----------------------------

def test_unknown_job_id_refuses():
    _assert_refuses(exc_type=PreconditionError, msg="no dispatch recorded with job id",
                     job_id="some-other-job")


def test_read_only_tier_refuses():
    _assert_refuses(exc_type=PolicyError, msg="produces no pull request", seed={"tier": "investigate"})


def test_failed_episode_refuses():
    _assert_refuses(exc_type=PolicyError, msg="not 'done'", seed={"status": "failed"})


def test_no_artifact_refuses():
    _assert_refuses(exc_type=PolicyError, msg="no artifact URL", seed={"artifact": None})


def test_issue_url_not_pr_refuses():
    _assert_refuses(exc_type=PolicyError, msg="not a pull request URL",
                     seed={"artifact": "https://github.com/jkrumm/gamma/issues/7"})


def test_foreign_owner_refuses():
    _assert_refuses(exc_type=PolicyError, msg="belongs to",
                     seed={"artifact": "https://github.com/someone-else/gamma/pull/7"})


def test_record_disagrees_with_itself_refuses():
    _assert_refuses(exc_type=PolicyError, msg="disagrees with itself",
                     seed={"artifact": "https://github.com/jkrumm/alpha/pull/7"})


def test_github_ruleset_requiring_a_review_is_githubs_call_not_a_local_pre_check():
    """GitHub's rules are enforced by the merge call itself and a refusal there is
    reported as-is. warden no longer reads the ruleset to pre-empt it: the merge is
    attempted, GitHub's 405 comes back as a PolicyError and the operation is failed."""
    rules = [{"type": "pull_request", "parameters": {"required_approving_review_count": 1}}]
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        refusal = PolicyError("GitHub refused the merge (405): the pull request is not mergeable under "
                              "this repo's rules. Nothing was merged.")
        with fakes(branch_rules=rules, merge_pr_error=refusal) as fx:
            try:
                _land(conn)
            except PolicyError as e:
                assert "GitHub refused the merge (405)" in str(e), e
            else:
                raise AssertionError("expected GitHub's refusal to be reported")
        assert fx.calls.get("merge_pr"), "the merge call is the check — it must be made"
        assert _op_row(conn)["outcome"] == "failed"
        assert _merged_at(conn) is None
    finally:
        conn.close()


def test_github_ruleset_requiring_zero_reviews_is_not_a_human_gate():
    """rollhook#26 (§97): PR-required by a local list, zero approvals by the
    real ruleset — mergeable, not a question for a human."""
    rules = [{"type": "pull_request", "parameters": {"required_approving_review_count": 0}},
             {"type": "required_linear_history", "parameters": None}]
    conn = _fresh_ledger()
    _seed_pr_dispatch(conn, validation_status="confirmed")
    with fakes(branch_rules=rules, repo={**_DEFAULT_REPO, "allow_merge_commit": True}) as fx:
        result = _land(conn, why="w", authorized_by="auto-from-item")
    assert result.merged
    assert fx.calls["merge_pr"][0][1]["method"] != "merge", "linear history rules out a merge commit"


def test_unreadable_check_runs_refuse_the_merge():
    """§110 — `weatherorb` is private and a fine-grained PAT without `Checks: read`
    403s the check-runs read. Unreadable is not "none exist": an unknown CI state
    must not pass the green-or-none gate, so the merge is refused and the item
    takes the not-mergeable-yet path."""
    _assert_refuses(exc_type=PolicyError, msg="Checks: read",
                    seed={"validation_status": "confirmed"},
                    fake={"check_runs_error": github.CheckRunsUnreadable("GitHub returned HTTP 403 reading check-runs")})


def test_pull_request_closed_refuses():
    _assert_refuses(exc_type=PolicyError, msg="not open", fake={"pr": {**_DEFAULT_PR, "state": "closed"}})


def test_already_merged_on_github_refuses():
    _assert_refuses(exc_type=PolicyError, msg="already merged", fake={"pr": {**_DEFAULT_PR, "merged": True}})


def test_base_retargeted_refuses():
    _assert_refuses(exc_type=PolicyError, msg="not the default branch",
                     fake={"pr": {**_DEFAULT_PR, "base": {"ref": "release"}}})


def test_head_not_dispatch_branch_refuses():
    _assert_refuses(exc_type=PolicyError, msg="not a dispatch",
                     fake={"pr": {**_DEFAULT_PR, "head": {"ref": "feature/x", "sha": "a" * 40,
                                                            "repo": {"full_name": "jkrumm/gamma"}}}})


def test_head_is_fork_refuses():
    _assert_refuses(exc_type=PolicyError, msg="fork",
                     fake={"pr": {**_DEFAULT_PR, "head": {"ref": "dispatch/x", "sha": "a" * 40,
                                                            "repo": {"full_name": "someone-else/gamma"}}}})


def test_size_and_ci_definition_changes_are_not_gates_in_any_repo():
    """No diff-size ceiling and no `.github` path rule: a large diff touching CI
    definitions merges in any repo, the loop's own executors included, when the
    PR is open, checks are green and the review confirmed."""
    for repo in ("gamma", "warden", "sideclaw", "dotfiles"):
        conn = _fresh_ledger()
        try:
            _seed_pr_dispatch(conn, repo=repo, validation_status="confirmed",
                              artifact=f"https://github.com/jkrumm/{repo}/pull/7")
            pr = {**_DEFAULT_PR, "changed_files": 99, "additions": 4000,
                  "head": {**_DEFAULT_PR["head"], "repo": {"full_name": f"jkrumm/{repo}"}}}
            with fakes(pr=pr, files=[{"filename": ".github/workflows/ci.yml"}]) as fx:
                assert _land(conn, why="w", authorized_by="auto-from-item").merged, repo
            assert "merge_pr" in fx.calls, repo
        finally:
            conn.close()


def test_no_merge_method_allowed_refuses():
    _assert_refuses(exc_type=PolicyError, msg="no merge method",
                     fake={"repo": {"default_branch": "master", "allow_squash_merge": False,
                                     "allow_rebase_merge": False, "allow_merge_commit": False}})


def test_already_merged_refuses_second_time():
    _assert_refuses(exc_type=PolicyError, msg="was already merged at", seed={"merged_at": _now_iso()})


# --- --why / job-id shape -------------------------------------------------------

def test_why_required_refuses():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes():
            try:
                _land(conn, why="")
            except UsageError as e:
                assert "--why" in str(e)
            else:
                raise AssertionError("expected UsageError")
    finally:
        conn.close()


def test_invalid_job_id_shape_refuses():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes():
            try:
                _land(conn, job_id="not a valid id!")
            except UsageError:
                pass
            else:
                raise AssertionError("expected UsageError")
    finally:
        conn.close()


# --- plan / dry-run --------------------------------------------------------------

def test_plan_without_confirm_returns_plan():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes() as fx:
            result = _land(conn, confirm=False)
        assert isinstance(result, merge.MergePlan)
        assert result.needs_confirm is True
        assert result.merge_method == "squash"
        assert _merged_at(conn) is None
        assert _op_count(conn) == 0
        assert "mark_ready_for_review" not in fx.calls
    finally:
        conn.close()


def test_dry_run_with_confirm_still_plans():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes() as fx:
            result = _land(conn, confirm=True, dry_run=True)
        assert isinstance(result, merge.MergePlan)
        assert result.needs_confirm is False
        assert _merged_at(conn) is None
        assert "mark_ready_for_review" not in fx.calls
    finally:
        conn.close()


# --- mergeability re-read, after the un-draft ------------------------------------

def test_not_mergeable_after_undraft_refuses():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(pr={**_DEFAULT_PR, "mergeable": False, "mergeable_state": "dirty"}) as fx:
            try:
                _land(conn)
            except PolicyError as e:
                assert "not mergeable" in str(e)
            else:
                raise AssertionError("expected PolicyError")
        assert fx.calls.get("mark_ready_for_review")
        assert "merge_pr" not in fx.calls
        assert _op_row(conn)["outcome"] == "failed"
    finally:
        conn.close()


def test_mergeability_loop_exhaustion_raises_remote_error():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        slept = []
        with fakes(pr={**_DEFAULT_PR, "mergeable": None, "mergeable_state": "unknown"}) as fx:
            try:
                merge.plan_or_land(conn, job_id=JOB_ID, why="test", confirm=True, dry_run=False,
                                    authorized_by="U123", now=_NOW, sleep=lambda s: slept.append(s))
            except RemoteError as e:
                assert "never finished computing mergeability" in str(e)
            else:
                raise AssertionError("expected RemoteError")
        assert len(slept) == 4
        assert len(fx.calls["read_pr"]) == 6
        assert _op_row(conn)["outcome"] == "failed"
    finally:
        conn.close()


# --- the happy path ---------------------------------------------------------------

def test_happy_path_merges():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes() as fx:
            result = _land(conn, why="landing the fix")
        assert isinstance(result, merge.MergeResult)
        assert result.merged is True
        assert result.merge_method == "squash"
        assert result.merge_commit == "mergedsha001"
        assert result.branch_deleted is True
        assert fx.calls["merge_pr"][0][1]["sha"] == "deadbeef" * 5
        assert fx.calls["merge_pr"][0][1]["method"] == "squash"
        assert len(fx.calls["delete_branch"]) == 1
        assert _merged_at(conn) is not None
        row = _op_row(conn)
        assert row["outcome"] == "done"
        receipt = json.loads(row["receipt_json"])
        assert receipt["mergeCommit"] == "mergedsha001"
        assert result.deploy["attempted"] is False
        assert "autoDeploy is false" in result.deploy["reason"]
        assert _op_count(conn, kind="deploy") == 0
    finally:
        conn.close()


def test_owner_cli_confirm_lands_a_confirmed_dispatch_without_revalidating():
    """The owner's manual land path (`warden merge <job> --why --confirm`, i.e.
    `plan_or_land(authorized_by="cli:confirm")`) lands a dispatch whose
    `validation_status` is already `confirmed`. It re-checks the merge gate
    (which reads the stored `validation_status`) and never re-opens a review,
    and the merge operation records `authorized_by="cli:confirm"`."""
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn, validation_status="confirmed")
        with fakes() as fx:
            result = _land(conn, why="owner approved the merge", authorized_by="cli:confirm")
        assert isinstance(result, merge.MergeResult)
        assert result.merged is True
        assert _merged_at(conn) is not None
        assert fx.calls.get("check_runs"), "the merge gate re-checks the stored confirmation"
        row = _op_row(conn)
        assert row["outcome"] == "done"
        assert row["authorized_by"] == "cli:confirm", row["authorized_by"]
    finally:
        conn.close()


def test_delete_branch_remote_error_does_not_fail_the_merge():
    """Cleanup, allowed to fail: a merged commit with a leftover branch is
    untidy, not a reason to raise out of an otherwise-successful merge."""
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes():
            def _boom(owner, repo, branch):
                raise RemoteError("network blip deleting the branch")
            github.delete_branch = _boom
            result = _land(conn, why="landing the fix")
        assert isinstance(result, merge.MergeResult)
        assert result.merged is True
        assert result.branch_deleted is False
        assert _merged_at(conn) is not None
        assert _op_row(conn)["outcome"] == "done"
    finally:
        conn.close()


def test_merge_operation_recorded_before_mark_ready_for_review():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        seen = {}

        def _spy(node_id):
            row = _op_row(conn)
            seen["exists_open"] = row is not None and row["outcome"] is None

        with fakes(mark_ready_fn=_spy):
            _land(conn)
        assert seen.get("exists_open") is True
    finally:
        conn.close()


# --- merge_gate_check, direct -------------------------------------------------------

def test_gate_no_checks_passes():
    """No check-runs at all is not a refusal — there is nothing to be red."""
    merge.merge_gate_check(repo="gamma", check_runs=[], validation="confirmed")


def test_gate_green_checks_pass():
    merge.merge_gate_check(
        repo="gamma", validation="confirmed",
        check_runs=[{"name": "a", "status": "completed", "conclusion": "success"},
                    {"name": "b", "status": "completed", "conclusion": "skipped"},
                    {"name": "c", "status": "completed", "conclusion": "neutral"}])


def test_gate_refuses_failing_check_run():
    try:
        merge.merge_gate_check(
            repo="gamma", validation="confirmed",
            check_runs=[{"name": "build", "status": "completed", "conclusion": "failure"}],
        )
    except PolicyError as e:
        assert "has not passed cleanly" in str(e)
    else:
        raise AssertionError("expected PolicyError")


def test_gate_refuses_pending_check_run():
    try:
        merge.merge_gate_check(repo="gamma", validation="confirmed",
                               check_runs=[{"name": "build", "status": "in_progress", "conclusion": None}])
    except PolicyError as e:
        assert "has not passed cleanly" in str(e) and "build" in str(e)
    else:
        raise AssertionError("expected PolicyError")


def test_gate_refuses_validation_disagreed():
    try:
        merge.merge_gate_check(repo="gamma", check_runs=[], validation="disagreed")
    except PolicyError as e:
        assert "has not confirmed" in str(e) and "disagreed" in str(e)
    else:
        raise AssertionError("expected PolicyError")


def test_gate_refuses_validation_missing():
    try:
        merge.merge_gate_check(repo="gamma", check_runs=[], validation=None)
    except PolicyError as e:
        assert "(none)" in str(e)
    else:
        raise AssertionError("expected PolicyError")


# --- the gate, wired through plan_or_land -------------------------------------------

def test_any_path_merges_without_a_per_repo_policy_entry():
    """No declared scope, no per-repo carve-out: a Makefile, a CI workflow, a
    plist, a lockfile or a manifest is a fix like any other, in every repo, for
    an unattended merge and for the owner's alike."""
    for path in ("Makefile", "scripts/deploy.sh", "package.json", "bun.lock", "launchd/x.plist",
                 "compose.yml", "apps/api/Dockerfile", ".env.tpl", ".github/workflows/ci.yml",
                 "pyproject.toml", "ops/com.example.job.plist", "src/x.py"):
        for authorized_by in ("auto-from-item", "owner:argo", "cli:confirm"):
            conn = _fresh_ledger()
            try:
                _seed_pr_dispatch(conn, validation_status="confirmed")
                with fakes(entry={}, files=[{"filename": path}]) as fx:
                    result = _land(conn, why="w", authorized_by=authorized_by)
                assert result.merged, (path, authorized_by)
                assert "merge_pr" in fx.calls, (path, authorized_by)
            finally:
                conn.close()


def test_plan_or_land_zero_ci_merges():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(check_runs=[]):
            result = _land(conn)
        assert result.merged is True
    finally:
        conn.close()


def test_plan_or_land_double_confirm_race_two_connections():
    """The double `--confirm` race: two concurrent lands of the SAME
    job_id. conn1 holds the LAND step's write lock mid-transaction
    (uncommitted); conn2's land for the same job must fail loudly rather
    than merge twice. After conn1 commits, a third connection's land
    refuses on the ordinary 'already in flight' check instead — the
    operations row conn1 left behind is now visible."""
    db_path = _tmp_dir("merge-race-db-") / "warden.db"
    conn1 = ledger.connect(db_path, migrate=True)
    _seed_pr_dispatch(conn1)

    conn1.execute("BEGIN IMMEDIATE")
    operations.record(conn1, event_id=None, kind="merge", repo="gamma", authorized_by="U1",
                       note=f"job:{JOB_ID}", commit=False)
    # conn1 now holds sqlite's write lock, uncommitted — the exact mid-LAND
    # window the double --confirm race exploited.

    conn2 = ledger.connect(db_path, migrate=False)
    conn2.execute("PRAGMA busy_timeout=200")  # fail fast rather than hang the suite
    try:
        with fakes(check_runs=[]):
            try:
                _land(conn2)
            except sqlite3.OperationalError as e:
                assert "locked" in str(e).lower(), e
            else:
                raise AssertionError("expected sqlite3.OperationalError: database is locked")
    finally:
        conn2.close()

    conn1.commit()
    assert _op_count(conn1, kind="merge") == 1, "the first connection's merge operation must have landed"

    conn3 = ledger.connect(db_path, migrate=False)
    try:
        with fakes(entry={}, check_runs=[]):
            try:
                _land(conn3)
            except PolicyError as e:
                assert "already in flight" in str(e), e
            else:
                raise AssertionError("expected PolicyError: already in flight")
    finally:
        conn3.close()
    assert _op_count(conn1, kind="merge") == 1, "a second land must never record a second merge operation"
    conn1.close()


def test_plan_or_land_refuses_failing_ci():
    _assert_refuses(
        exc_type=PolicyError, msg="has not passed cleanly",
        fake={"entry": {},
              "check_runs": [{"name": "build", "status": "completed", "conclusion": "failure"}]},
    )


def test_plan_or_land_refuses_validation_disagreed():
    _assert_refuses(exc_type=PolicyError, msg="has not confirmed", seed={"validation_status": "disagreed"},
                     fake={"entry": {}})


def test_plan_or_land_refuses_validation_missing():
    _assert_refuses(exc_type=PolicyError, msg="(none)", seed={"validation_status": None},
                     fake={"entry": {}})


# --- deploy: autoDeploy --------------------------------------------------------------

def test_autodeploy_runs_rollout_with_key():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(entry={"autoDeploy": True,
                           "deploy": "hyperdx-apply"}) as fx:
            result = _land(conn)
        assert fx.calls["rollout_run"][0][0][0] == "hyperdx-apply"
        assert result.deploy["attempted"] is True and result.deploy["ok"] is True
        assert result.deploy["key"] == "hyperdx-apply"
        row = _op_row(conn, kind="deploy")
        assert row["outcome"] == "done"
        assert json.loads(row["receipt_json"])["key"] == "hyperdx-apply"
    finally:
        conn.close()


def test_autodeploy_operation_recorded_before_rollout_runs():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        seen = {}

        def _spy(key, *, timeout_s):
            row = _op_row(conn, kind="deploy")
            seen["exists_open"] = row is not None and row["outcome"] is None
            return rollout.RolloutResult(True, 0, "ok")

        with fakes(entry={"autoDeploy": True,
                           "deploy": "hyperdx-apply"}, rollout_run_fn=_spy):
            _land(conn)
        assert seen.get("exists_open") is True
    finally:
        conn.close()


def test_autodeploy_expected_alerts_from_contents():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        alert_bytes = json.dumps({"name": "cpu-high", "threshold": 90, "thresholdType": "gt"}).encode()
        with fakes(entry={"autoDeploy": True,
                           "deploy": "hyperdx-apply"},
                   files=[{"filename": "observability/alerts/cpu.json"}],
                   contents={"observability/alerts/cpu.json": alert_bytes}):
            result = _land(conn)
        alerts = result.deploy["expectedAlerts"]
        assert len(alerts) == 1 and alerts[0]["name"] == "cpu-high" and alerts[0]["threshold"] == 90
    finally:
        conn.close()


def test_autodeploy_rollout_failure_marks_op_failed():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(entry={"autoDeploy": True,
                           "deploy": "hyperdx-apply"},
                   rollout_result=rollout.RolloutResult(False, 1, "boom")):
            result = _land(conn)
        assert result.deploy["ok"] is False
        assert _op_row(conn, kind="deploy")["outcome"] == "failed"
    finally:
        conn.close()


def test_autodeploy_no_key_declared():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(entry={"autoDeploy": True}):
            result = _land(conn)
        assert result.deploy["attempted"] is False
        assert "no deploy key is declared" in result.deploy["reason"]
        assert _op_count(conn, kind="deploy") == 0
    finally:
        conn.close()


def test_autodeploy_key_not_in_allowlist():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(entry={"autoDeploy": True,
                           "deploy": "not-a-real-key"}):
            result = _land(conn)
        assert result.deploy["attempted"] is False
        assert "not in the allowlist" in result.deploy["reason"]
        assert _op_count(conn, kind="deploy") == 0
    finally:
        conn.close()


# --- deploy: deployOnMerge ------------------------------------------------------------

def test_deploy_on_merge_queries_actions_runs():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(entry={"deployOnMerge": True},
                   merge_resp={"sha": _FULL_SHA},
                   actions_runs=[{"id": 1, "name": "deploy", "status": "completed",
                                   "conclusion": "success", "html_url": "https://x"}]):
            result = _land(conn)
        assert result.deploy["attempted"] is False
        assert result.deploy["mergeCommit"] == _FULL_SHA
        assert len(result.deploy["actionsRuns"]) == 1
        assert _op_row(conn, kind="deploy")["outcome"] == "done"
    finally:
        conn.close()


def test_deploy_on_merge_no_runs_leaves_op_open():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(entry={"deployOnMerge": True},
                   merge_resp={"sha": _FULL_SHA}, actions_runs=[]):
            result = _land(conn)
        assert result.deploy.get("note") == "no Actions run visible yet"
        assert _op_row(conn, kind="deploy")["outcome"] is None
    finally:
        conn.close()


def test_deploy_on_merge_actions_runs_remote_error():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(entry={"deployOnMerge": True},
                   merge_resp={"sha": _FULL_SHA}, actions_runs_error=RemoteError("timeout")):
            result = _land(conn)
        assert result.deploy["attempted"] is False
        assert _op_row(conn, kind="deploy")["outcome"] == "unknown"
    finally:
        conn.close()


def test_deploy_on_merge_ignored_without_full_sha():
    # merge_resp's default sha ("mergedsha001") is not 40 hex — deployOnMerge
    # must never fire off a malformed/short sha.
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(entry={"deployOnMerge": True}):
            result = _land(conn)
        assert result.deploy["attempted"] is False
        assert "autoDeploy is false" in result.deploy["reason"]
        assert _op_count(conn, kind="deploy") == 0
    finally:
        conn.close()


# --- merge_pr failure handling ----------------------------------------------------------

def test_merge_pr_remote_error_marks_unknown_and_propagates():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(merge_pr_error=RemoteError("GitHub 500", maybe_mutated=True)):
            try:
                _land(conn)
            except RemoteError:
                pass
            else:
                raise AssertionError("expected RemoteError")
        row = _op_row(conn)
        assert row["outcome"] == "unknown"
        assert "error" in json.loads(row["receipt_json"])
    finally:
        conn.close()


def test_merge_pr_409_marks_failed():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(merge_pr_error=PolicyError("GitHub refused the merge (409)")):
            try:
                _land(conn)
            except PolicyError:
                pass
            else:
                raise AssertionError("expected PolicyError")
        assert _op_row(conn)["outcome"] == "failed"
    finally:
        conn.close()


def test_mark_ready_for_review_remote_error_marks_op_failed():
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        with fakes(mark_ready_error=RemoteError("graphql down")) as fx:
            try:
                _land(conn)
            except RemoteError:
                pass
            else:
                raise AssertionError("expected RemoteError")
        assert "merge_pr" not in fx.calls
        assert _op_row(conn)["outcome"] == "failed"
    finally:
        conn.close()


# --- collect_expected_alerts, direct -----------------------------------------------------

def test_collect_expected_alerts_empty_without_merge_sha():
    result = merge.collect_expected_alerts(
        "jkrumm", "gamma", [{"filename": "observability/alerts/x.json"}], None
    )
    assert result == []


def test_collect_expected_alerts_skips_unparseable_content():
    orig = github.contents
    github.contents = lambda owner, repo, path, *, ref: b"not json"
    try:
        result = merge.collect_expected_alerts(
            "jkrumm", "gamma", [{"filename": "observability/alerts/x.json"}], "a" * 40
        )
    finally:
        github.contents = orig
    assert result == []


def test_collect_expected_alerts_ignores_non_alert_paths():
    orig = github.contents
    calls = []
    def _spy(owner, repo, path, *, ref):
        calls.append(path)
        return None
    github.contents = _spy
    try:
        result = merge.collect_expected_alerts(
            "jkrumm", "gamma", [{"filename": "src/main.py"}], "a" * 40
        )
    finally:
        github.contents = orig
    assert result == [] and calls == []


# --- to_json shapes ------------------------------------------------------------------------

def test_merge_plan_to_json_shape():
    plan = merge.MergePlan(needs_confirm=True, repo="jkrumm/gamma", pull_request=7, title="t",
                            head="dispatch/x", base="master", merge_method="squash",
                            changed_files=2, changed_lines=8)
    data = plan.to_json()
    assert data["dryRun"] is True
    assert data["needsConfirm"] is True
    assert data["mergeMethod"] == "squash"
    assert len(data["wouldDo"]) == 3
    assert len(data["wouldNeverDo"]) == 3
    assert "Re-invoke with --confirm" in data["note"]
    assert "verb" not in data and "ok" not in data


def test_merge_plan_to_json_note_stays_short_when_confirmed():
    plan = merge.MergePlan(needs_confirm=False, repo="jkrumm/gamma", pull_request=7, title="t",
                            head="dispatch/x", base="master", merge_method="squash",
                            changed_files=2, changed_lines=8)
    data = plan.to_json()
    assert data["note"] == "nothing was merged and nothing was un-drafted"


def test_merge_result_to_json_shape():
    result = merge.MergeResult(
        merged=True, repo_slug="jkrumm/gamma", pull_request=7, title="t", merge_method="squash",
        merge_commit="abc123", branch="dispatch/x", branch_deleted=True,
        deploy={"attempted": False, "reason": "x"}, merge_op_id="op-1", deploy_op_id=None,
    )
    data = result.to_json()
    assert data["merged"] is True
    assert data["mergeCommit"] == "abc123"
    assert data["deploy"]["attempted"] is False
    assert "verb" not in data and "ok" not in data
    assert "op-1" not in json.dumps(data)


# --- runner --------------------------------------------------------------------------------

def main() -> int:
    tests = [(name, fn) for name, fn in sorted(globals().items())
             if name.startswith("test_") and callable(fn)]
    passed = 0
    failures: list[str] = []
    for name, fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as e:
            failures.append(f"{name}: {e}")
        except Exception:
            failures.append(f"{name}: unexpected exception\n{traceback.format_exc()}")

    print(f"{passed}/{len(tests)} passed")
    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  {f}")
        return 1
    return 0


def test_owner_confirm_gets_the_same_shrunk_gate_not_a_bypass():
    """`warden merge --confirm` (cli) and the Argo click (`owner:argo`) go through
    exactly the gate an unattended merge does: a missing/unconfirmed review or a
    failing check still refuses, whoever asks."""
    for authorized_by in ("cli:confirm", "owner:argo"):
        _assert_refuses_as(authorized_by, msg="has not confirmed", seed={"validation_status": "blocked"})
        _assert_refuses_as(authorized_by, msg="has not passed cleanly",
                           fake={"check_runs": [{"name": "ci", "status": "completed", "conclusion": "failure"}]})


def test_github_blocked_state_is_reported_from_the_merge_call_not_pre_empted():
    """A `blocked` mergeable_state (classic branch protection, an unmet required
    review or check) is no longer refused by a local pre-check: the merge call is
    made, GitHub refuses it (405) and that refusal is reported, op failed."""
    conn = _fresh_ledger()
    try:
        _seed_pr_dispatch(conn)
        refusal = PolicyError("GitHub refused the merge (405): the pull request is not mergeable under "
                              "this repo's rules. Nothing was merged.")
        with fakes(pr={**_DEFAULT_PR, "mergeable": True, "mergeable_state": "blocked"},
                   merge_pr_error=refusal) as fx:
            try:
                _land(conn)
            except PolicyError as e:
                assert "GitHub refused the merge (405)" in str(e), str(e)
            else:
                raise AssertionError("a blocked PR must not merge")
        assert fx.calls.get("merge_pr"), "the merge call is the check"
        assert _op_row(conn)["outcome"] == "failed"
    finally:
        conn.close()


if __name__ == "__main__":
    sys.exit(main())
