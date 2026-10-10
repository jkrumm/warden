"""The agent-gateway job-server transport — the Python port of the retired bash CLI's
`sideclaw_submit` (1219-1251), `sideclaw_get` (1253-1263) and `wait_for`
(1439-1462).

Always `urllib.request`, never `curl`/`subprocess` — the whole point of this
port is that a Python process can talk HTTP directly. Base URL is read at
call time via `_base()` (not module load) so a test can set
`WARDEN_AGENT_GATEWAY_BASE` before calling into this module without reimporting
it.
"""

from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import re
import sys
import time
import urllib.error
import urllib.request
from collections.abc import Collection
from pathlib import Path
from typing import Any, Callable

from .errors import PolicyError, RemoteError, SubmitRefused

_DEFAULT_BASE = "http://localhost:7705"
_TIMEOUT_S = 30

# agent-gateway's own JobStatus. "cancelled" arrives with the cancel endpoint this
# wave adds.
TERMINAL = frozenset({"done", "failed", "interrupted", "cancelled"})

_JOB_ID_RE = re.compile(r"^[A-Za-z0-9-]+$")

# The verdict schemas warden consumes — published by agent-gateway
# (server/jobs/handlers/{dispatch,review}.ts) at GET /api/dispatch-schema and
# GET /api/review-schema, pinned here rather than copied by hand so drift is
# a loud refusal (assert_result_schema()) instead of a silently-ignored
# verdict. Bump these ONLY after re-reading the source constants they mirror
# — never guess a version or an outcome list.
#
# Dispatch keeps a small ACCEPTANCE WINDOW instead of one version, so a
# rolling agent-gateway restart cannot strike every in-flight episode: v5 adds the
# `checks_tool_failed` outcome (a check-tool infrastructure failure, distinct
# from the repo's own red suite), while an agent-gateway not yet restarted still
# answers with v4. v6 adds the optional `fallbackWithheld` field (additive).
# DISPATCH_SCHEMA_VERSIONS is what assert_result_schema()
# enforces; a version outside the window is refused as loudly as ever. Review
# has a single live version.
DISPATCH_SCHEMA_VERSIONS: frozenset[int] = frozenset({5, 6})
DISPATCH_SCHEMA_VERSION = max(DISPATCH_SCHEMA_VERSIONS)  # the current version, derived so the two cannot drift
REVIEW_SCHEMA_VERSION = 1

# server/jobs/handlers/dispatch.ts DISPATCH_OUTCOMES. `pr_updated` (a `revisionOf`
# episode pushed to the same branch and updated the existing PR) and `conflict`
# (the rebase onto the latest default branch failed; nothing pushed) arrive with
# schema version 4. `checks_tool_failed` (the check TOOL itself crashed — an
# infrastructure failure, never a finding about the code) arrives with schema
# version 5.
DISPATCH_OUTCOMES: tuple[str, ...] = (
    "verdict_only",
    "issue_declined",
    "issue_failed",
    "issue_filed",
    "no_changes",
    "diff_refused",
    "checks_failed",
    "checks_tool_failed",
    "branch_no_pr",
    "pr_failed",
    "pr_opened",
    "pr_updated",
    "conflict",
    "applied_in_place",
    "salvaged",
    "withheld",
)

# server/jobs/handlers/review.ts REVIEW_OUTCOMES at schema version 1.
REVIEW_OUTCOMES: tuple[str, ...] = ("clean", "actionable", "needs-human")

# server/jobs/handlers/update-pr.ts UPDATE_PR_OUTPUT `status`. The update_pr result carries
# no schemaVersion, so update_pr_result() validates the whole shape instead.
UPDATE_PR_STATUSES: tuple[str, ...] = ("updated", "up_to_date", "conflict")

_SHA_RE = re.compile(r"^[0-9a-f]{40}$")


def _base() -> str:
    return os.environ.get("WARDEN_AGENT_GATEWAY_BASE") or os.environ.get("WARDEN_SIDECLAW_BASE", _DEFAULT_BASE)


def valid_job_id(job_id: str) -> bool:
    return bool(_JOB_ID_RE.match(job_id))


def _request(method: str, path: str, body: dict[str, Any] | None) -> tuple[int, str]:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(
        f"{_base()}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:
            return resp.status, resp.read().decode("utf-8", errors="replace")
    except urllib.error.HTTPError as e:
        return e.code, e.read().decode("utf-8", errors="replace")


def _raise_for_submit_status(status: int, text: str) -> None:
    """A 4xx on submit is agent-gateway refusing (its repo allowlist, a tier above a
    repo's ceiling, an unverified model, bad params): `SubmitRefused`, carrying
    agent-gateway's own `error` text, never retried. Anything else non-200 (5xx) is
    a plain `RemoteError` and keeps its retry behaviour."""
    if status == 200:
        return
    if 400 <= status < 500:
        message = None
        try:
            parsed = json.loads(text)
            message = parsed.get("error") if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            pass
        raise SubmitRefused(
            f"agent-gateway refused the job (HTTP {status}): {message or text[:300]}", status=status,
        )
    raise RemoteError(f"agent-gateway returned HTTP {status}: {text[:300]}")


def submit(
    *,
    cwd: str,
    tier: str,
    brief: str,
    context: str | None = None,
    model: str | None = None,
    revision_of: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"cwd": cwd, "tier": tier, "brief": brief}
    if context:
        params["context"] = context
    if model:
        params["model"] = model
    if revision_of:
        params["revisionOf"] = revision_of
    body = {"tool": "dispatch", "params": params}

    try:
        status, text = _request("POST", "/api/jobs", body)
    except urllib.error.URLError as e:
        # Connection refused is definitive: nothing was sent, so a retry cannot duplicate.
        # Anything else (a timeout, a reset) is ambiguous — the POST may have landed.
        raise RemoteError(
            f"agent-gateway job submit failed (is the LaunchAgent up? curl {_base()}/health)",
            maybe_mutated=not isinstance(e.reason, ConnectionRefusedError),
        )
    except (TimeoutError, OSError):
        raise RemoteError(
            f"agent-gateway job submit failed (is the LaunchAgent up? curl {_base()}/health)",
            maybe_mutated=True,
        )

    _raise_for_submit_status(status, text)

    # From here agent-gateway answered 200: the job EXISTS, whatever the body says.
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise RemoteError(f"agent-gateway returned HTTP {status} with unparseable body: {text[:300]}",
                          maybe_mutated=True)

    job = parsed.get("job") if isinstance(parsed, dict) else None
    if not isinstance(job, dict) or "id" not in job:
        raise RemoteError("agent-gateway accepted the job but returned no id", maybe_mutated=True)
    return job


def _submit_job(tool: str, params: dict[str, Any]) -> dict[str, Any]:
    """`POST /api/jobs {"tool": tool, "params": params}` and return the `{"job": {...}}` envelope's
    job: the shared transport and error handling of `submit_review`, `submit_triage` and
    `submit_update_pr`. A 4xx is `SubmitRefused`, anything else a `RemoteError`."""
    try:
        status, text = _request("POST", "/api/jobs", {"tool": tool, "params": params})
    except (urllib.error.URLError, TimeoutError, OSError):
        raise RemoteError(
            f"agent-gateway {tool} submit failed (is the LaunchAgent up? curl {_base()}/health)",
            maybe_mutated=True,
        )

    _raise_for_submit_status(status, text)

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise RemoteError(f"agent-gateway returned HTTP {status} with unparseable body: {text[:300]}")

    job = parsed.get("job") if isinstance(parsed, dict) else None
    if not isinstance(job, dict) or "id" not in job:
        raise RemoteError(f"agent-gateway accepted the {tool} job but returned no id")
    return job


def submit_review(*, cwd: Path, pr: int, context: str | None = None, model: str | None = None) -> dict[str, Any]:
    """The `review` counterpart to `submit()` — same transport, error
    handling and `{"job": {...}}` envelope, different tool/params shape
    (`POST /api/jobs {"tool":"review","params":{cwd,pr,context}}`)."""
    params: dict[str, Any] = {"cwd": str(cwd), "pr": pr}
    if context:
        params["context"] = context
    if model:
        params["model"] = model
    return _submit_job("review", params)


def submit_triage(*, prompt: str, schema: dict[str, Any]) -> dict[str, Any]:
    """The single-shot `triage` job: `POST /api/jobs {"tool":"triage","params":{prompt,schema}}`.
    agent-gateway validates the model's answer against `schema` and, on a `done` job, returns it at
    `job.result.result`. No `model` param — agent-gateway routes the tool itself. Same transport and
    error handling as `submit_review()`: a 4xx is `SubmitRefused`, anything else a `RemoteError`."""
    return _submit_job("triage", {"prompt": prompt, "schema": schema})


def submit_update_pr(*, cwd: str | Path, pr: int) -> dict[str, Any]:
    """The merge train's `update_pr` job: `POST /api/jobs {"tool":"update_pr","params":{cwd,pr}}`
    — rebase one `dispatch/*` PR onto the latest default branch, re-run the repo's checks,
    force-with-lease push. Same transport and error handling as `submit_review()`: a 4xx is
    `SubmitRefused`, anything else a `RemoteError`. The result is read with
    `update_pr_result()`."""
    return _submit_job("update_pr", {"cwd": str(cwd), "pr": pr})


def update_pr_result(job: dict[str, Any]) -> dict[str, Any]:
    """The validated result of a `done` update_pr job (server/jobs/handlers/update-pr.ts
    UPDATE_PR_OUTPUT): `status` one of UPDATE_PR_STATUSES, `headSha`/`previousHeadSha` 40-hex
    commits, `prUrl` a string, and `checks{passed, summary}` present whenever the branch was
    `updated` (the train decides from it). Any other shape raises a loud `RemoteError` — never a
    best-effort read: a train that guessed would merge a SHA nobody checked."""
    result = job.get("result")
    if job.get("status") != "done" or not isinstance(result, dict):
        raise RemoteError(f"agent-gateway update_pr job {job.get('id')} ended '{job.get('status')}' with no result")
    status = result.get("status")
    if status not in UPDATE_PR_STATUSES:
        raise RemoteError(
            f"agent-gateway update_pr result status {status!r}, warden expects one of {UPDATE_PR_STATUSES} — "
            f"refusing to parse"
        )
    for key in ("headSha", "previousHeadSha"):
        value = result.get(key)
        if not isinstance(value, str) or not _SHA_RE.match(value):
            raise RemoteError(f"agent-gateway update_pr result {key} {value!r} is not a 40-hex commit — refusing to parse")
    if not isinstance(result.get("prUrl"), str):
        raise RemoteError("agent-gateway update_pr result carries no prUrl — refusing to parse")
    checks = result.get("checks")
    if checks is None and status == "updated":
        raise RemoteError("agent-gateway update_pr result is 'updated' but carries no checks — refusing to parse")
    if checks is not None and not (isinstance(checks, dict) and isinstance(checks.get("passed"), bool)
                                   and isinstance(checks.get("summary"), str)):
        raise RemoteError(f"agent-gateway update_pr result checks {checks!r} are malformed — refusing to parse")
    return result


# agent-gateway's per-repo lease refusal (server/lib/repo-lease.ts `repoLeaseRefusal(holder, tool)`):
# `<tool> refused: an implement episode is already running in this repo (job <holder>) — …`,
# where <tool> is `dispatch` for an implement episode and `update_pr` for the merge train's
# rebase — both take the same lease. The POST is accepted (HTTP 200); the job then FAILS
# carrying this text. It means "another implement episode holds this repo right now" —
# retry later, not an infrastructure failure.
_LEASE_REFUSAL_RE = re.compile(r"\b(?:dispatch|update_pr) refused: an implement episode is already running in this repo\b")

_BUNDLE_PATH_RE = re.compile(r"bundled at (\S+?)\.?(\s|$)")

# GET /api/routing's route for the stronger implement model (attempt 3+). Warden never
# names a model id itself: when agent-gateway has no such route, the dispatch carries no
# `model` key and agent-gateway's own default applies.
_ESCALATION_ROUTE = "dispatch_implement_escalation"
_escalation_cache: dict[str, str] = {}


def is_lease_refusal(job: dict[str, Any]) -> bool:
    """True for a FAILED job whose error is agent-gateway's per-repo lease refusal — of an implement
    dispatch or of an `update_pr`."""
    if job.get("status") != "failed":
        return False
    error = job.get("error")
    return isinstance(error, str) and bool(_LEASE_REFUSAL_RE.search(error))


def conflict_bundle_path(result: dict[str, Any]) -> str | None:
    """The path of the git bundle holding a `conflict` episode's commits. agent-gateway puts it
    only in the verdict's prose ("... The episode's commits were bundled at <path>."), and
    only when bundling worked — None when absent."""
    verdict = result.get("verdict")
    match = _BUNDLE_PATH_RE.search(verdict) if isinstance(verdict, str) else None
    return match.group(1) if match else None


def escalation_model() -> str | None:
    """The stronger implement model from `GET /api/routing` (`routes.dispatch_implement_escalation
    .model`), or None when the route is absent or the call fails — the caller then sends no
    `model` key. Cached for the life of the process (one loop tick); a failure is not cached."""
    cached = _escalation_cache.get("model")
    if cached:
        return cached
    try:
        status, text = _request("GET", "/api/routing", None)
        parsed = json.loads(text) if status == 200 else None
    except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError) as e:
        print(f"agent-gateway: could not read /api/routing for the escalation model: {e}", file=sys.stderr)
        return None
    routes = parsed.get("routes") if isinstance(parsed, dict) else None
    route = routes.get(_ESCALATION_ROUTE) if isinstance(routes, dict) else None
    model = route.get("model") if isinstance(route, dict) else None
    if not isinstance(model, str) or not model:
        print(f"agent-gateway: no {_ESCALATION_ROUTE} route (HTTP {status}) — escalating on the default model",
              file=sys.stderr)
        return None
    _escalation_cache["model"] = model
    return model


def dispatch_policy() -> dict[str, Any]:
    """agent-gateway's dispatch policy (`GET /api/dispatch-policy`: the repo roots, each repo's tier
    ceiling, the overrides) as it answers now. A refusal that the policy caused (a tier above a
    repo's ceiling) can only clear when this changes; see policy_hash(). Raises RemoteError when
    agent-gateway is unreachable or answers anything but a JSON object."""
    try:
        status, text = _request("GET", "/api/dispatch-policy", None)
    except (urllib.error.URLError, TimeoutError, OSError):
        raise RemoteError("agent-gateway dispatch-policy read failed (is the LaunchAgent up?)")
    if status != 200:
        raise RemoteError(f"agent-gateway returned HTTP {status} for its dispatch policy")
    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise RemoteError(f"agent-gateway's dispatch policy is not JSON: {text[:300]}")
    if not isinstance(parsed, dict):
        raise RemoteError("agent-gateway's dispatch policy is not a JSON object")
    return parsed


def policy_hash(policy: dict[str, Any]) -> str:
    """sha256 of the canonical JSON of a dispatch policy: equal policies, equal hash."""
    return hashlib.sha256(json.dumps(policy, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()


def get(job_id: str) -> dict[str, Any] | None:
    try:
        status, text = _request("GET", f"/api/jobs/{job_id}", None)
    except (urllib.error.URLError, TimeoutError, OSError):
        raise RemoteError(f"agent-gateway job poll failed for {job_id} (is the LaunchAgent up?)")

    if status == 404:
        return None
    if status != 200:
        raise RemoteError(f"agent-gateway returned HTTP {status} for job {job_id}")

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise RemoteError(f"agent-gateway returned HTTP {status} for job {job_id} with unparseable body: {text[:300]}")

    job = parsed.get("job") if isinstance(parsed, dict) else None
    if not isinstance(job, dict):
        raise RemoteError(f"agent-gateway returned no job for {job_id}")
    return job


def wait(
    job_id: str,
    *,
    timeout_s: int = 170,
    interval_s: int = 5,
    sleep: Callable[[float], None] = time.sleep,
    clock: Callable[[], float] = time.monotonic,
) -> dict[str, Any] | None:
    start = clock()
    while True:
        job = get(job_id)
        if job is None:
            raise RemoteError(f"could not read job status for {job_id}")
        if job.get("status") in TERMINAL:
            return job
        if clock() - start >= timeout_s:
            # The sweeper delivers later — a timeout here is not a failure.
            return None
        sleep(interval_s)


def cancel(job_id: str) -> dict[str, Any]:
    try:
        status, text = _request("POST", f"/api/jobs/{job_id}/cancel", {})
    except (urllib.error.URLError, TimeoutError, OSError):
        raise RemoteError(f"agent-gateway job cancel failed for {job_id} (is the LaunchAgent up?)")

    if status == 404:
        raise RemoteError(f"agent-gateway has no job {job_id}")
    if status == 409:
        try:
            parsed = json.loads(text)
            message = parsed.get("error") if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            message = None
        raise PolicyError(message or f"job {job_id} is already terminal")
    if status != 200:
        raise RemoteError(f"agent-gateway returned HTTP {status} cancelling job {job_id}: {text[:300]}")

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise RemoteError(f"agent-gateway returned HTTP {status} cancelling job {job_id} with unparseable body: {text[:300]}")

    job = parsed.get("job") if isinstance(parsed, dict) else None
    if not isinstance(job, dict):
        raise RemoteError(f"agent-gateway returned no job cancelling {job_id}")
    return job


def assert_result_schema(job: dict[str, Any], expected: int | Collection[int], tool: str) -> None:
    """Raise a LOUD refusal when a terminal `done` job's `result.schemaVersion`
    is not one of `expected` — the failure this exists to design out is a
    consumer (this file's own callers in triage.py) silently parsing a verdict
    whose shape moved under it. Every loop poll that reads a `result` off an
    `investigate`/`implement`/`review` job calls this first; the caller is
    expected to treat it as an infrastructure failure (a strike) carrying the exact
    message this raises, per DESIGN.md's "deferral must be visible".

    `expected` is a single version (review, `REVIEW_SCHEMA_VERSION`) or a
    collection of them (dispatch, `DISPATCH_SCHEMA_VERSIONS` — the acceptance
    window that spans a rolling agent-gateway restart). Only checked on
    `status == "done"`: a failed/interrupted/cancelled job carries no `result`
    worth pinning a shape to."""
    if job.get("status") != "done":
        return
    result = job.get("result")
    version = result.get("schemaVersion") if isinstance(result, dict) else None
    versions = {expected} if isinstance(expected, int) else set(expected)
    if version not in versions:
        expected_text = (str(expected) if isinstance(expected, int)
                         else "one of " + ", ".join(str(v) for v in sorted(versions)))
        raise RemoteError(
            f"agent-gateway {tool} result schemaVersion {version}, warden expects {expected_text} — refusing to parse"
        )


def assert_outcome(job: dict[str, Any], outcomes: tuple[str, ...], tool: str) -> None:
    """Companion to `assert_result_schema()`: raises a LOUD refusal when a
    terminal `done` job's `result.outcome` is missing, or is not one of the
    pinned `outcomes` (`DISPATCH_OUTCOMES`/`REVIEW_OUTCOMES` above) — the
    same "a consumer must never silently parse a verdict shape that moved"
    contract, now covering the outcome VOCABULARY as well as the schema
    version number. A caller that let an unrecognised outcome fall through
    to its own switch's `else` branch is fail-closed by construction, but a
    switch missing an `else` (or one whose `else` is itself permissive, as
    `poll_validation_jobs()` was before this) is not — this is the check
    that makes that impossible regardless of how the caller's own switch is
    written. Only checked on `status == "done"`, same as
    `assert_result_schema()`."""
    if job.get("status") != "done":
        return
    result = job.get("result")
    outcome = result.get("outcome") if isinstance(result, dict) else None
    if outcome not in outcomes:
        raise RemoteError(
            f"agent-gateway {tool} result outcome {outcome!r}, warden expects one of {outcomes} — refusing to parse"
        )


def finished_at_iso(job: dict[str, Any], *, fallback: dt.datetime) -> str:
    """The ledger's `finished_at` should read when agent-gateway itself finished
    the job (`JobView.finishedAt`, epoch ms — server/jobs/types.ts), not when
    this process happened to poll it. Every terminal poll response already
    carries this field; warden used to discard it and stamp its own wall
    clock instead, which is why a poll suspended overnight recorded a
    614-minute dispatch that actually took 20 (docs/history/state-log.md
    §79). Falls back to `fallback` (the caller's own `now`) only when
    agent-gateway's value is missing or not a number — every real terminal job
    carries one, but a fallback is cheaper than a caller-side branch."""
    raw = job.get("finishedAt")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return dt.datetime.fromtimestamp(raw / 1000, tz=dt.timezone.utc).isoformat()
    return fallback.isoformat()


def classify_dispatch_outcome(status: str | None, verdict_json: str | None) -> tuple[str, str | None]:
    """Classify a finished dispatch's outcome from agent-gateway's own published
    verdict shape — the ladder `scripts/watchdog-poll.py`'s `_dispatch_summary()`
    and `scripts/watchdog-summary.py`'s `_dispatch_outcome_note()` used to carry
    as two hand-mirrored copies. Returns `(kind, detail)`:

      "failed"     — `status` was `failed`/`interrupted`; `detail` is that status.
      "no_verdict" — dispatch finished with no `verdict_json` at all.
      "unreadable" — `verdict_json` didn't parse as a JSON object.
      "degraded"   — the verdict's own `degraded` flag was set (tool failure,
                     not a repo finding).
      "summary"    — the normal case; `detail` is the verdict's own `summary`
                     text, stripped, or `None` if it was empty.

    This function owns only the CLASSIFICATION — never the wording. Each
    caller renders `kind`/`detail` into its own phrasing, which is why the two
    downstream strings still read differently ("dispatch failed" vs. plain
    "failed") despite sharing this ladder now."""
    if status in ("failed", "interrupted"):
        return "failed", status
    if not verdict_json:
        return "no_verdict", None
    try:
        v = json.loads(verdict_json)
    except json.JSONDecodeError:
        return "unreadable", None
    if not isinstance(v, dict):
        return "unreadable", None
    if v.get("degraded"):
        return "degraded", None
    return "summary", (v.get("summary") or "").strip() or None
