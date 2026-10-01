"""The sideclaw job-server transport — the Python port of the retired bash CLI's
`sideclaw_submit` (1219-1251), `sideclaw_get` (1253-1263) and `wait_for`
(1439-1462).

Always `urllib.request`, never `curl`/`subprocess` — the whole point of this
port is that a Python process can talk HTTP directly. Base URL is read at
call time via `_base()` (not module load) so a test can set
`WARDEN_SIDECLAW_BASE` before calling into this module without reimporting
it.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any, Callable

from .errors import PolicyError, RemoteError

_DEFAULT_BASE = "http://localhost:7705"
_TIMEOUT_S = 30

# sideclaw's own JobStatus. "cancelled" arrives with the cancel endpoint this
# wave adds.
TERMINAL = frozenset({"done", "failed", "interrupted", "cancelled"})

_JOB_ID_RE = re.compile(r"^[A-Za-z0-9-]+$")

# The two verdict schemas warden consumes — published by sideclaw
# (server/jobs/handlers/{dispatch,review}.ts) at GET /api/dispatch-schema and
# GET /api/review-schema, pinned here rather than copied by hand so drift is
# a loud refusal (assert_result_schema()) instead of a silently-ignored
# verdict. Bump these ONLY after re-reading the source constants they mirror
# — never guess a version or an outcome list.
DISPATCH_SCHEMA_VERSION = 3
REVIEW_SCHEMA_VERSION = 1

# server/jobs/handlers/dispatch.ts DISPATCH_OUTCOMES at schema version 3.
DISPATCH_OUTCOMES: tuple[str, ...] = (
    "verdict_only",
    "issue_declined",
    "issue_failed",
    "issue_filed",
    "no_changes",
    "diff_refused",
    "checks_failed",
    "branch_no_pr",
    "pr_failed",
    "pr_opened",
    "applied_in_place",
    "salvaged",
    "withheld",
)

# server/jobs/handlers/review.ts REVIEW_OUTCOMES at schema version 1.
REVIEW_OUTCOMES: tuple[str, ...] = ("clean", "actionable", "needs-human")

# sideclaw's terminal statuses, shared by every dispatch tier. Sorted, not raw: the
# source is a frozenset whose iteration order varies with hash randomization, and a
# public constant must not present a different sequence per process to anything
# order-sensitive (logging, tests, a displayed list).
TERMINAL_STATUSES: tuple[str, ...] = tuple(sorted(TERMINAL))


def is_review_finding(entry: Any) -> bool:
    """True when `entry` is one published review finding.

    Mirrors `server/jobs/handlers/review.ts`'s `FINDING` (schema version
    `REVIEW_SCHEMA_VERSION`): `file` and `message` are required strings, `line` and
    `angle` are optional for a consumer that only quotes what it is given. This lives
    beside the schema constants because it is part of the same published contract
    `assert_result_schema()` refuses on — a reader must not invent its own idea of the
    shape. It is stricter than the producer in one direction on purpose: a blank
    `file`/`message` is rejected, because a finding warden cannot name is one it cannot
    report (DESIGN.md §115), so a stored payload carrying one is unusable rather than a
    quote with a hole in it."""
    return (isinstance(entry, dict)
            and isinstance(entry.get("file"), str) and bool(entry["file"].strip())
            and isinstance(entry.get("message"), str) and bool(entry["message"].strip()))


def _base() -> str:
    return os.environ.get("WARDEN_SIDECLAW_BASE", _DEFAULT_BASE)


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


def submit(
    *,
    cwd: str,
    tier: str,
    brief: str,
    context: str | None = None,
    sensitive: bool = False,
    model: str | None = None,
) -> dict[str, Any]:
    params: dict[str, Any] = {"cwd": cwd, "tier": tier, "brief": brief}
    if context:
        params["context"] = context
    # Only ever set for a repo the policy names in `sensitive` — sideclaw
    # re-checks the same investigate-only restriction independently.
    if sensitive:
        params["sensitive"] = True
    if model:
        params["model"] = model
    body = {"tool": "dispatch", "params": params}

    try:
        status, text = _request("POST", "/api/jobs", body)
    except (urllib.error.URLError, TimeoutError, OSError):
        # A timeout on submit is ambiguous — the POST may have landed.
        raise RemoteError(
            f"sideclaw job submit failed (is the LaunchAgent up? curl {_base()}/health)",
            maybe_mutated=True,
        )

    if status != 200:
        raise RemoteError(f"sideclaw returned HTTP {status}: {text[:300]}")

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise RemoteError(f"sideclaw returned HTTP {status} with unparseable body: {text[:300]}")

    job = parsed.get("job") if isinstance(parsed, dict) else None
    if not isinstance(job, dict) or "id" not in job:
        raise RemoteError("sideclaw accepted the job but returned no id")
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
    body = {"tool": "review", "params": params}

    try:
        status, text = _request("POST", "/api/jobs", body)
    except (urllib.error.URLError, TimeoutError, OSError):
        raise RemoteError(
            f"sideclaw review submit failed (is the LaunchAgent up? curl {_base()}/health)",
            maybe_mutated=True,
        )

    if status != 200:
        raise RemoteError(f"sideclaw returned HTTP {status}: {text[:300]}")

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise RemoteError(f"sideclaw returned HTTP {status} with unparseable body: {text[:300]}")

    job = parsed.get("job") if isinstance(parsed, dict) else None
    if not isinstance(job, dict) or "id" not in job:
        raise RemoteError("sideclaw accepted the review job but returned no id")
    return job


def get(job_id: str) -> dict[str, Any] | None:
    try:
        status, text = _request("GET", f"/api/jobs/{job_id}", None)
    except (urllib.error.URLError, TimeoutError, OSError):
        raise RemoteError(f"sideclaw job poll failed for {job_id} (is the LaunchAgent up?)")

    if status == 404:
        return None
    if status != 200:
        raise RemoteError(f"sideclaw returned HTTP {status} for job {job_id}")

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise RemoteError(f"sideclaw returned HTTP {status} for job {job_id} with unparseable body: {text[:300]}")

    job = parsed.get("job") if isinstance(parsed, dict) else None
    if not isinstance(job, dict):
        raise RemoteError(f"sideclaw returned no job for {job_id}")
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
        raise RemoteError(f"sideclaw job cancel failed for {job_id} (is the LaunchAgent up?)")

    if status == 404:
        raise RemoteError(f"sideclaw has no job {job_id}")
    if status == 409:
        try:
            parsed = json.loads(text)
            message = parsed.get("error") if isinstance(parsed, dict) else None
        except json.JSONDecodeError:
            message = None
        raise PolicyError(message or f"job {job_id} is already terminal")
    if status != 200:
        raise RemoteError(f"sideclaw returned HTTP {status} cancelling job {job_id}: {text[:300]}")

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError:
        raise RemoteError(f"sideclaw returned HTTP {status} cancelling job {job_id} with unparseable body: {text[:300]}")

    job = parsed.get("job") if isinstance(parsed, dict) else None
    if not isinstance(job, dict):
        raise RemoteError(f"sideclaw returned no job cancelling {job_id}")
    return job


def assert_result_schema(job: dict[str, Any], expected: int, tool: str) -> None:
    """Raise a LOUD refusal when a terminal `done` job's `result.schemaVersion`
    does not match `expected` — the failure this exists to design out is a
    consumer (this file's own callers in triage.py) silently parsing a verdict
    whose shape moved under it. Every loop poll that reads a `result` off an
    `investigate`/`implement`/`review` job calls this first; the caller is
    expected to land the item `needs_human` with the exact message this
    raises, per DESIGN.md's "deferral must be visible" — a silently-skipped
    item would just hit its deadline instead.

    Only checked on `status == "done"`: a failed/interrupted/cancelled job
    carries no `result` worth pinning a shape to."""
    if job.get("status") != "done":
        return
    result = job.get("result")
    version = result.get("schemaVersion") if isinstance(result, dict) else None
    if version != expected:
        raise RemoteError(
            f"sideclaw {tool} result schemaVersion {version}, warden expects {expected} — refusing to parse"
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
            f"sideclaw {tool} result outcome {outcome!r}, warden expects one of {outcomes} — refusing to parse"
        )


def finished_at_iso(job: dict[str, Any], *, fallback: dt.datetime) -> str:
    """The ledger's `finished_at` should read when sideclaw itself finished
    the job (`JobView.finishedAt`, epoch ms — server/jobs/types.ts), not when
    this process happened to poll it. Every terminal poll response already
    carries this field; warden used to discard it and stamp its own wall
    clock instead, which is why a poll suspended overnight recorded a
    614-minute dispatch that actually took 20 (docs/history/state-log.md
    §79). Falls back to `fallback` (the caller's own `now`) only when
    sideclaw's value is missing or not a number — every real terminal job
    carries one, but a fallback is cheaper than a caller-side branch."""
    raw = job.get("finishedAt")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return dt.datetime.fromtimestamp(raw / 1000, tz=dt.timezone.utc).isoformat()
    return fallback.isoformat()


def check_schema_versions() -> dict[str, dict[str, Any]]:
    """GET /api/dispatch-schema and /api/review-schema, compare `version` AND
    the published `outcomes` set against this module's pinned constants.
    Never raises — a connection failure is reported as `reachable: False` so
    `make status` can print `unreachable` rather than failing outright; only
    an actual version/outcome-set MISMATCH is `ok: False` with `reachable`
    True, the loud-refusal case `assert_result_schema()` enforces per-job."""
    out: dict[str, dict[str, Any]] = {}
    for tool, path, expected_version, expected_outcomes in (
        ("dispatch", "/api/dispatch-schema", DISPATCH_SCHEMA_VERSION, DISPATCH_OUTCOMES),
        ("review", "/api/review-schema", REVIEW_SCHEMA_VERSION, REVIEW_OUTCOMES),
    ):
        try:
            status, text = _request("GET", path, None)
            parsed = json.loads(text) if status == 200 else None
        except (urllib.error.URLError, TimeoutError, OSError, json.JSONDecodeError):
            parsed = None
        if not isinstance(parsed, dict):
            out[tool] = {"reachable": False, "ok": False}
            continue
        remote_version = parsed.get("version")
        remote_outcomes = tuple(parsed.get("outcomes") or ())
        ok = remote_version == expected_version and set(remote_outcomes) == set(expected_outcomes)
        out[tool] = {
            "reachable": True,
            "ok": ok,
            "expectedVersion": expected_version,
            "remoteVersion": remote_version,
            "expectedOutcomes": expected_outcomes,
            "remoteOutcomes": remote_outcomes,
        }
    return out


def classify_dispatch_outcome(status: str | None, verdict_json: str | None) -> tuple[str, str | None]:
    """Classify a finished dispatch's outcome from sideclaw's own published
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
