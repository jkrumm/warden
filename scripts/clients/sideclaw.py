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

import json
import os
import re
import time
import urllib.error
import urllib.request
from typing import Any, Callable

from .errors import PolicyError, RemoteError

_DEFAULT_BASE = "http://localhost:7705"
_TIMEOUT_S = 30

# sideclaw's own JobStatus. "cancelled" arrives with the cancel endpoint this
# wave adds.
TERMINAL = frozenset({"done", "failed", "interrupted", "cancelled"})

_JOB_ID_RE = re.compile(r"^[A-Za-z0-9-]+$")


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
