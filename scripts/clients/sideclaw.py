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
from typing import Any, Callable, Literal, NamedTuple, TypedDict

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


# The two fields warden REQUIRES on a published review finding, named so `check_schema_versions()`
# can compare them against sideclaw's own JSON schema (`/api/review-schema`'s
# `output.properties.blocking.items`) instead of both sides copying by hand. The direction of a
# disagreement is what matters, and only one direction is unsafe: requiring LESS than the
# producer is tolerated (an unmodelled field is simply not read), requiring MORE means rejecting
# real findings — a verdict silently ignored, the failure warden exists to fix. The live producer
# requires `file`, `message` AND `angle`; warden deliberately does not require `angle`, because a
# reader that only quotes what it is given should not refuse a finding over a missing reviewer
# name — so that difference is reported, not treated as a mismatch.
REVIEW_FINDING_REQUIRED = frozenset({"file", "message"})


def is_review_finding(entry: Any) -> bool:
    """True when `entry` is one published review finding.

    Mirrors `server/jobs/handlers/review.ts`'s `FINDING` (schema version
    `REVIEW_SCHEMA_VERSION`): the fields in `REVIEW_FINDING_REQUIRED` are required strings,
    `line` and `angle` are optional for a consumer that only quotes what it is given. This
    lives beside the schema constants because it is part of the same published contract
    `assert_result_schema()` refuses on — a reader must not invent its own idea of the
    shape. It is stricter than the producer in one direction on purpose: a blank `file`/
    `message` is rejected, because a finding warden cannot name is one it cannot report
    (DESIGN.md §115), so a stored payload carrying one is unusable rather than a quote with
    a hole in it.

    The required set is READ from `REVIEW_FINDING_REQUIRED` rather than restated here: the
    drift check compares that same constant against the producer's published schema, and a
    second hand-written copy would be a second place to change and one to forget — with the
    comparison still green while this reader disagreed with it (§136)."""
    if not isinstance(entry, dict):
        return False
    return all(isinstance(entry.get(name), str) and bool(entry[name].strip())
               for name in REVIEW_FINDING_REQUIRED)


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
        entry: dict[str, Any] = {
            "reachable": True,
            "ok": ok,
            "expectedVersion": expected_version,
            "remoteVersion": remote_version,
            "expectedOutcomes": expected_outcomes,
            "remoteOutcomes": remote_outcomes,
        }
        if tool == "review":
            # The finding shape is part of the same published contract and was the one piece
            # nothing compared: `is_review_finding()` mirrors it in Python, so a renamed find
            # shape would leave warden reading a shape the producer no longer emits, with the
            # version number unchanged and every check green.
            result = _published_finding_shape(parsed)
            if isinstance(result, UnreadableFindingShape):
                unreadable: FindingShapeUnreadable = {
                    "published": False, "reason": result.reason}
                entry["findingShape"] = unreadable
                entry["ok"] = ok = False
            else:
                shape = result
                missing_required = sorted(REVIEW_FINDING_REQUIRED - shape.required)
                missing_properties = sorted(REVIEW_FINDING_REQUIRED - shape.properties)
                # Names are not the whole contract: a field kept but retyped (a scalar that
                # became an array, say) leaves every name in place while `is_review_finding()`
                # rejects every finding the producer emits — the whole verdict silently
                # unreadable, with this check reporting success. Only `str` reads as a finding
                # in warden, so anything else — including a published property with no readable
                # type — is a disagreement, not a tolerated difference.
                mistyped = sorted(field for field in REVIEW_FINDING_REQUIRED
                                  if field in shape.properties
                                  and shape.types.get(field) != "string")
                report: FindingShapeComparison = {
                    "published": True,
                    "required": sorted(shape.required),
                    "properties": sorted(shape.properties),
                    "types": dict(sorted(shape.types.items())),
                    "wardenRequires": sorted(REVIEW_FINDING_REQUIRED),
                    "missingFromRequired": missing_required,
                    "missingFromProperties": missing_properties,
                    "mistyped": mistyped,
                }
                entry["findingShape"] = report
                if missing_required or missing_properties or mistyped:
                    entry["ok"] = ok = False
        out[tool] = entry
    return out


class FindingShapeUnreadable(TypedDict):
    """The endpoint publishes no readable finding shape, and then there is nothing to describe.

    No empty lists here on purpose: `[]` reads as "the producer requires nothing", which is the
    fabricated-clean-bill shape this branch keeps having to remove. An unreadable shape is a
    disagreement, not an empty comparison.

    `reason` names the requirement the body failed (§135). "No readable finding shape" is true of
    a body with no `blocking` at all and of one whose `blocking` stopped being an array, and the
    two send an operator to different files."""

    published: Literal[False]
    reason: str


class FindingShapeComparison(TypedDict):
    """What the published finding shape holds, against what warden requires of it."""

    published: Literal[True]
    required: list[str]
    properties: list[str]
    types: dict[str, Any]
    wardenRequires: list[str]
    missingFromRequired: list[str]
    missingFromProperties: list[str]
    mistyped: list[str]


# `published` discriminates the union, so a reader that narrows on it can subscript the rest —
# which is the point of typing this at all. It was a bare string-keyed dict written here and read
# by bracket in two other modules, where a renamed key type-checked and failed only at the read:
# the pattern `ReviewRow` was promoted out of on this branch.
FindingShapeReport = FindingShapeUnreadable | FindingShapeComparison


class UnreadableFindingShape(NamedTuple):
    """Why the published finding object could not be read, in terms of the requirement it failed.

    A returned value rather than `None`, for the reason `FindingShapeUnreadable.reason` exists
    one level up: `None` cannot tell "this endpoint publishes no finding object" apart from "it
    publishes one whose `blocking` is no longer an array", and only the second one means the
    runtime's own reading of that container has drifted (§135)."""

    reason: str


class PublishedFindingShape(NamedTuple):
    """What the endpoint publishes about the review finding object.

    `types` maps each published property to its JSON Schema `type` (None when the property
    carries no readable subschema), because the NAME of a field is not the whole contract: a
    producer that keeps `file` and changes it from a string to an array of strings leaves
    every name in place while `is_review_finding()` rejects every finding it emits."""

    required: frozenset[str]
    properties: frozenset[str]
    types: dict[str, Any]


def _as_object(value: Any) -> dict[str, Any] | None:
    """A JSON object, or None. The guard `_published_finding_shape()` applies at every level of
    the published body, so a malformed or drifted schema reads as "not published" — the refusal
    this check exists to produce — instead of raising out of a function nothing wraps."""
    return value if isinstance(value, dict) else None


class _UnreadableShape(Exception):
    """Internal: a `_require_object()` step that did not find its object.

    Never escapes `_published_finding_shape()`: it exists so the seven-step descent can be
    written as a descent instead of seven copies of the same two-line guard plus a return. Its
    message IS the refusal reason, which is what keeps the wording of each step in one place
    (§139)."""


def _require_object(container: dict[str, Any], key: str, what: str) -> dict[str, Any]:
    """The object at `container[key]`, or a refusal naming `what`.

    The rule at every level is the same — a level that is not a non-empty object makes the shape
    unreadable, and the reason has to name the level — so it is written once. `_as_object()` is
    the type test; an empty object is treated as absent, because a schema with no fields at that
    level says nothing either."""
    value = _as_object(container.get(key))
    if not value:
        raise _UnreadableShape(f"no {what} in the published schema")
    return value


def _published_finding_shape(
        parsed: dict[str, Any]) -> PublishedFindingShape | UnreadableFindingShape:
    """The finding object sideclaw publishes, or WHY it could not be read.

    Read from `output.properties.blocking.items` — the four arrays (`blocking`,
    `improvements`, `discussions`, `testGaps`) share this one object schema. An unreadable
    shape is a finding in itself, not a reason to skip the check: warden's
    `is_review_finding()` would then be an unverifiable copy, which is the drift this
    comparison exists to catch.

    The CONTAINERS are part of the contract, not only the field names inside them (§135):
    `blocking` must be exactly an array of objects, and not `["array", "null"]` either. A
    producer that made the container object-shaped or null-capable while keeping the same
    `items` would leave every field name green here and have every verdict rejected at runtime,
    because `_review_verdict_problems()` iterates `blocking` as a list.

    Every step is guarded, and the guards are the point rather than boilerplate: this runs on
    the operator-facing `make status` path OUTSIDE `check_schema_versions()`'s try/except, so
    the one body it must survive is the malformed one it is there to report. `required` is a
    list or tuple OF NAMES or it is nothing: `frozenset()` over a bare string would read as a
    set of its own characters (`{"f", "i", "l", "e"}` passing a name check it should fail) and
    a number would raise out of a function documented never to raise."""
    # One guard per level, each saying "not published, and here is what moved" rather than letting
    # anything out: the body this has to survive is precisely the malformed one it exists to
    # report. `_UnreadableShape` is caught at the bottom and becomes that return value.
    try:
        output = _require_object(parsed, "output", "`output` object")
        container = _require_object(output, "properties", "`output.properties` object")
        # …and the shape only matters if a NON-CLEAN outcome promises to publish it (§137). The
        # runtime reads that promise: `_review_verdict_problems()` reports `outcome 'actionable'
        # publishes no `blocking` list`, and such a verdict is unusable, so a producer that made
        # `blocking` optional in `output.required` while keeping this exact item schema would
        # leave every field and container green here and have every actionable review parked as
        # unusable. A conditional requirement (`if`/`then`) cannot be read as a plain promise, so
        # it is refused rather than assumed.
        output_required = output.get("required")
        if not isinstance(output_required, (list, tuple)) or not all(
                isinstance(name, str) for name in output_required):
            raise _UnreadableShape(
                "`output.required` is not a list of names, so the promise that a non-clean "
                "outcome publishes `blocking` cannot be read")
        if "blocking" not in output_required:
            raise _UnreadableShape(
                "a non-clean outcome must publish `blocking`, and `output.required` does not "
                "list it (a conditional requirement is not readable here): the runtime refuses a "
                "findings-shaped verdict without it, so every actionable review would be unusable")
        blocking = _require_object(
            container, "blocking", "`output.properties.blocking` schema object")
        if blocking.get("type") != "array":
            raise _UnreadableShape(
                "`output.properties.blocking` is not exactly an array "
                f"(type={blocking.get('type')!r}), and the runtime iterates it as a list")
        items = _require_object(
            blocking, "items", "`output.properties.blocking.items` object schema")
        if items.get("type") != "object":
            raise _UnreadableShape(
                "`output.properties.blocking.items` is not exactly an object "
                f"(type={items.get('type')!r}), and warden reads its fields by name")
        properties = _as_object(items.get("properties"))
        if not properties:
            raise _UnreadableShape(
                "`output.properties.blocking.items` publishes no fields")
        required = items.get("required", [])
        if not isinstance(required, (list, tuple)):
            # Catches `str` too, which is the case that matters: `frozenset("file")` is four
            # characters, and an int raises. Both are unreadable shapes, not missing fields.
            raise _UnreadableShape(
                f"`items.required` is a {type(required).__name__}, not a list of names")
        if not all(isinstance(name, str) for name in list(required) + list(properties)):
            raise _UnreadableShape(
                "`items.required`/`items.properties` carry non-string, non-field names")
        return PublishedFindingShape(
            frozenset(required),
            frozenset(properties),
            {name: (_as_object(subschema) or {}).get("type")
             for name, subschema in properties.items()},
        )
    except _UnreadableShape as exc:
        return UnreadableFindingShape(str(exc))


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
