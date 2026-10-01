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

# The names warden's runtime READS OFF a RESULT envelope, and therefore has to be promised by the
# producer's published `output.required` (§140). `assert_result_schema()` reads `schemaVersion`,
# `assert_outcome()` reads `outcome`, and `_review_verdict_problems()` reads `blocking`; all three
# fail CLOSED on a result that lacks one, which is why a producer that stopped requiring one —
# without touching its version or its outcome vocabulary — would leave this comparison green and
# have every review parked `needs_human` with nothing saying why. A tuple, not a set: the refusal
# names them in the order the runtime depends on them.
REVIEW_OUTPUT_REQUIRED = ("schemaVersion", "outcome", "blocking")


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
                # `entry["ok"]` is the value the caller reads; the local `ok` was only ever
                # a dead store here (nothing reads it after this line — the dict was built
                # with it above).
                entry["ok"] = False
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
                    entry["ok"] = False
            # The envelope is the third contract, and the one the `version`/`outcomes` comparison
            # above cannot see: those are the endpoint's own metadata, while these are the fields
            # the runtime reads by VALUE off every result and refuses one that fails (§143). A
            # producer can keep its metadata identical and still park every review by retyping
            # `schemaVersion` or widening `outcome`'s vocabulary.
            envelope_result = _published_envelope_shape(parsed)
            if isinstance(envelope_result, UnreadableEnvelopeShape):
                unreadable_envelope: EnvelopeShapeUnreadable = {
                    "published": False, "reason": envelope_result.reason}
                entry["envelopeShape"] = unreadable_envelope
                entry["ok"] = False
            else:
                envelope = envelope_result
                # Only the direction that breaks the runtime is a refusal, as everywhere else on
                # this check: an outcome the producer may emit and warden refuses is a review
                # parked per job, while a warden outcome the producer no longer emits is a branch
                # of ours gone unreachable — reported by the caller, not refused (§143).
                unknown_outcomes = sorted(set(envelope.outcomes) - set(REVIEW_OUTCOMES))
                version_pinned = _version_is_pinned(envelope.version_const, envelope.version_enum)
                envelope_report: EnvelopeShapeComparison = {
                    "published": True,
                    "versionTypes": sorted(envelope.version_types),
                    "versionConst": envelope.version_const,
                    "versionEnum": (list(envelope.version_enum)
                                    if envelope.version_enum is not None else None),
                    "versionPinned": version_pinned,
                    "outcomes": list(envelope.outcomes),
                    "wardenVersion": REVIEW_SCHEMA_VERSION,
                    "wardenOutcomes": sorted(REVIEW_OUTCOMES),
                    "unknownOutcomes": unknown_outcomes,
                }
                entry["envelopeShape"] = envelope_report
                if unknown_outcomes or not version_pinned:
                    entry["ok"] = False
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
    types: dict[str, str | None]
    wardenRequires: list[str]
    missingFromRequired: list[str]
    missingFromProperties: list[str]
    mistyped: list[str]


# `published` discriminates the union, so a reader that narrows on it can subscript the rest —
# which is the point of typing this at all. It was a bare string-keyed dict written here and read
# by bracket in two other modules, where a renamed key type-checked and failed only at the read:
# the pattern `ReviewRow` was promoted out of on this branch.
FindingShapeReport = FindingShapeUnreadable | FindingShapeComparison


class EnvelopeShapeUnreadable(TypedDict):
    """The endpoint publishes no readable result envelope, so there is nothing to compare.

    No empty lists and no `None` here on purpose, for the reason `FindingShapeUnreadable` states one
    level up: `[]` reads as "the producer constrains nothing", which is the fabricated-clean-bill
    shape this branch keeps removing. An unreadable envelope is a disagreement."""

    published: Literal[False]
    reason: str


class EnvelopeShapeComparison(TypedDict):
    """What the published result envelope holds, against what the runtime reads from it.

    `unknownOutcomes` is the refusal (`assert_outcome()` would park every review carrying one) and
    `versionTypeOk`/`versionConstOk` are the other two; the rest is the evidence an operator needs
    to see where the producer moved."""

    published: Literal[True]
    versionTypes: list[str]
    versionConst: object
    versionEnum: list[object] | None
    versionPinned: bool
    outcomes: list[str]
    wardenVersion: int
    wardenOutcomes: list[str]
    unknownOutcomes: list[str]


EnvelopeShapeReport = EnvelopeShapeUnreadable | EnvelopeShapeComparison


class UnreadableFindingShape(NamedTuple):
    """Why the published finding object could not be read, in terms of the requirement it failed.

    A returned value rather than `None`, for the reason `FindingShapeUnreadable.reason` exists
    one level up: `None` cannot tell "this endpoint publishes no finding object" apart from "it
    publishes one whose `blocking` is no longer an array", and only the second one means the
    runtime's own reading of that container has drifted (§135)."""

    reason: str


def _as_schema_type(value: Any) -> str | None:
    """A JSON Schema `type` as a string, or None when it is not one.

    `None` is the same answer the dict already gives for an absent subschema, and the only
    consumer compares against `"string"`, so a `type` that is a number, a list or an object
    already reads as a disagreement (§142). Coercing it here is what lets the field be typed
    `str | None` instead of `Any`: the mapping never claims to hold a type it did not read, and
    `{"type": 7}` reports the field as mistyped rather than comparing 7 to "string" by accident."""
    return value if isinstance(value, str) else None


def _as_schema_types(value: Any) -> frozenset[str]:
    """The JSON Schema `type` keyword as a set: the dialect allows a LIST of types, and only the
    string entries name a type (`frozenset()` for anything else, so an unreadable keyword reads as
    "no type published" rather than raising on the operator's `make status` path)."""
    if isinstance(value, str):
        return frozenset({value})
    if isinstance(value, (list, tuple)):
        return frozenset(entry for entry in value if isinstance(entry, str))
    return frozenset()


def _pins_version(value: object) -> bool:
    """Whether one published value pins the version warden compares.

    `==` alone is not enough: `True == 1` in Python, so a `const: true` would read as a pin on the
    number 1 — the accidental-equality class this branch keeps having to remove (§147)."""
    return not isinstance(value, bool) and value == REVIEW_SCHEMA_VERSION


def _version_is_pinned(const: object, enum: tuple[object, ...] | None) -> bool:
    """Whether the published `schemaVersion` schema PINS the version, not merely admits it.

    A `type` says which values are POSSIBLE; only a pin says which value is promised. `{"type":
    "number"}` admits `2`, and `{"type": ["integer", "string"]}` admits `"1"` — both are refused by
    `assert_result_schema()`, which compares the VALUE, so a type-only rule reports success for a
    producer that can park every review it emits. `const: 1` or a one-member `enum: [1]` is the pin
    this reads; a multi-member enum is not one, because the producer could emit the other value."""
    if _pins_version(const):
        return True
    return enum is not None and len(enum) == 1 and _pins_version(enum[0])


class PublishedEnvelopeShape(NamedTuple):
    """What the endpoint publishes about the two result fields warden reads by VALUE.

    `_published_finding_shape()` covers `output.properties.blocking.items` — the noun of a review —
    but a review's ENVELOPE carries two more contracts and neither was compared: the runtime's
    `assert_result_schema()` refuses any result whose `schemaVersion` is not
    `REVIEW_SCHEMA_VERSION` (a strict `!=`, so a producer that retypes it to a string refuses every
    review it emits), and `assert_outcome()` refuses any result whose `outcome` is not in
    `REVIEW_OUTCOMES`. Comparing only the top-level `version`/`outcomes` metadata and the trio's
    presence in `output.required` left a producer free to change both and have every live review
    parked with this check reporting success (§143).

    `version_const`/`version_enum` are `object` and not a string type on purpose: a `const` and an
    `enum` member are whatever JSON values the producer wrote, and the only thing warden does with
    them is compare them to an int (`_pins_version()`)."""

    version_types: frozenset[str]
    version_const: object
    version_enum: tuple[object, ...] | None
    outcomes: tuple[str, ...]


class UnreadableEnvelopeShape(NamedTuple):
    """Why the published result envelope could not be read, in terms of the requirement it failed.

    A returned value rather than `None`, for the reason the finding-shape pair next door exists:
    "this endpoint publishes no envelope fields" and "it publishes an `outcome` with no `enum`" send
    an operator to different lines, and a bare `None` cannot tell them apart (§139)."""

    reason: str


class PublishedFindingShape(NamedTuple):
    """What the endpoint publishes about the review finding object.

    `types` maps each published property to its JSON Schema `type` (None when the property
    carries no readable subschema, or carries one that is not a string), because the NAME of a
    field is not the whole contract: a producer that keeps `file` and changes it from a string to
    an array of strings leaves every name in place while `is_review_finding()` rejects every
    finding it emits."""

    required: frozenset[str]
    properties: frozenset[str]
    types: dict[str, str | None]


def _as_object(value: Any) -> dict[str, Any] | None:
    """A JSON object, or None. The guard `_published_finding_shape()` applies at every level of
    the published body, so a malformed or drifted schema reads as "not published" — the refusal
    this check exists to produce — instead of raising out of a function nothing wraps."""
    return value if isinstance(value, dict) else None


class _UnreadableShape(Exception):
    """Internal: a `_require_object()` step that did not find its object.

    Never escapes `_published_finding_shape()` or `_published_envelope_shape()`: it exists so each
    descent can be written as a descent instead of one copy of the same two-line guard per level.
    Its message IS the refusal reason, which is what keeps the wording of each step in one place
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


def _require_names(container: dict[str, Any], key: str, what: str, *,
                   because: str | None = None) -> list[str]:
    """The list of names at `container[key]`, or a refusal naming `what`.

    The rule is the same at both levels and is written once: a value that is not a list or tuple of
    names makes the shape unreadable, and a bare string is the case that matters (`frozenset("file")`
    is four characters and would pass a name check it should fail). `because` carries the reason a
    particular level gives, so the wording stays with the requirement it explains (§139)."""
    value = container.get(key)
    if not isinstance(value, (list, tuple)):
        raise _UnreadableShape(
            f"{what} is a {type(value).__name__}, not a list of names"
            + (f", {because}" if because else ""))
    names = list(value)
    if not all(isinstance(name, str) for name in names):
        # A name that is not a name is not a missing field, it is an unreadable list: a set built
        # from `[7]` would silently contain no names at all, and one built from a nested list would
        # raise out of a function documented never to raise.
        raise _UnreadableShape(f"{what} carries non-string, non-field names")
    return names


def _require_type_keyword(schema: dict[str, Any], what: str, expected: str, why: str) -> None:
    """Refuse unless `schema["type"]` is exactly `expected`, saying what the runtime does instead.

    Both callers exist for the same reason — a container whose JSON Schema shape no longer matches
    how warden reads it (a list it does not iterate, an object it does not subscript) leaves every
    field name green while every verdict is rejected — so the guard is written once with the
    consequence passed in."""
    found = schema.get("type")
    if found != expected:
        article = "an" if expected[:1] in "aeiou" else "a"
        raise _UnreadableShape(
            f"{what} is not exactly {article} {expected} (type={found!r}), and {why}")


def _require_output_promise(output: dict[str, Any]) -> None:
    """`output.required` must be a list of names AND must promise the whole envelope warden reads.

    The shape only matters if a NON-CLEAN outcome promises to publish it (§137): the runtime reports
    `outcome 'actionable' publishes no `blocking` list` and such a verdict is unusable, so a producer
    that made `blocking` optional while keeping this exact item schema would leave every field and
    container green here and have every actionable review parked as unusable. A conditional
    requirement (`if`/`then`) cannot be read as a plain promise, so it is refused rather than assumed.

    The three names are the whole envelope, not only `blocking` (§140): the runtime reads
    `schemaVersion`, `outcome` and `blocking` off every result and fails closed without any of them.
    Names checked against the LIVE producer, which lists them all — this is a check, not a refusal of
    the real shape."""
    output_required = _require_names(
        output, "required", "`output.required`",
        because="so the promise that a non-clean outcome publishes `blocking` cannot be read")
    unpromised = [name for name in REVIEW_OUTPUT_REQUIRED if name not in output_required]
    if unpromised:
        names = ", ".join(f"`{name}`" for name in unpromised)
        raise _UnreadableShape(
            f"`output.required` does not list {names}, and the runtime reads "
            f"{'that' if len(unpromised) == 1 else 'those'} off every result and fails closed "
            "without it (a conditional requirement is not readable here): a producer that "
            "stopped requiring one would leave this comparison green while every affected "
            "review was parked as unusable")


def _published_finding_shape(
        parsed: dict[str, Any]) -> PublishedFindingShape | UnreadableFindingShape:
    """The finding object sideclaw publishes, or WHY it could not be read.

    Read from `output.properties.blocking.items`, because `blocking` is the one array warden's
    runtime READS: `_review_verdict_problems()` iterates it and filters every entry through
    `is_review_finding()`, while `improvements`/`discussions`/`testGaps` are never inspected
    (`test_validation_actionable_with_empty_blocking_confirms` is the executable statement of
    that: they may be empty, or anything else, and the item still confirms and merges). An
    unreadable shape is a finding in itself, not a reason to skip the check: warden's
    `is_review_finding()` would then be an unverifiable copy, which is the drift this
    comparison exists to catch.

    This used to claim the four arrays "share this one object schema", which was never checked
    and is not true of the live producer (§142): `improvements` and `discussions` do publish the
    identical item object, but `testGaps` publishes `{"items": {"type": "string"}}` — strings,
    not findings. Asserting structural identity across all four would therefore refuse the shape
    sideclaw actually serves, and checking the three object arrays would police a shape nothing
    in warden reads. The check is `blocking`'s item object, and the docstring now says so.

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
        _require_output_promise(output)
        blocking = _require_object(
            container, "blocking", "`output.properties.blocking` schema object")
        _require_type_keyword(
            blocking, "`output.properties.blocking`", "array",
            "the runtime iterates it as a list")
        items = _require_object(
            blocking, "items", "`output.properties.blocking.items` object schema")
        _require_type_keyword(
            items, "`output.properties.blocking.items`", "object",
            "warden reads its fields by name")
        properties = _as_object(items.get("properties"))
        if not properties:
            raise _UnreadableShape(
                "`output.properties.blocking.items` publishes no fields")
        # `required`'s entries are checked by `_require_names()`, and `properties`' keys are JSON
        # object keys — always strings by the time `json.loads()` is done with them — so the guard
        # that used to sit here (`all(isinstance(name, str) for name in required + properties)`)
        # could not fire (§146). Removed rather than kept as reassurance: a check that cannot fail
        # reads as coverage the reader does not have.
        required = _require_names(items, "required", "`items.required`")
        return PublishedFindingShape(
            frozenset(required),
            frozenset(properties),
            {name: _as_schema_type((_as_object(subschema) or {}).get("type"))
             for name, subschema in properties.items()},
        )
    except _UnreadableShape as exc:
        return UnreadableFindingShape(str(exc))


def _published_envelope_shape(
        parsed: dict[str, Any]) -> PublishedEnvelopeShape | UnreadableEnvelopeShape:
    """The envelope fields sideclaw publishes, or WHY they could not be read.

    Read from `output.properties.schemaVersion` and `output.properties.outcome`, because those are
    the two fields the runtime reads by VALUE off every result: `assert_result_schema()` compares
    `result["schemaVersion"]` to `REVIEW_SCHEMA_VERSION` with a strict `!=`, and `assert_outcome()`
    tests `result["outcome"] in REVIEW_OUTCOMES`. Both fail CLOSED — the item lands `needs_human` —
    which is why a producer that drifts here is not a silent mis-parse but a fleet-wide parking, and
    why the check that exists to catch contract drift has to read them (§143).

    What is required of `schemaVersion` is a PIN, not a type: `version_const` and `version_enum`
    are read so the caller can tell whether the producer promises exactly the version warden
    compares, which is the only thing `assert_result_schema()` can rely on (`_version_is_pinned()`).
    Of `outcome` an `enum` of strings is required: `output.properties.outcome` without one constrains
    nothing, and the runtime's membership test is the only thing that would have caught an unknown
    value — at the point where it is too late to call it drift.

    Every step is guarded for the reason the finding reader's are: this runs OUTSIDE
    `check_schema_versions()`'s try/except, on the operator-facing `make status` path, and the one
    body it must survive is the malformed one it is there to report."""
    try:
        output = _require_object(parsed, "output", "`output` object")
        container = _require_object(output, "properties", "`output.properties` object")
        version = _require_object(
            container, "schemaVersion", "`output.properties.schemaVersion` property")
        outcome = _require_object(container, "outcome", "`output.properties.outcome` property")
        published_enum = outcome.get("enum")
        if not isinstance(published_enum, (list, tuple)) or not all(
                isinstance(value, str) for value in published_enum):
            raise _UnreadableShape(
                "`output.properties.outcome` publishes no `enum` of outcome names, so nothing "
                "promises the producer stays inside warden's vocabulary while the runtime refuses "
                "any value outside it")
        raw_version_enum = version.get("enum")
        return PublishedEnvelopeShape(
            _as_schema_types(version.get("type")),
            version.get("const"),
            (tuple(raw_version_enum) if isinstance(raw_version_enum, (list, tuple)) else None),
            tuple(published_enum),
        )
    except _UnreadableShape as exc:
        return UnreadableEnvelopeShape(str(exc))


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
