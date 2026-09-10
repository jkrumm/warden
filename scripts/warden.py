#!/usr/bin/env python3
"""warden — the CLI over warden's lifecycle modules.

The Python successor to the retired bash dispatch bridge: same closed verb
set, same "a dispatch names a repo, never a path" rule, same brief-is-data
rule, same recursion guard, same --json contract and audit log shape — now a
thin argument-parsing and JSON-rendering layer over `scripts/clients/` and
`scripts/lifecycle/`, which own everything this file used to embed as
`python3 -c '...'` fragments.

VERBS
  dispatch <repo>    open an episode (brief on stdin, never argv)
  status <job-id>    poll one
  list [scope]        open | today | all
  merge <job-id>      land the draft PR a dispatch opened (--why --confirm)
  abort <event-id>    cancel an in-flight implement/validate episode (--why)
  revert <event-id>   record a revert PR against a merged item (--pr --why)
  help

Global flags, anywhere on the line, `--flag value` or `--flag=value`:
  --json --confirm --dry-run --wait
  --why --tier --brief-file --context-file
  --origin-channel --origin-thread --origin-event --auto-from-item --model --pr

There is deliberately no `--brief`: the brief is data, never an argv string.

EXIT CODES  0 ok · 2 precondition failed · 3 remote failed · 4 policy/budget
            refusal · 64 usage error
AUDIT LOG   $WARDEN_CLI_LOG, default ~/Library/Logs/warden-cli.log
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_SCRIPTS = Path(__file__).resolve().parent
if str(REPO_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(REPO_SCRIPTS))

import ledger  # noqa: E402
from clients import github, sideclaw  # noqa: E402
from clients.errors import PolicyError, PreconditionError, RemoteError, UsageError, WardenError  # noqa: E402
from lifecycle import approvals, dispatch, items, merge, operations, policy  # noqa: E402

WAIT_TIMEOUT = 170
WAIT_INTERVAL = 5

_EFFECTS = {
    "investigate": [
        "opens a read-only session inside the repo",
        "returns a verdict; changes nothing anywhere",
    ],
    "author": [
        "opens a read-only session inside the repo",
        "files ONE GitHub issue in that repo, or none if it finds nothing worth tracking",
    ],
    "implement": [
        "cuts a fresh dispatch/… branch in an ISOLATED worktree, never the live checkout",
        "lets the session edit files and run the validators the repo defines",
        "commits, pushes that branch, and opens a DRAFT pull request",
    ],
}
_NEVER = [
    "never merges anything",
    "never pushes to a default branch, in any repo, including direct-to-master ones",
    "never touches .github/workflows or .github/actions",
    "never mutates infrastructure — that is hermes-ops.sh, not this",
]


# --- CLI-local state, for the audit line -------------------------------------


@dataclass
class _State:
    verb: str | None = None
    tier: str | None = None
    target: str | None = None
    approved_by: str | None = None
    did_mutate: bool = False
    dry_run: bool = False
    planned: bool = False
    start_monotonic: float = field(default_factory=time.monotonic)


def _redact(text: str) -> str:
    """Mask anything that looks like a credential — length >= 24, mixed case,
    a digit, and no `/` or `:` (which would mark it as a path or an op://
    ref / URL, both fine to keep readable). Mirrors the retired bash CLI's
    own `redact()`."""
    out = []
    for w in text.replace("\n", " ").split():
        if (
            len(w) >= 24
            and any(c.isupper() for c in w)
            and any(c.isdigit() for c in w)
            and "/" not in w
            and ":" not in w
        ):
            out.append("<redacted>")
        else:
            out.append(w)
    return " ".join(out) or "-"


def _mode(verb: str | None, state: _State) -> str:
    if verb in ("dispatch", "merge"):
        if state.did_mutate:
            return "merged" if verb == "merge" else "opened"
        if state.dry_run:
            return "dry-run"
        if state.planned:
            return "planned"
        return "refused"
    if verb == "abort":
        return "aborted" if state.did_mutate else "refused"
    if verb == "revert":
        return "reverted" if state.did_mutate else "refused"
    return "read"


def _audit_log_path() -> Path:
    if os.environ.get("WARDEN_CLI_LOG"):
        return Path(os.environ["WARDEN_CLI_LOG"]).expanduser()
    return Path.home() / "Library" / "Logs" / "warden-cli.log"


def _write_audit(state: _State, rc: int, args_str: str, why: str | None) -> None:
    ts = dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    dur = int(time.monotonic() - state.start_monotonic)
    mode = _mode(state.verb, state)
    line = "%s | verb=%s | mode=%s | tier=%s | args=%s | target=%s | rc=%s | dur=%ss | approved_by=%s | why=%s\n" % (
        ts,
        state.verb or "-",
        mode,
        state.tier or "-",
        _redact(args_str or "-"),
        state.target or "-",
        rc,
        dur,
        state.approved_by or "-",
        _redact(why or "-"),
    )
    try:
        log_path = _audit_log_path()
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("a", encoding="utf-8") as f:
            f.write(line)
    except OSError:
        pass


# --- preconditions ------------------------------------------------------------


# require_no_recursion() itself now lives in lifecycle/policy.py — called at
# the top of open_episode() and at the LAND step of plan_or_land(), the two
# places that actually mutate, so the guard holds even for a caller that
# skips this CLI. Re-exported here (not just imported at call sites) so
# every early `require_no_recursion()` call below still reads a clear error
# BEFORE this process reads stdin, rather than failing deeper in.
require_no_recursion = policy.require_no_recursion


def _secrets_run_path() -> Path:
    if os.environ.get("WARDEN_SECRETS_RUN"):
        return Path(os.environ["WARDEN_SECRETS_RUN"]).expanduser()
    return Path.home() / ".local" / "bin" / "secrets-run"


def require_backend() -> None:
    backend_file = (
        Path(os.environ["SECRETS_BACKEND_FILE"]).expanduser()
        if os.environ.get("SECRETS_BACKEND_FILE")
        else Path(os.environ.get("XDG_CONFIG_HOME") or (Path.home() / ".config")) / "secrets" / "backend"
    )
    try:
        backend = backend_file.read_text(encoding="utf-8").strip()
    except OSError:
        backend = ""
    if backend == "cache":
        pass
    elif backend == "op":
        if not sys.stdin.isatty():
            raise PreconditionError(
                "secrets backend is 'op' and there is no TTY — a biometric prompt would hang here. "
                "Run this on the Mac mini (backend 'cache') or from an interactive shell."
            )
    else:
        raise PreconditionError(
            f"no secrets backend marker at {backend_file} — refusing to run rather than hang on an "
            "interactive 'op' prompt. This script belongs on the Mac mini."
        )
    secrets_run = _secrets_run_path()
    if not (secrets_run.is_file() and os.access(secrets_run, os.X_OK)):
        raise PreconditionError(f"secrets-run not found at {secrets_run}")


# --- argument parsing ---------------------------------------------------------


@dataclass
class Flags:
    json: bool = False
    confirm: bool = False
    dry_run: bool = False
    wait: bool = False
    why: str | None = None
    tier: str | None = None
    brief_file: str | None = None
    context_file: str | None = None
    origin_channel: str | None = None
    origin_thread: str | None = None
    origin_event: str | None = None
    auto_from_item: str | None = None
    model: str | None = None
    pr: str | None = None


_VALUE_FLAGS = {
    "--why": "why",
    "--tier": "tier",
    "--brief-file": "brief_file",
    "--context-file": "context_file",
    "--origin-channel": "origin_channel",
    "--origin-thread": "origin_thread",
    "--origin-event": "origin_event",
    "--auto-from-item": "auto_from_item",
    "--model": "model",
    "--pr": "pr",
}
_BOOL_FLAGS = {"--json": "json", "--confirm": "confirm", "--dry-run": "dry_run", "--wait": "wait"}


def _parse_args(argv: list[str]) -> tuple[Flags, list[str]]:
    flags = Flags()
    positional: list[str] = []
    i = 0
    n = len(argv)
    while i < n:
        a = argv[i]
        if a == "--brief" or a.startswith("--brief="):
            raise UsageError(
                "there is no --brief: the brief is data, not an argument. Pass it on stdin with a "
                "QUOTED heredoc (<<'BRIEF') or via --brief-file <path>."
            )
        if a in _BOOL_FLAGS:
            setattr(flags, _BOOL_FLAGS[a], True)
            i += 1
            continue
        if a in _VALUE_FLAGS:
            if i + 1 >= n:
                raise UsageError(f"{a} needs a value")
            setattr(flags, _VALUE_FLAGS[a], argv[i + 1])
            i += 2
            continue
        eq_matched = False
        for flag, attr in _VALUE_FLAGS.items():
            prefix = flag + "="
            if a.startswith(prefix):
                setattr(flags, attr, a[len(prefix):])
                eq_matched = True
                break
        if eq_matched:
            i += 1
            continue
        if a in ("-h", "--help"):
            positional.append("help")
            i += 1
            continue
        if a.startswith("-"):
            raise UsageError(f"unknown flag: {a}")
        positional.append(a)
        i += 1
    return flags, positional


def _parse_int(text: str | None, label: str) -> int:
    if text is None or not text.isdigit():
        raise UsageError(f"{label} must be an integer (got: {text})")
    return int(text)


def _sanitized_argv(original_argv: list[str]) -> list[str]:
    """The argv a mint stores for audit purposes: the original invocation
    minus the --brief-file/--context-file operands (their bytes are stored
    separately, as stdin_text/context_text), plus --json."""
    out: list[str] = []
    skip_next = False
    for a in original_argv:
        if skip_next:
            skip_next = False
            continue
        if a in ("--brief-file", "--context-file"):
            skip_next = True
            continue
        if a.startswith("--brief-file=") or a.startswith("--context-file="):
            continue
        out.append(a)
    if "--json" not in out:
        out.append("--json")
    return out


def _approval_params(flags: Flags, origin_event: int | None) -> dict[str, Any]:
    params: dict[str, Any] = {}
    if flags.why:
        params["why"] = flags.why
    if flags.model:
        params["model"] = flags.model
    if flags.origin_channel:
        params["origin_channel"] = flags.origin_channel
    if flags.origin_thread:
        params["origin_thread_ts"] = flags.origin_thread
    if origin_event is not None:
        params["origin_event_id"] = origin_event
    return params


def _read_brief(flags: Flags) -> str:
    if flags.brief_file:
        p = Path(flags.brief_file)
        if not p.is_file():
            raise UsageError(f"--brief-file not found: {flags.brief_file}")
        return p.read_text(encoding="utf-8")
    if sys.stdin.isatty():
        raise UsageError(
            "no brief on stdin. Pass it with a QUOTED heredoc (warden dispatch <repo> <<'BRIEF' ... "
            "BRIEF) or --brief-file <path>. The brief is never an argv string — see the module docstring."
        )
    return sys.stdin.read()


def _read_context(flags: Flags) -> str | None:
    if not flags.context_file:
        return None
    p = Path(flags.context_file)
    if not p.is_file():
        raise UsageError(f"--context-file not found: {flags.context_file}")
    return p.read_text(encoding="utf-8")


def _budget_payload(conn, now: dt.datetime) -> dict[str, Any] | None:
    limits = policy.limits_from_env()
    counts = policy.budget_counts(conn, now)
    return policy.budget_json(counts, limits)


# --- dispatch ------------------------------------------------------------------


def _plan_payload(
    conn, *, name: str, tier: str, target: policy.RepoTarget, brief: str, why: str | None,
    needs_confirm: bool, now: dt.datetime,
) -> dict[str, Any]:
    note = "nothing ran — no episode was opened and no budget was consumed"
    if needs_confirm:
        note += (
            ". This tier is GATED: an approval request is posted to Slack (if --origin-channel was "
            "given) and Johannes must approve it there before anything runs. Passing --confirm is not "
            "possible on dispatch any more — approving happens in Slack."
        )
    out: dict[str, Any] = {
        "verb": "dispatch",
        "ok": True,
        "dryRun": True,
        "needsConfirm": needs_confirm,
        "repo": name,
        "tier": tier,
        "repoMaxTier": target.max_tier,
        "cwd": str(target.path),
        "briefChars": len(brief),
        "why": why or None,
        "wouldDo": _EFFECTS[tier],
        "wouldNeverDo": _NEVER,
        "note": note,
    }
    budget = _budget_payload(conn, now)
    if budget:
        out["budget"] = budget
    return out


def _result_payload(
    conn, *, job_id: str, name: str, tier: str | None, job: dict[str, Any], waited: bool,
    from_record: bool = False,
) -> dict[str, Any]:
    result = job.get("result")
    r = result if isinstance(result, dict) else {}
    out: dict[str, Any] = {
        "verb": "dispatch",
        "ok": job.get("status") == "done",
        "jobId": job_id,
        "repo": name,
        "tier": tier or "-",
        "status": job.get("status"),
        "waited": waited,
        "elapsedMs": job.get("elapsedMs"),
        "artifactUrl": r.get("artifactUrl"),
        "branch": r.get("branch"),
        "verdict": result,
        "error": job.get("error"),
    }
    if from_record:
        out["fromRecord"] = True
    budget = _budget_payload(conn, dt.datetime.now(dt.timezone.utc))
    if budget:
        out["budget"] = budget
    return out


def cmd_dispatch(conn, flags: Flags, positional: list[str], state: _State, original_argv: list[str]) -> dict[str, Any]:
    if not positional:
        raise UsageError("usage: warden dispatch <repo> [--tier investigate] [--wait] [--json] <<'BRIEF' ... BRIEF")
    name = positional[0]

    require_no_recursion()
    require_backend()

    if flags.confirm:
        raise UsageError("the Approve button in Slack runs an approved implement; --confirm is a merge flag")

    target = policy.resolve_repo(name)
    tier = flags.tier or "investigate"
    policy.resolve_tier(tier, target)
    state.tier = tier

    linked_job: str | None = None
    if flags.auto_from_item:
        linked_job = policy.require_auto_from_item(conn, event_id=flags.auto_from_item, repo=name, tier=tier)

    if tier in policy.GATED_TIERS and not flags.why:
        raise UsageError(
            f"tier '{tier}' requires --why \"<reason>\". It lands in the audit log and is the record of "
            "why an unattended episode was allowed to write. There is no default."
        )

    policy.valid_origin(channel=flags.origin_channel, thread_ts=flags.origin_thread, event_id=flags.origin_event)
    origin_event_int = int(flags.origin_event) if flags.origin_event else None

    brief = dispatch.normalize_brief(_read_brief(flags))
    context = dispatch.check_context(_read_context(flags))

    state.target = f"{name}:{tier}"
    now = dt.datetime.now(dt.timezone.utc)
    policy.budget_counts(conn, now)  # read for reporting; a rehearsal spends nothing

    needs_confirm = tier == "implement" and not flags.auto_from_item

    if flags.dry_run:
        return _plan_payload(conn, name=name, tier=tier, target=target, brief=brief, why=flags.why,
                              needs_confirm=needs_confirm, now=now)

    if needs_confirm:
        state.planned = True
        approvals.mint(
            conn, verb="dispatch", repo=name, tier=tier, body=brief, why=flags.why, context=context,
            channel=flags.origin_channel, params=_approval_params(flags, origin_event_int),
            argv=_sanitized_argv(original_argv),
        )
        return _plan_payload(conn, name=name, tier=tier, target=target, brief=brief, why=flags.why,
                              needs_confirm=needs_confirm, now=now)

    limits = policy.limits_from_env()
    counts = policy.budget_counts(conn, now)
    policy.check_dispatch_budget(counts, tier, limits)

    if flags.auto_from_item:
        policy.check_repo_not_in_flight(conn, repo=name)
        authorized_by = f"triage:item-{flags.auto_from_item}:job-{linked_job}"
        state.approved_by = authorized_by
    else:
        authorized_by = None

    origin = dispatch.Origin(
        channel=flags.origin_channel or None, thread_ts=flags.origin_thread or None, event_id=origin_event_int,
    )
    opened = dispatch.open_episode(
        conn, target=target, tier=tier, brief=brief, context=context, why=flags.why, model=flags.model,
        origin=origin, authorized_by=authorized_by, now=now,
    )
    state.did_mutate = True
    state.target = f"{name}:{tier}:{opened.job_id}"

    if flags.wait:
        job = sideclaw.wait(opened.job_id, timeout_s=WAIT_TIMEOUT, interval_s=WAIT_INTERVAL)
        if job is None:
            budget = _budget_payload(conn, dt.datetime.now(dt.timezone.utc))
            out: dict[str, Any] = {
                "verb": "dispatch", "ok": True, "jobId": opened.job_id, "repo": name, "tier": tier,
                "status": "running", "waited": True, "waitedSeconds": WAIT_TIMEOUT,
                "note": "Still running after the in-turn wait. The dispatch record is written, so the "
                        "sweeper will deliver the verdict into the origin thread — say so and move on "
                        "rather than waiting again.",
            }
            if budget:
                out["budget"] = budget
            return out
        dispatch.sync_record(conn, job, reported=True)
        return _result_payload(conn, job_id=opened.job_id, name=name, tier=tier, job=job, waited=True)

    budget = _budget_payload(conn, dt.datetime.now(dt.timezone.utc))
    out = {
        "verb": "dispatch", "ok": True, "jobId": opened.job_id, "repo": name, "tier": tier,
        "status": opened.job.get("status") or "queued", "waited": False,
        "note": f"Episode opened. It is NOT finished — poll with `warden status {opened.job_id}`, "
                "or let the 5-minute sweeper deliver the verdict into the origin thread.",
    }
    if budget:
        out["budget"] = budget
    return out


def cmd_status(conn, flags: Flags, positional: list[str], state: _State) -> dict[str, Any]:
    if not positional:
        raise UsageError("usage: warden status <job-id> [--json]")
    job_id = positional[0]
    if not sideclaw.valid_job_id(job_id):
        raise UsageError(f"not a valid job id: {job_id}")
    state.target = job_id

    job = sideclaw.get(job_id)
    from_record = False
    if job is None:
        row = conn.execute("SELECT * FROM dispatches WHERE job_id=?", (job_id,)).fetchone()
        if row is None:
            raise UsageError(f"no such job: {job_id}")
        if row["status"] not in sideclaw.TERMINAL:
            raise RemoteError(
                f"sideclaw no longer has job {job_id} and the dispatch record never saw it finish — "
                "the verdict is lost"
            )
        result = json.loads(row["verdict_json"]) if row["verdict_json"] else None
        job = {"id": job_id, "status": row["status"], "result": result, "finishedAt": row["finished_at"]}
        from_record = True
    elif job.get("status") in sideclaw.TERMINAL:
        dispatch.sync_record(conn, job, reported=False)

    return _result_payload(conn, job_id=job_id, name="-", tier=None, job=job, waited=False, from_record=from_record)


def cmd_list(conn, flags: Flags, positional: list[str], state: _State) -> dict[str, Any]:
    scope = positional[0] if positional else "open"
    state.target = scope
    now = dt.datetime.now(dt.timezone.utc)
    rows = dispatch.list_dispatches(conn, scope, now)
    out: dict[str, Any] = {"verb": "list", "ok": True, "scope": scope, "count": len(rows), "dispatches": rows}
    budget = _budget_payload(conn, now)
    if budget:
        out["budget"] = budget
    return out


def cmd_merge(conn, flags: Flags, positional: list[str], state: _State) -> dict[str, Any]:
    if not positional:
        raise UsageError('usage: warden merge <job-id> --why "<reason>" --confirm [--json]')
    job_id = positional[0]

    require_no_recursion()
    require_backend()

    if not sideclaw.valid_job_id(job_id):
        raise UsageError(f"not a valid job id: {job_id}")
    state.target = job_id

    row = conn.execute("SELECT tier FROM dispatches WHERE job_id=?", (job_id,)).fetchone()
    if row is not None:
        state.tier = row["tier"]

    now = dt.datetime.now(dt.timezone.utc)
    result = merge.plan_or_land(
        conn, job_id=job_id, why=flags.why or "", confirm=flags.confirm, dry_run=flags.dry_run,
        authorized_by="cli:confirm", now=now,
    )
    if isinstance(result, merge.MergeResult):
        state.did_mutate = True
        return {"verb": "merge", "ok": True, "merged": True, **result.to_json()}

    if flags.dry_run:
        state.dry_run = True
    else:
        state.planned = True
    return {"verb": "merge", "ok": True, **result.to_json()}


def cmd_abort(conn, flags: Flags, positional: list[str], state: _State) -> dict[str, Any]:
    require_no_recursion()
    # No require_backend(): abort only ever reaches sideclaw's cancel
    # endpoint, never GitHub — no token to resolve.
    if not positional:
        raise UsageError('usage: warden abort <event-id> --why "<reason>" [--json]')
    event_id = _parse_int(positional[0], "event-id")
    if not flags.why:
        raise UsageError('abort requires --why "<reason>" (it is what lands in the audit log)')
    state.target = str(event_id)

    row = conn.execute(
        "SELECT event_id, state, dispatch_job, implement_job, validation_job FROM triage_items WHERE event_id=?",
        (event_id,),
    ).fetchone()
    if row is None:
        raise UsageError(f"no triage_items row for event_id {event_id}")

    job_by_state = {
        "investigating": row["dispatch_job"],
        "implementing": row["implement_job"],
        "validating": row["validation_job"],
    }
    if row["state"] not in job_by_state:
        raise PolicyError(
            f"triage item {event_id} is in state '{row['state']}', not investigating/implementing/"
            "validating — there is no in-flight episode to abort"
        )
    job_id = job_by_state[row["state"]]

    cancelled = False
    if job_id:
        try:
            sideclaw.cancel(job_id)
            cancelled = True
        except RemoteError as exc:
            if "no job" not in str(exc).lower():
                raise

    now = dt.datetime.now(dt.timezone.utc)
    if job_id:
        now_iso = now.isoformat()
        conn.execute(
            "UPDATE dispatches SET status='cancelled', finished_at=COALESCE(finished_at, ?), "
            "reported_at=COALESCE(reported_at, ?), delivery_status=COALESCE(delivery_status, "
            "'undeliverable:aborted') WHERE job_id=?",
            (now_iso, now_iso, job_id),
        )

    for op in conn.execute(
        "SELECT op_id FROM operations WHERE event_id=? AND outcome IS NULL", (event_id,)
    ).fetchall():
        operations.complete(conn, op["op_id"], outcome="failed", receipt=json.dumps({"aborted": flags.why}))

    items.transition(conn, event_id, to_state=ledger.STATE_CLOSED, now=now, note=f"aborted: {flags.why}")
    conn.commit()
    state.did_mutate = True

    return {
        "verb": "abort", "ok": True, "eventId": event_id, "jobId": job_id, "cancelled": cancelled,
        "state": ledger.STATE_CLOSED,
    }


_REVERTABLE_STATES = ("merged", "liveness_pending", "fixed")


def cmd_revert(conn, flags: Flags, positional: list[str], state: _State) -> dict[str, Any]:
    require_no_recursion()
    require_backend()  # reads the pull request via GitHub below — needs the token
    if not positional:
        raise UsageError('usage: warden revert <event-id> --pr <number> --why "<reason>" [--json]')
    event_id = _parse_int(positional[0], "event-id")
    if not flags.pr:
        raise UsageError("revert requires --pr <number>")
    pr_number = _parse_int(flags.pr, "--pr")
    if not flags.why:
        raise UsageError('revert requires --why "<reason>" (it is what lands in the audit log)')
    state.target = str(event_id)

    row = conn.execute("SELECT event_id, state, repo FROM triage_items WHERE event_id=?", (event_id,)).fetchone()
    if row is None:
        raise UsageError(f"no triage_items row for event_id {event_id}")
    if row["state"] not in _REVERTABLE_STATES:
        raise PolicyError(
            f"triage item {event_id} is in state '{row['state']}', not one of "
            f"{'/'.join(_REVERTABLE_STATES)} — there is nothing merged to revert"
        )
    repo = row["repo"]
    if not repo:
        raise PreconditionError(f"triage item {event_id} has no recorded repo")

    pr = github.read_pr(github.GH_OWNER, repo, pr_number)
    head_repo = ((pr.get("head") or {}).get("repo") or {}).get("full_name")
    if head_repo != f"{github.GH_OWNER}/{repo}":
        raise PolicyError(
            f"pull request #{pr_number} is on '{head_repo}', not {github.GH_OWNER}/{repo} — a fork or a "
            "different repo is never accepted as this item's revert"
        )
    if pr.get("state") not in ("open", "closed"):
        raise RemoteError(f"GitHub's pull request response for {github.GH_OWNER}/{repo}#{pr_number} has no usable state")

    now = dt.datetime.now(dt.timezone.utc)
    items.transition(
        conn, event_id, to_state=ledger.STATE_REVERTED, now=now,
        note=f"reverted by PR #{pr_number}: {flags.why}", extra={"revert_pr": pr_number},
    )
    conn.commit()
    state.did_mutate = True

    return {"verb": "revert", "ok": True, "eventId": event_id, "pullRequest": pr_number, "state": ledger.STATE_REVERTED}


# --- plain-text rendering (best-effort; --json is the primary contract) ------


def _print_result_text(out: dict[str, Any]) -> None:
    print(f"status: {out.get('status')}")
    v = out.get("verdict") or {}
    if v:
        print(f"summary: {v.get('summary', '')}")
        print(f"confidence: {v.get('confidence')} | next: {v.get('nextAction')}")
        if out.get("artifactUrl"):
            print(f"artifact: {out['artifactUrl']}")
        elif out.get("branch"):
            print(f"branch pushed, no PR: {out['branch']}")
    elif out.get("error"):
        print(f"error: {out['error']}")


def _print_budget_text(out: dict[str, Any]) -> None:
    b = out.get("budget")
    if not b:
        return
    print(f"budget: {b['usedToday']}/{b['max']} dispatches today · implement {b['implementToday']}/{b['implementMax']}")


def _print_text(verb: str | None, out: dict[str, Any]) -> None:
    if verb == "dispatch":
        if out.get("dryRun"):
            print("PLAN — nothing executed.")
            print(f"  repo:  {out['repo']} ({out.get('cwd', '')})")
            print(f"  tier:  {out['tier']} (repo ceiling: {out.get('repoMaxTier')})")
            print(f"  brief: {out.get('briefChars')} chars")
            if out.get("why"):
                print(f"  why:   {out['why']}")
            if out.get("needsConfirm"):
                print("GATED — an approval request has been posted; nothing runs until Johannes approves it in Slack.")
            else:
                print("Re-invoke without --dry-run to open the episode.")
        elif out.get("waited"):
            _print_result_text(out)
        else:
            print(f"dispatch opened: {out['jobId']} ({out['repo']}, tier {out['tier']})")
            print(f"not finished — poll: warden status {out['jobId']}")
        _print_budget_text(out)
    elif verb == "status":
        _print_result_text(out)
        _print_budget_text(out)
    elif verb == "list":
        rows = out.get("dispatches") or []
        if not rows:
            print("no dispatches")
        for r in rows:
            print(f"{r['job_id'][:8]}  {r['status']:<11} {r['repo']:<18} {r['tier']:<11} {r['created_at'][:19]}")
        _print_budget_text(out)
    else:
        print(json.dumps(out, indent=2))


_HELP_TEXT = """\
warden — the CLI over warden's lifecycle modules.

VERBS
  dispatch <repo>    open an episode (brief on stdin, never argv)
  status <job-id>    poll one
  list [scope]        open | today | all
  merge <job-id>      land the draft PR a dispatch opened (--why --confirm)
  abort <event-id>    cancel an in-flight implement/validate episode (--why)
  revert <event-id>   record a revert PR against a merged item (--pr --why)
  help

Global flags, anywhere on the line, --flag value or --flag=value:
  --json --confirm --dry-run --wait
  --why --tier --brief-file --context-file
  --origin-channel --origin-thread --origin-event --auto-from-item --model --pr

There is deliberately no --brief: the brief is data, never an argv string.
Pass it on stdin with a QUOTED heredoc (<<'BRIEF' ... BRIEF) or --brief-file.

EXIT CODES  0 ok · 2 precondition failed · 3 remote failed · 4 policy/budget
            refusal · 64 usage error
AUDIT LOG   $WARDEN_CLI_LOG, default ~/Library/Logs/warden-cli.log
"""


def _print_help() -> None:
    print(_HELP_TEXT, end="")


# --- main ----------------------------------------------------------------------


def main(argv: list[str]) -> int:
    state = _State()
    rc = 0
    json_flag = "--json" in argv
    why: str | None = None
    args_str = "-"

    try:
        flags, positional = _parse_args(argv)
        json_flag = flags.json
        why = flags.why
        state.dry_run = flags.dry_run

        verb = positional[0] if positional else ""
        state.verb = verb or None
        rest = positional[1:]
        args_str = " ".join(positional[1:]) if positional else "-"

        if verb in ("", "help"):
            _print_help()
            return 0

        # `ledger.connect(migrate=False)` asserts the schema version and
        # raises a plain RuntimeError on any mismatch (including "no schema
        # at all yet") — never a WardenError, because ledger.py has no
        # dependency on clients/errors.py. The CLI never migrates, so that
        # RuntimeError is this process's own precondition failure, not a bug.
        try:
            conn = ledger.connect(migrate=False)
        except RuntimeError as exc:
            raise PreconditionError(str(exc)) from exc
        try:
            if verb == "dispatch":
                out = cmd_dispatch(conn, flags, rest, state, argv)
            elif verb == "status":
                out = cmd_status(conn, flags, rest, state)
            elif verb == "list":
                out = cmd_list(conn, flags, rest, state)
            elif verb == "merge":
                out = cmd_merge(conn, flags, rest, state)
            elif verb == "abort":
                out = cmd_abort(conn, flags, rest, state)
            elif verb == "revert":
                out = cmd_revert(conn, flags, rest, state)
            else:
                raise UsageError(
                    f"unknown verb: {verb}\n"
                    "valid verbs:\n"
                    "  dispatch <repo>   open an episode (brief on stdin)\n"
                    "  status <job-id>   poll one\n"
                    "  list [scope]      open | today | all\n"
                    "  merge <job-id>    land the draft PR that dispatch opened (--why --confirm)\n"
                    "  abort <event-id>  cancel an in-flight implement/validate episode (--why)\n"
                    "  revert <event-id> record a revert PR against a merged item (--pr --why)\n"
                    "  help"
                )
        finally:
            conn.close()

        if json_flag:
            print(json.dumps(out, indent=2))
        else:
            _print_text(state.verb, out)
        return 0

    except WardenError as exc:
        rc = exc.exit_code
        if json_flag:
            print(json.dumps({"verb": state.verb, "ok": False, "exitCode": rc, "error": str(exc)}, indent=2))
        else:
            print(f"warden: {exc}", file=sys.stderr)
        return rc
    finally:
        _write_audit(state, rc, args_str, why)


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
