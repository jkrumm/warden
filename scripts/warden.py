#!/usr/bin/env python3
"""warden — the CLI over warden's lifecycle modules.

The Python successor to the retired bash dispatch bridge: same closed verb
set, same "a dispatch names a repo, never a path" rule, same brief-is-data
rule, same recursion guard, same --json contract and audit log shape — now a
thin argument-parsing and JSON-rendering layer over `scripts/clients/` and
`scripts/lifecycle/`, which own everything this file used to embed as
`python3 -c '...'` fragments.

VERBS
  dispatch <repo>    open a BARE episode, no item (brief on stdin, never argv)
  run <repo>         open an ITEM riding the alert lifecycle (investigate first,
                     implement only if the verdict says so and policy allows;
                     brief on stdin) — the intake for a human or for Hermes
  status <job-id>    poll one
  list [scope]        open | today | all
  merge <job-id>      land the draft PR a dispatch opened (--why --confirm)
  abort <event-id>    cancel an in-flight implement/validate episode (--why)
  revert <event-id>   record a revert PR against a merged item (--pr --why)
  close <event-id>    resolve an open item by hand (--why required)
  help

Global flags, anywhere on the line, `--flag value` or `--flag=value`:
  --json --confirm --dry-run --wait
  --why --tier --brief-file --context-file
  --origin-channel --origin-thread --origin-event --auto-from-item --model --pr

There is deliberately no `--brief`: the brief is data, never an argv string.

EXIT CODES  0 ok · 2 precondition failed · 3 remote failed · 4 policy
            refusal · 64 usage error
AUDIT LOG   $WARDEN_CLI_LOG, default ~/Library/Logs/warden-cli.log
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sys
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

REPO_SCRIPTS = Path(__file__).resolve().parent
if str(REPO_SCRIPTS) not in sys.path:
    sys.path.insert(0, str(REPO_SCRIPTS))

import ledger  # noqa: E402
# Wave 6.1's `run` verb opens a triage_items row through triage.py's own
# open_origin_item()/escalate_origin_items() — the same functions the loop
# calls — rather than a second copy of that logic here.
import triage  # noqa: E402
from clients import github, sideclaw  # noqa: E402
from clients.errors import PolicyError, PreconditionError, RemoteError, UsageError, WardenError  # noqa: E402
from lifecycle import dispatch, items, merge, operations, policy  # noqa: E402

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
    noop: bool = False
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
    if verb in ("dispatch", "run", "merge"):
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
    if verb == "close":
        if state.did_mutate:
            return "closed"
        if state.noop:
            return "noop"
        if state.dry_run:
            return "dry-run"
        return "refused"
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


# --- dispatch ------------------------------------------------------------------


def _plan_payload(
    conn, *, name: str, tier: str, target: policy.RepoTarget, brief: str, now: dt.datetime,
) -> dict[str, Any]:
    out: dict[str, Any] = {
        "verb": "dispatch",
        "ok": True,
        "dryRun": True,
        "repo": name,
        "tier": tier,
        "repoMaxTier": target.max_tier,
        "cwd": str(target.path),
        "briefChars": len(brief),
        "wouldDo": _EFFECTS[tier],
        "wouldNeverDo": _NEVER,
        "note": "nothing ran — no episode was opened",
    }
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
    return out


def cmd_dispatch(conn, flags: Flags, positional: list[str], state: _State) -> dict[str, Any]:
    if not positional:
        raise UsageError("usage: warden dispatch <repo> [--tier investigate] [--wait] [--json] <<'BRIEF' ... BRIEF")
    name = positional[0]

    require_no_recursion()
    require_backend()

    if flags.confirm:
        raise UsageError("--confirm is a merge flag; dispatch has no confirmation step")

    target = policy.resolve_repo(name)
    tier = flags.tier or "investigate"
    policy.resolve_tier(tier, target)
    state.tier = tier

    linked_job: str | None = None
    if flags.auto_from_item:
        linked_job = policy.require_auto_from_item(conn, event_id=flags.auto_from_item, repo=name, tier=tier)

    policy.valid_origin(channel=flags.origin_channel, thread_ts=flags.origin_thread, event_id=flags.origin_event)
    origin_event_int = int(flags.origin_event) if flags.origin_event else None

    brief = dispatch.normalize_brief(_read_brief(flags))
    context = dispatch.check_context(_read_context(flags))

    state.target = f"{name}:{tier}"
    now = dt.datetime.now(dt.timezone.utc)

    if flags.dry_run:
        return _plan_payload(conn, name=name, tier=tier, target=target, brief=brief, now=now)

    if flags.auto_from_item:
        policy.check_repo_not_in_flight(conn, repo=name)
        authorized_by = f"triage:item-{flags.auto_from_item}:job-{linked_job}"
    else:
        authorized_by = "cli:dispatch"
    state.approved_by = authorized_by

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
            out: dict[str, Any] = {
                "verb": "dispatch", "ok": True, "jobId": opened.job_id, "repo": name, "tier": tier,
                "status": "running", "waited": True, "waitedSeconds": WAIT_TIMEOUT,
                "note": "Still running after the in-turn wait. The dispatch record is written, so the "
                        "sweeper will deliver the verdict into the origin thread — say so and move on "
                        "rather than waiting again.",
            }
            return out
        dispatch.sync_record(conn, job, reported=True)
        return _result_payload(conn, job_id=opened.job_id, name=name, tier=tier, job=job, waited=True)

    out = {
        "verb": "dispatch", "ok": True, "jobId": opened.job_id, "repo": name, "tier": tier,
        "status": opened.job.get("status") or "queued", "waited": False,
        "note": f"Episode opened. It is NOT finished — poll with `warden status {opened.job_id}`, "
                "or let the 5-minute sweeper deliver the verdict into the origin thread.",
    }
    return out


# --- run (Wave 6.1: the `human` origin) --------------------------------------


def _run_plan_payload(conn, *, name: str, tier: str, target: policy.RepoTarget, brief: str,
                       why: str | None, now: dt.datetime) -> dict[str, Any]:
    out: dict[str, Any] = {
        "verb": "run", "ok": True, "dryRun": True, "repo": name, "tier": tier,
        "repoMaxTier": target.max_tier, "cwd": str(target.path), "briefChars": len(brief),
        "why": why or None,
        "note": "nothing ran — no item was opened and no episode was dispatched",
    }
    return out


def cmd_run(conn, flags: Flags, positional: list[str], state: _State) -> dict[str, Any]:
    """`run` opens an ITEM riding the same lifecycle an alert does
    (investigating -> verdict -> auto-implement -> validating -> merged ->
    ...), starting at `investigate` and reaching `implement` only if the
    verdict says so AND policy allows it. `dispatch` stays the bare-episode,
    no-item door. `run` is the intake for a human
    typing at a terminal and for Hermes answering in a Slack thread."""
    if not positional:
        raise UsageError(
            "usage: warden run <repo> [--tier investigate|implement] [--wait] [--json] "
            "<<'BRIEF' ... BRIEF"
        )
    name = positional[0]

    require_no_recursion()
    require_backend()

    target = policy.resolve_repo(name)
    tier = flags.tier or "investigate"
    policy.resolve_tier(tier, target)
    state.tier = tier

    if tier == "implement" and not flags.why:
        raise UsageError(
            'tier \'implement\' requires --why "<reason>". It lands in the audit log and is the '
            "record of why this item was allowed to auto-implement on its own verdict, with no "
            "default."
        )

    policy.valid_origin(channel=flags.origin_channel, thread_ts=flags.origin_thread)

    brief = dispatch.normalize_brief(_read_brief(flags))
    max_tier = "implement" if tier == "implement" else "investigate"

    state.target = f"{name}:{tier}"
    now = dt.datetime.now(dt.timezone.utc)

    if flags.dry_run:
        return _run_plan_payload(conn, name=name, tier=tier, target=target, brief=brief, why=flags.why, now=now)

    event_id = triage.open_origin_item(
        conn, origin="human", repo=name, brief=brief, max_tier=max_tier,
        external_id=f"human:{uuid.uuid4()}", title=(flags.why or brief.splitlines()[0] or name)[:200],
        payload={"why": flags.why}, now=now,
        origin_channel=flags.origin_channel or None, origin_thread_ts=flags.origin_thread or None,
    )
    if event_id is None:
        # open_origin_item() only returns None for an ALREADY-TERMINAL item at
        # this exact external_id — unreachable here, since external_id is a
        # fresh uuid4 on every call and can never collide with an existing
        # row. Kept as a named refusal rather than an assert so a future
        # external_id scheme change fails loudly here instead of silently
        # dropping the brief.
        raise PreconditionError("warden run: could not open an item for this brief (no event_id returned)")

    triage.escalate_origin_items(conn, now)
    conn.commit()
    state.did_mutate = True

    item = conn.execute("SELECT * FROM triage_items WHERE event_id=?", (event_id,)).fetchone()
    item_state = item["state"] if item else None
    job_id = item["dispatch_job"] if item else None
    queued = item_state == triage.STATE_NEW

    out: dict[str, Any] = {
        "verb": "run", "ok": True, "eventId": event_id, "jobId": job_id, "state": item_state,
        "origin": "human", "maxTier": max_tier, "repo": name, "queued": queued,
        "note": item["note"] if item else None,
    }

    if flags.wait and job_id:
        job = sideclaw.wait(job_id, timeout_s=WAIT_TIMEOUT, interval_s=WAIT_INTERVAL)
        out["waited"] = True
        if job is None:
            out["waitedSeconds"] = WAIT_TIMEOUT
            out["note"] = (
                "Still running after the in-turn wait. The item and its dispatch record are written, "
                "so the sweeper will deliver the verdict into the origin thread — say so and move on "
                "rather than waiting again."
            )
            return out
        dispatch.sync_record(conn, job, reported=True)
        # Fold the verdict onto the item's own row NOW — the same call
        # dispatch-sweep.py's process_dispatch() makes once a dispatch
        # reaches a terminal status. sync_record() above just stamped
        # `reported_at`, and dispatch-sweep.py's own sweep only ever folds a
        # dispatch with `reported_at IS NULL` — without this call the item
        # would sit in `investigating` forever, its job already done, folded
        # by nothing (live item 989's bug). dispatch-sweep.py's own path is
        # unaffected: it always folds before it ever reports, so a dispatch
        # this call already folded is simply a no-op repeat fold for it.
        try:
            triage.fold_dispatch_verdict(conn, origin_event_id=event_id, job_id=job_id,
                                          now=dt.datetime.now(dt.timezone.utc), dry_run=False)
        except Exception as e:  # a folding failure must never hide the verdict itself
            print(f"warden: run --wait: folding the verdict onto the item failed: {e}", file=sys.stderr)
        folded_item = conn.execute("SELECT * FROM triage_items WHERE event_id=?", (event_id,)).fetchone()
        if folded_item is not None:
            out["state"] = folded_item["state"]
            out["note"] = folded_item["note"]
        # Derived the same way _result_payload() derives it for `dispatch`,
        # so Hermes's in-turn answer keeps the same shape regardless of
        # which verb opened the episode — nested under `result` rather than
        # returned as this verb's own payload, since `run`'s own top-level
        # shape (eventId/state/origin/maxTier/queued) has no `dispatch`
        # equivalent to collide with.
        result = job.get("result")
        r = result if isinstance(result, dict) else {}
        out["status"] = job.get("status")
        out["result"] = {
            "artifactUrl": r.get("artifactUrl"),
            "branch": r.get("branch"),
            "verdict": result,
            "error": job.get("error"),
        }
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

    if flags.dry_run:
        # The global dry-run contract (DESIGN.md): nothing outward-facing, and
        # nothing written. `abort` used to fall straight through and cancel the
        # episode for real — the one verb where "--dry-run" meant "do it".
        state.dry_run = True
        return {
            "verb": "abort", "ok": True, "dryRun": True, "eventId": event_id, "jobId": job_id,
            "fromState": row["state"], "toState": ledger.STATE_CLOSED, "cancelled": False,
            "discharged": [],
            "note": "nothing written — no cancel, no transition row, no state change",
        }

    cancelled = False
    if job_id:
        try:
            sideclaw.cancel(job_id)
            cancelled = True
        except RemoteError as exc:
            if "no job" not in str(exc).lower():
                raise
        except PolicyError:
            # sideclaw's 409 — the job is already terminal (a sibling's abort,
            # an idle-watchdog kill, a run that finished between our read and
            # this call). The abort's own intent, "no episode is running
            # against this cluster", already holds, so this is not a failure to
            # report: refusing here would abandon the whole cluster, because
            # `close` refuses every in-flight state and the sweep never folds a
            # row whose `reported_at` is already stamped (as it is below).
            pass

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

    # A cluster shares ONE `dispatch_job` (the same membership rule
    # `triage.fold_dispatch_verdict()` uses to fold a verdict onto every row),
    # so cancelling that job ends the episode for every member — not just the
    # one named here. Leaving the siblings in an in-flight state strands them
    # with no exit at all: `close` refuses in-flight states by design, a second
    # `abort` refuses because its job is already terminal, and the sweep only
    # reads rows with `reported_at IS NULL`, which the stamp below clears. The
    # only thing left for such a row is its deadline, which files a
    # needs_human card for work a human has already decided against.
    discharged: list[int] = []
    if job_id:
        for member in conn.execute(
            "SELECT event_id, state FROM triage_items WHERE dispatch_job=? AND event_id<>? "
            "ORDER BY event_id",
            (job_id, event_id),
        ).fetchall():
            if member["state"] in _CLUSTER_ABORT_STATES:
                items.transition(conn, member["event_id"], to_state=ledger.STATE_CLOSED, now=now,
                                 note=f"aborted: {flags.why}")
                discharged.append(member["event_id"])

    conn.commit()
    state.did_mutate = True

    return {
        "verb": "abort", "ok": True, "eventId": event_id, "jobId": job_id, "cancelled": cancelled,
        "state": ledger.STATE_CLOSED, "discharged": discharged,
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


# --- close ---------------------------------------------------------------------

# States a human can resolve by hand: no episode or operation is in flight,
# and the item is not already terminal. Mirrors the closed-allowlist shape
# used elsewhere in this repo (items._EXTRA_COLUMNS, triage.py's own
# _NEVER_CARDED_FIRST_STATES) — a state not named here is refused, never
# silently allowed.
_CLOSE_ALLOWED_STATES = (
    triage.STATE_NEW, triage.STATE_VERDICT, triage.STATE_NEEDS_HUMAN,
    triage.STATE_MERGE_BLOCKED, triage.STATE_QUIET, triage.STATE_NOTE,
)
# An episode or operation is in flight for these — `abort` is the verb for
# that, not `close`.
_CLOSE_INFLIGHT_STATES = (
    triage.STATE_INVESTIGATING, triage.STATE_IMPLEMENTING, triage.STATE_VALIDATING,
    triage.STATE_REMEDIATING, triage.STATE_LIVENESS_PENDING, triage.STATE_PR_OPEN,
)
# The narrower set `abort` discharges for the CLUSTER siblings sharing the
# cancelled job: exactly the states an episode puts a row in. A sibling that
# has already moved past the episode (`pr_open`, `merge_blocked`,
# `needs_human`) carries work of its own — a PR a human must review — and is
# never closed behind their back by cancelling the job.
_CLUSTER_ABORT_STATES = (
    triage.STATE_INVESTIGATING, triage.STATE_IMPLEMENTING, triage.STATE_VALIDATING,
)
# Already terminal — closing again is a no-op, not a refusal.
_CLOSE_TERMINAL_STATES = (
    ledger.STATE_CLOSED, triage.STATE_FIXED, triage.STATE_IGNORED, triage.STATE_DISMISSED,
)


def cmd_close(conn, flags: Flags, positional: list[str], state: _State) -> dict[str, Any]:
    """Resolve an open item by hand — the terminal counterpart to `abort`
    (which cancels an in-flight episode) for the items nothing is currently
    running against: a `needs_human`/`merge_blocked` card the owner answered
    outside warden entirely, or a `new`/`verdict`/`quiet`/`note` item that
    needs no further action. Writes through the same `items.transition()`
    every other CLI-only transition uses, so the state change and its
    `item_transitions` row are the loop's own shape, not a hand-rolled UPDATE."""
    require_no_recursion()
    # No require_backend(): close only ever writes triage_items/item_transitions
    # in the local ledger — no GitHub or sideclaw call to authenticate for.
    if not positional:
        raise UsageError('usage: warden close <event-id> --why "<reason>" [--json]')
    event_id = _parse_int(positional[0], "event-id")
    if not flags.why:
        raise UsageError(
            'close requires --why "<reason>" (it lands in the audit log and becomes the item\'s note)'
        )
    state.target = str(event_id)

    row = conn.execute("SELECT event_id, state FROM triage_items WHERE event_id=?", (event_id,)).fetchone()
    if row is None:
        raise UsageError(f"no triage_items row for event_id {event_id}")
    current_state = row["state"]

    if current_state in _CLOSE_TERMINAL_STATES:
        state.noop = True
        return {
            "verb": "close", "ok": True, "eventId": event_id, "fromState": current_state,
            "toState": current_state,
            "note": f"item {event_id} is already terminal (state={current_state}) — nothing to do",
        }

    if current_state not in _CLOSE_ALLOWED_STATES:
        in_flight = current_state in _CLOSE_INFLIGHT_STATES
        raise PreconditionError(
            f"triage item {event_id} is in state '{current_state}'"
            + (
                " — an episode or operation is in flight; 'abort' is the verb for that, not 'close'"
                if in_flight
                else " — 'close' has no closed-allowlist entry for this state"
            )
        )

    to_state = ledger.STATE_CLOSED
    note = f"closed by hand: {flags.why}"

    if flags.dry_run:
        state.dry_run = True
        return {
            "verb": "close", "ok": True, "dryRun": True, "eventId": event_id, "fromState": current_state,
            "toState": to_state, "note": "nothing written — no transition row, no state change",
        }

    now = dt.datetime.now(dt.timezone.utc)
    items.transition(conn, event_id, to_state=to_state, now=now, note=note)
    conn.commit()
    state.did_mutate = True

    return {
        "verb": "close", "ok": True, "eventId": event_id, "fromState": current_state, "toState": to_state,
        "note": note,
    }


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


def _print_text(verb: str | None, out: dict[str, Any]) -> None:
    if verb == "dispatch":
        if out.get("dryRun"):
            print("PLAN — nothing executed.")
            print(f"  repo:  {out['repo']} ({out.get('cwd', '')})")
            print(f"  tier:  {out['tier']} (repo ceiling: {out.get('repoMaxTier')})")
            print(f"  brief: {out.get('briefChars')} chars")
            print("Re-invoke without --dry-run to open the episode.")
        elif out.get("waited"):
            _print_result_text(out)
        else:
            print(f"dispatch opened: {out['jobId']} ({out['repo']}, tier {out['tier']})")
            print(f"not finished — poll: warden status {out['jobId']}")
    elif verb == "run":
        if out.get("dryRun"):
            print("PLAN — nothing executed.")
            print(f"  repo:  {out['repo']} ({out.get('cwd', '')})")
            print(f"  tier:  {out['tier']} (repo ceiling: {out.get('repoMaxTier')})")
            print(f"  brief: {out.get('briefChars')} chars")
            if out.get("why"):
                print(f"  why:   {out['why']}")
            print("Re-invoke without --dry-run to open the item.")
        else:
            print(f"item opened: event {out['eventId']} ({out['repo']}, state {out['state']})")
            if out.get("queued"):
                print(f"queued — {out.get('note') or 'waiting for a free slot'}")
            elif out.get("jobId"):
                print(f"investigating — job {out['jobId']}, not finished — poll: warden status {out['jobId']}")
            if out.get("result"):
                v = out["result"].get("verdict") or {}
                if v:
                    print(f"summary: {v.get('summary', '')}")
    elif verb == "status":
        _print_result_text(out)
    elif verb == "list":
        rows = out.get("dispatches") or []
        if not rows:
            print("no dispatches")
        for r in rows:
            print(f"{r['job_id'][:8]}  {r['status']:<11} {r['repo']:<18} {r['tier']:<11} {r['created_at'][:19]}")
    elif verb == "close":
        print(f"item {out['eventId']}: {out['fromState']} -> {out['toState']}")
        print(f"note: {out['note']}")
    else:
        print(json.dumps(out, indent=2))


_HELP_TEXT = """\
warden — the CLI over warden's lifecycle modules.

VERBS
  dispatch <repo>    open a BARE episode, no item (brief on stdin, never argv)
  run <repo>         open an ITEM riding the alert lifecycle (investigate first,
                     implement only if the verdict says so and policy allows;
                     brief on stdin) — the intake for a human or for Hermes
  status <job-id>    poll one
  list [scope]        open | today | all
  merge <job-id>      land the draft PR a dispatch opened (--why --confirm)
  abort <event-id>    cancel an in-flight implement/validate episode (--why)
  revert <event-id>   record a revert PR against a merged item (--pr --why)
  close <event-id>    resolve an open item by hand (--why required)
  help

Global flags, anywhere on the line, --flag value or --flag=value:
  --json --confirm --dry-run --wait
  --why --tier --brief-file --context-file
  --origin-channel --origin-thread --origin-event --auto-from-item --model --pr

There is deliberately no --brief: the brief is data, never an argv string.
Pass it on stdin with a QUOTED heredoc (<<'BRIEF' ... BRIEF) or --brief-file.

EXIT CODES  0 ok · 2 precondition failed · 3 remote failed · 4 policy
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
                out = cmd_dispatch(conn, flags, rest, state)
            elif verb == "run":
                out = cmd_run(conn, flags, rest, state)
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
            elif verb == "close":
                out = cmd_close(conn, flags, rest, state)
            else:
                raise UsageError(
                    f"unknown verb: {verb}\n"
                    "valid verbs:\n"
                    "  dispatch <repo>   open a bare episode, no item (brief on stdin)\n"
                    "  run <repo>        open an item riding the alert lifecycle (brief on stdin)\n"
                    "  status <job-id>   poll one\n"
                    "  list [scope]      open | today | all\n"
                    "  merge <job-id>    land the draft PR that dispatch opened (--why --confirm)\n"
                    "  abort <event-id>  cancel an in-flight implement/validate episode (--why)\n"
                    "  revert <event-id> record a revert PR against a merged item (--pr --why)\n"
                    "  close <event-id>  resolve an open item by hand (--why required)\n"
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
