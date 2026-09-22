"""Dispatch sweep — the return path that closes a dispatch without a human.

Runs every 300 s as the `com.jkrumm.warden-sweep` LaunchAgent. This script
does its own per-dispatch delivery, straight over the Slack Web API
(scripts/slack_client.py's `chat.postMessage`), into each dispatch's own
origin thread, which is a different target per row. So production stdout
(routed to `warden-sweep.log`) is always empty on purpose; every diagnostic
goes to stderr (`warden-sweep.err`).

DELIVERY TRANSPORT. This used to shell out to `hermes send`, which posts
through the gateway's Slack Socket Mode connection — the one piece of
infrastructure a control-plane sweeper cannot depend on staying up (it was
found in a reconnect loop the day this changed). It now calls
scripts/slack_client.py's `slack_post_message()` directly: plain `urllib`
over HTTPS, the exact same client scripts/triage.py already used for its
own cards, no subprocess, no gateway. The message posts under Warden's own
Slack app identity (`op://common/slack/WARDEN_BOT_TOKEN` — see
slack/README.md) once seeded, falling back to Hermes's bot user (same
`op://hermes/slack/bot-token` as before) until then — `resolve_slack_token()`
tries Warden first, unconditionally, everywhere it's called. See WAKE-UP
NUDGE below for why the Hermes case specifically still matters.

WHAT IT DOES, one pass: read every dispatch row with `reported_at IS NULL`
(watchdog.db's `dispatches` table, owned by scripts/hermes-cc.sh — see
docs/dispatch-bridge.md § "The dispatch record"). For each, poll sideclaw's
job endpoint. A still-running job is left alone. A terminal job (done,
failed, interrupted) gets folded back into the row (status, verdict_json,
finished_at), then — if the dispatch has an origin_channel — a deterministic,
no-LLM message is composed and sent via `send_message()` (the HTTP client
above), and only `ok: true` from Slack stamps `reported_at`. A dispatch with
no origin_channel (e.g. opened from a context with no Slack thread to answer
into) can never be delivered anywhere, so it is closed with a sentinel
instead of accumulating as a permanent debt — see UNDELIVERABLE_SENTINEL
below.

ADVANCE ON COMPLETION (docs/history/state-log.md §87). After that per-row
pass, every pass also calls `triage.advance_implement_chain()` — the same
verdict -> implementing -> validating -> merge/merge_blocked code
`triage.run()` calls on its own 600s tick. Before this, an item that just
crossed a stage boundary (its verdict folded above, or the row loop
noticing an implement/review job go terminal) waited for the loop's next
tick regardless — measured at a full lifecycle costing 27 minutes of wall
clock for under 9 minutes of real work (§80), because every stage boundary
was gated on the same 600s clock. Calling the identical, already
CAS-guarded functions from here too halves that worst case to this
script's own 300s cadence at no extra risk — see
`advance_implement_chain()`'s own docstring for why two cron processes
calling it is safe with no new lock.

TWO COLUMNS, NOT ONE OVERLOADED ONE. `reported_at` used to double as a
status field: a real timestamp meant delivered, and the string
UNDELIVERABLE_SENTINEL sitting in that same TEXT column meant "closed,
never deliverable" — a status wearing a timestamp column's clothes.
`dispatches.delivery_status` (schema 6) now carries that status on its own:
`'delivered'`, the sentinel string, or `'failed:<short reason>'` for a row
whose last attempt errored and is still awaiting retry (`reported_at` stays
NULL in that case, exactly as before this column existed — a `failed`
delivery_status is informational, not a close). `reported_at` goes back to
meaning only one thing: NULL until a real, final timestamp lands, delivered
or not.

CRASH SAFETY — read this before changing the write order. The script must be
killable at any instant without losing a debt:
  1. The terminal-status UPDATE (status/verdict_json/finished_at) commits
     BEFORE any delivery attempt. A kill here just means the next sweep
     re-polls a job that's already terminal and re-derives the same update —
     idempotent, no harm.
  2. `reported_at` is stamped ONLY after `send_message()` reports Slack's own
     `ok: true`, in its own commit, immediately. A kill between the send and
     this commit means the next sweep sends the same message again (the row
     is still `reported_at IS NULL`) — a duplicate Slack post, never a lost
     one.
This is an AT-LEAST-ONCE contract, not exactly-once, by design: the failure
mode this script must never have is a verdict that silently vanishes because
the process died between "sent" and "recorded." An occasional duplicate
message in a thread is a cosmetic annoyance; a lost verdict is the thing
Phase 3 exists to prevent.

WAKE-UP NUDGE. This existed because the verdict above used to post
unconditionally as Hermes's own Slack bot user, and Slack ingest
unconditionally drops Hermes's own messages (echo-loop protection), so
nothing woke the agent when an episode finished. That is still exactly true
during the Hermes-fallback window (Warden's own token unseeded — see
DELIVERY TRANSPORT above); once Warden's identity is live the verdict posts
as a distinct app the echo filter never sees, which may make this nudge
redundant for that case — a hermes-agent-side question, out of this repo's
scope, and harmless to leave running either way (best-effort, never raises).
For an *actionable* dispatch (done, implement tier, has an artifact URL, not
already merged — see is_actionable()), this script additionally posts a
short nudge through argo's Slack API, which posts as the HomeLab bot — a
different user Hermes does ingest — strictly AFTER `reported_at` is
stamped, and best-effort (its failure never affects `reported_at` and never
raises — see send_nudge()). The nudge body is restricted to fields the
dispatch bridge itself owns (job id, repo, tier, artifact URL) and never
carries episode-authored text — see build_nudge_body()'s docstring for why
that boundary is a security property, not a style choice.

PRUNED JOBS. sideclaw prunes a job ~24 h after it finishes. A dispatch whose
verdict was never folded in before that (sideclaw restarted mid-run, the
sweeper was down for a day, the job id was never real) answers 404 forever,
and "retry next sweep" forever is a debt that can never be paid. So a 404 —
and ONLY a 404, never a connection failure — increments `poll_misses` on the
row; the third consecutive one marks the row terminal `failed` (verdict_json
left NULL — there never was one), delivers a one-line notice into the origin
thread (or the undeliverable sentinel), and stamps `reported_at` so it is
never polled again. A successful poll resets the counter, so three misses
spread across a flapping sideclaw do not count.

CANCELLED JOBS. `cancelled` (sideclaw's own cancel endpoint) is terminal
exactly like done/failed/interrupted — folded back into the row, delivered
with its own short "cancelled (aborted)" message, and folded onto a triage
card via `fold_dispatch_verdict()` the same as any other terminal status.

Source of truth: ~/SourceRoot/warden/scripts/dispatch-sweep.py
~/.hermes/scripts/ is a symlink to hermes-agent/scripts, NOT to this
directory — this code left that repo on 2026-09-09 and is reached by its
own path now.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import os
import sqlite3
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

# scripts/ (this file's own directory) onto sys.path so `clients` is
# importable as a real package — the same reason triage.py does this.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from clients import secrets as _secrets, sideclaw as _sideclaw  # noqa: E402
from clients.errors import RemoteError  # noqa: E402

HERMES_HOME = Path.home() / ".hermes"
# The ledger. `~/.warden/warden.db` since the extraction — the same file
# scripts/ledger.py resolves, and the same two env vars, so a `--db` override, a
# test fixture and the module default cannot disagree about which database this
# is. It moved out of ~/.hermes because the control plane cannot keep living
# inside the thing it supervises; ~/.hermes/watchdog.db is left in place,
# untouched, as the rollback.
WARDEN_HOME = (Path(os.environ["WARDEN_HOME"]).expanduser()
               if os.environ.get("WARDEN_HOME") else Path.home() / ".warden")
DB_PATH = (Path(os.environ["WARDEN_DB"]).expanduser()
           if os.environ.get("WARDEN_DB") else WARDEN_HOME / "warden.db")

# sideclaw's own terminal statuses (done/failed/interrupted/cancelled) —
# clients/sideclaw.py's TERMINAL is this sweeper's own vocabulary now, not a
# copy of it: a job in any of these is done reporting, one way or another.
TERMINAL_STATUSES = _sideclaw.TERMINAL
# Consecutive sideclaw 404s before a row is declared pruned. Three sweeps = 15
# min, long enough to ride out a sideclaw restart that briefly answers 404 for
# everything, short enough that a pruned job does not haunt every sweep for
# weeks. There is no local `lost` status any more — a pruned job is recorded
# as a genuine `status='failed'` with `verdict_json` left NULL (there never
# was one to record), the same terminal status a real failure gets, just
# distinguishable by having no verdict at all.
LOST_AFTER_MISSES = 3
PRUNED_STATUS = "failed"

# --- Wake-up nudge (argo Slack API) -----------------------------------------
#
# The verdict above is sent via send_message() as Hermes's own
# Slack bot user — and Slack ingest unconditionally drops Hermes's own
# messages (echo-loop protection, independent of allow_bots), so nothing
# wakes the agent when an episode finishes. For an *actionable* dispatch
# (see is_actionable()) we additionally post a short nudge through argo's
# Slack API, which posts as the HomeLab bot — a different user, which Hermes
# DOES ingest — so it can decide whether to call `merge`.
#
# Same API_BASE + same bearer secret (op://common/api/SECRET, env
# HOMELAB_API_KEY) that scripts/briefing-coverage.py and
# scripts/watchdog-poll.py already use; resolve_api_key() below mirrors
# briefing-coverage.py's implementation exactly.
ARGO_API_BASE = "https://argo.jkrumm.com/api"
ARGO_API_KEY_REF = "op://common/api/SECRET"
ARGO_HTTP_TIMEOUT = 10  # seconds; a bare POST against a remote HTTP API

# A dispatch with no origin_channel was never asked from a Slack thread (e.g.
# opened from a cron context with nothing to answer into) — it can NEVER be
# delivered anywhere, so it must not sit open forever as unpaid debt. This
# string closes the row: it is written into `delivery_status` (schema 6) and
# `reported_at` is stamped with a REAL timestamp alongside it, which is what
# keeps the row out of `idx_dispatches_open` (`WHERE reported_at IS NULL`).
# Before schema 6 this string lived IN `reported_at` itself — a status
# wearing a timestamp column's clothes, safe only because every reader of
# that column tested NULL-ness and never parsed the value as a date. See
# scripts/ledger.py's migration 6 comment for the backfill that moved every
# historical occurrence of this string out of `reported_at` and into its own
# column.
UNDELIVERABLE_SENTINEL = "undeliverable:no-origin-channel"

# Slack mrkdwn block limit is 4000 chars; this leaves headroom for the
# "(message truncated)" suffix itself and any mrkdwn formatting overhead.
BODY_CHAR_LIMIT = 3800
EVIDENCE_CAP = 5

# scripts/triage.py's fold_dispatch_verdict() — loaded by path, the same
# mechanism the cron entry-point wrappers use (the filename is not
# importable). A dispatch tied to a triage item (origin_event_id set) needs
# its card folded to the terminal verdict as soon as this sweeper sees it,
# rather than waiting for triage.py's own next 10-minute pass. Import at
# module load, not lazily inside process_dispatch, so a broken sibling
# script fails loudly at import time instead of on the first row that
# happens to need it.
_TRIAGE_PATH = Path(__file__).resolve().parent / "triage.py"
_triage_spec = importlib.util.spec_from_file_location("triage", _TRIAGE_PATH)
assert _triage_spec and _triage_spec.loader, "Failed to load scripts/triage.py"
_triage = importlib.util.module_from_spec(_triage_spec)
_triage_spec.loader.exec_module(_triage)

# scripts/ledger.py — same by-path loading mechanism as the triage.py load
# just above. ledger.py is now the sole owner of the schema and migrations
# that used to live inline here as DB_SCHEMA plus an ALTER TABLE block.
_LEDGER_PATH = Path(__file__).resolve().parent / "ledger.py"
_ledger_spec = importlib.util.spec_from_file_location("ledger", _LEDGER_PATH)
assert _ledger_spec and _ledger_spec.loader, "Failed to load scripts/ledger.py"
_ledger = importlib.util.module_from_spec(_ledger_spec)
_ledger_spec.loader.exec_module(_ledger)

# scripts/slack_client.py — same by-path loading mechanism. The plain Slack
# Web API HTTP client triage.py already used, now the single delivery path
# for this file too — see that module's docstring for why `hermes send`
# (Slack Socket Mode, in a reconnect loop) is no longer in this file at all.
_SLACK_CLIENT_PATH = Path(__file__).resolve().parent / "slack_client.py"
_slack_client_spec = importlib.util.spec_from_file_location("slack_client", _SLACK_CLIENT_PATH)
assert _slack_client_spec and _slack_client_spec.loader, "Failed to load scripts/slack_client.py"
_slack_client = importlib.util.module_from_spec(_slack_client_spec)
_slack_client_spec.loader.exec_module(_slack_client)


def db_connect() -> sqlite3.Connection:
    """Assert-only: dispatch-sweep.py is not the migrator (DESIGN.md § The
    ledger — migrations run only by the loop, triage.py, at boot). If this
    runs before the loop has ever touched a fresh ledger, ledger.connect()
    raises a clear schema-version error rather than this file inventing its
    own `dispatches` table the way it used to."""
    return _ledger.connect(DB_PATH)


def _apply_db_override(argv: list[str]) -> None:
    """--db PATH — lets a test point this at a throwaway copy of the DB
    without touching the real ~/.warden/warden.db. Mirrors watchdog-poll.py's
    dry-run DB_PATH swap: reassign the module-level global before db_connect()
    ever opens it. The env-var override is WARDEN_DB, already applied once at
    module load (see DB_PATH above) — this only ever handles the argv form.
    Thin wrapper over ledger.apply_db_override(), no env_var of its own."""
    def _set(path: Path) -> None:
        global DB_PATH
        DB_PATH = path
    _ledger.apply_db_override(argv, _set)


NOT_FOUND = "not_found"


def poll_job(job_id: str) -> dict[str, Any] | str | None:
    """GET the sideclaw job via `clients.sideclaw.get()`. The job dict on a
    real read; the sentinel NOT_FOUND on a 404 (sideclaw pruned it, or never
    had it — `sideclaw.get()` already turns that into a bare `None`, which
    this function re-labels); None on any other transport/parse failure
    (`RemoteError`) — never raises, so one unreachable poll can't take the
    sweep down."""
    try:
        job = _sideclaw.get(job_id)
    except RemoteError as e:
        print(f"dispatch-sweep: could not poll sideclaw for job {job_id}: {e}", file=sys.stderr)
        return None
    return NOT_FOUND if job is None else job


def http_post_json(url: str, payload: dict[str, Any], headers: dict[str, str],
                    timeout: int = ARGO_HTTP_TIMEOUT) -> Any:
    """The one raw-urllib POST this script makes (the homelab nudge webhook).
    Never-raise contract: any transport/parse failure folds into a
    {"_error": ...} dict rather than propagating, so a bad nudge attempt
    can't take the sweep down."""
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data, method="POST",
        headers={"Content-Type": "application/json", **headers},
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError,
            TimeoutError, ValueError) as e:
        return {"_error": str(e)}


def resolve_api_key() -> str:
    """Mirrors scripts/briefing-coverage.py's resolve_api_key() exactly:
    process env first (HOMELAB_API_KEY), then the secrets-run cache shim with
    a widened PATH (the cache backend needs sops+jq, which the gateway's
    minimal cron PATH may not reach). Returns "" on any failure — a missing
    key is simply a nudge failure, never a hard error for this script. Thin
    wrapper over clients/secrets.py's resolve_secret(), the resolver every
    plain-HTTP client in this repo shares."""
    return _secrets.resolve_secret("HOMELAB_API_KEY", ARGO_API_KEY_REF)


def is_actionable(*, status: str, tier: str, artifact_url: str | None,
                   merged_at: str | None) -> bool:
    """A dispatch is actionable — i.e. worth waking Hermes for — only when
    ALL of: the job finished successfully (status == "done"), it was an
    implement-tier episode (the only tier that can produce something to
    merge), it actually produced an artifact (a PR URL), and it isn't
    already merged. Anything else (investigate/author tiers, a
    failed/interrupted job, no artifact, already merged) gets today's
    behaviour — verdict only, no nudge."""
    return status == "done" and tier == "implement" and bool(artifact_url) and not merged_at


def build_nudge_body(*, job_id: str, repo: str, tier: str, artifact_url: str) -> str:
    """Deterministic wake-up nudge, in the same voice as format_message().

    SECURITY — do not "improve" this by inlining the episode's summary,
    verdict, recommendation, evidence, or branch. This nudge is ingested by
    Hermes as a live user turn (that is the whole point — it's what wakes
    the agent), so any episode-authored prose in it becomes an instruction
    to an agent that can go on to merge code to master. The episode's
    output is derived from repo content, which can include third-party
    text. Restricting this body to fields the dispatch bridge itself owns
    — job id, repo, tier, artifact URL — keeps that injection surface
    empty by construction. The full verdict (including any episode prose)
    is already in the thread from the prior send_message() call — Hermes
    reads it there, as a human would, rather than having it re-injected
    here."""
    job_short = job_id[:8]
    return (
        f":bell: Implement episode finished — {repo}\n"
        f"Unmerged PR: {artifact_url}\n"
        f"Review the verdict above and decide whether to merge — use the `claude-dispatch` "
        f"skill's `merge {job_id}` verb, or explain why not.\n"
        f"_tier {tier} · job `{job_short}`_"
    )


def send_nudge(*, origin_channel: str, origin_thread_ts: str | None, job_id: str,
               repo: str, tier: str, artifact_url: str) -> None:
    """Best-effort wake-up nudge via argo's Slack API (posts as the HomeLab
    bot, which Hermes — unlike its own bot — actually ingests). Must run
    STRICTLY AFTER reported_at is already stamped (see the module
    docstring's crash-safety contract) and must NEVER affect it and NEVER
    raise: at-most-once is the correct trade here, since the worst case is
    "Hermes isn't woken and a human pokes the thread," which is exactly
    today's behaviour. A caught exception is logged to stderr, not
    propagated — this function is called from a context where letting it
    raise would incorrectly look like the sweep itself failed."""
    try:
        api_key = resolve_api_key()
        if not api_key:
            print(
                f"dispatch-sweep: no HOMELAB_API_KEY available, skipping nudge for "
                f"job {job_id} (repo {repo})",
                file=sys.stderr,
            )
            return
        body = build_nudge_body(job_id=job_id, repo=repo, tier=tier, artifact_url=artifact_url)
        if origin_thread_ts:
            url = f"{ARGO_API_BASE}/slack/channels/{origin_channel}/messages/{origin_thread_ts}/reply"
        else:
            url = f"{ARGO_API_BASE}/slack/channels/{origin_channel}/messages"
        result = http_post_json(url, {"text": body}, {"Authorization": f"Bearer {api_key}"})
        if not isinstance(result, dict) or "_error" in result:
            print(
                f"dispatch-sweep: nudge post failed for job {job_id} (repo {repo}): {result}",
                file=sys.stderr,
            )
    except Exception as e:  # a nudge failure must never look like a sweep failure
        print(f"dispatch-sweep: nudge raised for job {job_id} (repo {repo}): {e}", file=sys.stderr)


def send_message(target: str, body: str) -> tuple[int, str]:
    """Deliver `body` to `target` over the plain Slack HTTP client
    (scripts/slack_client.py's `chat.postMessage`) — no subprocess, no
    gateway Socket Mode. `target` is `slack:<channel>[:<thread_ts>]`, the
    same shape the old `hermes send --to` argument used, parsed the same
    way here.

    Returns `(0, "delivered")` on Slack's own `ok: true` — the `rc == 0`
    gate every caller uses to decide whether to stamp `reported_at` holds
    exactly as it did against `hermes send`'s exit code. Returns `(1,
    "failed:<short reason>")` on anything else: no token available, a
    Slack-side rejection (bad channel, revoked token, …), or a transport
    failure — never raises. The reason string is new: callers fold it into
    `delivery_status` so a row still awaiting retry (`reported_at` stays
    NULL) records why its last attempt failed, instead of nothing at all."""
    channel, _, thread_ts = target.removeprefix("slack:").partition(":")
    token = _slack_client.resolve_slack_token()
    if not token:
        return 1, "failed:no-slack-token"
    result = _slack_client.slack_post_message(token, channel, body, thread_ts or None)
    if result.get("ok"):
        return 0, "delivered"
    reason = str(result.get("error") or "unknown")[:80]
    return 1, f"failed:{reason}"


def _truncate(text: str, limit: int) -> tuple[str, bool]:
    if len(text) <= limit:
        return text, False
    return text[:limit].rstrip() + "…", True


def format_evidence(evidence: list[Any]) -> list[str]:
    capped = evidence[:EVIDENCE_CAP]
    lines: list[str] = []
    for e in capped:
        if isinstance(e, dict):
            file = e.get("file") or "?"
            detail = e.get("detail") or ""
            lines.append(f"- `{file}` — {detail}")
        else:
            lines.append(f"- {e}")
    overflow = len(evidence) - len(capped)
    if overflow > 0:
        lines.append(f"- … and {overflow} more")
    return lines


def _finalize(lines: list[str]) -> str:
    body = "\n".join(lines)
    body, truncated = _truncate(body, BODY_CHAR_LIMIT)
    if truncated:
        body += "\n\n_(message truncated)_"
    return body


def _format_merged_ts(merged_at: str) -> str:
    """Render `merged_at` as `YYYY-MM-DD HH:MM UTC`. Fails toward "say it is
    merged" — a malformed or unparseable value still counts as merged (the
    merge happened; only the display degrades), so this falls back to the
    raw string rather than raising or dropping the merged treatment."""
    try:
        parsed = dt.datetime.fromisoformat(merged_at)
    except (TypeError, ValueError):
        return merged_at
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed.astimezone(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def format_message(*, repo: str, tier: str, job_id: str, status: str,
                    result: dict[str, Any] | None, error: Any,
                    merged_at: str | None = None) -> str:
    """Deterministic Slack mrkdwn body for one terminal dispatch. No LLM —
    every field comes straight from sideclaw's schema-shaped verdict object
    (`{verdict, confidence, evidence[], recommendation, nextAction, summary,
    degraded?, artifactUrl?, branch?}`) or, for a failed/interrupted job, its
    `error` string.

    `merged_at` is threaded in separately from `result` — it lives on the
    dispatches row itself (stamped by hermes-cc.sh's `merge` verb, which
    deliberately does NOT stamp `reported_at`: the merge announcement and the
    sweeper's verdict are different messages). Without this, a dispatch that
    was already merged still renders as "here is your draft PR, review it" —
    observed live 2026-08-02 on job 6f7c9cc4, where the merge landed at
    19:06:00 and the sweeper posted the stale review instruction at
    19:10:02. A truthy `merged_at` here means: say it is merged, everywhere
    that would otherwise read as an outstanding review ask."""
    job_short = job_id[:8]
    merged = bool(merged_at)

    if status == "cancelled":
        lines = [
            f":no_entry_sign: Dispatch cancelled (aborted) — {repo}",
            f"_tier {tier} · job `{job_short}`_",
        ]
        return _finalize(lines)

    if status in ("failed", "interrupted"):
        lines = [
            f":warning: Dispatch {status} — {repo}",
            f"error: {error if error else 'no error detail returned by sideclaw'}",
            "",
            f"_tier {tier} · job `{job_short}`_",
        ]
        return _finalize(lines)

    if not isinstance(result, dict):
        # status == "done" but sideclaw returned no result object at all —
        # render this plainly rather than staying silent, since silence is
        # exactly the debt this sweeper exists to close.
        lines = [
            f":warning: Dispatch done with no verdict — {repo}",
            "sideclaw reported status=done but returned no result object.",
            "",
            f"_tier {tier} · job `{job_short}`_",
        ]
        return _finalize(lines)

    degraded = bool(result.get("degraded"))
    lines = []
    if degraded:
        header = f":grey_question: Dispatch degraded — {repo}"
        if merged:
            header += " _(already merged)_"
        lines.append(header)
        lines.append("_The tool run failed partway through — this is not a finding about the repo._")
    elif merged:
        lines.append(f":white_check_mark: Dispatch verdict — {repo} _(already merged)_")
    else:
        lines.append(f":mag: Dispatch verdict — {repo}")

    summary = (result.get("summary") or "").strip()
    if summary:
        lines.append(summary)

    # The artifact goes directly under the summary, above the prose, because it is the
    # only actionable line in the message and `_finalize` truncates from the bottom.
    # `branch` without `artifactUrl` is its own real outcome: the implement tier pushed
    # work but opened no PR (the episode declined to describe one, or the run degraded),
    # and saying so is what stops that branch from being silently orphaned.
    artifact_url = (result.get("artifactUrl") or "").strip()
    branch = (result.get("branch") or "").strip()
    if artifact_url:
        artifact_line = f"*Artifact:* {artifact_url}"
        if merged:
            artifact_line += f" — *merged* {_format_merged_ts(merged_at)}"
        lines.append(artifact_line)
    elif branch:
        lines.append(f"*Branch pushed, no PR opened:* `{branch}`")

    verdict_text = (result.get("verdict") or "").strip()
    if verdict_text:
        v_text, v_truncated = _truncate(verdict_text, 1500)
        lines.append("")
        lines.append(v_text)
        if v_truncated:
            lines.append("_(verdict text truncated)_")

    recommendation = (result.get("recommendation") or "").strip()
    if recommendation:
        lines.append("")
        lines.append(f"*Recommendation:* {recommendation}")

    evidence = result.get("evidence")
    if isinstance(evidence, list) and evidence:
        lines.append("")
        lines.append("*Evidence:*")
        lines.extend(format_evidence(evidence))

    confidence = result.get("confidence") or "?"
    next_action = "none (merged)" if merged else (result.get("nextAction") or "?")
    lines.append("")
    lines.append(f"_confidence {confidence} · next {next_action} · tier {tier} · job `{job_short}`_")

    return _finalize(lines)


def pruned_notice(*, repo: str, tier: str, job_id: str, misses: int) -> str:
    """The one line a pruned dispatch gets. Bridge-owned fields only."""
    return (
        f":ghost: Dispatch pruned — {repo}: sideclaw pruned this job before it reported "
        f"(`{job_id[:8]}`, {misses} consecutive 404s) and no verdict was ever recorded. "
        f"Not retried. _tier {tier}_"
    )


def _mark_pruned(conn: sqlite3.Connection, row: sqlite3.Row, misses: int, *, dry_run: bool) -> None:
    job_id, repo, tier = row["job_id"], row["repo"], row["tier"]
    body = pruned_notice(repo=repo, tier=tier, job_id=job_id, misses=misses)
    origin_channel = row["origin_channel"]
    origin_thread_ts = row["origin_thread_ts"]
    target = (f"slack:{origin_channel}:{origin_thread_ts}" if origin_thread_ts
              else f"slack:{origin_channel}") if origin_channel else None
    if dry_run:
        print(f"[dry-run] would mark job {job_id} (repo {repo}) pruned and send to {target or 'nowhere'}:\n{body}\n")
        return
    now_iso = dt.datetime.now(dt.timezone.utc).isoformat()
    # verdict_json is explicitly cleared — there never was one to record, and
    # this is now a genuine status='failed' row indistinguishable from a real
    # failure except by having no verdict at all. `error` gets warden's OWN
    # reason (there is no sideclaw failure text for a job sideclaw itself has
    # forgotten) — kept consistent with, not a duplicate of, pruned_notice()'s
    # Slack wording.
    conn.execute(
        "UPDATE dispatches SET status=?, verdict_json=NULL, finished_at=?, poll_misses=?, error=? "
        "WHERE job_id=?",
        (PRUNED_STATUS, now_iso, misses,
         f"sideclaw pruned this job before it reported ({misses} consecutive 404s)", job_id),
    )
    conn.commit()
    stamp: str | None = None
    if target is None:
        stamp, delivery_status = now_iso, UNDELIVERABLE_SENTINEL
    else:
        rc, delivery_status = send_message(target, body)
        if rc == 0:
            stamp = now_iso
        else:
            print(
                f"dispatch-sweep: pruned notice for job {job_id} (repo {repo}) did not send — "
                f"reported_at left NULL, next sweep re-sends the notice",
                file=sys.stderr,
            )
    if stamp is not None:
        conn.execute(
            "UPDATE dispatches SET reported_at=?, delivery_status=? WHERE job_id=? AND reported_at IS NULL",
            (stamp, delivery_status, job_id),
        )
    else:
        conn.execute(
            "UPDATE dispatches SET delivery_status=? WHERE job_id=? AND reported_at IS NULL",
            (delivery_status, job_id),
        )
    conn.commit()


def process_dispatch(conn: sqlite3.Connection, row: sqlite3.Row, *, dry_run: bool) -> None:
    job_id = row["job_id"]
    repo = row["repo"]
    tier = row["tier"]

    job = poll_job(job_id)
    if job is None:
        print(
            f"dispatch-sweep: could not poll sideclaw for job {job_id} (repo {repo}) "
            f"— retrying next sweep",
            file=sys.stderr,
        )
        return

    misses = int(row["poll_misses"] or 0) if "poll_misses" in row.keys() else 0
    if job == NOT_FOUND:
        misses += 1
        # No `row["status"]`-based fallback here any more: the pruned status
        # IS `'failed'` now (PRUNED_STATUS), the same value a real failure
        # gets, so branching on it would misfire on a genuinely failed
        # dispatch that sideclaw later prunes. `poll_misses` alone is the
        # trigger — and it is durable across passes (a delivery that failed
        # after marking pruned leaves poll_misses already at/above the
        # threshold, so the very next pass re-enters this branch regardless).
        if misses >= LOST_AFTER_MISSES:
            _mark_pruned(conn, row, misses, dry_run=dry_run)
            return
        print(
            f"dispatch-sweep: sideclaw has no job {job_id} (repo {repo}) — miss {misses}/"
            f"{LOST_AFTER_MISSES}, marking pruned at {LOST_AFTER_MISSES}",
            file=sys.stderr,
        )
        if not dry_run:
            conn.execute("UPDATE dispatches SET poll_misses=? WHERE job_id=?", (misses, job_id))
            conn.commit()
        return
    if misses and not dry_run:
        # A successful poll ends the streak — three misses across a flapping
        # sideclaw are not three consecutive ones.
        conn.execute("UPDATE dispatches SET poll_misses=0 WHERE job_id=?", (job_id,))
        conn.commit()

    status = job.get("status")
    if status not in TERMINAL_STATUSES:
        return  # still queued/running — nothing to report yet

    result = job.get("result")
    error = job.get("error")
    # `or None` so "" and NULL do not become two shapes of the same fact — every
    # reader of this column (and the actionable predicate below) filters on
    # IS NOT NULL / truthiness.
    artifact_url = ((result.get("artifactUrl") if isinstance(result, dict) else None) or "").strip() or None
    # Same normalization as artifact_url above, for the same reason: triage.py's
    # fold_dispatch_verdict() (ledger.py migration 10) reads this column to know
    # WHY a terminal dispatch carries no verdict, and "" vs NULL must not become
    # two shapes of "no reason recorded".
    error_text = (str(error).strip() if error else "") or None

    # Fold the terminal outcome back into the row and commit it BEFORE any
    # delivery attempt — see the module docstring's crash-safety contract.
    if not dry_run:
        now = dt.datetime.now(dt.timezone.utc)
        # sideclaw's own `finishedAt` (when it stamped the job terminal), not
        # this poll's wall clock — a sweep that finds a job late (an
        # unloaded LaunchAgent, a suspended host) must not misreport how
        # long the episode actually ran (docs/history/state-log.md §79: a
        # poll suspended overnight recorded a 614-minute dispatch that took
        # 20). `now` is still the fallback for the rare job with no such field.
        finished_at = _sideclaw.finished_at_iso(job, fallback=now)
        # `artifact_url` is denormalized out of the verdict into its own column so the
        # GitHub projection is a column read, not a JSON parse — the briefing and the
        # watchdog both want "what did this dispatch produce" without unpacking a blob.
        conn.execute(
            "UPDATE dispatches SET status=?, verdict_json=?, artifact_url=?, finished_at=?, error=? "
            "WHERE job_id=?",
            (
                status,
                json.dumps(result) if result is not None else None,
                artifact_url,
                finished_at,
                error_text,
                job_id,
            ),
        )
        conn.commit()

    # Fold the terminal verdict onto the triage card, if this dispatch was
    # opened BY triage.py (origin_event_id set — see scripts/triage.py's
    # escalate_one()). This is a different concern from the #watchdog/thread
    # delivery below: the card lives in its own Slack message, independent of
    # whether send_message() below succeeds, so it runs unconditionally here
    # rather than being duplicated into every branch of the delivery logic
    # that follows. A dispatch with no origin_event_id (the pre-existing
    # #watchdog path, and every non-triage dispatch) is untouched.
    origin_event_id = row["origin_event_id"] if "origin_event_id" in row.keys() else None
    if origin_event_id:
        try:
            _triage.fold_dispatch_verdict(
                conn, origin_event_id=origin_event_id, job_id=job_id,
                now=dt.datetime.now(dt.timezone.utc), dry_run=dry_run,
            )
        except Exception as e:  # a triage-card failure must never look like a sweep failure
            print(
                f"dispatch-sweep: folding triage card failed for job {job_id} "
                f"(origin_event_id {origin_event_id}): {e}",
                file=sys.stderr,
            )

    origin_channel = row["origin_channel"]
    origin_thread_ts = row["origin_thread_ts"]

    if not origin_channel:
        print(
            f"dispatch-sweep: job {job_id} (repo {repo}, tier {tier}) finished with no "
            f"origin_channel — cannot deliver to any thread, closing with a sentinel",
            file=sys.stderr,
        )
        if dry_run:
            print(
                f"[dry-run] would stamp reported_at=<now>, delivery_status={UNDELIVERABLE_SENTINEL!r} "
                f"for job {job_id} (no origin_channel, nothing sent)"
            )
            return
        now_iso_nc = dt.datetime.now(dt.timezone.utc).isoformat()
        conn.execute(
            "UPDATE dispatches SET reported_at=?, delivery_status=? WHERE job_id=? AND reported_at IS NULL",
            (now_iso_nc, UNDELIVERABLE_SENTINEL, job_id),
        )
        conn.commit()
        return

    body = format_message(repo=repo, tier=tier, job_id=job_id, status=status,
                           result=result, error=error, merged_at=row["merged_at"])
    target = f"slack:{origin_channel}:{origin_thread_ts}" if origin_thread_ts else f"slack:{origin_channel}"
    actionable = is_actionable(status=status, tier=tier, artifact_url=artifact_url,
                                merged_at=row["merged_at"])

    if dry_run:
        print(f"[dry-run] would send to {target} (job {job_id}, repo {repo}):\n{body}\n")
        if actionable:
            nudge_body = build_nudge_body(job_id=job_id, repo=repo, tier=tier,
                                           artifact_url=artifact_url)
            print(f"[dry-run] would nudge {target} (job {job_id}, repo {repo}):\n{nudge_body}\n")
        return

    rc, delivery_status = send_message(target, body)
    if rc == 0:
        now_iso2 = dt.datetime.now(dt.timezone.utc).isoformat()
        conn.execute(
            "UPDATE dispatches SET reported_at=?, delivery_status=? WHERE job_id=? AND reported_at IS NULL",
            (now_iso2, delivery_status, job_id),
        )
        conn.commit()
        # Strictly AFTER reported_at is stamped, and best-effort — see
        # send_nudge()'s docstring for why ordering here is load-bearing.
        if actionable:
            send_nudge(origin_channel=origin_channel, origin_thread_ts=origin_thread_ts,
                       job_id=job_id, repo=repo, tier=tier, artifact_url=artifact_url)
    else:
        conn.execute(
            "UPDATE dispatches SET delivery_status=? WHERE job_id=? AND reported_at IS NULL",
            (delivery_status, job_id),
        )
        conn.commit()
        print(
            f"dispatch-sweep: Slack post failed (rc={rc}) for job {job_id} (repo {repo}, "
            f"target {target}) — reported_at left NULL, next sweep retries",
            file=sys.stderr,
        )


# --- Heartbeat ------------------------------------------------------------------
#
# Mirrors triage.py's record_heartbeat() and watchdog-poll.py's own copy: an
# unconditional row per COMPLETED pass, because every other write in this file
# only happens when a dispatch's status actually changed — a sweep that finds
# nothing with `reported_at IS NULL` leaves no trace at all otherwise, making
# "the sweep ran and had nothing to do" indistinguishable from "the sweep did
# not run." /metrics' per-poller age (scripts/api.py) reads this cursor to
# answer that question for this sweeper by name.
HEARTBEAT_CURSOR_KEY = "dispatch_sweep_last_run"


def record_heartbeat(conn: sqlite3.Connection, *, considered: int, errors: int) -> None:
    # No timestamp argument — see triage.py's record_heartbeat() for why the
    # write clock is read here rather than accepted from a caller.
    value = json.dumps({"considered": considered, "errors": errors}, sort_keys=True)
    conn.execute(
        "INSERT INTO cursors(key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (HEARTBEAT_CURSOR_KEY, value, dt.datetime.now(dt.timezone.utc).isoformat()),
    )
    conn.commit()


def main(argv: list[str] | None = None) -> int:
    argv = list(argv if argv is not None else sys.argv[1:])
    dry_run = "--dry-run" in argv
    # Accepted but a deliberate no-op: main() already performs exactly one pass
    # per invocation (the cron's own 5-minute schedule is the loop), so there is
    # nothing for --once to change. Documented rather than silently ignored, so
    # a caller can't wonder whether omitting it does something different.
    _ = "--once" in argv

    _apply_db_override(argv)

    errors = 0
    try:
        conn = db_connect()
    except _ledger.LedgerBehind as e:
        print(
            f"dispatch-sweep: ledger behind this process's schema ({e}) — skipping this pass; "
            "the loop migrates at its next tick",
            file=sys.stderr,
        )
        return 0
    try:
        rows = conn.execute(
            "SELECT * FROM dispatches WHERE reported_at IS NULL ORDER BY id"
        ).fetchall()
        for row in rows:
            try:
                process_dispatch(conn, row, dry_run=dry_run)
            except Exception as e:  # one bad row must never stop the others
                errors += 1
                print(
                    f"dispatch-sweep: row job_id={row['job_id']!r} (repo {row['repo']!r}) "
                    f"raised: {e} — skipping, next sweep retries",
                    file=sys.stderr,
                )

        # ADVANCE ON COMPLETION (docs/history/state-log.md §87). The verdict
        # this pass just folded above (fold_dispatch_verdict(), inside
        # process_dispatch()) can make an item eligible for its next
        # deterministic transition RIGHT NOW rather than at the loop's next
        # 600s tick — triage.advance_implement_chain() is the exact same
        # verdict -> implementing -> validating -> merge/merge_blocked code
        # run() itself calls, see that function's own docstring for why
        # calling it from here, on this 300s cadence, needs no new lock and
        # is not a second loop. Runs unconditionally, once per pass, exactly
        # like run() does — every step re-derives its own eligibility from
        # the DB, so an empty ledger costs one cheap SELECT per step.
        try:
            _triage.advance_implement_chain(conn, _triage.load_policy(),
                                             dt.datetime.now(dt.timezone.utc), dry_run=dry_run)
        except Exception as e:  # this chain must never take the sweep itself down
            errors += 1
            print(f"dispatch-sweep: advance_implement_chain raised: {e} — the loop's own "
                  f"600s tick still covers it", file=sys.stderr)

        if not dry_run:
            record_heartbeat(conn, considered=len(rows), errors=errors)
    finally:
        conn.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
