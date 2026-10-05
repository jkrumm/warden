"""Shared foundation of the loop stages: paths and env, the state vocabulary, strike/claim
constants, every tunable, the policy file, the ledger connection and the one state writer
(`set_state`) with the retry rule (`strike`). A leaf: it imports no other loop module."""

from __future__ import annotations

import datetime as dt
import fnmatch
import importlib.util
import json
import os
import re
import sqlite3
import sys
from pathlib import Path
from typing import Any, NamedTuple

from lifecycle import items as _items


_SCRIPTS_DIR = Path(__file__).resolve().parent.parent
HERMES_HOME = Path.home() / ".hermes"
# The ledger: `~/.warden/warden.db`, the same file scripts/ledger.py resolves with the same two
# env vars, so a `--db` override, a test fixture and the module default cannot disagree about which
# database this is.
WARDEN_HOME = (Path(os.environ["WARDEN_HOME"]).expanduser()
               if os.environ.get("WARDEN_HOME") else Path.home() / ".warden")
DB_PATH = (Path(os.environ["WARDEN_DB"]).expanduser()
           if os.environ.get("WARDEN_DB") else WARDEN_HOME / "warden.db")

# Env var first, documented absolute default second: reconcile_operations() runs `gh` under a
# LaunchAgent, whose PATH is minimal, so a bare `gh` works interactively and fails under the
# agent.
_env_gh_bin = os.environ.get("GH_BIN")
GH_BIN = Path(_env_gh_bin).expanduser() if _env_gh_bin else Path("/opt/homebrew/bin/gh")

# This repo's own config/. lifecycle/policy.py's `triage_policy_path()` reads the same file for
# the merge/deploy half, via the same `WARDEN_TRIAGE_POLICY` env var POLICY_PATH honors: one file,
# two readers. TRIAGE_REPO_DIR is the repo root POLICY_PATH resolves against.
TRIAGE_REPO_DIR = _SCRIPTS_DIR.parent
_env_policy = os.environ.get("WARDEN_TRIAGE_POLICY") or os.environ.get("HERMES_TRIAGE_POLICY")
POLICY_PATH = (Path(_env_policy).expanduser() if _env_policy
               else TRIAGE_REPO_DIR / "config" / "triage-policy.json")

# Sources watchdog-poll.py dedups that this loop acts on. github_*, hermes_cron and stray_skill
# are excluded: they are self-describing (a GitHub issue/PR IS the durable card) or
# governance-cadence, not the reactive-alert firehose. op_refs_homelab/op_refs_vps ARE included: a
# dead 1Password ref blocks every future reseal of the mini's offline secrets cache, so it must
# reach at least Argo.
#
# Two more origins open a `triage_items` row without going through here: `human` (`warden run`,
# via open_origin_item()) and `github_issue` (ingest_github_issues()). Neither belongs in this
# tuple, which is `ingest()`'s alert-source door.
INGEST_SOURCES = ("slack_alert", "uk", "docker_homelab", "docker_vps", "hermes_log",
                   "op_refs_homelab", "op_refs_vps")

# The two INGEST_SOURCES that are grouped (upsert_grouped()-based, append-only) rather than
# state (reconcile()-based, disappearance-resolved): watchdog-poll.py's GROUPED_SOURCES minus
# slack_update, which this loop never ingests. See resolve_quiet_grouped()/
# resolve_recovery_paired().
GROUPED_TRIAGE_SOURCES = ("slack_alert", "hermes_log")

# The state machine:
#
#   new -> triaged -> working -> merging -> verifying -> fixed
#                        |          |          |
#                        +----------+----------+--> needs_decision | failed
#   quiet, closed (terminal; `closed` always carries a close_reason)
#
# Mirrored by scripts/ledger.py (the schema owner) for the readers that cannot import this
# module; tests/test_ledger.py pins the two together.
#
# `new`      event classified, waiting for debounce / capacity (overflow waits here; the one
#            state silence may resolve).
# `triaged`  decided worth working, waiting for its dispatch: a dissolved cluster member, or an
#            item an infrastructure failure sent back for a retry. Escalates as a singleton.
# `working`  an investigate, implement or host-verb episode is in flight, OR an implement verdict
#            is waiting for its implement dispatch, OR a blocked review is waiting for its
#            revision attempt. The phase is read off the row: implement_job NULL + an unfinished
#            dispatch_job is the investigation; implement_job NULL + a finished verdict saying
#            implement/issue is "waiting for its dispatch"; implement_job a claim sentinel is
#            "being submitted"; a real implement_job is the episode; a real implement_job whose
#            dispatch carries validation_status blocked/checks_failed is "waiting for revision".
# `merging`  a pull request exists; its review and the merge gate are running.
# `verifying` merged; `make deploy` runs, then `make verify` and the item's own signal stay quiet
#            for the window (maybe_verify()). Nothing to verify -> `fixed` at once.
# `needs_decision` a verdict carried a question only the owner can answer.
# `failed`   three infrastructure strikes, a sideclaw refusal, a merge refusal that will not
#            clear, or revisions exhausted. Never expires, never silence-resolved, never retried
#            automatically.
# `fixed`    verified. `quiet`  the signal went quiet before work started.
# `closed`   done without a verified fix; close_reason says why.
STATE_NEW = "new"
STATE_TRIAGED = "triaged"
STATE_WORKING = "working"
STATE_MERGING = "merging"
STATE_VERIFYING = "verifying"
STATE_NEEDS_DECISION = "needs_decision"
STATE_FAILED = "failed"
STATE_FIXED = "fixed"
STATE_QUIET = "quiet"
STATE_CLOSED = "closed"

# Terminal means: no poller, no deadline, no exit but a genuine recurrence (reopen_if_needed())
# or an owner's `warden reopen`.
TERMINAL_STATES = (STATE_FIXED, STATE_QUIET, STATE_CLOSED)

# `closed` always carries one of these in triage_items.close_reason.
CLOSE_DUPLICATE = "duplicate"
CLOSE_FIXED_BY = "fixed_by"
CLOSE_IGNORED = "ignored"
CLOSE_RESOLVED = "resolved"
CLOSE_REASONS = (CLOSE_DUPLICATE, CLOSE_FIXED_BY, CLOSE_IGNORED, CLOSE_RESOLVED)

# Forward order of the pipeline. set_state() resets the strike counter when an
# item advances to merging or beyond (or leaves an end state), so "three strikes"
# always means three consecutive failures of ONE step.
_PIPELINE_RANK = {STATE_NEW: 0, STATE_TRIAGED: 1, STATE_WORKING: 2, STATE_MERGING: 3,
                  STATE_VERIFYING: 4, STATE_FIXED: 5}
_END_STATES = (STATE_NEEDS_DECISION, STATE_FAILED, *TERMINAL_STATES)
# An item in one of these is no longer open: not a target to attach or merge another into.
NOT_OPEN_STATES = (*TERMINAL_STATES, STATE_FAILED)

# The one retry rule. An INFRASTRUCTURE failure (sideclaw 5xx or unreachable, a terminal episode
# with no verdict, a review that produced no verdict, an implement episode that ended without a
# pull request, a lost in-flight operation) is retried with backoff; the third strike lands
# `failed` carrying the reason. Never a clock on a RUNNING episode: a strike is only recorded once
# the episode is over.
STRIKE_LIMIT = 3
STRIKE_BACKOFF_MINUTES = (10, 30)   # after strike 1, after strike 2

# implement_job values that mean "claimed, not yet (or no longer) a sideclaw job".
# The claim is a compare-and-set on `implement_job IS NULL`, written BEFORE the
# external call, so two processes (the loop and the sweep) never submit twice.
IMPLEMENT_CLAIM = "claiming"
HOST_VERB_CLAIM_PREFIX = "host-verb:"


def is_claim(value: str | None) -> bool:
    return bool(value) and (value == IMPLEMENT_CLAIM or value.startswith(HOST_VERB_CLAIM_PREFIX))


# The only states the shared channel is told about, one line each (notify_cluster()).
NOTIFY_STATES = (STATE_NEEDS_DECISION, STATE_FIXED)
# A pseudo-state, not a ledger state: a `closed(resolved)` item that ASKED for an answer
# (max_tier=investigate) and has its own origin thread is answered THERE, once. It is
# what `card_hash` holds after that line was posted, and the shared channel never hears it.
NOTIFY_ANSWERED = "answered"
# `card_hash` while one process is posting a line: `posting:<state>:<iso claimed-at>`. The
# loop and the sweep both notify, so the claim is what keeps one line from posting twice;
# a claim older than one loop interval is a crashed poster's and may be retaken.
NOTIFY_CLAIM_PREFIX = "posting:"
NOTIFY_CLAIM_STALE_S = 600

# `triage_items.note` prefixes for the grouped-source resolve paths. Deliberately NOT
# "fixed"/"resolved" wording: a service that is fully down also stops emitting, so silence is
# never proof of a fix. The one genuine "actually fixed" claim is VERIFIED_NOTE_PREFIX
# (maybe_verify()), backed by `make verify` and the deployed change.
QUIET_RESOLVE_NOTE_PREFIX = "signal quiet since "
RECOVERY_PAIRED_NOTE_PREFIX = "recovery message observed: "
# _dissolve_cluster()'s note prefix: the dissolve verdict's text (summary + verdict +
# recommendation, the text DISSOLVE_MARKER is matched against), so a `triaged` row's obligation is
# readable on its own row, not only inside dispatches.verdict_json.
SPLIT_VERDICT_NOTE_PREFIX = "cluster split — the investigation's verdict, pending individual re-evaluation: "

NOTIFY_ICON = {
    STATE_NEEDS_DECISION: ":raising_hand:",
    STATE_FIXED: ":white_check_mark:",
    NOTIFY_ANSWERED: ":speech_balloon:",
}

DEFAULT_CARD_CHANNEL = "C0BVDE5R562"  # #agents — see config/triage-policy.json
DEFAULT_MIN_OCCURRENCES = 3
DEFAULT_MIN_OPEN_MINUTES = 30
DEFAULT_COOLDOWN_HOURS = 6
# Grouped sources (slack_alert, hermes_log) never disappearance-resolve on their own:
# watchdog-poll.py's sweep_stale_grouped() only clears them after 7 idle DAYS (GROUPED_TTL_DAYS),
# housekeeping rather than signal. 2h is the triage-side default: well past the 30-min
# watchdog-poll cadence (4+ consecutive misses before a false resolve) and short of either grouped
# source's re-emit window (6h/24h), so a signature that is genuinely still flapping is re-noticed
# well before this fires, while a fixed-and-deployed alert still closes the same day. See
# resolve_quiet_grouped().
DEFAULT_QUIET_RESOLVE_HOURS = 2.0

# A signature that reopened this many times inside the window is CHRONIC: each occurrence clears
# on its own, so every silence path would take it back to `quiet` in the same pass that reopened
# it, and escalate(), which runs after them, would never see it. A self-clearing alert that keeps
# coming back is itself the defect (a real fault or a miscalibrated monitor), so a chronic,
# mapped `new` row is exempt from silence-resolve and escalates like any other. See
# _is_chronic().
DEFAULT_CHRONIC_RECURRENCES = 3
DEFAULT_CHRONIC_WINDOW_DAYS = 7.0

# Implement attempts per item: the first, plus up to three revisions or re-dispatches (a blocked
# review, failed checks, a base that moved; see maybe_revise_blocked()).
# `triage_items.revision_count` counts the attempts after the first. An attempt count per item,
# never a turn or time limit on the episode itself.
MAX_IMPLEMENT_ATTEMPTS = 4

# Attempt N >= this one runs on sideclaw's escalation implement model (GET /api/routing), when
# it has one; see implement_model().
ESCALATION_ATTEMPT = 3

# How long an item waits after sideclaw's per-repo implement lease refused its episode (another
# implement episode holds the repo) before the attempt is submitted again. Not a strike.
LEASE_RETRY_MINUTES = 10

# maybe_auto_remediate()'s cooldown/attempt-cap defaults: the same shape as DEFAULT_COOLDOWN_HOURS
# but against `operations`, not `dispatches` (see _host_verb_cooldown_ok()). A flapping signal must
# not restart a live process every 10 minutes, and a verb that already failed twice against THIS
# item is a deterministic failure.
DEFAULT_HOST_VERB_COOLDOWN_HOURS = 6.0
DEFAULT_HOST_VERB_MAX_ATTEMPTS = 2

# A host verb keeps a confidence bar that auto-implement does not have (review is its gate): a
# restart from HOST_VERB_ALLOWLIST is idempotent, followed by a positive liveness probe before the
# item is marked done, and capped at `hostVerbMaxAttempts`, so a wrong guess costs one restart and
# a `failed` card with the receipt, cheaper than a human running the same restart. The default is
# `medium`; a policy may choose another bar from this closed vocabulary.
CONFIDENCE_RANK: dict[str, int] = {"low": 0, "medium": 1, "high": 2}
DEFAULT_HOST_VERB_MIN_CONFIDENCE = "medium"

# Concurrency ceiling: simultaneously-open CLUSTERS (distinct dispatch_job values of a `working`
# investigation) this loop may have outstanding. It bounds how many run AT THE SAME TIME, which
# matters because sideclaw's own concurrency limits are shared with every other dispatch source.
MAX_OPEN_INVESTIGATIONS = int(os.environ.get("TRIAGE_MAX_OPEN_INVESTIGATIONS", "3"))
# A bare GET/POST against sideclaw or Slack must never hang a 10-minute cron indefinitely.
SUBPROCESS_TIMEOUT = int(os.environ.get("TRIAGE_SUBPROCESS_TIMEOUT", "60"))
# How many signatures ride in one cluster's brief. The rest stay in `new` and wait for a later
# run: never dropped, never silently merged in anyway.
MAX_CLUSTER_SIGNATURES = 5

# Briefs are capped in Python at the point the text is assembled, not left to a downstream
# script to enforce alone.
MAX_BRIEF_CHARS = 8000

# A cluster is a hypothesis, not an assertion. The brief asks the episode to say so, in these
# exact words, if it finds the grouped signatures do NOT share a root cause: a plain,
# case-sensitive substring check on the folded verdict's own text, never an LLM call here.
DISSOLVE_MARKER = "UNRELATED SIGNATURES"

DAILY_DIGEST_CURSOR_KEY = "triage_failed_digest_date"

# hermes-ops.sh deliberately lives in hermes-agent: the own-monitor probe
# (gather_kuma_push_fresh) shells out to it as a live cross-repo argv. Same env-override shape as
# GH_BIN: env var first, documented default second.
_env_ops_bin = os.environ.get("WARDEN_HERMES_OPS_BIN")
HERMES_OPS_BIN = Path(_env_ops_bin).expanduser() if _env_ops_bin else (HERMES_HOME / "scripts" / "hermes-ops.sh")

# Host verbs: a closed allowlist. A policy rule (`hostVerbs` in load_policy()) may SELECT a key
# from this dict, never express an argv of its own: a launchd label or container name reaching
# config would let policy express commands (DESIGN.md C2). An entry is added only once its target
# is confirmed live; a guessed name in a closed, security-relevant allowlist is worse than no
# entry.
HOST_VERB_ALLOWLIST: dict[str, list[str]] = {
    "restart-hermes-gateway": ["launchctl", "kickstart", "-k", f"gui/{os.getuid()}/ai.hermes.gateway"],
}
HOST_VERB_TIMEOUT = 90

# The push-heartbeat Uptime Kuma monitor whose Slack line PROVES a verb's target came back.
# Lives in CODE, not policy: a policy edit must never choose what a positive liveness probe
# confirms. Keyed by VERB, not by the triggering item's signature: several signatures map to the
# same restart, so they share the same liveness check regardless of which fired first.
HOST_VERB_LIVENESS_MONITOR: dict[str, str] = {
    "restart-hermes-gateway": "Hermes Agent - Push",
}

# Enforced at IMPORT time: a HOST_VERB_ALLOWLIST key with no HOST_VERB_LIVENESS_MONITOR entry
# would still run, but its item would get `deploy_expect_json="[]"` on success, and
# gather_kuma_push_fresh() refuses an empty `expected`, so the item would cycle verifying -> new
# after every verify window, never confirmed and never reaching a human. A missing pairing is a
# bug in THIS FILE, not a policy mistake, so it fails the module import.
_missing_liveness_monitor = sorted(set(HOST_VERB_ALLOWLIST) - set(HOST_VERB_LIVENESS_MONITOR))
if _missing_liveness_monitor:
    raise AssertionError(
        f"HOST_VERB_ALLOWLIST key(s) {_missing_liveness_monitor} have no matching "
        f"HOST_VERB_LIVENESS_MONITOR entry — every host verb needs a push monitor to confirm "
        f"liveness against, or its items can never resolve"
    )

ALERTS_CHANNEL = "C0AS1LAUQ3C"  # #alerts — same channel watchdog-poll.py's slack_alert source reads

# The own-monitor probe is bounded by a hard wall-clock timeout: a hung hermes-ops call must never stall a 10-minute cron.
EVIDENCE_TIMEOUT = int(os.environ.get("TRIAGE_EVIDENCE_TIMEOUT", "20"))

# Step 7 is a genuinely SEPARATE read of the implement episode's diff: sideclaw's `review` job,
# which returns a TYPED verdict (`outcome`/`blocking`/...), so the step is machine-readable without
# a substring match on prose. See open_validation_dispatch(), advance_merge_trains() and
# clients/sideclaw.py's REVIEW_SCHEMA_VERSION/assert_result_schema().
#
# Matches a GitHub pull-request URL's trailing `/pull/<n>`, deliberately strict (anchored at the
# end, digits only) so an unexpected URL fails loudly rather than reviewing the wrong number.
PR_NUMBER_RE = re.compile(r"/pull/(\d+)/?$")

# How long a deployed item's own signal must stay quiet before it is `fixed` (maybe_verify()).
# Comfortably longer than one 10-minute cron cycle, so a slow-to-propagate change is not mistaken
# for a failure.
VERIFY_WINDOW_HOURS = float(os.environ.get("TRIAGE_VERIFY_WINDOW_HOURS", "2"))
# Consecutive verification passes that may fail before the failure is real.
VERIFY_FAILURE_LIMIT = 3

# scripts/ledger.py, loaded by path (the sibling filenames are not importable as modules). It
# owns the schema and the migrations.
_LEDGER_PATH = _SCRIPTS_DIR / "ledger.py"
_ledger_spec = importlib.util.spec_from_file_location("ledger", _LEDGER_PATH)
assert _ledger_spec and _ledger_spec.loader, "Failed to load scripts/ledger.py"
_ledger = importlib.util.module_from_spec(_ledger_spec)
_ledger_spec.loader.exec_module(_ledger)

# scripts/api.py, loaded by path like ledger.py. Its `health_payload()`/`metrics_payload()`/
# `board_payload()`/`item_payload()` take an open connection and return a plain dict, and
# importing it has no side effect (its HTTP server only starts behind `--serve`), so
# build_argo_snapshot() and warden-api's own `/health`/`/metrics`/`/board`/`/items` agree by
# construction.
_API_PATH = _SCRIPTS_DIR / "api.py"
_api_spec = importlib.util.spec_from_file_location("warden_api", _API_PATH)
assert _api_spec and _api_spec.loader, "Failed to load scripts/api.py"
api = importlib.util.module_from_spec(_api_spec)
_api_spec.loader.exec_module(api)


def db_connect() -> sqlite3.Connection:
    """The loop (`scripts/triage.py`) is the only process allowed to pass migrate=True: DESIGN.md
    § The ledger names it as the one process running at boot. Every other reader/writer
    (watchdog-poll.py, dispatch-sweep.py) calls ledger.connect() without it and asserts the
    version, so a process that starts before the loop has touched a fresh ledger fails loudly
    rather than inventing its own tables."""
    return _ledger.connect(DB_PATH, migrate=True)


def apply_db_override(argv: list[str]) -> None:
    """--db PATH, or the HERMES_CC_DB env var hermes-cc.sh/dispatch-sweep.py also honor: points the
    loop at a throwaway copy of the DB. Thin wrapper over ledger.apply_db_override(), which owns no
    DB_PATH of its own; this module's global is rebound via the setter below."""
    def _set(path: Path) -> None:
        global DB_PATH
        DB_PATH = path
    _ledger.apply_db_override(argv, _set, env_var="HERMES_CC_DB")


# `resolve_slack_token` is re-bound as a plain module global so tests patch it here; every call
# site resolves it at call time.
_SLACK_CLIENT_PATH = _SCRIPTS_DIR / "slack_client.py"
_slack_client_spec = importlib.util.spec_from_file_location("slack_client", _SLACK_CLIENT_PATH)
assert _slack_client_spec and _slack_client_spec.loader, "Failed to load scripts/slack_client.py"
_slack_client = importlib.util.module_from_spec(_slack_client_spec)
_slack_client_spec.loader.exec_module(_slack_client)

resolve_slack_token = _slack_client.resolve_slack_token


# normalize_title() / fingerprint() are reused from watchdog-poll.py, loaded by path, so a change
# there is picked up here. The hand-mirrored fallback only runs if that script could not be
# loaded at all, which keeps this module independently runnable.
_WATCHDOG_POLL_PATH = _SCRIPTS_DIR / ("watchdog" + "-poll.py")
try:
    _wp_spec = importlib.util.spec_from_file_location("watchdog_poll_for_triage", _WATCHDOG_POLL_PATH)
    assert _wp_spec and _wp_spec.loader
    _watchdog_poll = importlib.util.module_from_spec(_wp_spec)
    _wp_spec.loader.exec_module(_watchdog_poll)
    normalize_title = _watchdog_poll.normalize_title
    fingerprint = _watchdog_poll.fingerprint
except Exception as _wp_exc:  # pragma: no cover - defensive: keep this module independently runnable
    import re as _re

    # Once per import, stderr only: the mirror below can drift from watchdog-poll.py, so the
    # fallback must never be taken silently (fingerprints would diverge from the poller's).
    print(f"warden: watchdog-poll.py failed to load ({_wp_exc!r}) — using the hand-mirrored "
          "normalize_title()/fingerprint() fallback", file=sys.stderr)

    _DEDUP_NORMALIZE = _re.compile(r"[^a-z0-9]+")

    def normalize_title(text: str) -> str:  # type: ignore[no-redef]
        """Mirrors watchdog-poll.py's normalize_title() by hand; only runs if that script could not be loaded."""
        return _DEDUP_NORMALIZE.sub("-", text.lower()).strip("-")[:120]

    fingerprint = normalize_title  # type: ignore[assignment]


def post_line(channel: str, text: str, token: str, *,
              thread_ts: str | None = None) -> tuple[bool, str | None]:
    """One plain `chat.postMessage`: text only, no blocks, never an edit. Returns (ok, Slack's own
    `ts`). `WARDEN_SLACK_API` (clients/slack.py's `slack_api_base()`, read at call time) retargets
    it at a stub server."""
    result = _slack_client.slack_post_message(token, channel, text, thread_ts)
    if not result.get("ok"):
        print(f"triage: slack post failed: {result.get('error', 'unknown')}", file=sys.stderr)
        return False, None
    return True, result.get("ts")


def _valid_rule(r: Any) -> bool:
    """A routing rule needs a `match` and a `repo`: a match is a label route to that repo."""
    return isinstance(r, dict) and bool(r.get("match")) and bool(r.get("repo"))


def _valid_host_verb_rule(r: Any) -> bool:
    """A `hostVerbs` rule needs `match` and a `verb` FROM HOST_VERB_ALLOWLIST, a closed key set: a
    policy file names a KEY, never a command, and a typo'd key is a policy bug worth surfacing
    loudly at load time."""
    if not (isinstance(r, dict) and r.get("match") and r.get("verb")):
        return False
    verb = r["verb"]
    if verb not in HOST_VERB_ALLOWLIST:
        print(f"triage: policy hostVerbs rule {r.get('match')!r} names verb {verb!r}, not in "
              f"HOST_VERB_ALLOWLIST {sorted(HOST_VERB_ALLOWLIST)} — dropping this rule", file=sys.stderr)
        return False
    return True


def _valid_host_verb_min_confidence(value: Any) -> str:
    """`hostVerbMinConfidence` names a level from `CONFIDENCE_RANK` (`high`/`medium`/`low`), never a
    number: the same closed-vocabulary validate-at-load shape as `_valid_host_verb_rule()`. An absent
    value defaults to DEFAULT_HOST_VERB_MIN_CONFIDENCE silently; an unrecognized one is a policy
    typo and falls back the same way, but LOUDLY, so it never reads as warden quietly requiring
    less confidence than the file says."""
    if value is None:
        return DEFAULT_HOST_VERB_MIN_CONFIDENCE
    level = str(value).strip().lower()
    if level in CONFIDENCE_RANK:
        return level
    print(f"triage: policy hostVerbMinConfidence {value!r} is not one of {sorted(CONFIDENCE_RANK)} — "
          f"falling back to {DEFAULT_HOST_VERB_MIN_CONFIDENCE!r}", file=sys.stderr)
    return DEFAULT_HOST_VERB_MIN_CONFIDENCE


def _valid_host_verb_positive_number(value: Any, *, key: str, default: float, cast: type) -> float | int:
    """Shared by `hostVerbCooldownHours`/`hostVerbMaxAttempts` at load time.

    Deliberately NOT `data.get(key) or default`: that treats `0` as absent and falls back to the
    default INSTEAD OF THE CONFIGURED ZERO. A configured `0` or negative number is not "unset", it
    would make the cooldown/attempt-cap gates always pass or never bind. Same contract as
    _valid_host_verb_min_confidence(): an absent value defaults silently, anything present but
    non-numeric or `<= 0` is a policy mistake and falls back with a stderr line naming what was
    rejected."""
    if value is None:
        return default
    try:
        parsed = cast(value)
    except (TypeError, ValueError):
        print(f"triage: policy {key} {value!r} is not a number — falling back to {default!r}", file=sys.stderr)
        return default
    if parsed <= 0:
        print(f"triage: policy {key} {value!r} must be > 0 — falling back to {default!r}", file=sys.stderr)
        return default
    return parsed


def load_policy() -> dict[str, Any]:
    try:
        data = json.loads(POLICY_PATH.read_text())
    except (OSError, json.JSONDecodeError) as e:
        print(f"triage: could not read policy at {POLICY_PATH}: {e} — nothing will map or escalate this run",
              file=sys.stderr)
        data = {}
    return {
        "cardChannel": data.get("cardChannel") or DEFAULT_CARD_CHANNEL,
        "minOccurrences": int(data.get("minOccurrences") or DEFAULT_MIN_OCCURRENCES),
        "minOpenMinutes": int(data.get("minOpenMinutes") or DEFAULT_MIN_OPEN_MINUTES),
        "cooldownHours": int(data.get("cooldownHours") or DEFAULT_COOLDOWN_HOURS),
        "quietResolveHours": float(data.get("quietResolveHours") or DEFAULT_QUIET_RESOLVE_HOURS),
        "chronicRecurrences": int(data.get("chronicRecurrences") or DEFAULT_CHRONIC_RECURRENCES),
        "chronicWindowDays": float(data.get("chronicWindowDays") or DEFAULT_CHRONIC_WINDOW_DAYS),
        # Routing rules: a signature glob -> repo, the deterministic label tier (see label_route()).
        # Validated here, at load, like hostVerbs.
        "rules": [r for r in (data.get("rules") or []) if _valid_rule(r)],
        # `ignore` entries are a bare pattern string or an object with a `match` string; only `match` is used for fnmatch.
        "ignore": [
            p if isinstance(p, str) else p["match"]
            for p in (data.get("ignore") or [])
            if isinstance(p, str) or (isinstance(p, dict) and isinstance(p.get("match"), str))
        ],
        # The host-verb allowlist's rule set (HOST_VERB_ALLOWLIST): first-match-wins over the two match
        # targets (see maybe_auto_remediate()), validated at load time so a typo'd verb key is a loud
        # stderr line here instead of a rule that silently never fires.
        "hostVerbs": [r for r in (data.get("hostVerbs") or []) if _valid_host_verb_rule(r)],
        "hostVerbCooldownHours": _valid_host_verb_positive_number(
            data.get("hostVerbCooldownHours"), key="hostVerbCooldownHours",
            default=DEFAULT_HOST_VERB_COOLDOWN_HOURS, cast=float),
        "hostVerbMaxAttempts": _valid_host_verb_positive_number(
            data.get("hostVerbMaxAttempts"), key="hostVerbMaxAttempts",
            default=DEFAULT_HOST_VERB_MAX_ATTEMPTS, cast=int),
        "hostVerbMinConfidence": _valid_host_verb_min_confidence(data.get("hostVerbMinConfidence")),
        # Filters Hermes's own conversational replies that watchdog-poll.py ingested from #alerts as if
        # they were alerts; routed to `closed(ignored)` (see classify()).
        "ignoreUnstructuredSlackProse": bool(data.get("ignoreUnstructuredSlackProse")),
    }


def card_channel(policy: dict[str, Any]) -> str:
    return policy["cardChannel"]


# The sources whose identity is `fingerprint(title)` (watchdog-poll.py GROUPED_SOURCES): their
# external_id and their title target are both digit-free, so a policy pattern for one never
# contains a digit.
_FINGERPRINTED_SOURCES = ("slack_alert", "slack_update", "hermes_log")
_BATCH_SUFFIX_RE = re.compile(r"\s*\(×\d+ in batch\)$")


def match_targets(event_row: sqlite3.Row) -> list[str]:
    """Two candidate strings a policy rule can match against, in order: the raw
    `source:external_id` (works for grouped/self-describing sources), and `source:<normalized
    title>` (works for a state source like `uk`, whose external_id is an opaque, unglobbable monitor
    id). The normalized title of a grouped source is its `fingerprint()`, the form its external_id
    has, of the title with the display suffix ` (×N in batch)` stripped; a state source's is
    `normalize_title()`."""
    source = event_row["source"]
    external_id = event_row["external_id"] or ""
    targets = [f"{source}:{external_id}"]
    title = event_row["title"] or ""
    if source in _FINGERPRINTED_SOURCES:
        norm = fingerprint(_BATCH_SUFFIX_RE.sub("", title))
    else:
        norm = normalize_title(title)
    if norm:
        alt = f"{source}:{norm}"
        if alt not in targets:
            targets.append(alt)
    return targets


def fnmatch_any(targets: list[str], patterns: list[str]) -> bool:
    return any(fnmatch.fnmatch(t, p) for t in targets for p in patterns)


def match_rule(targets: list[str], rules: list[dict[str, Any]]) -> dict[str, Any] | None:
    """First rule (already validated by _valid_rule()/_valid_host_verb_rule()) whose `match`
    fnmatches any target."""
    for rule in rules:
        for t in targets:
            if fnmatch.fnmatch(t, rule["match"]):
                return rule
    return None


# `[`  — UptimeKuma's own bracketed monitor-name format: "[X] [:red_circle: Down] ..."
# emoji — HyperDX/argo-alert style: "🚨 ...", "✅ ...", "⚠️ ...", "*⚠️ ..." (bold mrkdwn)
_BOT_ALERT_PREFIXES = ("[", "\U0001F6A8", "✅", "⚠️", "*⚠️")


def looks_like_bot_alert(title: str) -> bool:
    return (title or "").lstrip().startswith(_BOT_ALERT_PREFIXES)


def now_iso(now: dt.datetime) -> str:
    return now.isoformat()


def safe_json(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return data if isinstance(data, dict) else {}


def parse_ts(value: str | None) -> dt.datetime | None:
    if not value:
        return None
    try:
        parsed = dt.datetime.fromisoformat(value)
    except (TypeError, ValueError):
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=dt.timezone.utc)
    return parsed


def fmt_ts(value: str | None) -> str:
    parsed = parse_ts(value)
    if parsed is None:
        return value or "?"
    return parsed.astimezone(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")


def age_minutes(first_seen: str | None, now: dt.datetime) -> float:
    parsed = parse_ts(first_seen)
    if parsed is None:
        return 0.0
    return (now - parsed).total_seconds() / 60.0


def signature(event_row: sqlite3.Row) -> str:
    return f"{event_row['source']}:{event_row['external_id']}"


def get_event(conn: sqlite3.Connection, event_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()


def get_item(conn: sqlite3.Connection, event_id: int) -> sqlite3.Row | None:
    return conn.execute("SELECT * FROM triage_items WHERE event_id=?", (event_id,)).fetchone()


# Lives in lifecycle/items.py so the CLI's own transitions stamp it the same way.
occurrence_mark = _items.occurrence_mark


# Every column a state transition may write alongside `state`. A closed allowlist: these names
# are interpolated into SQL, and a policy file (or any caller) may name and parameterise but never
# express. `state` and `updated_at` are not in it; they are the helper's own, written on every
# transition, never by a caller.
_SET_STATE_COLUMNS = (
    "note", "dispatch_job", "card_channel", "card_ts", "card_hash", "artifact_url",
    "pr_url", "implement_job", "validation_job", "deploy_expect_json",
    "verify_started_at", "verify_mark", "verify_failures", "verify_result",
    "revert_pr", "close_reason", "strikes", "retry_at", "revision_count",
    "root_cause", "duplicate_of", "triage_job", "triage_job_at", "repo",
    "train_stage", "train_sha", "train_job", "reviewed_sha", "train_evidence", "train_rewinds",
    "train_pushed_at", "merged_sha", "merge_method", "reverting_sha", "revert_json",
    "sweep_pr", "sweep_job", "sweep_job_at", "sweep_attempts", "sweep_candidates", "fixed_by_pr",
)

# The merge train's position, meaningful only while the item is `merging`: leaving it clears
# them (set_state()), so a train never resumes from a stale stage or SHA. `reviewed_sha` and
# `train_evidence` are not among them; see advance_merge_trains().
_TRAIN_POSITION = ("train_stage", "train_sha", "train_job", "train_pushed_at")

# An item entering `verifying` starts with no verification history: no deploy yet
# (`verify_started_at` NULL), no baseline, no failures. `deploy_expect_json` is the host verb's
# monitor record; nothing else writes it. `merged_sha` is what a failed verification reverts: an
# entry from a merge sets it over this reset (merged_entry()), together with how it merged
# (`merge_method`); every other entry (a host verb) has nothing to revert. `fixed_by_pr` marks an
# item a fixed-by sweep put in `verifying` (signal-only); any other entry starts without it.
VERIFY_RESET: dict[str, Any] = {
    "verify_started_at": None, "verify_mark": None, "verify_failures": 0, "verify_result": None,
    "deploy_expect_json": None, "merged_sha": None, "merge_method": None, "fixed_by_pr": None,
}


def merged_entry(item: sqlite3.Row, sha: str | None, method: str | None) -> dict[str, Any]:
    """The columns of `item` entering `verifying` from a merge that landed as `sha` by `method`
    (`unknown` when nothing on record says). A fix's merge (not a revert's: is_revert()) also
    queues the fixed-by sweep of its pull request (advance_fixed_by_sweeps()), whatever becomes of
    this item afterwards."""
    entry = {**VERIFY_RESET, "merged_sha": sha, "merge_method": method or "unknown"}
    if item["pr_url"] and not is_revert(item):
        entry.update(sweep_pr=item["pr_url"], sweep_job=None, sweep_job_at=None, sweep_attempts=0,
                     sweep_candidates=None)
    return entry


class Coalesce(NamedTuple):
    """`set_state(..., artifact_url=Coalesce(url))` writes `artifact_url=COALESCE(?, artifact_url)`:
    keep the existing value when the new one is NULL. One call site needs it (fold_dispatch_verdict(),
    where a verdict with no artifact must not erase the pull request an earlier fold recorded), and
    it is a sentinel rather than a second helper so that site is not a raw UPDATE."""

    value: Any


def set_state(conn: sqlite3.Connection, event_id: int, state: str, now: dt.datetime, *,
                expect_state: str | None = None, expect_null: tuple[str, ...] = (),
                expect_eq: dict[str, Any] | None = None, **columns: Any) -> int:
    """The ONLY place triage_items.state is written. Returns rowcount.

    `closed` must carry its reason: a `close_reason` in CLOSE_REASONS is required, and every other
    state clears it (as it does `duplicate_of`, which only a `closed(duplicate)` item carries), so a
    reopened item never keeps the reason it was closed with. Entering `new` clears `triage_job` (and
    `triage_job_at`) the same way, any state but `merging` clears the merge train's position
    (_TRAIN_POSITION), and entering `new` or `triaged` clears the revert record (`reverting_sha`,
    `revert_json`): an item back before its investigation starts over and must not reopen an old
    revert.

    The strike counter is owned here too: an item that advances to `merging`, `verifying` or `fixed`
    (a step SUCCEEDED; claiming `working` is not progress, the episode may still fail), or that
    leaves an end state (needs_decision, failed, terminal) by any route, gets `strikes=0,
    retry_at=NULL` unless the caller passed `strikes` itself (strike() does), so "three strikes"
    always means three consecutive failures of one step. A retry (which moves an item BACKWARD, or
    leaves it where it is) never resets its own counter; a caller whose success does not change
    state (a verdict folded onto a `working` row) resets it explicitly.

    `expect_state`/`expect_null`/`expect_eq` turn the UPDATE into a compare-and-swap;
    maybe_auto_implement() claims an item that way, and the returned rowcount is how it learns
    whether it won.

    `note` is capped to one short line (lifecycle/items.py `cap_note()`): whitespace collapsed, at
    most 200 characters, the last of them an ellipsis when cut.

    Also writes `occurrence_mark` (see occurrence_mark()) on EVERY transition, computed here from
    the event row and not caller-settable: a closed list of "states that need a mark" is one more
    list to forget to update. reopen_if_needed() is the only reader: it compares the stored mark
    against the event's CURRENT mark to tell a closed row that is still quiet from one a fresh
    occurrence reopened underneath.

    See `record_created_transition()` for the one exception: the two `INSERT INTO triage_items`
    creation sites, which start a row at `new` without calling this function.

    Also appends exactly one `item_transitions` row on a REAL state change (rowcount>0 AND the prior
    state differs from `state`) and nothing otherwise. It is the only writer of that table, for the
    same reason it is the only writer of `triage_items.state`: a second writer of history is a
    second source of truth. The guard matters because this UPDATE also serves callers that do not
    change state (they pass `state` unchanged); recording those would fill the table with noise and
    corrupt every duration /metrics computes from it. `note` rides along verbatim when the caller
    passed one; otherwise it stays NULL."""
    known = (STATE_NEW, STATE_TRIAGED, STATE_WORKING, STATE_MERGING, STATE_VERIFYING,
             STATE_NEEDS_DECISION, STATE_FAILED, *TERMINAL_STATES)
    if state not in known:
        raise ValueError(f"{state!r} is not a state of this machine: {known}")
    if state == STATE_CLOSED:
        if columns.get("close_reason") not in CLOSE_REASONS:
            raise ValueError(f"a `closed` transition must carry close_reason= one of {CLOSE_REASONS}")
    else:
        columns["close_reason"] = None
    if columns.get("close_reason") != CLOSE_DUPLICATE:
        columns.setdefault("duplicate_of", None)
    if state == STATE_NEW:
        # `new` means "not yet through the triage step": a triage job left on a row that re-enters it (a
        # recurrence, a manual reopen) would never be submitted again.
        columns.setdefault("triage_job", None)
    if state in (STATE_NEW, STATE_TRIAGED):
        columns.setdefault("reverting_sha", None)
        columns.setdefault("revert_json", None)
    if columns.get("triage_job", "") is None:
        columns.setdefault("triage_job_at", None)   # no job, no job age
    if state != STATE_MERGING:
        for col in _TRAIN_POSITION:
            columns.setdefault(col, None)
        columns.setdefault("train_rewinds", 0)
    expect_eq = expect_eq or {}
    unknown = tuple(c for c in (*columns, *expect_null, *expect_eq) if c not in _SET_STATE_COLUMNS)
    if unknown:
        raise ValueError(f"{unknown} not in _SET_STATE_COLUMNS — column names reach SQL here, so the "
                         f"list is closed on purpose")

    if "note" in columns:
        note_column = columns["note"]
        columns["note"] = (Coalesce(_items.cap_note(note_column.value)) if isinstance(note_column, Coalesce)
                           else _items.cap_note(note_column))

    mark = occurrence_mark(get_event(conn, event_id))

    # The row's state BEFORE this write: the only way to tell a real transition from a column-only write below.
    prev_row = conn.execute("SELECT state FROM triage_items WHERE event_id=?", (event_id,)).fetchone()
    prev_state = prev_row["state"] if prev_row is not None else None

    if "strikes" not in columns and prev_state is not None and prev_state != state:
        advanced = (state in _PIPELINE_RANK and prev_state in _PIPELINE_RANK
                    and _PIPELINE_RANK[state] >= _PIPELINE_RANK[STATE_MERGING]
                    and _PIPELINE_RANK[state] > _PIPELINE_RANK[prev_state])
        if advanced or prev_state in _END_STATES:
            columns["strikes"] = 0
            columns["retry_at"] = None

    sql = "UPDATE triage_items SET state=?, occurrence_mark=?, updated_at=?"
    params: list[Any] = [state, mark, now_iso(now)]
    if "card_hash" not in columns:
        # card_hash is the state a Slack line was posted for (notify_cluster()): leaving that state
        # clears it, so re-entering it later posts again. The CASE reads the row's state from BEFORE this
        # UPDATE.
        sql += ", card_hash=CASE WHEN state=? THEN card_hash END"
        params.append(state)
    for col, value in columns.items():
        if isinstance(value, Coalesce):
            sql += f", {col}=COALESCE(?, {col})"
            params.append(value.value)
        else:
            sql += f", {col}=?"
            params.append(value)
    sql += " WHERE event_id=?"
    params.append(event_id)
    if expect_state is not None:
        sql += " AND state=?"
        params.append(expect_state)
    for col in expect_null:
        sql += f" AND {col} IS NULL"
    for col, value in expect_eq.items():
        if value is None:
            sql += f" AND {col} IS NULL"
        else:
            sql += f" AND {col}=?"
            params.append(value)
    rowcount = conn.execute(sql, params).rowcount

    if rowcount > 0 and prev_state != state:
        note_value = columns.get("note")
        if isinstance(note_value, Coalesce):
            note_value = note_value.value
        conn.execute(
            "INSERT INTO item_transitions(event_id, from_state, to_state, at, note) VALUES (?,?,?,?,?)",
            (event_id, prev_state, state, now_iso(now), note_value),
        )
    return rowcount


def retry_ready_sql(now: dt.datetime, alias: str = "") -> tuple[str, list[str]]:
    """The predicate every poller that SUBMITS adds to its candidate query: a row waiting out a
    strike's backoff is skipped until `retry_at` passes."""
    col = f"{alias}retry_at"
    return f"({col} IS NULL OR {col} <= ?)", [now_iso(now)]


def strike(conn: sqlite3.Connection, event_id: int, now: dt.datetime, reason: str, *,
            retry_state: str, expect_state: str | None = None, expect_eq: dict[str, Any] | None = None,
            **retry_columns: Any) -> str:
    """The one retry rule. An infrastructure failure of the step the item is on increments `strikes`;
    below STRIKE_LIMIT the item goes to `retry_state` (the state whose poller re-submits the failed
    step: `triaged` for an investigation, `working` for an implement/host-verb episode, `merging` for
    a review or merge) with `retry_at` pushed out by the backoff, and `retry_columns` applied (the
    failed attempt's handle cleared, so the poller starts a fresh one). At STRIKE_LIMIT the item is
    `failed`, `reason` is its note, and its columns are left alone as evidence. Returns the state the
    item landed in, or, when `expect_state`/`expect_eq` no longer matched (another pass moved it
    first) and nothing was written, the state it is actually in, logged to stderr.

    A SUBMIT REFUSED by sideclaw (4xx) is not an infrastructure failure and never comes through
    here; see end_on_refusal()."""
    row = get_item(conn, event_id)
    if row is None:
        raise LookupError(f"strike on event {event_id}: no such triage item")
    strikes = row["strikes"] + 1
    if strikes >= STRIKE_LIMIT:
        landed = STATE_FAILED
        written = set_state(conn, event_id, STATE_FAILED, now, expect_state=expect_state,
                             expect_eq=expect_eq, note=reason, strikes=strikes, retry_at=None)
    else:
        landed = retry_state
        backoff = STRIKE_BACKOFF_MINUTES[min(strikes, len(STRIKE_BACKOFF_MINUTES)) - 1]
        retry_at = now_iso(now + dt.timedelta(minutes=backoff))
        written = set_state(conn, event_id, retry_state, now, expect_state=expect_state,
                             expect_eq=expect_eq, note=f"{reason} — retry {strikes}/{STRIKE_LIMIT - 1} after {backoff} min",
                             strikes=strikes, retry_at=retry_at, **retry_columns)
    if written:
        return landed
    # The compare-and-set lost: another pass moved the item first, so nothing here landed and what
    # is reported must be where the item really is.
    current = get_item(conn, event_id)
    if current is None:
        raise LookupError(f"strike on event {event_id}: no such triage item")
    actual = current["state"]
    print(f"triage: strike on event {event_id} lost its compare-and-set (expected {expect_state!r}, "
          f"item is {actual!r}) — nothing written: {reason}", file=sys.stderr)
    return actual


def record_created_transition(conn: sqlite3.Connection, event_id: int, state: str, at: str) -> None:
    """Write the one `item_transitions` row a creation site owes: a raw `INSERT INTO triage_items`
    (unlike every later move) never goes through `set_state()`, so without this call a brand-new
    item has no row in its own history. `from_state` is NULL (there was no prior state) and `at` is
    the item's own `created_at`, not `now()`."""
    conn.execute(
        "INSERT INTO item_transitions(event_id, from_state, to_state, at, note) VALUES (?,?,?,?,?)",
        (event_id, None, state, at, "created"),
    )


def wp_module() -> Any | None:
    """The sibling watchdog-poll.py module, if it loaded (see the try/except above `normalize_title`),
    for callers that need its resolve_secret()/poll_slack_messages() rather than a reimplementation.
    None (never raises) if that load failed, so a broken import degrades one caller, not the
    whole run."""
    return globals().get("_watchdog_poll")


def is_revert(item: sqlite3.Row) -> bool:
    """The item's change in flight is warden's revert of a merged fix, not a fix. True from the
    verify failure until the revert passed `make verify`; in particular when the revert merges,
    which is the predicate a fixed-by sweep after a fix's merge must skip on."""
    return item["reverting_sha"] is not None
