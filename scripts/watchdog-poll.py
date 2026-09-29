"""Watchdog poll — runs every 1800 s as the `com.jkrumm.warden-poll` LaunchAgent.

Polls UptimeKuma, Docker (homelab + vps), GitHub, Slack #alerts/#updates,
Hermes self-state, 1Password ref health on homelab + vps (a no-op
`op run -- true` over the shared .env.tpl — detects a dangling ref directly,
the failure class behind the 2026-08-01 outage, instead of inferring it hours
later from silent heartbeats), and stray agent-created skills under
~/.hermes/skills/ (untracked, unreviewed — the failure class behind the
2026-08-02 skill-sprawl cleanup). Reconciles against ~/.hermes/watchdog.db
(SQLite). Emits NEW=, REMINDERS=, RESOLVED= blocks for the LLM cron prompt.

Source of truth: ~/SourceRoot/warden/scripts/watchdog-poll.py
~/.hermes/scripts/ is a symlink to hermes-agent/scripts, NOT to this
directory — this code left that repo on 2026-09-09 and is reached by its
own path now.
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import json
import os
import re
import sqlite3
import subprocess
import sys
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

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

# scripts/ledger.py — loaded by path, the same mechanism triage.py already
# uses for its own sibling loads (the filenames here are not importable).
# ledger.py is now the sole owner of the schema and the migrations that used
# to live inline as this file's own SCHEMA constant plus an ALTER TABLE
# block.
_LEDGER_PATH = Path(__file__).resolve().parent / "ledger.py"
_ledger_spec = importlib.util.spec_from_file_location("ledger", _LEDGER_PATH)
assert _ledger_spec and _ledger_spec.loader, "Failed to load scripts/ledger.py"
_ledger = importlib.util.module_from_spec(_ledger_spec)
_ledger_spec.loader.exec_module(_ledger)

# scripts/slack_client.py — same by-path loading mechanism, the same shim
# triage.py and dispatch-sweep.py already load: the plain Slack Web API HTTP
# client (scripts/clients/slack.py) this file's own _slack_call() shares its
# request construction with below.
_SLACK_CLIENT_PATH = Path(__file__).resolve().parent / "slack_client.py"
_slack_client_spec = importlib.util.spec_from_file_location("slack_client", _SLACK_CLIENT_PATH)
assert _slack_client_spec and _slack_client_spec.loader, "Failed to load scripts/slack_client.py"
_slack_client = importlib.util.module_from_spec(_slack_client_spec)
_slack_client_spec.loader.exec_module(_slack_client)

# scripts/ (this file's own directory) onto sys.path so `clients` is
# importable as a real package — the same reason triage.py and
# dispatch-sweep.py do this (slack_client.py's own load above already does
# it too, but this makes the precondition explicit for the import below).
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from clients import secrets as _secrets, sideclaw as _sideclaw  # noqa: E402

STATE_PATH = HERMES_HOME / "scripts" / "briefing-state.json"
JOBS_PATH = HERMES_HOME / "cron" / "jobs.json"
CONFIG_PATH = HERMES_HOME / "config.yaml"
SKILLS_DIR = HERMES_HOME / "skills"
BUNDLED_MANIFEST_PATH = SKILLS_DIR / ".bundled_manifest"
SKILL_USAGE_PATH = SKILLS_DIR / ".usage.json"

API_BASE = "https://argo.jkrumm.com/api"
GH_OWNER = "jkrumm"
# `gh search --owner` filters by repo OWNER, never by item AUTHOR — every repo
# here is public, so anyone can open an issue/PR and have its title enter the
# agent loop. Only this login is self-authored/trusted; anything else (or an
# unparseable/missing author) is fail-closed third-party. See _github_author.
TRUSTED_GH_LOGIN = GH_OWNER

CH_ALERTS = "C0AS1LAUQ3C"
CH_UPDATES = "C0ARZJD824W"

# Where the composed NEW/REMINDERS/RESOLVED digest itself is delivered — not one
# of the two channels polled above. Matches the retiring gateway cron job's
# `"deliver": "slack:C0ASRULFTSS"` (hermes-agent cron/jobs.json, job
# 4b1faabda97d, "Watchdog"); this constant is what replaces that gateway-side
# delivery target now that the poller posts for itself instead of relying on
# the gateway to deliver its stdout.
WATCHDOG_CHANNEL = os.environ.get("WATCHDOG_SLACK_CHANNEL", "C0ASRULFTSS")

# Same env-first, absolute-default shape as triage.py's GH_BIN: launchd hands
# this job PATH=/usr/bin:/bin, where a bare `gh` does not exist.
_env_gh_bin = os.environ.get("GH_BIN")
GH_BIN = Path(_env_gh_bin).expanduser() if _env_gh_bin else Path("/opt/homebrew/bin/gh")

UK_DOWN_GATE_MIN = 30
DOCKER_UNHEALTHY_GATE_MIN = 30
GITHUB_STALE_DAYS = 3

# Slack: a topic must fire at least this many times in a single poll batch to surface.
# Single hits are dropped — user already gets a push notification for those.
SLACK_FLAP_THRESHOLD = 3
SLACK_REEMIT_COOLDOWN_HOURS = 24

# Grouped sources are recorded via upsert_grouped (append-only) — reconcile()'s
# disappearance-based resolution never runs for them, so open rows accrue forever
# (and stale hermes_log rows pollute the morning briefing's open list). Sweep any
# open grouped event idle for longer than this.
GROUPED_SOURCES = ("slack_alert", "slack_update", "hermes_log")
# Consecutive failed Slack polls before the run reports failure. At the 30-min cron
# cadence three misses is 90 minutes blind — past any plausible argo/Slack blip, and
# still inside the Kuma monitor's own grace.
SLACK_FAIL_STREAK_ALERT = 3
GROUPED_TTL_DAYS = 7

REM_HOURS = {
    "uk": 6,
    "docker_homelab": 6,
    "docker_vps": 6,
    "github_pr": 72,
    "hermes_cron": 6,
    "hermes_log": 24,
    "op_refs_homelab": 6,
    "op_refs_vps": 6,
    # Governance backlog, not an outage — a weekly cadence rather than
    # uk/docker/op_refs's 6h operational urgency. Still needs to recur (not
    # go silent for weeks): that silence is exactly how the 2026-08-02 stray
    # (89 patches, 20x growth over 18 days) went unnoticed. `github_issue`
    # used to share this exact cadence for the same reason — it no longer
    # appears in this dict at all (docs/waves/PLAN.md Wave 1): every open
    # issue is now a warden triage item with its own investigate/implement
    # verdict and its own deadlines, which supersedes this reminder-digest
    # cadence entirely.
    "stray_skill": 168,
}

# docs/history/state-log.md §49 — an event whose triage_items row is STATE_NEEDS_HUMAN carries a
# real outstanding decision (a verdict with a 7-day deadline), which is a
# different thing than "still down/unresolved on the source's own cadence".
# 24h, not the source's REM_HOURS entry — §41 known-open item 3 scoped
# "reminder at 1d" for exactly this state and left the notification path
# unowned, which is the gap this closes.
REM_HOURS_NEEDS_HUMAN = 24

HERMES_LOG_FILES = [
    ("hermes_errors_log_offset", "logs/errors.log"),
    ("hermes_gw_error_log_offset", "logs/gateway.error.log"),
]
LOG_LEVEL_RE = re.compile(r"\s(ERROR|CRITICAL)\s")
LOG_PARSE_RE = re.compile(r"^\S+\s+\S+\s+(ERROR|CRITICAL)\s+(?:\[[^\]]*\]\s+)?([\w\.]+)\s*:\s*(.+)$")
LOG_TS_RE = re.compile(r"^(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})")
LOG_RECENT_HOURS = 6  # log lines older than this are ignored even on first run

# The sentinel model id a *deliberate* fallback probe sends (see
# hermes-agent's skills/hermes-gateway/SKILL.md): a post-rollout check overrides
# the model to `…-does-not-exist` so the endpoint is guaranteed to 404, proving
# the fallback chain engages. Matched on the raw log line — see poll_hermes_logs().
PROBE_SENTINEL_RE = re.compile(r"model\s+'[^']*-does-not-exist'")

QUIET_START_H = 0
QUIET_END_H = 7


# Probes whether the shared .env.tpl on each host still fully resolves — the
# root cause of the 2026-08-01 outage: `op run --env-file=... -- <script>` (every
# homelab/vps cron's launcher) fails WHOLESALE the instant a single referenced
# 1Password item goes missing (e.g. deleted as part of a service retirement),
# taking every cron sharing that template down at once. This detects the cause
# directly instead of waiting hours for it to surface as silent heartbeats.
# Mirrors scripts/hermes-ops.sh's `env-check` verb (same `op run -- true` probe,
# same stderr parse) but is reimplemented here rather than shelled out to, so the
# watchdog stays a self-contained script with no dependency on that file's shape.
#
# Each command sources the host profile FIRST, behind the `[ -r ]` guard, because
# the crons it stands in for do: every op-wrapped homelab cron line begins
# `. /home/jkrumm/.profile;`, and that is where `OP_SOCK` is pinned. Skipping it
# leaves `op` without the daemon socket, so the client dials an absent path, the
# cache never engages, and the probe spends network requests — which is how a
# host whose six crons were all green got reported as "1Password refs unresolved
# on homelab" with the shared budget's `[ERROR] Too many requests` as the cause
# (2026-09-29). A probe that tests an environment no cron uses answers a
# different question than the one it was built for. The `[ -r ]` guard is
# load-bearing, not decoration: `.` is a POSIX special builtin, so dash aborts
# the ENTIRE command line when the name cannot be opened, and an absent profile
# would then cost every poll rather than the credential alone (homelab
# docs/decisions.md -> 1Password CLI in cron shells).
OP_REF_PROFILE = "[ -r ~/.profile ] && . ~/.profile; "
OP_REF_SSH_TIMEOUT = 20  # seconds; the remote command is a no-op ("-- true")
OP_REF_HOSTS: dict[str, str] = {
    "homelab": OP_REF_PROFILE + "cd ~/homelab && op run --env-file=.env.tpl -- true",
    "vps": OP_REF_PROFILE + "cd ~/vps && op run --env-file=.env.tpl -- true",
}
# `op run` prints "could not find item <name> in vault <id>" or "could not resolve
# item UUID for item <name>: ...". Only the item name is ever extracted — the
# vault id is deliberately left alone, it never needs to be logged.
OP_RUN_MISSING_ITEM_RE = re.compile(r"could not find item (\S+) in vault")
OP_RUN_MISSING_UUID_RE = re.compile(r"could not resolve item UUID for item ([^:\s]+)")

# Timestamp shapes to strip from the `raw:` op-refs fallback signature (see
# poll_op_refs()) BEFORE it reaches normalize_title() — deliberately NOT a
# change to normalize_title() itself, which every other source depends on
# unchanged. Without this, a real op run error like "[ERROR] 2026/09/01
# 15:00:34 (504) Unknown: An unknown error occurred." mints a fresh
# external_id every 30-min poll (the timestamp differs each time), so
# reconcile()'s disappearance logic resolves the "old" row and inserts a
# "new" one every cycle — the DB reports the dangling ref clearing every 30
# minutes while it stays dead indefinitely. Matches an ISO-ish date
# (dash OR slash separated) with an optional attached time, and separately
# any standalone run of 6+ digits (epoch-like numbers, long ids) — applied
# on the RAW pre-normalization text, since the timestamp appears in its
# original punctuated form there, not yet collapsed to normalize_title()'s
# dash-joined shape.
OP_REFS_TIMESTAMP_RE = re.compile(
    r"\d{4}[-/]\d{2}[-/]\d{2}(?:[ T]\d{2}[:-]\d{2}[:-]\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?"
)
OP_REFS_LONG_DIGIT_RUN_RE = re.compile(r"\d{6,}")


def _strip_op_refs_timestamps(text: str) -> str:
    """See OP_REFS_TIMESTAMP_RE above — used ONLY by poll_op_refs()'s `raw:`
    fallback dedup key, never by normalize_title()'s other callers."""
    text = OP_REFS_TIMESTAMP_RE.sub(" ", text)
    text = OP_REFS_LONG_DIGIT_RUN_RE.sub(" ", text)
    return text


# op:// refs the watchdog needs. When it runs inside the gateway, the cron
# scheduler's subprocess sanitizer (tools/environments/local.py) strips high-value
# secrets such as GITHUB_TOKEN from the inherited env, and there is no plaintext
# ~/.hermes/.env anymore — so any of these missing from os.environ is resolved on
# demand from the encrypted cache via `secrets-run read` (the drop-in op shim:
# cache backend on the mini, biometric op on the MacBook). Mirrors ~/.hermes/.env.tpl.
_CACHE_REFS: dict[str, str] = {
    "GITHUB_TOKEN": "op://hermes/github/token",
    "HOMELAB_API_KEY": "op://common/api/SECRET",
    "UPTIME_PUSH_WATCHDOG": "op://hermes/uptime-kuma/watchdog-push-url",
    # The Hermes fallback ref triage.py's resolve_slack_token() also carries
    # (its _SLACK_TOKEN_REF) — kept in sync by hand, the same way that file's
    # own Slack helpers are hand-mirrored rather than imported (see its
    # "Reused, not reimplemented" note). Used here ONLY through
    # resolve_alerts_read_token() below, never through resolve_slack_token()'s
    # own Warden-first resolution — see that function's docstring for why.
    "SLACK_BOT_TOKEN": "op://hermes/slack/bot-token",
}


def resolve_alerts_read_token() -> str:
    """Pinned to the Hermes identity — env `SLACK_BOT_TOKEN`, else
    `secrets-run read op://hermes/slack/bot-token` — deliberately never
    Warden's own app (`scripts/clients/slack.py`'s `resolve_slack_token()`,
    which now tries Warden's `WARDEN_BOT_TOKEN` first). The digest this
    poller composes summarizes Slack history read via the homelab API
    (`poll_slack_messages()`, `HOMELAB_API_KEY`, itself proxying to Slack's
    own `conversations.history` with read scopes Warden's `chat:write`-only
    app does not have and is not meant to), so posting that digest under a
    second identity would fragment one feed across two apps for zero
    benefit — this whole read-then-digest path stays Hermes end to end."""
    return resolve_secret("SLACK_BOT_TOKEN")


def resolve_secret(key: str) -> str:
    """Resolve one needed secret: the inherited process env (the gateway exports
    cache-resolved secrets; some survive the cron subprocess sanitizer) first, else
    the encrypted cache via secrets-run — clients/secrets.py's shared resolve_secret(),
    same 15s timeout, same widened PATH every plain-HTTP client in this repo already
    uses. Warns to stderr on total failure so a broken cache surfaces in cron output
    instead of a silently degraded (but rc=0) poll."""
    ref = _CACHE_REFS.get(key, "")
    val = _secrets.resolve_secret(key, ref) if ref else os.environ.get(key, "")
    if not val:
        print(
            f"watchdog: secret {key} unresolved (process env and secrets cache both "
            f"empty) — poll data for its source may be incomplete this cycle",
            file=sys.stderr,
        )
    return val


def load_env() -> dict[str, str]:
    # Inherited process env plus any needed secret backfilled from the cache. Failures
    # to backfill are logged (resolve_secret) rather than silent. No plaintext .env.
    env: dict[str, str] = dict(os.environ)
    for key in _CACHE_REFS:
        if not env.get(key):
            val = resolve_secret(key)
            if val:
                env[key] = val
    return env


def load_state() -> dict[str, Any]:
    try:
        return json.loads(STATE_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def in_quiet_hours(state: dict[str, Any]) -> bool:
    tz_name = state.get("timezone") or "Europe/Berlin"
    try:
        tz = ZoneInfo(tz_name)
    except Exception:
        tz = ZoneInfo("Europe/Berlin")
    return QUIET_START_H <= dt.datetime.now(tz).hour < QUIET_END_H


def vacation_active(state: dict[str, Any]) -> bool:
    vu = state.get("vacation_until")
    if not vu:
        return False
    try:
        return dt.date.today() <= dt.date.fromisoformat(vu)
    except ValueError:
        return False


def http_get(url: str, headers: dict[str, str] | None = None, timeout: int = 15) -> Any:
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError) as e:
        return {"_error": str(e)}


# --- Slack delivery --------------------------------------------------------
#
# A plain HTTP client, mirroring triage.py's _slack_call()/post_blocks() shape
# exactly — never the gateway's live slack_bolt connection. That is the
# property that lets this poller deliver its own digest once it runs as its
# own LaunchAgent instead of as a gateway cron job whose stdout the gateway
# posts on its behalf.
SLACK_POST_URL = "https://slack.com/api/chat.postMessage"


def _slack_call(url: str, payload: dict[str, Any], token: str) -> tuple[bool, str | None]:
    """Transport is clients/slack.py's shared slack_raw_post() (loaded above
    as _slack_client); this wrapper owns only the return-shape and the
    "watchdog:"-prefixed stderr line."""
    data = _slack_client.slack_raw_post(url, payload, token)
    if data is None:
        return False, None
    ok = bool(data.get("ok"))
    if not ok:
        print(f"watchdog: slack call failed: {data.get('error', 'unknown')}", file=sys.stderr)
    return ok, data.get("ts")


def post_text(channel: str, text: str, token: str) -> tuple[bool, str | None]:
    return _slack_call(
        SLACK_POST_URL,
        {"channel": channel, "text": text, "unfurl_links": False},
        token,
    )


def db_connect() -> sqlite3.Connection:
    """Assert-only: watchdog-poll.py is not the migrator (DESIGN.md § The
    ledger — migrations run only by the loop, triage.py, at boot). If this
    runs before the loop has ever touched a fresh ledger, ledger.connect()
    raises a clear schema-version error rather than this file inventing its
    own tables the way it used to."""
    return _ledger.connect(DB_PATH)


def _dispatch_status(conn: sqlite3.Connection, dispatch_id: int | None) -> sqlite3.Row | None:
    """Look up one dispatches row for the watchdog projection. None whenever
    there's nothing to project: no dispatch_id set, the dispatches table
    doesn't exist yet (the CLI / dispatch-sweep.py may never have run on
    a fresh mini), or the row is gone. Every caller treats None as "behave
    exactly as before the dispatch bridge existed" — this must never raise,
    since watchdog-poll.py is a 30-min production cron and a regression here
    is an alerting outage, not a cosmetic miss.
    """
    if not dispatch_id:
        return None
    try:
        return conn.execute(
            "SELECT status, reported_at, verdict_json FROM dispatches WHERE id=?",
            (dispatch_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None


def _triage_item(conn: sqlite3.Connection, event_id: int) -> sqlite3.Row | None:
    """Look up one triage_items row for the reminder path's own state check
    (docs/history/state-log.md §49 — the reminder branch never read the ledger's own
    classification of the event it was about to remind on). None whenever
    there's nothing to project: the triage_items table doesn't exist yet
    (triage.py may never have run against a fresh ledger), or no row for this
    event — every caller treats None as "behave exactly as before this lookup
    existed", the same contract _dispatch_status already uses and for the
    same reason: this is a 30-min production cron and a regression here is an
    alerting outage, not a cosmetic miss.
    """
    try:
        return conn.execute(
            "SELECT state, note FROM triage_items WHERE event_id=?",
            (event_id,),
        ).fetchone()
    except sqlite3.OperationalError:
        return None


def _dispatch_summary(status: str | None, verdict_json: str | None) -> str:
    """Render a completed dispatch's outcome for the reminder digest, in place
    of a bare 'reminder #N'. Deterministic, no LLM — verdict_json is sideclaw's
    own schema-shaped result object, persisted verbatim by dispatch-sweep.py.
    Classification lives once, in clients/sideclaw.py's classify_dispatch_outcome();
    this function owns only the wording."""
    kind, detail = _sideclaw.classify_dispatch_outcome(status, verdict_json)
    if kind == "failed":
        return f"dispatch {detail}"
    if kind == "no_verdict":
        return "dispatch finished, no verdict"
    if kind == "unreadable":
        return "dispatch finished, verdict unreadable"
    if kind == "degraded":
        return "dispatch degraded (tool failure, not a repo finding)"
    return detail or "dispatch finished, no summary"


def cursor_get(conn: sqlite3.Connection, key: str) -> str | None:
    row = conn.execute("SELECT value FROM cursors WHERE key=?", (key,)).fetchone()
    return row["value"] if row else None


def cursor_get_int(conn: sqlite3.Connection, key: str, default: int = 0) -> int:
    """Cursor value as a non-negative int; ``default`` for missing or non-numeric."""
    value = cursor_get(conn, key)
    return int(value) if value and value.isdigit() else default


def cursor_set(conn: sqlite3.Connection, key: str, value: str, now_iso: str) -> None:
    conn.execute(
        "INSERT INTO cursors(key,value,updated_at) VALUES(?,?,?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (key, value, now_iso),
    )


def poll_uk(env: dict[str, str]) -> list[dict[str, Any]]:
    headers = {"Authorization": f"Bearer {env.get('HOMELAB_API_KEY', '')}"}
    data = http_get(f"{API_BASE}/uptime-kuma/monitors", headers)
    if isinstance(data, dict) and "_error" in data:
        return []
    monitors: list[Any] = []
    if isinstance(data, list):
        monitors = data
    elif isinstance(data, dict):
        monitors = data.get("monitors", []) or []
    out: list[dict[str, Any]] = []
    for m in monitors:
        if not isinstance(m, dict):
            continue
        # docs/history/state-log.md §49 / DESIGN.md § Open questions ("Resolved since v1"):
        # group monitors (uk:95 "VPS", uk:179 "Services", uk:186 "Local") are
        # group PARENTS whose children alert on their own — repo IS NULL, so
        # escalate() can never dispatch on them, and they remind every 6h
        # forever. Drop them at the source rather than teach every downstream
        # consumer to special-case a monitor that can never resolve into
        # anything actionable.
        if m.get("type") == "group":
            continue
        # triage.py's synthetic trip (§103) makes shadow monitors go DOWN on
        # purpose; they carry no notification and must never become an event.
        if str(m.get("name") or "").startswith("warden-trip:"):
            continue
        status = m.get("status")
        is_down = (isinstance(status, str) and status.lower() == "down") or status == 0 or status is False
        if not is_down:
            continue
        mid = str(m.get("id") or m.get("monitorId") or m.get("name") or "")
        if not mid:
            continue
        out.append({
            "external_id": mid,
            "title": m.get("name", "monitor") or "monitor",
            "url": m.get("url") or "",
            "payload": {"type": m.get("type"), "status": status},
        })
    return out


def poll_docker(env: dict[str, str], host: str) -> list[dict[str, Any]]:
    """Parse `/docker/{host}/summary` `alerts` block.

    Shape: `{"alerts": {"unhealthyContainers": [name, ...], "highRestartContainers": [name, ...]}}`
    Each entry may be a string (just the name) or a dict with metadata.
    """
    headers = {"Authorization": f"Bearer {env.get('HOMELAB_API_KEY', '')}"}
    data = http_get(f"{API_BASE}/docker/{host}/summary", headers)
    if isinstance(data, dict) and "_error" in data:
        return []
    out: list[dict[str, Any]] = []
    if not isinstance(data, dict):
        return out
    alerts = data.get("alerts") or {}
    if not isinstance(alerts, dict):
        return out

    def _name(item: Any) -> str | None:
        if isinstance(item, str):
            return item
        if isinstance(item, dict):
            return item.get("name") or item.get("container") or item.get("service")
        return None

    for unhealthy in alerts.get("unhealthyContainers", []) or []:
        name = _name(unhealthy)
        if not name:
            continue
        out.append({
            "external_id": f"unhealthy:{name}",
            "title": f"{name} unhealthy ({host})",
            "url": "",
            "payload": {"host": host, "kind": "unhealthy"},
        })
    for restart in alerts.get("highRestartContainers", []) or []:
        name = _name(restart)
        if not name:
            continue
        restarts = restart.get("restarts") if isinstance(restart, dict) else None
        title = f"{name} restart-loop ({host})"
        if restarts:
            title = f"{name} restart-loop ×{restarts} ({host})"
        out.append({
            "external_id": f"restart:{name}",
            "title": title,
            "url": "",
            "payload": {"host": host, "kind": "restart_loop", "restarts": restarts},
        })
    return out


def poll_github(env: dict[str, str]) -> dict[str, list[dict[str, Any]] | None]:
    """A kind maps to None when its search failed — never to `[]`, which
    reconcile() reads as "every open item disappeared" and resolves them all.
    That is what happened from 2026-09-09: launchd's PATH had no `gh`, every
    search raised FileNotFoundError, and six still-open issues were resolved
    on the first LaunchAgent run and never seen again.

    `github_issue` was dropped from this poll's `out` shape (docs/waves/
    PLAN.md Wave 1): warden's own no-label issue intake
    (`triage.py`'s `ingest_github_issues()`) now assesses every open issue
    directly, which supersedes this stale-issue governance digest entirely —
    a `github_issue` item carries a real investigate/implement verdict,
    while this poll only ever produced an age-gated "still open" line.
    `github_pr` is unaffected: it stays exactly as it was, the review-without-
    CodeRabbit path (FLOWS.md flow 4) has nothing to do with issue intake."""
    out: dict[str, list[dict[str, Any]] | None] = {"github_pr": None}
    gh_env = os.environ.copy()
    if env.get("GITHUB_TOKEN"):
        gh_env["GITHUB_TOKEN"] = env["GITHUB_TOKEN"]
    threshold = dt.datetime.now(dt.timezone.utc) - dt.timedelta(days=GITHUB_STALE_DAYS)

    queries = [
        ("github_pr", "prs", "title,repository,url,number,updatedAt,createdAt,isDraft,author"),
    ]
    for kind, search, fields in queries:
        try:
            res = subprocess.run(
                [str(GH_BIN), "search", search, "--owner", GH_OWNER, "--state", "open",
                 "--json", fields, "--limit", "50"],
                capture_output=True, text=True, timeout=30, env=gh_env,
            )
            if res.returncode != 0:
                print(f"watchdog: gh search {search} exited {res.returncode}: {res.stderr.strip()[:200]}",
                      file=sys.stderr)
                continue
            items = json.loads(res.stdout or "[]")
        except (subprocess.TimeoutExpired, json.JSONDecodeError, FileNotFoundError) as e:
            print(f"watchdog: gh search {search} failed: {e!r}", file=sys.stderr)
            continue

        out[kind] = []
        for it in items:
            if it.get("isDraft"):
                continue
            updated = it.get("updatedAt") or it.get("createdAt")
            try:
                dt_updated = dt.datetime.fromisoformat(updated.replace("Z", "+00:00")) if updated else None
            except (ValueError, AttributeError):
                dt_updated = None
            if dt_updated and dt_updated > threshold:
                continue
            repo_obj = it.get("repository") or {}
            repo = repo_obj.get("nameWithOwner") or repo_obj.get("name") or "?"
            num = it.get("number")
            # Fail closed: a missing/null author is never treated as trusted.
            author_obj = it.get("author") or {}
            author_login = author_obj.get("login") if isinstance(author_obj, dict) else None
            out[kind].append({
                "external_id": f"{repo}#{num}",
                "title": it.get("title", "?"),
                "url": it.get("url", ""),
                "payload": {"repo": repo, "updatedAt": updated, "author": author_login},
            })
    return out


def poll_slack_messages(env: dict[str, str], channel_id: str, since_ts: str | None,
                        skip_uk_push: bool = False,
                        ) -> tuple[list[dict[str, Any]], str | None, bool]:
    """Third element is ``ok``: False when the fetch itself failed.

    A failure here is indistinguishable from "no new messages" at the call site, which is
    how a broken Slack token left #alerts and #updates unwatched for hours on 2026-09-07
    with every run still reporting success. The caller turns a sustained streak into a
    non-zero exit so the UptimeKuma heartbeat is withheld.
    """
    headers = {"Authorization": f"Bearer {env.get('HOMELAB_API_KEY', '')}"}
    data = http_get(f"{API_BASE}/slack/channels/{channel_id}/messages?limit=50", headers)
    if isinstance(data, dict) and "_error" in data:
        print(f"watchdog: slack poll failed for {channel_id}: {data['_error']}", file=sys.stderr)
        return [], since_ts, False
    msgs = data.get("messages", []) if isinstance(data, dict) else []
    out: list[dict[str, Any]] = []
    latest = since_ts
    for m in msgs:
        ts = m.get("ts", "")
        if not ts:
            continue
        # Always advance the cursor by raw ts, even if filtered out — avoids reprocessing.
        if not latest or ts > latest:
            latest = ts
        if since_ts and ts <= since_ts:
            continue
        text = (m.get("text") or "").strip()
        if not text:
            continue
        if "ist dem Channel beigetreten" in text or "added an integration" in text:
            continue
        if skip_uk_push and re.search(r"\[[^\]]+Push\]", text):
            continue
        out.append({
            "external_id": ts,
            "title": text[:240].replace("\n", " "),
            "url": "",
            "payload": {"text": text},
        })
    return out, latest, True


def poll_hermes_logs(conn: sqlite3.Connection, now: dt.datetime, now_iso: str) -> list[dict[str, Any]]:
    """Tail Hermes error logs since byte cursor. Group by error signature.

    Lines older than LOG_RECENT_HOURS are skipped — guards against history floods
    on first run and after log rotation.
    """
    recency_cutoff = now - dt.timedelta(hours=LOG_RECENT_HOURS)
    # Logs use Europe/Berlin local timestamps without tz; treat them as such.
    log_tz = ZoneInfo("Europe/Berlin")
    sigs: dict[str, dict[str, Any]] = {}
    for cur_key, rel_path in HERMES_LOG_FILES:
        path = HERMES_HOME / rel_path
        if not path.exists():
            continue
        cursor = cursor_get(conn, cur_key)
        try:
            last_offset = int(cursor) if cursor else 0
        except ValueError:
            last_offset = 0
        size = path.stat().st_size
        if last_offset > size:
            last_offset = 0  # log was rotated/truncated
        # On first run (cursor=None), process the entire file so existing
        # error signatures surface. Aggregation + cooldown collapse repeats.
        if last_offset >= size:
            continue
        try:
            with path.open(encoding="utf-8", errors="replace") as f:
                f.seek(last_offset)
                new_text = f.read()
        except OSError:
            continue
        cursor_set(conn, cur_key, str(size), now_iso)
        for line in new_text.splitlines():
            if not LOG_LEVEL_RE.search(line):
                continue
            # A deliberate fallback probe's 404 is the probe's expected result, not a
            # fault (the fallback then serves the turn — see PROBE_SENTINEL_RE). Hermes
            # retries `api_max_retries` times, so one probe writes exactly three ERROR
            # lines and lands on minOccurrences: a card per probe run. Filtered here
            # rather than in triage-policy.json's `ignore`, which keys on external_id —
            # the signature is truncated before the model id, so an ignore entry cannot
            # separate the sentinel from a genuine 404 for the real brain model.
            if PROBE_SENTINEL_RE.search(line):
                continue
            ts_m = LOG_TS_RE.match(line)
            if ts_m:
                try:
                    line_dt = dt.datetime.strptime(ts_m.group(1), "%Y-%m-%d %H:%M:%S").replace(tzinfo=log_tz)
                    if line_dt < recency_cutoff:
                        continue
                except ValueError:
                    pass
            m = LOG_PARSE_RE.match(line)
            if m:
                level, module, msg = m.group(1), m.group(2), m.group(3)
                # Drop Hermes's structured-metadata tail (" | provider=… tokens=~6,455"),
                # whose per-line counters would otherwise split one recurring error into
                # a fresh signature every poll (the cron "API call failed" flood).
                msg = msg.split(" | ", 1)[0].rstrip()
                sig_text = f"{module}: {msg[:120]}"
            else:
                sig_text = line[:160]
            key = normalize_title(sig_text)
            if not key:
                continue
            g = sigs.setdefault(key, {
                "external_id": key,
                "title": sig_text,
                "url": "",
                "payload": {"first_line": line[:500]},
                "count": 0,
            })
            g["count"] += 1
    return list(sigs.values())


def poll_hermes_cron() -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    try:
        data = json.loads(JOBS_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return out
    for j in data.get("jobs", []):
        if not j.get("enabled", True):
            continue
        status = j.get("last_status")
        if status and status != "ok":
            out.append({
                "external_id": j["id"],
                "title": f"Cron '{j.get('name', j['id'])}' status={status}",
                "url": "",
                "payload": {
                    "error": j.get("last_error"),
                    "last_run_at": j.get("last_run_at"),
                    "next_run_at": j.get("next_run_at"),
                },
            })
    return out


def poll_op_refs(host: str, remote_cmd: str) -> tuple[list[dict[str, Any]], bool]:
    """Probe one host's shared .env.tpl via a no-op `op run -- true` over ssh.

    Returns (observed_events, reachable). `reachable=False` means the probe itself
    didn't complete (ssh timeout, connection failure, or a spawn error) — a network
    condition, not a verdict about the refs. The caller must skip reconciling that
    host's state on a cycle where this is False, so a blip can neither page as a
    missing secret nor silently auto-resolve a real dangling one.
    """
    try:
        r = subprocess.run(
            ["ssh", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", host, remote_cmd],
            capture_output=True, text=True, timeout=OP_REF_SSH_TIMEOUT,
        )
    except (subprocess.TimeoutExpired, OSError, subprocess.SubprocessError):
        return [], False
    if r.returncode == 0:
        return [], True
    # ssh exits 255 when it can't reach/authenticate to the host at all (as opposed
    # to the remote command itself failing) — that's connectivity, not a dangling
    # ref. The remote command is a bare `-- true`, so a non-255 non-zero code can
    # only come from `op run` itself refusing to resolve the template.
    if r.returncode == 255:
        return [], False

    out = (r.stderr or "") + (r.stdout or "")
    items = sorted(set(OP_RUN_MISSING_ITEM_RE.findall(out)) | set(OP_RUN_MISSING_UUID_RE.findall(out)))
    if not items:
        # op failed for a reason we can't name a specific item for — degrade to
        # host + the raw error's first line, never to a bare boolean.
        first_line = next((ln.strip() for ln in out.splitlines() if ln.strip()), "op run failed (no output)")
        # The dedup key strips timestamps (see OP_REFS_TIMESTAMP_RE) so the
        # SAME underlying error mints the SAME external_id every poll — the
        # displayed title below deliberately keeps the raw, timestamped
        # first_line, only the dedup key is normalized differently.
        key = normalize_title(_strip_op_refs_timestamps(first_line))[:80] or "unknown"
        return [{
            "external_id": f"raw:{key}",
            "title": f"1Password refs unresolved on {host} (.env.tpl) — {first_line[:160]}",
            "url": "",
            "payload": {"host": host, "raw_error": first_line[:500]},
        }], True

    return [
        {
            "external_id": item,
            "title": f"1Password item '{item}' unresolved on {host} — op run blocks every cron sharing this template",
            "url": "",
            "payload": {"host": host, "item": item},
        }
        for item in items
    ], True


def _load_external_skill_dirs() -> list[Path]:
    """Parse `skills.external_dirs` out of config.yaml with a line-scan.

    The file has exactly one `external_dirs:` key, a plain YAML list of
    `- ~/path` entries — simple enough that pulling in a YAML dependency isn't
    worth it, matching this script's stdlib-only parsing elsewhere.
    """
    try:
        text = CONFIG_PATH.read_text()
    except OSError:
        return []
    dirs: list[Path] = []
    in_block = False
    for line in text.splitlines():
        if re.match(r"^\s*external_dirs:\s*$", line):
            in_block = True
            continue
        if not in_block:
            continue
        m = re.match(r"^\s*-\s*(.+?)\s*$", line)
        if not m:
            break  # first non-list-item line ends the block
        dirs.append(Path(m.group(1)).expanduser())
    resolved: list[Path] = []
    for d in dirs:
        try:
            resolved.append(d.resolve())
        except OSError:
            continue
    return resolved


def _load_bundled_manifest_names() -> set[str]:
    """Skill names synced from the bundled repo (`name:origin_hash` per line)."""
    try:
        lines = BUNDLED_MANIFEST_PATH.read_text().splitlines()
    except OSError:
        return set()
    return {line.split(":", 1)[0].strip() for line in lines if line.strip()}


def _load_skill_usage() -> dict[str, Any]:
    try:
        data = json.loads(SKILL_USAGE_PATH.read_text())
    except (OSError, json.JSONDecodeError):
        return {}
    return data if isinstance(data, dict) else {}


def poll_stray_skills() -> list[dict[str, Any]]:
    """Detect skill dirs the background self-improvement pass created directly
    under ~/.hermes/skills/, with no review path (see CLAUDE.md's external_dirs
    paragraph — a skill symlinked from this repo is protected from the curator's
    write guard; one the agent creates ad hoc under ~/.hermes/skills/ is not).

    Scans two levels deep: a top-level skill dir (`<name>/SKILL.md`) or one
    nested inside a bundled category dir (`<category>/<name>/SKILL.md` — real
    layout for bundled/hub skills, and where the worst incident this closes
    (`homelab-alerts`, 89 silent rewrites) was actually found, under `devops/`).
    Depth is hard-bounded to those two levels — no further recursion.

    A candidate is a stray only if ALL of:
      - has its own SKILL.md
      - is not a symlink (repo-symlinked skills are the protected, tracked ones)
      - does not resolve under skills.external_dirs
      - its name is absent from .bundled_manifest (the synced-bundled registry)
      - .usage.json shows it was locally MUTATED: created_by == "agent", or
        patch_count >= 1

    That last condition is load-bearing, not redundant with the manifest check.
    The live tree carries 19 dirs that are absent from .bundled_manifest by name
    yet entirely legitimate — hub-installed, or seeded by an early
    pre-manifest-tracking sync. Matching on the manifest alone flags all of
    them. But 18 of the 19 have never been touched, so "has been locally
    mutated" separates the two cleanly: exactly one candidate on the current
    tree, against six real strays it would have caught.

    Mutation is the right signal rather than authorship. `created_by == "agent"`
    alone (skill_manager_tool.py sets it only inside the background curator
    fork) catches the worst class — `homelab-alerts`, 89 silent rewrites — but
    missed four of the six cleaned up on 2026-08-02: the curator's consolidations
    of upstream skills carried created_by=None with 1-5 patches each. Those are
    the same problem, a divergent local copy `hermes update` will never refresh,
    and the patch count is what exposes them.
    """
    try:
        if not SKILLS_DIR.is_dir():
            return []
        external_dirs = _load_external_skill_dirs()
        manifest_names = _load_bundled_manifest_names()
        usage = _load_skill_usage()

        def _is_external(path: Path) -> bool:
            try:
                real = path.resolve()
            except OSError:
                return False
            return any(real == ext or real.is_relative_to(ext) for ext in external_dirs)

        candidates: list[Path] = []
        for top in SKILLS_DIR.iterdir():
            if not top.is_dir() or top.name.startswith("."):
                continue
            candidates.append(top)
            if top.is_symlink():
                continue  # a symlinked skill dir has no nested skills of its own
            for nested in top.iterdir():
                if nested.is_dir() and not nested.name.startswith("."):
                    candidates.append(nested)

        out: list[dict[str, Any]] = []
        for cand in candidates:
            if not (cand / "SKILL.md").is_file():
                continue
            if cand.is_symlink() or _is_external(cand):
                continue
            name = cand.name
            if name in manifest_names:
                continue
            rec = usage.get(name) or {}
            created_by = rec.get("created_by")
            patch_count = rec.get("patch_count") or 0
            if created_by != "agent" and patch_count < 1:
                continue
            relpath = cand.relative_to(SKILLS_DIR).as_posix()
            origin = "agent-created" if created_by == "agent" else "locally patched"
            out.append({
                "external_id": name,
                "title": (
                    f"Stray skill '{name}' at {relpath} — {origin}, "
                    f"{patch_count} patches, not under skills.external_dirs"
                ),
                "url": "",
                "payload": {"path": relpath, "patch_count": patch_count, "created_by": created_by},
            })
        return out
    except Exception as e:  # filesystem/config parsing must never take the poll down
        print(f"watchdog: stray_skill probe raised: {e}", file=sys.stderr)
        return []


def reconcile(conn: sqlite3.Connection, source: str, observed: list[dict[str, Any]],
              now: dt.datetime, gate_min: int, reminder_h: int | None,
              track_resolution: bool = True,
              deliver: bool = True) -> tuple[list[dict], list[dict], list[dict]]:
    """Fold `observed` into the events table and return (new, reminders, resolved).

    `deliver=False` means the caller already knows the digest will be suppressed
    (quiet hours or vacation). Rows are still inserted, refreshed and resolved —
    only the notification BOOKKEEPING is withheld, so nothing is marked notified
    that nobody was told about.

    WHY THAT MATTERS. It used to poll, stamp `notified_at`/`last_reminder_at`, and
    then have compose_slack_body() return "" — burning the notification. With a
    cooldown at or near 24h that is not a delayed message, it is a permanently
    silent one: the next eligible emit lands at the same wall-clock hour, i.e. back
    inside the same quiet window, forever. Observed 2026-08-24: a Slack socket died
    at 03:44, its `hermes_log` signature (24h cooldown) fired into the quiet window,
    and the resulting 48-hour / ~17,300-line reconnect flood never reached the
    digest once. The gap was found by hand two days later.
    """
    cur = conn.cursor()
    obs_ids = {o["external_id"] for o in observed}

    for o in observed:
        row = cur.execute(
            "SELECT * FROM events WHERE source=? AND external_id=?",
            (source, o["external_id"]),
        ).fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO events(source, external_id, title, url, payload_json, first_seen) "
                "VALUES(?,?,?,?,?,?)",
                (source, o["external_id"], o["title"], o.get("url", ""),
                 json.dumps(o.get("payload", {})), now.isoformat()),
            )
        elif row["resolved_at"] is not None:
            cur.execute(
                "UPDATE events SET resolved_at=NULL, first_seen=?, notified_at=NULL, "
                "last_reminder_at=NULL, reminder_count=0, title=?, url=?, payload_json=? WHERE id=?",
                (now.isoformat(), o["title"], o.get("url", ""),
                 json.dumps(o.get("payload", {})), row["id"]),
            )
        else:
            cur.execute(
                "UPDATE events SET title=?, url=?, payload_json=? WHERE id=?",
                (o["title"], o.get("url", ""), json.dumps(o.get("payload", {})), row["id"]),
            )

    new_events: list[dict] = []
    reminder_events: list[dict] = []
    for row in cur.execute(
        "SELECT * FROM events WHERE source=? AND resolved_at IS NULL", (source,),
    ).fetchall():
        first_seen = dt.datetime.fromisoformat(row["first_seen"])
        age_min = (now - first_seen).total_seconds() / 60
        if age_min < gate_min:
            continue
        if row["notified_at"] is None:
            if not deliver:
                continue  # stays un-notified; fires as NEW on the first delivering poll
            new_events.append(dict(row))
            cur.execute("UPDATE events SET notified_at=? WHERE id=?", (now.isoformat(), row["id"]))
        elif reminder_h:
            # Ledger projection (docs/history/state-log.md §49): the reminder path used to
            # anchor purely on events/REM_HOURS and never look at the ledger's
            # own classification of the event. A triage_items row in a
            # TERMINAL_STATES state (ignored/note included) is a settled
            # verdict — remind on it is pure noise, and skipping here doesn't
            # even bump the anchor, so a later reopen (triage.py's own
            # reopen/unsnooze pass) is re-checked cheaply next poll instead of
            # going quiet for a full reminder_h window. item is None (no
            # table, no row) behaves exactly as before this projection
            # existed. See _triage_item.
            item = _triage_item(conn, row["id"])
            if item is not None and item["state"] in _ledger.TERMINAL_STATES:
                continue
            effective_reminder_h = reminder_h
            if item is not None and item["state"] == _ledger.STATE_NEEDS_HUMAN:
                # A real outstanding decision, not an ongoing outage — its own
                # cadence, REM_HOURS_NEEDS_HUMAN, regardless of the source's.
                effective_reminder_h = REM_HOURS_NEEDS_HUMAN
            anchor = row["last_reminder_at"] or row["notified_at"]
            if (now - dt.datetime.fromisoformat(anchor)).total_seconds() >= effective_reminder_h * 3600:
                # Dispatch-bridge projection (Phase 3): an event with an OPEN
                # dispatch already has an investigation in flight, so
                # re-reminding is noise — skip silently (don't bump the
                # anchor either, so this just re-checks cheaply next poll
                # instead of going quiet for a full reminder_h window). An
                # event whose dispatch has CLOSED gets the outcome folded into
                # the reminder instead of a bare "reminder #N" — see
                # _dispatch_summary. dispatch is None (no dispatch_id, no
                # table, row gone) behaves exactly as before this projection
                # existed.
                dispatch = _dispatch_status(conn, row["dispatch_id"])
                if dispatch is not None and dispatch["reported_at"] is None:
                    continue
                if not deliver:
                    continue  # anchor untouched — re-checked cheaply next poll
                d = dict(row)
                if dispatch is not None:
                    d["dispatch_summary"] = _dispatch_summary(dispatch["status"], dispatch["verdict_json"])
                elif item is not None and item["state"] == _ledger.STATE_NEEDS_HUMAN and item["note"]:
                    # No dispatch outcome to show yet — fold in the triage
                    # verdict itself, the way dispatch_summary is folded in
                    # above, so the reminder carries the actual pending
                    # decision instead of a bare "reminder #N".
                    d["triage_note"] = item["note"]
                reminder_events.append(d)
                cur.execute(
                    "UPDATE events SET last_reminder_at=?, reminder_count=reminder_count+1 WHERE id=?",
                    (now.isoformat(), row["id"]),
                )

    resolved_events: list[dict] = []
    if track_resolution:
        for row in cur.execute(
            "SELECT * FROM events WHERE source=? AND resolved_at IS NULL", (source,),
        ).fetchall():
            if row["external_id"] not in obs_ids:
                cur.execute(
                    "UPDATE events SET resolved_at=? WHERE id=?", (now.isoformat(), row["id"]),
                )
                if row["notified_at"]:
                    d = dict(row)
                    d["resolved_at"] = now.isoformat()
                    resolved_events.append(d)

    return new_events, reminder_events, resolved_events


_DEDUP_NORMALIZE = re.compile(r"[^a-z0-9]+")


def normalize_title(text: str) -> str:
    return _DEDUP_NORMALIZE.sub("-", text.lower()).strip("-")[:120]


def aggregate_slack_batch(msgs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group raw slack messages by normalized title. Returns list of grouped dicts."""
    groups: dict[str, dict[str, Any]] = {}
    for m in msgs:
        key = normalize_title(m["title"])
        if not key:
            continue
        g = groups.setdefault(key, {
            "external_id": key,
            "title": m["title"],
            "url": "",
            "payload": {"first_text": m["title"], "ts_first": m["external_id"], "ts_last": m["external_id"]},
            "count": 0,
        })
        g["count"] += 1
        ts = m["external_id"]
        if ts > g["payload"]["ts_last"]:
            g["payload"]["ts_last"] = ts
        if ts < g["payload"]["ts_first"]:
            g["payload"]["ts_first"] = ts
    return list(groups.values())


def upsert_grouped(conn: sqlite3.Connection, source: str, groups: list[dict[str, Any]],
                   now: dt.datetime, flap_threshold: int = SLACK_FLAP_THRESHOLD,
                   cooldown_hours: int = SLACK_REEMIT_COOLDOWN_HOURS,
                   deliver: bool = True) -> list[dict]:
    """Upsert by dedup key with batch flap threshold + re-emit cooldown.

    A group surfaces only if (a) count in this batch >= flap_threshold, AND
    (b) we haven't surfaced this dedup key within the cooldown window.
    Always records to the DB so cumulative trends are queryable later.

    `deliver=False` withholds the emission and its bookkeeping — see reconcile()
    for why stamping a notification nobody received is permanently, not
    temporarily, silencing.

    A signature that RECURS after sweep_stale_grouped() resolved it re-opens, the
    way reconcile() has always re-opened a state event. Without that the row stays
    resolved forever while last_reminder_at keeps ticking, so it is invisible to
    every reader that filters on `resolved_at IS NULL` — the morning briefing's
    open list among them. The Session-is-closed signature sat in exactly that
    state from 2026-06-23 until it was found by hand.
    """
    cur = conn.cursor()
    out: list[dict] = []
    for g in groups:
        row = cur.execute(
            "SELECT * FROM events WHERE source=? AND external_id=?",
            (source, g["external_id"]),
        ).fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO events(source, external_id, title, url, payload_json, first_seen) "
                "VALUES(?,?,?,?,?,?)",
                (source, g["external_id"], g["title"], "",
                 json.dumps({**g["payload"], "batch_count": g["count"]}), now.isoformat()),
            )
            row = cur.execute(
                "SELECT * FROM events WHERE source=? AND external_id=?",
                (source, g["external_id"]),
            ).fetchone()

        if row["resolved_at"] is not None:
            cur.execute(
                "UPDATE events SET resolved_at=NULL, first_seen=?, notified_at=NULL, "
                "last_reminder_at=NULL, reminder_count=0 WHERE id=?",
                (now.isoformat(), row["id"]),
            )
            row = cur.execute(
                "SELECT * FROM events WHERE source=? AND external_id=?",
                (source, g["external_id"]),
            ).fetchone()

        last_emit = row["last_reminder_at"] or row["notified_at"]
        cooldown_passed = True
        if last_emit:
            cooldown_passed = (now - dt.datetime.fromisoformat(last_emit)).total_seconds() >= cooldown_hours * 3600

        should_emit = deliver and g["count"] >= flap_threshold and cooldown_passed

        if should_emit:
            display_title = f"{g['title']} (×{g['count']} in batch)"
            if row["notified_at"] is None:
                cur.execute(
                    "UPDATE events SET notified_at=?, title=?, payload_json=? WHERE id=?",
                    (now.isoformat(), display_title,
                     json.dumps({**g["payload"], "batch_count": g["count"]}), row["id"]),
                )
            else:
                cur.execute(
                    "UPDATE events SET last_reminder_at=?, reminder_count=reminder_count+1, "
                    "title=?, payload_json=? WHERE id=?",
                    (now.isoformat(), display_title,
                     json.dumps({**g["payload"], "batch_count": g["count"]}), row["id"]),
                )
            out.append({
                "source": source,
                "external_id": g["external_id"],
                "title": display_title,
                "url": "",
                "reminder_count": (row["reminder_count"] or 0) + (1 if row["notified_at"] else 0),
            })
        else:
            # Silent record — bump cumulative count but don't surface
            payload = json.loads(row["payload_json"] or "{}")
            payload["batch_count_last"] = g["count"]
            if "ts_last" in g["payload"]:
                payload["ts_last"] = g["payload"]["ts_last"]
            cur.execute(
                "UPDATE events SET payload_json=? WHERE id=?",
                (json.dumps(payload), row["id"]),
            )
    return out


def sweep_stale_grouped(conn: sqlite3.Connection, now: dt.datetime,
                        ttl_days: int = GROUPED_TTL_DAYS) -> int:
    """Silently resolve open grouped-source events idle for > ttl_days.

    Resolution is silent (no 'Resolved' emission) — sweeping months-old slack/log
    rows shouldn't trigger a notification burst; it's housekeeping, not an event.
    Idle anchor is the last activity: last_reminder_at → notified_at → first_seen.
    Returns the number of rows resolved.
    """
    cutoff = (now - dt.timedelta(days=ttl_days)).isoformat()
    placeholders = ",".join("?" for _ in GROUPED_SOURCES)
    cur = conn.execute(
        f"UPDATE events SET resolved_at=? "
        f"WHERE resolved_at IS NULL AND source IN ({placeholders}) "
        f"AND COALESCE(last_reminder_at, notified_at, first_seen) < ?",
        (now.isoformat(), *GROUPED_SOURCES, cutoff),
    )
    return cur.rowcount


def resolve_stale_github_issue_events(conn: sqlite3.Connection, now: dt.datetime) -> int:
    """One-time cleanup (docs/waves/PLAN.md Wave 1): `poll_github()` no
    longer polls issues at all, so every still-open `github_issue` event
    this poller wrote before that change would otherwise sit open forever —
    nothing left reconciles it, since `_run_poll()`'s own `for kind in
    (...)` loop dropped `github_issue`. `triage.py`'s no-label issue intake
    (`ingest_github_issues()`, event source `github_go` — deliberately a
    DIFFERENT source, see `open_origin_item()`'s own docstring) now assesses
    every open issue directly with a real verdict, which supersedes this
    digest's age-gated "still open" line entirely.

    Same resolve shape `ingest_github_issues()` already uses for a
    disappeared `github_go` event: `resolved_at=now`, no note column
    on `events` to carry one — the explanation lives here, and in the one
    stderr line this prints when it actually resolves something. Runs on
    every poll (idempotent: once every open row is resolved, the UPDATE
    matches zero rows and this is silent), not gated behind a cursor,
    because unlike `sweep_stale_grouped()` this isn't a recurring TTL sweep —
    it is a single migration-shaped fact that becomes a permanent no-op the
    moment it has run once."""
    cur = conn.execute(
        "UPDATE events SET resolved_at=? WHERE source='github_issue' AND resolved_at IS NULL",
        (now.isoformat(),),
    )
    if cur.rowcount:
        print(
            f"watchdog: resolving {cur.rowcount} stale github_issue digest event(s) — issue items "
            "now supersede this digest, see docs/waves/PLAN.md Wave 1",
            file=sys.stderr,
        )
    return cur.rowcount


SOURCE_EMOJI = {
    "uk": ":satellite_antenna:",
    "docker_homelab": ":whale:",
    "docker_vps": ":whale:",
    "github_pr": ":cat:",
    "slack_alert": ":mega:",
    "slack_update": ":package:",
    "hermes_cron": ":robot_face:",
    "hermes_log": ":bug:",
    "op_refs_homelab": ":key:",
    "op_refs_vps": ":key:",
    "stray_skill": ":ghost:",
}

SECTION_CAP = 8


def _github_author(item: dict[str, Any]) -> str | None:
    """Read the author login stashed in payload_json by poll_github. Rows
    written before this key existed (or a corrupt/unparseable payload) return
    None — same fail-closed treatment as a missing author, i.e. third-party."""
    try:
        payload = json.loads(item.get("payload_json") or "{}")
    except (json.JSONDecodeError, TypeError):
        return None
    if not isinstance(payload, dict):
        return None
    return payload.get("author")


def _render_bullet(item: dict[str, Any], kind: str, now: dt.datetime) -> str:
    """kind ∈ {'new', 'reminder', 'resolved'}. Returns a single Slack-mrkdwn line, no leading dash."""
    src = item.get("source", "?")
    emoji = SOURCE_EMOJI.get(src, ":grey_question:")
    title = (item.get("title") or "?").strip()
    url = (item.get("url") or "").strip()

    # source-specific body
    if src == "github_pr":
        ext = (item.get("external_id") or "").strip()
        # Strip owner from "owner/repo#N" — display as "repo#N".
        if "/" in ext:
            ext = ext.split("/", 1)[1]
        body = f"{emoji} {title}"
        suffix_bits = []
        if ext:
            suffix_bits.append(f"`{ext}`")
        if url:
            suffix_bits.append(f"<{url}>")
        if suffix_bits:
            body += f" ({', '.join(suffix_bits)})"
        # gh search --owner filters by repo OWNER, never item AUTHOR, and every
        # repo is public — anyone can open a PR here. Mark non-self items
        # unmistakably so neither Hermes nor Johannes mistakes
        # attacker-controlled text for his own note (claude-dispatch can pick
        # a stale PR as a dispatch trigger, which would hand the attacker's
        # own body to a live Claude Code episode — prompt injection).
        author = _github_author(item)
        if author != TRUSTED_GH_LOGIN:
            who = author or "unknown"
            body = f":warning: THIRD-PARTY (@{who}) — untrusted, do not dispatch on this without Johannes\n{body}"
    elif src.startswith("docker_"):
        host = "homelab" if src == "docker_homelab" else "vps"
        # Docker titles end with " (homelab)" or " (vps)" — strip to avoid duplication
        # since we already render "on <host>".
        cleaned = re.sub(r"\s*\((?:homelab|vps)\)\s*$", "", title)
        body = f"{emoji} {cleaned} on {host}"
    else:
        body = f"{emoji} {title}"
        if url:
            body += f" (<{url}>)"

    # kind-specific suffixes
    if kind == "reminder":
        dispatch_summary = item.get("dispatch_summary")
        triage_note = item.get("triage_note")
        if dispatch_summary:
            # Dispatch-bridge projection: the investigation closed, so report
            # its outcome instead of a bare "reminder #N".
            body += f" — {dispatch_summary}"
        elif triage_note:
            # Ledger projection (docs/history/state-log.md §49): needs_human carries a real
            # verdict already — report it instead of a bare "reminder #N".
            body += f" — needs human: {triage_note}"
        else:
            rc = item.get("reminder_count") or 0
            if rc:
                body += f" — reminder #{rc}"

    return body


def _render_section(label: str, header_emoji: str, items: list[dict[str, Any]], kind: str,
                    now: dt.datetime) -> list[str]:
    if not items:
        return []
    lines = [f"{header_emoji} **{label}**"]
    capped = items[:SECTION_CAP]
    overflow = len(items) - len(capped)
    for it in capped:
        lines.append(f"- {_render_bullet(it, kind, now)}")
    if overflow > 0:
        lines.append(f"- … and {overflow} more")
    return lines


def compose_slack_body(
    new: list[dict[str, Any]],
    reminders: list[dict[str, Any]],
    resolved: list[dict[str, Any]],
    *,
    quiet: bool,
    vacation: bool,
    now: dt.datetime,
) -> str:
    """Returns the Slack mrkdwn message body, or empty string for suppression.

    Under `--post` (the `com.jkrumm.warden-poll` LaunchAgent's production
    path), an empty return means no Slack call is made at all — silent by
    design, not an error.
    """
    if quiet or vacation:
        return ""
    if not new and not reminders and not resolved:
        return ""

    sections: list[list[str]] = []
    if new:
        sections.append(_render_section("New", ":rotating_light:", new, "new", now))
    if reminders:
        sections.append(_render_section("Reminders", ":bell:", reminders, "reminder", now))
    if resolved:
        sections.append(_render_section("Resolved", ":white_check_mark:", resolved, "resolved", now))

    return "\n\n".join("\n".join(sec) for sec in sections)


def fmt_block(label: str, items: list[dict[str, Any]]) -> str:
    if not items:
        return f"{label}=[]"
    lines = [f"{label}=["]
    for it in items:
        src = it.get("source", "?")
        title = it.get("title", "?")
        url = it.get("url") or ""
        rc = it.get("reminder_count")
        dispatch_summary = it.get("dispatch_summary")
        suffix_parts = []
        if url:
            suffix_parts.append(url)
        if dispatch_summary:
            # Dispatch-bridge projection: surfaces in the raw block too, so the
            # cron prompt's LLM sees the closed investigation, not just a count.
            suffix_parts.append(f"dispatch: {dispatch_summary}")
        elif rc:
            suffix_parts.append(f"reminder#{rc}")
        suffix = f" ({', '.join(suffix_parts)})" if suffix_parts else ""
        lines.append(f"  - [{src}] {title}{suffix}")
    lines.append("]")
    return "\n".join(lines)


def _run_poll(conn: sqlite3.Connection, now: dt.datetime, env: dict[str, str],
              deliver: bool = True) -> tuple[list[dict], list[dict], list[dict]]:
    """Run the full polling pipeline against `conn`. Returns (new, reminders, resolved).

    `deliver=False` when the caller already knows the digest will be suppressed
    (quiet hours / vacation). The poll still runs in full — cursors advance, rows
    are inserted, refreshed and resolved — but nothing is stamped as notified, so
    the backlog fires on the first delivering poll instead of being consumed by a
    message that was never sent. See reconcile() for the failure this closes.

    THE INVARIANT, and it is load-bearing: **no network call may happen inside
    an open write transaction.** Every `conn.commit()` below is a commit point
    placed immediately after a source's writes and immediately before the next
    source's probe — not tidiness, and not durability.

    Python's sqlite3 opens a DEFERRED write transaction on the first INSERT or
    UPDATE and holds it until commit. This function used to have none of its
    own: `main()` committed once, at the very end. So from `reconcile()`'s first
    write until that commit, this poller held the ledger's write lock across
    every probe still to come — two `ssh` round trips for docker, one per
    op-refs host, a `gh` subprocess with a 30s timeout, and two Slack HTTP
    fetches. Minutes, on a bad day.

    On 2026-09-09 at 13:05Z the act-loop died with `sqlite3.OperationalError:
    database is locked` on the first INSERT of its own pass. `ledger.py` sets
    `busy_timeout=5000`; against a lock held for the length of a `gh` call it
    never stood a chance. The loop is the process whose job is noticing that
    things have stopped, and the ingest poller stopped it.

    Committing per source shrinks the window from "the whole poll" to "one
    source's rows". The trade, stated because it IS a trade: a crash mid-poll
    now leaves earlier sources committed instead of rolling the whole pass
    back. That is the right way round — every `reconcile()` re-derives its
    rows from `observed` on the next pass, so a partial poll is self-healing,
    while a deadlocked act-loop is not.
    """
    now_iso = now.isoformat()
    all_new: list[dict] = []
    all_rem: list[dict] = []
    all_res: list[dict] = []

    n, r, res = reconcile(conn, "uk", poll_uk(env), now, UK_DOWN_GATE_MIN, REM_HOURS["uk"], deliver=deliver)
    all_new += n; all_rem += r; all_res += res
    conn.commit()

    for host in ("homelab", "vps"):
        src = f"docker_{host}"
        n, r, res = reconcile(conn, src, poll_docker(env, host), now,
                              DOCKER_UNHEALTHY_GATE_MIN, REM_HOURS[src], deliver=deliver)
        all_new += n; all_rem += r; all_res += res
        conn.commit()

    for host, remote_cmd in OP_REF_HOSTS.items():
        src = f"op_refs_{host}"
        try:
            observed, reachable = poll_op_refs(host, remote_cmd)
        except Exception as e:  # probe must never take the rest of the poll down
            print(f"watchdog: op_refs probe for {host} raised: {e}", file=sys.stderr)
            continue
        if not reachable:
            # Network blip / unreachable host this cycle — skip reconciling so it
            # can neither page as a missing secret nor auto-resolve a real one.
            continue
        n, r, res = reconcile(conn, src, observed, now, 0, REM_HOURS[src], deliver=deliver)
        all_new += n; all_rem += r; all_res += res
        conn.commit()

    resolve_stale_github_issue_events(conn, now)
    conn.commit()

    gh = poll_github(env)
    for kind in ("github_pr",):
        if gh[kind] is None:
            # Search failed this cycle — skip reconciling, same as an
            # unreachable op_refs host: a blind poll must not resolve anything.
            continue
        n, r, res = reconcile(conn, kind, gh[kind], now, 0, REM_HOURS[kind], deliver=deliver)
        all_new += n; all_rem += r; all_res += res
        conn.commit()

    n, r, res = reconcile(conn, "hermes_cron", poll_hermes_cron(), now, 0, REM_HOURS["hermes_cron"], deliver=deliver)
    all_new += n; all_rem += r; all_res += res
    conn.commit()

    n, r, res = reconcile(conn, "stray_skill", poll_stray_skills(), now, 0, REM_HOURS["stray_skill"], deliver=deliver)
    all_new += n; all_rem += r; all_res += res
    conn.commit()

    log_groups = poll_hermes_logs(conn, now, now_iso)
    if log_groups:
        all_new += upsert_grouped(conn, "hermes_log", log_groups, now,
                                  flap_threshold=1, cooldown_hours=REM_HOURS["hermes_log"],
                                  deliver=deliver)
    conn.commit()

    for cur_key, ch_id, src, skip_uk in [
        ("slack_alert_ts", CH_ALERTS, "slack_alert", True),
        ("slack_update_ts", CH_UPDATES, "slack_update", False),
    ]:
        since = cursor_get(conn, cur_key)
        # Immediately before the probe, NOT at the tail of this body: both
        # `continue`s below jump straight to the next iteration, so a commit
        # placed at the end is skipped by exactly the paths that just wrote a
        # fail-streak cursor. tests/test_watchdog_locking.py caught this.
        conn.commit()
        msgs, latest, ok = poll_slack_messages(env, ch_id, since, skip_uk_push=skip_uk)
        streak_key = f"{src}_fail_streak"
        if ok:
            cursor_set(conn, streak_key, "0", now_iso)
        else:
            cursor_set(conn, streak_key, str(cursor_get_int(conn, streak_key) + 1), now_iso)
            continue
        if since is None:
            if latest:
                cursor_set(conn, cur_key, latest, now_iso)
            continue
        if msgs:
            groups = aggregate_slack_batch(msgs)
            all_new += upsert_grouped(conn, src, groups, now, deliver=deliver)
        if latest and latest != since:
            cursor_set(conn, cur_key, latest, now_iso)
        conn.commit()

    sweep_stale_grouped(conn, now)

    return all_new, all_rem, all_res


def slack_poll_failure(conn: sqlite3.Connection) -> str | None:
    """Message describing any Slack source blind for SLACK_FAIL_STREAK_ALERT runs, else None."""
    blind = []
    for src in ("slack_alert", "slack_update"):
        streak = cursor_get_int(conn, f"{src}_fail_streak")
        if streak >= SLACK_FAIL_STREAK_ALERT:
            blind.append(f"{src} ({streak} consecutive failures)")
    if not blind:
        return None
    return "watchdog: Slack polling is blind: " + ", ".join(blind)


def _push_uptime_heartbeat() -> None:
    """Self-health heartbeat, inherited from hermes-agent's watchdog-slack.py
    wrapper (the cron entry-point this replaces) — ping the UptimeKuma push
    monitor, best-effort, exceptions swallowed. Callers fire this only on a
    clean run (rc == 0), so a crash, hang, or non-zero exit trips the
    "Watchdog last successful run" Kuma alert. This is the only thing that
    notices the ingest poller itself has died.
    """
    push_url = resolve_secret("UPTIME_PUSH_WATCHDOG")
    if not push_url:
        return
    # uptime.jkrumm.com sits behind Cloudflare, which 403s the default
    # Python-urllib User-Agent — send a curl-like UA so the heartbeat lands.
    req = urllib.request.Request(push_url, headers={"User-Agent": "curl/8.7.1"})
    try:
        with urllib.request.urlopen(req, timeout=10):
            pass
    except Exception:
        pass


# --- Heartbeat ----------------------------------------------------------------
#
# Mirrors triage.py's record_heartbeat() exactly — same key shape, same
# "unconditional row per COMPLETED pass, skipped under --dry-run" contract, for
# the same reason: every other write in this file only happens when something
# CHANGED, so an idle pass (nothing new, nothing to remind, nothing resolved)
# leaves no trace at all, and "the poller ran and found nothing" becomes
# indistinguishable from "the poller did not run." This is what /metrics'
# per-poller age (warden/scripts/api.py) reads to answer that question for
# this poller specifically, by name, rather than only "something is blind."
HEARTBEAT_CURSOR_KEY = "watchdog_poll_last_run"


def record_heartbeat(conn: sqlite3.Connection, *,
                     new_count: int, reminder_count: int, resolved_count: int,
                     slack_blind: bool) -> None:
    # No timestamp argument — see triage.py's record_heartbeat() for why the
    # write clock is read here rather than accepted from a caller holding the
    # pass's START time.
    value = json.dumps({
        "new": new_count, "reminders": reminder_count, "resolved": resolved_count,
        "slack_blind": slack_blind,
    }, sort_keys=True)
    conn.execute(
        "INSERT INTO cursors(key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (HEARTBEAT_CURSOR_KEY, value, dt.datetime.now(dt.timezone.utc).isoformat()),
    )
    conn.commit()


def main(argv: list[str] | None = None) -> int:
    args = set(argv if argv is not None else sys.argv[1:])
    post = "--post" in args
    # --post implies --slack-body: it composes the same digest, it just
    # delivers it itself instead of printing it for a caller to deliver.
    emit_slack_body = "--slack-body" in args or post
    dry_run = "--dry-run" in args

    env = load_env()
    state = load_state()
    quiet = in_quiet_hours(state)
    vacation = vacation_active(state)
    # compose_slack_body() suppresses on either, so tell the poll up front rather
    # than letting it stamp notifications for a message that will never be sent.
    # --slack-body is the production path; the block-printing path below always
    # delivers to its caller, so it stays deliver=True.
    deliver = not (quiet or vacation) if emit_slack_body else True
    now = dt.datetime.now(dt.timezone.utc)
    now_iso = now.isoformat()

    if dry_run:
        # Run poll against a temp copy of the DB so we don't advance notified_at /
        # last_reminder_at on operator-driven invocations.
        import shutil
        import tempfile
        global DB_PATH
        original_db = DB_PATH
        tmpdir = tempfile.mkdtemp(prefix="watchdog-dryrun-")
        tmp_db = Path(tmpdir) / "watchdog.db"
        if original_db.exists():
            shutil.copy2(original_db, tmp_db)
        DB_PATH = tmp_db
        try:
            conn = db_connect()
            all_new, all_rem, all_res = _run_poll(conn, now, env, deliver=deliver)
            conn.commit()
            slack_blind = slack_poll_failure(conn)
            conn.close()
        finally:
            DB_PATH = original_db
            shutil.rmtree(tmpdir, ignore_errors=True)
    else:
        try:
            conn = db_connect()
        except _ledger.LedgerBehind as e:
            print(
                f"watchdog: ledger behind this process's schema ({e}) — skipping this pass; "
                "the loop migrates at its next tick",
                file=sys.stderr,
            )
            if post and not dry_run:
                _push_uptime_heartbeat()
            return 0
        all_new, all_rem, all_res = _run_poll(conn, now, env, deliver=deliver)
        conn.commit()
        slack_blind = slack_poll_failure(conn)
        record_heartbeat(conn, new_count=len(all_new), reminder_count=len(all_rem),
                         resolved_count=len(all_res), slack_blind=bool(slack_blind))
        conn.close()

    # stderr, never stdout: --post delivers the digest itself via chat.postMessage,
    # so stdout carries no message for anything to consume.
    if slack_blind:
        print(slack_blind, file=sys.stderr)

    if emit_slack_body:
        body = compose_slack_body(all_new, all_rem, all_res,
                                  quiet=quiet, vacation=vacation, now=now)
        rc = 1 if slack_blind else 0

        if post:
            # --dry-run beats --post, always: compose as usual, make zero
            # Slack calls (and no heartbeat — that is a network call too),
            # print what would have been sent, and stop.
            if dry_run:
                print(f"[dry-run] would post {len(body)} chars to {WATCHDOG_CHANNEL}", file=sys.stderr)
                return rc
            # An empty body is the NORMAL case — quiet hours, vacation, or simply
            # nothing new — and compose_slack_body() already made that call, so
            # posting nothing is not an error. Note what this branch must NOT do:
            # skip the heartbeat. The monitor this feeds answers "is the poller
            # still running", not "did the poller have news"; treating a quiet poll
            # as a missed heartbeat would page on every silent half hour and train
            # the alert to be ignored — which is how an eleven-day blindness goes
            # unnoticed in the first place. The retiring wrapper pinged on rc == 0
            # regardless of whether a body was printed, and that is the behaviour.
            if body:
                token = resolve_alerts_read_token()
                if not token:
                    print("watchdog: no Slack token, cannot post digest", file=sys.stderr)
                    return 1
                ok, _ = post_text(WATCHDOG_CHANNEL, body, token)
                if not ok:
                    print("watchdog: slack post failed, digest not delivered", file=sys.stderr)
                    return 1
            if rc == 0:
                _push_uptime_heartbeat()
            return rc

        if body:
            print(body)
        return rc

    print(f"QUIET_HOURS={'true' if quiet else 'false'}")
    print(f"VACATION={'true' if vacation else 'false'}")
    print(f"NOW={now_iso}")
    print(fmt_block("NEW", all_new))
    print(fmt_block("REMINDERS", all_rem))
    print(fmt_block("RESOLVED", all_res))
    return 1 if slack_blind else 0


if __name__ == "__main__":
    sys.exit(main())
