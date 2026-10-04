"""Watchdog poll — runs every 1800 s as the `com.jkrumm.warden-poll` LaunchAgent.

Polls UptimeKuma, Docker (homelab + vps), GitHub, Slack #alerts/#updates,
Hermes self-state, 1Password ref health on homelab + vps (a no-op
`op run -- true` over the shared .env.tpl — detects a dangling ref directly,
the failure class behind the 2026-08-01 outage, instead of inferring it hours
later from silent heartbeats), and stray agent-created skills under
~/.hermes/skills/ (untracked, unreviewed — the failure class behind the
2026-08-02 skill-sprawl cleanup). Reconciles against ~/.hermes/watchdog.db
(SQLite). Posts nothing to Slack: the New/Resolved raw-event digest is gone (a
warden item reaches Slack only as `fixed`/`needs_decision`, triage.py's
notify_cluster(); everything else lives in Argo). What stays is the poller's own
health: the blind-Slack-poll alarm on stderr and the exit code, and the UptimeKuma
heartbeat on a clean run.

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

# scripts/ (this file's own directory) onto sys.path so `clients` is
# importable as a real package — the same reason triage.py and
# dispatch-sweep.py do this.
_SCRIPTS_DIR = Path(__file__).resolve().parent
if str(_SCRIPTS_DIR) not in sys.path:
    sys.path.insert(0, str(_SCRIPTS_DIR))

from clients import secrets as _secrets  # noqa: E402

JOBS_PATH = HERMES_HOME / "cron" / "jobs.json"
CONFIG_PATH = HERMES_HOME / "config.yaml"
SKILLS_DIR = HERMES_HOME / "skills"
BUNDLED_MANIFEST_PATH = SKILLS_DIR / ".bundled_manifest"
SKILL_USAGE_PATH = SKILLS_DIR / ".usage.json"

API_BASE = "https://argo.jkrumm.com/api"
GH_OWNER = "jkrumm"
CH_ALERTS = "C0AS1LAUQ3C"
CH_UPDATES = "C0ARZJD824W"

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

# Probes whether the shared .env.tpl on each host still fully resolves — the
# root cause of the 2026-08-01 outage: `op run --env-file=... -- <script>` (every
# homelab/vps cron's launcher) fails WHOLESALE the instant a single referenced
# 1Password item goes missing (e.g. deleted as part of a service retirement),
# taking every cron sharing that template down at once. This detects the cause
# directly instead of waiting hours for it to surface as silent heartbeats.
# Self-contained: no dependency on hermes-ops.sh's shape.
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
}


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


def http_get(url: str, headers: dict[str, str] | None = None, timeout: int = 15) -> Any:
    req = urllib.request.Request(url, headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError, TimeoutError) as e:
        return {"_error": str(e)}


def db_connect() -> sqlite3.Connection:
    """Assert-only: watchdog-poll.py is not the migrator (DESIGN.md § The
    ledger — migrations run only by the loop, triage.py, at boot). If this
    runs before the loop has ever touched a fresh ledger, ledger.connect()
    raises a clear schema-version error rather than this file inventing its
    own tables the way it used to."""
    return _ledger.connect(DB_PATH)


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
        # A shadow monitor left behind by the retired synthetic trip (§103) goes DOWN on
        # purpose; it carries no notification and must never become an event.
        if str(m.get("name") or "").startswith("warden-trip:"):
            continue
        status = m.get("status")
        is_down = (isinstance(status, str) and status.lower() == "down") or status == 0 or status is False
        if not is_down:
            continue
        mid = str(m.get("id") or m.get("monitorId") or m.get("name") or "")
        if not mid:
            continue
        payload: dict[str, Any] = {"type": m.get("type"), "status": status}
        # The monitor's own tags, when the endpoint carries them: triage's label routing
        # (lifecycle/intake.py route_by_label()) reads a tag naming a repo.
        if m.get("tags"):
            payload["tags"] = m["tags"]
        out.append({
            "external_id": mid,
            "title": m.get("name", "monitor") or "monitor",
            "url": m.get("url") or "",
            "payload": payload,
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
            key = fingerprint(sig_text)
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
        # The dedup key is the event fingerprint (timestamps, ids and numbers
        # stripped) so the SAME underlying error mints the SAME external_id every
        # poll — a real "[ERROR] 2026/09/01 15:00:34 (504) Unknown: …" would
        # otherwise read as the dangling ref clearing and reappearing every 30
        # minutes. The displayed title below keeps the raw first_line.
        key = fingerprint(first_line)[:80] or "unknown"
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
              now: dt.datetime, gate_min: int,
              track_resolution: bool = True,
              deliver: bool = True) -> tuple[list[dict], list[dict]]:
    """Fold `observed` into the events table and return (new, resolved).

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

    return new_events, resolved_events


_DEDUP_NORMALIZE = re.compile(r"[^a-z0-9]+")


def normalize_title(text: str) -> str:
    return _DEDUP_NORMALIZE.sub("-", text.lower()).strip("-")[:120]


# The event fingerprint — the identity of every grouped/derived source (slack_alert,
# slack_update, hermes_log, the op-refs `raw:` key): the title with everything that
# varies between two occurrences of the SAME event stripped, so the same line from
# two log files, or the same alert an hour later, is one event. State sources
# (uk monitor id, docker `unhealthy:{name}`, github `repo#num`) keep their native
# ids; they are not fingerprinted.
#
# Order matters: each class removes the longer shape before a shorter rule can
# half-eat it (a UUID or ISO timestamp is gone before the digit-run rule runs; a
# URL's path before the path rule). Every class is replaced by a space, never
# deleted, so neighbours cannot fuse into a new token.
_FP_URL_PATH_RE = re.compile(r"(\b[a-z][a-z0-9+.-]*://[^/\s'\"`)\]>]*)/[^\s'\"`)\]>]*", re.I)
_FP_UUID_RE = re.compile(
    r"(?<![0-9a-f])[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}(?![0-9a-f])", re.I)
_FP_ISO_TS_RE = re.compile(
    r"\d{4}[-/]\d{2}[-/]\d{2}(?:[ T_]\d{2}[:-]?\d{2}[:-]?\d{2}(?:[.,]\d+)?(?:Z|[+-]\d{2}:?\d{2})?)?")
_FP_RFC_TS_RE = re.compile(
    r"(?:\b(?:Mon|Tue|Wed|Thu|Fri|Sat|Sun),?\s+)?\d{1,2}\s+"
    r"(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)[a-z]*\s+\d{4}"
    r"(?:\s+\d{2}:\d{2}(?::\d{2})?(?:\s*(?:GMT|UTC|Z|[+-]\d{4}))?)?", re.I)
_FP_DATE_RE = re.compile(r"\b\d{1,2}[./]\d{1,2}[./]\d{2,4}\b")
_FP_TIME_RE = re.compile(r"\b\d{1,2}:\d{2}(?::\d{2})?(?:[.,]\d+)?(?:\s*[AP]M)?(?:Z|[+-]\d{2}:?\d{2})?", re.I)
_FP_HEX_LITERAL_RE = re.compile(r"\b0x[0-9a-f]+\b", re.I)
# `/a/b`, `~/a`, `./a`, `../a` — not preceded by a word char, so "and/or" and a
# date's inner slashes are left alone.
_FP_PATH_RE = re.compile(r"(?<![\w.~:/-])(?:~|\.{1,2})?/[^\s'\"`),;\]>]+")
_FP_LOGFILE_RE = re.compile(r"(?<![\w-])[\w.-]+\.log(?:\.\d+)*(?:\.gz)?(?![\w])")
# 6+ hex chars with at least one digit (git SHAs, session/request ids): pure-letter
# words like "decade" survive; bounded by non-alphanumerics so `_ea474d_` matches.
_FP_HEX_ID_RE = re.compile(r"(?<![A-Za-z0-9])(?=[0-9a-f]*\d)[0-9a-f]{6,}(?![A-Za-z0-9])", re.I)
_FP_NUMBER_RE = re.compile(r"\d+")
_FP_STRIP_ORDER = (
    (_FP_URL_PATH_RE, r"\1 "), (_FP_UUID_RE, " "), (_FP_ISO_TS_RE, " "), (_FP_RFC_TS_RE, " "),
    (_FP_DATE_RE, " "), (_FP_TIME_RE, " "), (_FP_HEX_LITERAL_RE, " "), (_FP_PATH_RE, " "),
    (_FP_LOGFILE_RE, " "), (_FP_HEX_ID_RE, " "), (_FP_NUMBER_RE, " "),
)


def fingerprint(title: str) -> str:
    """Event identity: `title` minus timestamps, UUIDs, hex ids, paths, log-file
    names and numbers, then normalize_title()'d. Falls back to normalize_title()
    of the raw title when stripping leaves nothing (an all-numbers title)."""
    stripped = title
    for pattern, repl in _FP_STRIP_ORDER:
        stripped = pattern.sub(repl, stripped)
    return normalize_title(stripped) or normalize_title(title)


def aggregate_slack_batch(msgs: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Group raw slack messages by event fingerprint. Returns list of grouped dicts."""
    groups: dict[str, dict[str, Any]] = {}
    for m in msgs:
        key = fingerprint(m["title"])
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


def _run_poll(conn: sqlite3.Connection, now: dt.datetime, env: dict[str, str],
              deliver: bool = True) -> tuple[list[dict], list[dict]]:
    """Run the full polling pipeline against `conn`. Returns (new, resolved).

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
    all_res: list[dict] = []

    n, res = reconcile(conn, "uk", poll_uk(env), now, UK_DOWN_GATE_MIN, deliver=deliver)
    all_new += n; all_res += res
    conn.commit()

    for host in ("homelab", "vps"):
        src = f"docker_{host}"
        n, res = reconcile(conn, src, poll_docker(env, host), now,
                           DOCKER_UNHEALTHY_GATE_MIN, deliver=deliver)
        all_new += n; all_res += res
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
        n, res = reconcile(conn, src, observed, now, 0, deliver=deliver)
        all_new += n; all_res += res
        conn.commit()

    resolve_stale_github_issue_events(conn, now)
    conn.commit()

    gh = poll_github(env)
    for kind in ("github_pr",):
        if gh[kind] is None:
            # Search failed this cycle — skip reconciling, same as an
            # unreachable op_refs host: a blind poll must not resolve anything.
            continue
        n, res = reconcile(conn, kind, gh[kind], now, 0, deliver=deliver)
        all_new += n; all_res += res
        conn.commit()

    n, res = reconcile(conn, "hermes_cron", poll_hermes_cron(), now, 0, deliver=deliver)
    all_new += n; all_res += res
    conn.commit()

    n, res = reconcile(conn, "stray_skill", poll_stray_skills(), now, 0, deliver=deliver)
    all_new += n; all_res += res
    conn.commit()

    log_groups = poll_hermes_logs(conn, now, now_iso)
    if log_groups:
        all_new += upsert_grouped(conn, "hermes_log", log_groups, now,
                                  flap_threshold=1, deliver=deliver)
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

    return all_new, all_res


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
# CHANGED, so an idle pass (nothing new, nothing resolved)
# leaves no trace at all, and "the poller ran and found nothing" becomes
# indistinguishable from "the poller did not run." This is what /metrics'
# per-poller age (warden/scripts/api.py) reads to answer that question for
# this poller specifically, by name, rather than only "something is blind."
HEARTBEAT_CURSOR_KEY = "watchdog_poll_last_run"


def record_heartbeat(conn: sqlite3.Connection, *,
                     new_count: int, resolved_count: int,
                     slack_blind: bool) -> None:
    # No timestamp argument — see triage.py's record_heartbeat() for why the
    # write clock is read here rather than accepted from a caller holding the
    # pass's START time.
    value = json.dumps({
        "new": new_count, "resolved": resolved_count,
        "slack_blind": slack_blind,
    }, sort_keys=True)
    conn.execute(
        "INSERT INTO cursors(key, value, updated_at) VALUES (?, ?, ?) "
        "ON CONFLICT(key) DO UPDATE SET value=excluded.value, updated_at=excluded.updated_at",
        (HEARTBEAT_CURSOR_KEY, value, dt.datetime.now(dt.timezone.utc).isoformat()),
    )
    conn.commit()


def main(argv: list[str] | None = None) -> int:
    """One poll pass. `--post` is what the LaunchAgent passes: on a clean run (rc == 0) it
    pings the UptimeKuma push monitor, which is the only thing that notices this poller
    itself has died. `--dry-run` polls a temp copy of the ledger and writes, pings and posts
    nothing. Nothing is ever posted to Slack from here.

    rc is 1 while a Slack source is blind (SLACK_FAIL_STREAK_ALERT consecutive failed
    polls), reported on stderr, so the heartbeat is withheld and Kuma raises it."""
    args = set(argv if argv is not None else sys.argv[1:])
    post = "--post" in args
    dry_run = "--dry-run" in args

    env = load_env()
    now = dt.datetime.now(dt.timezone.utc)

    if dry_run:
        # Run poll against a temp copy of the DB so we don't advance notified_at
        # on operator-driven invocations.
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
            all_new, all_res = _run_poll(conn, now, env)
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
            # The Kuma monitor measures "is the poller alive", not "did the ledger
            # open": a pass deliberately skipped during a migration window is a
            # live poller.
            if post:
                _push_uptime_heartbeat()
            return 0
        all_new, all_res = _run_poll(conn, now, env)
        conn.commit()
        slack_blind = slack_poll_failure(conn)
        record_heartbeat(conn, new_count=len(all_new),
                         resolved_count=len(all_res), slack_blind=bool(slack_blind))
        conn.close()

    if slack_blind:
        print(slack_blind, file=sys.stderr)
    rc = 1 if slack_blind else 0
    # The ping answers "is the poller still running", not "did it have news": a quiet
    # poll is the normal case and must still ping, or the alert pages on every silent
    # half hour and gets ignored — which is how an eleven-day blindness goes unnoticed.
    if post and not dry_run and rc == 0:
        _push_uptime_heartbeat()
    return rc


if __name__ == "__main__":
    sys.exit(main())
