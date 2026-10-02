"""slack_client — the plain Slack Web API HTTP client, in one place.

WHY THIS EXISTS. `dispatch-sweep.py` used to deliver every verdict and nudge
by shelling out to `hermes send`, which posts through the gateway's Slack
Socket Mode connection — the same connection that was found in a reconnect
loop, i.e. the one piece of infrastructure a control-plane sweeper cannot
depend on staying up. `scripts/triage.py` never had that problem: it always
posted straight to `https://slack.com/api/chat.postMessage` /
`chat.update` over plain `urllib`, no gateway, no subprocess. This module
lifts that token resolver and a minimal `chat.postMessage` call out of
triage.py so `dispatch-sweep.py` (and anything else in this repo) can use
the exact same HTTP path instead of a second, gateway-dependent one.

stdlib only. Every function here either returns a value or raises and never
prints, with one deliberate exception: `resolve_slack_token()` prints once
per process when it falls back off Warden's own identity onto Hermes's — see
its own docstring. Every other caller still owns its own diagnostics.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request
from typing import Any

from .secrets import resolve_secret

SLACK_POST_URL = "https://slack.com/api/chat.postMessage"


def slack_api_base() -> str:
    """`WARDEN_SLACK_API`, defaulting to Slack's own base, so a test can
    point every Slack-facing call in this module at a stub server without
    touching a second constant."""
    return os.environ.get("WARDEN_SLACK_API", "https://slack.com/api")


# The pre-Warden identity, unchanged: the gateway's own env var first (set
# when this runs inside it), then the offline secrets-run cache (safe
# headless — see ~/.claude/CLAUDE.md § Secrets, "never `op read`/`op run` on
# the mini"). Kept as the FALLBACK below, not the primary — see
# resolve_slack_token()'s own docstring for why a second identity exists at
# all and slack/README.md for how it's created and seeded.
_SLACK_TOKEN_REF = "op://hermes/slack/bot-token"

# Warden's own app (slack/app-manifest.json) — chat:write/chat:write.public
# only, no incoming-webhook, no socket mode. Tried FIRST so triage cards,
# remediation receipts are attributable to Warden rather than
# riding under the Hermes bot's username.
_WARDEN_SLACK_TOKEN_REF = "op://common/slack/WARDEN_BOT_TOKEN"

# Printed at most once per process (see resolve_slack_token()) so a
# still-unseeded Warden app is loud exactly once in cron output rather than
# once per card.
_warned_hermes_fallback = False


def resolve_slack_token() -> str:
    """Warden's own identity first: env `WARDEN_SLACK_BOT_TOKEN`, else
    `secrets-run read op://common/slack/WARDEN_BOT_TOKEN`. Falls back,
    unchanged, to the pre-Warden identity: env `SLACK_BOT_TOKEN`, else
    `secrets-run read op://hermes/slack/bot-token`. "" on total failure — a
    caller decides whether a missing token is fatal or best-effort, this
    function never does.

    Landing on the Hermes fallback prints one stderr line PER PROCESS
    (`_warned_hermes_fallback` below), not per call — triage.py alone calls
    this once per card per pass, and a LaunchAgent process lives for exactly
    one pass, so "once" here already means "once per tick" without a caller
    having to know that. Thin wrapper over `clients.secrets.resolve_secret()`,
    the resolver every plain-HTTP client in this package shares."""
    global _warned_hermes_fallback
    token = resolve_secret("WARDEN_SLACK_BOT_TOKEN", _WARDEN_SLACK_TOKEN_REF)
    if token:
        return token
    token = resolve_secret("SLACK_BOT_TOKEN", _SLACK_TOKEN_REF)
    if token and not _warned_hermes_fallback:
        print(
            "triage: Slack posting as Hermes — op://common/slack/WARDEN_BOT_TOKEN "
            "unresolved; see slack/README.md",
            file=sys.stderr,
        )
        _warned_hermes_fallback = True
    return token


def slack_raw_post(url: str, payload: dict[str, Any], token: str, *,
                    timeout: int = 15) -> dict[str, Any] | None:
    """The bare `chat.*` POST that `scripts/triage.py`'s and
    `scripts/watchdog-poll.py`'s own `_slack_call()` wrappers share —
    arbitrary `url` (postMessage or update), Slack's parsed JSON on any
    completed round-trip, `None` on a transport/parse failure. Each wrapper
    interprets `None` and Slack's own `{"ok": false}` its own,
    historically-different way, which is why this stays the bare primitive
    rather than folding their differing return shapes in here."""
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError,
            TimeoutError, OSError):
        return None


def slack_post_message(token: str, channel: str, text: str,
                        thread_ts: str | None = None, *, timeout: int = 15) -> dict[str, Any]:
    """POST `chat.postMessage`. Returns Slack's own parsed JSON response on
    any completed round-trip — including Slack's own `{"ok": false, "error":
    "..."}` for a Slack-side rejection (bad token, channel_not_found, …) —
    and a synthetic `{"ok": False, "error": "..."}` on a transport/parse
    failure, so a caller can treat both shapes identically via
    `response.get("ok")` and never has to catch an exception from this
    function. Never raises."""
    payload: dict[str, Any] = {"channel": channel, "text": text}
    if thread_ts:
        payload["thread_ts"] = thread_ts
    body = json.dumps(payload).encode()
    req = urllib.request.Request(
        f"{slack_api_base()}/chat.postMessage", data=body,
        headers={"Authorization": f"Bearer {token}", "Content-Type": "application/json; charset=utf-8"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            return json.loads(resp.read().decode())
    except (urllib.error.URLError, urllib.error.HTTPError, json.JSONDecodeError,
            TimeoutError, OSError) as e:
        return {"ok": False, "error": str(e)}
