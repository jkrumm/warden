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

`hermes-agent/plugins/dispatch-approval/__init__.py`'s `_send_to_origin()`
mirrors this module by hand rather than importing it — that plugin runs
inside a different process, in a different repo, and this repo's modules
are reached by local path, not a package it can depend on.

stdlib only, no logging side effects — every function here either returns
a value or raises; it never prints. Callers own their own diagnostics.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request
from typing import Any

from .secrets import resolve_secret

SLACK_POST_URL = "https://slack.com/api/chat.postMessage"


def slack_api_base() -> str:
    """`WARDEN_SLACK_API`, defaulting to Slack's own base — the same
    override the retired bash CLI's `post_approval_buttons` read as
    `WARDEN_SLACK_API`, so a test can point every Slack-facing call in
    this module at a stub server without touching a second constant."""
    return os.environ.get("WARDEN_SLACK_API", "https://slack.com/api")


# Same secret, same fallback order triage.py's resolve_slack_token() has
# always used: the gateway's own env var first (set when this runs inside
# it), then the offline secrets-run cache (safe headless — see
# ~/.claude/CLAUDE.md § Secrets, "never `op read`/`op run` on the mini").
_SLACK_TOKEN_REF = "op://hermes/slack/bot-token"


def resolve_slack_token() -> str:
    """`SLACK_BOT_TOKEN` env first, else `secrets-run read op://hermes/slack/
    bot-token` with a 15s timeout. "" on any failure — a caller decides
    whether a missing token is fatal or best-effort, this function never
    does. Thin wrapper over `clients.secrets.resolve_secret()`, the resolver
    every plain-HTTP client in this package shares."""
    return resolve_secret("SLACK_BOT_TOKEN", _SLACK_TOKEN_REF)


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


def slack_post_blocks(token: str, channel: str, text: str, blocks: list[dict[str, Any]],
                       thread_ts: str | None = None, *, timeout: int = 15) -> dict[str, Any]:
    """POST `chat.postMessage` with Block Kit `blocks` alongside the plain
    `text` fallback — the transport `mint()` (lifecycle/approvals.py) uses to
    post the Approve/Deny buttons, the Python port of the retired bash CLI's
    `post_approval_buttons` (977-1038). Same never-raises contract as
    `slack_post_message`."""
    payload: dict[str, Any] = {"channel": channel, "text": text, "blocks": blocks}
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
