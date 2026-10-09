"""argo_client — the plain HTTP client that pushes warden's own projection to
Argo (the dashboard on the VPS, `https://argo.jkrumm.com/api`) after every
loop tick.

WHY THIS EXISTS. Argo cannot reach the mini — there is no tailnet door
onto this box's loopback-only `warden-api` (see `scripts/api.py`'s own
docstring). So the direction has to reverse: the mini pushes, Argo just
holds the last snapshot it was handed. `POST /warden/snapshot` is being
built in parallel on the Argo side; until it deploys every push here 404s,
which is a logged non-event, never a failed tick — see
`notify.push_argo_snapshot()`.

Same shape as `scripts/clients/slack.py`: one token resolver, one thin
`urllib`-only POST, never raises. stdlib only.
"""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.request

from .secrets import resolve_secret

# 1 MB cap the Argo endpoint itself enforces (413 past it) — checked here
# too, before ever sending, so an oversize payload never even opens a
# connection.
MAX_BODY_BYTES = 1_000_000

_ARGO_TOKEN_REF = "op://common/api/SECRET"


def argo_api_base() -> str:
    """`ARGO_URL` env, re-read at call time (same reason `agent-gateway._base()`/
    `slack.slack_api_base()` are functions, not captured module constants: a
    test can retarget this without reimporting), else Argo's own default —
    trailing slashes stripped so the `/warden/snapshot` join never doubles
    up a `//`."""
    return os.environ.get("ARGO_URL", "https://argo.jkrumm.com/api").rstrip("/")


def resolve_argo_token() -> str:
    """`ARGO_API_SECRET` env first, else `secrets-run read op://common/api/
    SECRET` with a 15s timeout — same fallback order and failure shape as
    `slack.resolve_slack_token()`. "" on any failure; a caller decides
    whether a missing token is fatal or best-effort, this function never
    does. Thin wrapper over `clients.secrets.resolve_secret()`."""
    return resolve_secret("ARGO_API_SECRET", _ARGO_TOKEN_REF)


def push_snapshot(payload: dict, *, token: str | None = None, timeout: float = 15.0) -> str:
    """POST the whole projection to `/warden/snapshot`. Never raises — every
    outcome is folded into the returned status string:

    - `"ok"` — 2xx.
    - `"no-secret"` — no token resolvable; nothing was sent.
    - `"encode-error"` — `payload` was not JSON-serializable; nothing was sent.
    - `"too-large"` — the encoded body exceeds `MAX_BODY_BYTES`; nothing was
      sent (the endpoint's own 413 is a backstop, not the primary check).
    - `"http-error:{code}"` — a non-2xx response, including the 404 every
      push gets until the Argo endpoint deploys.
    - `"network-error"` — unreachable host, timeout, or any other transport
      failure.
    """
    resolved_token = token if token is not None else resolve_argo_token()
    if not resolved_token:
        return "no-secret"
    try:
        body = json.dumps(payload).encode()
    except (TypeError, ValueError):
        return "encode-error"
    if len(body) > MAX_BODY_BYTES:
        return "too-large"
    req = urllib.request.Request(
        f"{argo_api_base()}/warden/snapshot", data=body,
        headers={"Authorization": f"Bearer {resolved_token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if 200 <= resp.status < 300:
                return "ok"
            return f"http-error:{resp.status}"
    except urllib.error.HTTPError as e:
        return f"http-error:{e.code}"
    except (urllib.error.URLError, TimeoutError, OSError):
        return "network-error"


def fetch_actions(machine: str, *, token: str | None = None, timeout: float = 15.0) -> tuple[str, list[dict]]:
    """GET the owner's pending action queue for `machine` off
    `/warden/actions?machine={machine}&status=pending`. Never raises — every
    outcome is folded into the returned status string, same vocabulary as
    `push_snapshot()`:

    - `"ok"` — 2xx with a JSON list body. `actions` carries it.
    - `"no-secret"` — no token resolvable; nothing was sent.
    - `"decode-error"` — a 2xx body that is not valid JSON, or is valid JSON
      that is not a list.
    - `"http-error:{code}"` — a non-2xx response.
    - `"network-error"` — unreachable host, timeout, or any other transport
      failure.

    `actions` is `[]` unless `status` is `"ok"`. Each entry is expected to
    carry `id`, `event_id`, `verb` and `payload`, but this function does not
    validate that shape — it only decodes JSON and hands it back; the caller
    (`notify.apply_argo_actions()`) owns validation."""
    resolved_token = token if token is not None else resolve_argo_token()
    if not resolved_token:
        return "no-secret", []
    req = urllib.request.Request(
        f"{argo_api_base()}/warden/actions?machine={machine}&status=pending",
        headers={"Authorization": f"Bearer {resolved_token}"},
        method="GET",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if not (200 <= resp.status < 300):
                return f"http-error:{resp.status}", []
            # Bounded the same way every outbound body in this file is
            # (MAX_BODY_BYTES) — this is the one INBOUND read, new surface
            # push_snapshot()/ack_action() never had, since they only ever
            # POST. An oversized or misbehaving response is folded into the
            # same "decode-error" outcome as unparsable JSON, never fully
            # buffered first.
            raw = resp.read(MAX_BODY_BYTES + 1)
            if len(raw) > MAX_BODY_BYTES:
                return "decode-error", []
    except urllib.error.HTTPError as e:
        return f"http-error:{e.code}", []
    except (urllib.error.URLError, TimeoutError, OSError):
        return "network-error", []
    try:
        data = json.loads(raw)
    except (TypeError, ValueError):
        return "decode-error", []
    if not isinstance(data, list):
        return "decode-error", []
    return "ok", data


def ack_action(action_id: str | int, *, status: str, result: dict | None = None,
               error: str | None = None, token: str | None = None, timeout: float = 15.0) -> str:
    """POST the outcome of one action back to `/warden/actions/{action_id}/ack`.

    `status` here is the ACTION's own outcome (`"applied"`/`"rejected"`/
    `"failed"`), not the HTTP-outcome status this function returns — those
    are two different vocabularies, easy to conflate because both are named
    `status`. The body is `{"status": status, "result": result, "error":
    error}` with any key the caller left as `None` omitted, never sent as a
    literal JSON `null`.

    Never raises. Returns the HTTP-outcome status string, same vocabulary as
    `push_snapshot()`:

    - `"ok"` — 2xx.
    - `"no-secret"` — no token resolvable; nothing was sent.
    - `"encode-error"` — the body was not JSON-serializable; nothing was sent.
    - `"too-large"` — the encoded body exceeds `MAX_BODY_BYTES`; nothing was
      sent.
    - `"http-error:{code}"` — a non-2xx response.
    - `"network-error"` — unreachable host, timeout, or any other transport
      failure.
    """
    resolved_token = token if token is not None else resolve_argo_token()
    if not resolved_token:
        return "no-secret"
    payload: dict = {"status": status}
    if result is not None:
        payload["result"] = result
    if error is not None:
        payload["error"] = error
    try:
        body = json.dumps(payload).encode()
    except (TypeError, ValueError):
        return "encode-error"
    if len(body) > MAX_BODY_BYTES:
        return "too-large"
    req = urllib.request.Request(
        f"{argo_api_base()}/warden/actions/{action_id}/ack", data=body,
        headers={"Authorization": f"Bearer {resolved_token}", "Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if 200 <= resp.status < 300:
                return "ok"
            return f"http-error:{resp.status}"
    except urllib.error.HTTPError as e:
        return f"http-error:{e.code}"
    except (urllib.error.URLError, TimeoutError, OSError):
        return "network-error"
