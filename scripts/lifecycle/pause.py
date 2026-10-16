"""pause — warden's kill-switch: one flag file, one question, `is_paused()`.

`warden pause [--reason <text>]` writes `~/.warden/paused` (JSON: `reason`, `since`); `warden resume`
removes it. While it exists, warden still ingests, classifies and triages, but the LOOP starts nothing
new: no investigate / implement / revision / review / `update_pr` / revert episode, no host verb, no
loop-driven merge, no deploy, no `warden run` / `warden dispatch` dispatch. The owner's own explicit
merge is NOT stopped (below). Every site that starts one asks `is_paused()` (the
loop re-exports it as `core.is_paused()`), and `require_running()` is the backstop under
`dispatch.open_episode()` / `open_review()`.

Not gated, on purpose: ingest, classify, triage jobs and their fold (a classification, not an
action), polling and folding episodes already running, the fixed-by sweep's triage job, the owner's
own Argo `merge` / `warden merge` (an explicit human act on a PR the owner chose; the deploy that
follows it is still held), and every
state-only CLI verb (`close`, `retry`, `reinvestigate`, `adopt`: they move an item, the gated stage
pollers do the starting).

Fail closed: a flag file that exists but cannot be read or parsed still pauses — a kill-switch that
silently opens on a half-written file is not one. A missing file is the only "running" answer.

The path is `$WARDEN_PAUSE_FILE`, else `$WARDEN_HOME/paused`, else `~/.warden/paused`, read at call
time (like `rollout.deploy_lock()`), so a test points it at a temp file with one env var and the
LaunchAgents, the CLI and the API all agree on one file without sharing a process.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import tempfile
from pathlib import Path
from typing import Any

from clients.errors import PolicyError

UNREADABLE_REASON = "(the pause flag exists but could not be read — treated as paused)"


class PausedError(PolicyError):
    """A dispatch was attempted while warden is paused. A refusal (exit 4), never a failure of the
    item it was for: callers that already treat `PolicyError` as a deferral keep the item as it was."""


def flag_path() -> Path:
    if os.environ.get("WARDEN_PAUSE_FILE"):
        return Path(os.environ["WARDEN_PAUSE_FILE"]).expanduser()
    home = (Path(os.environ["WARDEN_HOME"]).expanduser()
            if os.environ.get("WARDEN_HOME") else Path.home() / ".warden")
    return home / "paused"


def state() -> dict[str, Any] | None:
    """`{"reason": str | None, "since": str | None}` while paused, None while running."""
    path = flag_path()
    try:
        raw = path.read_text(encoding="utf-8")
    except FileNotFoundError:
        return None
    except (OSError, UnicodeDecodeError):   # a flag of non-UTF-8 bytes is as unreadable as one we cannot open
        return {"reason": UNREADABLE_REASON, "since": None}
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        data = None
    if not isinstance(data, dict):
        return {"reason": UNREADABLE_REASON, "since": None}
    reason, since = data.get("reason"), data.get("since")
    return {"reason": reason if isinstance(reason, str) and reason else None,
            "since": since if isinstance(since, str) and since else None}


def is_paused() -> bool:
    return state() is not None


def describe(paused: dict[str, Any] | None = None) -> str:
    """One line for a log or an error: `paused since <time>: <reason>`."""
    paused = paused if paused is not None else state()
    if paused is None:
        return "running"
    since = f" since {paused['since']}" if paused.get("since") else ""
    reason = f": {paused['reason']}" if paused.get("reason") else ""
    return f"paused{since}{reason}"


def require_running(what: str) -> None:
    """Raise PausedError naming `what` was refused. The backstop, not the primary gate: stages check
    `is_paused()` first so a pause leaves no half-claimed item behind."""
    paused = state()
    if paused is not None:
        raise PausedError(f"warden is {describe(paused)} — {what} refused; `warden resume` to lift it")


def pause(reason: str | None = None, now: dt.datetime | None = None) -> dict[str, Any]:
    """Write the flag, atomically. Already paused: keep the original `since`, and take a new reason
    only when one is given. Returns the resulting state."""
    now = now or dt.datetime.now(dt.timezone.utc)
    current = state()
    since = current["since"] if current and current["since"] else now.astimezone(dt.timezone.utc).isoformat()
    new_reason = (reason or "").strip() or (current["reason"] if current else None)
    if new_reason == UNREADABLE_REASON:
        new_reason = None
    body = {"reason": new_reason, "since": since}
    path = flag_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    fd, tmp = tempfile.mkstemp(prefix=".paused-", dir=path.parent)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(body, f)
        os.replace(tmp, path)
    except BaseException:
        Path(tmp).unlink(missing_ok=True)
        raise
    return body


def resume() -> dict[str, Any] | None:
    """Remove the flag. Returns the state it lifted, or None when warden was not paused."""
    previous = state()
    flag_path().unlink(missing_ok=True)
    return previous
