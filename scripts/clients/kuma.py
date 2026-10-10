"""Uptime Kuma push heartbeat for the loop: one GET at the end of every completed pass.

The URL lives in a mode-600 file, not the secrets cache — a monitor must not depend on the thing it
monitors, and `op` is not signed in on this host. An absent file means "no monitor configured" and is
silent; a failed ping is a stderr line and never fails the pass (Kuma reads the silence as the page).
"""

from __future__ import annotations

import os
import sys
import urllib.error
import urllib.request
from pathlib import Path

PUSH_TIMEOUT_S = 10


def _push_file() -> Path:
    return Path(os.path.expanduser(os.environ.get("WARDEN_LOOP_PUSH_FILE")
                                   or "~/.config/uptime-kuma/warden-loop-push-url"))


def ping_loop() -> bool:
    """Push `up` for the loop monitor. True when Kuma answered 2xx; False when there is no URL file
    or the ping failed (the latter is logged)."""
    try:
        url = _push_file().read_text().strip()
    except OSError:
        return False
    if not url.startswith("https://"):
        return False
    req = urllib.request.Request(url, headers={"User-Agent": "curl/8"}, method="GET")
    try:
        with urllib.request.urlopen(req, timeout=PUSH_TIMEOUT_S) as resp:
            return 200 <= resp.status < 300
    except (urllib.error.URLError, OSError) as exc:
        print(f"triage: Kuma loop heartbeat failed: {exc}", file=sys.stderr)
        return False
