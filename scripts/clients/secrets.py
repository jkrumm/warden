"""secrets — the one env-var-first, secrets-run-second resolver every
plain-HTTP client in this package used to copy by hand (`slack.py`'s
`resolve_slack_token()`, `argo.py`'s `resolve_argo_token()`). `github.py`'s
`token()` is deliberately NOT ported onto this — see its own comment — the
two have diverged in ways that matter, not by accident.

stdlib only, never raises: any failure (missing binary, non-zero exit, empty
stdout, a subprocess timeout) resolves to `""`. A caller decides whether a
missing secret is fatal or best-effort; this module never does.
"""

from __future__ import annotations

import os
import subprocess
from pathlib import Path

_SECRETS_RUN = Path.home() / ".local" / "bin" / "secrets-run"


def resolve_secret(env_var: str, ref: str, *, timeout: float = 15.0) -> str:
    """`env_var` first, else `secrets-run read <ref>` with `timeout` seconds
    (default 15s, matching every prior copy of this resolver). `PATH` is
    widened to include Homebrew/`/usr/local/bin` ahead of whatever the
    caller inherited — the same fix every prior copy already carried, because
    a LaunchAgent's own PATH does not reliably include either."""
    val = os.environ.get(env_var, "")
    if val:
        return val
    env = os.environ.copy()
    env["PATH"] = "/opt/homebrew/bin:/usr/local/bin:" + env.get("PATH", "/usr/bin:/bin")
    try:
        r = subprocess.run(
            [str(_SECRETS_RUN), "read", ref],
            capture_output=True, text=True, timeout=timeout, env=env,
        )
    except (OSError, subprocess.SubprocessError):
        return ""
    return r.stdout.strip() if r.returncode == 0 else ""
