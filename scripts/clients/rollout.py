"""The one-arm closed allowlist for the single deploy command a merge may
run — the Python port of the retired bash CLI's `deploy_argv` (1782-1787) and the
argv-build-and-run half of `run_deploy_if_enabled` (1839-1894). The policy
half of that function (autoDeploy / deploy key lookup in the triage policy
JSON) is NOT here — it belongs to the lifecycle module that calls this one.

A policy file may name and parameterise, never express. Config carries the
key; this module owns the argv.
"""

from __future__ import annotations

import subprocess
from typing import Callable, NamedTuple

from .errors import PolicyError

ROLLOUTS: dict[str, tuple[str, ...]] = {
    "hyperdx-apply": ("ssh", "vps", "cd ~/vps && make hyperdx-apply ENV=prod"),
    # homelab's `make uk-sync`, spelled out: the Makefile is a deploy-target
    # definition and so outside every autoMergePaths — warden must not be able
    # to write the code it then runs (DESIGN.md § Self-concealing change). op
    # runs ON the homelab server (its own service account), never on the mini.
    # No --delete-orphans: without a TTY sync.py only lists orphans.
    "uk-sync": ("ssh", "homelab",
                "cd ~/homelab && git pull --ff-only && op run --env-file=.env.tpl -- "
                "uptime-kuma/.venv/bin/python uptime-kuma/sync.py "
                "--extra-config ../homelab-private/uptime-kuma/monitors.yaml"),
    # weatherorb's periodic LaunchAgents (watchdog, obs, fcstlog, backfill,
    # blendfield) exec the live checkout on every run, so a fast-forward alone
    # rolls them out. tileserver (uvicorn over src/) and sync (ops/run-sync.sh)
    # are KeepAlive daemons that keep their loaded process, so they are
    # kickstarted after the pull (§105) — without that a merged tileserver fix
    # would read `fixed` against a watchdog that never ran it. serve is the
    # vendored open-meteo binary: nothing a merge can change without a rebuild,
    # so it is left alone. `kickstart -k` never re-reads a plist; ops/*.plist
    # is NEVER_AUTO_MERGE anyway. A failed kickstart fails the deploy — a merged
    # PR with a daemon still on the old code is `needs_human`, never `merged`.
    "weatherorb-pull": ("/bin/sh", "-c",
                        'git -C "$HOME/SourceRoot/weatherorb" pull --ff-only'
                        ' && for job in tileserver sync; do'
                        ' launchctl kickstart -k "gui/$(id -u)/com.jkrumm.weatherorb.$job" || exit 1; done'),
}


class RolloutResult(NamedTuple):
    ok: bool
    exit_code: int
    output: str


def argv_for(key: str) -> tuple[str, ...] | None:
    return ROLLOUTS.get(key)


def run(key: str, *, timeout_s: int = 180, runner: Callable[..., "subprocess.CompletedProcess[str]"] = subprocess.run) -> RolloutResult:
    argv = argv_for(key)
    if argv is None:
        raise PolicyError(f"deploy key {key} is not in the allowlist")

    try:
        proc = runner(list(argv), capture_output=True, text=True, timeout=timeout_s)
    except subprocess.TimeoutExpired:
        return RolloutResult(False, 124, f"timed out after {timeout_s}s")
    except (OSError, FileNotFoundError) as exc:
        # `ssh` missing from PATH, a permission error, anything the OS
        # refuses before a process even starts — never a raw exception out
        # of the actuator; a landed merge with a deploy that could not even
        # launch is still `attempted: True, ok: False`, not a crash.
        return RolloutResult(False, 127, str(exc))

    combined = (proc.stdout or "") + (proc.stderr or "")
    return RolloutResult(proc.returncode == 0, proc.returncode, combined[-2000:])
