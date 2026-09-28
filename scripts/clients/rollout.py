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
from pathlib import Path
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
    # weatherorb's periodic LaunchAgents (watchdog, sync, obs, ...) exec the
    # live checkout on every run, so a fast-forward IS the rollout for them.
    "weatherorb-pull": ("git", "-C", str(Path.home() / "SourceRoot" / "weatherorb"), "pull", "--ff-only"),
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
