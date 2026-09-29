#!/usr/bin/env python3
"""The op-refs probe must run in the crons' own environment.

Run: .venv/bin/python3 tests/test_watchdog_op_refs_env.py

THE BUG, measured 2026-09-29. Every op-wrapped cron line on homelab begins
`. /home/jkrumm/.profile;` — that is where `OP_SOCK` is pinned, and with it the
`op` client reaches the daemon socket and the cache serves the whole template
for ZERO network requests. The probe did not source it:

    ssh homelab 'cd ~/homelab && op run --env-file=.env.tpl -- true'

so `op` derived its socket from an unset XDG_RUNTIME_DIR, dialled
/var/run/user/1000/op-daemon.sock — which does not exist; the live daemon
listens at ~/.config/op/ — missed the daemon cache, went to the network, and
reported the shared service-account budget's

    [ERROR] Too many requests. Your client has been rate-limited.

as "1Password refs unresolved on homelab" (the `raw:` fallback signature) while
all six op-wrapped crons on that host were green. A probe that tests an
environment no cron uses can only ever answer a different question than the one
it was built for: "would the crons survive this template?".

THE INVARIANT THIS FILE ENFORCES: **every OP_REF_HOSTS command sources the host
profile before `op run`, behind a `[ -r ]` guard.** The guard is load-bearing,
not decoration — `.` is a POSIX special builtin, so dash aborts the ENTIRE
command line when the name cannot be opened, and one absent profile would then
cost every poll rather than the credential alone (homelab docs/decisions.md ->
1Password CLI in cron shells).
"""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
_spec = importlib.util.spec_from_file_location(
    "watchdog_poll", REPO / "scripts" / "watchdog-poll.py")
assert _spec is not None and _spec.loader is not None
wp = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(wp)

TRIAGE_PATH = REPO / "scripts" / "triage.py"
_t_spec = importlib.util.spec_from_file_location("triage", TRIAGE_PATH)
assert _t_spec is not None and _t_spec.loader is not None
triage = importlib.util.module_from_spec(_t_spec)
_t_spec.loader.exec_module(triage)

failures: list[str] = []


def check(name: str, got, want) -> None:
    if got == want:
        print(f"  ok   {name}")
    else:
        failures.append(f"{name}: got {got!r}, want {want!r}")
        print(f"  FAIL {name}: got {got!r}, want {want!r}")


def test_every_probe_sources_the_profile_before_op_run() -> None:
    for host, cmd in wp.OP_REF_HOSTS.items():
        check(f"{host}: sources the profile behind a [ -r ] guard",
              cmd.startswith("[ -r ~/.profile ] && . ~/.profile;"), True)
        check(f"{host}: profile sourced before op runs",
              ". ~/.profile" in cmd and cmd.index(". ~/.profile") < cmd.index("op run"), True)


def test_poll_op_refs_sends_the_profiled_command() -> None:
    """The assertion that matters: the argv ssh actually receives."""
    seen: list[list[str]] = []
    original = wp.subprocess.run

    def fake_run(argv, **kwargs):
        seen.append(list(argv))
        return wp.subprocess.CompletedProcess(argv, 0, stdout="", stderr="")

    wp.subprocess.run = fake_run
    try:
        wp.poll_op_refs("homelab", wp.OP_REF_HOSTS["homelab"])
    finally:
        wp.subprocess.run = original

    check("one ssh invocation", len(seen), 1)
    argv = seen[0]
    check("ssh target", argv[-2], "homelab")
    check("remote command sources the profile",
          argv[-1].startswith("[ -r ~/.profile ] && . ~/.profile;"), True)
    check("remote command is still the no-op probe",
          argv[-1].endswith("op run --env-file=.env.tpl -- true"), True)


def test_a_missing_item_is_still_reported() -> None:
    """Faithfulness must not cost the probe its verdict."""
    original = wp.subprocess.run
    wp.subprocess.run = lambda argv, **k: wp.subprocess.CompletedProcess(
        argv, 1, stdout="",
        stderr="[ERROR] could not resolve item UUID for item gone: could not "
               "find item gone in vault XXXX\n")
    try:
        events, reachable = wp.poll_op_refs("homelab", wp.OP_REF_HOSTS["homelab"])
    finally:
        wp.subprocess.run = original
    check("reachable", reachable, True)
    check("one item named", [e["external_id"] for e in events], ["gone"])


def test_the_kuma_trip_path_sources_the_profile_too() -> None:
    """triage.py's trip path runs from the 600s loop, so an unprofiled call
    burns the budget ~144 times a day and reports its own 429 as a failed trip."""
    seen: list[list[str]] = []
    original = triage.subprocess.run
    triage.subprocess.run = lambda argv, **k: (
        seen.append(list(argv)),
        triage.subprocess.CompletedProcess(argv, 0, stdout="", stderr=""))[1]
    try:
        triage._kuma_trip("check", "1")
    finally:
        triage.subprocess.run = original
    check("one ssh invocation", len(seen), 1)
    check("trip command sources the profile",
          seen[0][-1].startswith("[ -r ~/.profile ] && . ~/.profile;"), True)


def main() -> int:
    tests = [
        ("probe commands source the profile", test_every_probe_sources_the_profile_before_op_run),
        ("poll_op_refs sends the profiled command", test_poll_op_refs_sends_the_profiled_command),
        ("a missing item is still reported", test_a_missing_item_is_still_reported),
        ("the kuma trip path sources the profile", test_the_kuma_trip_path_sources_the_profile_too),
    ]
    for name, fn in tests:
        print(f"\n{name}")
        fn()
    if failures:
        print(f"\n{len(failures)} failure(s):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("\nall cases as expected")
    return 0


if __name__ == "__main__":
    sys.exit(main())
