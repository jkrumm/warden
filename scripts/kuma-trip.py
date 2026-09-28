"""kuma-trip — the synthetic trip's hands on Uptime Kuma (state-log §103).

Runs ON the homelab server, never on the mini: triage.py pipes this file to
`uptime-kuma/.venv/bin/python -` over `ssh homelab`, inside homelab's own
`op run --env-file=.env.tpl` (the same door `make uk-sync` uses), and reads one
JSON line back. It owns no policy: triage.py decides when to trip and what a
result means; this only touches Kuma, and only monitors it created itself.

    start <monitor name> <tag>  clone the LIVE monitor's detection config into a
                                shadow `warden-trip:<tag>` push monitor with NO
                                notification provider, verify that, arm it with
                                one UP push, print its id and window
    check <shadow id>           has the shadow gone DOWN since it was armed?
    stop <shadow id>            delete the shadow (heartbeats go with it)
    sweep [<keep id> ...]       delete every `warden-trip:` monitor not kept

The shadow is the whole safety property: the productive monitor is never
touched, so its notification (Slack - Alerts, `isDefault`) can never page, and
a monitor created through the API carries no provider unless one is passed —
checked after creation, and the shadow is deleted before arming if one stuck.
"""

from __future__ import annotations

import json
import os
import sys
import time
import urllib.request

from uptime_kuma_api import MonitorType, UptimeKumaApi

KUMA_URL = "http://localhost:3010"
SHADOW_PREFIX = "warden-trip:"


def _out(**kw: object) -> None:
    print(json.dumps(kw))


def _status(beat: dict) -> int:
    st = beat.get("status")
    return int(getattr(st, "value", st))


def _is_shadow(monitor: dict) -> bool:
    return str(monitor.get("name") or "").startswith(SHADOW_PREFIX)


def start(api: UptimeKumaApi, name: str, tag: str) -> None:
    live = next((m for m in api.get_monitors() if m.get("name") == name), None)
    if live is None:
        _out(ok=False, error=f"no live monitor named {name!r}")
        return
    mtype = getattr(live.get("type"), "value", live.get("type"))
    if mtype != "push":
        _out(ok=False, gap=f"monitor type {mtype!r}: no residue-free synthetic violation (push only)")
        return
    if live.get("upsideDown"):
        _out(ok=False, gap="upside-down push monitor: silence is its UP state, a silence trip proves nothing")
        return
    window = {
        "interval": int(live.get("interval") or 60),
        "retryInterval": int(live.get("retryInterval") or live.get("interval") or 60),
        "maxretries": int(live.get("maxretries") or 0),
    }
    created = api.add_monitor(type=MonitorType.PUSH, name=f"{SHADOW_PREFIX}{tag}",
                              notificationIDList=[], **window)
    shadow_id = created["monitorID"]
    shadow = api.get_monitor(shadow_id)
    if shadow.get("notificationIDList"):
        api.delete_monitor(shadow_id)
        _out(ok=False, error="a notification provider attached itself to the shadow — deleted, not armed")
        return
    token = shadow.get("pushToken")
    urllib.request.urlopen(f"{KUMA_URL}/api/push/{token}?status=up&msg=warden-trip-arm", timeout=10).read()
    _out(ok=True, shadowId=shadow_id, armedAt=time.time(), **window)


def check(api: UptimeKumaApi, shadow_id: int) -> None:
    monitor = next((m for m in api.get_monitors() if m.get("id") == shadow_id), None)
    if monitor is None or not _is_shadow(monitor):
        _out(ok=True, exists=False, down=False)
        return
    beats = api.get_monitor_beats(shadow_id, 24)
    _out(ok=True, exists=True, down=any(_status(b) == 0 for b in beats), beats=len(beats))


def stop(api: UptimeKumaApi, shadow_id: int) -> None:
    monitor = next((m for m in api.get_monitors() if m.get("id") == shadow_id), None)
    if monitor is not None and _is_shadow(monitor):
        api.delete_monitor(shadow_id)
    gone = not any(m.get("id") == shadow_id for m in api.get_monitors())
    _out(ok=gone, removed=gone)


def sweep(api: UptimeKumaApi, keep: set[int]) -> None:
    removed = []
    for monitor in api.get_monitors():
        if _is_shadow(monitor) and monitor.get("id") not in keep:
            api.delete_monitor(monitor["id"])
            removed.append(monitor["id"])
    _out(ok=True, removed=removed)


def main(argv: list[str]) -> int:
    if not argv:
        _out(ok=False, error="usage: start <name> <tag> | check <id> | stop <id> | sweep [<keep id> ...]")
        return 2
    api = UptimeKumaApi(KUMA_URL, timeout=30)
    try:
        api.login(os.environ.get("KUMA_USER", "jkrumm"), os.environ["UPTIME_KUMA_PASSWORD"])
        verb, args = argv[0], argv[1:]
        if verb == "start" and len(args) == 2:
            start(api, args[0], args[1])
        elif verb in ("check", "stop") and len(args) == 1:
            (check if verb == "check" else stop)(api, int(args[0]))
        elif verb == "sweep":
            sweep(api, {int(a) for a in args})
        else:
            _out(ok=False, error=f"bad invocation: {argv!r}")
            return 2
    except Exception as e:  # noqa: BLE001 — one JSON line is the whole contract
        _out(ok=False, error=f"{type(e).__name__}: {e}")
        return 1
    finally:
        api.disconnect()
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
