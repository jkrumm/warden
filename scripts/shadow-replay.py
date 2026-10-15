#!/usr/bin/env python3
"""Replay labelled ledger history through the decision models (Wave 6 of the OpenCode v2 plan).

Ground truth is the ledger's own outcome for items that went through intake:
  closed(duplicate) with duplicate_of -> attach (target = duplicate_of)
  closed(fixed_by)                    -> fixed_by
  closed(ignored)                     -> ignore
  everything that reached triaged or later (an implement/dispatch job, or a post-triage state) -> new
Each item is rebuilt as it stood AT ITS CREATION (lifecycle/shadow.build_triage(as_of=True)): the
candidates are the item's own repo's items as of then. Caveats, written down because they bias the
number: candidates are limited to the item's true repo (the live path may offer every repo), the
event payload is the current one, and `new` items are a sample (--new-sample).

    python3 scripts/shadow-replay.py [--models a,b] [--new-sample 40] [--json]

Prints per model: n, action agreement, attach-target agreement, cost, latency, and agreement by
confidence bucket. Read-only on the ledger; spends decision-model tokens (fractions of a cent).
"""

from __future__ import annotations

import argparse
import datetime as dt
import json
import sys
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

import ledger  # noqa: E402
from clients import agent_gateway  # noqa: E402
from lifecycle import shadow  # noqa: E402
from loop import core  # noqa: E402

DEFAULT_MODELS = ["pplx-decider-v1-27b", "decision-1-eu", "decision-1", "clef-eu"]
SMALL_BUDGET = {"clef": 5000, "clef-eu": 5000, "clef-flash": 5000}
NEW_STATES = ("triaged", "working", "merging", "verifying", "needs_decision", "failed", "fixed")


PREFILTER_NOTE = "matched the policy%"


def labelled(conn, new_sample: int, *, legacy: bool = False):
    """The labelled replay set. `legacy` is the Wave 6 set (33 items, kept to compare against the table
    in docs/decision-shadow.md). The default (Wave 11) is faithful to what triage actually sees:

    - ignore: alerts the triage job (or the owner) closed as noise, repo-less ones included. Alerts the
      policy ignore list closes before triage ('matched the policy ignore list') are labelled
      `ignore*` — they never reach a model live, so they are reported apart. Human-origin ignores are
      aborted tasks, not noise, and are dropped.
    - attach / fixed_by: closed(duplicate with duplicate_of) / closed(fixed_by), any origin.
    - new: alert and issue items that were worked to a fix (resolved, or any post-triage state)."""
    if legacy:
        rows = conn.execute(
            "SELECT ti.*, CASE WHEN ti.close_reason='duplicate' AND ti.duplicate_of IS NOT NULL THEN 'attach' "
            "WHEN ti.close_reason='fixed_by' THEN 'fixed_by' WHEN ti.close_reason='ignored' THEN 'ignore' END AS truth "
            "FROM triage_items ti WHERE ti.state='closed' AND ti.close_reason IN ('duplicate','fixed_by','ignored') "
            "AND ti.repo IS NOT NULL").fetchall()
        news = conn.execute(
            f"SELECT ti.*, 'new' AS truth FROM triage_items ti WHERE ti.repo IS NOT NULL "
            f"AND ti.state IN ({','.join('?' * len(NEW_STATES))}) AND ti.origin != 'human' "
            f"ORDER BY ti.event_id DESC LIMIT ?", (*NEW_STATES, new_sample)).fetchall()
        return [*rows, *news]
    rows = conn.execute(
        "SELECT ti.*, CASE WHEN ti.close_reason='duplicate' AND ti.duplicate_of IS NOT NULL THEN 'attach' "
        "WHEN ti.close_reason='fixed_by' THEN 'fixed_by' "
        "WHEN ti.close_reason='ignored' AND ti.note LIKE ? THEN 'ignore*' "
        "WHEN ti.close_reason='ignored' THEN 'ignore' END AS truth "
        "FROM triage_items ti WHERE ti.state='closed' AND ti.close_reason IN ('duplicate','fixed_by','ignored') "
        "AND NOT (ti.close_reason='ignored' AND ti.origin='human')", (PREFILTER_NOTE,)).fetchall()
    news = conn.execute(
        f"SELECT ti.*, 'new' AS truth FROM triage_items ti WHERE ti.repo IS NOT NULL "
        f"AND (ti.state IN ({','.join('?' * len(NEW_STATES))}) OR (ti.state='closed' AND ti.close_reason='resolved')) "
        f"AND ti.origin IN ('alert', 'github_issue') ORDER BY ti.event_id DESC LIMIT ?",
        (*NEW_STATES, new_sample)).fetchall()
    return [*rows, *news]


def candidates_for(item, event, legacy: bool) -> list[str]:
    if legacy:
        return [item["repo"]]
    label = item["repo"] if item["origin"] in ("github_issue", "human") else shadow._intake.route_by_label(item, event)
    return [label] if label else shadow._intake.known_repos()


def one(conn, item, model: str, legacy: bool = False, signals: bool = True):
    event = core.get_event(conn, item["event_id"])
    when = core.parse_ts(item["created_at"])
    if event is None or when is None:
        return None
    built = shadow.build_triage(conn, item, event, candidates_for(item, event, legacy), when, as_of=True,
                                budget=SMALL_BUDGET.get(model), **({} if signals else {"signals": False}))
    if built is None:
        return None
    state, questions, _shown, _info = built
    job = agent_gateway.submit_decide(state=state, questions=questions, model=model)
    job = agent_gateway.wait(job["id"], timeout_s=300, interval_s=1)
    result = (job or {}).get("result")
    if not job or job.get("status") != "done" or not isinstance(result, dict):
        return {"error": (job or {}).get("error", "no job")}
    dec = shadow.decision_triage(result["answers"])
    return {"dec": dec, "cost": (result.get("usage") or {}).get("costUsd") or 0, "ms": result.get("latencyMs") or 0}


def run_model(items, model: str, legacy: bool = False, signals: bool = True):
    # one connection per thread (sqlite3 objects are not shareable)
    conn = ledger.connect(migrate=False)
    stats = {"model": model, "n": 0, "errors": 0, "action": 0, "attach_n": 0, "attach_target": 0, "cost": 0.0, "ms": 0,
             "byConfidence": {}, "confusion": {}, "errorReasons": [], "misses": []}
    for item in items:
        out = one(conn, item, model, legacy, signals)
        if out is None:
            continue
        if "error" in out:
            stats["errors"] += 1
            stats["errorReasons"].append(str(out["error"])[:120])
            continue
        dec, truth = out["dec"], item["truth"]
        stats["n"] += 1
        hit = dec.get("action") == truth.rstrip("*")
        stats["action"] += int(hit)
        if not hit:
            stats["misses"].append({"event": item["event_id"], "truth": truth, "got": dec.get("action"), "conf": dec.get("confidence")})
        key = f"{truth}->{dec.get('action')}"
        stats["confusion"][key] = stats["confusion"].get(key, 0) + 1
        if truth == "attach":
            stats["attach_n"] += 1
            stats["attach_target"] += int(hit and dec.get("item") == item["duplicate_of"])
        stats["cost"] += out["cost"]
        stats["ms"] += out["ms"]
        b = stats["byConfidence"].setdefault(shadow._bucket(dec.get("confidence")), [0, 0])
        b[0] += 1
        b[1] += int(hit)
    return stats


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--models", default=",".join(DEFAULT_MODELS))
    ap.add_argument("--new-sample", type=int, default=60)
    ap.add_argument("--json", action="store_true")
    ap.add_argument("--legacy", action="store_true", help="the Wave 6 33-item set and candidate rule")
    ap.add_argument("--no-signals", action="store_true", help="omit the Wave 11 history blocks")
    args = ap.parse_args()
    conn = ledger.connect(migrate=False)
    items = labelled(conn, args.new_sample, legacy=args.legacy)
    counts: dict[str, int] = {}
    for i in items:
        counts[i["truth"]] = counts.get(i["truth"], 0) + 1
    models = args.models.split(",")
    with ThreadPoolExecutor(max_workers=len(models)) as pool:
        results = list(pool.map(lambda m: run_model(items, m, args.legacy, not args.no_signals), models))
    out = {"labels": counts, "models": results, "at": dt.datetime.now(dt.timezone.utc).isoformat()}
    if args.json:
        print(json.dumps(out, indent=2))
        return 0
    print(f"labelled items: {counts}")
    for r in results:
        n = r["n"] or 1
        print(f"{r['model']:<22} n={r['n']:<4} err={r['errors']:<3} action={r['action'] / n:.0%} "
              f"attach-target={r['attach_target']}/{r['attach_n']} cost=${r['cost']:.5f} avg={r['ms'] / n:.0f}ms "
              f"byConf={ {k: f'{v[1]}/{v[0]}' for k, v in sorted(r['byConfidence'].items())} }")
        print(f"  confusion (truth->decision): {dict(sorted(r['confusion'].items()))}")
        if r["errorReasons"]:
            print(f"  errors: {sorted(set(r['errorReasons']))}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
