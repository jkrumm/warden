"""shadow — the decision model next to the LLM triage (Wave 6 of the OpenCode v2 plan).

For every triage job and every fixed-by sweep job, the same material is also put to an IU decision
model through agent-gateway's `decide` job: typed answers with calibrated confidence, no text, no
search. Both answers land in `decision_shadow` and nothing reads that table to decide anything:
it is the evidence the promotion rule (docs/decision-shadow.md) is judged on.

The decision model sees ONLY the `state` text built here, so this module is the context builder:
the incoming item, every same-repo candidate (title, summary, root cause, state, PR, last activity),
the recently merged/fixed work, and the repo's one-line purpose, cut to a character budget with
what was cut recorded. Everything is best-effort: a shadow failure is a stderr line and a row marked
`failed`, never a strike on the item.

Pure builders + three ledger writers (submit / record_llm / settle) + the agreement report. No
network except through `clients.agent_gateway`.
"""

from __future__ import annotations

import datetime as dt
import json
import os
import sqlite3
import sys
from pathlib import Path
from typing import Any

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))  # `python3 scripts/lifecycle/shadow.py report`

from clients import agent_gateway as _agent_gateway
from clients.errors import WardenError
from lifecycle import intake as _intake
from loop import core

# Cut order when the state is over budget: the event, the instructions and the repo purposes
# (PURPOSE_CHARS each) are fixed cost and never cut; the oldest fixed items go first, then the
# oldest open items.
STATE_BUDGET_CHARS = int(os.environ.get("WARDEN_SHADOW_BUDGET_CHARS", "24000"))
PURPOSE_CHARS = 300
MAX_TARGET_OPTIONS = 30
MAX_SWEEP_ITEMS = 16
NONE_OPTION = "none"

KIND_TRIAGE = "triage"
KIND_SWEEP = "sweep"

TRIAGE_QUESTIONS_BASE: dict[str, Any] = {
    "action": {
        "type": "choice",
        "instructions": "What should happen to the incoming event?",
        "options": {
            "attach": "it is the same defect as one of the OPEN items listed",
            "fixed_by": "one of the RECENTLY FIXED items or merged PRs listed already fixes it",
            "new": "a real defect that none of the listed items covers",
            "ignore": "noise: a recovery notice, a test or canary message",
        },
    },
}


def _model() -> str | None:
    return os.environ.get("WARDEN_SHADOW_MODEL") or None


def _purpose(repo: str) -> str:
    text = _intake.repo_description(repo)
    return " ".join(text.split())[:PURPOSE_CHARS]


def _last_activity(row: sqlite3.Row) -> str:
    return str(row["last_seen"] or row["created_at"] or "")[:10]


# The item's state at instant ?: its last transition at or before it, `new` if it had none yet.
# Used only by the replay (as_of); live rows read `ti.state` directly.
_STATE_AT = ("COALESCE((SELECT t.to_state FROM item_transitions t WHERE t.event_id = ti.event_id AND t.at <= ? "
             "ORDER BY t.at DESC, t.id DESC LIMIT 1), 'new')")


def _open_rows(conn: sqlite3.Connection, repos: list[str], exclude: int,
               as_of: dt.datetime | None = None) -> list[sqlite3.Row]:
    repos = [r for r in repos if not _intake.is_private(r)]
    if not repos:
        return []
    marks = ",".join("?" * len(repos))
    states = ",".join("?" * len(_intake.OPEN_STATES))
    at = core.now_iso(as_of) if as_of else None
    state_expr = _STATE_AT if at else "ti.state"
    return conn.execute(
        f"SELECT ti.event_id, ti.repo, {state_expr} AS state, ti.root_cause, ti.note, ti.pr_url, ti.last_seen, "
        f"ti.created_at, e.title FROM triage_items ti JOIN events e ON e.id = ti.event_id WHERE ti.repo IN ({marks}) "
        f"AND {state_expr} IN ({states}) AND ti.event_id != ? {'AND ti.created_at <= ?' if at else ''} "
        f"ORDER BY ti.created_at DESC, ti.event_id DESC LIMIT ?",
        (*([at] if at else []), *repos, *([at] if at else []), *_intake.OPEN_STATES, exclude, *([at] if at else []),
         _intake.MAX_OPEN_ITEMS),
    ).fetchall()


def _fixed_rows(conn: sqlite3.Connection, repos: list[str], exclude: int, now: dt.datetime,
                as_of: bool = False) -> list[sqlite3.Row]:
    repos = [r for r in repos if not _intake.is_private(r)]
    if not repos:
        return []
    marks = ",".join("?" * len(repos))
    since = (now - dt.timedelta(days=_intake.FIXED_WINDOW_DAYS)).isoformat()
    done_at = ("COALESCE((SELECT MAX(t.at) FROM item_transitions t WHERE t.event_id = ti.event_id "
               "AND t.to_state IN ('fixed', 'closed')), ti.updated_at)")
    return conn.execute(
        f"SELECT ti.event_id, ti.repo, ti.state, ti.root_cause, ti.note, ti.pr_url, ti.last_seen, ti.created_at, "
        f"e.title, d.verdict_json FROM triage_items ti JOIN events e ON e.id = ti.event_id "
        f"LEFT JOIN dispatches d ON d.job_id = ti.implement_job WHERE ti.repo IN ({marks}) AND ti.event_id != ? "
        f"AND {done_at} >= ? {f'AND {done_at} <= ? ' if as_of else ''}"
        f"AND (ti.state='fixed' OR (ti.state='closed' AND ti.pr_url IS NOT NULL)) "
        f"ORDER BY {done_at} DESC, ti.event_id DESC LIMIT ?",
        (*repos, exclude, since, *([core.now_iso(now)] if as_of else []), _intake.MAX_FIXED_ITEMS),
    ).fetchall()


def _item_line(row: sqlite3.Row, *, fixed: bool) -> str:
    title = _intake._clip(row["title"], _intake.MAX_TITLE_CHARS)
    line = f"#{row['event_id']} [{row['repo']}] {'fixed' if fixed else row['state']}: {title}"
    if row["root_cause"]:
        line += f" | root cause: {_intake._clip(row['root_cause'], 120)}"
    if row["note"]:
        line += f" | summary: {_intake._clip(row['note'], _intake.MAX_NOTE_CHARS)}"
    if row["pr_url"]:
        line += f" | PR: {row['pr_url']}"
    if fixed:
        pr_title = (_intake._payload(row["verdict_json"]) or {}).get("prTitle")
        if pr_title:
            line += f" | PR title: {_intake._clip(pr_title, _intake.MAX_TITLE_CHARS)}"
    return line + f" | last activity: {_last_activity(row)}"


def _fit(blocks: list[tuple[str, list[str]]], fixed_chars: int, budget: int) -> tuple[list[tuple[str, list[str]]], dict[str, int]]:
    """Drop lines from the tail of the cuttable blocks (listed oldest-cut-first) until the total fits.
    Returns the kept blocks and how many lines each lost."""
    cut: dict[str, int] = {name: 0 for name, _ in blocks}
    kept = [(name, list(lines)) for name, lines in blocks]
    total = fixed_chars + sum(len(line) + 1 for _, lines in kept for line in lines)
    for name, lines in kept:
        while lines and total > budget:
            total -= len(lines.pop()) + 1
            cut[name] += 1
    return kept, cut


def build_triage(conn: sqlite3.Connection, item: sqlite3.Row, event: sqlite3.Row, candidates: list[str],
                 now: dt.datetime, *, as_of: bool = False, budget: int | None = None,
                 ) -> tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]] | None:
    """(state text, questions, shown ids, context info) for one incoming item, or None when the item
    must not reach a model at all (it, or its only candidate repo, is private). `as_of` rebuilds the
    candidate lists as they stood at `now` (the replay); `budget` overrides STATE_BUDGET_CHARS."""
    if not _intake.may_reach_model(item, candidates):
        return None
    excerpt = item["brief"] or json.dumps(_intake._payload(event["payload_json"]), ensure_ascii=False, sort_keys=True)
    head = "\n".join([
        "# Incoming event",
        f"source: {event['source']} — {_intake._SOURCE_NOTES.get(event['source'], 'an alert signal')}",
        f"origin: {item['origin']}   occurrences: {item['occurrences']}",
        f"title: {_intake._clip(event['title'], _intake.MAX_TITLE_CHARS)}",
        f"url: {event['url'] or '-'}",
        f"payload: {_intake._clip(excerpt, _intake.MAX_EXCERPT_CHARS)}",
    ])
    opens = _open_rows(conn, candidates, item["event_id"], now if as_of else None)
    fixed = _fixed_rows(conn, candidates, item["event_id"], now, as_of)
    purposes = [f"{name}: {_purpose(name)}" for name in candidates if not _intake.is_private(name)]
    blocks = [
        ("fixed", [_item_line(r, fixed=True) for r in fixed]),
        ("open", [_item_line(r, fixed=False) for r in opens]),
    ]
    blocks, cut = _fit(blocks, len(head) + sum(len(p) + 1 for p in purposes) + 200,
                      budget or STATE_BUDGET_CHARS)
    kept = dict(blocks)
    kept_open = opens[: len(kept["open"])]
    kept_fixed = fixed[: len(kept["fixed"])]
    state = "\n\n".join([
        head,
        "# Candidate repos (one line each)\n" + ("\n".join(purposes) or "(none)"),
        "# Open items\n" + ("\n".join(kept["open"]) or "(none)"),
        f"# Recently fixed (last {_intake.FIXED_WINDOW_DAYS} days)\n" + ("\n".join(kept["fixed"]) or "(none)"),
    ])
    questions: dict[str, Any] = {k: dict(v) for k, v in TRIAGE_QUESTIONS_BASE.items()}
    shown: dict[str, Any] = {"open": [], "fixed": []}
    for qid, rows, label, key in (("attach_target", kept_open[:MAX_TARGET_OPTIONS], "OPEN item", "open"),
                                  ("fixed_by_target", kept_fixed[:MAX_TARGET_OPTIONS], "RECENTLY FIXED item", "fixed")):
        if not rows:
            continue
        options = {str(r["event_id"]): _intake._clip(r["title"], 100) for r in rows}
        options[NONE_OPTION] = f"none of the listed {label}s"
        questions[qid] = {"type": "choice", "options": options,
                          "instructions": f"If the event relates to one of the listed {label}s, which one?"}
        shown[key] = [r["event_id"] for r in rows]
    info = {"chars": len(state), "cut_open": cut["open"], "cut_fixed": cut["fixed"]}
    return state, questions, shown, info


def build_sweep(*, repo: str, pr_title: str, pr_body: str, diff: str,
                candidates: list[dict[str, Any]]) -> tuple[str, dict[str, Any], dict[str, Any], dict[str, Any]] | None:
    """(state text, questions, shown ids, context info) for one fixed-by sweep: the merged PR and one
    boolean per waiting item (does the change fix this item?). None for a `-private` repo: the guard
    lives here, not at the call site, so no caller can send a private diff to a model."""
    if _intake.is_private(repo):
        return None
    shown_candidates = candidates[:MAX_SWEEP_ITEMS]
    lines = []
    for c in shown_candidates:
        line = f"#{c['id']} {c['state']}: {_intake._clip(c['title'], _intake.MAX_TITLE_CHARS)}"
        if c.get("root_cause"):
            line += f" | root cause: {_intake._clip(c['root_cause'], 120)}"
        if c.get("note"):
            line += f" | summary: {_intake._clip(c['note'], _intake.MAX_NOTE_CHARS)}"
        lines.append(line)
    budget_for_diff = max(2000, STATE_BUDGET_CHARS - 4000 - sum(len(x) + 1 for x in lines))
    clipped = diff[:budget_for_diff]
    state = "\n\n".join([
        f"# Merged pull request in {repo}",
        f"title: {_intake._clip(pr_title, _intake.MAX_TITLE_CHARS)}\nbody: {_intake._clip(pr_body, _intake.MAX_SWEEP_BODY_CHARS)}",
        "# Diff\n" + clipped,
        "# Items still waiting for a fix\n" + "\n".join(lines),
    ])
    questions = {
        f"item_{c['id']}": {"type": "boolean",
                            "instructions": f"Does the merged pull request fix the defect described in item #{c['id']}?"}
        for c in shown_candidates
    }
    info = {"chars": len(state), "cut_diff": max(0, len(diff) - len(clipped)),
            "cut_items": max(0, len(candidates) - len(shown_candidates))}
    return state, questions, {"items": [c["id"] for c in shown_candidates]}, info


# ── ledger writes — every one best-effort ─────────────────────────────────────────────────────

def submit(conn: sqlite3.Connection, kind: str, event_id: int, llm_job: str, built: tuple | None,
           now: dt.datetime) -> None:
    """Record a shadow row for `llm_job` and submit its `decide` job. `built` is a build_*() result,
    or None (private repo: nothing may reach a model; no row)."""
    if built is None:
        return
    state, questions, shown, info = built
    try:
        job = _agent_gateway.submit_decide(state=state, questions=questions, model=_model())
        conn.execute(
            "INSERT OR IGNORE INTO decision_shadow(kind, event_id, llm_job, decision_job, status, shown_json, "
            "context_json, created_at) VALUES (?,?,?,?,?,?,?,?)",
            (kind, event_id, llm_job, job["id"], "submitted", json.dumps(shown), json.dumps(info), core.now_iso(now)),
        )
        conn.commit()
    except Exception as e:  # noqa: BLE001 — the shadow lane must never touch the real one
        print(f"shadow: {kind} decide submit failed for #{event_id}: {e}", file=sys.stderr)


def record_llm(conn: sqlite3.Connection, kind: str, event_id: int, llm_job: str, answer: Any) -> None:
    """The LLM's folded answer, next to the decision model's. `answer` is the validated triage result
    (`{action, item?, pr?}`) or the sweep's list of matched item ids."""
    try:
        conn.execute("UPDATE decision_shadow SET llm_json=? WHERE kind=? AND event_id=? AND llm_job=?",
                     (json.dumps(answer), kind, event_id, llm_job))
        conn.commit()
    except Exception as e:  # noqa: BLE001 — the shadow lane must never touch the real one
        print(f"shadow: could not record the LLM answer for #{event_id}: {e}", file=sys.stderr)


SETTLE_GIVE_UP_HOURS = 24
SETTLED_RETENTION_DAYS = 90


def settle(conn: sqlite3.Connection, now: dt.datetime) -> None:
    """Fold every finished `decide` job onto its row; a job that failed, vanished or stayed
    non-terminal SETTLE_GIVE_UP_HOURS marks the row `failed` with the reason. A job still running
    is left for the next pass. Settled rows older than SETTLED_RETENTION_DAYS are pruned.
    Never raises: a shadow failure is a stderr line."""
    try:
        rows = conn.execute("SELECT id, decision_job, created_at FROM decision_shadow WHERE status='submitted'").fetchall()
        for row in rows:
            _settle_row(conn, row, now)
        _prune_settled(conn, now)
    except Exception as e:  # noqa: BLE001 — the shadow lane must never touch the real one
        print(f"shadow: settle failed: {e}", file=sys.stderr)


def _prune_settled(conn: sqlite3.Connection, now: dt.datetime) -> None:
    cutoff = core.now_iso(now - dt.timedelta(days=SETTLED_RETENTION_DAYS))
    conn.execute("DELETE FROM decision_shadow WHERE status IN ('recorded', 'failed') AND settled_at < ?", (cutoff,))
    conn.commit()


def _settle_row(conn: sqlite3.Connection, row: sqlite3.Row, now: dt.datetime) -> None:
    try:
        job = _agent_gateway.get(row["decision_job"])
    except WardenError as e:
        print(f"shadow: could not poll decide job {row['decision_job']}: {e}", file=sys.stderr)
        return
    result = job.get("result") if job else None
    if job is not None and job.get("status") == "done" and isinstance(result, dict) and "answers" in result:
        usage = {"model": result.get("model"), "latencyMs": result.get("latencyMs"),
                 "truncatedChars": result.get("truncatedChars"), **(result.get("usage") or {})}
        conn.execute("UPDATE decision_shadow SET status='recorded', decision_json=?, usage_json=?, model=?, "
                     "settled_at=? WHERE id=?",
                     (json.dumps(result["answers"]), json.dumps(usage), result.get("model"), core.now_iso(now), row["id"]))
    elif job is not None and job.get("status") not in _agent_gateway.TERMINAL:
        created = core.parse_ts(row["created_at"])
        if created is None or now - created < dt.timedelta(hours=SETTLE_GIVE_UP_HOURS):
            return
        conn.execute("UPDATE decision_shadow SET status='failed', error=?, settled_at=? WHERE id=?",
                     (f"still {job.get('status')} after {SETTLE_GIVE_UP_HOURS}h", core.now_iso(now), row["id"]))
    else:
        why = (job.get("error") if job else None) or "job missing or without answers"
        conn.execute("UPDATE decision_shadow SET status='failed', error=?, settled_at=? WHERE id=?",
                     (str(why)[:300], core.now_iso(now), row["id"]))
    conn.commit()


# ── agreement ─────────────────────────────────────────────────────────────────────────────────

def decision_triage(answers: dict[str, Any]) -> dict[str, Any]:
    """The decision model's triage answer in the LLM answer's own shape: `{action, item?}` plus the
    confidences. A none target is no target."""
    action_answer = answers.get("action") or {}
    action = action_answer.get("choice")
    out: dict[str, Any] = {"action": action, "confidence": action_answer.get("confidence")}
    qid = {"attach": "attach_target", "fixed_by": "fixed_by_target"}.get(action)
    target = answers.get(qid) if qid else None
    if target and str(target.get("choice")).isdigit():
        out["item"] = int(target["choice"])
        out["target_confidence"] = target.get("confidence")
    return out


def compare_triage(llm: dict[str, Any], decision: dict[str, Any]) -> dict[str, bool]:
    """Action agreement, and — only where both chose attach/fixed_by — target agreement."""
    same_action = llm.get("action") == decision.get("action")
    out = {"action": same_action}
    if same_action and llm.get("action") in ("attach", "fixed_by"):
        out["target"] = llm.get("item") == decision.get("item")
    return out


def compare_sweep(llm_matches: list[int], answers: dict[str, Any], shown: list[int]) -> dict[str, int]:
    """Per shown item: did both say fixed / both say not? Counts of agreeing and differing items."""
    agree = differ = 0
    for item_id in shown:
        yes = bool((answers.get(f"item_{item_id}") or {}).get("value"))
        if yes == (item_id in llm_matches):
            agree += 1
        else:
            differ += 1
    return {"agree": agree, "differ": differ}


def _bucket(confidence: float | None) -> str:
    if confidence is None:
        return "n/a"
    return "≥0.95" if confidence >= 0.95 else "≥0.8" if confidence >= 0.8 else "<0.8"


def report(conn: sqlite3.Connection) -> dict[str, Any]:
    """Agreement over every settled triage row that has both answers, by decision confidence bucket,
    plus the sweep totals and the summed decision-model cost. The input to the promotion rule."""
    rows = conn.execute("SELECT * FROM decision_shadow WHERE status='recorded' AND llm_json IS NOT NULL").fetchall()
    triage: dict[str, dict[str, int]] = {}
    sweep = {"agree": 0, "differ": 0}
    cost = 0.0
    models: dict[str, int] = {}
    for r in rows:
        answers = json.loads(r["decision_json"])
        llm = json.loads(r["llm_json"])
        usage = json.loads(r["usage_json"] or "{}")
        cost += float(usage.get("costUsd") or 0)
        models[r["model"] or "?"] = models.get(r["model"] or "?", 0) + 1
        if r["kind"] == KIND_TRIAGE and isinstance(llm, dict):
            dec = decision_triage(answers)
            cmp = compare_triage(llm, dec)
            b = triage.setdefault(_bucket(dec.get("confidence")), {"n": 0, "action": 0, "target_n": 0, "target": 0})
            b["n"] += 1
            b["action"] += int(cmp["action"])
            if "target" in cmp:
                b["target_n"] += 1
                b["target"] += int(cmp["target"])
        elif r["kind"] == KIND_SWEEP and isinstance(llm, list):
            c = compare_sweep(llm, answers, json.loads(r["shown_json"]).get("items", []))
            sweep["agree"] += c["agree"]
            sweep["differ"] += c["differ"]
    pending = conn.execute("SELECT status, count(*) c FROM decision_shadow GROUP BY status").fetchall()
    return {"triageByConfidence": triage, "sweep": sweep, "costUsd": round(cost, 6), "models": models,
            "rowsByStatus": {p["status"]: p["c"] for p in pending}}


def main(argv: list[str]) -> int:
    """`python3 scripts/lifecycle/shadow.py report` — the agreement report as JSON (read-only)."""
    import ledger
    if argv[:1] != ["report"]:
        print("usage: shadow.py report", file=sys.stderr)
        return 2
    print(json.dumps(report(ledger.connect(migrate=False)), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
