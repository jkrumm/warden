#!/usr/bin/env python3
"""lifecycle/shadow.py — the decision-model shadow lane: context builder, budget, ledger writes,
agreement report. House convention: plain `test_*` functions, run with a bare interpreter."""

from __future__ import annotations

import datetime as dt
import json
import sys
import tempfile
import traceback
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "scripts"))

import ledger  # noqa: E402
from clients import agent_gateway  # noqa: E402
from clients.errors import RemoteError  # noqa: E402
from lifecycle import shadow  # noqa: E402
from loop import core  # noqa: E402

NOW = dt.datetime(2026, 10, 15, 12, 0, tzinfo=dt.timezone.utc)


def fresh() -> "ledger.sqlite3.Connection":
    path = Path(tempfile.mkdtemp()) / "warden.db"
    conn = ledger.connect(path, migrate=True)
    conn.row_factory = ledger.sqlite3.Row
    return conn


def add(conn, event_id: int, *, repo="argo", state="triaged", title="t", origin="alert", pr_url=None,
        root_cause=None, note=None, created="2026-10-10T00:00:00+00:00") -> None:
    conn.execute("INSERT INTO events(id, source, external_id, title, payload_json, first_seen) "
                 "VALUES (?,?,?,?,?,?)", (event_id, "uk", f"x{event_id}", title, "{}", created))
    conn.execute("INSERT INTO triage_items(event_id, signature, repo, state, occurrences, created_at, updated_at, "
                 "origin, pr_url, root_cause, note, last_seen, brief) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                 (event_id, f"sig{event_id}", repo, state, 1, created, created, origin, pr_url, root_cause, note,
                  created, ""))
    conn.commit()


def build(conn, event_id: int, **kw):
    return shadow.build_triage(conn, core.get_item(conn, event_id), core.get_event(conn, event_id), ["argo"], NOW, **kw)


def test_migration_creates_table() -> None:
    conn = fresh()
    assert conn.execute("SELECT count(*) FROM decision_shadow").fetchone()[0] == 0


def test_build_lists_candidates_and_questions() -> None:
    conn = fresh()
    add(conn, 1, title="nginx restart loop", state="working", root_cause="bad upstream", pr_url="https://x/pr/1")
    add(conn, 2, title="disk full", state="fixed", pr_url="https://x/pr/2")
    add(conn, 3, title="incoming", state="new")
    state, questions, shown, info = build(conn, 3)
    assert "#1 [argo] working: nginx restart loop" in state and "root cause: bad upstream" in state
    assert "#2 [argo] fixed: disk full" in state
    assert shown == {"open": [1], "fixed": [2]}
    assert set(questions) == {"action", "attach_target", "fixed_by_target"}
    assert "none" in questions["attach_target"]["options"] and "1" in questions["attach_target"]["options"]
    assert info["cut_open"] == 0


def test_private_repo_reaches_no_model() -> None:
    conn = fresh()
    add(conn, 1, repo="homelab-private", state="new")
    item, event = core.get_item(conn, 1), core.get_event(conn, 1)
    assert shadow.build_triage(conn, item, event, ["homelab-private"], NOW) is None


def test_budget_cuts_fixed_first_and_records_it() -> None:
    conn = fresh()
    for i in range(1, 6):
        add(conn, i, title=f"open item {i}", state="working")
    for i in range(6, 11):
        add(conn, i, title=f"fixed item {i}", state="fixed")
    add(conn, 11, state="new")
    full = build(conn, 11)
    cut = build(conn, 11, budget=len(full[0]) - 150)
    assert cut[3]["cut_fixed"] > 0 and cut[3]["cut_open"] == 0
    assert len(cut[2]["fixed"]) < len(full[2]["fixed"])


def test_as_of_uses_the_state_at_that_instant() -> None:
    conn = fresh()
    add(conn, 1, title="was open then", state="fixed", created="2026-10-01T00:00:00+00:00")
    conn.execute("INSERT INTO item_transitions(event_id, from_state, to_state, at) VALUES (1,'triaged','fixed','2026-10-12T00:00:00+00:00')")
    add(conn, 2, state="new", created="2026-10-05T00:00:00+00:00")
    early = dt.datetime(2026, 10, 5, tzinfo=dt.timezone.utc)
    built = shadow.build_triage(conn, core.get_item(conn, 2), core.get_event(conn, 2), ["argo"], early, as_of=True)
    assert built[2]["open"] == [1] and built[2]["fixed"] == []


def test_decision_and_compare() -> None:
    answers = {"action": {"choice": "attach", "confidence": 0.97},
               "attach_target": {"choice": "14", "confidence": 0.9}}
    dec = shadow.decision_triage(answers)
    assert dec == {"action": "attach", "confidence": 0.97, "item": 14, "target_confidence": 0.9}
    assert shadow.compare_triage({"action": "attach", "item": 14}, dec) == {"action": True, "target": True}
    assert shadow.compare_triage({"action": "attach", "item": 12}, dec) == {"action": True, "target": False}
    assert shadow.compare_triage({"action": "new"}, dec) == {"action": False}
    none = shadow.decision_triage({"action": {"choice": "attach", "confidence": 0.5}, "attach_target": {"choice": "none", "confidence": 0.5}})
    assert "item" not in none
    assert shadow.compare_sweep([3], {"item_3": {"value": True}, "item_4": {"value": True}}, [3, 4]) == {"agree": 1, "differ": 1}


def test_submit_settle_report_round_trip() -> None:
    conn = fresh()
    jobs: dict[str, dict] = {}
    orig_submit, orig_get = agent_gateway.submit_decide, agent_gateway.get
    agent_gateway.submit_decide = lambda **kw: jobs.setdefault("d1", {"id": "d1", "kw": kw}) and {"id": "d1"}  # type: ignore
    agent_gateway.get = lambda job_id: {"id": job_id, "status": "done", "result": {  # type: ignore
        "model": "pplx-decider-v1-27b", "latencyMs": 800, "usage": {"costUsd": 0.00002},
        "answers": {"action": {"choice": "new", "confidence": 0.99}}}}
    try:
        add(conn, 1, state="new")
        built = build(conn, 1)
        shadow.submit(conn, shadow.KIND_TRIAGE, 1, "llmjob", built, NOW)
        shadow.record_llm(conn, shadow.KIND_TRIAGE, 1, "llmjob", {"action": "new"})
        shadow.settle(conn, NOW)
    finally:
        agent_gateway.submit_decide, agent_gateway.get = orig_submit, orig_get
    row = conn.execute("SELECT * FROM decision_shadow").fetchone()
    assert row["status"] == "recorded" and row["model"] == "pplx-decider-v1-27b"
    rep = shadow.report(conn)
    assert rep["triageByConfidence"]["≥0.95"] == {"n": 1, "action": 1, "target_n": 0, "target": 0}
    assert rep["costUsd"] == 0.00002


def test_submit_failure_is_swallowed() -> None:
    conn = fresh()
    orig = agent_gateway.submit_decide

    def boom(**_kw):
        raise RemoteError("down")

    agent_gateway.submit_decide = boom  # type: ignore
    try:
        add(conn, 1, state="new")
        shadow.submit(conn, shadow.KIND_TRIAGE, 1, "j", build(conn, 1), NOW)
    finally:
        agent_gateway.submit_decide = orig
    assert conn.execute("SELECT count(*) FROM decision_shadow").fetchone()[0] == 0


def test_sweep_of_a_private_repo_builds_nothing() -> None:
    assert shadow.build_sweep(repo="vps-private", pr_title="t", pr_body="b", diff="d", candidates=[]) is None
    assert shadow.build_sweep(repo="argo", pr_title="t", pr_body="b", diff="d",
                              candidates=[{"id": 1, "state": "triaged", "title": "x"}]) is not None


def test_settle_and_record_never_raise_and_stale_rows_give_up() -> None:
    conn = fresh()
    orig = agent_gateway.get
    agent_gateway.get = lambda job_id: {"id": job_id, "status": "running"}  # type: ignore
    try:
        add(conn, 1, state="new")
        conn.execute("INSERT INTO decision_shadow(kind,event_id,llm_job,decision_job,status,created_at) "
                     "VALUES ('triage',1,'l','d','submitted',?)", ("2026-10-10T00:00:00+00:00",))
        conn.commit()
        shadow.settle(conn, NOW)  # running for days -> failed, no raise
        assert conn.execute("SELECT status FROM decision_shadow").fetchone()[0] == "failed"
        conn.close()
        shadow.settle(conn, NOW)  # closed connection: swallowed
        shadow.record_llm(conn, shadow.KIND_TRIAGE, 1, "l", {"action": "new"})
    finally:
        agent_gateway.get = orig


def test_settled_rows_past_retention_are_pruned() -> None:
    conn = fresh()
    orig = agent_gateway.get
    agent_gateway.get = lambda job_id: {"id": job_id, "status": "running"}  # type: ignore
    try:
        add(conn, 1, state="new")
        rows = [("recorded", "2026-06-01T00:00:00+00:00"), ("failed", "2026-06-01T00:00:00+00:00"),
                ("recorded", "2026-10-01T00:00:00+00:00"), ("submitted", None)]
        for i, (status, settled) in enumerate(rows, start=1):
            conn.execute("INSERT INTO decision_shadow(kind,event_id,llm_job,decision_job,status,created_at,settled_at) "
                         "VALUES ('triage',1,?,?,?,?,?)", (f"l{i}", f"d{i}", status, "2026-10-15T06:00:00+00:00", settled))
        conn.commit()
        shadow.settle(conn, NOW)
    finally:
        agent_gateway.get = orig
    left = [r[0] for r in conn.execute("SELECT status FROM decision_shadow ORDER BY id").fetchall()]
    assert left == ["recorded", "submitted"], left


def test_prune_deletes_one_batch_per_settle() -> None:
    conn = fresh()
    orig = agent_gateway.get
    agent_gateway.get = lambda job_id: {"id": job_id, "status": "running"}  # type: ignore
    orig_batch = shadow.PRUNE_BATCH_SIZE
    shadow.PRUNE_BATCH_SIZE = 2
    try:
        add(conn, 1, state="new")
        # Five old settled rows: one settle() deletes at most one batch (2), so the backlog drains
        # across passes instead of in one open transaction holding SQLite's single-writer lock.
        for i in range(5):
            conn.execute("INSERT INTO decision_shadow(kind,event_id,llm_job,decision_job,status,created_at,settled_at) "
                         "VALUES ('triage',1,?,?,?,?,?)",
                         (f"l{i}", f"d{i}", "recorded", "2026-10-15T06:00:00+00:00", "2026-06-01T00:00:00+00:00"))
        conn.commit()
        shadow.settle(conn, NOW)
        assert conn.execute("SELECT count(*) FROM decision_shadow").fetchone()[0] == 3
        shadow.settle(conn, NOW)
        assert conn.execute("SELECT count(*) FROM decision_shadow").fetchone()[0] == 1
        shadow.settle(conn, NOW)
        assert conn.execute("SELECT count(*) FROM decision_shadow").fetchone()[0] == 0
    finally:
        agent_gateway.get = orig
        shadow.PRUNE_BATCH_SIZE = orig_batch


def test_non_numeric_target_choice_is_no_target() -> None:
    dec = shadow.decision_triage({"action": {"choice": "attach", "confidence": 0.9},
                                  "attach_target": {"choice": "weird", "confidence": 0.9}})
    assert "item" not in dec


def main() -> int:
    tests = [(n, f) for n, f in sorted(globals().items()) if n.startswith("test_") and callable(f)]
    failures = []
    for name, fn in tests:
        try:
            fn()
        except Exception:
            failures.append(f"{name}\n{traceback.format_exc()}")
    print(f"{len(tests) - len(failures)}/{len(tests)} passed")
    for f in failures:
        print(f)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
