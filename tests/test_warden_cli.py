#!/usr/bin/env python3
"""Black-box regression suite for `scripts/warden` — the Python CLI that
replaced `scripts/hermes-cc.sh` (Wave 5.2/5.3).

Every test runs the real launcher (`scripts/warden`, exec'ing
`scripts/warden.py`) as a subprocess, against a throwaway `HOME`, a fresh
migrated ledger, a sandboxed dispatch policy, and a single in-process
`stubs.StubServer` per test standing in for sideclaw / GitHub / Slack — never
a real network call, never the developer's own `~/.warden` or `~/.hermes`.

This is a deliberately reduced port of the retired tests/test_hermes_cc.py
(165 cases) and tests/test_dispatch_approval.py's CLI-facing half. Dropped
outright, because the CLI itself dropped them:
  - the `cancel` verb (sideclaw grew a real cancel endpoint; `abort` replaced
    it) and its `lost`/`queued` dispatch statuses.
  - `--confirm` as a dispatch flag and the old shell-out "the Approve button
    replays this exact command line" replay path — the Approve click now
    drains an intent in-process (see the click-path tests below) rather than
    re-invoking a CLI.
Replaced with: `abort`/`revert` verb coverage, and a click-path test that
exercises `scripts/intents.py --record`/`--drain` end to end against a
plan this suite mints.

Run: .venv/bin/python3 tests/test_warden_cli.py  (or: make test, from warden/)
"""

from __future__ import annotations

import datetime as dt
import json
import os
import re
import stat
import subprocess
import sys
import tempfile
import traceback
import uuid
from pathlib import Path
from typing import Any

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO / "scripts"))

import stubs  # noqa: E402
from clients import signer  # noqa: E402

from cryptography.hazmat.primitives import serialization  # noqa: E402
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey  # noqa: E402

WARDEN_BIN = REPO / "scripts" / "warden"
LEDGER_PY = REPO / "scripts" / "ledger.py"
INTENTS_PY = REPO / "scripts" / "intents.py"

VALID_BRIEF = "Investigate why the check job keeps failing."


# --- harness -------------------------------------------------------------------


class Harness:
    def __init__(self):
        self.tmp = Path(tempfile.mkdtemp(prefix="warden-cli-"))
        self.home = self.tmp / "home"
        self.home.mkdir()
        self.repos_root = self.tmp / "repos"
        self.repos_root.mkdir()
        self.secrets_run = self._write_secrets_run()
        self.backend_file = self._write_backend_file()
        self.pr_required_json = self._write_json(
            "pr-required.json", {"repos": []}
        )
        self.no_such_pubkey = self.tmp / "no-such.pub"
        self.repos_json = self._write_repos_json()
        self.triage_policy_json = self._write_triage_policy()

        for name in ("alpha", "gamma", "secretrepo", "capped"):
            self.make_repo(name)

    # -- fixtures --

    def make_repo(self, name: str) -> Path:
        p = self.repos_root / name
        (p / ".git").mkdir(parents=True, exist_ok=True)
        return p

    def _write_json(self, name: str, data: Any) -> Path:
        p = self.tmp / name
        p.write_text(json.dumps(data), encoding="utf-8")
        return p

    def _write_secrets_run(self) -> Path:
        p = self.tmp / "secrets-run"
        p.write_text("#!/bin/sh\necho stub-secret-token\nexit 0\n", encoding="utf-8")
        p.chmod(p.stat().st_mode | stat.S_IEXEC | stat.S_IXGRP | stat.S_IXOTH)
        return p

    def _write_backend_file(self) -> Path:
        return self._write_text("backend", "cache")

    def _write_text(self, name: str, text: str) -> Path:
        p = self.tmp / name
        p.write_text(text, encoding="utf-8")
        return p

    def _write_repos_json(self) -> Path:
        return self._write_json("dispatch-repos.json", {
            "root": str(self.repos_root),
            "defaultTier": "investigate",
            "deny": ["secretrepo"],
            "sensitive": ["secretrepo"],
            "tiers": {"implement": ["gamma"]},
        })

    def _write_triage_policy(self) -> Path:
        return self._write_json("triage-policy.json", {
            "repos": {
                "gamma": {"autoMergePaths": ["**"], "noCiRequired": True, "autoDeploy": False},
            }
        })

    def new_db(self) -> Path:
        db = self.tmp / f"db-{uuid.uuid4().hex[:8]}.db"
        subprocess.run(
            [sys.executable, str(LEDGER_PY), "--migrate", str(db)],
            check=True, capture_output=True, text=True,
        )
        return db

    def new_log(self, name: str) -> Path:
        return self.tmp / f"{name}.log"

    # -- env / run --

    def base_env(self, *, db: Path | None = None, sideclaw: str | None = None,
                 gh: str | None = None, slack: str | None = None,
                 log: Path | None = None, pubkey: Path | None = None) -> dict[str, str]:
        return {
            "HOME": str(self.home),
            "PATH": "/usr/bin:/bin:/usr/local/bin:/opt/homebrew/bin",
            "WARDEN_DB": str(db or self.new_db()),
            "WARDEN_DISPATCH_REPOS": str(self.repos_json),
            "WARDEN_TRIAGE_POLICY": str(self.triage_policy_json),
            "WARDEN_PR_REQUIRED_JSON": str(self.pr_required_json),
            "WARDEN_CLI_LOG": str(log or self.new_log("audit")),
            "WARDEN_SECRETS_RUN": str(self.secrets_run),
            "SECRETS_BACKEND_FILE": str(self.backend_file),
            "SLACK_BOT_TOKEN": "xoxb-stub-token",
            "WARDEN_SIDECLAW_BASE": sideclaw or f"http://127.0.0.1:{stubs.closed_port()}",
            "WARDEN_GH_API": gh or f"http://127.0.0.1:{stubs.closed_port()}",
            "WARDEN_SLACK_API": slack or f"http://127.0.0.1:{stubs.closed_port()}",
            "WARDEN_APPROVAL_PUBKEY": str(pubkey or self.no_such_pubkey),
        }

    def run(self, args: list[str], *, env: dict[str, str] | None = None,
            env_extra: dict[str, str] | None = None, stdin: str | None = "",
            timeout: float = 20) -> subprocess.CompletedProcess:
        e = dict(env if env is not None else self.base_env())
        if env_extra:
            e.update(env_extra)
        return subprocess.run(
            [str(WARDEN_BIN), *args], input=stdin, capture_output=True, text=True, env=e, timeout=timeout,
        )


def _json_or_fail(proc: subprocess.CompletedProcess) -> dict[str, Any]:
    try:
        return json.loads(proc.stdout)
    except json.JSONDecodeError as err:
        raise AssertionError(f"stdout is not one JSON object: {err}\nstdout={proc.stdout!r}\nstderr={proc.stderr!r}")


def _row(conn, sql: str, params: tuple = ()):
    return conn.execute(sql, params).fetchone()


def _connect(db: Path):
    import importlib.util
    spec = importlib.util.spec_from_file_location("ledger_for_cli_test", LEDGER_PY)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod.connect(db, migrate=False), mod


def _seed_dispatch(db: Path, job_id: str, *, tier="investigate", repo="alpha", status="done",
                    artifact_url=None, verdict_json=None, created_at=None, merged_at=None,
                    validation_status=None) -> None:
    conn, _ = _connect(db)
    now = created_at or dt.datetime.now(dt.timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO dispatches(job_id,tier,repo,brief,status,verdict_json,artifact_url,created_at,"
        "finished_at,merged_at,validation_status) VALUES(?,?,?,?,?,?,?,?,?,?,?)",
        (job_id, tier, repo, "some brief", status, verdict_json, artifact_url, now, now, merged_at,
         validation_status),
    )
    conn.commit()
    conn.close()


def _seed_item(db: Path, event_id: int, *, state: str, repo: str | None = None,
                dispatch_job=None, implement_job=None, validation_job=None) -> None:
    conn, _ = _connect(db)
    now = dt.datetime.now(dt.timezone.utc).isoformat()
    conn.execute(
        "INSERT INTO events(id,source,external_id,title,first_seen) VALUES(?,?,?,?,?)",
        (event_id, "test", str(event_id), "t", now),
    )
    conn.execute(
        "INSERT INTO triage_items(event_id,signature,repo,state,dispatch_job,implement_job,"
        "validation_job,created_at,updated_at) VALUES(?,?,?,?,?,?,?,?,?)",
        (event_id, "sig", repo, state, dispatch_job, implement_job, validation_job, now, now),
    )
    conn.commit()
    conn.close()


# --- closed verb set / usage --------------------------------------------------


def test_unknown_verb_is_usage_error_naming_valid_verbs():
    h = Harness()
    proc = h.run(["frobnicate"])
    assert proc.returncode == 64, proc
    assert "unknown verb" in proc.stderr and "dispatch" in proc.stderr, proc.stderr


def test_cancel_is_no_longer_a_known_verb():
    h = Harness()
    proc = h.run(["cancel", "job-1"])
    assert proc.returncode == 64, proc
    assert "unknown verb" in proc.stderr, proc.stderr


def test_bare_invocation_prints_help_and_exits_zero():
    h = Harness()
    proc = h.run([])
    assert proc.returncode == 0, proc
    assert "dispatch <repo>" in proc.stdout


def test_help_works_inside_a_claude_code_session():
    """help is not a mutating verb — it must never trip the recursion guard."""
    h = Harness()
    proc = h.run(["help"], env_extra={"CLAUDECODE": "1"})
    assert proc.returncode == 0, proc


# --- argument bounding ---------------------------------------------------------


def test_dispatch_with_no_repo_is_usage_error():
    h = Harness()
    proc = h.run(["dispatch"])
    assert proc.returncode == 64, proc


def test_status_with_no_job_id_is_usage_error():
    h = Harness()
    proc = h.run(["status"])
    assert proc.returncode == 64, proc


def test_merge_with_no_job_id_is_usage_error():
    h = Harness()
    proc = h.run(["merge"])
    assert proc.returncode == 64, proc


def test_unknown_flag_is_usage_error():
    h = Harness()
    proc = h.run(["dispatch", "alpha", "--bogus-flag"])
    assert proc.returncode == 64 and "unknown flag" in proc.stderr, proc.stderr


def test_why_flag_needs_a_value():
    h = Harness()
    proc = h.run(["dispatch", "alpha", "--why"])
    assert proc.returncode == 64, proc


def test_tier_flag_needs_a_value():
    h = Harness()
    proc = h.run(["dispatch", "alpha", "--tier"])
    assert proc.returncode == 64, proc


def test_equals_form_of_a_value_flag_is_accepted():
    h = Harness()
    proc = h.run(["dispatch", "alpha", "--tier=investigate", "--dry-run", "--json"], stdin=VALID_BRIEF)
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["tier"] == "investigate", proc.stdout


# --- brief is data, never argv --------------------------------------------------


def test_brief_flag_is_refused_by_name():
    h = Harness()
    proc = h.run(["dispatch", "alpha", "--brief", "hello"])
    assert proc.returncode == 64 and "no --brief" in proc.stderr, proc.stderr


def test_empty_brief_is_rejected():
    h = Harness()
    proc = h.run(["dispatch", "alpha", "--json"], stdin="   \n  ")
    out = _json_or_fail(proc)
    assert proc.returncode == 64 and "empty" in out["error"], out


def test_oversize_brief_is_rejected():
    h = Harness()
    proc = h.run(["dispatch", "alpha", "--json"], stdin="x" * 8001)
    out = _json_or_fail(proc)
    assert proc.returncode == 64 and "8000" in out["error"], out


def test_brief_file_not_found_is_usage_error():
    h = Harness()
    proc = h.run(["dispatch", "alpha", "--brief-file", str(h.tmp / "nope.txt")])
    assert proc.returncode == 64, proc


def test_brief_file_is_read_instead_of_stdin():
    h = Harness()
    path = h.tmp / "brief.txt"
    path.write_text(VALID_BRIEF, encoding="utf-8")
    srv = stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-bf", "status": "running"}})})
    try:
        proc = h.run(["dispatch", "alpha", "--brief-file", str(path), "--json"],
                      env=h.base_env(sideclaw=srv.base), stdin=None)
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0, proc.stderr
    assert srv.requests[0]["body"]["params"]["brief"] == VALID_BRIEF


# --- repo resolution -------------------------------------------------------------


def test_unknown_repo_lists_dispatchable():
    h = Harness()
    proc = h.run(["dispatch", "does-not-exist", "--json"], stdin=VALID_BRIEF)
    out = _json_or_fail(proc)
    assert proc.returncode == 64 and "alpha" in out["error"], out


def test_repo_name_traversal_is_rejected():
    h = Harness()
    proc = h.run(["dispatch", "../etc", "--json"], stdin=VALID_BRIEF)
    out = _json_or_fail(proc)
    assert proc.returncode == 64 and "not a repo name" in out["error"], out


def test_denied_and_sensitive_repo_permits_investigate_only():
    h = Harness()
    srv = stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-s", "status": "running"}})})
    try:
        ok = h.run(["dispatch", "secretrepo", "--json"], env=h.base_env(sideclaw=srv.base), stdin=VALID_BRIEF)
        refused = h.run(["dispatch", "secretrepo", "--tier", "author", "--json"], stdin=VALID_BRIEF)
    finally:
        srv.stop()
    assert ok.returncode == 0, ok.stderr
    ref_out = _json_or_fail(refused)
    assert refused.returncode == 4 and "sensitive" in ref_out["error"], ref_out


# --- tier gating -----------------------------------------------------------------


def test_tier_over_repo_ceiling_is_refused():
    h = Harness()
    proc = h.run(["dispatch", "alpha", "--tier", "implement", "--why", "w", "--json"], stdin=VALID_BRIEF)
    out = _json_or_fail(proc)
    assert proc.returncode == 4 and "capped at tier" in out["error"], out


# --- json contract ---------------------------------------------------------------


def test_json_error_object_shape():
    h = Harness()
    proc = h.run(["status", "not valid!", "--json"])
    out = _json_or_fail(proc)
    assert set(out) == {"verb", "ok", "exitCode", "error"}, out
    assert out["ok"] is False and out["exitCode"] == proc.returncode == 64


def test_json_success_is_exactly_one_object():
    h = Harness()
    proc = h.run(["list", "--json"])
    assert proc.returncode == 0
    out = json.loads(proc.stdout)  # raises if there is trailing data
    assert out["verb"] == "list"


# --- audit log ---------------------------------------------------------------------


def test_audit_log_gets_one_line_per_invocation():
    h = Harness()
    log = h.new_log("audit-one")
    env = h.base_env(log=log)
    h.run(["list", "--json"], env=env)
    h.run(["status", "nope!", "--json"], env=env)
    lines = log.read_text(encoding="utf-8").strip().splitlines()
    assert len(lines) == 2, lines
    assert re.match(r"^\S+ \| verb=list \| mode=read \| .* \| rc=0 \| ", lines[0]), lines[0]
    assert re.match(r"^\S+ \| verb=status \| mode=read \| .* \| rc=64 \| ", lines[1]), lines[1]


def test_audit_log_records_refused_dispatch():
    h = Harness()
    log = h.new_log("audit-refused")
    env = h.base_env(log=log)
    h.run(["dispatch", "alpha", "--tier", "implement", "--why", "w", "--json"], env=env, stdin=VALID_BRIEF)
    line = log.read_text(encoding="utf-8").strip()
    assert "verb=dispatch" in line and "mode=refused" in line and "rc=4" in line, line


# --- recursion guard -----------------------------------------------------------------


def test_recursion_guard_blocks_dispatch():
    h = Harness()
    proc = h.run(["dispatch", "alpha", "--json"], env_extra={"CLAUDECODE": "1"}, stdin=VALID_BRIEF)
    out = _json_or_fail(proc)
    assert proc.returncode == 4 and "CLAUDECODE" in out["error"], out


def test_recursion_guard_blocks_merge():
    h = Harness()
    proc = h.run(["merge", "job-x", "--why", "w", "--json"], env_extra={"CLAUDE_CODE_SESSION": "1"})
    out = _json_or_fail(proc)
    assert proc.returncode == 4 and "CLAUDE_CODE_SESSION" in out["error"], out


def test_recursion_guard_does_not_block_read_verbs():
    h = Harness()
    proc = h.run(["list", "--json"], env_extra={"CLAUDECODE": "1"})
    assert proc.returncode == 0, proc


# --- dispatch record / status / list ------------------------------------------------


def test_dispatch_record_and_status_and_list_round_trip():
    h = Harness()
    db = h.new_db()
    env = h.base_env(db=db)
    srv = stubs.StubServer({
        ("POST", "/api/jobs"): (200, {"job": {"id": "job-rt", "status": "running"}}),
        ("GET", "/api/jobs/job-rt"): (200, {"job": {"id": "job-rt", "status": "done",
                                                     "result": {"summary": "s", "artifactUrl": None}}}),
    })
    try:
        env["WARDEN_SIDECLAW_BASE"] = srv.base
        dispatched = h.run(["dispatch", "alpha", "--json"], env=env, stdin=VALID_BRIEF)
        d_out = _json_or_fail(dispatched)
        assert dispatched.returncode == 0 and d_out["status"] == "running", d_out

        listed = h.run(["list", "open", "--json"], env=env)
        l_out = _json_or_fail(listed)
        assert l_out["count"] == 1 and l_out["dispatches"][0]["job_id"] == "job-rt", l_out

        status = h.run(["status", "job-rt", "--json"], env=env)
        s_out = _json_or_fail(status)
        assert status.returncode == 0 and s_out["status"] == "done" and s_out["ok"] is True, s_out
    finally:
        srv.stop()


def test_dispatch_wait_returns_terminal_result():
    h = Harness()
    srv = stubs.StubServer({
        ("POST", "/api/jobs"): (200, {"job": {"id": "job-w", "status": "running"}}),
        ("GET", "/api/jobs/job-w"): (200, {"job": {"id": "job-w", "status": "done", "result": {"summary": "ok"}}}),
    })
    try:
        proc = h.run(["dispatch", "alpha", "--wait", "--json"], env=h.base_env(sideclaw=srv.base), stdin=VALID_BRIEF)
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["waited"] is True and out["status"] == "done", out


# --- run (Wave 6.1: the `human` origin) --------------------------------------


def test_run_record_round_trip():
    h = Harness()
    srv = stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-run-rt", "status": "running"}})})
    try:
        proc = h.run(["run", "alpha", "--json"], env=h.base_env(sideclaw=srv.base), stdin=VALID_BRIEF)
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0, out
    assert out["verb"] == "run" and out["origin"] == "human" and out["repo"] == "alpha"
    assert out["maxTier"] == "investigate" and out["queued"] is False
    assert out["jobId"] == "job-run-rt" and out["state"] == "investigating"
    assert out["eventId"] is not None


def test_run_with_origin_channel_and_thread_carries_onto_the_dispatch_row():
    """Hermes answering in its own thread (`--origin-channel --origin-thread`,
    no `--wait`): the investigate dispatch's row must carry that thread, not
    the shared triage card's — the verdict has to land where the asker can
    read it."""
    h = Harness()
    db = h.new_db()
    srv = stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-run-origin", "status": "running"}})})
    try:
        proc = h.run(
            ["run", "alpha", "--origin-channel", "C0ORIGIN0001", "--origin-thread", "1111.000001", "--json"],
            env=h.base_env(db=db, sideclaw=srv.base), stdin=VALID_BRIEF,
        )
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0, out
    conn, _ = _connect(db)
    row = _row(conn, "SELECT origin_channel, origin_thread_ts FROM dispatches ORDER BY id DESC LIMIT 1")
    conn.close()
    assert row["origin_channel"] == "C0ORIGIN0001", dict(row)
    assert row["origin_thread_ts"] == "1111.000001", dict(row)


def test_run_without_origin_channel_uses_the_shared_card_as_before():
    """No `--origin-channel`/`--origin-thread`: the dispatch row's origin
    channel is the shared triage card's (triage-policy.json's cardChannel,
    or the module default) — unchanged from before these columns existed."""
    h = Harness()
    db = h.new_db()
    srv = stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-run-noorigin", "status": "running"}})})
    try:
        proc = h.run(["run", "alpha", "--json"], env=h.base_env(db=db, sideclaw=srv.base), stdin=VALID_BRIEF)
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0, out
    conn, _ = _connect(db)
    row = _row(conn, "SELECT origin_channel FROM dispatches ORDER BY id DESC LIMIT 1")
    conn.close()
    assert row["origin_channel"] is not None, "must fall back to the shared card's channel, not stay NULL"


def test_run_dry_run_opens_nothing():
    h = Harness()
    db = h.new_db()
    env = h.base_env(db=db)
    proc = h.run(["run", "alpha", "--dry-run", "--json"], env=env, stdin=VALID_BRIEF)
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["dryRun"] is True, out
    conn, _ = _connect(db)
    count = conn.execute("SELECT COUNT(*) FROM triage_items").fetchone()[0]
    conn.close()
    assert count == 0, "a dry-run must never open an item"


def test_run_tier_implement_without_why_is_usage_error():
    h = Harness()
    proc = h.run(["run", "gamma", "--tier", "implement", "--json"], stdin=VALID_BRIEF)
    out = _json_or_fail(proc)
    assert proc.returncode == 64 and "--why" in out["error"], out


def test_run_wait_returns_result():
    h = Harness()
    srv = stubs.StubServer({
        ("POST", "/api/jobs"): (200, {"job": {"id": "job-run-w", "status": "running"}}),
        ("GET", "/api/jobs/job-run-w"): (200, {"job": {"id": "job-run-w", "status": "done",
                                                        "result": {"summary": "ok"}}}),
    })
    try:
        proc = h.run(["run", "alpha", "--wait", "--json"], env=h.base_env(sideclaw=srv.base), stdin=VALID_BRIEF)
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["waited"] is True, out
    assert out["result"]["verdict"]["summary"] == "ok", out


def test_run_wait_folds_the_verdict_before_returning():
    """The bug this test pins: `run --wait` used to call
    `dispatch.sync_record(conn, job, reported=True)` and stop there — that
    stamps `reported_at`, so dispatch-sweep.py's own sweep (which only ever
    folds a dispatch with `reported_at IS NULL`) would never fold this one,
    leaving the item stuck in `investigating` with a DONE job underneath it
    forever. `run --wait` must fold the verdict itself before it returns,
    the same call dispatch-sweep.py's process_dispatch() makes."""
    h = Harness()
    db = h.new_db()
    srv = stubs.StubServer({
        ("POST", "/api/jobs"): (200, {"job": {"id": "job-run-fold", "status": "running"}}),
        ("GET", "/api/jobs/job-run-fold"): (200, {"job": {"id": "job-run-fold", "status": "done",
                                                           "result": {"summary": "all good, no fix needed"}}}),
    })
    try:
        proc = h.run(["run", "alpha", "--wait", "--json"], env=h.base_env(db=db, sideclaw=srv.base),
                      stdin=VALID_BRIEF)
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["waited"] is True, out
    assert out["state"] == "closed", out
    assert out["note"] and out["note"].startswith("answered:"), out

    conn, _ = _connect(db)
    row = _row(conn, "SELECT state, note FROM triage_items WHERE event_id=?", (out["eventId"],))
    conn.close()
    assert row["state"] == "closed", dict(row)
    assert row["note"].startswith("answered:"), dict(row)


def test_status_unknown_job_is_usage_error():
    h = Harness()
    srv = stubs.StubServer({"default": (404, {"error": "no such job"})})
    try:
        proc = h.run(["status", "job-nope", "--json"], env=h.base_env(sideclaw=srv.base))
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 64 and "no such job" in out["error"], out


def test_status_terminal_record_survives_a_pruned_job():
    h = Harness()
    db = h.new_db()
    _seed_dispatch(db, "job-pruned", status="done", verdict_json=json.dumps({"summary": "s"}))
    srv = stubs.StubServer({"default": (404, {"error": "gone"})})
    try:
        proc = h.run(["status", "job-pruned", "--json"], env=h.base_env(db=db, sideclaw=srv.base))
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["fromRecord"] is True and out["status"] == "done", out


def test_status_non_terminal_record_with_pruned_job_is_remote_error():
    h = Harness()
    db = h.new_db()
    _seed_dispatch(db, "job-lost", status="running")
    srv = stubs.StubServer({"default": (404, {"error": "gone"})})
    try:
        proc = h.run(["status", "job-lost", "--json"], env=h.base_env(db=db, sideclaw=srv.base))
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 3 and "verdict is lost" in out["error"], out


# --- write-tier gate: mint + Slack buttons ----------------------------------------


def test_implement_dispatch_mints_an_approval_and_posts_slack_buttons():
    h = Harness()
    db = h.new_db()
    priv, pub_hex = _keypair()
    pubkey_path = h.tmp / "gate-dispatch-approval.pub"
    pubkey_path.write_text(pub_hex + "\n", encoding="utf-8")
    slack_srv = stubs.StubServer({("POST", "/chat.postMessage"): (200, {"ok": True, "ts": "1.1"})})
    try:
        env = h.base_env(db=db, slack=slack_srv.base, pubkey=pubkey_path)
        proc = h.run(
            ["dispatch", "gamma", "--tier", "implement", "--why", "fix it", "--origin-channel", "C1234ABCD",
             "--json"],
            env=env, stdin=VALID_BRIEF,
        )
        out = _json_or_fail(proc)
        assert proc.returncode == 0 and out["needsConfirm"] is True and out["dryRun"] is True, out
        assert len(slack_srv.requests) == 1, slack_srv.requests
        assert slack_srv.requests[0]["body"]["blocks"][1]["elements"][0]["action_id"] == "hermes_cc_approve"
    finally:
        slack_srv.stop()

    conn, _ = _connect(db)
    row = _row(conn, "SELECT * FROM dispatch_approvals")
    assert row is not None and row["repo"] == "gamma" and row["key_id"] == signer.key_id(pub_hex), (
        dict(row) if row else None
    )
    conn.close()


def test_confirm_flag_on_dispatch_is_usage_error():
    h = Harness()
    proc = h.run(["dispatch", "gamma", "--tier", "implement", "--confirm", "--why", "w", "--json"],
                  stdin=VALID_BRIEF)
    out = _json_or_fail(proc)
    assert proc.returncode == 64 and "--confirm is a merge flag" in out["error"], out


def test_implement_without_why_is_usage_error():
    h = Harness()
    proc = h.run(["dispatch", "gamma", "--tier", "implement", "--json"], stdin=VALID_BRIEF)
    out = _json_or_fail(proc)
    assert proc.returncode == 64 and "requires --why" in out["error"], out


# --- auto-from-item ----------------------------------------------------------------


def test_auto_from_item_happy_path_opens_directly_no_plan():
    h = Harness()
    db = h.new_db()
    _seed_dispatch(db, "job-verdict", tier="investigate", repo="gamma", status="done",
                    verdict_json=json.dumps({"nextAction": "implement", "confidence": "high"}))
    _seed_item(db, 1, state="verdict", repo="gamma", dispatch_job="job-verdict")
    srv = stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-auto", "status": "running"}})})
    try:
        proc = h.run(
            ["dispatch", "gamma", "--tier", "implement", "--auto-from-item", "1", "--why", "w", "--json"],
            env=h.base_env(db=db, sideclaw=srv.base), stdin=VALID_BRIEF,
        )
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out.get("jobId") == "job-auto", out


def test_auto_from_item_unknown_event_is_policy_error():
    h = Harness()
    proc = h.run(["dispatch", "gamma", "--tier", "implement", "--auto-from-item", "999", "--why", "w", "--json"],
                  stdin=VALID_BRIEF)
    out = _json_or_fail(proc)
    assert proc.returncode == 4 and "no triage_items row" in out["error"], out


def test_auto_from_item_wrong_state_is_policy_error():
    h = Harness()
    db = h.new_db()
    _seed_item(db, 2, state="needs_human", repo="gamma")
    proc = h.run(["dispatch", "gamma", "--tier", "implement", "--auto-from-item", "2", "--why", "w", "--json"],
                  env=h.base_env(db=db), stdin=VALID_BRIEF)
    out = _json_or_fail(proc)
    assert proc.returncode == 4 and "not 'verdict'" in out["error"], out


# --- merge -------------------------------------------------------------------------


_MERGE_PR_BODY = {
    "state": "open", "merged": False, "title": "the fix",
    "base": {"ref": "master"}, "head": {"ref": "dispatch/x", "sha": "deadbeef" * 5, "repo": {"full_name": "jkrumm/gamma"}},
    "changed_files": 1, "additions": 1, "deletions": 0, "node_id": "PR_node",
    "mergeable": True, "mergeable_state": "clean",
}


def _merge_stub() -> stubs.StubServer:
    return stubs.StubServer({
        ("GET", "/repos/jkrumm/gamma/pulls/9"): (200, _MERGE_PR_BODY),
        ("GET", "/repos/jkrumm/gamma"): (200, {"default_branch": "master", "allow_squash_merge": True}),
        ("GET", "/repos/jkrumm/gamma/pulls/9/files?per_page=100"): (200, [{"filename": "a.py"}]),
        ("GET", f"/repos/jkrumm/gamma/commits/{'deadbeef' * 5}/check-runs"): (200, {"check_runs": []}),
        ("POST", "/graphql"): (200, {"data": {"markPullRequestReadyForReview": {"pullRequest": {"isDraft": False}}}}),
        ("PUT", "/repos/jkrumm/gamma/pulls/9/merge"): (200, {"sha": "merged-sha", "merged": True}),
        ("DELETE", "/repos/jkrumm/gamma/git/refs/heads/dispatch/x"): (204, None),
    })


def test_merge_without_why_is_usage_error():
    h = Harness()
    proc = h.run(["merge", "job-any", "--confirm", "--json"])
    out = _json_or_fail(proc)
    assert proc.returncode == 64 and "requires --why" in out["error"], out


def test_merge_unknown_job_is_precondition_error():
    h = Harness()
    proc = h.run(["merge", "job-ghost", "--why", "w", "--confirm", "--json"])
    out = _json_or_fail(proc)
    assert proc.returncode == 2 and "no dispatch recorded" in out["error"], out


def test_merge_dry_run_plan_does_not_land():
    h = Harness()
    db = h.new_db()
    _seed_dispatch(db, "job-plan", tier="implement", repo="gamma", status="done",
                    artifact_url="https://github.com/jkrumm/gamma/pull/9", validation_status="confirmed")
    srv = _merge_stub()
    try:
        proc = h.run(["merge", "job-plan", "--why", "land it", "--json"], env=h.base_env(db=db, gh=srv.base))
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["dryRun"] is True and out["needsConfirm"] is True, out
    assert not any(r["method"] == "PUT" for r in srv.requests), srv.requests


def test_merge_confirmed_lands_the_pull_request():
    h = Harness()
    db = h.new_db()
    _seed_dispatch(db, "job-land", tier="implement", repo="gamma", status="done",
                    artifact_url="https://github.com/jkrumm/gamma/pull/9", validation_status="confirmed")
    srv = _merge_stub()
    try:
        proc = h.run(["merge", "job-land", "--why", "land it", "--confirm", "--json"], env=h.base_env(db=db, gh=srv.base))
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["merged"] is True and out["mergeCommit"] == "merged-sha", out
    conn, _ = _connect(db)
    assert _row(conn, "SELECT merged_at FROM dispatches WHERE job_id='job-land'")["merged_at"] is not None
    conn.close()


# --- abort ---------------------------------------------------------------------------


def test_abort_cancels_running_episode_and_closes_the_item():
    h = Harness()
    db = h.new_db()
    _seed_dispatch(db, "job-abort", tier="implement", repo="gamma", status="running")
    _seed_item(db, 5, state="implementing", repo="gamma", implement_job="job-abort")
    srv = stubs.StubServer({("POST", "/api/jobs/job-abort/cancel"): (200, {"job": {"id": "job-abort", "status": "cancelled"}})})
    try:
        proc = h.run(["abort", "5", "--why", "stuck", "--json"], env=h.base_env(db=db, sideclaw=srv.base))
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["cancelled"] is True and out["state"] == "closed", out
    conn, _ = _connect(db)
    assert _row(conn, "SELECT status FROM dispatches WHERE job_id='job-abort'")["status"] == "cancelled"
    assert _row(conn, "SELECT state FROM triage_items WHERE event_id=5")["state"] == "closed"
    conn.close()


def test_abort_wrong_state_is_policy_error():
    h = Harness()
    db = h.new_db()
    _seed_item(db, 6, state="verdict", repo="gamma")
    proc = h.run(["abort", "6", "--why", "stuck", "--json"], env=h.base_env(db=db))
    out = _json_or_fail(proc)
    assert proc.returncode == 4 and "not investigating/implementing/validating" in out["error"], out


def test_abort_unknown_item_is_usage_error():
    h = Harness()
    proc = h.run(["abort", "404", "--why", "stuck", "--json"])
    out = _json_or_fail(proc)
    assert proc.returncode == 64, out


def test_abort_discharges_every_member_of_the_cluster_sharing_the_job():
    """A batch files several items onto ONE dispatch_job (three signatures, one
    investigation). Cancelling the job ends the episode for all of them, so the
    siblings must not be left behind in an in-flight state: `close` refuses
    them (in-flight states are abort's), and the sweep never folds a row it has
    already reported — a stranded sibling can only expire to needs_human."""
    h = Harness()
    db = h.new_db()
    _seed_dispatch(db, "job-cluster", tier="investigate", repo="gamma", status="running")
    _seed_item(db, 21, state="investigating", repo="gamma", dispatch_job="job-cluster")
    _seed_item(db, 22, state="investigating", repo="gamma", dispatch_job="job-cluster")
    srv = stubs.StubServer({("POST", "/api/jobs/job-cluster/cancel"): (200, {"job": {"id": "job-cluster", "status": "cancelled"}})})
    try:
        proc = h.run(["abort", "21", "--why", "one batch, one cause", "--json"],
                     env=h.base_env(db=db, sideclaw=srv.base))
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0, out
    assert out.get("discharged") == [22], out
    conn, _ = _connect(db)
    states = {r["event_id"]: r["state"] for r in conn.execute("SELECT event_id, state FROM triage_items").fetchall()}
    assert states == {21: "closed", 22: "closed"}, states
    notes = {r["event_id"]: r["note"] for r in conn.execute("SELECT event_id, note FROM triage_items").fetchall()}
    assert all("one batch, one cause" in (n or "") for n in notes.values()), notes
    conn.close()


def test_abort_tolerates_an_already_terminal_job_and_still_discharges_the_cluster():
    """The sibling's abort is the second one against this job, so sideclaw
    answers 409 'job already cancelled'. The abort's own intent — no episode
    running — already holds, so it must proceed to the transition instead of
    refusing and abandoning every member of the cluster."""
    h = Harness()
    db = h.new_db()
    _seed_dispatch(db, "job-dead", tier="investigate", repo="gamma", status="cancelled")
    _seed_item(db, 31, state="investigating", repo="gamma", dispatch_job="job-dead")
    _seed_item(db, 32, state="investigating", repo="gamma", dispatch_job="job-dead")
    srv = stubs.StubServer({("POST", "/api/jobs/job-dead/cancel"): (409, {"error": "job already cancelled"})})
    try:
        proc = h.run(["abort", "31", "--why", "already cancelled by the sibling", "--json"],
                     env=h.base_env(db=db, sideclaw=srv.base))
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["cancelled"] is False, out
    conn, _ = _connect(db)
    states = {r["event_id"]: r["state"] for r in conn.execute("SELECT event_id, state FROM triage_items").fetchall()}
    assert states == {31: "closed", 32: "closed"}, states
    conn.close()


def test_abort_leaves_a_cluster_sibling_that_already_opened_a_pr_alone():
    """Discharging the cluster must stop at the episode: a sibling that has
    moved on to `pr_open` carries a draft PR a human still has to review, and
    cancelling the investigation job is not a reason to close it."""
    h = Harness()
    db = h.new_db()
    _seed_dispatch(db, "job-pr", tier="investigate", repo="gamma", status="running")
    _seed_item(db, 41, state="investigating", repo="gamma", dispatch_job="job-pr")
    _seed_item(db, 42, state="pr_open", repo="gamma", dispatch_job="job-pr")
    srv = stubs.StubServer({("POST", "/api/jobs/job-pr/cancel"): (200, {"job": {"id": "job-pr", "status": "cancelled"}})})
    try:
        proc = h.run(["abort", "41", "--why", "duplicate", "--json"], env=h.base_env(db=db, sideclaw=srv.base))
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["discharged"] == [], out
    conn, _ = _connect(db)
    states = {r["event_id"]: r["state"] for r in conn.execute("SELECT event_id, state FROM triage_items").fetchall()}
    assert states == {41: "closed", 42: "pr_open"}, states
    conn.close()


def test_abort_dry_run_cancels_nothing_and_writes_nothing():
    """The global `--dry-run` contract: no outward call, no write. `abort` was
    the one verb that fell straight through it and cancelled the episode for
    real — observed live 2026-09-22, where the "dry run" is what actually
    cancelled job 6e53fcb4 and closed item 1114."""
    h = Harness()
    db = h.new_db()
    _seed_dispatch(db, "job-dry", tier="investigate", repo="gamma", status="running")
    _seed_item(db, 51, state="investigating", repo="gamma", dispatch_job="job-dry")
    _seed_item(db, 52, state="investigating", repo="gamma", dispatch_job="job-dry")
    # No stub server at all: a closed port, so any cancel attempt fails loudly.
    proc = h.run(["abort", "51", "--why", "preview", "--dry-run", "--json"], env=h.base_env(db=db))
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out.get("dryRun") is True and out["cancelled"] is False, out
    conn, _ = _connect(db)
    states = {r["event_id"]: r["state"] for r in conn.execute("SELECT event_id, state FROM triage_items").fetchall()}
    assert states == {51: "investigating", 52: "investigating"}, states
    assert _row(conn, "SELECT status FROM dispatches WHERE job_id='job-dry'")["status"] == "running"
    assert _row(conn, "SELECT COUNT(*) AS n FROM item_transitions")["n"] == 0
    conn.close()


# --- revert --------------------------------------------------------------------------


def test_revert_records_the_pr_and_closes_the_item():
    h = Harness()
    db = h.new_db()
    _seed_item(db, 7, state="fixed", repo="gamma")
    srv = stubs.StubServer({
        ("GET", "/repos/jkrumm/gamma/pulls/11"): (200, {"state": "closed", "head": {"repo": {"full_name": "jkrumm/gamma"}}}),
    })
    try:
        proc = h.run(["revert", "7", "--pr", "11", "--why", "regressed", "--json"], env=h.base_env(db=db, gh=srv.base))
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["state"] == "reverted" and out["pullRequest"] == 11, out
    conn, _ = _connect(db)
    row = _row(conn, "SELECT state, revert_pr FROM triage_items WHERE event_id=7")
    assert row["state"] == "reverted" and row["revert_pr"] == 11, dict(row)
    conn.close()


def test_revert_fork_pr_is_policy_error():
    h = Harness()
    db = h.new_db()
    _seed_item(db, 8, state="merged", repo="gamma")
    srv = stubs.StubServer({
        ("GET", "/repos/jkrumm/gamma/pulls/12"): (200, {"state": "closed", "head": {"repo": {"full_name": "someone-else/gamma"}}}),
    })
    try:
        proc = h.run(["revert", "8", "--pr", "12", "--why", "regressed", "--json"], env=h.base_env(db=db, gh=srv.base))
    finally:
        srv.stop()
    out = _json_or_fail(proc)
    assert proc.returncode == 4 and "fork" in out["error"], out


def test_revert_wrong_state_is_policy_error():
    h = Harness()
    db = h.new_db()
    _seed_item(db, 9, state="verdict", repo="gamma")
    proc = h.run(["revert", "9", "--pr", "1", "--why", "x", "--json"], env=h.base_env(db=db))
    out = _json_or_fail(proc)
    assert proc.returncode == 4, out


# --- close ---------------------------------------------------------------------


def test_close_from_needs_human_writes_closed_and_a_transition_row():
    h = Harness()
    db = h.new_db()
    _seed_item(db, 20, state="needs_human", repo="gamma")
    proc = h.run(["close", "20", "--why", "answered in a session", "--json"], env=h.base_env(db=db))
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["toState"] == "closed", out
    assert out["note"].startswith("closed by hand: "), out
    conn, _ = _connect(db)
    row = _row(conn, "SELECT state, note FROM triage_items WHERE event_id=20")
    assert row["state"] == "closed" and row["note"].startswith("closed by hand: "), dict(row)
    trans = _row(
        conn, "SELECT from_state, to_state FROM item_transitions WHERE event_id=20 ORDER BY id DESC LIMIT 1"
    )
    assert trans["from_state"] == "needs_human" and trans["to_state"] == "closed", dict(trans)
    conn.close()


def test_close_text_output_prints_the_transition():
    """Every other close test passes --json; the plain-text renderer read
    snake_case keys off a camelCase result and crashed after the write
    (item 543, 2026-09-14)."""
    h = Harness()
    db = h.new_db()
    _seed_item(db, 25, state="needs_human", repo="gamma")
    proc = h.run(["close", "25", "--why", "fixed elsewhere"], env=h.base_env(db=db))
    assert proc.returncode == 0, proc
    assert "item 25: needs_human -> closed" in proc.stdout, proc.stdout


def test_close_without_why_is_usage_error():
    h = Harness()
    db = h.new_db()
    _seed_item(db, 21, state="needs_human", repo="gamma")
    proc = h.run(["close", "21", "--json"], env=h.base_env(db=db))
    assert proc.returncode == 64, proc


def test_close_from_implementing_is_refused_and_item_unchanged():
    h = Harness()
    db = h.new_db()
    _seed_item(db, 22, state="implementing", repo="gamma", implement_job="job-x")
    proc = h.run(["close", "22", "--why", "nope", "--json"], env=h.base_env(db=db))
    out = _json_or_fail(proc)
    assert proc.returncode == 2, out
    conn, _ = _connect(db)
    row = _row(conn, "SELECT state FROM triage_items WHERE event_id=22")
    assert row["state"] == "implementing", dict(row)
    count = _row(conn, "SELECT COUNT(*) AS n FROM item_transitions WHERE event_id=22")["n"]
    assert count == 0, count
    conn.close()


def test_close_of_an_already_closed_item_is_a_no_op():
    h = Harness()
    db = h.new_db()
    _seed_item(db, 23, state="closed", repo="gamma")
    proc = h.run(["close", "23", "--why", "still done", "--json"], env=h.base_env(db=db))
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out["toState"] == "closed", out
    conn, _ = _connect(db)
    count = _row(conn, "SELECT COUNT(*) AS n FROM item_transitions WHERE event_id=23")["n"]
    assert count == 0, count
    conn.close()


def test_close_dry_run_writes_nothing():
    h = Harness()
    db = h.new_db()
    _seed_item(db, 24, state="needs_human", repo="gamma")
    proc = h.run(["close", "24", "--why", "answered", "--dry-run", "--json"], env=h.base_env(db=db))
    out = _json_or_fail(proc)
    assert proc.returncode == 0 and out.get("dryRun") is True, out
    conn, _ = _connect(db)
    row = _row(conn, "SELECT state FROM triage_items WHERE event_id=24")
    assert row["state"] == "needs_human", dict(row)
    count = _row(conn, "SELECT COUNT(*) AS n FROM item_transitions WHERE event_id=24")["n"]
    assert count == 0, count
    conn.close()


# --- ledger schema assertion -------------------------------------------------------


def test_wrong_schema_version_is_precondition_error():
    h = Harness()
    db = h.new_db()
    conn, mod = _connect(db)
    conn.execute("UPDATE schema_version SET version=?", (mod.LEDGER_SCHEMA_VERSION + 1,))
    conn.commit()
    conn.close()
    proc = h.run(["list", "--json"], env=h.base_env(db=db))
    out = _json_or_fail(proc)
    assert proc.returncode == 2 and "schema_version" in out["error"], out


def test_missing_schema_is_precondition_error():
    h = Harness()
    empty_db = h.tmp / "empty.db"
    import sqlite3
    sqlite3.connect(empty_db).close()
    proc = h.run(["list", "--json"], env=h.base_env(db=empty_db))
    out = _json_or_fail(proc)
    assert proc.returncode == 2, out


# --- click path: plan -> sign -> intents.py --record/--drain -----------------------


def _keypair():
    priv = Ed25519PrivateKey.generate()
    pub_hex = priv.public_key().public_bytes(
        encoding=serialization.Encoding.Raw, format=serialization.PublicFormat.Raw,
    ).hex()
    return priv, pub_hex


def test_click_path_drains_a_signed_approval_into_an_opened_episode():
    h = Harness()
    db = h.new_db()
    priv, pub_hex = _keypair()
    pubkey_path = h.tmp / "dispatch-approval.pub"
    pubkey_path.write_text(pub_hex + "\n", encoding="utf-8")

    slack_srv = stubs.StubServer({("POST", "/chat.postMessage"): (200, {"ok": True})})
    try:
        env = h.base_env(db=db, slack=slack_srv.base, pubkey=pubkey_path)
        planned = h.run(
            ["dispatch", "gamma", "--tier", "implement", "--why", "fix it", "--json"], env=env, stdin=VALID_BRIEF,
        )
    finally:
        slack_srv.stop()
    assert planned.returncode == 0, planned.stderr

    conn, _ = _connect(db)
    row = _row(conn, "SELECT * FROM dispatch_approvals")
    nonce, payload_hash, expires_at = row["nonce"], row["payload_hash"], row["expires_at"]
    assert row["stdin_text"] == VALID_BRIEF
    conn.close()

    message = signer.canonical_message(nonce, payload_hash, "approve", "U999", expires_at)
    intent = {
        "v": 1, "kind": "approval_decision", "created_at": dt.datetime.now(dt.timezone.utc).isoformat(),
        "source": "test-click", "nonce": nonce, "decision": "approve", "decided_by": "U999",
        "signature": priv.sign(message).hex(),
    }

    intents_env = {**h.base_env(db=db, pubkey=pubkey_path), "WARDEN_INTENTS_DIR": str(h.tmp / "intents")}
    rec = subprocess.run(
        [sys.executable, str(INTENTS_PY), "--record"], input=json.dumps(intent),
        capture_output=True, text=True, env=intents_env,
    )
    assert rec.returncode == 0, rec.stderr

    sideclaw_srv = stubs.StubServer({("POST", "/api/jobs"): (200, {"job": {"id": "job-click", "status": "running"}})})
    try:
        intents_env["WARDEN_SIDECLAW_BASE"] = sideclaw_srv.base
        drained = subprocess.run(
            [sys.executable, str(INTENTS_PY), "--drain", str(db)],
            capture_output=True, text=True, env=intents_env,
        )
        assert drained.returncode == 0, drained.stderr
        assert sideclaw_srv.requests[0]["body"]["params"]["brief"] == VALID_BRIEF

        # A replay of an already-drained (now-gone) intent file opens nothing
        # a second time — there is nothing left to drain, and re-running is a no-op.
        replay = subprocess.run(
            [sys.executable, str(INTENTS_PY), "--drain", str(db)],
            capture_output=True, text=True, env=intents_env,
        )
        assert replay.returncode == 0 and replay.stdout.strip() == "applied=0 rejected=0", replay.stdout
        assert len(sideclaw_srv.requests) == 1, "a replayed drain must never submit a second episode"
    finally:
        sideclaw_srv.stop()

    conn, _ = _connect(db)
    row = _row(conn, "SELECT * FROM dispatch_approvals WHERE nonce=?", (nonce,))
    assert row["spent_job_id"] == "job-click", dict(row)
    op = _row(conn, "SELECT * FROM operations WHERE repo='gamma'")
    assert op["authorized_by"] == "signed:U999", dict(op)
    conn.close()


# --- no free-form surface ------------------------------------------------------------


def test_warden_py_never_shells_out_freeform():
    src = (REPO / "scripts" / "warden.py").read_text(encoding="utf-8")
    for needle in ("shell=True", "os.system(", "eval("):
        assert needle not in src, f"found {needle!r} in scripts/warden.py"
    assert "subprocess" not in src, "warden.py itself must never shell out — the clients/ layer does"


# --- runner ------------------------------------------------------------------------

def main() -> int:
    tests = [(name, fn) for name, fn in sorted(globals().items())
             if name.startswith("test_") and callable(fn)]
    passed = 0
    failures: list[str] = []
    for name, fn in tests:
        try:
            fn()
            passed += 1
        except AssertionError as e:
            failures.append(f"{name}: {e}")
        except Exception:
            failures.append(f"{name}: unexpected exception\n{traceback.format_exc()}")

    print(f"{passed}/{len(tests)} passed")
    if failures:
        print("\nFAILURES:")
        for f in failures:
            print(f"  {f}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
