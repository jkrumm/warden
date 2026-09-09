#!/usr/bin/env python3
"""Regression suite for scripts/api.py — GET /metrics and GET /health.

Covers real arithmetic against a seeded ledger (not smoke tests), the honesty
rules (`null` + a reason, never a fabricated `0`; `history_since` telling an
empty-by-construction window apart from a measured zero), the 503-on-schema-
mismatch contract, the GET-only method surface, and — structurally, via a
filesystem that forbids writes entirely — that the handler never opens a
writable connection.

Run: .venv/bin/python3 tests/test_api.py
"""

from __future__ import annotations

import datetime as dt
import http.client
import importlib.util
import json
import sqlite3
import stat
import sys
import tempfile
import time
import traceback
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent


def _load(name: str, relpath: str):
    spec = importlib.util.spec_from_file_location(name, REPO / relpath)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


ledger = _load("ledger", "scripts/ledger.py")
api = _load("api", "scripts/api.py")


def _iso(d: dt.datetime) -> str:
    return d.isoformat()


def _tmp_db() -> Path:
    return Path(tempfile.mkdtemp(prefix="api-test-")) / "warden.db"


def _fresh_conn():
    path = _tmp_db()
    return ledger.connect(path, migrate=True), path


def _event(conn, event_id: int, now: dt.datetime, source: str = "s") -> None:
    conn.execute(
        "INSERT INTO events (id, source, external_id, title, first_seen) VALUES (?,?,?,?,?)",
        (event_id, source, f"e{event_id}", f"title{event_id}", _iso(now)),
    )


def _item(conn, event_id: int, *, state: str, repo: str | None = "r", dispatch_job: str | None = None,
         now: dt.datetime) -> None:
    conn.execute(
        "INSERT INTO triage_items (event_id, signature, repo, state, dispatch_job, created_at, updated_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (event_id, f"sig{event_id}", repo, state, dispatch_job, _iso(now), _iso(now)),
    )


def _dispatch(conn, job_id: str, *, tier: str = "investigate", verdict_json: str | None = "{}",
             now: dt.datetime) -> None:
    conn.execute(
        "INSERT INTO dispatches (job_id, tier, repo, brief, status, verdict_json, created_at) "
        "VALUES (?,?,?,?,?,?,?)",
        (job_id, tier, "r", "brief", "done", verdict_json, _iso(now)),
    )


def _transition(conn, event_id: int, from_state: str | None, to_state: str, at: dt.datetime) -> None:
    conn.execute(
        "INSERT INTO item_transitions (event_id, from_state, to_state, at) VALUES (?,?,?,?)",
        (event_id, from_state, to_state, _iso(at)),
    )


def _cursor(conn, key: str, at: dt.datetime, value: str = "{}") -> None:
    conn.execute(
        "INSERT INTO cursors (key, value, updated_at) VALUES (?,?,?)",
        (key, value, _iso(at)),
    )


def _approval(conn, nonce: str, *, spent_at: dt.datetime | None) -> None:
    conn.execute(
        "INSERT INTO dispatch_approvals (nonce, verb, repo, tier, payload_hash, created_at, expires_at, spent_at) "
        "VALUES (?,?,?,?,?,?,?,?)",
        (nonce, "implement", "r", "implement", "hash", _iso(dt.datetime.now(dt.timezone.utc)),
         _iso(dt.datetime.now(dt.timezone.utc)), _iso(spent_at) if spent_at else None),
    )


# --- Metric 1: verdicts -> recorded disposition --------------------------------

def test_metric1_arithmetic_numerator_denominator():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    for i in range(1, 5):
        _event(conn, i, now)
    _dispatch(conn, "j1", verdict_json="{}", now=now)   # eligible denom
    _dispatch(conn, "j2", verdict_json="{}", now=now)   # eligible denom
    _dispatch(conn, "j3", verdict_json=None, now=now)   # NOT eligible — no verdict
    _dispatch(conn, "j4", tier="implement", verdict_json="{}", now=now)  # NOT eligible — wrong tier
    _item(conn, 1, state="fixed", dispatch_job="j1", now=now)     # disposition
    _item(conn, 2, state="investigating", dispatch_job="j2", now=now)  # NOT a disposition
    conn.commit()

    m = api._metric_verdicts_recorded_disposition(conn)
    assert m["denominator"] == 2, m
    assert m["numerator"] == 1, m
    assert m["value"] == 0.5, m
    assert m["unavailable"] is None
    assert m["states"] == {"fixed": 1, "investigating": 1}, m
    conn.close()


def test_metric1_quiet_is_not_a_disposition():
    """DESIGN.md principle 5 / § Lifecycle: silence-resolve is never an
    outcome. `quiet` must NOT count toward the numerator."""
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _dispatch(conn, "j1", verdict_json="{}", now=now)
    _item(conn, 1, state="quiet", dispatch_job="j1", now=now)
    conn.commit()

    m = api._metric_verdicts_recorded_disposition(conn)
    assert m["denominator"] == 1
    assert m["numerator"] == 0, "quiet must not count as a recorded disposition"
    assert m["value"] == 0.0
    conn.close()


def test_metric1_zero_denominator_is_null_not_zero():
    conn, _ = _fresh_conn()
    m = api._metric_verdicts_recorded_disposition(conn)
    assert m["value"] is None
    assert m["unavailable"]
    conn.close()


# --- Metric 2: verified fixes vs silence ----------------------------------------

def test_metric2_arithmetic_restricted_to_mapped_signatures():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    for i in range(1, 6):
        _event(conn, i, now)
    _item(conn, 1, state="fixed", repo="r", now=now)
    _item(conn, 2, state="fixed", repo="r", now=now)
    _item(conn, 3, state="quiet", repo="r", now=now)
    _item(conn, 4, state="closed", repo="r", now=now)
    _item(conn, 5, state="fixed", repo=None, now=now)  # unmapped — excluded
    conn.commit()

    m = api._metric_verified_fixes_vs_silence(conn)
    assert m["fixed"] == 2, m
    assert m["quiet"] == 1, m
    assert m["closed"] == 1, m
    assert m["value"] == 2 / 3, m
    assert m["unavailable"] is None
    conn.close()


def test_metric2_zero_denominator_is_null():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _item(conn, 1, state="closed", repo="r", now=now)
    conn.commit()
    m = api._metric_verified_fixes_vs_silence(conn)
    assert m["value"] is None
    assert m["unavailable"]
    conn.close()


# --- Metric 3: median needs_human -> decision -----------------------------------

def test_metric3_excludes_dismissed_exit_includes_other_exit():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _event(conn, 2, now)

    # Event 1: needs_human -> dismissed (the 7d expiry clock) — must be excluded.
    _transition(conn, 1, "investigating", "needs_human", now - dt.timedelta(hours=10))
    _transition(conn, 1, "needs_human", "dismissed", now - dt.timedelta(hours=2))

    # Event 2: needs_human -> investigating (a real human decision) — 4h, included.
    _transition(conn, 2, "investigating", "needs_human", now - dt.timedelta(hours=8))
    _transition(conn, 2, "needs_human", "investigating", now - dt.timedelta(hours=4))
    conn.commit()

    m = api._metric_median_needs_human_to_decision(conn, now)
    assert m["pairs"] == 1, m
    assert m["excluded_dismissed_pairs"] == 1, m
    assert abs(m["value"] - 4.0) < 0.01, m
    conn.close()


def test_metric3_no_pairs_is_null_not_zero():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    m = api._metric_median_needs_human_to_decision(conn, now)
    assert m["value"] is None
    assert m["unavailable"]
    conn.close()


def test_metric3_windowed_on_entry():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    # Entry 10 days ago — outside the 7-day window — must not count.
    _transition(conn, 1, "investigating", "needs_human", now - dt.timedelta(days=10))
    _transition(conn, 1, "needs_human", "investigating", now - dt.timedelta(days=9))
    conn.commit()
    m = api._metric_median_needs_human_to_decision(conn, now)
    assert m["pairs"] == 0, m
    assert m["value"] is None
    conn.close()


# --- Metric 4: verified unattended fixes per week -------------------------------

def test_metric4_unattended_is_null_with_reason_never_zero():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _transition(conn, 1, "implementing", "fixed", now - dt.timedelta(hours=1))
    _approval(conn, "n1", spent_at=now - dt.timedelta(hours=1))
    conn.commit()

    m = api._metric_verified_unattended_fixes_per_week(conn, now)
    assert m["value"] is None
    assert m["unavailable"] and len(m["unavailable"]) > 0
    assert m["fixes_in_window"] == 1, m
    assert m["approvals_spent_in_window"] == 1, m
    conn.close()


# --- Metric 5: poller ages --------------------------------------------------------

def test_metric5_per_poller_ages_and_worst_case():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _cursor(conn, "triage_last_run", now - dt.timedelta(minutes=5))
    _cursor(conn, "watchdog_poll_last_run", now - dt.timedelta(minutes=120))
    conn.commit()
    # dispatch_sweep_last_run: never recorded.

    m = api._metric_poller_ages(conn, now)
    assert m["pollers"]["loop"]["age_minutes"] is not None
    assert abs(m["pollers"]["loop"]["age_minutes"] - 5) < 0.1
    assert m["pollers"]["watchdog_poll"]["stale"] is True  # 120min > 90min threshold
    assert m["pollers"]["dispatch_sweep"]["age_minutes"] is None
    assert m["pollers"]["dispatch_sweep"]["unavailable"]
    assert abs(m["value"] - 120) < 0.1, "worst-case age across available pollers"
    conn.close()


# --- Metric 6: reverts and reopen-after-fixed -----------------------------------

def test_metric6_reopen_after_fixed_counts_and_reverts_is_null():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    _transition(conn, 1, "fixed", "new", now - dt.timedelta(hours=1))
    conn.commit()

    m = api._metric_reverts_and_reopens(conn, now)
    assert m["reopen_after_fixed"]["value"] == 1, m
    assert m["reverts"]["value"] is None
    assert m["reverts"]["unavailable"] and len(m["reverts"]["unavailable"]) > 0
    conn.close()


# --- history_since ---------------------------------------------------------------

def test_history_since_null_on_empty_item_transitions():
    conn, _ = _fresh_conn()
    payload = api.metrics_payload(conn)
    assert payload["history_since"] is None
    conn.close()


def test_history_since_is_earliest_transition():
    conn, _ = _fresh_conn()
    now = dt.datetime.now(dt.timezone.utc)
    _event(conn, 1, now)
    earliest = now - dt.timedelta(days=3)
    _transition(conn, 1, "new", "investigating", earliest)
    _transition(conn, 1, "investigating", "needs_human", now - dt.timedelta(days=1))
    conn.commit()
    payload = api.metrics_payload(conn)
    assert payload["history_since"] == _iso(earliest)
    conn.close()


# --- HTTP-level: schema mismatch, method surface, read-only structural guard ----

def _wait_for_port(host: str, port: int, timeout: float = 5.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            conn = http.client.HTTPConnection(host, port, timeout=0.5)
            conn.request("GET", "/health")
            conn.getresponse()
            conn.close()
            return
        except (ConnectionRefusedError, OSError):
            time.sleep(0.05)
    raise TimeoutError(f"server on {host}:{port} never came up")


class _ServerHandle:
    def __init__(self, db_path: Path):
        import http.server
        self._original_db_path = api.DB_PATH
        api.DB_PATH = db_path
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), api.Handler)
        self.port = self.server.server_address[1]
        import threading
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()
        _wait_for_port("127.0.0.1", self.port)

    def get(self, path: str) -> tuple[int, dict]:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request("GET", path)
        resp = conn.getresponse()
        body = json.loads(resp.read().decode())
        status = resp.status
        conn.close()
        return status, body

    def request(self, method: str, path: str) -> int:
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=5)
        conn.request(method, path)
        resp = conn.getresponse()
        resp.read()
        status = resp.status
        conn.close()
        return status

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        api.DB_PATH = self._original_db_path


def test_schema_mismatch_returns_503_not_200():
    path = _tmp_db()
    conn = ledger.connect(path, migrate=True)
    conn.execute("UPDATE schema_version SET version = ?", (ledger.SCHEMA_VERSION + 1,))
    conn.commit()
    conn.close()

    handle = _ServerHandle(path)
    try:
        status, body = handle.get("/metrics")
        assert status == 503, (status, body)
        assert "schema_version" in body.get("error", ""), body
        status, body = handle.get("/health")
        assert status == 503, (status, body)
    finally:
        handle.stop()


def test_post_is_405_and_unknown_path_is_404():
    conn, path = _fresh_conn()
    conn.close()
    handle = _ServerHandle(path)
    try:
        assert handle.request("POST", "/metrics") == 405
        assert handle.request("PUT", "/health") == 405
        status, body = handle.get("/board")
        assert status == 404, (status, body)
    finally:
        handle.stop()


def test_metrics_and_health_are_200_with_expected_top_level_keys():
    conn, path = _fresh_conn()
    conn.close()
    handle = _ServerHandle(path)
    try:
        status, body = handle.get("/metrics")
        assert status == 200, body
        for key in ("verdicts_recorded_disposition", "verified_fixes_vs_silence",
                    "median_needs_human_to_decision_hours", "verified_unattended_fixes_per_week",
                    "poller_ages", "reverts_and_reopens", "history_since", "window_days"):
            assert key in body, f"missing {key}"
        status, body = handle.get("/health")
        assert status == 200, body
        assert "ok" in body
    finally:
        handle.stop()


def test_handler_serves_normally_off_a_read_only_file():
    """Structural, not a comment: chmod the ledger FILE itself to 0400 (owner
    read-only — the directory stays writable, which is what lets a read-only
    WAL connection create the `-shm` index it needs regardless of who can
    write the data) and confirm both endpoints still serve 200. If the
    handler's connection ever attempted a real write, this file's permission
    bits would turn that into a 503, not a comment turning green on its own."""
    conn, path = _fresh_conn()
    conn.close()

    file_mode = path.stat().st_mode
    path.chmod(stat.S_IRUSR)
    try:
        handle = _ServerHandle(path)
        try:
            status, _ = handle.get("/metrics")
            assert status == 200, "handler must serve fine off a file it cannot write"
            status, _ = handle.get("/health")
            assert status == 200
        finally:
            handle.stop()
    finally:
        path.chmod(file_mode)


def test_handler_connection_refuses_an_insert():
    """The precise structural guarantee behind the test above: open a
    connection EXACTLY the way api.py's `_serve()` does
    (`ledger.connect(DB_PATH, readonly=True)`) and prove SQLite itself refuses
    a write through it — the `mode=ro` URI flag, not filesystem permission
    bits, is what makes this true, and it is what a mutation dropping
    `readonly=True` from that call would break."""
    conn, path = _fresh_conn()
    conn.close()
    ro = api._ledger.connect(path, readonly=True)
    try:
        ro.execute("INSERT INTO cursors(key,value,updated_at) VALUES ('x','y','z')")
        raise AssertionError("a connection opened exactly as api.py's handler does allowed a write")
    except sqlite3.OperationalError:
        pass
    finally:
        ro.close()


# --- runner ------------------------------------------------------------------

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
