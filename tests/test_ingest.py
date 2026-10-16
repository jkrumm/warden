#!/usr/bin/env python3
"""Regression suite for warden-api's ingest door: POST /ingest/kuma and POST /ingest/hyperdx
(scripts/api.py is the HTTP shell, scripts/lifecycle/ingest.py the logic).

Real HTTP against a ThreadingHTTPServer on an ephemeral port and a throwaway migrated ledger. The
token resolver is replaced with a stub (`api._fetch_ingest_token`), so nothing here runs `secrets-run`.
No test touches ~/.warden/warden.db or port 7735.

Run: .venv/bin/python3 tests/test_ingest.py
"""

from __future__ import annotations

import contextlib
import datetime as dt
import http.client
import http.server
import importlib.util
import io
import json
import socket
import sqlite3
import sys
import tempfile
import threading
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
api = _load("api", "scripts/api.py")   # puts scripts/ on sys.path

from lifecycle import ingest as ingest_mod  # noqa: E402
from lifecycle import intake as lifecycle_intake  # noqa: E402
from loop import core, intake as loop_intake  # noqa: E402

TOKEN = "tok-9f2c1e7a-do-not-log"
NOW = dt.datetime(2026, 10, 16, 12, 0, tzinfo=dt.timezone.utc)


def _tmp_db() -> Path:
    return Path(tempfile.mkdtemp(prefix="ingest-test-")) / "warden.db"


def _fresh() -> Path:
    path = _tmp_db()
    ledger.connect(path, migrate=True).close()
    return path


def _rows(path: Path, where: str = "1=1", args: tuple = ()) -> list[sqlite3.Row]:
    conn = ledger.connect(path, readonly=True)
    try:
        return conn.execute(f"SELECT * FROM events WHERE {where} ORDER BY id", args).fetchall()
    finally:
        conn.close()


def _exec(path: Path, sql: str, args: tuple = ()) -> None:
    conn = ledger.connect(path)
    try:
        conn.execute(sql, args)
        conn.commit()
    finally:
        conn.close()


class _Door:
    """A running warden-api against `db_path`, with the ingest token stubbed to `token`."""

    def __init__(self, db_path: Path, token: str = TOKEN):
        self.token = token
        self.fetches = 0
        self._saved = (api.DB_PATH, api._fetch_ingest_token, api._token_cache, api._token_tried_at, api._monotonic)
        api.DB_PATH = db_path
        api._token_cache, api._token_tried_at = "", None

        def _fetch() -> str:
            self.fetches += 1
            return self.token
        api._fetch_ingest_token = _fetch
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), api.Handler)
        self.port = self.server.server_address[1]
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()

    def close(self) -> None:
        self.server.shutdown()
        self.server.server_close()
        api.DB_PATH, api._fetch_ingest_token, api._token_cache, api._token_tried_at, api._monotonic = self._saved

    def request(self, method: str, path: str, body: bytes | dict | list | None = None, *,
                auth: str | None = "default", headers: dict[str, str] | None = None) -> tuple[int, dict]:
        hdrs = dict(headers or {})
        if auth == "default":
            hdrs["Authorization"] = f"Bearer {self.token}"
        elif auth is not None:
            hdrs["Authorization"] = auth
        payload = body if isinstance(body, (bytes, type(None))) else json.dumps(body).encode()
        conn = http.client.HTTPConnection("127.0.0.1", self.port, timeout=10)
        try:
            conn.request(method, path, body=payload, headers=hdrs)
            resp = conn.getresponse()
            raw = resp.read()
            return resp.status, json.loads(raw.decode()) if raw else {}
        finally:
            conn.close()

    def post(self, path: str, body=None, **kw) -> tuple[int, dict]:
        return self.request("POST", path, body, **kw)

    def raw(self, data: bytes) -> tuple[int, dict]:
        """A hand-written request, for the shapes http.client will not produce."""
        with socket.create_connection(("127.0.0.1", self.port), timeout=10) as s:
            s.sendall(data)
            s.shutdown(socket.SHUT_WR)
            buf = b""
            while chunk := s.recv(65536):
                buf += chunk
        head, _, rest = buf.partition(b"\r\n\r\n")
        try:
            status = int(head.split(b" ", 2)[1])
        except (ValueError, IndexError):   # HTTP/0.9-style reply: no status line at all
            return 0, {}
        try:
            return status, json.loads(rest.decode())
        except ValueError:                 # the stdlib's own HTML error page for a request it cannot parse
            return status, {}


@contextlib.contextmanager
def door(token: str = TOKEN, db: Path | None = None):
    path = db or _fresh()
    d = _Door(path, token)
    d.db = path
    try:
        yield d
    finally:
        d.close()


def kuma_body(status: int = 0, *, mid=204, name="Weatherorb Push", url="https://example.com/x", mtype="push",
              tags=None, **extra) -> dict:
    monitor = {"id": mid, "name": name, "url": url, "type": mtype, "tags": tags if tags is not None else []}
    monitor.update(extra)
    return {"heartbeat": {"status": status, "msg": "x", "time": "2026-10-16 12:00:00"}, "monitor": monitor,
            "msg": f"[{name}] [Down] x"}


def hyperdx_body(alert="Error rate high", state="ALERT", **extra) -> dict:
    return {"alert": alert, "state": state, **extra}


# --- auth ---------------------------------------------------------------------------

def test_auth_no_header_wrong_token_and_no_token_write_nothing():
    with door() as d:
        assert d.post("/ingest/kuma", kuma_body(), auth=None)[0] == 401
        assert d.post("/ingest/kuma", kuma_body(), auth="Bearer not-the-token")[0] == 401
        assert d.post("/ingest/kuma", kuma_body(), auth="Basic " + TOKEN)[0] == 401
        assert d.post("/ingest/kuma", kuma_body(), auth=TOKEN)[0] == 401
        assert d.post("/ingest/hyperdx", hyperdx_body(), auth="Bearer ")[0] == 401
        assert _rows(d.db) == []
        status, body = d.post("/ingest/kuma", kuma_body())
        assert status == 202 and body["action"] == "opened", (status, body)
    with door(token="") as d:
        status, body = d.post("/ingest/kuma", kuma_body(), auth="Bearer anything")
        assert status == 503 and "error" in body, (status, body)
        assert d.post("/ingest/hyperdx", hyperdx_body(), auth=None)[0] == 503
        assert _rows(d.db) == []


def test_auth_is_decided_before_the_body_is_read_or_parsed():
    with door() as d:
        assert d.post("/ingest/kuma", b"{not json", auth=None)[0] == 401
        assert d.post("/ingest/kuma", b"{not json", auth="Bearer wrong")[0] == 401
        # An over-cap length is a 413 only for an authorized caller.
        assert d.raw(b"POST /ingest/kuma HTTP/1.0\r\nContent-Length: 999999\r\n\r\n")[0] == 401
        assert d.raw(b"POST /ingest/kuma HTTP/1.0\r\n\r\n")[0] == 401


def test_a_non_ascii_authorization_header_is_a_401_not_a_crash():
    with door() as d:
        status, _ = d.post("/ingest/kuma", kuma_body(), auth="Bearer töken")
        assert status == 401, status


def test_token_is_resolved_lazily_cached_and_an_empty_result_retried_once_a_minute():
    with door(token="") as d:
        clock = [1000.0]
        api._monotonic = lambda: clock[0]
        assert d.fetches == 0, "nothing resolves before the first ingest request"
        assert d.post("/ingest/kuma", kuma_body())[0] == 503 and d.fetches == 1
        clock[0] += 59
        assert d.post("/ingest/kuma", kuma_body())[0] == 503 and d.fetches == 1, "inside 60 s: no second attempt"
        d.token = TOKEN                                       # the secret got seeded; no restart
        clock[0] += 2
        status, _ = d.post("/ingest/kuma", kuma_body())
        assert status == 202 and d.fetches == 2, (status, d.fetches)
        assert d.post("/ingest/hyperdx", hyperdx_body())[0] == 202 and d.fetches == 2, "a non-empty token is cached"


def test_the_token_comparison_is_constant_time():
    src = (REPO / "scripts" / "api.py").read_text()
    assert "hmac.compare_digest" in src


# --- body ---------------------------------------------------------------------------

def test_body_cap_413_over_and_accepted_at_the_cap():
    with door() as d:
        over = b"{" + b" " * (api.INGEST_MAX_BODY_BYTES) + b"}"
        assert len(over) > api.INGEST_MAX_BODY_BYTES
        status, body = d.post("/ingest/kuma", over)
        assert status == 413 and "error" in body, (status, body)
        # Exactly at the cap: valid JSON padded to 65536 bytes.
        base = kuma_body()
        base["pad"] = ""
        pad = api.INGEST_MAX_BODY_BYTES - len(json.dumps(base).encode())
        base["pad"] = "x" * pad
        raw = json.dumps(base).encode()
        assert len(raw) == api.INGEST_MAX_BODY_BYTES, len(raw)
        assert d.post("/ingest/kuma", raw)[0] == 202
        assert len(_rows(d.db)) == 1


def test_missing_or_malformed_length_and_chunked_bodies_are_400():
    with door() as d:
        auth = f"Authorization: Bearer {TOKEN}\r\n".encode()
        assert d.raw(b"POST /ingest/kuma HTTP/1.0\r\n" + auth + b"\r\n")[0] == 400
        assert d.raw(b"POST /ingest/kuma HTTP/1.0\r\n" + auth + b"Content-Length: abc\r\n\r\n{}")[0] == 400
        assert d.raw(b"POST /ingest/kuma HTTP/1.0\r\n" + auth + b"Content-Length: -5\r\n\r\n{}")[0] == 400
        # More digits than int() will convert is a 400, not a dropped connection.
        assert d.raw(b"POST /ingest/kuma HTTP/1.0\r\n" + auth + b"Content-Length: " + b"9" * 5000 + b"\r\n\r\n{}")[0] == 400
        chunked = (b"POST /ingest/kuma HTTP/1.1\r\n" + auth + b"Transfer-Encoding: chunked\r\n\r\n"
                   b"2\r\n{}\r\n0\r\n\r\n")
        assert d.raw(chunked)[0] == 400
        # A body shorter than its declared length is a 400, not a hang or a 500.
        assert d.raw(b"POST /ingest/kuma HTTP/1.0\r\n" + auth + b"Content-Length: 50\r\n\r\n{}")[0] == 400
        assert _rows(d.db) == []


def test_invalid_json_and_non_object_bodies_are_400():
    with door() as d:
        for raw in (b"", b"{not json", b"[]", b"42", b'"str"', b"null", b"\xff\xfe", b"[" * 40000):
            status, body = d.post("/ingest/kuma", raw)
            assert status == 400 and "error" in body, (raw[:20], status, body)
        assert _rows(d.db) == []


# --- /ingest/kuma ----------------------------------------------------------------------

def test_kuma_down_opens_an_event_in_the_poll_shape():
    with door() as d:
        tags = [{"name": "repo", "value": "weatherorb", "color": "#fff"}]
        status, body = d.post("/ingest/kuma", kuma_body(0, mid=204, tags=tags))
        assert status == 202 and body["accepted"] is True and body["action"] == "opened", (status, body)
        (row,) = _rows(d.db)
        assert body["event_id"] == row["id"]
        assert (row["source"], row["external_id"], row["title"], row["url"]) == (
            "uk", "204", "Weatherorb Push", "https://example.com/x")
        assert row["resolved_at"] is None and row["notified_at"] is None and row["reminder_count"] == 0
        assert json.loads(row["payload_json"]) == {"type": "push", "status": 0, "tags": tags, "via": "webhook"}


def test_kuma_down_without_tags_has_no_tags_key():
    with door() as d:
        d.post("/ingest/kuma", kuma_body(0, tags=[]))
        assert "tags" not in json.loads(_rows(d.db)[0]["payload_json"])


def test_kuma_same_payload_twice_is_one_row_and_the_second_only_refreshes():
    with door() as d:
        s1, b1 = d.post("/ingest/kuma", kuma_body(0))
        first = _rows(d.db)[0]
        _exec(d.db, "UPDATE events SET notified_at=?, last_reminder_at=?, reminder_count=2 WHERE id=?",
              ("2026-10-16T11:00:00+00:00", "2026-10-16T11:30:00+00:00", first["id"]))
        s2, b2 = d.post("/ingest/kuma", kuma_body(0, name="Weatherorb Push (renamed)"))
        assert (s1, s2) == (202, 202) and b2["action"] == "refreshed" and b2["event_id"] == b1["event_id"], (b1, b2)
        (row,) = _rows(d.db)
        assert row["title"] == "Weatherorb Push (renamed)"
        assert row["first_seen"] == first["first_seen"], "a refresh never restarts first_seen"
        assert row["notified_at"] == "2026-10-16T11:00:00+00:00" and row["reminder_count"] == 2
        assert row["last_reminder_at"] == "2026-10-16T11:30:00+00:00"


def test_kuma_up_resolves_the_open_row_and_is_a_no_op_otherwise():
    with door() as d:
        status, body = d.post("/ingest/kuma", kuma_body(1, mid=7))
        assert (status, body) == (202, {"accepted": False, "action": "ignored"}), (status, body)
        assert _rows(d.db) == [], "an UP for a monitor warden never saw DOWN writes nothing"
        d.post("/ingest/kuma", kuma_body(0, mid=7))
        status, body = d.post("/ingest/kuma", kuma_body(1, mid=7))
        assert status == 202 and body["accepted"] is True and body["action"] == "resolved", body
        (row,) = _rows(d.db)
        assert row["resolved_at"] is not None
        stamp = row["resolved_at"]
        status, body = d.post("/ingest/kuma", kuma_body(1, mid=7))
        assert status == 202 and body["accepted"] is False and body["action"] == "ignored", body
        assert _rows(d.db)[0]["resolved_at"] == stamp, "a repeated UP does not move resolved_at"


def test_kuma_down_after_resolve_reopens_like_reconcile():
    with door() as d:
        d.post("/ingest/kuma", kuma_body(0))
        eid = _rows(d.db)[0]["id"]
        _exec(d.db, "UPDATE events SET notified_at='n', last_reminder_at='r', reminder_count=3, first_seen='old' WHERE id=?",
              (eid,))
        d.post("/ingest/kuma", kuma_body(1))
        status, body = d.post("/ingest/kuma", kuma_body(0, name="Back Again"))
        assert status == 202 and body["action"] == "opened" and body["event_id"] == eid, body
        (row,) = _rows(d.db)
        assert row["resolved_at"] is None and row["notified_at"] is None and row["last_reminder_at"] is None
        assert row["reminder_count"] == 0 and row["first_seen"] != "old" and row["title"] == "Back Again"


def test_kuma_pending_maintenance_group_and_test_ping_are_accepted_and_ignored():
    with door() as d:
        ignored = {"accepted": False, "action": "ignored"}
        for body in (kuma_body(2), kuma_body(3), kuma_body(0, mtype="group"), kuma_body(1, mtype="group"),
                     {"msg": "Sent Successfully."}, {}):
            assert d.post("/ingest/kuma", body) == (202, ignored), body
        assert _rows(d.db) == []
        # A pending beat does not resolve an open DOWN either.
        d.post("/ingest/kuma", kuma_body(0))
        d.post("/ingest/kuma", kuma_body(2))
        assert _rows(d.db)[0]["resolved_at"] is None


def test_kuma_malformed_shapes_are_400():
    with door() as d:
        no_id = kuma_body(0)
        del no_id["monitor"]["id"]
        no_name = kuma_body(0)
        del no_name["monitor"]["name"]
        bad = [no_id, no_name, kuma_body(0, name="  "), kuma_body(0, mid=""), kuma_body(0, mid=True),
               kuma_body(7), kuma_body("0"), {"heartbeat": {"status": 0}}, {"monitor": {"id": 1, "name": "x"}},
               {"heartbeat": "x", "monitor": []}]
        for body in bad:
            status, resp = d.post("/ingest/kuma", body)
            assert status == 400 and "error" in resp, (body, status, resp)
        assert _rows(d.db) == []


# --- /ingest/hyperdx ---------------------------------------------------------------------

def test_hyperdx_alert_opens_and_ok_resolves_with_our_contract():
    with door() as d:
        body = hyperdx_body("Audio Gateway error rate", "ALERT", message="5xx above 5%", url="https://example.com/a",
                            service="audio-gateway", value=12.5)
        status, resp = d.post("/ingest/hyperdx", body)
        assert status == 202 and resp["accepted"] is True and resp["action"] == "opened", resp
        (row,) = _rows(d.db)
        assert row["source"] == "hyperdx" and row["title"] == "Audio Gateway error rate"
        assert row["external_id"] == core.fingerprint("Audio Gateway error rate")
        assert row["url"] == "https://example.com/a"
        assert json.loads(row["payload_json"]) == {
            "state": "ALERT", "message": "5xx above 5%", "service": "audio-gateway", "value": 12.5, "via": "webhook"}
        status, resp = d.post("/ingest/hyperdx", hyperdx_body("Audio Gateway error rate", "OK"))
        assert status == 202 and resp["action"] == "resolved" and resp["event_id"] == row["id"], resp
        assert _rows(d.db)[0]["resolved_at"] is not None


def test_hyperdx_states_are_case_insensitive_and_accept_firing_and_resolved():
    with door() as d:
        for i, (down, up) in enumerate((("alert", "ok"), ("Firing", "Resolved"), (" FIRING ", "OK"))):
            name = f"Alert number {'abc'[i]}"
            assert d.post("/ingest/hyperdx", hyperdx_body(name, down))[1]["action"] == "opened", down
            assert d.post("/ingest/hyperdx", hyperdx_body(name, up))[1]["action"] == "resolved", up


def test_hyperdx_titles_differing_only_in_numbers_are_one_event():
    with door() as d:
        a = d.post("/ingest/hyperdx", hyperdx_body("p95 latency 812 ms over 500 at 14:03:11", "ALERT"))[1]
        b = d.post("/ingest/hyperdx", hyperdx_body("p95 latency 944 ms over 500 at 15:41:09", "ALERT"))[1]
        assert a["event_id"] == b["event_id"] and b["action"] == "refreshed", (a, b)
        (row,) = _rows(d.db)
        assert row["title"] == "p95 latency 944 ms over 500 at 15:41:09", "the title refreshes to the latest"
        # ...and the OK for a differently numbered rendering resolves that same row.
        assert d.post("/ingest/hyperdx", hyperdx_body("p95 latency 120 ms over 500 at 16:00:00", "OK"))[1]["action"] == "resolved"
        assert len(_rows(d.db)) == 1
        assert d.post("/ingest/hyperdx", hyperdx_body("A different alert", "ALERT"))[1]["action"] == "opened"
        assert len(_rows(d.db)) == 2


def test_hyperdx_identity_is_the_fingerprint_of_the_whole_name_capped_like_slack_alert():
    # normalize_title() caps every fingerprint at 120 chars (the slack_alert key too): names that differ
    # inside it are two events, whatever follows; the stored title is clipped separately at 200.
    with door() as d:
        tail = "x" * 150
        one = d.post("/ingest/hyperdx", hyperdx_body("alpha " + tail, "ALERT"))[1]
        two = d.post("/ingest/hyperdx", hyperdx_body("beta " + tail, "ALERT"))[1]
        assert one["event_id"] != two["event_id"], (one, two)
        assert d.post("/ingest/hyperdx", hyperdx_body("alpha " + tail, "OK"))[1]["action"] == "resolved"
        assert [r["id"] for r in _rows(d.db) if r["resolved_at"] is None] == [two["event_id"]]


def test_a_query_string_still_routes_to_its_endpoint():
    with door() as d:
        raw = json.dumps(kuma_body()).encode()
        req = (b"POST /ingest/kuma?x=1 HTTP/1.0\r\nAuthorization: Bearer " + TOKEN.encode()
               + b"\r\nContent-Length: " + str(len(raw)).encode() + b"\r\n\r\n" + raw)
        assert d.raw(req)[0] == 202


def test_hyperdx_same_payload_twice_is_one_row_and_reopens_after_ok():
    with door() as d:
        body = hyperdx_body("Queue depth", "ALERT", message="m")
        first = d.post("/ingest/hyperdx", body)[1]
        second = d.post("/ingest/hyperdx", body)[1]
        assert (first["action"], second["action"]) == ("opened", "refreshed")
        assert len(_rows(d.db)) == 1
        d.post("/ingest/hyperdx", hyperdx_body("Queue depth", "OK"))
        assert d.post("/ingest/hyperdx", body)[1]["action"] == "opened"
        (row,) = _rows(d.db)
        assert row["resolved_at"] is None and row["notified_at"] is None


def test_hyperdx_ok_for_an_unknown_alert_is_accepted_false():
    with door() as d:
        assert d.post("/ingest/hyperdx", hyperdx_body("Never fired", "OK")) == (
            202, {"accepted": False, "action": "ignored"})
        assert _rows(d.db) == []


def test_hyperdx_malformed_bodies_are_400():
    with door() as d:
        for body in ({"state": "ALERT"}, {"alert": "", "state": "ALERT"}, {"alert": "  ", "state": "OK"},
                     {"alert": 5, "state": "ALERT"}, {"alert": "x"}, {"alert": "x", "state": "weird"},
                     {"alert": "x", "state": 1}, {"alert": "!!!", "state": "ALERT"}):
            status, resp = d.post("/ingest/hyperdx", body)
            assert status == 400 and "error" in resp, (body, status, resp)
        assert _rows(d.db) == []


def test_hyperdx_sender_text_is_bounded():
    with door() as d:
        d.post("/ingest/hyperdx", hyperdx_body("a" * 5000, "ALERT", message="m" * 9000, service="s" * 900,
                                               url="https://example.com/" + "u" * 3000, value=["a", "list"]))
        row = _rows(d.db)[0]
        payload = json.loads(row["payload_json"])
        assert len(row["title"]) <= ingest_mod.MAX_TITLE_CHARS and len(row["url"]) <= ingest_mod.MAX_URL_CHARS
        assert len(payload["message"]) <= ingest_mod.MAX_MESSAGE_CHARS
        assert len(payload["service"]) <= ingest_mod.MAX_LABEL_CHARS and isinstance(payload["value"], str)


# --- equivalence with watchdog-poll.reconcile() ---------------------------------------------

def test_the_webhook_row_transitions_match_watchdog_polls_reconcile():
    """The same observed sequence through reconcile() (the poll's path) and through ingest.apply()
    leaves the same row at every step, so a uk event is one row whichever door touched it."""
    wp = core._watchdog_poll
    poll_db, hook_db = ledger.connect(_tmp_db(), migrate=True), ledger.connect(_tmp_db(), migrate=True)

    def cols(conn):
        r = conn.execute("SELECT source, external_id, title, url, payload_json, first_seen, notified_at, "
                         "last_reminder_at, reminder_count, resolved_at FROM events").fetchall()
        return [tuple(x) for x in r]

    def down(title, status=0):
        return {"external_id": "204", "title": title, "url": "u", "payload": {"type": "push", "status": status}}

    t = [NOW + dt.timedelta(minutes=10 * i) for i in range(8)]
    steps = [("down", "A", t[0]), ("stamp", None, None), ("down", "B", t[1]), ("up", None, t[2]),
             ("up", None, t[3]), ("down", "C", t[4]), ("down", "C", t[5]), ("up", None, t[6])]
    for kind, title, at in steps:
        if kind == "stamp":
            for c in (poll_db, hook_db):
                c.execute("UPDATE events SET notified_at='n', last_reminder_at='r', reminder_count=4")
                c.commit()
            continue
        if kind == "down":
            wp.reconcile(poll_db, "uk", [down(title)], at, gate_min=10**9, track_resolution=False)
            ev = ingest_mod.IngestEvent(kind="down", source="uk", external_id="204", title=title, url="u",
                                        payload={"type": "push", "status": 0})
        else:
            wp.reconcile(poll_db, "uk", [], at, gate_min=10**9, track_resolution=True)
            ev = ingest_mod.IngestEvent(kind="up", source="uk", external_id="204", title="A")
        poll_db.commit()
        ingest_mod.apply(hook_db, ev, at)
        assert cols(poll_db) == cols(hook_db), (kind, title, cols(poll_db), cols(hook_db))
    poll_db.close()
    hook_db.close()


# --- routing ---------------------------------------------------------------------------------

@contextlib.contextmanager
def _repos(*names: str):
    import os
    root = Path(tempfile.mkdtemp(prefix="ingest-repos-"))
    for name in names:
        (root / name).mkdir()
        (root / name / "AGENTS.md").write_text("# x\n")
    saved = os.environ.get("WARDEN_REPOS_ROOT")
    os.environ["WARDEN_REPOS_ROOT"] = str(root)
    try:
        yield
    finally:
        if saved is None:
            os.environ.pop("WARDEN_REPOS_ROOT", None)
        else:
            os.environ["WARDEN_REPOS_ROOT"] = saved


def _route(path: Path, event_id: int) -> str | None:
    conn = ledger.connect(path, readonly=True)
    try:
        ev = conn.execute("SELECT * FROM events WHERE id=?", (event_id,)).fetchone()
        return lifecycle_intake.route_by_label({"origin": "alert"}, ev)
    finally:
        conn.close()


def test_kuma_webhook_tags_route_exactly_like_the_poll_shape():
    """Kuma's webhook tags are `[{name, value, ...}]`; the poll stores whatever the Argo list carried
    (strings or `{name, value}`). Both reach the same repo through route_by_label()."""
    with _repos("weatherorb", "audio-gateway"), door() as d:
        shapes = [
            ([{"name": "repo", "value": "weatherorb", "color": "#4B5563"}], "weatherorb"),
            ([{"name": "weatherorb", "value": ""}], "weatherorb"),
            (["audio-gateway"], "audio-gateway"),
            ([{"name": "team", "value": "infra"}], None),
        ]
        for i, (tags, expected) in enumerate(shapes):
            status, resp = d.post("/ingest/kuma", kuma_body(0, mid=500 + i, tags=tags))
            assert status == 202, resp
            assert _route(d.db, resp["event_id"]) == expected, (tags, expected)
            # The same tags without "via" route identically.
            conn = ledger.connect(d.db)
            poll_payload = json.loads(conn.execute("SELECT payload_json FROM events WHERE id=?",
                                                   (resp["event_id"],)).fetchone()[0])
            poll_payload.pop("via")
            conn.execute("UPDATE events SET payload_json=? WHERE id=?", (json.dumps(poll_payload), resp["event_id"]))
            conn.commit()
            conn.close()
            assert _route(d.db, resp["event_id"]) == expected, ("poll shape", tags)


def test_hyperdx_service_routes_only_when_it_equals_a_known_repo():
    with _repos("audio-gateway"), door() as d:
        cases = [("audio-gateway", "audio-gateway"), ("Audio-Gateway", "audio-gateway"),
                 ("some-unknown-service", None), (None, None)]
        for i, (service, expected) in enumerate(cases):
            extra = {"service": service} if service else {}
            resp = d.post("/ingest/hyperdx", hyperdx_body(f"Alert {'xyz'[i % 3]}{'q' * i}", "ALERT", **extra))[1]
            assert _route(d.db, resp["event_id"]) == expected, (service, expected)


# --- the loop sees the events ---------------------------------------------------------------------

def test_a_hyperdx_event_becomes_an_alert_item_like_uk_and_resolves_to_quiet():
    assert "hyperdx" in core.INGEST_SOURCES and "uk" in core.INGEST_SOURCES
    assert "hyperdx" not in core.GROUPED_TRIAGE_SOURCES, "a state source: never silence-resolved"
    assert "hyperdx" in lifecycle_intake._SOURCE_NOTES
    assert "firing right now" in lifecycle_intake._SOURCE_NOTES["hyperdx"]
    with door() as d:
        up = d.post("/ingest/hyperdx", hyperdx_body("Disk almost full 91%", "ALERT"))[1]
        uk = d.post("/ingest/kuma", kuma_body(0))[1]
        conn = ledger.connect(d.db)
        try:
            loop_intake.ingest(conn, NOW)
            states = {r["event_id"]: (r["state"], r["signature"]) for r in
                      conn.execute("SELECT event_id, state, signature FROM triage_items")}
            assert states[up["event_id"]][0] == core.STATE_NEW and states[uk["event_id"]][0] == core.STATE_NEW, states
            assert states[up["event_id"]][1].startswith("hyperdx:")
        finally:
            conn.close()
        d.post("/ingest/hyperdx", hyperdx_body("Disk almost full 72%", "OK"))
        conn = ledger.connect(d.db)
        try:
            loop_intake.apply_resolutions(conn, NOW, {})
            row = conn.execute("SELECT state FROM triage_items WHERE event_id=?", (up["event_id"],)).fetchone()
            assert row["state"] == core.STATE_QUIET, dict(row)
            other = conn.execute("SELECT state FROM triage_items WHERE event_id=?", (uk["event_id"],)).fetchone()
            assert other["state"] == core.STATE_NEW
        finally:
            conn.close()


def test_hyperdx_match_targets_carry_the_fingerprint_once():
    with door() as d:
        resp = d.post("/ingest/hyperdx", hyperdx_body("Queue 12 deep at 14:03", "ALERT"))[1]
        conn = ledger.connect(d.db, readonly=True)
        ev = conn.execute("SELECT * FROM events WHERE id=?", (resp["event_id"],)).fetchone()
        conn.close()
        targets = core.match_targets(ev)
        assert targets == [f"hyperdx:{core.fingerprint('Queue 12 deep at 14:03')}"], targets


# --- ledger failures ---------------------------------------------------------------------------------

def test_schema_mismatch_either_way_is_503_and_writes_nothing():
    for delta in (1, -1):
        path = _fresh()
        _exec(path, "UPDATE schema_version SET version = ?", (ledger.LEDGER_SCHEMA_VERSION + delta,))
        with door(db=path) as d:
            for ep, body in (("/ingest/kuma", kuma_body(0)), ("/ingest/hyperdx", hyperdx_body())):
                status, resp = d.post(ep, body)
                assert status == 503 and "error" in resp, (delta, status, resp)
                assert str(path) not in json.dumps(resp), "the 503 body does not name the ledger file"
            assert _rows(path) == []


def test_a_missing_ledger_is_503_and_is_not_created():
    path = _tmp_db()
    with door(db=path) as d:
        status, _ = d.post("/ingest/kuma", kuma_body(0))
        assert status == 503
    assert not path.exists(), "the API never creates a ledger"


def test_ingest_never_migrates():
    path = _tmp_db()
    path.parent.mkdir(parents=True, exist_ok=True)
    sqlite3.connect(path).close()       # an empty file, no schema at all
    with door(db=path) as d:
        status, _ = d.post("/ingest/kuma", kuma_body(0))
        assert status == 503
    conn = sqlite3.connect(path)
    tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
    conn.close()
    assert tables == [], tables


def test_a_concurrent_writer_is_waited_for_not_a_500():
    path = _fresh()
    with door(db=path) as d:
        blocker = sqlite3.connect(path, check_same_thread=False)
        blocker.execute("BEGIN IMMEDIATE")

        def release():
            time.sleep(0.6)
            blocker.rollback()
            blocker.close()
        thread = threading.Thread(target=release)
        thread.start()
        started = time.monotonic()
        status, body = d.post("/ingest/kuma", kuma_body(0))
        thread.join()
        assert status == 202, (status, body)
        assert time.monotonic() - started >= 0.5, "the request must have waited on the lock"
        assert len(_rows(path)) == 1


def test_a_writer_that_never_lets_go_is_a_503_so_the_sender_retries():
    path = _fresh()
    original = api._ledger.connect

    def short_wait(*a, **kw):
        conn = original(*a, **kw)
        conn.execute("PRAGMA busy_timeout=150")
        return conn
    with door(db=path) as d:
        blocker = sqlite3.connect(path)
        blocker.execute("BEGIN IMMEDIATE")
        api._ledger.connect = short_wait
        try:
            status, body = d.post("/ingest/kuma", kuma_body(0))
        finally:
            api._ledger.connect = original
            blocker.rollback()
            blocker.close()
        assert status == 503 and "error" in body, (status, body)
        assert _rows(path) == []
        assert d.post("/ingest/kuma", kuma_body(0))[0] == 202, "and it recovers once the lock is gone"


def test_the_ingest_connection_is_writable_unmigrated_and_has_a_busy_timeout():
    path = _fresh()
    calls: list[tuple[tuple, dict]] = []
    original = api._ledger.connect
    seen: list[int] = []

    def recording(*a, **kw):
        calls.append((a, kw))
        conn = original(*a, **kw)
        if not kw.get("readonly"):
            seen.append(conn.execute("PRAGMA busy_timeout").fetchone()[0])
        return conn
    api._ledger.connect = recording
    try:
        with door(db=path) as d:
            assert d.post("/ingest/kuma", kuma_body(0))[0] == 202
            assert d.request("GET", "/board", auth=None)[0] == 200
            assert d.request("GET", "/health", auth=None)[0] == 200
    finally:
        api._ledger.connect = original
    post_calls = [c for c in calls if not c[1].get("readonly")]
    get_calls = [c for c in calls if c[1].get("readonly") is True]
    assert len(post_calls) == 1 and len(get_calls) == 2, calls
    assert post_calls[0][1].get("migrate") in (None, False), "the API never migrates"
    assert seen and all(v > 0 for v in seen), seen


def test_the_get_path_stays_read_only_with_the_ingest_door_in_the_same_process():
    path = _fresh()
    with door(db=path) as d:
        d.post("/ingest/kuma", kuma_body(0))
        ro = original = api._ledger.connect(path, readonly=True)
        try:
            try:
                ro.execute("INSERT INTO cursors(key,value,updated_at) VALUES ('x','y','z')")
                raise AssertionError("a read-only handle allowed a write")
            except sqlite3.OperationalError:
                pass
        finally:
            original.close()
        # GET endpoints see the pushed event's effects and still serve.
        assert d.request("GET", "/board", auth=None)[0] == 200


# --- the method surface ----------------------------------------------------------------------------------

def test_get_on_ingest_is_405_and_other_methods_and_paths_keep_their_answers():
    with door() as d:
        for path in ("/ingest", "/ingest/", "/ingest/kuma", "/ingest/hyperdx", "/ingest/nope"):
            assert d.request("GET", path, auth=None)[0] == 405, path
        assert d.request("PUT", "/ingest/kuma", b"{}")[0] == 405
        assert d.request("DELETE", "/ingest/kuma")[0] == 405
        assert d.post("/metrics", {})[0] == 405
        assert d.post("/nope", {})[0] == 405
        assert d.post("/ingest/nope", {})[0] == 404
        assert d.request("GET", "/nope", auth=None)[0] == 404
        assert _rows(d.db) == []


# --- the token never reaches a log ---------------------------------------------------------------------------

def test_the_token_never_appears_in_a_log_line_or_a_response():
    captured = io.StringIO()
    saved = sys.stderr
    sys.stderr = captured
    try:
        with door() as d:
            bodies = []
            bodies.append(d.post("/ingest/kuma", kuma_body(0)))
            bodies.append(d.post(f"/ingest/kuma?token={TOKEN}", kuma_body(0)))
            bodies.append(d.post("/ingest/kuma", kuma_body(0), auth="Bearer wrong-secret-value"))
            bodies.append(d.post("/ingest/kuma", b"{bad", auth=f"Bearer {TOKEN}"))
            bodies.append(d.post("/ingest/hyperdx", hyperdx_body(), auth=f"Bearer {TOKEN}x"))
            bodies.append(d.request("GET", f"/ingest/kuma?access={TOKEN}", auth=None))
            d.raw(f"POST /ingest/kuma?k={TOKEN} HTTP/1.0\r\nAuthorization: Bearer {TOKEN}\r\n\r\n".encode())
            d.raw(f"BAD {TOKEN} HTTP/1.0 extra\r\n\r\n".encode())
            time.sleep(0.2)
    finally:
        sys.stderr = saved
    log = captured.getvalue()
    assert "/ingest/kuma" in log, "sanity: the requests were logged at all"
    for secret in (TOKEN, "wrong-secret-value"):
        assert secret not in log, f"{secret!r} leaked into the log:\n{log}"
        assert secret not in json.dumps(bodies), "a response echoed a credential"
    assert "Bearer" not in log.replace("Bearer <redacted>", "")


def test_redact_removes_the_cached_token_bearer_values_and_query_strings():
    api._token_cache = "abc-secret"
    try:
        out = api._redact('"POST /ingest/kuma?x=abc-secret HTTP/1.1" 202 - Bearer abc-secret abc-secret')
        assert "abc-secret" not in out, out
    finally:
        api._token_cache = ""


# --- runner ---------------------------------------------------------------------------------------------------------

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
