"""stubs — the in-process `ThreadingHTTPServer` stub shared by every test
suite that needs to stand in for sideclaw, GitHub, or Slack's HTTP APIs
without a real network call.

Extracted from tests/test_clients.py (Wave 5.1) so tests/test_warden_cli.py
can drive the same recording, per-test route table against a single stub
server for sideclaw submit/get/cancel, GitHub, and Slack — rather than
reimplementing it a second time.

Usage: `StubServer({("POST", "/api/jobs"): (200, {...}), "default": (404, {...})})`.
A route value is either `(status, json_body)` or a callable
`(request) -> (status, json_body)` for a response that depends on what was
sent. Every request the server sees is recorded on `.requests` in arrival
order: `{"method", "path", "headers", "body"}`.
"""

from __future__ import annotations

import http.server
import json
import socket
import threading
import time
from typing import Any, Callable


def wait_for_socket(host: str, port: int, *, attempts: int = 200, interval: float = 0.025) -> None:
    last_err: OSError | None = None
    for _ in range(attempts):
        try:
            with socket.create_connection((host, port), timeout=interval):
                return
        except OSError as e:
            last_err = e
            time.sleep(interval)
    raise TimeoutError(f"server on {host}:{port} never came up: {last_err}")


class _RecordingHandler(http.server.BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):  # noqa: A002 — silence stdout spam
        pass

    def _handle(self) -> None:
        length = int(self.headers.get("Content-Length", 0) or 0)
        raw = self.rfile.read(length) if length else b""
        try:
            body = json.loads(raw) if raw else None
        except json.JSONDecodeError:
            body = raw.decode("utf-8", errors="replace")

        self.server.requests.append({  # type: ignore[attr-defined]
            "method": self.command,
            "path": self.path,
            "headers": dict(self.headers),
            "body": body,
        })

        route = self.server.routes.get((self.command, self.path))  # type: ignore[attr-defined]
        if route is None:
            route = self.server.routes.get("default")  # type: ignore[attr-defined]
        if callable(route):
            status, resp_body = route(self.server.requests[-1])  # type: ignore[attr-defined]
        elif route is not None:
            status, resp_body = route
        else:
            status, resp_body = 404, {"error": "no route for " + self.command + " " + self.path}

        payload = json.dumps(resp_body).encode("utf-8") if resp_body is not None else b""
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        if payload:
            self.wfile.write(payload)

    def do_GET(self):
        self._handle()

    def do_POST(self):
        self._handle()

    def do_PUT(self):
        self._handle()

    def do_PATCH(self):
        self._handle()

    def do_DELETE(self):
        self._handle()


RouteValue = tuple[int, Any] | Callable[[dict[str, Any]], tuple[int, Any]]


class StubServer:
    def __init__(self, routes: dict[tuple[str, str] | str, RouteValue]):
        self.server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), _RecordingHandler)
        self.server.routes = routes  # type: ignore[attr-defined]
        self.server.requests = []  # type: ignore[attr-defined]
        self.port = self.server.server_address[1]
        self.base = f"http://127.0.0.1:{self.port}"
        self._thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        self._thread.start()
        try:
            wait_for_socket("127.0.0.1", self.port)
        except Exception:
            self.server.shutdown()
            self.server.server_close()
            raise

    @property
    def requests(self) -> list[dict[str, Any]]:
        return self.server.requests  # type: ignore[attr-defined]

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()


def closed_port() -> int:
    """A port nothing is listening on, for the connection-refused case."""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port
