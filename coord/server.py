"""Minimal HTTP facade over the coordinator (stdlib only).

Endpoints
---------
POST /tasks                 create + dispatch attempt #1
POST /callbacks             downstream callback intake (out-of-order safe)
GET  /tasks/{id}            task + attempts + fragments + audit trail
POST /internal/tick         run one worker sweep (also runs on a background loop)
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from .coordinator import CallbackEvent, Coordinator
from .worker import Worker


class _Handler(BaseHTTPRequestHandler):
    server_version = "coord/1.0"

    def log_message(self, fmt: str, *args: Any) -> None:  # quiet by default
        pass

    def _send(self, code: int, body: dict) -> None:
        data = json.dumps(body, ensure_ascii=False).encode("utf-8")
        self.send_response(code)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def _body(self) -> dict:
        length = int(self.headers.get("Content-Length", 0))
        if not length:
            return {}
        return json.loads(self.rfile.read(length))

    # ------------------------------------------------------------------ #
    def do_POST(self) -> None:
        app: Coordinator = self.server.coord  # type: ignore[attr-defined]
        try:
            body = self._body()
            if self.path == "/tasks":
                task = app.create_task(
                    body["task_id"], body["op_type"], body.get("payload", {})
                )
                self._send(201, {"task": task})
            elif self.path == "/callbacks":
                ev = CallbackEvent(
                    task_id=body["task_id"],
                    attempt_no=int(body["attempt_no"]),
                    callback_id=body["callback_id"],
                    status=body["status"],
                    detail_n=body.get("detail_n"),
                    checksum=body.get("checksum"),
                    fragment_seq=body.get("fragment_seq"),
                    fragment=body.get("fragment"),
                    error_code=body.get("error_code"),
                    downstream_ref=body.get("downstream_ref"),
                    raw=body,
                )
                result, task = app.record_callback(ev)
                self._send(200, {"result": result.value, "task": task})
            elif self.path == "/internal/tick":
                self._send(200, self.server.worker.tick())  # type: ignore[attr-defined]
            else:
                self._send(404, {"error": "not found"})
        except KeyError as exc:
            self._send(400, {"error": f"missing field: {exc.args[0]}"})
        except Exception as exc:  # noqa: BLE001
            self._send(500, {"error": str(exc)})

    def do_GET(self) -> None:
        app: Coordinator = self.server.coord  # type: ignore[attr-defined]
        if self.path.startswith("/tasks/"):
            task_id = self.path.split("/", 2)[2]
            state = app.full_state(task_id)
            if not state:
                self._send(404, {"error": "unknown task"})
                return
            state["materialized_detail"] = app.materialized_detail(task_id)
            self._send(200, state)
        elif self.path == "/health":
            self._send(200, {"ok": True})
        else:
            self._send(404, {"error": "not found"})


def build_server(coord: Coordinator, host: str = "127.0.0.1",
                 port: int = 8080, *, auto_worker: bool = True
                 ) -> ThreadingHTTPServer:
    server = ThreadingHTTPServer((host, port), _Handler)
    server.coord = coord  # type: ignore[attr-defined]
    worker = Worker(coord)
    server.worker = worker  # type: ignore[attr-defined]
    if auto_worker:
        worker.start()
    server._coord_lock = threading.Lock()  # type: ignore[attr-defined]
    return server
