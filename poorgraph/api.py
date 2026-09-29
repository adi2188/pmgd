"""Small HTTP API for the graph service."""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any
from urllib.parse import urlparse

from .schema import SchemaError
from .storage import GraphConflict, GraphStore

MAX_BODY_BYTES = 2 * 1024 * 1024


def build_handler(store: GraphStore) -> type[BaseHTTPRequestHandler]:
    class GraphHandler(BaseHTTPRequestHandler):
        server_version = "poorgraph/0.1"

        def do_GET(self) -> None:
            path = urlparse(self.path).path
            try:
                if path == "/health":
                    self._send_json(200, {"ok": True})
                elif path == "/schema":
                    self._send_json(200, store.schema())
                elif path == "/stats":
                    self._send_json(200, store.stats())
                elif path == "/algorithms":
                    self._send_json(200, {"algorithms": store.algorithms()})
                else:
                    self._send_json(404, {"error": f"unknown endpoint: {path}"})
            except Exception as exc:
                self._handle_error(exc)

        def do_POST(self) -> None:
            path = urlparse(self.path).path
            try:
                body = self._read_json()
                if path == "/bulk-load":
                    self._send_json(200, store.bulk_load(body))
                elif path == "/cdc":
                    self._send_json(200, store.apply_cdc(body))
                elif path.startswith("/query/"):
                    self._send_json(200, store.query(path.rsplit("/", 1)[-1], body))
                else:
                    self._send_json(404, {"error": f"unknown endpoint: {path}"})
            except Exception as exc:
                self._handle_error(exc)

        def log_message(self, fmt: str, *args: Any) -> None:
            return

        def _read_json(self) -> dict[str, Any]:
            transfer_encoding = self.headers.get("Transfer-Encoding", "").lower()
            if "chunked" in transfer_encoding:
                raise ValueError("chunked request bodies are not supported; send Content-Length")
            length_header = self.headers.get("Content-Length")
            if length_header is None:
                raise ValueError("Content-Length is required")
            length = int(length_header)
            if length > MAX_BODY_BYTES:
                raise ValueError(f"request body exceeds {MAX_BODY_BYTES} bytes")
            if length == 0:
                return {}
            try:
                data = json.loads(self.rfile.read(length).decode("utf-8"))
            except json.JSONDecodeError as exc:
                raise ValueError(f"invalid JSON: {exc}") from exc
            if not isinstance(data, dict):
                raise ValueError("request body must be a JSON object")
            return data

        def _send_json(self, status: int, payload: dict[str, Any]) -> None:
            encoded = json.dumps(payload, indent=2, sort_keys=True).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def _handle_error(self, exc: Exception) -> None:
            if isinstance(exc, KeyError):
                self._send_json(404, {"error": str(exc)})
            elif isinstance(exc, GraphConflict):
                self._send_json(409, {"error": str(exc)})
            elif isinstance(exc, (SchemaError, ValueError)):
                self._send_json(400, {"error": str(exc)})
            else:
                self._send_json(500, {"error": str(exc)})

    return GraphHandler


def run_server(db_path: str, host: str, port: int) -> None:
    store = GraphStore(db_path)
    server = ThreadingHTTPServer((host, port), build_handler(store))
    print(f"poorgraph listening on http://{host}:{port} using {db_path}")
    try:
        server.serve_forever()
    finally:
        store.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the poorgraph HTTP server")
    parser.add_argument("--db", default="poorgraph.db", help="SQLite database path")
    parser.add_argument("--host", default="127.0.0.1", help="Bind host")
    parser.add_argument("--port", default=8000, type=int, help="Bind port")
    args = parser.parse_args()
    run_server(args.db, args.host, args.port)


if __name__ == "__main__":
    main()
