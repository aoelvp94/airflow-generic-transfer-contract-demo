"""Invented APIs (Acme Orders, Globex CRM, Initech Events, Hooli Feeds) on http.server."""
from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

ORDERS = [{"id": i, "product": "Acme Widget"} for i in range(1, 26)]          # 25
CUSTOMERS = [{"id": i, "name": f"Globex customer {i}"} for i in range(1, 12)]  # 11
EVENTS = [{"id": str(i), "type": "click"} for i in range(990, 1006)]           # "990".."1005"
FEEDS = [{"id": i, "title": f"Hooli post {i}"} for i in range(1, 8)]           # 7


class Handler(BaseHTTPRequestHandler):
    def log_message(self, *a):
        pass

    def _json(self, body, status=200):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def do_GET(self):
        u = urlparse(self.path)
        q = {k: v[0] for k, v in parse_qs(u.query).items()}
        base = f"http://{self.headers['Host']}"
        if u.path == "/orders":
            off, lim = int(q.get("offset", 0)), int(q.get("limit", 10))
            chunk = ORDERS[off:off + lim]
            return self._json({"data": chunk, "has_more": off + lim < len(ORDERS)})
        if u.path == "/customers":
            page, size = int(q.get("page", 1)), int(q.get("page_size", 5))
            total = -(-len(CUSTOMERS) // size)
            return self._json({"results": CUSTOMERS[(page - 1) * size:page * size], "total_pages": total})
        if u.path == "/events":
            lim = int(q.get("limit", 5))
            after = q.get("after")
            rows = [e for e in EVENTS if after is None or int(e["id"]) >= int(after)]  # inclusive
            return self._json({"events": rows[:lim]})
        if u.path == "/feeds":
            lim, start = int(q.get("limit", 3)), int(q.get("start", 0))
            chunk = FEEDS[start:start + lim]
            nxt = f"{base}/feeds?limit={lim}&start={start + lim}" if start + lim < len(FEEDS) else None
            return self._json({"items": chunk, "page": {"next": nxt}})
        if u.path == "/feeds/trap":
            return self._json({"items": FEEDS[:2], "page": {"next": "http://evil.invalid/feeds?start=2"}})
        self._json({"error": "not found"}, 404)


def start(port: int = 0):
    server = ThreadingHTTPServer(("127.0.0.1", port), Handler)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server, f"http://127.0.0.1:{server.server_address[1]}"
