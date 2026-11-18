"""Minimal per-pair baseline: HttpToS3-style operator (page 1 only by default).

Mirrors stock ``HttpToS3Operator`` behaviour: one GET, upload the response body
(or, when ``paginate=True`` + helpers are wired, all pages into one object).
Used only in parity tests so we can show the generic path matches the good
outcome and beats the stock single-page pitfall.
"""
from __future__ import annotations

import json
from typing import Any, Callable

import requests


class HttpToObjectBaseline:
    """Pair-style operator: HTTP source hard-wired to a local/object file sink."""

    def __init__(
        self,
        *,
        endpoint: str,
        store_put: Callable[[str, bytes], None],
        key: str,
        session: requests.Session | None = None,
        params: dict[str, Any] | None = None,
        pagination: Callable[[Any], dict | None] | None = None,
        records_key: str | None = None,
        max_pages: int = 1,
    ) -> None:
        self.endpoint = endpoint
        self.store_put = store_put
        self.key = key
        self.session = session or requests.Session()
        self.params = dict(params or {})
        self.pagination = pagination
        self.records_key = records_key
        self.max_pages = max_pages
        self.request_count = 0

    def execute(self) -> dict[str, Any]:
        # Stock HttpToS3: max_pages=1, raw body. Paginated variant collects records.
        url = self.endpoint
        params: dict[str, Any] | None = dict(self.params) or None
        pages = 0
        items: list[Any] = []
        raw_last: bytes | None = None
        while url is not None and pages < self.max_pages:
            resp = self.session.get(url, params=params, timeout=30)
            resp.raise_for_status()
            self.request_count += 1
            pages += 1
            raw_last = resp.content
            body = resp.json()
            if self.records_key:
                chunk = body.get(self.records_key) or []
                items.extend(chunk)
            if self.pagination is None or pages >= self.max_pages:
                break
            nxt = self.pagination(resp)
            if nxt is None:
                break
            url = nxt.get("url", url)
            params = nxt.get("params")
        if self.records_key:
            data = json.dumps(items).encode()
            n = len(items)
        else:
            data = raw_last or b""
            n = 1
        self.store_put(self.key, data)
        return {"key": self.key, "pages": pages, "records": n, "bytes": len(data)}
