"""Wire-level HTTP request counter for the emulator benches.

Counts *real* HTTP requests (not logical get/put labels) by patching the two
transports every client in this repo ends up on:

* ``aiohttp.ClientSession._request`` -- s3fs (aiobotocore), gcsfs, adlfs (azure aio)
* ``urllib3.connectionpool.HTTPConnectionPool.urlopen`` -- boto3, google-cloud-storage
  (requests), azure-storage-blob sync (requests)

Requests are bucketed by emulator port -> provider and by a coarse operation
label (method + query hint) so a reviewer can see *which* calls dominate.

``set_latency_ms(x)`` additionally injects ``x`` ms before **every** HTTP
request (``asyncio.sleep`` on aiohttp so async concurrency is preserved,
``time.sleep`` on urllib3 so it blocks the calling thread like a real RTT).
That models per-request round-trip time for listing, HEAD, GET and PUT alike,
which is what makes request count and concurrency matter on real clouds.
"""
from __future__ import annotations

import asyncio
import threading
import time
from collections import Counter
from contextlib import contextmanager
from urllib.parse import urlsplit

PORT_PROVIDER = {19090: "s3", 14443: "gcs", 10000: "azure"}

_lock = threading.Lock()
_counts: Counter = Counter()
_installed = False
_latency_s = 0.0


def set_latency_ms(ms: float) -> None:
    global _latency_s
    _latency_s = max(0.0, float(ms)) / 1000.0


def _label(method: str, url: str) -> str:
    parts = urlsplit(url)
    provider = PORT_PROVIDER.get(parts.port or 0, parts.hostname or "?")
    q = parts.query
    if "restype=container" in q and "comp=list" in q:
        op = "list"
    elif "list-type=2" in q or ("prefix=" in q and method == "GET" and "/o?" not in url and provider == "s3"):
        op = "list"
    elif provider == "gcs" and method == "GET" and parts.path.rstrip("/").endswith("/o"):
        op = "list"
    elif method == "HEAD":
        op = "head"
    elif provider == "gcs" and method == "GET" and "alt=media" not in q:
        op = "metadata"
    elif method == "GET":
        op = "get"
    elif method in ("PUT", "POST"):
        op = "put"
    else:
        op = method.lower()
    return f"{provider}:{op}"


def _bump(method: str, url: str) -> None:
    with _lock:
        _counts[_label(method, str(url))] += 1


def install() -> None:
    global _installed
    if _installed:
        return
    import aiohttp
    import urllib3.connectionpool as cp

    orig_req = aiohttp.ClientSession._request

    async def _request(self, method, str_or_url, *a, **k):  # type: ignore[no-untyped-def]
        _bump(method, str(str_or_url))
        if _latency_s:
            await asyncio.sleep(_latency_s)
        return await orig_req(self, method, str_or_url, *a, **k)

    aiohttp.ClientSession._request = _request  # type: ignore[assignment]

    orig_urlopen = cp.HTTPConnectionPool.urlopen

    def urlopen(self, method, url, *a, **k):  # type: ignore[no-untyped-def]
        full = url if "://" in url else f"{self.scheme}://{self.host}:{self.port}{url}"
        _bump(method, full)
        if _latency_s:
            time.sleep(_latency_s)
        return orig_urlopen(self, method, url, *a, **k)

    cp.HTTPConnectionPool.urlopen = urlopen  # type: ignore[assignment]
    _installed = True


def reset() -> None:
    with _lock:
        _counts.clear()


def snapshot() -> dict[str, int]:
    with _lock:
        return dict(sorted(_counts.items()))


@contextmanager
def counting():
    install()
    reset()
    box: dict[str, int] = {}
    try:
        yield box
    finally:
        box.update(snapshot())
