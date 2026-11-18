"""Wire-level HTTP / API request counters for S3, GCS, and Azure emulator clients."""

from __future__ import annotations

from collections import Counter
from contextlib import contextmanager
from typing import Any, Iterator


class WireCounter:
    def __init__(self) -> None:
        self.counts: Counter[str] = Counter()

    def bump(self, name: str) -> None:
        self.counts[name] += 1

    def as_dict(self) -> dict[str, int]:
        return dict(sorted(self.counts.items()))


@contextmanager
def count_s3(client: Any) -> Iterator[WireCounter]:
    counter = WireCounter()

    def _before(**kwargs: Any) -> None:
        model = kwargs.get("model")
        name = getattr(model, "name", None) or "unknown"
        counter.bump(f"s3.{name}")

    ev = client.meta.events
    ev.register("before-call.s3", _before)
    try:
        yield counter
    finally:
        ev.unregister("before-call.s3", _before)


@contextmanager
def count_gcs(client: Any) -> Iterator[WireCounter]:
    """Count HTTP methods on the GCS client's requests session."""
    counter = WireCounter()
    session = getattr(client, "_http", None)
    if session is None:
        yield counter
        return
    orig = session.request

    def wrapped(method, url, *a, **k):
        counter.bump(f"gcs.{str(method).upper()}")
        return orig(method, url, *a, **k)

    session.request = wrapped
    try:
        yield counter
    finally:
        session.request = orig


@contextmanager
def count_azure(client: Any) -> Iterator[WireCounter]:
    counter = WireCounter()
    try:
        from azure.core.pipeline.policies import SansIOHTTPPolicy

        class Policy(SansIOHTTPPolicy):
            def on_request(self, request):  # type: ignore[no-untyped-def]
                http = request.http_request
                counter.bump(f"azure.{getattr(http, 'method', 'UNKNOWN')}")
                return super().on_request(request)

        policies = list(client._pipeline._impl_policies)
        policies.insert(0, Policy())
        client._pipeline._impl_policies = policies
        try:
            yield counter
        finally:
            client._pipeline._impl_policies = policies[1:]
    except Exception:
        yield counter


def count_backend(backend, flavour: str):
    """Return a context manager that counts the backend's client."""
    fl = flavour.lower()
    if fl == "s3":
        return count_s3(backend.client)
    if fl == "gcs":
        return count_gcs(backend.client)
    if fl == "azure":
        return count_azure(backend.client)
    raise ValueError(flavour)
