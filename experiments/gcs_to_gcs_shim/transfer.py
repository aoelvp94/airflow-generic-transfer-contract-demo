"""Generic file-transfer path built on the prototype contract.

Listing and destination-key mapping are the caller\'s job (pair-specific
policy). This module only applies optional mtime filtering, performs
server-side copy (or would stream-copy if a caller passed reader/writer
instead), optionally deletes the source for move, and returns dest URIs.
"""

from __future__ import annotations

from typing import Any, Iterable

from contract import ObjectDeleter, ObjectRef, ServerSideCopy, TransferItem
from gcs_backend import GcsMtimeFilter
from google.api_core.exceptions import NotFound


def transfer_objects(
    items: Iterable[TransferItem],
    *,
    copier: ServerSideCopy,
    mtime_filter: GcsMtimeFilter | None = None,
    deleter: ObjectDeleter | None = None,
    move: bool = False,
    copy_extra: dict[str, Any] | None = None,
    log=None,
) -> list[str]:
    """Copy each planned item. Returns destination URIs for objects that copied.

    A rejected mtime filter skips the object (returns no URI), matching
    GCSToGCSOperator._copy_single_object. A source already-deleted on move
    is logged and still yields the destination URI.
    """
    extra = copy_extra or {}
    result: list[str] = []
    for item in items:
        uri = _copy_one(
            item,
            copier=copier,
            mtime_filter=mtime_filter,
            deleter=deleter,
            move=move,
            extra=extra,
            log=log,
        )
        if uri:
            result.append(uri)
    return result


def _copy_one(
    item: TransferItem,
    *,
    copier: ServerSideCopy,
    mtime_filter: GcsMtimeFilter | None,
    deleter: ObjectDeleter | None,
    move: bool,
    extra: dict[str, Any],
    log=None,
) -> str | None:
    source, destination = item.source, item.destination
    if mtime_filter is not None and mtime_filter.enabled() and not mtime_filter.accept(source):
        return None
    if log:
        log.info(
            "Executing copy of gs://%s/%s to gs://%s/%s",
            source.bucket,
            source.key,
            destination.bucket,
            destination.key,
        )
    copier.server_side_copy(source, destination, **extra)
    try:
        if move and deleter is not None:
            deleter.delete(source)
    except NotFound:
        if log:
            log.warning(
                "Object %s does not exist in the source bucket %s",
                source.key,
                source.bucket,
            )
    return destination.uri()
