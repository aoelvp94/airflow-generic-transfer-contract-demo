"""Generic copy loop: mtime, overwrite, server-side copy or stream, optional move."""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from contract import ObjectDeleter, ObjectReader, ObjectRef, ObjectWriter, ServerSideCopy, TransferItem
from listing import ListedObject


class MtimeWindow:
    def __init__(
        self,
        *,
        last_modified_time: datetime | None = None,
        maximum_modified_time: datetime | None = None,
        is_older_than: int | None = None,
    ) -> None:
        self.last_modified_time = last_modified_time
        self.maximum_modified_time = maximum_modified_time
        self.is_older_than = is_older_than

    def enabled(self) -> bool:
        return bool(self.last_modified_time or self.maximum_modified_time or self.is_older_than)

    def accept(self, last_modified: datetime | None) -> bool:
        if not self.enabled():
            return True
        if last_modified is None:
            return False
        ts = last_modified
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc)
        if self.is_older_than is not None:
            return (now - ts).total_seconds() >= self.is_older_than
        if self.last_modified_time is not None and self.maximum_modified_time is not None:
            lo, hi = self.last_modified_time, self.maximum_modified_time
            if lo.tzinfo is None:
                lo = lo.replace(tzinfo=timezone.utc)
            if hi.tzinfo is None:
                hi = hi.replace(tzinfo=timezone.utc)
            return lo <= ts <= hi
        if self.last_modified_time is not None:
            lo = self.last_modified_time
            if lo.tzinfo is None:
                lo = lo.replace(tzinfo=timezone.utc)
            return ts >= lo
        if self.maximum_modified_time is not None:
            hi = self.maximum_modified_time
            if hi.tzinfo is None:
                hi = hi.replace(tzinfo=timezone.utc)
            return ts <= hi
        return True


def transfer_items(
    items: Iterable[TransferItem],
    *,
    reader: ObjectReader,
    writer: ObjectWriter,
    copier: ServerSideCopy | None = None,
    deleter: ObjectDeleter | None = None,
    source_lister_lookup: dict[str, ListedObject] | None = None,
    mtime: MtimeWindow | None = None,
    replace: bool = True,
    move: bool = False,
    use_server_side: bool = False,
) -> list[str]:
    copied: list[str] = []
    lookup = source_lister_lookup or {}
    for item in items:
        meta = lookup.get(item.source.key)
        if mtime is not None and mtime.enabled():
            lm = meta.last_modified if meta is not None else None
            if not mtime.accept(lm):
                continue
        if use_server_side and copier is not None:
            copier.server_side_copy(item.source, item.destination)
        else:
            data = reader.read_bytes(item.source)
            try:
                writer.write_bytes(item.destination, data, overwrite=replace)
            except TypeError:
                writer.write_bytes(item.destination, data)
        if move and deleter is not None:
            deleter.delete(item.source)
        copied.append(item.destination.key)
    return copied
