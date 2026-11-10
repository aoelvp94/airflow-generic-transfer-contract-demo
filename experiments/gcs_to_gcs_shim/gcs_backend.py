"""GCSHook-backed implementation of the prototype file-transfer contract.

Every method delegates to the same GCSHook methods the upstream unit tests
patch (list, exists, rewrite, delete, is_older_than, is_updated_*). That is
deliberate: those tests assert hook-call shape, so a "pure" generic backend
that used different hook methods would fail even with identical behaviour.
"""

from __future__ import annotations

from typing import Any

from contract import ListQuery, ObjectRef


class GcsHookBackend:
    """Adapters around a GCSHook instance (the object tests mock)."""

    def __init__(self, hook: Any) -> None:
        self.hook = hook

    def list_objects(self, query: ListQuery) -> list[str]:
        kwargs: dict[str, Any] = {"prefix": query.prefix, "delimiter": query.delimiter}
        if not query.omit_match_glob:
            kwargs["match_glob"] = query.match_glob
        return self.hook.list(query.bucket, **kwargs)

    def exists(self, ref: ObjectRef) -> bool:
        return self.hook.exists(ref.bucket, ref.key)

    def open_reader(self, ref: ObjectRef) -> Any:
        return self.hook.download(bucket_name=ref.bucket, object_name=ref.key)

    def open_writer(self, ref: ObjectRef) -> Any:
        raise NotImplementedError("stream writer is not used by the GCSToGCS server-side path")

    def server_side_copy(self, source: ObjectRef, destination: ObjectRef, **extra: Any) -> None:
        self.hook.rewrite(source.bucket, source.key, destination.bucket, destination.key, **extra)

    def delete(self, ref: ObjectRef) -> None:
        self.hook.delete(ref.bucket, ref.key)


class GcsMtimeFilter:
    """Optional mtime capability. Precedence matches GCSToGCSOperator."""

    def __init__(
        self,
        hook: Any,
        *,
        is_older_than=None,
        last_modified_time=None,
        maximum_modified_time=None,
        log=None,
    ) -> None:
        self.hook = hook
        self.is_older_than = is_older_than
        self.last_modified_time = last_modified_time
        self.maximum_modified_time = maximum_modified_time
        self.log = log

    def enabled(self) -> bool:
        return bool(self.is_older_than or self.last_modified_time or self.maximum_modified_time)

    def accept(self, ref: ObjectRef) -> bool:
        hook = self.hook
        if self.is_older_than:
            if hook.is_older_than(ref.bucket, ref.key, self.is_older_than):
                if self.log:
                    self.log.info("Object is older than %s seconds ago", self.is_older_than)
                return True
            if self.log:
                self.log.debug("Object is not older than %s seconds ago", self.is_older_than)
            return False
        if self.last_modified_time and self.maximum_modified_time:
            if hook.is_updated_between(
                ref.bucket, ref.key, self.last_modified_time, self.maximum_modified_time
            ):
                if self.log:
                    self.log.info(
                        "Object has been modified between %s and %s",
                        self.last_modified_time,
                        self.maximum_modified_time,
                    )
                return True
            if self.log:
                self.log.debug(
                    "Object was not modified between %s and %s",
                    self.last_modified_time,
                    self.maximum_modified_time,
                )
            return False
        if self.last_modified_time is not None:
            if hook.is_updated_after(ref.bucket, ref.key, self.last_modified_time):
                if self.log:
                    self.log.info("Object has been modified after %s ", self.last_modified_time)
                return True
            if self.log:
                self.log.debug("Object was not modified after %s ", self.last_modified_time)
            return False
        if self.maximum_modified_time is not None:
            if hook.is_updated_before(ref.bucket, ref.key, self.maximum_modified_time):
                if self.log:
                    self.log.info("Object has been modified before %s ", self.maximum_modified_time)
                return True
            if self.log:
                self.log.debug("Object was not modified before %s ", self.maximum_modified_time)
            return False
        return True
