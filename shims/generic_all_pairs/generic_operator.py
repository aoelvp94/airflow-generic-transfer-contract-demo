"""Generic file-transfer operator: contract only, not a pair subclass.

Named constructor arguments: 16 (below the pre-registered cap of 18,
the size of GCSToGCSOperator).
"""

from __future__ import annotations

from datetime import datetime
from typing import Any

from listing import plan_from_listed
from transfer import MtimeWindow, transfer_items


class GenericFileTransferOperator:
    """Copy objects from a source backend to a destination backend.

    Backends implement the experiment file contract. This class does not
    inherit from any Airflow pair operator and does not call pair listing
    helpers.
    """

    # 16 named public arguments — keep in sync with THRESHOLD / RESULTS.
    NAMED_ARGS = (
        "source_backend",
        "dest_backend",
        "source_bucket",
        "dest_bucket",
        "source_prefix",
        "dest_prefix",
        "replace",
        "move",
        "marker_policy",
        "path_layout",
        "match_glob",
        "exact_match",
        "server_side_copy",
        "last_modified_time",
        "maximum_modified_time",
        "is_older_than",
    )

    def __init__(
        self,
        *,
        source_backend: Any,
        dest_backend: Any,
        source_bucket: str,
        dest_bucket: str,
        source_prefix: str = "",
        dest_prefix: str = "",
        replace: bool = True,
        move: bool = False,
        marker_policy: str = "copy",
        path_layout: str = "preserve",
        match_glob: str | None = None,
        exact_match: bool = False,
        server_side_copy: str = "auto",
        last_modified_time: datetime | None = None,
        maximum_modified_time: datetime | None = None,
        is_older_than: int | None = None,
    ) -> None:
        self.source_backend = source_backend
        self.dest_backend = dest_backend
        self.source_bucket = source_bucket
        self.dest_bucket = dest_bucket
        self.source_prefix = source_prefix
        self.dest_prefix = dest_prefix
        self.replace = replace
        self.move = move
        self.marker_policy = marker_policy
        self.path_layout = path_layout
        self.match_glob = match_glob
        self.exact_match = exact_match
        self.server_side_copy = server_side_copy
        self.last_modified_time = last_modified_time
        self.maximum_modified_time = maximum_modified_time
        self.is_older_than = is_older_than

    def _use_server_side(self) -> bool:
        src, dst = self.source_backend, self.dest_backend
        same = getattr(src, "flavour", None) and src.flavour == getattr(dst, "flavour", None)
        can = same and hasattr(src, "server_side_copy")
        mode = self.server_side_copy
        if mode == "require":
            if not can:
                raise TypeError("server_side_copy=require but backends cannot copy server-side")
            return True
        if mode == "never":
            return False
        return bool(can)

    def execute(self, context: dict | None = None) -> list[str]:
        del context
        src, dst = self.source_backend, self.dest_backend
        from contract import ListQuery

        listed = src.list_objects(
            ListQuery(bucket=self.source_bucket, prefix=self.source_prefix, match_glob=self.match_glob)
        )
        items = plan_from_listed(
            listed,
            source_bucket=self.source_bucket,
            source_prefix=self.source_prefix,
            dest_bucket=self.dest_bucket,
            dest_prefix=self.dest_prefix,
            source_flavour=getattr(src, "flavour", ""),
            dest_flavour=getattr(dst, "flavour", ""),
            marker_policy=self.marker_policy,
            path_layout=self.path_layout,
            match_glob=self.match_glob,
            exact_match=self.exact_match,
            replace=self.replace,
            dest_exists=dst,
        )
        lookup = {o.key: o for o in listed}
        use_ss = self._use_server_side()
        # Same-provider copy must run on the destination backend when
        # source and dest share a flavour (rewrite / CopyObject / blob copy).
        copier = dst if use_ss and hasattr(dst, "server_side_copy") else (src if use_ss else None)
        # For GCS/S3/Azure same-account, destination.server_side_copy needs
        # to reach the source object; dest backend client must see both buckets.
        return transfer_items(
            items,
            reader=src,
            writer=dst,
            copier=copier,
            deleter=src if self.move else None,
            source_lister_lookup=lookup,
            mtime=MtimeWindow(
                last_modified_time=self.last_modified_time,
                maximum_modified_time=self.maximum_modified_time,
                is_older_than=self.is_older_than,
            ),
            replace=self.replace,
            move=self.move,
            use_server_side=use_ss,
        )
