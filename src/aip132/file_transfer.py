"""Thin ``FileTransferOperator`` for an AIP-132 prefix copy.

Listing, marker classification, the copy plan, and the bytes live in
:mod:`aip132.path_ops`. OpenLineage lives in :mod:`aip132.lineage`. This class
resolves nothing itself: it checks the arguments a Dag author passed, forwards
them to :func:`aip132.path_ops.copy_path`, and emits lineage. There is no
per-backend branch here.

``PrefixFileTransferOperator`` is this class. Existing Dags and imports keep
that name. Upstream, ``FileTransferOperator`` grows these arguments and the
extra subclass goes away. This module subclasses the installed operator only
because that class is not edited in site-packages.

.. code-block:: python

    from aip132.file_transfer import FileTransferOperator

    copy = FileTransferOperator(
        task_id="copy_sales",
        src="s3://src/sales/",
        dst="gs://dst/sales/",
        source_conn_id="aws",
        dest_conn_id="gcp",
        overwrite=True,
    )
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from airflow.providers.common.io.operators.file_transfer import (
    FileTransferOperator as _FileTransferOperator,
)
from airflow.sdk import ObjectStoragePath

from aip132.lineage import complete_facets, record_copied, start_facets
from aip132.path_ops import (
    DEFAULT_CHUNK_SIZE,
    FOLDER_MARKER_POLICIES,
    child_path,
    classify_folder_markers,
    copy_files,
    copy_object,
    copy_path,
    copy_prefix,
    copy_single_file,
    list_entries,
    list_prefix_once,
    plan_prefix_copy,
    resolve_storage_path,
    stream_copy,
)

# Re-export path helpers so existing ``from aip132.file_transfer import ...``
# call sites keep working. copy_files is an alias of copy_path.


class FileTransferOperator(_FileTransferOperator):
    """Copy one object or a prefix. The copy itself is :func:`aip132.path_ops.copy_path`.

    A single-file ``src`` matches the installed operator: ``overwrite`` false
    raises when the destination file exists, the bytes move through
    ``ObjectStoragePath.copy``, and ``execute`` returns ``None``.

    A prefix (trailing slash, or a directory) returns the copied destination
    keys. ``folder_markers`` defaults to ``skip``. ``overwrite`` false skips
    destination keys that already exist. ``keep_structure`` false flattens to
    basenames. ``match_glob`` filters the relative key and its basename.

    :param src: Source file or prefix. A prefix should end with ``/``.
    :param dst: Destination file or prefix.
    :param source_conn_id: Connection id for ``src`` when ``src`` is a string.
    :param dest_conn_id: Connection id for ``dst`` when ``dst`` is a string.
    :param match_glob: Optional fnmatch pattern. ``None`` copies every object.
    :param overwrite: When false, skip existing prefix keys. Single files still raise.
    :param keep_structure: When true, keep the relative path under ``dst``.
    :param folder_markers: ``skip`` (default) or ``copy``.
    :param chunk_size: Bytes per read on the cross-store stream. Default 1 MiB.
    """

    template_fields: Sequence[str] = (
        "src",
        "dst",
        "source_conn_id",
        "dest_conn_id",
        "match_glob",
    )

    def __init__(
        self,
        *,
        src: str | ObjectStoragePath,
        dst: str | ObjectStoragePath,
        source_conn_id: str | None = None,
        dest_conn_id: str | None = None,
        match_glob: str | None = None,
        overwrite: bool = False,
        keep_structure: bool = True,
        folder_markers: str = "skip",
        chunk_size: int = DEFAULT_CHUNK_SIZE,
        **kwargs: Any,
    ) -> None:
        if folder_markers not in FOLDER_MARKER_POLICIES:
            raise ValueError(f"folder_markers must be 'skip' or 'copy', got {folder_markers!r}")
        if chunk_size <= 0:
            raise ValueError(f"chunk_size must be positive, got {chunk_size}")
        super().__init__(
            src=src,
            dst=dst,
            source_conn_id=source_conn_id,
            dest_conn_id=dest_conn_id,
            overwrite=overwrite,
            **kwargs,
        )
        # Parent stores the destination connection as ``dst_conn_id``.
        self.dest_conn_id = dest_conn_id
        self.match_glob = match_glob
        self.keep_structure = keep_structure
        self.folder_markers = folder_markers
        self.chunk_size = chunk_size

    def execute(self, context: Any) -> list[str] | None:
        pairs: list[tuple[ObjectStoragePath, ObjectStoragePath]] = []
        result = copy_path(
            self.src,
            self.dst,
            source_conn_id=self.source_conn_id,
            dest_conn_id=self.dest_conn_id,
            match_glob=self.match_glob,
            overwrite=self.overwrite,
            keep_structure=self.keep_structure,
            folder_markers=self.folder_markers,
            chunk_size=self.chunk_size,
            lineage=pairs,
        )
        record_copied(self, pairs, result)
        return result

    def get_openlineage_facets_on_start(self) -> Any:
        """Name the configured source and destination. Does not list."""
        return start_facets(self.src, self.dst)

    def get_openlineage_facets_on_complete(self, task_instance: Any = None) -> Any:
        """Name each object that was copied, or the start datasets if unknown."""
        return complete_facets(self, task_instance)


# Backwards-compatible name. Same class, not a second implementation.
PrefixFileTransferOperator = FileTransferOperator
