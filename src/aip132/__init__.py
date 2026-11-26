"""AIP-132 phase 1 demo: prefix copy on path helpers, with a thin operator.

Importing this package imports Airflow. Install the ``aip132`` extra first.
``copy_path`` does the copy. ``FileTransferOperator`` forwards to it.
``PrefixFileTransferOperator`` is that same class.
"""

from aip132.file_transfer import FileTransferOperator, PrefixFileTransferOperator
from aip132.path_ops import (
    classify_folder_markers,
    copy_files,
    copy_object,
    copy_path,
    copy_prefix,
    copy_single_file,
    list_entries,
    list_prefix_once,
    plan_prefix_copy,
    stream_copy,
)

__all__ = [
    "FileTransferOperator",
    "PrefixFileTransferOperator",
    "classify_folder_markers",
    "copy_files",
    "copy_object",
    "copy_path",
    "copy_prefix",
    "copy_single_file",
    "list_entries",
    "list_prefix_once",
    "plan_prefix_copy",
    "stream_copy",
]
