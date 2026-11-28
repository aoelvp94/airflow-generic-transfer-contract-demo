"""Per-backend subclasses of ``FileTransferOperator``. mypy must reject this file.

Not collected by pytest. ``just aip132-final-check`` runs mypy on it.
"""

from typing import Any

from aip132.file_transfer import FileTransferOperator


class S3ToGCSOperator(FileTransferOperator):
    """Override ``execute``. The class line is the type error."""

    def execute(self, context: Any) -> None:
        return None


class GCSToS3Operator(FileTransferOperator):
    """No ``execute`` override. A backend-specific constructor is still a subclass."""

    def __init__(self, *, bucket: str, **kwargs: Any) -> None:
        self.bucket = bucket
        super().__init__(**kwargs)
