"""``PrefixFileTransferOperator`` is the same final class, not a subclass.

Not collected by pytest. ``just aip132-final-check`` expects mypy to accept this file.
"""

from aip132.file_transfer import FileTransferOperator, PrefixFileTransferOperator

operator_type: type[FileTransferOperator] = PrefixFileTransferOperator
