"""OpenLineage dataset names for ``PrefixFileTransferOperator``.

The operator emits one input dataset and one output dataset on start, naming
the source and destination paths it was given (the prefix, or the single
file). On complete it emits one input and one output per object that was
actually copied. Start does not list: the copy itself lists once, inside
``execute``.

Names follow the OpenLineage dataset spec
(https://openlineage.io/docs/spec/naming):

* ``s3`` and ``s3a`` -> namespace ``s3://{bucket}``, name = object key
* ``gs`` and ``gcs`` -> namespace ``gs://{bucket}``, name = object key
* ``abfs`` and ``abfss`` -> namespace ``abfss://{container}`` or, when the
  account is known, ``abfss://{container}@{account}.dfs.core.windows.net``
* ``wasb`` and ``wasbs`` -> namespace ``wasbs://{container}`` or
  ``wasbs://{container}@{account}.blob.core.windows.net``

``abfs`` and ``wasb`` are the same datasets as ``abfss`` and ``wasbs``. The
spec names the TLS scheme. The account host is included when the URI
authority is ``container@account....windows.net``. That authority is parsed
from the URI text: ``ObjectStoragePath`` treats the text before ``@`` as an
Airflow connection id, which would turn the container into ``conn_id`` and
the first path segment into the container. A bare ``abfs://container/key``
has no account, so the namespace stays ``abfss://container`` unless an
already attached filesystem exposes ``account_name``.

A prefix name keeps the trailing slash from the URI the task was given.
``ObjectStoragePath.key`` drops that slash. A single file does not end with
``/``, and for ``file``, ``s3``, and ``gs`` the start datasets match
``FileTransferOperator`` (``namespace`` and ``key``).
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any
from urllib.parse import urlsplit

from airflow.sdk import ObjectStoragePath

_S3 = {"s3", "s3a"}
_GCS = {"gs", "gcs"}
_ABFS = {"abfs", "abfss"}
_WASB = {"wasb", "wasbs"}


def endpoint_uri(value: str | ObjectStoragePath) -> str:
    """URI text for a path argument. A string is returned unchanged."""
    if isinstance(value, str):
        return value
    return str(value)


def identity_uri(uri: str) -> tuple[str, str]:
    """``(namespace, name)`` for a URI string, including ``wasb``.

    ``wasb`` is not a filesystem ``ObjectStoragePath`` can construct. Azure
    authorities are parsed here so the container is not read as a connection
    id. Other schemes go through ``ObjectStoragePath`` and then put back a
    trailing slash the path object removed.
    """
    scheme = _scheme(uri)
    if scheme in _ABFS or scheme in _WASB:
        return _azure_from_uri(uri, path=None)
    path = ObjectStoragePath(uri)
    return openlineage_identity(path, original=uri)


def openlineage_identity(
    path: ObjectStoragePath, *, original: str | None = None
) -> tuple[str, str]:
    """Return ``(namespace, name)`` for one object or prefix path."""
    scheme = _scheme(original) if original else ""
    protocol = scheme or (path.protocol or "file").lower()
    if protocol in _ABFS or protocol in _WASB:
        return _azure_from_uri(original or str(path), path)
    name = path.key
    if original and original.endswith("/") and name and not name.endswith("/"):
        name += "/"
    if protocol in _S3:
        bucket = path.bucket
        return (f"s3://{bucket}" if bucket else "s3"), name
    if protocol in _GCS:
        bucket = path.bucket
        return (f"gs://{bucket}" if bucket else "gs"), name
    return path.namespace, name


def lineage_from_uris(src: str, dst: str) -> Any:
    """One input and one output named from the configured URI strings."""
    from airflow.providers.common.compat.openlineage.facet import Dataset
    from airflow.providers.openlineage.extractors import OperatorLineage

    src_ns, src_name = identity_uri(src)
    dst_ns, dst_name = identity_uri(dst)
    return OperatorLineage(
        inputs=[Dataset(namespace=src_ns, name=src_name)],
        outputs=[Dataset(namespace=dst_ns, name=dst_name)],
    )


def operator_lineage(
    pairs: Sequence[tuple[ObjectStoragePath, ObjectStoragePath]],
    *,
    output_names: Sequence[str] | None = None,
) -> Any:
    """Build an ``OperatorLineage`` from source/destination path pairs.

    ``output_names`` overrides each output dataset name. The copy uses this
    for the keys it returned, so a trailing slash ``ObjectStoragePath.key``
    dropped is still reported. The OpenLineage provider is imported here so
    the operator module can be imported without that extra installed.
    """
    from airflow.providers.common.compat.openlineage.facet import Dataset
    from airflow.providers.openlineage.extractors import OperatorLineage

    inputs = []
    outputs = []
    for index, (src, dst) in enumerate(pairs):
        src_ns, src_name = openlineage_identity(src)
        dst_ns, dst_name = openlineage_identity(dst)
        if output_names is not None:
            dst_name = output_names[index]
        inputs.append(Dataset(namespace=src_ns, name=src_name))
        outputs.append(Dataset(namespace=dst_ns, name=dst_name))
    return OperatorLineage(inputs=inputs, outputs=outputs)


def _scheme(uri: str | None) -> str:
    if not uri or "://" not in uri:
        return ""
    return uri.split("://", 1)[0].lower()


def _attached_account(path: ObjectStoragePath) -> str | None:
    """Account name from a filesystem that is already attached.

    ``path.fs`` builds a client and may talk to the network. This only reads
    a cache that a previous ``attach`` filled in.
    """
    candidates = (path, getattr(path, "__wrapped__", None))
    for obj in candidates:
        if obj is None:
            continue
        fs = getattr(obj, "_fs_cached", None)
        if fs is None:
            continue
        account = getattr(fs, "account_name", None)
        if account:
            return str(account)
    return None


def _azure_from_uri(uri: str, path: ObjectStoragePath | None) -> tuple[str, str]:
    """Parse an ABFS or WASB URI without ``ObjectStoragePath``.

    ``abfs://container@account.dfs.core.windows.net/key`` keeps ``container``
    as the container. ``abfs://conn@container/key`` (no dot in the host) is
    the Airflow form: the host is the container and the userinfo is a
    connection id, which is not part of the dataset name.
    """
    parts = urlsplit(uri)
    scheme_in = (parts.scheme or "").lower()
    ol = "wasbs" if scheme_in in _WASB else "abfss"
    name = (parts.path or "").lstrip("/")
    user, _, host = parts.netloc.rpartition("@")
    if user and (host.endswith(".windows.net") or "." in host):
        return f"{ol}://{user}@{host}", name
    container = host or user
    if path is not None and container and ".windows.net" not in container:
        account = _attached_account(path)
        if account:
            suffix = ".dfs.core.windows.net" if ol == "abfss" else ".blob.core.windows.net"
            return f"{ol}://{container}@{account}{suffix}", name
    if container:
        return f"{ol}://{container}", name
    return ol, name
