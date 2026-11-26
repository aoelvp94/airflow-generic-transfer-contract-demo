"""OpenLineage dataset names for the prefix copy.

:func:`start_facets` names the source and destination paths the task was
given (the prefix, or the single file) and does not list. :func:`complete_facets`
names each object that was actually copied. The operator calls these two
functions. The copy itself lists once, inside ``execute``, not here.

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


_COPIED: dict[tuple[str | None, str | None], list[tuple[ObjectStoragePath, ObjectStoragePath]]] = {}


def start_facets(src: str | ObjectStoragePath, dst: str | ObjectStoragePath) -> Any:
    """One input and one output for the paths the task was given. Does not list."""
    return lineage_from_uris(endpoint_uri(src), endpoint_uri(dst))


def record_copied(
    operator: Any,
    pairs: list[tuple[ObjectStoragePath, ObjectStoragePath]],
    result: list[str] | None,
) -> None:
    """Remember the pairs ``execute`` copied so the complete hook can name them.

    The OpenLineage listener may hold a different operator instance in the
    same process. A listener in another process reads the task return value
    instead.
    """
    operator._ol_pairs = list(pairs)
    operator._ol_output_names = list(result) if isinstance(result, list) else None
    operator._ol_ran = True
    if operator.dag_id and operator.task_id:
        _COPIED[(operator.dag_id, operator.task_id)] = list(pairs)


def complete_facets(operator: Any, task_instance: Any = None) -> Any:
    """One dataset pair per copied object.

    Markers are included only when they were copied. Keys left in place
    because ``overwrite`` is false are absent. When this hook cannot see the
    copy (no ``execute`` in this process and no return value on
    ``task_instance``), it uses the start datasets.
    """
    found = _copied_pairs(operator, task_instance)
    if found is None:
        return start_facets(operator.src, operator.dst)
    pairs, names = found
    return operator_lineage(pairs, output_names=names)


def _copied_pairs(
    operator: Any, task_instance: Any
) -> tuple[list[tuple[ObjectStoragePath, ObjectStoragePath]], list[str] | None] | None:
    if getattr(operator, "_ol_ran", False):
        pairs = list(getattr(operator, "_ol_pairs", []))
        names = getattr(operator, "_ol_output_names", None)
        if names is not None and len(names) != len(pairs):
            names = None
        return pairs, names
    cached = _COPIED.get((operator.dag_id, operator.task_id))
    if cached is not None:
        return list(cached), None
    keys = _xcom_return_keys(task_instance, operator.task_id)
    if keys is None:
        return None
    return _pairs_from_dest_keys(operator, keys), keys


def _pairs_from_dest_keys(
    operator: Any, dest_keys: list[str]
) -> list[tuple[ObjectStoragePath, ObjectStoragePath]]:
    from aip132.path_ops import child_path, resolve_storage_path

    src = resolve_storage_path(operator.src, operator.source_conn_id)
    dst = resolve_storage_path(operator.dst, operator.dest_conn_id)
    dst_key = dst.key
    pairs: list[tuple[ObjectStoragePath, ObjectStoragePath]] = []
    for dest_key in dest_keys:
        dest_path = _replace_key(dst, dest_key)
        if operator.keep_structure and dst_key and dest_key.startswith(dst_key):
            rel = dest_key[len(dst_key) :]
            src_path = child_path(src, rel) if rel else src
        elif operator.keep_structure and not dst_key:
            src_path = child_path(src, dest_key)
        else:
            src_path = src
        pairs.append((src_path, dest_path))
    return pairs


def _replace_key(root: ObjectStoragePath, key: str) -> ObjectStoragePath:
    """Same bucket and connection as ``root``, with ``key`` as the object key."""
    protocol = root.protocol or "file"
    text = key.lstrip("/")
    if root.bucket:
        uri = f"{protocol}://{root.bucket}/{text}"
    else:
        uri = f"{protocol}:///{text}"
    return ObjectStoragePath(uri, conn_id=root.conn_id)


def _xcom_return_keys(task_instance: Any, task_id: str | None) -> list[str] | None:
    """Return value pushed by ``execute``, when the task instance exposes it."""
    if task_instance is None:
        return None
    pull = getattr(task_instance, "xcom_pull", None)
    value: Any = None
    if callable(pull):
        try:
            value = pull(task_ids=task_id, key="return_value")
        except TypeError:
            try:
                value = pull(key="return_value")
            except Exception:
                return None
        except Exception:
            return None
    elif isinstance(getattr(task_instance, "xcom", None), dict):
        value = task_instance.xcom.get("return_value")
    else:
        return None
    if isinstance(value, list) and all(isinstance(item, str) for item in value):
        return value
    return None


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
