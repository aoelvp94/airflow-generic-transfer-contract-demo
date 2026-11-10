"""Generic listing and destination-key planning.

Marker policy, glob, exact match, path layout, and replace=False dest
existence checks live here. This is not a port of GCSToGCSOperator or
any other pair listing method.
"""

from __future__ import annotations

import fnmatch
from collections.abc import Sequence

from contract import ListedObject, ListQuery, ObjectExists, ObjectLister, ObjectRef, TransferItem

MARKER_COPY = "copy"
MARKER_SKIP = "skip"
MARKER_SKIP_OVERLAPPING = "skip_overlapping"
LAYOUT_PRESERVE = "preserve"
LAYOUT_FLATTEN = "flatten"
LAYOUT_JOIN_SOURCE = "join_source_key"


def is_folder_marker(obj: ListedObject) -> bool:
    """Explicit marker rule: trailing-slash key, or zero-byte key ending in /."""
    if obj.key.endswith("/"):
        return True
    return False


def apply_marker_policy(objects: Sequence[ListedObject], policy: str) -> list[ListedObject]:
    policy = policy or MARKER_COPY
    if policy == MARKER_COPY:
        return list(objects)
    if policy == MARKER_SKIP:
        return [o for o in objects if not is_folder_marker(o)]
    if policy == MARKER_SKIP_OVERLAPPING:
        keys = [o.key for o in objects]
        ordered = sorted(set(keys))
        drop: set[str] = set()
        for current, nxt in zip(ordered, ordered[1:]):
            if current.endswith("/") and nxt.startswith(current):
                drop.add(current)
        return [o for o in objects if o.key not in drop]
    raise ValueError(f"unknown marker_policy {policy!r}")


def apply_glob_and_exact(
    objects: Sequence[ListedObject],
    *,
    prefix: str,
    match_glob: str | None,
    exact_match: bool,
) -> list[ListedObject]:
    out = list(objects)
    if exact_match:
        out = [o for o in out if o.key == prefix]
    if match_glob:
        out = [o for o in out if fnmatch.fnmatch(o.key, match_glob) or fnmatch.fnmatch(_basename(o.key), match_glob)]
    return out


def dest_key(source_key: str, *, source_prefix: str, dest_prefix: str, path_layout: str) -> str:
    if path_layout == LAYOUT_JOIN_SOURCE:
        base = dest_prefix or ""
        if base and not base.endswith("/"):
            base = base + "/"
        out = base + source_key
        if source_key.endswith("/") and out and not out.endswith("/"):
            out += "/"
        return out
    if path_layout == LAYOUT_FLATTEN:
        name = _basename(source_key.rstrip("/"))
        if source_key.endswith("/"):
            name = name + "/" if name else source_key
        base = dest_prefix.rstrip("/")
        if not name:
            return dest_prefix
        return f"{base}/{name}" if base else name
    # preserve
    prefix = source_prefix
    if prefix and source_key.startswith(prefix):
        rel = source_key[len(prefix) :]
    else:
        rel = source_key
    rel = rel.lstrip("/") if not source_key.endswith("/") or rel != "/" else rel
    # keep a trailing slash on markers
    dest = dest_prefix
    if dest and not dest.endswith("/") and rel and not dest.endswith(rel):
        # join
        if rel.startswith("/"):
            out = dest.rstrip("/") + rel
        elif dest.endswith("/") or dest == "":
            out = dest + rel
        else:
            out = dest.rstrip("/") + "/" + rel
    else:
        out = (dest or "") + rel
    if source_key.endswith("/") and out and not out.endswith("/"):
        out += "/"
    return out


def _basename(key: str) -> str:
    key = key.rstrip("/")
    if "/" not in key:
        return key
    return key.rsplit("/", 1)[-1]


def plan_from_listed(
    listed: Sequence[ListedObject],
    *,
    source_bucket: str,
    source_prefix: str,
    dest_bucket: str,
    dest_prefix: str,
    source_flavour: str,
    dest_flavour: str,
    marker_policy: str = MARKER_COPY,
    path_layout: str = LAYOUT_PRESERVE,
    match_glob: str | None = None,
    exact_match: bool = False,
    replace: bool = True,
    dest_exists: ObjectExists | None = None,
) -> list[TransferItem]:
    listed = apply_glob_and_exact(listed, prefix=source_prefix, match_glob=match_glob, exact_match=exact_match)
    listed = apply_marker_policy(listed, marker_policy)
    items: list[TransferItem] = []
    for obj in listed:
        dkey = dest_key(
            obj.key,
            source_prefix=source_prefix,
            dest_prefix=dest_prefix,
            path_layout=path_layout,
        )
        dest_ref = ObjectRef(dest_bucket, dkey, dest_flavour)
        if not replace and dest_exists is not None and dest_exists.exists(dest_ref):
            continue
        items.append(
            TransferItem(
                source=ObjectRef(source_bucket, obj.key, source_flavour),
                destination=dest_ref,
            )
        )
    return items


def plan_transfers(
    *,
    source: ObjectLister,
    source_bucket: str,
    source_prefix: str,
    dest_bucket: str,
    dest_prefix: str,
    source_flavour: str,
    dest_flavour: str,
    marker_policy: str = MARKER_COPY,
    path_layout: str = LAYOUT_PRESERVE,
    match_glob: str | None = None,
    exact_match: bool = False,
    replace: bool = True,
    dest_exists: ObjectExists | None = None,
) -> list[TransferItem]:
    listed = source.list_objects(ListQuery(bucket=source_bucket, prefix=source_prefix, match_glob=match_glob))
    return plan_from_listed(
        listed,
        source_bucket=source_bucket,
        source_prefix=source_prefix,
        dest_bucket=dest_bucket,
        dest_prefix=dest_prefix,
        source_flavour=source_flavour,
        dest_flavour=dest_flavour,
        marker_policy=marker_policy,
        path_layout=path_layout,
        match_glob=match_glob,
        exact_match=exact_match,
        replace=replace,
        dest_exists=dest_exists,
    )
