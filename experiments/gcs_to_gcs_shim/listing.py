\
"""GCSToGCS listing and destination-key policy, expressed against the contract.

This is the pair-specific part of the experiment. Wildcard splits, exact_match,
delimiter, destination prefix rewriting, replace=False dest-list subtraction,
and the empty-list-then-exists fallback are all ported from
airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSToGCSOperator at
SHA 2b59699b so hook.list / hook.exists call shapes stay comparable to the
upstream tests. A production generic operator would need an equivalent
listing policy per source kind, not this GCS-only encoding.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from contract import ListQuery, ObjectExists, ObjectLister, ObjectRef, TransferItem

WILDCARD = "*"


class PlanError(ValueError):
    """Input / wildcard errors the shim turns into AirflowException."""


@dataclass
class GcsToGcsPlanConfig:
    source_bucket: str
    destination_bucket: str
    destination_object: str | None
    delimiter: str | None = None
    match_glob: str | None = None
    replace: bool = True
    exact_match: bool = False
    source_object_required: bool = False


def plan_prefix(
    prefix: str,
    *,
    config: GcsToGcsPlanConfig,
    lister: ObjectLister,
    exists: ObjectExists,
    log=None,
) -> list[TransferItem]:
    if WILDCARD in prefix:
        return _plan_with_wildcard(prefix, config=config, lister=lister, exists=exists, log=log)
    return _plan_without_wildcard(prefix, config=config, lister=lister, exists=exists, log=log)


def _plan_without_wildcard(
    prefix: str,
    *,
    config: GcsToGcsPlanConfig,
    lister: ObjectLister,
    exists: ObjectExists,
    log=None,
) -> list[TransferItem]:
    objects = lister.list_objects(
        ListQuery(
            bucket=config.source_bucket,
            prefix=prefix,
            delimiter=config.delimiter,
            match_glob=config.match_glob,
        )
    )
    objects = [obj for obj in objects if _check_exact_match(obj, prefix, config.exact_match)]

    if not config.replace:
        objects = _ignore_existing_files(
            prefix,
            objects=objects,
            config=config,
            lister=lister,
            log=log,
            omit_match_glob=False,
            delimiter=config.delimiter,
            match_glob=config.match_glob,
        )

    items: list[TransferItem] = []
    if len(objects) == 0 and prefix:
        source_ref = ObjectRef(config.source_bucket, prefix)
        if exists.exists(source_ref):
            destination_object = config.destination_object or prefix
            dest_ref = ObjectRef(config.destination_bucket, destination_object)
            if not config.replace and exists.exists(dest_ref):
                if log:
                    log.info(
                        "Skipped object %s; already exists in destination bucket %s.",
                        destination_object,
                        config.destination_bucket,
                    )
            else:
                # Upstream copies with destination_object as given (may be None),
                # not the coalesced key used for the exists check.
                dest_key = config.destination_object
                items.append(
                    TransferItem(
                        source=source_ref,
                        destination=ObjectRef(config.destination_bucket, dest_key),
                    )
                )
        elif config.source_object_required:
            msg = f"{prefix} does not exist in bucket {config.source_bucket}"
            if log:
                log.warning(msg)
            raise PlanError(msg)
    if len(objects) == 1 and objects[0][-1] != "/":
        items.extend(_plan_file(objects[0], config=config))
    elif len(objects):
        items.extend(_plan_multiple(objects, prefix, config=config))
    return items


def _plan_file(source_object: str, *, config: GcsToGcsPlanConfig) -> list[TransferItem]:
    destination_object = config.destination_object or source_object
    if config.destination_object and config.destination_object[-1] == "/":
        destination_object += source_object.split("/")[-1]
    return [
        TransferItem(
            source=ObjectRef(config.source_bucket, source_object),
            destination=ObjectRef(config.destination_bucket, destination_object),
        )
    ]


def _plan_multiple(
    source_objects: list[str], prefix: str, *, config: GcsToGcsPlanConfig
) -> list[TransferItem]:
    _pref = prefix.rstrip("/")
    is_directory = prefix.endswith("/") or all(
        obj.replace(_pref, "", 1).startswith("/") for obj in source_objects
    )
    if is_directory:
        base_path = prefix.rstrip("/") + "/"
    else:
        base_path = prefix[0 : prefix.rfind("/") + 1] if "/" in prefix else ""

    items: list[TransferItem] = []
    for source_obj in source_objects:
        if not _check_exact_match(source_obj, prefix, config.exact_match):
            continue
        if config.destination_object is None:
            destination_object = source_obj
        else:
            file_name_postfix = source_obj.replace(base_path, "", 1)
            destination_object = config.destination_object.rstrip("/") + "/" + file_name_postfix
        items.append(
            TransferItem(
                source=ObjectRef(config.source_bucket, source_obj),
                destination=ObjectRef(config.destination_bucket, destination_object),
            )
        )
    return items


def _plan_with_wildcard(
    prefix: str,
    *,
    config: GcsToGcsPlanConfig,
    lister: ObjectLister,
    exists: ObjectExists,
    log=None,
) -> list[TransferItem]:
    total_wildcards = prefix.count(WILDCARD)
    if total_wildcards > 1:
        raise PlanError(
            "Only one wildcard '*' is allowed in source_object parameter. "
            f"Found {total_wildcards} in {prefix}."
        )
    if log:
        log.info("Delimiter ignored because wildcard is in prefix")
    prefix_, delimiter = prefix.split(WILDCARD, 1)
    objects = lister.list_objects(
        ListQuery(
            bucket=config.source_bucket,
            prefix=prefix_,
            delimiter=delimiter,
            omit_match_glob=True,
        )
    )
    if not config.replace:
        objects = _ignore_existing_files(
            prefix_,
            objects=objects,
            config=config,
            lister=lister,
            log=log,
            omit_match_glob=False,
            delimiter=delimiter,
            match_glob=None,
        )
    items: list[TransferItem] = []
    for source_object in objects:
        if config.destination_object is None:
            destination_object = source_object
        else:
            destination_object = source_object.replace(prefix_, config.destination_object, 1)
        items.append(
            TransferItem(
                source=ObjectRef(config.source_bucket, source_object),
                destination=ObjectRef(config.destination_bucket, destination_object),
            )
        )
    return items


def _ignore_existing_files(
    prefix: str,
    *,
    objects: list[str],
    config: GcsToGcsPlanConfig,
    lister: ObjectLister,
    log=None,
    omit_match_glob: bool,
    delimiter: str | None,
    match_glob: str | None,
) -> list[str]:
    if config.destination_object is None:
        existing_objects = lister.list_objects(
            ListQuery(
                bucket=config.destination_bucket,
                prefix=prefix,
                delimiter=delimiter,
                match_glob=match_glob,
                omit_match_glob=omit_match_glob,
            )
        )
    else:
        if log:
            log.info("Replaced destination_object with source_object prefix.")
        destination_objects = lister.list_objects(
            ListQuery(
                bucket=config.destination_bucket,
                prefix=config.destination_object,
                delimiter=delimiter,
                match_glob=match_glob,
                omit_match_glob=omit_match_glob,
            )
        )
        existing_objects = [
            dest_object.replace(config.destination_object, prefix, 1)
            for dest_object in destination_objects
        ]
    objects = list(set(objects) - set(existing_objects))
    if objects:
        if log:
            log.info("%s files are going to be synced: %s.", len(objects), objects)
    else:
        if log:
            log.info("There are no new files to sync. Have a nice day!")
    return objects


def _check_exact_match(source_object: str, prefix: str, exact_match: bool) -> bool:
    if exact_match and (source_object != prefix or not source_object.endswith(prefix)):
        return False
    return True
