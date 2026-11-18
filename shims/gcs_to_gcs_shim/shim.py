"""GCSToGCSOperator shim: same constructor, execute() uses the generic path.

The class subclasses the upstream operator so template_fields, ui_color,
OpenLineage, and the public __init__ signature stay identical (criterion 3
in THRESHOLD.md). execute() is replaced: it validates arguments the same
way, plans TransferItems through the pair listing policy, then copies via
transfer.transfer_objects (GCSHook.rewrite as server_side_copy).
"""

from __future__ import annotations

from typing import Any

from gcs_backend import GcsHookBackend, GcsMtimeFilter
from listing import GcsToGcsPlanConfig, PlanError, plan_prefix
from transfer import transfer_objects

# Imported from the *module object* at class-build time so
# @mock.patch("airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSHook")
# still intercepts hook construction.


def make_shim_class(gcs_mod: Any) -> type:
    """Build a shim class bound to *gcs_mod* (the module tests patch)."""

    Upstream = gcs_mod.GCSToGCSOperator
    hook_ctor_holder = gcs_mod  # GCSHook looked up at execute-time

    class GCSToGCSOperatorShim(Upstream):
        def execute(self, context) -> list[str]:
            self._warn_on_deprecated_template_fields()

            hook = hook_ctor_holder.GCSHook(
                gcp_conn_id=self.gcp_conn_id,
                impersonation_chain=self.impersonation_chain,
            )

            if self.source_objects and self.source_object:
                error_msg = (
                    "You can either set source_object parameter or source_objects parameter but not both. "
                    f"Found source_object={self.source_object} and source_objects={self.source_objects}"
                )
                raise gcs_mod.AirflowException(error_msg) if hasattr(gcs_mod, "AirflowException") else _airflow_exc(error_msg)

            if not self.source_object and not self.source_objects:
                error_msg = "You must set source_object parameter or source_objects parameter. None set"
                raise _airflow_exc(error_msg)

            if self.source_objects and not all(isinstance(item, str) for item in self.source_objects):
                raise _airflow_exc("At least, one of the `objects` in the `source_objects` is not a string")

            if self.source_object:
                self.source_objects = [self.source_object]

            if self.destination_bucket is None:
                self.log.warning(
                    "destination_bucket is None. Defaulting it to source_bucket (%s)", self.source_bucket
                )
                self.destination_bucket = self.source_bucket

            if len(self.source_objects) == 0:
                self.source_objects = [""]
            if self.source_objects.count("") > 1:
                raise _airflow_exc("You can't have two empty strings inside source_object")

            backend = GcsHookBackend(hook)
            mtime = GcsMtimeFilter(
                hook,
                is_older_than=self.is_older_than,
                last_modified_time=self.last_modified_time,
                maximum_modified_time=self.maximum_modified_time,
                log=self.log,
            )
            copy_extra: dict[str, Any] = {}
            if self.retain_until_time is not None:
                copy_extra["retain_until_time"] = self.retain_until_time
                if self.retention_mode is not None:
                    copy_extra["retention_mode"] = self.retention_mode

            config = GcsToGcsPlanConfig(
                source_bucket=self.source_bucket,
                destination_bucket=self.destination_bucket,
                destination_object=self.destination_object,
                delimiter=self.delimiter,
                match_glob=self.match_glob,
                replace=self.replace,
                exact_match=self.exact_match,
                source_object_required=self.source_object_required,
            )

            destination_uris_result: list[str] = []
            try:
                for prefix in self.source_objects:
                    items = plan_prefix(
                        prefix,
                        config=config,
                        lister=backend,
                        exists=backend,
                        log=self.log,
                    )
                    destination_uris_result.extend(
                        transfer_objects(
                            items,
                            copier=backend,
                            mtime_filter=mtime,
                            deleter=backend,
                            move=self.move_object,
                            copy_extra=copy_extra,
                            log=self.log,
                        )
                    )
            except PlanError as exc:
                raise _airflow_exc(str(exc)) from exc

            destination_uris_result = list(dict.fromkeys(destination_uris_result))
            return [u for u in destination_uris_result if not u.endswith("/")]

    GCSToGCSOperatorShim.__name__ = "GCSToGCSOperator"
    GCSToGCSOperatorShim.__qualname__ = "GCSToGCSOperator"
    return GCSToGCSOperatorShim


def _airflow_exc(msg: str):
    from airflow.providers.common.compat.sdk import AirflowException

    return AirflowException(msg)
