# Pre-registered success criteria: GCSToGCSOperator on a generic file contract

Written and committed **before** the shim is run against upstream tests.
This is the experiment Claude suggested: turn "file→file maps" from a
heuristic into a measured fact, or recommend option (c) if it does not.

## Corpus (fixed)

- Airflow SHA: `2b59699b0fe9683c418806a334b47077513bde5a`
- Upstream tests: `providers/google/tests/unit/google/cloud/transfers/test_gcs_to_gcs.py`
- **45** test methods on `TestGoogleCloudStorageToCloudStorageOperator`
- Operator under test: `airflow.providers.google.cloud.transfers.gcs_to_gcs.GCSToGCSOperator`

Those tests mock `GCSHook` heavily. They are likely to assert on specific
hook method names (`rewrite`, `list`, `exists`, `delete`, `is_updated_*`,
`is_older_than`) rather than on observable copy behaviour alone. That
coupling is recorded below so it cannot be waved away after the fact.

## Operator surface observed in source (not yet executed)

`GCSToGCSOperator` today supports, at least:

- single object and prefix listing (`source_object` / `source_objects`)
- one wildcard (`*`) in a source object, plus `match_glob`, `exact_match`, deprecated `delimiter`
- `replace` / skip-if-exists
- `move_object` (copy then `hook.delete`)
- mtime filters: `last_modified_time`, `maximum_modified_time`, `is_older_than`
- `source_object_required`
- destination object-retention (`retain_until_time`, `retention_mode`)
- OpenLineage facets on complete
- return value: de-duplicated list of destination URIs, excluding directory-like (`/`-suffixed) URIs
- server-side copy via `GCSHook.rewrite` (not a download/upload)

These map onto the audit catalog as: `wildcard_or_prefix`, `replace`,
`move_semantics`, `incremental_filter`, `object_retention`,
`server_side_copy`, plus lineage (informational). `source_object_required`
and the exact return-URI rules are not named capabilities in the catalog
today; they are called out here so a failure on them is not silently
filed as "already in the catalog."

## Success — all three must hold

1. **Pass rate.** At least **80%** of the 45 unmodified upstream unit
   tests pass when the test module imports the shim in place of
   `GCSToGCSOperator` (36 of 45). Tests are not edited. A skip or
   collection/import/environment error counts as not passed.
2. **Failure mapping.** Every test that does not pass is classified as
   one of: `missing_capability`, `hook_call_shape`, `behavior_difference`,
   `env_issue`. For the experiment to *succeed*, every failure must map
   to a **capability already in the audit catalog**
   (`wildcard_or_prefix`, `replace`, `move_semantics`,
   `incremental_filter`, `object_retention`, `server_side_copy`, or
   another catalog flag). Failures whose real cause is hook-call-shape
   coupling, a missing non-catalog behaviour (`source_object_required`,
   URI return shape, deprecation warning text, OpenLineage facet shape),
   a genuine behaviour difference, or an environment/setup problem
   **fail this criterion**, even if the pass rate is ≥ 80%.
3. **Constructor.** The generic/shim constructor used as the
   `GCSToGCSOperator` replacement exposes **no more public arguments**
   than the pair operator (the 18 listed in `__init__`, plus standard
   `BaseOperator` kwargs such as `task_id`). A wider generic API fails
   this criterion.

## Failure — recommend option (c)

If any of the three criteria fail, the written recommendation is
**option (c): keep thin pair operators, extract shared hook/helpers,
and enforce a conformance suite** — not "one generic operator is already
a drop-in replacement for `GCSToGCSOperator`."

Hook-call-shape failures are evidence of *implementation* coupling in
the upstream tests, not automatically evidence that the contract is
wrong. They still fail criterion 2 as pre-registered above, and they
are reported honestly in `RESULTS.md`.

## How the run is scored

- One pytest session against the upstream file, with the operator class
  aliased to the shim (monkeypatch / import shim), at this Airflow SHA.
- A table of every test: pass / fail / error, category if not passed,
  and a short note.
- Constructor arg comparison: pair vs generic/shim.
- Verdict: **success** or **fail → option (c)**, against this document
  only. Numbers are not invented; they come from that pytest run.

No emulators are required: the upstream unit tests mock `GCSHook`.
