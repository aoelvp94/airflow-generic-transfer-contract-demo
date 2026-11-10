# Pre-registered success criteria: S3 same-provider copy on a generic file contract

Written and committed **before** the shim is run against upstream tests
or against the S3Mock/MinIO emulator. Same rules as the GCS experiment
in `experiments/gcs_to_gcs_shim/THRESHOLD.md`.

## Operators in scope (verified in airflow-src at this SHA)

There is **no** `S3ToS3` module under `providers/amazon/**/transfers/`.
Same-provider S3 copy lives in the Amazon **operators** package:

- `airflow.providers.amazon.aws.operators.s3.S3CopyObjectOperator`
  — one object, `S3Hook.copy_object` (server-side copy). Optional
  `source_version_id`, `acl_policy`, `meta_data_directive`, `kms_key_id`,
  `kms_encryption_type`.
- `airflow.providers.amazon.aws.operators.s3.S3CopyPrefixOperator`
  — list under a prefix (`list_objects_v2` paginator) and
  `copy_object` each key, remapping the prefix. Optional KMS, ACL,
  metadata directive, and `continue_on_failure`.

Airflow SHA: `2b59699b0fe9683c418806a334b47077513bde5a`.
Tests: `providers/amazon/tests/unit/amazon/aws/operators/test_s3.py`,
classes `TestS3CopyObjectOperator` (**6** methods) and
`TestS3CopyPrefixOperator` (**10** methods). **16** methods total.

Those tests use moto (`@mock_aws`) and assert on destination object
keys (and, for KMS, on a monkeypatched `S3Hook.copy_object` call
shape). They also cover OpenLineage facets and `template_fields`.
Hook-call-shape coupling is still possible on the KMS test and
anywhere the hook or paginator is asserted.

## Catalog mapping (and what is *not* in the catalog)

- `server_side_copy` — `S3Hook.copy_object`
- `wildcard_or_prefix` — prefix listing on `S3CopyPrefixOperator`
- `acl_or_extra_args` — `acl_policy`
- `dest_encryption` — `kms_key_id` / `kms_encryption_type`

Called out so a failure is not silently filed as "already in the
catalog": `source_version_id`, `meta_data_directive`,
`continue_on_failure`, s3:// vs bucket+key argument combinations,
OpenLineage facet shape, and `template_fields`.

## Success — unit tests (all three must hold)

1. **Pass rate.** At least **80%** of the 16 unmodified upstream unit
   tests pass when the test module imports the shim in place of the
   pair class (13 of 16). Tests are not edited. A skip or
   collection/import/environment error counts as not passed. Score
   methods, not only parametrized items; report both.
2. **Failure mapping.** Every non-pass is classified as
   `missing_capability`, `hook_call_shape`, `behavior_difference`, or
   `env_issue`. For the experiment to *succeed*, every failure must
   map to a **capability already in the audit catalog**. Hook-call-shape,
   a missing non-catalog behaviour (the list above), a genuine
   behaviour difference, or an environment problem **fail this
   criterion**, even if the pass rate is ≥ 80%.
3. **Constructor.** Each shim exposes **no more public arguments** than
   its pair operator: 9 named args on `S3CopyObjectOperator` and 9 on
   `S3CopyPrefixOperator`, plus standard `AwsBaseOperator` / `BaseOperator`
   kwargs (`task_id`, `aws_conn_id`, …). A wider generic API fails this
   criterion.

## Success — behavioural parity on S3Mock

After the unit-test run, a separate check compares the **pair
operator** and the **shim** against this repo's Adobe S3Mock
(`just up`, port 19090). MinIO is the fallback if S3Mock cannot be
used. Scored scenarios, all executed for both implementations with
the same source objects:

- single-object copy: destination key and bytes match
- prefix copy of several keys, including a nested key: destination
  keys and bytes match
- overwrite: destination already holds different bytes; after copy,
  destination matches the source (the pair has no `replace=False`)
- folder markers: a trailing-slash key under the prefix is either
  copied or omitted **the same way** by pair and shim
- request counts: CopyObject / List / Get / Put counts during the
  transfer (excluding bucket-create setup) are recorded; a mismatch
  **fails** this parity check

Parity succeeds only if every scored scenario matches on objects,
markers, overwrite behaviour, *and* request counts. An emulator
outage is `env_issue` and fails the overall experiment (same as a
unit-test env failure).

The emulator is stopped when the check finishes. No leftover
processes.

## Failure — recommend option (c)

If any unit-test criterion or the S3Mock parity check fails, the
written recommendation is **option (c): keep thin pair operators,
extract shared hook/helpers, and enforce a conformance suite**.

## How the run is scored

- One pytest session against the upstream `test_s3.py` classes above,
  with the operator classes aliased to the shims, at this Airflow SHA.
- A table of every test: pass / fail / error, category if not passed.
- Constructor arg comparison: pair vs shim, for both operators.
- A parity table (objects, markers, overwrite, request counts).
- Verdict: **success** or **fail → option (c)**, against this document
  only. Numbers are not invented.

No AWS account is required: unit tests use moto; parity uses S3Mock.
