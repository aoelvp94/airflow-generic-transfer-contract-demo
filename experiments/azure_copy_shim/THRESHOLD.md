# Pre-registered success criteria: Azure closest-transfer shims

Written and committed **before** any shim is run against upstream tests
or against Azurite. Same rules as the GCS experiment in
`experiments/gcs_to_gcs_shim/THRESHOLD.md`.

## What exists in airflow-src (verified, this SHA)

Airflow SHA: `2b59699b0fe9683c418806a334b47077513bde5a`.

There is **no** same-provider Azure blob/WASB or ADLS copy operator.

Searched `providers/microsoft/azure` transfers and operators, and
related Google transfers. Found no `WasbToWasb`, `ADLSToADLS`,
`AzureBlobToAzureBlob`, or equivalent copy operator. WASB/ADLS
operators in tree are delete/list/create, not copy. Transfers into
Azure blob/ADLS are all *cross-system*:

- `LocalFilesystemToWasbOperator` (local file → blob)
- `LocalFilesystemToADLSOperator`
- `S3ToAzureBlobStorageOperator` (`s3_to_wasb.py`)
- `GCSToAzureBlobStorageOperator`
- `SFTPToWasbOperator`
- and the reverse-direction `AzureBlobStorageToGCSOperator` /
  `AzureBlobToS3` in other providers

This experiment therefore cannot repeat "GCSToGCS / S3-to-S3" for
Azure. The closest operators used instead, as the follow-up brief
allowed:

1. **Azure-only closest:** `LocalFilesystemToWasbOperator`
   (`providers/microsoft/azure/src/.../transfers/local_to_wasb.py`).
   Uploads one local file via `WasbHook.load_file`. Constructor args:
   `file_path`, `container_name`, `blob_name`, `wasb_conn_id`,
   `create_container`, `load_options` (6 named, plus `BaseOperator`
   kwargs). Tests:
   `providers/microsoft/azure/tests/unit/microsoft/azure/transfers/test_local_to_wasb.py`,
   class `TestLocalFilesystemToWasbOperator`, **2** methods
   (`test_init`, `test_execute`).
2. **Closest multi-object transfer that writes Azure blob:**
   `S3ToAzureBlobStorageOperator`
   (`.../transfers/s3_to_wasb.py`). Lists S3 keys (optional prefix),
   optional `replace`, folder-marker stripping, uploads via WasbHook.
   Constructor: 13 named args
   (`aws_conn_id`, `wasb_conn_id`, `s3_bucket`, `container_name`,
   `s3_prefix`, `s3_key`, `blob_prefix`, `blob_name`,
   `create_container`, `replace`, `s3_verify`, `s3_extra_args`,
   `wasb_extra_args`) plus `BaseOperator` kwargs. Tests:
   `.../transfers/test_s3_to_wasb.py`, **10** methods.

These are scored as **two corpora**, not one blended rate.

## Catalog mapping (and what is *not* in the catalog)

- `replace` — `S3ToAzureBlobStorageOperator.replace`; overwrite may
  also appear in `LocalFilesystemToWasbOperator.load_options`
- `wildcard_or_prefix` — S3 prefix listing on the S3→Wasb operator
- reader/writer (local file or S3 get + Wasb `load_file` / `load_file`)
  rather than `server_side_copy` (there is no Azure same-account
  rewrite in these operators)

Called out so a failure is not silently filed as catalog:
`create_container`, `load_options` / `wasb_extra_args` / `s3_extra_args`
shape, folder-marker stripping, "too many files to a single blob"
errors, `test_init` attribute assignment, and hook-call shape on
`WasbHook.load_file` / `S3Hook.list_keys`.

## Success — unit tests (all three must hold, *per corpus*)

1. **Pass rate.** At least **80%** of the unmodified upstream unit
   tests pass when the test module imports the shim in place of the
   pair class. Tests are not edited. A skip or collection/import /
   environment error counts as not passed.
   - LocalFilesystemToWasb: 80% of 2 is 1.6, so the bar is **2/2**.
   - S3ToAzureBlobStorage: the bar is **8/10**.
2. **Failure mapping.** Every non-pass is classified as
   `missing_capability`, `hook_call_shape`, `behavior_difference`, or
   `env_issue`. For that corpus to *succeed*, every failure must map
   to a **capability already in the audit catalog**. Hook-call-shape,
   a missing non-catalog behaviour (the list above), a genuine
   behaviour difference, or an environment problem **fail this
   criterion**, even if the pass rate meets the bar.
3. **Constructor.** Each shim exposes **no more public arguments** than
   its pair operator (6 named for Local→Wasb, 13 named for S3→Wasb,
   plus standard `BaseOperator` kwargs). A wider generic API fails
   this criterion.

The Azure experiment succeeds on unit tests only if **both** corpora
meet all three criteria.

## Success — behavioural parity on Azurite

A separate check compares the **pair operator** and the **shim**
against this repo's Azurite (`just up`, port 10000).

- LocalFilesystemToWasb (Azurite only): upload a file and compare
  blob bytes; overwrite true vs false (via `load_options`, matching
  whatever the pair hook does); request counts during the upload
  (excluding container-create setup) must match.
- S3ToAzureBlobStorage (S3Mock + Azurite): copy one key and a small
  prefix, including a folder marker and a `replace=False` skip when
  the dest blob already exists; destination blob keys and bytes must
  match; request counts during the transfer must match.

Parity succeeds only if every scored scenario matches on objects,
markers (where applicable), overwrite behaviour, *and* request
counts. An emulator outage is `env_issue` and fails the experiment.
Emulators are stopped when the check finishes.

## Failure — recommend option (c)

If either corpus fails a unit-test criterion, or either parity check
fails, the written recommendation is **option (c): keep thin pair
operators, extract shared hook/helpers, and enforce a conformance
suite**. The absence of a wasb→wasb operator is itself evidence that
Azure does not have a same-provider file→file pair to genericise;
that fact is reported regardless of the verdict on these closest
operators.

## How the run is scored

- One pytest session per corpus against the upstream test file, with
  the operator class aliased to the shim, at this Airflow SHA.
- A table of every test: pass / fail / error, category if not passed.
- Constructor arg comparison: pair vs shim, per operator.
- A parity table (objects, markers, overwrite, request counts).
- Verdict: **success** or **fail → option (c)**, against this document
  only. Numbers are not invented. The missing same-provider operator
  is stated up front and is not scored as a test failure.
