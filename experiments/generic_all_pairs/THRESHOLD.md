# Pre-registered success criteria: one generic file-transfer operator on all 9 pairs

Written and committed **before** the generic operator is implemented or
run against emulators or upstream tests. This experiment tests the real
claim: a single operator, built **only** on a shared file contract —
not a subclass of any pair operator, and not a port of any pair's
listing logic — can cover all nine `{S3, GCS, Azure Blob} × {S3, GCS,
Azure Blob}` combinations on this repo's emulators.

Airflow SHA for upstream references:
`2b59699b0fe9683c418806a334b47077513bde5a`.

## What this operator is (and is not)

The generic operator's execute path must:

- list via **its own** lister, with an **explicit marker policy**
  (`copy` / `skip` / `skip_overlapping`)
- read/write stream when server-side copy is unavailable
- optionally use `server_side_copy` (same-provider rewrite / CopyObject /
  blob copy)
- optionally `delete` the source for move
- optionally apply an mtime filter
- honour an overwrite / replace policy
- apply a path-layout rule (preserve prefix vs flatten)

It must **not** subclass `GCSToGCSOperator`, `S3CopyPrefixOperator`,
`S3ToGCSOperator`, or any other pair class. It must **not** import or
paste a pair's listing helpers (no GCS `*` wildcard split port, no
pair-specific `_ignore_existing_files` / `_copy_source_with_wildcard`
logic). A thin **compatibility wrapper** used only to drive unmodified
upstream unit tests may map pair constructor args onto the generic
operator; that wrapper is not the operator under test.

The existing repo `GenericFileTransfer` / fsspec `copy_files` path may
be consulted, but the experiment operator lives under
`experiments/generic_all_pairs/` and is scored against this document.

## References, verified in airflow-src at this SHA

| Pair | Upstream reference | Named `__init__` args | Unit-test corpus (methods) |
|---|---|---:|---|
| s3→s3 | `S3CopyPrefixOperator` (multi-object) and `S3CopyObjectOperator` (single-object) in `amazon.aws.operators.s3` | 9 / 9 | `TestS3CopyObjectOperator` + `TestS3CopyPrefixOperator`: **16** methods in `test_s3.py` |
| gcs→gcs | `GCSToGCSOperator` | **18** (largest pair) | `TestGoogleCloudStorageToCloudStorageOperator`: **45** methods in `test_gcs_to_gcs.py` |
| azure→azure | **None.** No `WasbToWasb`, `ADLSToADLS`, or Azure blob-copy operator exists | — | — |
| s3→gcs | `S3ToGCSOperator` | 14 | **27** methods in `test_s3_to_gcs.py` |
| gcs→s3 | `GCSToS3Operator` | 14 | **20** methods in `test_gcs_to_s3.py` |
| s3→azure | `S3ToAzureBlobStorageOperator` | 13 | **10** methods in `test_s3_to_wasb.py` |
| azure→s3 | `AzureBlobStorageToS3Operator` | 12 | **7** methods in `test_azure_blob_to_s3.py` |
| gcs→azure | `GCSToAzureBlobStorageOperator` | 13 | **10** methods in `test_gcs_to_wasb.py` |
| azure→gcs | `AzureBlobStorageToGCSOperator` | 10 | **3** methods in `test_azure_blob_to_gcs.py` |

For **azure→azure**, the reference is this repo's `naive_pair_copy`
(`src/generic_transfer/adapters/pair_baseline.py`), default
`skip_markers=False` (copies folder markers).

`AzureBlobStorageToGCSOperator` is a **single-blob** operator
(`blob_name` / `object_name`), not a prefix copy. Prefix and marker
scenarios for azure→gcs are scored against `naive_pair_copy`; the
single-blob scenario is scored against the upstream operator.

Largest pair constructor: **18** named arguments
(`GCSToGCSOperator`).

## Core parity scenarios (all 9 pairs)

Each pair is run on the emulators (S3Mock port 19090, fake-gcs-server
port 14443, Azurite port 10000). The generic operator and the
reference both see the same source objects. Scored, where they apply:

1. **Prefix copy** of several keys, including a nested key: dest keys
   and bytes must match the reference.
2. **Folder marker** under the prefix: a trailing-slash (or
   provider-equivalent) zero-byte key is copied or omitted **the same
   way** as the reference, under the generic policy that corresponds to
   the reference (documented in the harness; default `copy` matches
   naive / S3CopyPrefix).
3. **Overwrite / replace=True**: dest already holds different bytes;
   after the run, dest matches the source.
4. **replace=False**: an existing dest key is skipped, or both sides
   raise the same error class with the same dest state (the pair
   operators do not all implement skip-if-exists the same way).
5. **Move**: scored against the reference when it supports move
   (`GCSToGCSOperator.move_object`). For other pairs, generic move is
   a capability check only (dest present, source absent) and is
   reported separately, not as a pair-parity failure.
6. **Prefix / wildcard**: prefix listing is required on all nine.
   GCS `*` wildcard split and deprecated `delimiter` are **pair
   listing policy**, which this experiment is forbidden from porting.
   A difference on GCS wildcard-only cases is a documented policy
   difference, not a silent pass and not an automatic fail of core
   prefix parity.
7. **HTTP request counts at the wire** (botocore events, GCS/Azurite
   HTTP, or equivalent): recorded for every scenario. A mismatch
   fails the counts cell. It fails the *experiment* unless the
   entire difference is explained by an explicit capability or
   documented policy (for example generic `server_side_copy` vs
   naive azure→azure get+put). An unexplained mismatch fails.

Parity on a pair succeeds only if scenarios 1–4 and 6 (prefix), plus
5 when the reference has move, match on keys, bytes, markers, and
overwrite behaviour, and request-count differences are either zero or
fully explained as above.

## Constructor (criterion b)

The generic operator's public constructor must expose **no more named
arguments than 18** (the largest pair). `**kwargs` / `BaseOperator`
fields such as `task_id` do not count against the 18.

The results document must list every scored pair operator's named
args and mark which have **no generic equivalent**. Those missing
equivalents are not extra generic args; they are the cost of a
smaller shared API.

A generic constructor with 19 or more named args fails this
criterion.

## Upstream tests via a thin wrapper (criterion c) — scored, not the bar

For each pair that has an upstream unit-test corpus in the table
above, a thin wrapper maps that pair's constructor args onto the
generic operator and runs the **unmodified** upstream tests. Tests
are not edited. A skip or collection/import/environment error counts
as not passed.

Every non-pass is classified as `missing_capability`,
`hook_call_shape`, `behavior_difference`, or `env_issue`.

**This experiment does not treat hook-call-shape failures as an
experiment failure.** The operator is forbidden from being a
pair subclass written to hook-call shape; many upstream tests
assert `GCSHook.rewrite` / `S3Hook.copy_object` / `WasbHook.load_file`
call shapes and will fail a contract-native implementation even when
object outcomes match. Those failures are reported honestly.

A `behavior_difference` that is not explained by an explicit
capability or a documented policy **fails the experiment**. An
`env_issue` is reported and does not by itself fail parity (a), but
it counts as not-passed in the wrapper pass rate.

There is no azure→azure upstream corpus.

## Success — all of the following must hold

1. **Parity.** All 9 pairs pass the core scenarios in the parity
   section (including azure→azure vs `naive_pair_copy`).
2. **Explained differences.** Every behaviour difference that appears
   in parity or in wrapper tests is tied to an explicit contract
   capability or a policy documented in this file (wildcard listing,
   request-count shape of server-side vs stream copy, hook-call-shape
   coupling, single-blob azure→gcs vs prefix). Unexplained
   differences fail.
3. **Constructor.** Generic named args ≤ 18, with the no-equivalent
   list published.

If any of the three fail, the written recommendation is **option
(c): keep thin pair operators, extract shared hook/helpers, and
enforce a conformance suite**.

## How the run is scored

- One parity harness (`just experiment-all-pairs`) against the three
  emulators. Emulators that this run starts are stopped at the end;
  nothing is left running.
- A 9×scenario matrix (pass/fail) and a request-count comparison
  table, numbers taken from that run only.
- Constructor-arg comparison: generic vs each pair.
- Wrapper pass rates and a failure-category table per corpus.
- Verdict: **success** or **fail → option (c)**, against this
  document only. Numbers are not invented.

No real cloud accounts are used. Emulators are not production S3 /
GCS / Azure; that is a pre-registered caveat, not a post-hoc excuse.
