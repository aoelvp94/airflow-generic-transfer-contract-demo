# First-wave exclusion criteria

Operators that should **stay out** of the first deprecation / rebase wave onto
one generic transfer operator. Criteria are evidence-based from the current
Airflow pair-operator surface, not a final AIP decision.

## Criteria (any one is enough to defer)

| # | Criterion | Why it blocks a thin shell |
|---|---|---|
| C1 | Destination-specific **partitioning / sharding** of output paths | Operator owns path layout, not just hook params (e.g. `BaseSQLToGCSOperator` partition columns → object keys). |
| C2 | Side-car artifacts (schema JSON, manifests, success files) | Extra writes with their own contracts; cannot be expressed as `write_batches` alone. |
| C3 | Format conversion with provider-specific options (Parquet/ORC/Avro codecs, CSV dialect tied to the pair) | Needs a declared format capability on the sink, not yet in the Protocol. |
| C4 | Source-side SQL rendering / temp-table lifecycle unique to one RDBMS pair | Belongs on the DbApiHook / SQL contract, not the transfer operator. |
| C5 | Non-batch semantics (single-object move/rename, ACL/presign, multipart upload policy) | Closer to `FileCopyable` extras or a storage-admin operator. |
| C6 | Lineage / OpenLineage facets hardcoded in the pair operator today | Wait until `LineageProvider` on hooks is real in Airflow before deleting the pair class. |

## Table (illustrative — not exhaustive)

| Operator / family | Criteria | First wave? | Notes |
|---|---|---|---|
| `HttpToS3Operator` / `HttpToGCSOperator` (no pagination) | — | **Yes** | Pagination-in-hook is the demo's strongest win. |
| S3↔GCS↔Azure file-pair operators (folder-marker bug class) | — | **Yes** | Rebase onto `FileCopyable` + shared `list_transfer_keys`. |
| `FileTransferOperator` / `ObjectStoragePath` | — | **Keep / extend** | Already the AIP-58 target; become the file half. |
| `BaseSQLToGCSOperator` (+ BigQuery/S3 variants with schema JSON) | C1, C2, C3 | **No** | Partitioning + schema file are operator semantics. |
| `SQLToS3Operator` with filetype-specific args | C3 | **No** (first wave) | Needs format capability negotiation. |
| Operators that only wrap `load_file` / `bulk_load` with vendor options | C5 | **Maybe later** | Evaluate per provider. |
| Deferrable pair operators with custom triggers | C6 + async form | **No** until `AsyncBatch*` graduates | Sketch only in this demo. |

## What *does* go in the first wave

1. File-to-file pairs that are list/get/put with optional marker skip.
2. Record sources whose only missing feature is pagination / retry in the hook.
3. Thin shells that forward params to hooks already implementing the Protocols.

Reproduce the criteria discussion with: `just test && just bench` (pagination) and
`just up && just test-integration` (folder markers).
