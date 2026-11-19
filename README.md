# airflow-generic-transfer-contract-demo

One generic transfer operator, driven by hook contracts, can cover the transfer
pairs Airflow currently implements as a separate operator class for every
source and destination. Pagination, folder-marker skipping, request counts,
and server-side copy then live on the hook (or on one shared helper) and show
up for every pair that hook serves. This repository is a runnable prototype of
that shape for the Apache Airflow dev@ thread
[DISCUSS] One transfer operator on hook contracts instead of one operator per
source/destination pair
([thread](https://lists.apache.org/thread/5nl6g6oosr5sg69nrfy151fwy5b6j5zf),
[Aldo Escobar's reply](https://lists.apache.org/thread/2f26z6sznm9tr5hzsmlq2wp3m09qx0ko)).
It does not install Airflow. It uses an in-process fake HTTP API and local
object-store emulators. Apache-2.0. English throughout.

## Quick start

Requires Python 3.10+ and [uv](https://docs.astral.sh/uv/). Docker Compose v2
is required only for the emulator tests and file benches. `just` is the task
runner (`justfile`).

```bash
uv venv .venv
uv pip install -e '.[dev,iceberg,emulators,adbc]'

# Unit, parity, edge cases, ADBC (skipped if the extra is missing), audit classifier.
.venv/bin/pytest -v -m 'not integration'

# Object-store emulators: Adobe S3Mock, fake-gcs-server, Azurite.
./emulators/emulators.sh up          # or: just up
.venv/bin/pytest -v -m integration
.venv/bin/python benchmarks/bench.py  # pagination: 10/25 vs 25/25
./emulators/emulators.sh down
```

Other targets: `just bench-emulators`, `just bench-scale` (full 100 MiB / 1000
objects; `just bench-scale-quick` is the short loop), `just bench-adbc`,
`just audit` (needs a providers checkout at `$AIRFLOW_SRC` or
`/workspace/airflow-src`), `just rank-capabilities`. Shim runs against
unmodified upstream unit tests are `just experiment-gcs-to-gcs`,
`just experiment-s3-copy`, and `just experiment-azure-copy` (separate venv;
see `shims/`).

Pinned emulator images (`emulators/docker-compose.yml`):

| Service | Image |
|---|---|
| S3 | `adobe/s3mock:3.12.0` (MinIO profile: `EMULATOR_S3_BACKEND=minio`) |
| GCS | `fsouza/fake-gcs-server:1.56.1` |
| Azure | `mcr.microsoft.com/azure-storage/azurite:3.37.0` |

Connection settings are in `emulators/emulators.env`. Quote Azure's connection
string: it contains `;`.

## Repo map

`just` recipes call the implementation directly. Each driver sits next to the
output it writes, so a maintainer can open one directory and see both.

```
src/generic_transfer/
  contract/protocols.py   BatchReadable, BatchWritable, FileCopyable,
                          LineageProvider, ParamsDescribable, async forms
  contract/__init__.py    stable import path: generic_transfer.contract
  operator.py             GenericRecordTransfer, GenericFileTransfer,
                          GenericAsyncRecordTransfer
  pagination.py           offset, page, cursor, next-link helpers
  wire.py                 transport-level HTTP counters
  adapters/               one module per capability (HTTP, object store,
                          Iceberg, ADBC), plus the naive pair baseline
  adapters/systems/       s3.py, gcs.py, azure.py — emulator connections
tests/unit/               contracts, edges, listing, markers, ADBC, audit
tests/parity/             generic transfer vs an HttpToS3-style page-1 baseline
tests/integration/        S3/GCS/Azure emulator pairs and wire counts
benchmarks/               bench drivers
benchmarks/results/       committed BENCH, BENCH_SCALE, BENCH_ADBC, probes
audits/                   AST classifier and capability ranking
audits/results/           PAIR_AUDIT.csv/json, CAPABILITY_RANK
docs/                     narrative write-ups (index: docs/README.md)
emulators/                compose file, env, lifecycle script, fake HTTP API
shims/                    Airflow file-contract shims (GCS, S3, Azure, 9 pairs)
experiments/              S3ToAzureBlobStorageOperator replace=False repro
examples/                 TaskFlow-shaped hook usage (no scheduler required)
```

Public names are re-exported from the package root:

```python
from generic_transfer import (
    BatchReadable,
    FileCopyable,
    GenericFileTransfer,
    GenericRecordTransfer,
)
```

Per-system modules exist for emulator connections because those clients
differ. The operator does not import them. Pair-specific Airflow behaviour
that the prototype has actually wired lives under `shims/`, one directory
per system, scored against a threshold that was committed before the run.

## Evidence

Numbers below are copied from the committed snapshots. Full tables and the
machine header are in the linked files. The index of those write-ups is
[docs/README.md](docs/README.md).

### Transfer-operator audit

Static AST pass over Airflow `2b59699b0fe9683c418806a334b47077513bde5a`
(`docs/PAIR_AUDIT.md`, `audits/results/PAIR_AUDIT.csv`). 87 modules under
`providers/**/transfers/`, 13 provider packages. Excluding
`amazon/aws/transfers/base.py` leaves 86 pair-transfer modules. 84 concrete
operator classes are the denominator (4 base classes are classified and then
left out of the percentages).

| Bucket | Concrete operators | Share of 84 | Reading |
|---|---:|---:|---|
| `maps_cleanly` | 15 | 17.9% | Fits a generic source-hook / dest-hook operator as-is |
| `needs_capability` | 43 | 51.2% | Fits once a shared capability exists (listing, replace, compression, ACL, server-side copy, …) |
| `special` | 26 | 31.0% | Pair-specific core semantics (partitioning, dest-table DDL, warehouse COPY/UNLOAD, required custom callables, Slack) |

58/84 (69.0%) are `maps_cleanly` or `needs_capability`. Group A (file/record,
no SQL endpoint): 7 / 33 / 4 of 44. Group B (touches SQL): 8 / 10 / 22 of 40.
Conservative view, where an optional special flag still forces `special`:
12 / 40 / 32.

Capability ranking over the 43 `needs_capability` operators
(`audits/results/CAPABILITY_RANK.md`): 19 are unlocked by a single capability
(`compression` 7, `replace` 4, then `bulk_load`, `move_semantics`, and
`schema_sidecar` at 2 each). A greedy set-cover orders 16 capabilities and
unlocks all 43.

### Parity checks

| Check | Result | Where |
|---|---|---|
| Paginated generic transfer vs page-1 HttpToS3-style baseline | Baseline keeps page 1 (10 of 25 records, 1 request). Generic follows the hook's pages (25 of 25, 3 requests). | `tests/parity/`, `just bench` |
| Six emulator pairs (S3, GCS, Azure) | Shared `list_transfer_keys` copies 2 objects and skips markers. Naive native-SDK copy copies 4 objects, including markers. Request counts on that tree: naive 9, generic 5, on every pair. | `benchmarks/results/BENCH.md` |
| `GCSToGCSOperator` shim vs unmodified upstream tests | 45/45 test methods, 81/81 collected items | `shims/gcs_to_gcs_shim/RESULTS.md` |
| `S3CopyObjectOperator` + `S3CopyPrefixOperator` shim | 16/16 methods, 17/17 items, plus an S3Mock behaviour check | `shims/s3_copy_shim/RESULTS.md` |
| Azure closest operators (`LocalFilesystemToWasbOperator`, `S3ToAzureBlobStorageOperator`) | 2/2 and 10/10 methods; 3/3 and 25/25 items; Azurite parity held | `shims/azure_copy_shim/RESULTS.md` |
| One generic file operator on all 9 pairs | 34/35 scored cells passed. The miss is `s3→azure` `replace=False`: dest keys match, bytes do not. Generic constructor has 16 named arguments (cap was 18). | `shims/generic_all_pairs/RESULTS.md` |

The 9-pair harness was scored against a threshold committed before the run.
Because cell (a) failed, that write-up's own recommendation is option (c):
keep thin pair operators, extract shared hook helpers, and enforce a
conformance suite. The failing cell is the `replace=False` comparison
reproduced under `experiments/s3_to_wasb_replace_repro/`: the operator
subtracts full dest blob names from relative source names, so an existing
`out/a.txt` is not recognized as the image of `pre/a.txt`. The patch in that
directory is local; it was not applied upstream. The report is
[apache/airflow#76880](https://github.com/apache/airflow/issues/76880)
(open, filed by aoelvp94).

### Scale bench

`benchmarks/results/BENCH_SCALE.md`. Captured 2026-11-10T07:30:59Z, wall clock
62.5 min. Host: `Intel(R) Xeon(R) Processor` x8, 15.6 GiB RAM,
`Linux-6.12.113+-x86_64-with-glibc2.41`, Python 3.12.15, Docker 29.1.4.
Emulators: the three images in the table above. 5 rounds. Small objects are
1000 × 4 KiB. The large object is 100 MiB. `wire_latency` injects 20 ms
before every HTTP request, 200 objects. Warm median is rounds 1..N-1; max is
the slowest round.

`small_objects`, warm median (max), seconds. Generic sequential HTTP count is
2002 (1000 objects). Naive is 2024 or 2034 (1011 objects; it keeps markers).

| Pair | Naive | Generic sequential | Generic `workers=16` |
|---|---:|---:|---:|
| `s3->gcs` | 8.374 (8.638) | 8.757 (8.923) | 6.322 (6.414) |
| `gcs->s3` | 8.705 (8.952) | 7.354 (7.637) | 5.133 (7.277) |
| `s3->azure` | 13.200 (14.220) | 13.530 (14.164) | 8.236 (8.412) |
| `azure->s3` | 13.119 (13.470) | 12.314 (12.668) | 8.586 (9.125) |
| `gcs->azure` | 10.149 (10.339) | 8.930 (9.302) | 5.759 (6.038) |
| `azure->gcs` | 10.252 (11.280) | 10.581 (10.769) | 7.464 (7.525) |

`wire_latency` warm median, seconds (same file):

| Pair | Naive | Generic sequential | Generic per-key listing | Generic `workers=16` |
|---|---:|---:|---:|---:|
| `s3->gcs` | 9.968 | 9.927 | 20.338 | 1.431 |
| `gcs->s3` | 10.380 | 9.785 | 19.229 | 1.098 |
| `s3->azure` | 11.237 | 11.535 | 22.447 | 1.659 |
| `azure->s3` | 11.104 | 11.067 | 22.778 | 1.733 |
| `gcs->azure` | 11.093 | 10.596 | 19.950 | 1.246 |
| `azure->gcs` | 10.976 | 10.280 | 20.999 | 1.563 |

Same-provider server-side copy after the shared listing and cached client,
warm median seconds (HTTP requests): `s3->s3` 0.014 (3) vs read/write 0.023 (5);
`gcs->gcs` 0.006 (3) vs 0.009 (5); `azure->azure` 0.023 (4) vs 0.036 (6).

On the 100 MiB object, `azure->s3` naive max is 40.479 s (one stalled round;
warm median 2.198 s). Generic sequential on that pair stayed at warm median
1.458 s, max 1.708 s.

Arrow/ADBC fit check (`benchmarks/results/BENCH_ADBC.md`). Measured
2026-11-19T10:20:44Z on `Intel(R) Xeon(R) Processor` x4, 15.6 GiB RAM,
`Linux-6.12.113+-x86_64-with-glibc2.39`, Python 3.12.3. 1,000,000 rows,
batch size 50,000, 3 rounds. Peak RSS is `VmHWM` from `/proc/self/status`
in a fresh child process. Medians: Arrow end-to-end 0.543 s, VmHWM 79.7 MB
before the transfer and 94.5 MB after. Arrow coerced to rows then sunk
0.549 s, VmHWM 79.8 MB before and 344.1 MB after. Protocols accept `Any`
batches. `AdbcSqliteSource.batch_format` is `record_batch`.
`GenericRecordTransfer` streams the batches without knowing Arrow.

A local check (`tests/unit/test_rss_bound.py`, no Docker) runs that Arrow
path at 40,000 and 160,000 rows (1,024-byte values, batch size 2,000), one
child process each. On the same host, VmHWM was 87.5 MB before and 107.45 MB
after at 40,000 rows, and 87.46 MB before and 107.11 MB after at 160,000
rows (ratio 0.997).

## Caveats

- This is a prototype. The operators are plain Python classes with the same
  param surface a real operator would expose. They are not
  `BaseOperator` subclasses, and nothing here is wired into the Airflow
  scheduler.
- Emulators on one machine are not a cloud. The scale host is a single
  8-core Xeon with 15.6 GiB RAM. Times without injected latency are client
  and emulator CPU. `wire_latency` adds a fixed 20 ms delay; it is not a
  trace of S3, GCS, or Azure.
- Sample size is small: 5 rounds, 1000 objects of 4 KiB, one 100 MiB object,
  200 objects in the latency scenario. One naive `azure->s3` large-object
  round stalled at 40.479 s. Max is reported so that stall stays visible.
- The audit is a static AST heuristic over one Airflow commit. It does not
  import Airflow or execute operators. Bucket boundaries move when the
  keyword rules move; `docs/PAIR_AUDIT.md` records the previous snapshot
  (16 / 36 / 32) and the rule change that produced 15 / 43 / 26.
- The 9-pair score is 34/35, not 35/35. GCS/S3/Azure shim success means
  unmodified unit tests passed against a file contract, with each pair's
  listing policy supplied to that contract. It is not a claim that one
  listing policy is a drop-in for every operator.
- `gcsfs` 2026.10+ defaults to a gRPC client. Call sites pin
  `gcsfs.core.GCSFileSystem` so fake-gcs-server's HTTP API answers.

## Out of scope

- Shipping this tree as an Airflow provider, and deleting pair operators.
- Deferrable triggers. `GenericAsyncRecordTransfer` is an async sketch.
- Operators the first-wave note defers: partitioning, schema sidecars,
  format-specific codecs, RDBMS temp-table lifecycles
  ([docs/first-wave-exclusions.md](docs/first-wave-exclusions.md)).
- The separate Arrow/ADBC GenericTransfer proposal. `adapters/adbc_io.py` is
  a fit check on the existing batch protocol.
- Merging the `replace=False` fix. The report is
  [apache/airflow#76880](https://github.com/apache/airflow/issues/76880)
  (open, filed by aoelvp94). The repro, the local patch, and the issue draft
  live under `experiments/s3_to_wasb_replace_repro/`.
- Real cloud accounts, IAM, regions, or throughput claims.

## How this could map upstream

A plausible landing, matching the split already in this tree:

1. Put the Protocols next to the file primitives Airflow already has
   (`ObjectStoragePath`, `FileTransferOperator` in `common.io`), or in a new
   module if the community wants record batches and file copy declared
   separately. This demo keeps `FileCopyable` off `write_batches` so a record
   sink is not overloaded with filesystem params.
2. One `GenericFileTransfer` operator forwards hook params, validates them
   when the hook implements `describe_params`, and optionally collects
   lineage. File-to-file pairs that are list/get/put become thin shells that
   bind two hooks, then a conformance test, then deletion. The 9-pair result
   is the argument for doing the conformance suite before the deletion.
3. One `GenericRecordTransfer` for batch sources (HTTP pagination, SQL
   readers, ADBC). Format conversion, partition layout, and warehouse
   COPY/UNLOAD stay on the pair operator until a capability exists. The audit
   puts those in `special` (26 of 84 on the primary rule).
4. Shared behaviour that is copied today (folder-marker skip, pagination,
   server-side copy, client reuse) moves once, onto the hook or a helper the
   hook calls. The emulator bench is the concrete case: six pairs, one
   `list_transfer_keys`, naive copies the marker and the generic path does
   not.

## Hooks inside a TaskFlow task

The operator is optional. A task can call the hooks directly:
`destination.write_batches(source.read_batches(...))`. The same transfer
through `GenericRecordTransfer` is the comparison. Both bodies live in
`examples/taskflow_transfer.py` and run without a scheduler; the unit test
`tests/unit/test_taskflow_helper.py` calls them that way.

Airflow 3 (including 3.4) decorates the function with `@task` from
`airflow.sdk`. Airflow 2.4 through 2.10 imports `task` from
`airflow.decorators`. `airflow_tasks()` in that module picks whichever is
installed.

```python
from airflow.sdk import task  # Airflow 2.4–2.10: airflow.decorators.task

from examples.taskflow_transfer import copy_orders, copy_orders_with_operator

copy_orders = task(copy_orders)
copy_orders_with_operator = task(copy_orders_with_operator)
```

`copy_orders` builds a `PaginatedHttpSourceHook` and a `FileBatchSink` inside
the task and returns `destination.write_batches(source.read_batches(...))`.
`copy_orders_with_operator` passes those same hooks to
`GenericRecordTransfer`.

## License

Apache-2.0. See [LICENSE](LICENSE).
