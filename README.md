# airflow-generic-transfer-contract-demo

Local prototype for **one transfer operator on hook contracts** instead of one
operator per source/destination pair. Supports the position in David Blain's
dev-list thread
([DISCUSS] One transfer operator on hook contracts instead of one operator per
source/destination pair, 2026-11-09).

No Airflow install required. Fake APIs + local object-store emulators only.
No employer branding. English throughout.

> Out of scope here: Arrow / ADBC end-to-end GenericTransfer optimisations.
> That is a separate proposal and must stay on its own thread.

## Thesis

Airflow today ships ~86 transfer modules (~17,500 lines) across 13 providers.
Every fix and every feature (pagination, file format, async, lineage) lands in
one pair operator and is missing from the others. The folder-marker bug is the
concrete proof: the same "skip a zero-byte key that has keys under it" rule was
applied in four PRs across three providers in six months, and main now carries
three copies of the same helper.

The structure leaves no other option when each pair is its own operator.

We have solved this before. `common.sql` replaced per-database operators with
`SQLExecuteQueryOperator` on the `DbApiHook` contract. AIP-14 asked for the
same for transfers; AIP-58 delivered `ObjectStoragePath` and
`FileTransferOperator`, but the 28 file-pair operators were never rebased or
deprecated, and there is still no contract for **record** sources (paginated
HTTP/REST, SaaS APIs, SQL→file).

This demo shows the missing piece as runnable code:

1. **Capability on the hook** — `read_batches(**params)` / `write_batches(batches, **params)`.
2. **One generic operator** — forwards `source_hook_params` /
   `destination_hook_params`, validates at task run time, knows neither vocabulary.
3. **File destinations through one path** — an `ObjectStoragePath`-like facade
   (fsspec) plus a shared folder-marker skip.
4. **Pair operators become thin shells** — `tests/baseline_httptos3.py` and
   `adapters/pair_baseline.py` stand in for today's pair operators.

## Mapping to David's questions

| # | Question | What this demo argues |
|---|---|---|
| 1 | Are per-pair operators a maintenance problem? Should we stop accepting new ones? | Yes. Folder-marker test + stock page-1 baseline show the same bug/feature tax. |
| 2 | Rebase file-to-file on `ObjectStoragePath` / `FileTransferOperator` first? | Yes as step one. `list_transfer_keys` + emulator pairs is that shape. |
| 3 | Where do record contracts live: `common.sql`, `common.io`, or new? | Demo is neutral; contracts are tiny Protocols. Real home is a community call. |
| 4 | New AIP? | Yes — AIP-14 is closed and incomplete. This is evidence for that AIP, not the AIP itself. |

## Reproduce (reviewer one-liner)

```bash
# Requires: Docker (compose v2), uv, and just (or make). On TCP-only Docker daemons:
#   export DOCKER_HOST=tcp://127.0.0.1:2375
just reproduce
# equivalent: just install && just e2e
# or:          make reproduce
```

Expected:

- `just test-all` → **26 passed** (12 unit + 14 integration).
- `just bench` → stock HttpToS3-style keeps page 1 (10/25); generic lands 25/25.
- `just bench-emulators` → writes `bench-out/BENCH.md` + `bench-out/latest.json`.
  Naive pair copy lands **4** objects (includes markers); generic lands **2**.
  Request totals ≈ **9** (naive) vs **5** (generic) per pair on this tree.

Individual steps:

```bash
just install          # uv venv + extras (dev, iceberg, emulators)
just up               # S3Mock + fake-gcs-server + Azurite
just test             # unit only
just test-integration # needs emulators
just bench            # in-process HTTP pagination evidence
just bench-emulators  # cross-cloud file copy + marker findings
just down
```

Pinned emulator images (see `docker-compose.yml`):

| Service | Image |
|---|---|
| S3 | `adobe/s3mock:3.12.0` (default; MinIO profile available) |
| GCS | `fsouza/fake-gcs-server:1.56.1` |
| Azure | `mcr.microsoft.com/azure-storage/azurite:3.37.0` |

## Emulators

`docker-compose.yml` + `scripts/emulators.sh` start three local object stores so
we can reproduce the behaviour of `S3ToGCSOperator`, `GCSToS3Operator`,
`AzureBlobStorageToS3Operator`, `S3ToAzureBlobStorageOperator`, and the
zero-byte **folder marker** bug (`dir/` with keys beneath it) end to end.

**S3 backend note.** This box could not pull `minio/minio` (Docker Hub denied)
or download `dl.min.io` (HTTP 410). Default is **Adobe S3Mock**, which speaks
enough of the S3 API for path-style listing/get/put and folder markers.
Reviewers with Docker Hub access can switch:

```bash
# optional preferred S3
docker compose --profile minio up -d s3-minio
# then in emulators.env:
#   EMULATOR_S3_BACKEND=minio
#   S3_ENDPOINT=http://127.0.0.1:19000
#   S3_ACCESS_KEY=minioadmin
#   S3_SECRET_KEY=minioadmin
```

A native MinIO binary can also be built with `go install github.com/minio/minio@latest`
into `./bin/` (gitignored); wire it via the same env vars.

Connection settings: `emulators.env` (quote Azure's connection string — it
contains `;`).

## Findings → dev-list claims

| Claim (dev-list) | Evidence in this repo | Command |
|---|---|---|
| Same folder-marker fix landed 4× across providers | Naive S3→GCS copies 4 keys including `data/` + `data/nested/`; generic copies 2. Shared `is_folder_marker` used once. | `just up && just test-integration -k marker` / `just bench-emulators` |
| Pagination missing in HttpToS3 / HttpToGCS-style operators | Stock baseline keeps page 1 (10/25); generic + hook pagination lands 25/25. | `just bench` / `just test -k parity` |
| Per-pair vs generic behaviour parity (files) | All 6 ordered pairs (S3↔GCS↔Azure) pass the same generic copy assertions. | `just up && just test-integration` |
| Per-pair vs generic request counts | Naive ≈ 9 req (1 list + 4 get + 4 put); generic ≈ 5 (1 list + 2 get + 2 put) on the marker tree. | `just bench-emulators` → `bench-out/BENCH.md` |
| fsspec marker behaviour differs by backend | Native SDK always lists trailing-slash markers. `gcsfs.core` keeps them (info `type=directory`). `adlfs` drops the walked prefix marker and keeps nested markers **without** trailing slash. `s3fs` varies. Shared `list_transfer_keys` collapses all three to the real objects. | `just bench-emulators` (findings table) / `tests/test_emulator_pairs.py::test_fsspec_find_differs_on_markers` |

Committed snapshot of the last local emulator run: [`bench-out/BENCH.md`](bench-out/BENCH.md).

## Layout

```
src/generic_transfer/
  contract.py           BatchReadable / BatchWritable (runtime_checkable Protocol)
  operator.py           GenericRecordTransfer
  pagination.py         Stateless HTTP pagination helpers
  adapters/
    http_source.py      PaginatedHttpSourceHook
    file_sink.py        FileBatchSink, ObjectStoreCopySink, DictObjectStore, folder markers
    iceberg_sink.py     IcebergAppendSink (pyiceberg + SQLite catalog)
    emulators.py        fsspec + native SDK factory for S3/GCS/Azure emulators
    pair_baseline.py    Naive per-pair native-SDK copy (pre-fix behaviour)
fake_api/server.py      In-process Acme/Globex/Initech/Hooli APIs
docker-compose.yml      S3Mock + fake-gcs-server + Azurite (+ MinIO profile)
emulators.env           Endpoints and credentials
scripts/emulators.sh    up / down / wait / status
scripts/bench.py        HTTP pagination wall time + request counts
scripts/bench_emulators.py  Cross-cloud cold/warm median + marker findings
tests/                  Contract, parity, folder-marker, emulator integration
bench-out/BENCH.md      Last committed emulator bench snapshot
```

## Bench (local, in-process fake API)

Acme Orders: 25 records, `limit=10` → 3 pages. 5 rounds, mean wall time.

| Path | Wall (mean, 5 rounds) | Requests | Records landed |
|------|----------------------|----------|----------------|
| Stock HttpToS3-style (page 1 only) | ~1.8 ms | 1 | 10 / 25 (silent truncate) |
| `GenericRecordTransfer` (paginated) | ~4.0 ms | 3 | 25 / 25 |

Re-run: `just bench` → `bench-out/bench.json`.

The point is not raw speed: the stock path is "faster" because it drops pages.
The generic path is complete, and the pagination capability lives in the hook
so every destination (file, Iceberg, …) gets it for free.

## Limits (honest)

- **Lineage** — not modelled. A real design must say whether `write_batches`
  emits lineage once per transfer (same as `insert_rows` today).
- **Deferrable** — not modelled. Trigger payloads that carry rows stay on a
  row path unless the contract grows an async form.
- **Discoverability** — Protocols are structural; docs and provider metadata
  still need a way for users to find which hooks advertise which half.
- **Not Airflow** — `GenericRecordTransfer` is intentionally not a
  `BaseOperator` subclass so the demo stays dependency-light. Graduating it
  means wiring template fields and hook conn ids into the real operator.
- **File-to-file vs records** — the copy sink accepts an empty batch iterator
  and takes `source_fs` / `source_prefix` via destination params. A production
  design may want a separate file-level contract rather than overloading
  record batches.
- **gcsfs 2026.10+** — defaults to gRPC `ExtendedGcsFileSystem`; this demo
  pins call sites to `gcsfs.core.GCSFileSystem` so fake-gcs-server works.

## License

Apache-2.0. See [LICENSE](LICENSE).
