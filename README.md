# airflow-generic-transfer-contract-demo

Local prototype for **one transfer operator on hook contracts** instead of one
operator per source/destination pair. Supports the position in David Blain's
dev-list thread
([DISCUSS] One transfer operator on hook contracts instead of one operator per
source/destination pair, 2026-11-09).

No Airflow install required. Fake APIs + local object-store emulators only.
No employer branding. English throughout.

## Thesis

Airflow today ships ~86 transfer modules (~17,500 lines) across 13 providers.
Every fix and every feature (pagination, file format, async, lineage) lands in
one pair operator and is missing from the others. The folder-marker bug is the
concrete proof: the same "skip a zero-byte key that has keys under it" rule was
applied in four PRs across three providers in six months, and main now carries
three copies of the same helper.

This demo shows the missing piece as runnable code:

1. **Capability on the hook** — `read_batches(**params)` / `write_batches(batches, **params)`.
2. **Separate file contract** — `FileCopyable.copy_files(**params)` (not overloaded on record batches).
3. **One generic operator** — forwards hook params, validates at run time, optional lineage.
4. **Optional async form** — `aread_batches` / `awrite_batches` sketch for deferrable.
5. **Discoverability** — `describe_params()` JSON-schema-like; operator rejects bad kwargs.
6. **Arrow/ADBC fit** — batch payload is `Any`; hooks advertise `batch_format`.

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
# or: make reproduce
```

Expected:

- `just test-all` → **47 passed** (33 unit + 14 integration).
- `just bench` → stock HttpToS3-style keeps page 1 (10/25); generic lands 25/25.
- `just bench-emulators` → naive 4 objects / ~9 req; generic 2 / ~5 (marker tree).
- `just bench-scale-quick` → large/small/mixed/server-side/latency → `bench-out/BENCH_SCALE.md`.
- `just bench-scale` → full **100 MiB** + **1000** small objects (slower).
- `just bench-adbc` → 1M-row Arrow e2e vs rows → `bench-out/BENCH_ADBC.md`.

Individual steps:

```bash
just install              # uv venv + extras (dev, iceberg, emulators, adbc)
just up                   # S3Mock + fake-gcs-server + Azurite
just test                 # unit (includes edge + open points + ADBC)
just test-integration     # needs emulators
just bench                # HTTP pagination evidence
just bench-emulators      # cross-cloud marker findings
just bench-scale-quick    # scale/latency (8 MiB / 100 objs)
just bench-adbc           # Arrow vs rows on the same contract
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

**S3 backend note.** Default is **Adobe S3Mock** (MinIO Docker Hub pulls were
denied on this box). Reviewers with Docker Hub access can switch — see earlier
commits / `EMULATOR_S3_BACKEND=minio` in `emulators.env`.

Connection settings: `emulators.env` (quote Azure's connection string — it
contains `;`).

## Findings → dev-list claims

| Claim (dev-list) | Evidence in this repo | Command |
|---|---|---|
| Same folder-marker fix landed 4× across providers | Naive S3→GCS copies 4 keys including markers; generic copies 2. Shared `list_transfer_keys` used once. | `just up && just test-integration -k marker` / `just bench-emulators` |
| Pagination missing in HttpToS3-style operators | Stock baseline keeps page 1 (10/25); generic + hook pagination lands 25/25. | `just bench` / `just test -k parity` |
| Per-pair vs generic behaviour parity (files) | All 6 ordered pairs (S3↔GCS↔Azure) pass the same generic copy assertions. | `just up && just test-integration` |
| Per-pair vs generic request counts | Naive ≈ 9 req; generic ≈ 5 on the marker tree. | `just bench-emulators` |
| fsspec marker behaviour differs by backend | gcsfs keeps trailing-slash markers; adlfs drops walked prefix / keeps nested without slash; s3fs varies. Shared helper collapses all three. | `just bench-emulators` / `test_fsspec_find_differs_on_markers` |
| Request count dominates under real RTT | Latency demo injects 25 ms/req; wall time tracks request_total (naive 9 vs generic 5). | `just bench-scale-quick` → latency_demo |
| Parallelism is a param, not a new operator | `workers=4/8/16` on `FileCopyable.copy_files`. | `just bench-scale` |
| Same-provider server-side copy | S3 `copy_object` / GCS `copy_blob` / Azure `start_copy_from_url` vs read+write. | `just bench-scale` → server_side |
| Lineage / discoverability / deferrable | Optional Protocols + tests; async sketch with asyncio. | `just test -k open_points` |
| File copy ≠ record batches | `FileCopyable` + `GenericFileTransfer`; `write_batches` shim kept for compat. | `just test -k file_copyable` |
| Arrow/ADBC fits the Protocol | `AdbcSqliteSource/Sink` stream `RecordBatch` through `GenericRecordTransfer`. | `just test -k adbc` / `just bench-adbc` |
| Some operators stay out of wave one | Partitioning / schema JSON / format-specific pairs deferred. | [`docs/first-wave-exclusions.md`](docs/first-wave-exclusions.md) |

Committed snapshots: [`bench-out/BENCH.md`](bench-out/BENCH.md),
[`bench-out/BENCH_SCALE.md`](bench-out/BENCH_SCALE.md),
[`bench-out/BENCH_ADBC.md`](bench-out/BENCH_ADBC.md).

## Does the contract fit Arrow/ADBC?

**Short answer: yes, with open decisions.** This is a fit check only — not the
separate Arrow/ADBC GenericTransfer proposal.

| Works (evidence) | Needs a decision |
|---|---|
| Protocols take `Any` batches; `isinstance` is structural | Batch-type negotiation when source is Arrow and sink wants rows (or vice versa) |
| Hooks advertise `batch_format` (`rows` / `record_batch`) | Schema cast policy at operator vs hook boundary |
| `AdbcSqliteSource` → `AdbcSqliteSink` e2e via `GenericRecordTransfer` | Whether file + record share one operator or stay split (`GenericFileTransfer` vs record) |
| 1M-row bench: Arrow e2e vs Arrow→rows coercion (`just bench-adbc`) | |

See `src/generic_transfer/adapters/adbc_io.py` and `tests/test_adbc.py`.

## Layout

```
src/generic_transfer/
  contract.py           BatchReadable/Writable, FileCopyable, Lineage, Params, Async*
  operator.py           GenericRecordTransfer, GenericFileTransfer, GenericAsyncRecordTransfer
  pagination.py         Stateless HTTP pagination helpers
  adapters/
    http_source.py      PaginatedHttpSourceHook (+ retry, lineage, describe_params, async)
    file_sink.py        FileBatchSink, ObjectStoreCopySink (FileCopyable), markers
    file_copy.py        Parallel / sequential / server-side / latency injector
    iceberg_sink.py     IcebergAppendSink (schema-drift policy + async)
    adbc_io.py          AdbcSqliteSource/Sink + rows baselines
    emulators.py        fsspec + native SDK factory; seed large/small/mixed
    pair_baseline.py    Naive per-pair native-SDK copy
docs/first-wave-exclusions.md
fake_api/server.py
docker-compose.yml / emulators.env / scripts/
tests/                  contract, parity, markers, emulators, edges, open points, ADBC
bench-out/              BENCH.md, BENCH_SCALE.md, BENCH_ADBC.md
```

## Bench (local, in-process fake API)

Acme Orders: 25 records, `limit=10` → 3 pages. 5 rounds, mean wall time.

| Path | Wall (mean, 5 rounds) | Requests | Records landed |
|------|----------------------|----------|----------------|
| Stock HttpToS3-style (page 1 only) | ~1.8 ms | 1 | 10 / 25 (silent truncate) |
| `GenericRecordTransfer` (paginated) | ~4.0 ms | 3 | 25 / 25 |

Re-run: `just bench` → `bench-out/bench.json`.

## Limits (honest)

- **Not Airflow** — operators are intentionally not `BaseOperator` subclasses.
- **Deferrable** — async sketch only; no real Trigger/Deferral integration.
- **Emulators ≠ cloud** — localhost medians are client overhead; use the
  latency demo for request-count arguments.
- **gcsfs 2026.10+** — call sites pin `gcsfs.core.GCSFileSystem` for fake-gcs-server.
- **Server-side copy** — depends on emulator support; failures are recorded in
  `BENCH_SCALE.md` rather than hidden.

## License

Apache-2.0. See [LICENSE](LICENSE).
