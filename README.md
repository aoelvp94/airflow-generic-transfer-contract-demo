# airflow-generic-transfer-contract-demo

Local prototype for **one transfer operator on hook contracts** instead of one
operator per source/destination pair. Supports the position in David Blain's
dev-list thread
([DISCUSS] One transfer operator on hook contracts instead of one operator per
source/destination pair, 2026-11-09).

No Airflow install required. Fake APIs only (Acme Orders, Globex CRM, …). No
employer branding. English throughout.

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
4. **Pair operators become thin shells** — the baseline in `tests/baseline_httptos3.py`
   stands in for today's `HttpToS3Operator`.

## Mapping to David's questions

| # | Question | What this demo argues |
|---|---|---|
| 1 | Are per-pair operators a maintenance problem? Should we stop accepting new ones? | Yes. Folder-marker test + stock page-1 baseline show the same bug/feature tax. |
| 2 | Rebase file-to-file on `ObjectStoragePath` / `FileTransferOperator` first? | Yes as step one. `ObjectStoreCopySink` + `list_transfer_keys` is that shape. |
| 3 | Where do record contracts live: `common.sql`, `common.io`, or new? | Demo is neutral; contracts are tiny Protocols. Real home is a community call. |
| 4 | New AIP? | Yes — AIP-14 is closed and incomplete. This is evidence for that AIP, not the AIP itself. |

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
fake_api/server.py      In-process Acme/Globex/Initech/Hooli APIs
tests/                  Contract, parity, folder-marker
scripts/bench.py        Wall time + request counts
```

## Run

```bash
uv venv .venv
uv pip install -e '.[dev,iceberg]'
.venv/bin/pytest -v
.venv/bin/python scripts/bench.py
```

## Bench (local, in-process fake API)

Acme Orders: 25 records, `limit=10` → 3 pages. 5 rounds, mean wall time.

| Path | Wall (mean, 5 rounds) | Requests | Records landed |
|------|----------------------|----------|----------------|
| Stock HttpToS3-style (page 1 only) | ~1.8 ms | 1 | 10 / 25 (silent truncate) |
| `GenericRecordTransfer` (paginated) | ~4.0 ms | 3 | 25 / 25 |

Numbers from `scripts/bench.py` on this box (in-process fake API). Re-run to refresh
`bench-out/bench.json`.

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

## License

Apache-2.0. See [LICENSE](LICENSE).
