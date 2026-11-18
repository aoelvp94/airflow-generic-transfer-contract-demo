# Emulator bench results

- Captured (UTC): `2026-11-09T21:33:37.286304+00:00`
- Platform: `Linux-6.12.94+-x86_64-with-glibc2.41`
- Python: `3.12.15`
- S3 backend: `s3mock` at `http://127.0.0.1:19090`
- GCS: `http://127.0.0.1:14443`
- Azure: `http://127.0.0.1:10000/devstoreaccount1`
- Rounds per path: **5** (cold = round 1, warm median = rounds 2..N)
- Images: `{'s3': 'adobe/s3mock:3.12.0', 'gcs': 'fsouza/fake-gcs-server:1.56.1', 'azure': 'mcr.microsoft.com/azure-storage/azurite:3.37.0'}`
- Packages: `{'fsspec': '2026.9.0', 's3fs': '2026.9.0', 'gcsfs': '2026.10.1', 'adlfs': '2026.8.0', 'boto3': '1.43.117', 'azure-storage-blob': '12.31.1', 'google-cloud-storage': '3.17.1'}`

## fsspec folder-marker differences

| Flavour | Native keys (SDK) | fsspec find() | Shared `list_transfer_keys` |
|---|---|---|---|
| s3 | `['benchdata/', 'benchdata/a.json', 'benchdata/nested/', 'benchdata/nested/b.json']` | `['benchdata/', 'benchdata/a.json', 'benchdata/nested/', 'benchdata/nested/b.json']` | `['src-bucket/benchdata/a.json', 'src-bucket/benchdata/nested/b.json']` |
| gcs | `['benchdata/', 'benchdata/a.json', 'benchdata/nested/', 'benchdata/nested/b.json']` | `['benchdata/', 'benchdata/a.json', 'benchdata/nested/', 'benchdata/nested/b.json']` | `['src-bucket/benchdata/a.json', 'src-bucket/benchdata/nested/b.json']` |
| azure | `['benchdata/', 'benchdata/a.json', 'benchdata/nested/', 'benchdata/nested/b.json']` | `['benchdata/a.json', 'benchdata/nested', 'benchdata/nested/b.json']` | `['src-container/benchdata/a.json', 'src-container/benchdata/nested/b.json']` |

Notes:
- **s3**: s3fs find() may or may not surface the walked prefix marker; nested zero-byte markers appear (sometimes without trailing slash). native=['benchdata/', 'benchdata/nested/'] fsspec=['benchdata/', 'benchdata/nested/'].
- **gcs**: gcsfs.core find() keeps trailing-slash markers; info() reports type=directory. native_markers=['benchdata/', 'benchdata/nested/'] fsspec_markers=['benchdata/', 'benchdata/nested/'].
- **azure**: adlfs find() drops the walked prefix marker but keeps nested zero-byte markers usually without a trailing slash. native=['benchdata/', 'benchdata/nested/'] fsspec=['benchdata/nested'].

## Per-pair copy (naive SDK baseline vs generic)

| Pair | Path | Cold (s) | Warm median (s) | Median (s) | Objects | Requests | Markers copied? |
|---|---|---:|---:|---:|---:|---:|---|
| `s3->gcs` | naive | 0.0475 | 0.0444 | 0.0449 | 4 | 9 | True |
| `s3->gcs` | generic | 0.0391 | 0.0346 | 0.0359 | 2 | 5 | False |
| `gcs->s3` | naive | 0.0461 | 0.0457 | 0.0461 | 4 | 9 | True |
| `gcs->s3` | generic | 0.0258 | 0.0244 | 0.0245 | 2 | 5 | False |
| `s3->azure` | naive | 0.0554 | 0.0512 | 0.0533 | 4 | 9 | True |
| `s3->azure` | generic | 0.0429 | 0.0372 | 0.0382 | 2 | 5 | False |
| `azure->s3` | naive | 0.0516 | 0.0544 | 0.0535 | 4 | 9 | True |
| `azure->s3` | generic | 0.0471 | 0.0468 | 0.0468 | 2 | 5 | False |
| `gcs->azure` | naive | 0.0317 | 0.0291 | 0.0298 | 4 | 9 | True |
| `gcs->azure` | generic | 0.0174 | 0.0177 | 0.0174 | 2 | 5 | False |
| `azure->gcs` | naive | 0.0291 | 0.0294 | 0.0292 | 4 | 9 | True |
| `azure->gcs` | generic | 0.0339 | 0.0337 | 0.0339 | 2 | 5 | False |

## Takeaway

list_transfer_keys skips markers on every pair; naive pair copy without the rule lands 4 keys including markers.

HTTP pagination evidence lives in scripts/bench.py (in-process); this bench is the file-pair / folder-marker half.

Reproduce: `just up && just bench-emulators`
