# Emulator bench results

- Captured (UTC): `2026-11-09T22:59:58.123868+00:00`
- Platform: `Linux-6.12.113+-x86_64-with-glibc2.41`
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
| `s3->gcs` | naive | 0.0505 | 0.0540 | 0.0528 | 4 | 9 | True |
| `s3->gcs` | generic | 0.0408 | 0.0427 | 0.0424 | 2 | 5 | False |
| `gcs->s3` | naive | 0.0320 | 0.0331 | 0.0320 | 4 | 9 | True |
| `gcs->s3` | generic | 0.0257 | 0.0177 | 0.0178 | 2 | 5 | False |
| `s3->azure` | naive | 0.0667 | 0.0708 | 0.0703 | 4 | 9 | True |
| `s3->azure` | generic | 0.0528 | 0.0529 | 0.0528 | 2 | 5 | False |
| `azure->s3` | naive | 0.0541 | 0.0406 | 0.0411 | 4 | 9 | True |
| `azure->s3` | generic | 0.0444 | 0.0447 | 0.0444 | 2 | 5 | False |
| `gcs->azure` | naive | 0.0323 | 0.0305 | 0.0318 | 4 | 9 | True |
| `gcs->azure` | generic | 0.0172 | 0.0190 | 0.0190 | 2 | 5 | False |
| `azure->gcs` | naive | 0.0343 | 0.0270 | 0.0274 | 4 | 9 | True |
| `azure->gcs` | generic | 0.0355 | 0.0384 | 0.0382 | 2 | 5 | False |

## Takeaway

list_transfer_keys skips markers on every pair; naive pair copy without the rule lands 4 keys including markers.

HTTP pagination evidence lives in scripts/bench.py (in-process); this bench is the file-pair / folder-marker half.

Reproduce: `just up && just bench-emulators`
