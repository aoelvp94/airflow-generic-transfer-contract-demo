# Emulator PUT/GET latency vs objects stored

- Captured (UTC): `2026-11-10T06:26:46.814707+00:00`  samples per step: 30
- 4 KiB objects, native SDK PUT, fsspec GET, one bucket per provider.

| Provider | Objects in bucket | PUT median (ms) | GET median (ms) |
|---|---:|---:|---:|
| s3 | 0 | 4.6 | 7.3 |
| s3 | 2000 | 4.0 | 6.8 |
| s3 | 5000 | 5.6 | 8.9 |
| s3 | 10000 | 29.4 | 40.5 |
| s3 | 20000 | 69.3 | 92.5 |
| gcs | 0 | 1.2 | 2.1 |
| gcs | 2000 | 2.9 | 2.8 |
| gcs | 5000 | 5.6 | 3.8 |
| gcs | 10000 | 10.5 | 6.5 |
| gcs | 20000 | 20.5 | 10.1 |
| azure | 0 | 4.3 | 5.8 |
| azure | 2000 | 3.7 | 5.3 |
| azure | 5000 | 3.3 | 4.4 |
| azure | 10000 | 3.5 | 4.8 |
| azure | 20000 | 3.5 | 4.9 |
