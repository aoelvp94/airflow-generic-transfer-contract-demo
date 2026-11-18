# Native SDK client construction cost per GET

- Captured (UTC): `2026-11-10T06:51:26.290146+00:00`; median of 50; 4 KiB object

| Provider | New client + GET (ms) | Cached client GET (ms) | Overhead per object (ms) |
|---|---:|---:|---:|
| s3 | 5.6 | 1.6 | 4.0 |
| gcs | 2.4 | 0.7 | 1.8 |
| azure | 2.7 | 2.3 | 0.4 |
