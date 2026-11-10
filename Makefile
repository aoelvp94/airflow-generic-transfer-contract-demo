# Mirror of justfile targets for reviewers without `just`.
.PHONY: audit rank-capabilities experiment-gcs-to-gcs experiment-s3-copy experiment-azure-copy install up down status test test-integration test-all bench bench-emulators bench-scale bench-scale-quick bench-scale-fresh profile-listing probe-emulators bench-adbc e2e check reproduce

install:
	uv venv .venv
	uv pip install -e '.[dev,iceberg,emulators,adbc]'

up:
	./scripts/emulators.sh up

down:
	./scripts/emulators.sh down

status:
	./scripts/emulators.sh status

test:
	.venv/bin/pytest -v -m 'not integration'

test-integration:
	.venv/bin/pytest -v -m integration

test-all:
	.venv/bin/pytest -v

bench:
	.venv/bin/python scripts/bench.py

bench-emulators:
	.venv/bin/python scripts/bench_emulators.py --rounds 5

bench-scale:
	.venv/bin/python scripts/bench_scale.py --rounds 5

bench-scale-quick:
	.venv/bin/python scripts/bench_scale.py --quick --rounds 3

# Restart emulators first: S3Mock slows down as objects accumulate (see docs/EMULATORS.md).
bench-scale-fresh:
	./scripts/emulators.sh down
	./scripts/emulators.sh up
	.venv/bin/python scripts/bench_scale.py --rounds 5

profile-listing:
	.venv/bin/python scripts/profile_listing.py --n 1000 --rounds 3 --cprofile

probe-emulators:
	./scripts/emulators.sh down
	./scripts/emulators.sh up
	.venv/bin/python scripts/probe_emulator_degradation.py --no-cleanup
	./scripts/emulators.sh down
	./scripts/emulators.sh up

bench-adbc:
	.venv/bin/python scripts/bench_adbc.py --rows 1000000 --rounds 3

# Pair-mapping audit of apache/airflow transfer operators (no emulators).
# Requires a clone at $AIRFLOW_SRC or /workspace/airflow-src (providers/ is enough).
audit:
	.venv/bin/python scripts/audit_transfers.py

rank-capabilities:
	.venv/bin/python scripts/rank_capabilities.py

# Run unmodified upstream GCSToGCSOperator unit tests against the file-contract shim.
# Uses a dedicated venv under experiments/gcs_to_gcs_shim/.venv (not the demo venv).
# Does not start emulators. Times out after 3 minutes.
experiment-gcs-to-gcs:
	@if [ ! -x experiments/gcs_to_gcs_shim/.venv/bin/python ]; then echo "experiment venv missing; see just experiment-gcs-to-gcs"; exit 1; fi
	timeout 180 env AIRFLOW_SRC="$${AIRFLOW_SRC:-/workspace/airflow-src}" experiments/gcs_to_gcs_shim/.venv/bin/python experiments/gcs_to_gcs_shim/run_upstream.py

e2e: up test-all bench bench-emulators bench-scale-quick bench-adbc

check: test bench

reproduce: install e2e
	@echo "Results in bench-out/ (BENCH.md, BENCH_SCALE_QUICK.md, BENCH_ADBC.md). Full scale run: just bench-scale-fresh"

experiment-s3-copy:
	timeout 180 env AIRFLOW_SRC="$${AIRFLOW_SRC:-/workspace/airflow-src}" experiments/gcs_to_gcs_shim/.venv/bin/python experiments/s3_copy_shim/run_upstream.py

experiment-azure-copy:
	timeout 180 env AIRFLOW_SRC="$${AIRFLOW_SRC:-/workspace/airflow-src}" experiments/gcs_to_gcs_shim/.venv/bin/python experiments/azure_copy_shim/run_upstream.py
