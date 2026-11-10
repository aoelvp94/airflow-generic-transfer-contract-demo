# One-command targets for reviewers.
# Emulators: scripts/emulators.sh auto-detects DOCKER_HOST (unix socket or tcp://127.0.0.1:2375).

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
    #!/usr/bin/env bash
    set -euo pipefail
    DIR="experiments/gcs_to_gcs_shim"
    AIRFLOW_SRC="${AIRFLOW_SRC:-/workspace/airflow-src}"
    if [ ! -x "$DIR/.venv/bin/python" ]; then
        echo "experiment venv missing; create it with: uv venv $DIR/.venv && uv pip install --python $DIR/.venv/bin/python pytest apache-airflow-providers-google apache-airflow-providers-common-compat apache-airflow-providers-openlineage 'apache-airflow-core>=3'"
        exit 1
    fi
    timeout 180 env AIRFLOW_SRC="$AIRFLOW_SRC" "$DIR/.venv/bin/python" "$DIR/run_upstream.py"

# Full evidence loop for the dev-list thread.
e2e: up test-all bench bench-emulators bench-scale-quick bench-adbc

check: test bench

reproduce: install e2e
    @echo "Results in bench-out/ (BENCH.md, BENCH_SCALE_QUICK.md, BENCH_ADBC.md). Full scale run: just bench-scale-fresh"


# Unmodified upstream S3CopyObject + S3CopyPrefix unit tests against the file-contract shims.
# Reuses the experiment venv. No emulators. 3-minute timeout.
experiment-s3-copy:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -z "${DOCKER_HOST:-}" && ! -S /var/run/docker.sock ]]; then
        if curl -sf --connect-timeout 1 http://127.0.0.1:2375/version >/dev/null 2>&1; then
            export DOCKER_HOST=tcp://127.0.0.1:2375
        fi
    fi
    VENV="experiments/gcs_to_gcs_shim/.venv/bin/python"
    AIRFLOW_SRC="${AIRFLOW_SRC:-/workspace/airflow-src}"
    timeout 180 env AIRFLOW_SRC="$AIRFLOW_SRC" "$VENV" experiments/s3_copy_shim/run_upstream.py

# Unmodified upstream LocalFilesystemToWasb + S3ToAzureBlobStorage unit tests against the shims.
experiment-azure-copy:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -z "${DOCKER_HOST:-}" && ! -S /var/run/docker.sock ]]; then
        if curl -sf --connect-timeout 1 http://127.0.0.1:2375/version >/dev/null 2>&1; then
            export DOCKER_HOST=tcp://127.0.0.1:2375
        fi
    fi
    VENV="experiments/gcs_to_gcs_shim/.venv/bin/python"
    AIRFLOW_SRC="${AIRFLOW_SRC:-/workspace/airflow-src}"
    timeout 180 env AIRFLOW_SRC="$AIRFLOW_SRC" "$VENV" experiments/azure_copy_shim/run_upstream.py


# S3Mock parity for S3 copy shims. Starts only s3 if needed; stops only what it started.
experiment-s3-parity:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -z "${DOCKER_HOST:-}" && ! -S /var/run/docker.sock ]]; then
        if curl -sf --connect-timeout 1 http://127.0.0.1:2375/version >/dev/null 2>&1; then
            export DOCKER_HOST=tcp://127.0.0.1:2375
        fi
    fi
    VENV="experiments/gcs_to_gcs_shim/.venv/bin/python"
    STARTED=0
    if ! curl -s -o /dev/null -w '' --connect-timeout 1 http://127.0.0.1:19090/ && ! curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:19090/ | grep -qE '^[12345]'; then
        docker compose -f docker-compose.yml up -d s3
        STARTED=1
        for i in $(seq 1 60); do
            code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:19090/ || true)
            if [[ "$code" =~ ^[12345][0-9][0-9]$ ]]; then break; fi
            sleep 0.5
        done
    fi
    set +e
    timeout 180 env AIRFLOW_SRC="${AIRFLOW_SRC:-/workspace/airflow-src}" "$VENV" experiments/s3_copy_shim/parity.py
    RC=$?
    set -e
    if [ "$STARTED" = "1" ]; then docker compose -f docker-compose.yml stop s3; fi
    exit $RC

# Azurite (+ S3Mock for S3ToWasb) parity. Starts only s3+azure if needed; stops only what it started.
experiment-azure-parity:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -z "${DOCKER_HOST:-}" && ! -S /var/run/docker.sock ]]; then
        if curl -sf --connect-timeout 1 http://127.0.0.1:2375/version >/dev/null 2>&1; then
            export DOCKER_HOST=tcp://127.0.0.1:2375
        fi
    fi
    VENV="experiments/gcs_to_gcs_shim/.venv/bin/python"
    STARTED_S3=0
    STARTED_AZ=0
    code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:19090/ || true)
    if ! [[ "$code" =~ ^[12345][0-9][0-9]$ ]]; then
        docker compose -f docker-compose.yml up -d s3
        STARTED_S3=1
        for i in $(seq 1 60); do
            code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:19090/ || true)
            if [[ "$code" =~ ^[12345][0-9][0-9]$ ]]; then break; fi
            sleep 0.5
        done
    fi
    code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:10000/ || true)
    if ! [[ "$code" =~ ^[12345][0-9][0-9]$ ]]; then
        docker compose -f docker-compose.yml up -d azure
        STARTED_AZ=1
        for i in $(seq 1 60); do
            code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:10000/ || true)
            if [[ "$code" =~ ^[12345][0-9][0-9]$ ]]; then break; fi
            sleep 0.5
        done
    fi
    set +e
    timeout 180 env AIRFLOW_SRC="${AIRFLOW_SRC:-/workspace/airflow-src}" "$VENV" experiments/azure_copy_shim/parity.py
    RC=$?
    set -e
    if [ "$STARTED_S3" = "1" ]; then docker compose -f docker-compose.yml stop s3; fi
    if [ "$STARTED_AZ" = "1" ]; then docker compose -f docker-compose.yml stop azure; fi
    exit $RC


# One generic file-transfer operator on all 9 {S3,GCS,Azure}x{S3,GCS,Azure} pairs.
# Starts s3+gcs+azure only if they are down; stops only what this recipe started.
# Leaves nothing running. Wrapper unit tests do not need emulators and run second.
experiment-all-pairs:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -z "${DOCKER_HOST:-}" && ! -S /var/run/docker.sock ]]; then
        if curl -sf --connect-timeout 1 http://127.0.0.1:2375/version >/dev/null 2>&1; then
            export DOCKER_HOST=tcp://127.0.0.1:2375
        fi
    fi
    VENV="experiments/gcs_to_gcs_shim/.venv/bin/python"
    AIRFLOW_SRC="${AIRFLOW_SRC:-/workspace/airflow-src}"
    started=()
    ready() {
        local url="$1"
        local code
        code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 "$url" || true)
        [[ "$code" =~ ^[12345][0-9][0-9]$ ]]
    }
    wait_ready() {
        local url="$1" name="$2"
        local i
        for i in $(seq 1 60); do
            if ready "$url"; then
                echo "ok $name"
                return 0
            fi
            sleep 0.5
        done
        echo "FAIL $name not ready: $url" >&2
        return 1
    }
    if ! ready http://127.0.0.1:19090/; then
        docker compose -f docker-compose.yml up -d s3
        started+=(s3)
        wait_ready http://127.0.0.1:19090/ s3
    fi
    if ! ready http://127.0.0.1:14443/storage/v1/b; then
        docker compose -f docker-compose.yml up -d gcs
        started+=(gcs)
        wait_ready http://127.0.0.1:14443/storage/v1/b gcs
    fi
    if ! ready http://127.0.0.1:10000/; then
        docker compose -f docker-compose.yml up -d azure
        started+=(azure)
        wait_ready http://127.0.0.1:10000/ azure
    fi
    set +e
    timeout 600 env AIRFLOW_SRC="$AIRFLOW_SRC" "$VENV" experiments/generic_all_pairs/parity.py
    PARITY_RC=$?
    timeout 600 env AIRFLOW_SRC="$AIRFLOW_SRC" "$VENV" experiments/generic_all_pairs/run_wrappers.py
    WRAP_RC=$?
    set -e
    if ((${#started[@]})); then
        docker compose -f docker-compose.yml stop "${started[@]}"
    fi
    if [ "$PARITY_RC" -ne 0 ]; then exit "$PARITY_RC"; fi
    exit "$WRAP_RC"
