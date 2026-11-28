# One-command targets for reviewers.
# Emulators: emulators/emulators.sh. Auto-detects DOCKER_HOST.

install:
    uv venv .venv
    uv pip install -e '.[dev,iceberg,emulators,adbc]'

up:
    ./emulators/emulators.sh up

down:
    ./emulators/emulators.sh down

status:
    ./emulators/emulators.sh status

test:
    .venv/bin/pytest -v -m 'not integration'

test-integration:
    .venv/bin/pytest -v -m integration

test-all:
    .venv/bin/pytest -v

bench:
    .venv/bin/python benchmarks/bench.py

bench-emulators:
    .venv/bin/python benchmarks/bench_emulators.py --rounds 5

bench-scale:
    .venv/bin/python benchmarks/bench_scale.py --rounds 5

bench-scale-quick:
    .venv/bin/python benchmarks/bench_scale.py --quick --rounds 3

# Restart emulators first: S3Mock slows down as objects accumulate (see docs/AZURE_SLOWDOWN.md).
bench-scale-fresh:
    ./emulators/emulators.sh down
    ./emulators/emulators.sh up
    .venv/bin/python benchmarks/bench_scale.py --rounds 5

profile-listing:
    .venv/bin/python benchmarks/profile_listing.py --n 1000 --rounds 3 --cprofile

probe-emulators:
    ./emulators/emulators.sh down
    ./emulators/emulators.sh up
    .venv/bin/python benchmarks/probe_emulator_degradation.py --no-cleanup
    ./emulators/emulators.sh down
    ./emulators/emulators.sh up

bench-adbc:
    .venv/bin/python benchmarks/bench_adbc.py --rows 1000000 --rounds 3

# Pair-mapping audit of apache/airflow transfer operators (no emulators).
# Requires a clone at $AIRFLOW_SRC or /workspace/airflow-src (providers/ is enough).
audit:
    .venv/bin/python audits/audit_transfers.py

rank-capabilities:
    .venv/bin/python audits/rank_capabilities.py

# Run unmodified upstream GCSToGCSOperator unit tests against the file-contract shim.
# Uses a dedicated venv under shims/gcs_to_gcs_shim/.venv (not the demo venv).
# Does not start emulators. Times out after 3 minutes.
experiment-gcs-to-gcs:
    #!/usr/bin/env bash
    set -euo pipefail
    DIR="shims/gcs_to_gcs_shim"
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
    @echo "Results in benchmarks/results/ (BENCH.md, BENCH_SCALE_QUICK.md, BENCH_ADBC.md). Full scale run: just bench-scale-fresh"


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
    VENV="shims/gcs_to_gcs_shim/.venv/bin/python"
    AIRFLOW_SRC="${AIRFLOW_SRC:-/workspace/airflow-src}"
    timeout 180 env AIRFLOW_SRC="$AIRFLOW_SRC" "$VENV" shims/s3_copy_shim/run_upstream.py

# Unmodified upstream LocalFilesystemToWasb + S3ToAzureBlobStorage unit tests against the shims.
experiment-azure-copy:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -z "${DOCKER_HOST:-}" && ! -S /var/run/docker.sock ]]; then
        if curl -sf --connect-timeout 1 http://127.0.0.1:2375/version >/dev/null 2>&1; then
            export DOCKER_HOST=tcp://127.0.0.1:2375
        fi
    fi
    VENV="shims/gcs_to_gcs_shim/.venv/bin/python"
    AIRFLOW_SRC="${AIRFLOW_SRC:-/workspace/airflow-src}"
    timeout 180 env AIRFLOW_SRC="$AIRFLOW_SRC" "$VENV" shims/azure_copy_shim/run_upstream.py


# S3Mock parity for S3 copy shims. Starts only s3 if needed; stops only what it started.
experiment-s3-parity:
    #!/usr/bin/env bash
    set -euo pipefail
    if [[ -z "${DOCKER_HOST:-}" && ! -S /var/run/docker.sock ]]; then
        if curl -sf --connect-timeout 1 http://127.0.0.1:2375/version >/dev/null 2>&1; then
            export DOCKER_HOST=tcp://127.0.0.1:2375
        fi
    fi
    VENV="shims/gcs_to_gcs_shim/.venv/bin/python"
    STARTED=0
    if ! curl -s -o /dev/null -w '' --connect-timeout 1 http://127.0.0.1:19090/ && ! curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:19090/ | grep -qE '^[12345]'; then
        docker compose -f emulators/docker-compose.yml up -d s3
        STARTED=1
        for i in $(seq 1 60); do
            code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:19090/ || true)
            if [[ "$code" =~ ^[12345][0-9][0-9]$ ]]; then break; fi
            sleep 0.5
        done
    fi
    set +e
    timeout 180 env AIRFLOW_SRC="${AIRFLOW_SRC:-/workspace/airflow-src}" "$VENV" shims/s3_copy_shim/parity.py
    RC=$?
    set -e
    if [ "$STARTED" = "1" ]; then docker compose -f emulators/docker-compose.yml stop s3; fi
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
    VENV="shims/gcs_to_gcs_shim/.venv/bin/python"
    STARTED_S3=0
    STARTED_AZ=0
    code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:19090/ || true)
    if ! [[ "$code" =~ ^[12345][0-9][0-9]$ ]]; then
        docker compose -f emulators/docker-compose.yml up -d s3
        STARTED_S3=1
        for i in $(seq 1 60); do
            code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:19090/ || true)
            if [[ "$code" =~ ^[12345][0-9][0-9]$ ]]; then break; fi
            sleep 0.5
        done
    fi
    code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:10000/ || true)
    if ! [[ "$code" =~ ^[12345][0-9][0-9]$ ]]; then
        docker compose -f emulators/docker-compose.yml up -d azure
        STARTED_AZ=1
        for i in $(seq 1 60); do
            code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 http://127.0.0.1:10000/ || true)
            if [[ "$code" =~ ^[12345][0-9][0-9]$ ]]; then break; fi
            sleep 0.5
        done
    fi
    set +e
    timeout 180 env AIRFLOW_SRC="${AIRFLOW_SRC:-/workspace/airflow-src}" "$VENV" shims/azure_copy_shim/parity.py
    RC=$?
    set -e
    if [ "$STARTED_S3" = "1" ]; then docker compose -f emulators/docker-compose.yml stop s3; fi
    if [ "$STARTED_AZ" = "1" ]; then docker compose -f emulators/docker-compose.yml stop azure; fi
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
    VENV="shims/gcs_to_gcs_shim/.venv/bin/python"
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
        docker compose -f emulators/docker-compose.yml up -d s3
        started+=(s3)
        wait_ready http://127.0.0.1:19090/ s3
    fi
    if ! ready http://127.0.0.1:14443/storage/v1/b; then
        docker compose -f emulators/docker-compose.yml up -d gcs
        started+=(gcs)
        wait_ready http://127.0.0.1:14443/storage/v1/b gcs
    fi
    if ! ready http://127.0.0.1:10000/; then
        docker compose -f emulators/docker-compose.yml up -d azure
        started+=(azure)
        wait_ready http://127.0.0.1:10000/ azure
    fi
    set +e
    timeout 600 env AIRFLOW_SRC="$AIRFLOW_SRC" "$VENV" shims/generic_all_pairs/parity.py
    PARITY_RC=$?
    timeout 600 env AIRFLOW_SRC="$AIRFLOW_SRC" "$VENV" shims/generic_all_pairs/run_wrappers.py
    WRAP_RC=$?
    set -e
    if ((${#started[@]})); then
        docker compose -f emulators/docker-compose.yml stop "${started[@]}"
    fi
    if [ "$PARITY_RC" -ne 0 ]; then exit "$PARITY_RC"; fi
    exit "$WRAP_RC"


# AIP-132 phase 1. Separate venv so the default demo env stays free of Airflow.
# Unit tests and the file:// / memory:// conformance suite. Integration is
# documented in docs/AIP-132-demo.md and needs `just up`.
aip132-test:
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="${HOME}/.local/bin:${PATH}"
    PY=".venv-aip132/bin/python"
    if [[ ! -x "$PY" ]] || ! "$PY" -c 'import airflow, aip132.file_transfer, airflow.providers.openlineage' >/dev/null 2>&1; then
        command -v uv >/dev/null 2>&1 || { echo "uv is required: https://docs.astral.sh/uv/"; exit 1; }
        uv venv .venv-aip132 --python 3.12
        uv pip install --python .venv-aip132/bin/python -e '.[dev,aip132]' \
            --constraint 'https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt'
    fi
    export AIRFLOW_HOME="${AIRFLOW_HOME:-$PWD/.airflow-aip132}"
    export AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__UNIT_TEST_MODE=True
    "$PY" -m pytest -v -m 'not integration and not e2e' tests/unit/test_aip132_file_transfer.py tests/unit/test_aip132_openlineage.py tests/conformance/test_storage_contract.py

aip132-demo:
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="${HOME}/.local/bin:${PATH}"
    PY=".venv-aip132/bin/python"
    if [[ ! -x "$PY" ]] || ! "$PY" -c 'import airflow, aip132.file_transfer, airflow.providers.openlineage' >/dev/null 2>&1; then
        command -v uv >/dev/null 2>&1 || { echo "uv is required: https://docs.astral.sh/uv/"; exit 1; }
        uv venv .venv-aip132 --python 3.12
        uv pip install --python .venv-aip132/bin/python -e '.[dev,aip132]' \
            --constraint 'https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt'
    fi
    export AIRFLOW_HOME="${AIRFLOW_HOME:-$PWD/.airflow-aip132}"
    export AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__UNIT_TEST_MODE=True
    "$PY" examples/aip132_s3_prefix_to_gcs.py

# Scheduler run of examples/aip132_s3_prefix_to_gcs.py.
# Brings up the object-store emulators and Airflow 3.4.0 (api-server, scheduler,
# dag-processor; LocalExecutor, no triggerer), triggers the Dag, checks the
# XCom and an OpenLineage console event, then tears both stacks down.
aip132-e2e:
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="${HOME}/.local/bin:${PATH}"
    if [[ -z "${DOCKER_HOST:-}" && ! -S /var/run/docker.sock ]]; then
        if curl -sf --connect-timeout 1 http://127.0.0.1:2375/version >/dev/null 2>&1; then
            export DOCKER_HOST=tcp://127.0.0.1:2375
        fi
    fi
    PY=".venv-aip132/bin/python"
    if [[ ! -x "$PY" ]] || ! "$PY" -c 'import airflow, aip132.file_transfer, airflow.providers.openlineage' >/dev/null 2>&1; then
        command -v uv >/dev/null 2>&1 || { echo "uv is required: https://docs.astral.sh/uv/"; exit 1; }
        uv venv .venv-aip132 --python 3.12
        uv pip install --python .venv-aip132/bin/python -e '.[dev,aip132]' \
            --constraint 'https://raw.githubusercontent.com/apache/airflow/constraints-3.4.0/constraints-3.12.txt'
    fi
    COMPOSE=(docker compose -f aip132/docker/docker-compose.yml)
    mkdir -p aip132/docker/logs
    if [[ ! -f aip132/docker/.env ]]; then
        key=$("$PY" -c 'from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())')
        printf 'FERNET_KEY=%s\nAIRFLOW_UID=%s\n' "$key" "$(id -u)" > aip132/docker/.env
    fi
    cleanup() {
        "${COMPOSE[@]}" logs --no-color > aip132/docker/e2e-compose.log 2>&1 || true
        "${COMPOSE[@]}" down -v --remove-orphans || true
        ./emulators/emulators.sh down || true
    }
    trap cleanup EXIT
    ./emulators/emulators.sh up
    rm -rf aip132/docker/build
    mkdir -p aip132/docker/build
    cp -a src/aip132 aip132/docker/build/aip132
    find aip132/docker/build -type d -name __pycache__ -print0 | xargs -0 -r rm -rf
    cp examples/aip132_s3_prefix_to_gcs.py aip132/docker/build/aip132_s3_prefix_to_gcs.py
    "${COMPOSE[@]}" up -d --build
    echo "waiting for the Airflow API and scheduler"
    api_ok=0
    sched_ok=0
    for _ in $(seq 1 90); do
        code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 http://127.0.0.1:18080/api/v2/monitor/health || true)
        if [[ "$code" == "200" ]]; then api_ok=1; fi
        code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 2 http://127.0.0.1:8974/health || true)
        if [[ "$code" == "200" ]]; then sched_ok=1; fi
        if [[ "$api_ok" == "1" && "$sched_ok" == "1" ]]; then
            echo "api and scheduler are up"
            break
        fi
        sleep 2
    done
    if [[ "$api_ok" != "1" || "$sched_ok" != "1" ]]; then
        echo "Airflow did not become healthy (api=$api_ok scheduler=$sched_ok)" >&2
        "${COMPOSE[@]}" ps || true
        exit 1
    fi
    export AIP132_E2E_REQUIRE=1
    export AIP132_AIRFLOW_URL=http://127.0.0.1:18080
    export AIRFLOW_HOME="${AIRFLOW_HOME:-$PWD/.airflow-aip132}"
    export AIRFLOW__CORE__LOAD_EXAMPLES=False
    export AIRFLOW__CORE__UNIT_TEST_MODE=True
    "$PY" -m pytest -vv -s tests/e2e/test_aip132_scheduler.py

# singledispatch versus the Protocol checks. No Airflow extra.
singledispatch-check:
    #!/usr/bin/env bash
    set -euo pipefail
    export PATH="${HOME}/.local/bin:${PATH}"
    uv run --no-project --python 3.12 --with pytest \
        python -m pytest -v --noconftest tests/unit/test_singledispatch_vs_protocol.py
    uv run --no-project --python 3.12 python examples/singledispatch_vs_protocol.py
