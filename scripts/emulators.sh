#!/usr/bin/env bash
# Start / stop / wait for object-store emulators.
# Prefers Docker Compose. Falls back to native Azurite (npm) if the azure
# container cannot start; S3/GCS still require Docker on this path.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Detect Docker. Prefer DOCKER_HOST if already set; else try unix socket, then TCP.
if [[ -z "${DOCKER_HOST:-}" ]]; then
  if [[ -S /var/run/docker.sock ]]; then
    :
  elif curl -sf --connect-timeout 1 http://127.0.0.1:2375/version >/dev/null 2>&1; then
    export DOCKER_HOST=tcp://127.0.0.1:2375
  fi
fi

COMPOSE=(docker compose -f docker-compose.yml)
if ! docker compose version >/dev/null 2>&1; then
  echo "docker compose plugin required" >&2
  exit 1
fi

load_env() {
  set -a
  # shellcheck disable=SC1091
  source "$ROOT/emulators.env"
  set +a
}

wait_http() {
  local url="$1" name="$2" attempts="${3:-60}"
  local i
  for i in $(seq 1 "$attempts"); do
    if curl -sf --connect-timeout 1 "$url" >/dev/null 2>&1 \
      || curl -sf --connect-timeout 1 -o /dev/null -w '' "$url" >/dev/null 2>&1; then
      echo "ok  $name ($url)"
      return 0
    fi
    # Some emulators return 403/400/404 on / — treat any HTTP response as up.
    code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 "$url" || true)
    if [[ "$code" =~ ^[12345][0-9][0-9]$ ]]; then
      echo "ok  $name ($url http=$code)"
      return 0
    fi
    sleep 0.5
  done
  echo "FAIL $name did not become ready: $url" >&2
  return 1
}

cmd_up() {
  load_env
  echo "DOCKER_HOST=${DOCKER_HOST:-unix:///var/run/docker.sock}"
  "${COMPOSE[@]}" up -d s3 gcs azure
  cmd_wait
}

cmd_down() {
  "${COMPOSE[@]}" down --remove-orphans || true
}

cmd_wait() {
  load_env
  wait_http "${S3_ENDPOINT}/" "s3(s3mock)"
  wait_http "http://${STORAGE_EMULATOR_HOST}/storage/v1/b" "gcs(fake-gcs)"
  wait_http "http://127.0.0.1:10000/" "azure(azurite)"
  echo "all emulators ready"
}

cmd_status() {
  load_env
  "${COMPOSE[@]}" ps || true
  echo "---"
  for pair in "s3 ${S3_ENDPOINT}/" "gcs http://${STORAGE_EMULATOR_HOST}/storage/v1/b" "azure http://127.0.0.1:10000/"; do
    set -- $pair
    code=$(curl -s -o /dev/null -w '%{http_code}' --connect-timeout 1 "$2" || echo down)
    echo "$1 -> $code ($2)"
  done
}

cmd_logs() {
  "${COMPOSE[@]}" logs --tail=80 "$@"
}

usage() {
  echo "usage: $0 {up|down|wait|status|logs}" >&2
  exit 2
}

case "${1:-}" in
  up) shift; cmd_up "$@" ;;
  down) shift; cmd_down "$@" ;;
  wait) shift; cmd_wait "$@" ;;
  status) shift; cmd_status "$@" ;;
  logs) shift; cmd_logs "$@" ;;
  *) usage ;;
esac
