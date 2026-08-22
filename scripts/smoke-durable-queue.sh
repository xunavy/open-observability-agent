#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
database="/tmp/open-observability-durable-queue.sqlite"
log_file="/tmp/open-observability-durable-queue.log"
response_file="/tmp/open-observability-durable-response.json"
tenant="$(cat /proc/sys/kernel/random/uuid)"
observation_id="$(cat /proc/sys/kernel/random/uuid)"
rm -f "$database" "$database-shm" "$database-wal" "$log_file" "$response_file"

curl_local() {
  curl --noproxy '*' --connect-timeout 1 --max-time 2 "$@"
}

start_api() {
  local poll_ms="$1"
  OBSERVABILITY_STORAGE=sqlite \
    OBSERVABILITY_SQLITE_DATA="$database" \
    OBSERVABILITY_INGEST_MODE=durable \
    OBSERVABILITY_QUEUE_POLL_MS="$poll_ms" \
    cargo run -p observability-api --offline >"$log_file" 2>&1 &
  api_pid=$!
  for _ in $(seq 1 180); do
    if curl_local -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; then
      return
    fi
    sleep 1
  done
  cat "$log_file"
  return 1
}

stop_api() {
  kill "$api_pid" 2>/dev/null || true
  wait "$api_pid" 2>/dev/null || true
}

cleanup() {
  stop_api
}
trap cleanup EXIT

start_api 60000
status="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  -X POST http://127.0.0.1:8080/v1/observations \
  -H 'content-type: application/json' \
  -d "{\"id\":\"$observation_id\",\"tenant_id\":\"$tenant\",\"trace_id\":\"durable-trace\",\"span_id\":\"durable-span\",\"kind\":\"Agent\",\"name\":\"durable.smoke\",\"status\":\"Ok\",\"started_at_ms\":1,\"duration_ms\":2,\"attributes\":{\"source\":\"smoke\"}}")"
test "$status" = "202"
queue_before="$(curl_local -fsS "http://127.0.0.1:8080/v1/ingestion/queue?tenant_id=$tenant")"
echo "$queue_before" | grep -q '"pending":1'
stop_api

start_api 10
for _ in $(seq 1 100); do
  observations="$(curl_local -fsS "http://127.0.0.1:8080/v1/observations?tenant_id=$tenant")"
  queue_after="$(curl_local -fsS "http://127.0.0.1:8080/v1/ingestion/queue?tenant_id=$tenant")"
  if echo "$observations" | grep -q "$observation_id" && echo "$queue_after" | grep -q '"pending":0'; then
    echo "$observations"
    echo "$queue_after"
    echo 'Durable queue restart smoke passed'
    exit 0
  fi
  sleep 0.05
done

cat "$log_file"
echo 'Durable queue did not drain after restart' >&2
exit 1
