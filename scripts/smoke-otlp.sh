#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
database="/tmp/open-observability-otlp.sqlite"
log_file="/tmp/open-observability-otlp.log"
response_file="/tmp/open-observability-otlp-response.json"
observations_file="/tmp/open-observability-otlp-observations.json"
gzip_file="/tmp/open-observability-otlp-traces.json.gz"
tenant="$(cat /proc/sys/kernel/random/uuid)"
api_key="otlp-smoke-secret"
rm -f "$database" "$database-shm" "$database-wal" "$log_file" "$response_file" "$observations_file" "$gzip_file"
gzip -c examples/otlp-traces.json >"$gzip_file"

curl_local() {
  curl --noproxy '*' --connect-timeout 1 --max-time 3 "$@"
}

OBSERVABILITY_ENV=production \
  OBSERVABILITY_API_KEYS="$tenant=$api_key" \
  OBSERVABILITY_STORAGE=sqlite \
  OBSERVABILITY_SQLITE_DATA="$database" \
  OBSERVABILITY_INGEST_MODE=durable \
  OBSERVABILITY_QUEUE_POLL_MS=10 \
  cargo run -p observability-api --offline >"$log_file" 2>&1 &
api_pid=$!
cleanup() {
  kill "$api_pid" 2>/dev/null || true
  wait "$api_pid" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 180); do
  if curl_local -fsS http://127.0.0.1:8080/health >/dev/null 2>&1; then
    break
  fi
  sleep 1
done
curl_local -fsS http://127.0.0.1:8080/health >/dev/null

accepted="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  -X POST http://127.0.0.1:8080/v1/traces \
  -H 'content-type: application/json' \
  -H "x-api-key: $api_key" \
  -H "x-tenant-id: $tenant" \
  --data-binary @examples/otlp-traces.json)"
test "$accepted" = "200"

accepted_gzip="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  -X POST http://127.0.0.1:8080/v1/traces \
  -H 'content-type: application/json' \
  -H 'content-encoding: gzip' \
  -H "x-api-key: $api_key" \
  -H "x-tenant-id: $tenant" \
  --data-binary @"$gzip_file")"
test "$accepted_gzip" = "200"
test "$(cat "$response_file")" = "{}"

for _ in $(seq 1 100); do
  curl_local -fsS "http://127.0.0.1:8080/v1/observations?tenant_id=$tenant" \
    -H "x-api-key: $api_key" \
    -H "x-tenant-id: $tenant" >"$observations_file"
  if python3 -c 'import json,sys; raise SystemExit(0 if len(json.load(open(sys.argv[1], encoding="utf-8"))) == 1 else 1)' "$observations_file"; then
    break
  fi
  sleep 0.05
done

python3 -c '
import json, sys
items = json.load(open(sys.argv[1], encoding="utf-8"))
assert len(items) == 1, items
item = items[0]
assert item["trace_id"] == "01010101010101010101010101010101", item
assert item["span_id"] == "0202020202020202", item
assert item["kind"] == "Agent", item
assert item["status"] == "Error", item
assert item["duration_ms"] == 328, item
assert item["attributes"]["resource.service.name"] == "checkout-service", item
' "$observations_file"

metrics="$(curl_local -fsS http://127.0.0.1:8080/metrics \
  -H "x-api-key: $api_key" \
  -H "x-tenant-id: $tenant")"
grep -q '^observability_otlp_spans_accepted_total 2$' <<<"$metrics"
grep -q '^observability_otlp_spans_rejected_total 0$' <<<"$metrics"

echo 'OTLP trace smoke passed'
