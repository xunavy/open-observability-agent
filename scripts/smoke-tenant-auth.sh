#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
data_file="/tmp/open-observability-auth.jsonl"
usage_file="/tmp/open-observability-auth-usage.jsonl"
log_file="/tmp/open-observability-auth.log"
response_file="/tmp/open-observability-auth-response.json"
cors_headers="/tmp/open-observability-auth-cors-headers.txt"
tenant="$(cat /proc/sys/kernel/random/uuid)"
other_tenant="$(cat /proc/sys/kernel/random/uuid)"
observation_id="$(cat /proc/sys/kernel/random/uuid)"
api_key="tenant-smoke-secret"
rm -f "$data_file" "$usage_file" "$log_file" "$response_file" "$cors_headers"

curl_local() {
  curl --noproxy '*' --connect-timeout 1 --max-time 2 "$@"
}

OBSERVABILITY_ENV=production \
  OBSERVABILITY_API_KEYS="$tenant=$api_key" \
  OBSERVABILITY_CORS_ORIGINS=http://console.example.test \
  OBSERVABILITY_DATA="$data_file" \
  OBSERVABILITY_USAGE_DATA="$usage_file" \
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

preflight="$(curl_local -sS -o /dev/null -D "$cors_headers" -w '%{http_code}' \
  -X OPTIONS http://127.0.0.1:8080/v1/observations \
  -H 'origin: http://console.example.test' \
  -H 'access-control-request-method: POST' \
  -H 'access-control-request-headers: content-type,x-api-key,x-tenant-id')"
test "$preflight" = "200"
grep -qi '^access-control-allow-origin: http://console.example.test' "$cors_headers"
grep -qi '^access-control-allow-methods:.*POST' "$cors_headers"
grep -qi '^access-control-allow-headers:.*x-api-key' "$cors_headers"
grep -qi '^access-control-allow-headers:.*x-tenant-id' "$cors_headers"

unauthorized="$(curl_local -sS -o "$response_file" -w '%{http_code}' http://127.0.0.1:8080/metrics)"
test "$unauthorized" = "401"

cross_tenant="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  -X POST http://127.0.0.1:8080/v1/observations \
  -H 'content-type: application/json' \
  -H "x-api-key: $api_key" \
  -H "x-tenant-id: $tenant" \
  -d "{\"id\":\"$observation_id\",\"tenant_id\":\"$other_tenant\",\"trace_id\":\"auth-trace\",\"span_id\":\"auth-span\",\"kind\":\"Agent\",\"name\":\"auth.smoke\",\"status\":\"Ok\",\"started_at_ms\":1,\"duration_ms\":2,\"attributes\":{}}")"
test "$cross_tenant" = "403"

accepted="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  -X POST http://127.0.0.1:8080/v1/observations \
  -H 'content-type: application/json' \
  -H "x-api-key: $api_key" \
  -H "x-tenant-id: $tenant" \
  -d "{\"id\":\"$observation_id\",\"tenant_id\":\"$tenant\",\"trace_id\":\"auth-trace\",\"span_id\":\"auth-span\",\"kind\":\"Agent\",\"name\":\"auth.smoke\",\"status\":\"Ok\",\"started_at_ms\":1,\"duration_ms\":2,\"attributes\":{}}")"
test "$accepted" = "201"

echo 'Tenant auth smoke passed'
