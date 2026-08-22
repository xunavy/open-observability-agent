#!/usr/bin/env bash
set -euo pipefail

cd "$(dirname "$0")/.."
database="/tmp/open-observability-investigation.sqlite"
log_file="/tmp/open-observability-investigation.log"
response_file="/tmp/open-observability-investigation-response.json"
tenant_a="$(cat /proc/sys/kernel/random/uuid)"
tenant_b="$(cat /proc/sys/kernel/random/uuid)"
evidence_a="$(cat /proc/sys/kernel/random/uuid)"
evidence_b="$(cat /proc/sys/kernel/random/uuid)"
key_a="investigation-key-a"
key_b="investigation-key-b"
period="$(date -u +%Y-%m)"
rm -f "$database" "$database-shm" "$database-wal" "$log_file" "$response_file"

curl_local() {
  curl --noproxy '*' --connect-timeout 1 --max-time 3 "$@"
}

start_api() {
  OBSERVABILITY_ENV=production \
    OBSERVABILITY_API_KEYS="$tenant_a=$key_a,$tenant_b=$key_b" \
    OBSERVABILITY_STORAGE=sqlite \
    OBSERVABILITY_SQLITE_DATA="$database" \
    OBSERVABILITY_INGEST_MODE=direct \
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
  kill "${api_pid:-}" 2>/dev/null || true
  wait "${api_pid:-}" 2>/dev/null || true
}

cleanup() {
  stop_api
}
trap cleanup EXIT

post_observation() {
  local tenant="$1"
  local key="$2"
  local observation_id="$3"
  local name="$4"
  local status
  status="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
    -X POST http://127.0.0.1:8080/v1/observations \
    -H 'content-type: application/json' \
    -H "x-tenant-id: $tenant" \
    -H "x-api-key: $key" \
    -d "{\"id\":\"$observation_id\",\"tenant_id\":\"$tenant\",\"trace_id\":\"investigation-trace\",\"span_id\":\"$observation_id\",\"kind\":\"Agent\",\"name\":\"$name\",\"status\":\"Error\",\"started_at_ms\":1787414400000,\"duration_ms\":420,\"attributes\":{\"source\":\"smoke\"}}")"
  test "$status" = "201"
}

start_api
post_observation "$tenant_a" "$key_a" "$evidence_a" "checkout.failed"
post_observation "$tenant_b" "$key_b" "$evidence_b" "private.failed"

create_status="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  -X POST http://127.0.0.1:8080/v1/investigations \
  -H 'content-type: application/json' \
  -H 'idempotency-key: investigation-smoke-1' \
  -H "x-tenant-id: $tenant_a" \
  -H "x-api-key: $key_a" \
  -d "{\"objective\":\"解释 checkout 失败\",\"evidence_ids\":[\"$evidence_a\"]}")"
test "$create_status" = "201"
run_id="$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$response_file")"
test "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["status"])' "$response_file")" = "Planned"

repeat_status="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  -X POST http://127.0.0.1:8080/v1/investigations \
  -H 'content-type: application/json' \
  -H 'idempotency-key: investigation-smoke-1' \
  -H "x-tenant-id: $tenant_a" \
  -H "x-api-key: $key_a" \
  -d "{\"objective\":\"解释 checkout 失败\",\"evidence_ids\":[\"$evidence_a\"]}")"
test "$repeat_status" = "200"
test "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["id"])' "$response_file")" = "$run_id"

conflict_status="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  -X POST http://127.0.0.1:8080/v1/investigations \
  -H 'content-type: application/json' \
  -H 'idempotency-key: investigation-smoke-1' \
  -H "x-tenant-id: $tenant_a" \
  -H "x-api-key: $key_a" \
  -d "{\"objective\":\"不同请求\",\"evidence_ids\":[\"$evidence_a\"]}")"
test "$conflict_status" = "409"

cross_evidence_status="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  -X POST http://127.0.0.1:8080/v1/investigations \
  -H 'content-type: application/json' \
  -H 'idempotency-key: cross-tenant-evidence' \
  -H "x-tenant-id: $tenant_a" \
  -H "x-api-key: $key_a" \
  -d "{\"objective\":\"读取其他租户\",\"evidence_ids\":[\"$evidence_b\"]}")"
test "$cross_evidence_status" = "400"

execute_status="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  -X POST "http://127.0.0.1:8080/v1/investigations/$run_id/execute" \
  -H "x-tenant-id: $tenant_a" \
  -H "x-api-key: $key_a")"
test "$execute_status" = "200"
result_id="$(python3 -c 'import json,sys; data=json.load(open(sys.argv[1])); assert data["status"] == "Completed"; print(data["result_observation_id"])' "$response_file")"

curl_local -fsS -X POST "http://127.0.0.1:8080/v1/investigations/$run_id/execute" \
  -H "x-tenant-id: $tenant_a" \
  -H "x-api-key: $key_a" >"$response_file"
test "$(python3 -c 'import json,sys; print(json.load(open(sys.argv[1]))["result_observation_id"])' "$response_file")" = "$result_id"

cross_run_status="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  "http://127.0.0.1:8080/v1/investigations/$run_id" \
  -H "x-tenant-id: $tenant_b" \
  -H "x-api-key: $key_b")"
test "$cross_run_status" = "404"

forged_usage_status="$(curl_local -sS -o "$response_file" -w '%{http_code}' \
  -X POST http://127.0.0.1:8080/v1/usage \
  -H 'content-type: application/json' \
  -H "x-tenant-id: $tenant_a" \
  -H "x-api-key: $key_a" \
  -d "{\"tenant_id\":\"$tenant_a\",\"period\":\"$period\",\"kind\":\"AgentRun\",\"quantity\":999}")"
test "$forged_usage_status" = "403"

stop_api
start_api

curl_local -fsS "http://127.0.0.1:8080/v1/investigations/$run_id" \
  -H "x-tenant-id: $tenant_a" \
  -H "x-api-key: $key_a" >"$response_file"
python3 -c 'import json,sys; data=json.load(open(sys.argv[1])); assert data["status"] == "Completed"; assert data["result_observation_id"] == sys.argv[2]' "$response_file" "$result_id"

curl_local -fsS "http://127.0.0.1:8080/v1/observations?tenant_id=$tenant_a&page=1&page_size=100" \
  -H "x-tenant-id: $tenant_a" \
  -H "x-api-key: $key_a" >"$response_file"
python3 -c 'import json,sys; items=json.load(open(sys.argv[1])); matches=[item for item in items if item["name"] == "investigation.inspect_failure_context"]; assert len(matches) == 1; assert matches[0]["id"] == sys.argv[2]' "$response_file" "$result_id"

curl_local -fsS "http://127.0.0.1:8080/v1/usage?tenant_id=$tenant_a&period=$period" \
  -H "x-tenant-id: $tenant_a" \
  -H "x-api-key: $key_a" >"$response_file"
python3 -c 'import json,sys; items=json.load(open(sys.argv[1])); assert sum(item["quantity"] for item in items if item["kind"] == "AgentRun") == 1' "$response_file"

echo 'Investigation restart, tenant isolation, and idempotent metering smoke passed'
