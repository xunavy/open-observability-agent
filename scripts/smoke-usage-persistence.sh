#!/usr/bin/env bash
set -euo pipefail

data_file="/tmp/open-observability-usage.jsonl"
observation_file="/tmp/open-observability-observations.jsonl"
rm -f "$data_file" "$observation_file"
tenant="$(cat /proc/sys/kernel/random/uuid)"
start_api() {
  OBSERVABILITY_DATA="$observation_file" OBSERVABILITY_USAGE_DATA="$data_file" \
    cargo run -p observability-api --offline >/tmp/open-observability-persistence.log 2>&1 &
  api_pid=$!
  for _ in $(seq 1 30); do
    if curl -fsS http://127.0.0.1:8080/health >/dev/null; then return; fi
    sleep 1
  done
  return 1
}
stop_api() { kill "$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true; }

cd "$(dirname "$0")/.."
start_api
trap stop_api EXIT
curl -fsS -X POST http://127.0.0.1:8080/v1/usage \
  -H 'content-type: application/json' \
  -d "{\"tenant_id\":\"$tenant\",\"period\":\"2026-08\",\"kind\":\"Observation\",\"quantity\":7}" >/dev/null
stop_api
start_api
usage="$(curl -fsS "http://127.0.0.1:8080/v1/usage?tenant_id=$tenant&period=2026-08")"
echo "$usage" | grep -q '"quantity":7'
echo "$usage"
echo 'Usage persistence smoke passed'
