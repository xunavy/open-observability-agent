#!/usr/bin/env bash
set -euo pipefail

data_file="${OBSERVABILITY_DATA:-/tmp/open-observability-smoke.jsonl}"
rm -f "$data_file"
OBSERVABILITY_DATA="$data_file" cargo run -p observability-api --offline >/tmp/open-observability-api.log 2>&1 &
api_pid=$!
cleanup() { kill "$api_pid" 2>/dev/null || true; wait "$api_pid" 2>/dev/null || true; }
trap cleanup EXIT

for _ in $(seq 1 30); do
  if curl -fsS http://127.0.0.1:8080/health >/tmp/open-observability-health.json; then
    break
  fi
  sleep 1
done
test -s /tmp/open-observability-health.json
grep -q '"ok"' /tmp/open-observability-health.json
cat /tmp/open-observability-health.json
model_response="$(curl -fsS -X POST http://127.0.0.1:8080/v1/model/complete \
  -H 'content-type: application/json' \
  -d '{"tenant_id":"00000000-0000-4000-8000-000000000010","model":"local-deterministic","prompt":"smoke model","evidence_ids":[]}')"
echo "$model_response" | grep -q 'model.complete'
echo "$model_response" | grep -q 'input_tokens'
echo "API HTTP smoke passed"
