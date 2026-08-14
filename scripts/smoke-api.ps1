$ErrorActionPreference = 'Stop'

$apiUrl = if ($env:OBSERVABILITY_API_URL) { $env:OBSERVABILITY_API_URL } else { 'http://127.0.0.1:8080' }
$tenant = [guid]::NewGuid()
$observation = @{
  id = [guid]::NewGuid()
  tenant_id = $tenant
  trace_id = 'smoke-trace'
  span_id = 'smoke-span'
  kind = 'Agent'
  name = 'smoke.agent'
  status = 'Error'
  started_at_ms = 1
  duration_ms = 42
  attributes = @{ source = 'smoke' }
} | ConvertTo-Json -Depth 5

Invoke-RestMethod "$apiUrl/health" | Out-Null
$metrics = Invoke-WebRequest "$apiUrl/metrics" | Select-Object -ExpandProperty Content
if ($metrics -notmatch 'observability_api_up 1') { throw 'metrics endpoint is not healthy' }
Invoke-RestMethod "$apiUrl/v1/observations" -Method Post -ContentType 'application/json' -Body $observation | Out-Null
$query = Invoke-RestMethod "$apiUrl/v1/observations?tenant_id=$tenant&page=1&page_size=10"
if ($query.Count -lt 1) { throw 'observation query returned no rows' }

$request = @{ tenant_id = $tenant; objective = 'smoke diagnosis'; observation_ids = @(([regex]::Match($observation, '"id":\s*"([^"]+)"').Groups[1].Value)) } | ConvertTo-Json -Depth 5
$decision = Invoke-RestMethod "$apiUrl/v1/agent/plan" -Method Post -ContentType 'application/json' -Body $request
if ($decision.evidence_ids.Count -lt 1) { throw 'agent plan returned no evidence' }
Write-Output 'API smoke passed'
