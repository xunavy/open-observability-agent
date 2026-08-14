use axum::{
    body::Body,
    extract::{Query, State},
    http::{header::HeaderValue, Request, StatusCode},
    middleware::{self, Next},
    response::Response,
    routing::{get, post},
    Json, Router,
};
use observability_core::{
    diagnose, execute_safe_tool, model_observation, plan_agent_request, quote_monthly_usage,
    AgentDecision, AgentRequest, BillingQuote, DeterministicModelProvider, Finding,
    JsonlObservationStore, JsonlUsageStore, ModelProvider, ModelRequest, Observation,
    ObservationRepository, ObservationStatus, SubscriptionPlan, TenantId, ToolExecutionRequest,
    ToolExecutionResult, UsageEntry, UsageKind, UsageLedger, UsageRepository,
};
use observability_sqlite::SqliteStore;
use serde::Deserialize;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{atomic::{AtomicU64, Ordering}, Arc, RwLock},
};
use tokio::sync::Semaphore;
use tower_http::cors::{AllowOrigin, CorsLayer};
use uuid::Uuid;

const MAX_BATCH_SIZE: usize = 1_000;

#[derive(Clone)]
struct AppState {
    observations: Arc<dyn ObservationRepository>,
    usage_store: Arc<dyn UsageRepository>,
    usage: Arc<RwLock<UsageLedger>>,
    batch_slots: Arc<Semaphore>,
    observations_ingested: Arc<AtomicU64>,
    model_calls: Arc<AtomicU64>,
    agent_executions: Arc<AtomicU64>,
}

#[derive(Deserialize)]
struct TenantQuery {
    tenant_id: Uuid,
    name: Option<String>,
    status: Option<String>,
    page: Option<usize>,
    page_size: Option<usize>,
}

#[derive(Deserialize)]
struct ObservationBatch {
    observations: Vec<Observation>,
}

fn filter_observations(mut items: Vec<Observation>, query: &TenantQuery) -> Vec<Observation> {
    items.retain(|item| {
        let tenant_matches = item.tenant_id.0 == query.tenant_id;
        let name_matches = query.name.as_ref().is_none_or(|name| item.name == *name);
        let status_matches = query.status.as_ref().is_none_or(|status| {
            matches!(
                (status.as_str(), &item.status),
                ("ok", ObservationStatus::Ok) | ("error", ObservationStatus::Error)
            )
        });
        tenant_matches && name_matches && status_matches
    });
    items.sort_by_key(|item| item.started_at_ms);
    let page = query.page.unwrap_or(1).max(1);
    let page_size = query.page_size.unwrap_or(100).clamp(1, 1000);
    let start = page.saturating_sub(1).saturating_mul(page_size);
    items.into_iter().skip(start).take(page_size).collect()
}

fn enforce_tenant(tenant_id: &Uuid) -> Result<(), (StatusCode, String)> {
    let configured = std::env::var("OBSERVABILITY_TENANT_ID").ok();
    if let Some(expected) = configured {
        let expected = Uuid::parse_str(&expected).map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "OBSERVABILITY_TENANT_ID must be a UUID".into(),
            )
        })?;
        if expected != *tenant_id {
            return Err((StatusCode::FORBIDDEN, "tenant is not authorized".into()));
        }
    }
    Ok(())
}

async fn ingest(
    State(state): State<AppState>,
    Json(observation): Json<Observation>,
) -> Result<(StatusCode, Json<Observation>), (StatusCode, String)> {
    enforce_tenant(&observation.tenant_id.0)?;
    observation
        .validate()
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    state
        .observations
        .append(&observation)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    state.observations_ingested.fetch_add(1, Ordering::Relaxed);
    Ok((StatusCode::CREATED, Json(observation)))
}

async fn ingest_batch(
    State(state): State<AppState>,
    Json(batch): Json<ObservationBatch>,
) -> Result<(StatusCode, Json<usize>), (StatusCode, String)> {
    for observation in &batch.observations {
        enforce_tenant(&observation.tenant_id.0)?;
    }
    let _slot = state.batch_slots.clone().try_acquire_owned().map_err(|_| {
        (
            StatusCode::TOO_MANY_REQUESTS,
            "batch ingestion is busy; retry later".into(),
        )
    })?;
    if batch.observations.is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "observations must not be empty".into(),
        ));
    }
    if batch.observations.len() > MAX_BATCH_SIZE {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            "batch cannot contain more than 1000 observations".into(),
        ));
    }
    for observation in &batch.observations {
        observation
            .validate()
            .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    }
    for observation in &batch.observations {
        state
            .observations
            .append(observation)
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    }
    state
        .observations_ingested
        .fetch_add(batch.observations.len() as u64, Ordering::Relaxed);
    Ok((StatusCode::CREATED, Json(batch.observations.len())))
}

async fn list(
    State(state): State<AppState>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<Vec<Observation>>, (StatusCode, String)> {
    enforce_tenant(&query.tenant_id)?;
    let items = state
        .observations
        .list()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(filter_observations(items, &query)))
}

async fn diagnostics(
    State(state): State<AppState>,
    Json(query): Json<TenantQuery>,
) -> Result<Json<Vec<Finding>>, (StatusCode, String)> {
    enforce_tenant(&query.tenant_id)?;
    let items = state
        .observations
        .list()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(diagnose(&TenantId(query.tenant_id), &items)))
}

async fn agent_plan(
    State(state): State<AppState>,
    Json(request): Json<AgentRequest>,
) -> Result<Json<AgentDecision>, (StatusCode, String)> {
    enforce_tenant(&request.tenant_id.0)?;
    if request.objective.trim().is_empty() {
        return Err((
            StatusCode::BAD_REQUEST,
            "objective must not be empty".into(),
        ));
    }
    let items = state
        .observations
        .list()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(plan_agent_request(&request, &items)))
}

async fn agent_execute(
    State(state): State<AppState>,
    Json(request): Json<ToolExecutionRequest>,
) -> Result<Json<ToolExecutionResult>, (StatusCode, String)> {
    enforce_tenant(&request.tenant_id.0)?;
    state.agent_executions.fetch_add(1, Ordering::Relaxed);
    let items = state
        .observations
        .list()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(execute_safe_tool(&request, &items)))
}

fn parse_model_response(
    body: serde_json::Value,
    model: &str,
) -> Result<observability_core::ModelResponse, String> {
    let text = body["choices"][0]["message"]["content"]
        .as_str()
        .ok_or_else(|| "provider response missing choices[0].message.content".to_string())?;
    Ok(observability_core::ModelResponse {
        model: model.to_owned(),
        text: text.to_owned(),
        input_tokens: body["usage"]["prompt_tokens"].as_u64().unwrap_or_default(),
        output_tokens: body["usage"]["completion_tokens"]
            .as_u64()
            .unwrap_or_default(),
    })
}

async fn model_complete(
    State(state): State<AppState>,
    Json(request): Json<ModelRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    enforce_tenant(&request.tenant_id.0)?;
    state.model_calls.fetch_add(1, Ordering::Relaxed);
    let response = if let Some(endpoint) = std::env::var("MODEL_PROVIDER_URL").ok() {
        let key = std::env::var("MODEL_API_KEY").map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "MODEL_API_KEY is required when MODEL_PROVIDER_URL is configured".into(),
            )
        })?;
        let client = reqwest::Client::new();
        let provider_response = client
            .post(endpoint)
            .bearer_auth(key)
            .json(&serde_json::json!({
                "model": &request.model,
                "messages": [{"role": "user", "content": &request.prompt}],
                "metadata": {"evidence_ids": &request.evidence_ids}
            }))
            .send()
            .await
            .map_err(|error| (StatusCode::BAD_GATEWAY, error.to_string()))?;
        if !provider_response.status().is_success() {
            return Err((
                StatusCode::BAD_GATEWAY,
                format!("model provider returned {}", provider_response.status()),
            ));
        }
        let body: serde_json::Value = provider_response
            .json()
            .await
            .map_err(|error| (StatusCode::BAD_GATEWAY, error.to_string()))?;
        parse_model_response(body, &request.model).map_err(|error| (StatusCode::BAD_GATEWAY, error))?
    } else {
        DeterministicModelProvider
            .complete(&request)
            .map_err(|error| (StatusCode::BAD_REQUEST, error))?
    };
    let observation = model_observation(&request, &response, &request.model, 0, 0);
    state
        .observations
        .append(&observation)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    let usage = UsageEntry {
        tenant_id: request.tenant_id,
        period: "current".into(),
        kind: UsageKind::ModelToken,
        quantity: response.input_tokens + response.output_tokens,
    };
    state
        .usage_store
        .append(&usage)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    state
        .usage
        .write()
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "usage lock poisoned".into(),
            )
        })?
        .record(usage);
    Ok(Json(
        serde_json::json!({ "response": response, "observation": observation }),
    ))
}

async fn record_usage(
    State(state): State<AppState>,
    Json(entry): Json<UsageEntry>,
) -> Result<StatusCode, (StatusCode, String)> {
    enforce_tenant(&entry.tenant_id.0)?;
    state
        .usage_store
        .append(&entry)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    state
        .usage
        .write()
        .map_err(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "usage lock poisoned".into(),
            )
        })?
        .record(entry);
    Ok(StatusCode::CREATED)
}

async fn usage(
    State(state): State<AppState>,
    Query(query): Query<UsageQuery>,
) -> Result<Json<Vec<UsageEntry>>, (StatusCode, String)> {
    enforce_tenant(&query.tenant_id)?;
    let ledger = state.usage.read().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "usage lock poisoned".into(),
        )
    })?;
    Ok(Json(
        ledger.snapshot(&TenantId(query.tenant_id), &query.period),
    ))
}

#[derive(Deserialize)]
struct UsageQuery {
    tenant_id: Uuid,
    period: String,
}

#[derive(Deserialize)]
struct BillingRequest {
    tenant_id: Uuid,
    period: String,
    plan: SubscriptionPlan,
}

async fn billing_quote(
    State(state): State<AppState>,
    Json(request): Json<BillingRequest>,
) -> Result<Json<BillingQuote>, (StatusCode, String)> {
    enforce_tenant(&request.tenant_id)?;
    let ledger = state.usage.read().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "usage lock poisoned".into(),
        )
    })?;
    Ok(Json(quote_monthly_usage(
        &ledger,
        &TenantId(request.tenant_id),
        &request.period,
        request.plan,
    )))
}

async fn health() -> Json<HashMap<&'static str, &'static str>> {
    Json(HashMap::from([("status", "ok")]))
}

async fn metrics(
    State(state): State<AppState>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    let storage = std::env::var("OBSERVABILITY_STORAGE").unwrap_or_else(|_| "jsonl".into());
    let body = format!(
        "# HELP observability_api_info Runtime configuration of the API.\n# TYPE observability_api_info gauge\nobservability_api_info{{storage=\"{storage}\"}} 1\n# HELP observability_api_up Whether the API process is serving requests.\n# TYPE observability_api_up gauge\nobservability_api_up 1\n# HELP observability_observations_ingested_total Observations accepted by the API.\n# TYPE observability_observations_ingested_total counter\nobservability_observations_ingested_total {}\n# HELP observability_model_calls_total Model completion requests accepted by the API.\n# TYPE observability_model_calls_total counter\nobservability_model_calls_total {}\n# HELP observability_agent_executions_total Agent tool executions accepted by the API.\n# TYPE observability_agent_executions_total counter\nobservability_agent_executions_total {}\n",
        state.observations_ingested.load(Ordering::Relaxed),
        state.model_calls.load(Ordering::Relaxed),
        state.agent_executions.load(Ordering::Relaxed)
    );
    ([(axum::http::header::CONTENT_TYPE, "text/plain; version=0.0.4")], body)
}

async fn api_key_guard(request: Request<Body>, next: Next) -> Result<Response, StatusCode> {
    let configured = std::env::var("OBSERVABILITY_API_KEY").ok();
    if std::env::var("OBSERVABILITY_ENV").as_deref() == Ok("production")
        && configured.as_deref().is_none_or(str::is_empty)
    {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    if let Some(expected) = configured {
        let supplied = request
            .headers()
            .get("x-api-key")
            .and_then(|value| value.to_str().ok());
        if supplied != Some(expected.as_str()) {
            return Err(StatusCode::UNAUTHORIZED);
        }
    }
    Ok(next.run(request).await)
}

fn cors_layer() -> CorsLayer {
    match std::env::var("OBSERVABILITY_CORS_ORIGINS") {
        Ok(origins) => {
            let values: Vec<HeaderValue> = origins
                .split(',')
                .filter_map(|origin| HeaderValue::from_str(origin.trim()).ok())
                .collect();
            CorsLayer::new().allow_origin(AllowOrigin::list(values))
        }
        Err(_) => CorsLayer::permissive(),
    }
}

#[tokio::main]
async fn main() {
    let data_path = std::env::var("OBSERVABILITY_DATA")
        .unwrap_or_else(|_| "data/observations.jsonl".to_string());
    let usage_path = std::env::var("OBSERVABILITY_USAGE_DATA")
        .unwrap_or_else(|_| "data/usage.jsonl".to_string());
    let storage_kind = std::env::var("OBSERVABILITY_STORAGE").unwrap_or_else(|_| "jsonl".into());
    if let Some(parent) = std::path::Path::new(&data_path).parent() {
        std::fs::create_dir_all(parent).expect("create data directory");
    }
    if let Some(parent) = std::path::Path::new(&usage_path).parent() {
        std::fs::create_dir_all(parent).expect("create usage directory");
    }
    let (observations, usage_store, usage_ledger): (
        Arc<dyn ObservationRepository>,
        Arc<dyn UsageRepository>,
        UsageLedger,
    ) = if storage_kind == "sqlite" {
        let sqlite_path = std::env::var("OBSERVABILITY_SQLITE_DATA")
            .unwrap_or_else(|_| "data/observability.sqlite".into());
        if let Some(parent) = std::path::Path::new(&sqlite_path).parent() {
            std::fs::create_dir_all(parent).expect("create sqlite directory");
        }
        let store = Arc::new(SqliteStore::open(sqlite_path).expect("open sqlite store"));
        let ledger = UsageRepository::load(store.as_ref()).expect("load sqlite usage ledger");
        (store.clone(), store, ledger)
    } else {
        let observation_store: Arc<dyn ObservationRepository> =
            Arc::new(JsonlObservationStore::new(data_path));
        let usage_store: Arc<dyn UsageRepository> = Arc::new(JsonlUsageStore::new(usage_path));
        let ledger = UsageRepository::load(usage_store.as_ref()).expect("load usage ledger");
        (observation_store, usage_store, ledger)
    };
    let app = Router::new()
        .route("/health", get(health))
        .route("/metrics", get(metrics))
        .route("/v1/observations", post(ingest).get(list))
        .route("/v1/observations/batch", post(ingest_batch))
        .route("/v1/diagnostics", post(diagnostics))
        .route("/v1/agent/plan", post(agent_plan))
        .route("/v1/agent/execute", post(agent_execute))
        .route("/v1/model/complete", post(model_complete))
        .route("/v1/usage", post(record_usage).get(usage))
        .route("/v1/billing/quote", post(billing_quote))
        .layer(middleware::from_fn(api_key_guard))
        .layer(cors_layer())
        .with_state(AppState {
            observations,
            usage_store,
            usage: Arc::new(RwLock::new(usage_ledger)),
            batch_slots: Arc::new(Semaphore::new(8)),
            observations_ingested: Arc::new(AtomicU64::new(0)),
            model_calls: Arc::new(AtomicU64::new(0)),
            agent_executions: Arc::new(AtomicU64::new(0)),
        });
    let address: SocketAddr = "0.0.0.0:8080".parse().expect("valid listen address");
    let listener = tokio::net::TcpListener::bind(address)
        .await
        .expect("bind API listener");
    println!("observability-api listening on {address}");
    axum::serve(listener, app).await.expect("serve API");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn observation(tenant_id: Uuid, started_at_ms: i64, status: ObservationStatus) -> Observation {
        Observation {
            id: Uuid::new_v4(),
            tenant_id: TenantId(tenant_id),
            trace_id: "trace".into(),
            span_id: started_at_ms.to_string(),
            kind: observability_core::ObservationKind::Agent,
            name: "agent.run".into(),
            status,
            started_at_ms,
            duration_ms: 1,
            attributes: BTreeMap::new(),
        }
    }

    #[test]
    fn filtering_is_tenant_scoped_and_paginated() {
        let tenant = Uuid::new_v4();
        let query = TenantQuery {
            tenant_id: tenant,
            name: Some("agent.run".into()),
            status: Some("error".into()),
            page: Some(1),
            page_size: Some(1),
        };
        let items = vec![
            observation(tenant, 2, ObservationStatus::Error),
            observation(tenant, 1, ObservationStatus::Error),
            observation(Uuid::new_v4(), 0, ObservationStatus::Error),
            observation(tenant, 3, ObservationStatus::Ok),
        ];
        let result = filter_observations(items, &query);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].started_at_ms, 1);
    }

    #[test]
    fn batch_limit_is_explicit() {
        assert_eq!(MAX_BATCH_SIZE, 1_000);
        assert!(MAX_BATCH_SIZE < 10_000);
    }

    #[test]
    fn parses_openai_compatible_model_response_and_usage() {
        let response = parse_model_response(
            serde_json::json!({
                "choices": [{"message": {"content": "root cause found"}}],
                "usage": {"prompt_tokens": 12, "completion_tokens": 7}
            }),
            "example-model",
        )
        .unwrap();
        assert_eq!(response.model, "example-model");
        assert_eq!(response.text, "root cause found");
        assert_eq!(response.input_tokens, 12);
        assert_eq!(response.output_tokens, 7);
    }

    #[test]
    fn rejects_model_response_without_message_content() {
        let error = parse_model_response(serde_json::json!({"choices": []}), "example-model")
            .unwrap_err();
        assert!(error.contains("message.content"));
    }
}
