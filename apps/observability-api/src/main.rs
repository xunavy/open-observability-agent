use axum::{
    body::Body,
    extract::{Extension, Query, State},
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
    ObservationError, ObservationQueueItem, ObservationQueueRepository, ObservationQueueStats,
    ObservationRepository, ObservationStatus, QueueDisposition, SubscriptionPlan, TenantId,
    ToolExecutionRequest, ToolExecutionResult, UsageEntry, UsageKind, UsageLedger, UsageRepository,
};
use observability_sqlite::SqliteStore;
use serde::Deserialize;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    time::{SystemTime, UNIX_EPOCH},
};
use tokio::sync::Semaphore;
use tower_http::cors::{AllowOrigin, CorsLayer};
use uuid::Uuid;

const MAX_BATCH_SIZE: usize = 1_000;

type StorageRuntime = (
    Arc<dyn ObservationRepository>,
    Arc<dyn UsageRepository>,
    UsageLedger,
    Option<Arc<dyn ObservationQueueRepository>>,
);

#[derive(Clone)]
struct QueueRuntime {
    repository: Arc<dyn ObservationQueueRepository>,
    max_attempts: u32,
    poll_interval_ms: u64,
    lease_ms: i64,
}

#[derive(Clone, Default)]
struct AuthorizedTenant(Option<Uuid>);

#[derive(Clone)]
struct AppState {
    observations: Arc<dyn ObservationRepository>,
    queue: Option<QueueRuntime>,
    usage_store: Arc<dyn UsageRepository>,
    usage: Arc<RwLock<UsageLedger>>,
    batch_slots: Arc<Semaphore>,
    observations_ingested: Arc<AtomicU64>,
    model_calls: Arc<AtomicU64>,
    agent_executions: Arc<AtomicU64>,
    queue_processed: Arc<AtomicU64>,
    queue_retries: Arc<AtomicU64>,
    queue_dead_letters: Arc<AtomicU64>,
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

#[derive(Deserialize)]
struct QueueQuery {
    tenant_id: Uuid,
    limit: Option<usize>,
}

#[derive(Deserialize)]
struct DeadLetterReplayRequest {
    tenant_id: Uuid,
    observation_id: Uuid,
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn retry_delay_ms(attempts: u32) -> i64 {
    let exponent = attempts.saturating_sub(1).min(8);
    (250_i64 * 2_i64.pow(exponent)).min(60_000)
}

fn accept_observations(
    state: &AppState,
    observations: &[Observation],
) -> Result<StatusCode, (StatusCode, String)> {
    if let Some(queue) = &state.queue {
        let inserted = queue
            .repository
            .enqueue_batch(observations, now_ms())
            .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        state
            .observations_ingested
            .fetch_add(inserted as u64, Ordering::Relaxed);
        Ok(StatusCode::ACCEPTED)
    } else {
        for observation in observations {
            state
                .observations
                .append(observation)
                .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
        }
        state
            .observations_ingested
            .fetch_add(observations.len() as u64, Ordering::Relaxed);
        Ok(StatusCode::CREATED)
    }
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

fn enforce_tenant_for(
    authorized: &AuthorizedTenant,
    tenant_id: &Uuid,
) -> Result<(), (StatusCode, String)> {
    enforce_tenant(tenant_id)?;
    if authorized.0.is_some_and(|expected| expected != *tenant_id) {
        return Err((StatusCode::FORBIDDEN, "tenant is not authorized".into()));
    }
    Ok(())
}

async fn ingest(
    State(state): State<AppState>,
    Extension(authorized): Extension<AuthorizedTenant>,
    Json(observation): Json<Observation>,
) -> Result<(StatusCode, Json<Observation>), (StatusCode, String)> {
    enforce_tenant_for(&authorized, &observation.tenant_id.0)?;
    observation
        .validate()
        .map_err(|e| (StatusCode::BAD_REQUEST, e.to_string()))?;
    let status = accept_observations(&state, std::slice::from_ref(&observation))?;
    Ok((status, Json(observation)))
}

async fn ingest_batch(
    State(state): State<AppState>,
    Extension(authorized): Extension<AuthorizedTenant>,
    Json(batch): Json<ObservationBatch>,
) -> Result<(StatusCode, Json<usize>), (StatusCode, String)> {
    for observation in &batch.observations {
        enforce_tenant_for(&authorized, &observation.tenant_id.0)?;
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
    let status = accept_observations(&state, &batch.observations)?;
    Ok((status, Json(batch.observations.len())))
}

async fn list(
    State(state): State<AppState>,
    Extension(authorized): Extension<AuthorizedTenant>,
    Query(query): Query<TenantQuery>,
) -> Result<Json<Vec<Observation>>, (StatusCode, String)> {
    enforce_tenant_for(&authorized, &query.tenant_id)?;
    let items = state
        .observations
        .list()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(filter_observations(items, &query)))
}

async fn diagnostics(
    State(state): State<AppState>,
    Extension(authorized): Extension<AuthorizedTenant>,
    Json(query): Json<TenantQuery>,
) -> Result<Json<Vec<Finding>>, (StatusCode, String)> {
    enforce_tenant_for(&authorized, &query.tenant_id)?;
    let items = state
        .observations
        .list()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()))?;
    Ok(Json(diagnose(&TenantId(query.tenant_id), &items)))
}

async fn agent_plan(
    State(state): State<AppState>,
    Extension(authorized): Extension<AuthorizedTenant>,
    Json(request): Json<AgentRequest>,
) -> Result<Json<AgentDecision>, (StatusCode, String)> {
    enforce_tenant_for(&authorized, &request.tenant_id.0)?;
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
    Extension(authorized): Extension<AuthorizedTenant>,
    Json(request): Json<ToolExecutionRequest>,
) -> Result<Json<ToolExecutionResult>, (StatusCode, String)> {
    enforce_tenant_for(&authorized, &request.tenant_id.0)?;
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
    Extension(authorized): Extension<AuthorizedTenant>,
    Json(request): Json<ModelRequest>,
) -> Result<Json<serde_json::Value>, (StatusCode, String)> {
    enforce_tenant_for(&authorized, &request.tenant_id.0)?;
    state.model_calls.fetch_add(1, Ordering::Relaxed);
    let response = if let Ok(endpoint) = std::env::var("MODEL_PROVIDER_URL") {
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
        parse_model_response(body, &request.model)
            .map_err(|error| (StatusCode::BAD_GATEWAY, error))?
    } else {
        DeterministicModelProvider
            .complete(&request)
            .map_err(|error| (StatusCode::BAD_REQUEST, error))?
    };
    let observation = model_observation(&request, &response, &request.model, 0, 0);
    accept_observations(&state, std::slice::from_ref(&observation))?;
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
    Extension(authorized): Extension<AuthorizedTenant>,
    Json(entry): Json<UsageEntry>,
) -> Result<StatusCode, (StatusCode, String)> {
    enforce_tenant_for(&authorized, &entry.tenant_id.0)?;
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
    Extension(authorized): Extension<AuthorizedTenant>,
    Query(query): Query<UsageQuery>,
) -> Result<Json<Vec<UsageEntry>>, (StatusCode, String)> {
    enforce_tenant_for(&authorized, &query.tenant_id)?;
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
    Extension(authorized): Extension<AuthorizedTenant>,
    Json(request): Json<BillingRequest>,
) -> Result<Json<BillingQuote>, (StatusCode, String)> {
    enforce_tenant_for(&authorized, &request.tenant_id)?;
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

fn process_queue_once(state: &AppState) -> Result<bool, ObservationError> {
    let Some(queue) = &state.queue else {
        return Ok(false);
    };
    let claimed_at = now_ms();
    let Some(item) = queue
        .repository
        .claim_next(claimed_at, claimed_at.saturating_add(queue.lease_ms))?
    else {
        return Ok(false);
    };
    match state.observations.append(&item.observation) {
        Ok(()) => {
            queue.repository.complete(&item.id)?;
            state.queue_processed.fetch_add(1, Ordering::Relaxed);
        }
        Err(error) => {
            let retry_at_ms = now_ms().saturating_add(retry_delay_ms(item.attempts));
            match queue.repository.fail(
                &item.id,
                &error.to_string(),
                retry_at_ms,
                queue.max_attempts,
            )? {
                QueueDisposition::Pending => {
                    state.queue_retries.fetch_add(1, Ordering::Relaxed);
                }
                QueueDisposition::DeadLetter => {
                    state.queue_dead_letters.fetch_add(1, Ordering::Relaxed);
                }
            }
        }
    }
    Ok(true)
}

async fn run_queue_worker(state: AppState) {
    let poll_interval_ms = state
        .queue
        .as_ref()
        .map(|queue| queue.poll_interval_ms)
        .unwrap_or(250);
    loop {
        tokio::time::sleep(std::time::Duration::from_millis(poll_interval_ms)).await;
        loop {
            match process_queue_once(&state) {
                Ok(true) => continue,
                Ok(false) => break,
                Err(error) => {
                    eprintln!("observation queue worker error: {error}");
                    break;
                }
            }
        }
    }
}

fn configured_queue(
    state: &AppState,
) -> Result<&Arc<dyn ObservationQueueRepository>, (StatusCode, String)> {
    state
        .queue
        .as_ref()
        .map(|queue| &queue.repository)
        .ok_or_else(|| {
            (
                StatusCode::CONFLICT,
                "durable ingestion queue is not enabled".into(),
            )
        })
}

async fn queue_stats(
    State(state): State<AppState>,
    Extension(authorized): Extension<AuthorizedTenant>,
    Query(query): Query<QueueQuery>,
) -> Result<Json<ObservationQueueStats>, (StatusCode, String)> {
    enforce_tenant_for(&authorized, &query.tenant_id)?;
    let stats = configured_queue(&state)?
        .stats(&TenantId(query.tenant_id))
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(stats))
}

async fn dead_letters(
    State(state): State<AppState>,
    Extension(authorized): Extension<AuthorizedTenant>,
    Query(query): Query<QueueQuery>,
) -> Result<Json<Vec<ObservationQueueItem>>, (StatusCode, String)> {
    enforce_tenant_for(&authorized, &query.tenant_id)?;
    let items = configured_queue(&state)?
        .dead_letters(&TenantId(query.tenant_id), query.limit.unwrap_or(100))
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    Ok(Json(items))
}

async fn replay_dead_letter(
    State(state): State<AppState>,
    Extension(authorized): Extension<AuthorizedTenant>,
    Json(request): Json<DeadLetterReplayRequest>,
) -> Result<StatusCode, (StatusCode, String)> {
    enforce_tenant_for(&authorized, &request.tenant_id)?;
    let replayed = configured_queue(&state)?
        .requeue_dead_letter(
            &TenantId(request.tenant_id),
            &request.observation_id,
            now_ms(),
        )
        .map_err(|error| (StatusCode::INTERNAL_SERVER_ERROR, error.to_string()))?;
    if replayed {
        Ok(StatusCode::ACCEPTED)
    } else {
        Err((StatusCode::NOT_FOUND, "dead letter was not found".into()))
    }
}

async fn health() -> Json<HashMap<&'static str, &'static str>> {
    Json(HashMap::from([("status", "ok")]))
}

async fn metrics(
    State(state): State<AppState>,
) -> ([(axum::http::header::HeaderName, &'static str); 1], String) {
    let storage = std::env::var("OBSERVABILITY_STORAGE").unwrap_or_else(|_| "jsonl".into());
    let ingestion = std::env::var("OBSERVABILITY_INGEST_MODE").unwrap_or_else(|_| "direct".into());
    let body = format!(
        "# HELP observability_api_info Runtime configuration of the API.\n# TYPE observability_api_info gauge\nobservability_api_info{{storage=\"{storage}\",ingestion=\"{ingestion}\"}} 1\n# HELP observability_api_up Whether the API process is serving requests.\n# TYPE observability_api_up gauge\nobservability_api_up 1\n# HELP observability_observations_ingested_total Observations accepted by the API.\n# TYPE observability_observations_ingested_total counter\nobservability_observations_ingested_total {}\n# HELP observability_model_calls_total Model completion requests accepted by the API.\n# TYPE observability_model_calls_total counter\nobservability_model_calls_total {}\n# HELP observability_agent_executions_total Agent tool executions accepted by the API.\n# TYPE observability_agent_executions_total counter\nobservability_agent_executions_total {}\n# HELP observability_queue_processed_total Durable queue items persisted to observation storage.\n# TYPE observability_queue_processed_total counter\nobservability_queue_processed_total {}\n# HELP observability_queue_retries_total Durable queue items scheduled for retry.\n# TYPE observability_queue_retries_total counter\nobservability_queue_retries_total {}\n# HELP observability_queue_dead_letters_total Durable queue items moved to dead letter.\n# TYPE observability_queue_dead_letters_total counter\nobservability_queue_dead_letters_total {}\n",
        state.observations_ingested.load(Ordering::Relaxed),
        state.model_calls.load(Ordering::Relaxed),
        state.agent_executions.load(Ordering::Relaxed),
        state.queue_processed.load(Ordering::Relaxed),
        state.queue_retries.load(Ordering::Relaxed),
        state.queue_dead_letters.load(Ordering::Relaxed)
    );
    (
        [(
            axum::http::header::CONTENT_TYPE,
            "text/plain; version=0.0.4",
        )],
        body,
    )
}

async fn api_key_guard(request: Request<Body>, next: Next) -> Result<Response, StatusCode> {
    let mut request = request;
    if request.uri().path() == "/health" {
        return Ok(next.run(request).await);
    }
    let mapping = std::env::var("OBSERVABILITY_API_KEYS")
        .ok()
        .filter(|value| !value.trim().is_empty());
    let configured = std::env::var("OBSERVABILITY_API_KEY")
        .ok()
        .filter(|value| !value.is_empty());
    if std::env::var("OBSERVABILITY_ENV").as_deref() == Ok("production")
        && mapping.is_none()
        && configured.is_none()
    {
        return Err(StatusCode::INTERNAL_SERVER_ERROR);
    }
    let authorized_tenant = if let Some(mapping) = mapping {
        let supplied_key = request
            .headers()
            .get("x-api-key")
            .and_then(|value| value.to_str().ok());
        let supplied_tenant = request
            .headers()
            .get("x-tenant-id")
            .and_then(|value| value.to_str().ok())
            .and_then(|value| Uuid::parse_str(value).ok());
        let matched = mapping.split(',').find_map(|entry| {
            let (tenant, secret) = entry.split_once('=')?;
            let tenant = Uuid::parse_str(tenant.trim()).ok()?;
            (supplied_key == Some(secret.trim()) && supplied_tenant == Some(tenant))
                .then_some(tenant)
        });
        let Some(tenant) = matched else {
            return Err(StatusCode::UNAUTHORIZED);
        };
        Some(tenant)
    } else {
        None
    };
    request
        .extensions_mut()
        .insert(AuthorizedTenant(authorized_tenant));
    if authorized_tenant.is_none() {
        if let Some(expected) = configured {
            let supplied = request
                .headers()
                .get("x-api-key")
                .and_then(|value| value.to_str().ok());
            if supplied != Some(expected.as_str()) {
                return Err(StatusCode::UNAUTHORIZED);
            }
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
    let ingest_mode =
        std::env::var("OBSERVABILITY_INGEST_MODE").unwrap_or_else(|_| "direct".into());
    if !matches!(ingest_mode.as_str(), "direct" | "durable") {
        panic!("OBSERVABILITY_INGEST_MODE must be direct or durable");
    }
    if ingest_mode == "durable" && storage_kind != "sqlite" {
        panic!("durable ingestion requires OBSERVABILITY_STORAGE=sqlite");
    }
    if let Some(parent) = std::path::Path::new(&data_path).parent() {
        std::fs::create_dir_all(parent).expect("create data directory");
    }
    if let Some(parent) = std::path::Path::new(&usage_path).parent() {
        std::fs::create_dir_all(parent).expect("create usage directory");
    }
    let (observations, usage_store, usage_ledger, queue_repository): StorageRuntime =
        if storage_kind == "sqlite" {
            let sqlite_path = std::env::var("OBSERVABILITY_SQLITE_DATA")
                .unwrap_or_else(|_| "data/observability.sqlite".into());
            if let Some(parent) = std::path::Path::new(&sqlite_path).parent() {
                std::fs::create_dir_all(parent).expect("create sqlite directory");
            }
            let store = Arc::new(SqliteStore::open(sqlite_path).expect("open sqlite store"));
            let ledger = UsageRepository::load(store.as_ref()).expect("load sqlite usage ledger");
            let queue: Option<Arc<dyn ObservationQueueRepository>> = (ingest_mode == "durable")
                .then(|| store.clone() as Arc<dyn ObservationQueueRepository>);
            (store.clone(), store, ledger, queue)
        } else {
            let observation_store: Arc<dyn ObservationRepository> =
                Arc::new(JsonlObservationStore::new(data_path));
            let usage_store: Arc<dyn UsageRepository> = Arc::new(JsonlUsageStore::new(usage_path));
            let ledger = UsageRepository::load(usage_store.as_ref()).expect("load usage ledger");
            (observation_store, usage_store, ledger, None)
        };
    let queue = queue_repository.map(|repository| QueueRuntime {
        repository,
        max_attempts: std::env::var("OBSERVABILITY_QUEUE_MAX_ATTEMPTS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(5)
            .max(1),
        poll_interval_ms: std::env::var("OBSERVABILITY_QUEUE_POLL_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(250)
            .max(10),
        lease_ms: std::env::var("OBSERVABILITY_QUEUE_LEASE_MS")
            .ok()
            .and_then(|value| value.parse().ok())
            .unwrap_or(30_000)
            .max(1_000),
    });
    let state = AppState {
        observations,
        queue,
        usage_store,
        usage: Arc::new(RwLock::new(usage_ledger)),
        batch_slots: Arc::new(Semaphore::new(8)),
        observations_ingested: Arc::new(AtomicU64::new(0)),
        model_calls: Arc::new(AtomicU64::new(0)),
        agent_executions: Arc::new(AtomicU64::new(0)),
        queue_processed: Arc::new(AtomicU64::new(0)),
        queue_retries: Arc::new(AtomicU64::new(0)),
        queue_dead_letters: Arc::new(AtomicU64::new(0)),
    };
    if state.queue.is_some() {
        tokio::spawn(run_queue_worker(state.clone()));
    }
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
        .route("/v1/ingestion/queue", get(queue_stats))
        .route("/v1/ingestion/dead-letters", get(dead_letters))
        .route(
            "/v1/ingestion/dead-letters/replay",
            post(replay_dead_letter),
        )
        .layer(middleware::from_fn(api_key_guard))
        .layer(cors_layer())
        .with_state(state);
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
        let error =
            parse_model_response(serde_json::json!({"choices": []}), "example-model").unwrap_err();
        assert!(error.contains("message.content"));
    }

    #[test]
    fn tenant_scoped_key_cannot_use_another_tenant() {
        let authorized = Uuid::new_v4();
        let other = Uuid::new_v4();
        let scope = AuthorizedTenant(Some(authorized));
        assert!(enforce_tenant_for(&scope, &authorized).is_ok());
        assert_eq!(
            enforce_tenant_for(&scope, &other).unwrap_err().0,
            StatusCode::FORBIDDEN
        );
    }

    #[test]
    fn retry_backoff_is_bounded() {
        assert_eq!(retry_delay_ms(1), 250);
        assert_eq!(retry_delay_ms(2), 500);
        assert_eq!(retry_delay_ms(20), 60_000);
    }

    #[test]
    fn queue_worker_persists_and_acknowledges_observation() {
        let store = Arc::new(SqliteStore::open(":memory:").unwrap());
        let tenant = Uuid::new_v4();
        let observation = observation(tenant, 1, ObservationStatus::Ok);
        ObservationQueueRepository::enqueue_batch(
            store.as_ref(),
            std::slice::from_ref(&observation),
            100,
        )
        .unwrap();
        let queue_repository: Arc<dyn ObservationQueueRepository> = store.clone();
        let observation_repository: Arc<dyn ObservationRepository> = store.clone();
        let usage_repository: Arc<dyn UsageRepository> = store.clone();
        let state = AppState {
            observations: observation_repository,
            queue: Some(QueueRuntime {
                repository: queue_repository,
                max_attempts: 3,
                poll_interval_ms: 10,
                lease_ms: 1_000,
            }),
            usage_store: usage_repository,
            usage: Arc::new(RwLock::new(UsageLedger::default())),
            batch_slots: Arc::new(Semaphore::new(1)),
            observations_ingested: Arc::new(AtomicU64::new(0)),
            model_calls: Arc::new(AtomicU64::new(0)),
            agent_executions: Arc::new(AtomicU64::new(0)),
            queue_processed: Arc::new(AtomicU64::new(0)),
            queue_retries: Arc::new(AtomicU64::new(0)),
            queue_dead_letters: Arc::new(AtomicU64::new(0)),
        };

        assert!(process_queue_once(&state).unwrap());
        assert_eq!(
            ObservationRepository::list(store.as_ref()).unwrap(),
            vec![observation]
        );
        assert_eq!(
            ObservationQueueRepository::stats(store.as_ref(), &TenantId(tenant)).unwrap(),
            ObservationQueueStats::default()
        );
        assert_eq!(state.queue_processed.load(Ordering::Relaxed), 1);
    }
}
