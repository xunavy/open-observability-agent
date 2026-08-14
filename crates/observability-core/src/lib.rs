use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::collections::VecDeque;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use thiserror::Error;
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub struct TenantId(pub Uuid);

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Observation {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub trace_id: String,
    pub span_id: String,
    pub kind: ObservationKind,
    pub name: String,
    pub status: ObservationStatus,
    pub started_at_ms: i64,
    pub duration_ms: u64,
    pub attributes: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObservationKind {
    Agent,
    Tool,
    Model,
    Workflow,
    Http,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ObservationStatus {
    Ok,
    Error,
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum ObservationError {
    #[error("observation name must not be empty")]
    EmptyName,
    #[error("duration must be within the ingestion limit")]
    DurationTooLarge,
    #[error("observation storage error: {0}")]
    Storage(String),
}

#[derive(Debug, Clone)]
pub struct JsonlObservationStore {
    path: PathBuf,
}

pub trait ObservationRepository: Send + Sync {
    fn append(&self, observation: &Observation) -> Result<(), ObservationError>;
    fn list(&self) -> Result<Vec<Observation>, ObservationError>;
}

#[derive(Debug, Clone)]
pub struct BoundedObservationQueue {
    capacity: usize,
    items: VecDeque<Observation>,
}

impl BoundedObservationQueue {
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "queue capacity must be positive");
        Self {
            capacity,
            items: VecDeque::with_capacity(capacity),
        }
    }

    pub fn try_push(&mut self, observation: Observation) -> Result<(), Observation> {
        if self.items.len() >= self.capacity {
            return Err(observation);
        }
        self.items.push_back(observation);
        Ok(())
    }

    pub fn pop(&mut self) -> Option<Observation> {
        self.items.pop_front()
    }
    pub fn len(&self) -> usize {
        self.items.len()
    }
    pub fn is_empty(&self) -> bool {
        self.items.is_empty()
    }
    pub fn capacity(&self) -> usize {
        self.capacity
    }
}

impl JsonlObservationStore {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn append(&self, observation: &Observation) -> Result<(), ObservationError> {
        observation.validate()?;
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| ObservationError::Storage(e.to_string()))?;
        serde_json::to_writer(&mut file, observation)
            .map_err(|e| ObservationError::Storage(e.to_string()))?;
        file.write_all(b"\n")
            .map_err(|e| ObservationError::Storage(e.to_string()))
    }

    pub fn list(&self) -> Result<Vec<Observation>, ObservationError> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(Vec::new()),
            Err(error) => return Err(ObservationError::Storage(error.to_string())),
        };
        BufReader::new(file)
            .lines()
            .map(|line| {
                let line = line.map_err(|e| ObservationError::Storage(e.to_string()))?;
                serde_json::from_str(&line).map_err(|e| ObservationError::Storage(e.to_string()))
            })
            .collect()
    }
}

impl ObservationRepository for JsonlObservationStore {
    fn append(&self, observation: &Observation) -> Result<(), ObservationError> {
        Self::append(self, observation)
    }

    fn list(&self) -> Result<Vec<Observation>, ObservationError> {
        Self::list(self)
    }
}

impl Observation {
    pub fn validate(&self) -> Result<(), ObservationError> {
        if self.name.trim().is_empty() {
            return Err(ObservationError::EmptyName);
        }
        if self.duration_ms > 86_400_000 {
            return Err(ObservationError::DurationTooLarge);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRun {
    pub id: Uuid,
    pub tenant_id: TenantId,
    pub agent_name: String,
    pub objective: String,
    pub observation_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolCall {
    pub id: Uuid,
    pub agent_run_id: Uuid,
    pub tenant_id: TenantId,
    pub tool_name: String,
    pub input_hash: String,
    pub success: bool,
    pub duration_ms: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentRequest {
    pub tenant_id: TenantId,
    pub objective: String,
    pub observation_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentDecision {
    pub tenant_id: TenantId,
    pub summary: String,
    pub actions: Vec<AgentAction>,
    pub evidence_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelRequest {
    pub tenant_id: TenantId,
    pub model: String,
    pub prompt: String,
    pub evidence_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ModelResponse {
    pub model: String,
    pub text: String,
    pub input_tokens: u64,
    pub output_tokens: u64,
}

pub fn model_observation(
    request: &ModelRequest,
    response: &ModelResponse,
    trace_id: impl Into<String>,
    started_at_ms: i64,
    duration_ms: u64,
) -> Observation {
    let mut attributes = BTreeMap::new();
    attributes.insert("model".into(), response.model.clone());
    attributes.insert("input_tokens".into(), response.input_tokens.to_string());
    attributes.insert("output_tokens".into(), response.output_tokens.to_string());
    attributes.insert(
        "evidence_count".into(),
        request.evidence_ids.len().to_string(),
    );
    Observation {
        id: Uuid::new_v4(),
        tenant_id: request.tenant_id.clone(),
        trace_id: trace_id.into(),
        span_id: Uuid::new_v4().to_string(),
        kind: ObservationKind::Model,
        name: "model.complete".into(),
        status: ObservationStatus::Ok,
        started_at_ms,
        duration_ms,
        attributes,
    }
}

pub trait ModelProvider: Send + Sync {
    fn complete(&self, request: &ModelRequest) -> Result<ModelResponse, String>;
}

#[derive(Debug, Default, Clone, Copy)]
pub struct DeterministicModelProvider;

impl ModelProvider for DeterministicModelProvider {
    fn complete(&self, request: &ModelRequest) -> Result<ModelResponse, String> {
        if request.prompt.trim().is_empty() {
            return Err("prompt must not be empty".into());
        }
        Ok(ModelResponse {
            model: request.model.clone(),
            text: format!(
                "基于 {} 条证据生成确定性摘要：{}",
                request.evidence_ids.len(),
                request.prompt
            ),
            input_tokens: request.prompt.len() as u64,
            output_tokens: request.evidence_ids.len() as u64 + 8,
        })
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct AgentAction {
    pub tool_name: String,
    pub reason: String,
    pub requires_approval: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolExecutionRequest {
    pub tenant_id: TenantId,
    pub tool_name: String,
    pub evidence_ids: Vec<Uuid>,
    pub approved: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct ToolExecutionResult {
    pub tool_name: String,
    pub status: String,
    pub output: String,
    pub evidence_ids: Vec<Uuid>,
}

pub fn execute_safe_tool(
    request: &ToolExecutionRequest,
    observations: &[Observation],
) -> ToolExecutionResult {
    let known = matches!(
        request.tool_name.as_str(),
        "inspect_failure_context" | "summarize_trace"
    );
    let evidence_count = observations
        .iter()
        .filter(|observation| {
            observation.tenant_id == request.tenant_id
                && request.evidence_ids.contains(&observation.id)
        })
        .count();
    if !known {
        return ToolExecutionResult {
            tool_name: request.tool_name.clone(),
            status: "rejected".into(),
            output: "tool is not in the safe allowlist".into(),
            evidence_ids: Vec::new(),
        };
    }
    if !request.approved {
        return ToolExecutionResult {
            tool_name: request.tool_name.clone(),
            status: "approval_required".into(),
            output: "execution requires explicit approval".into(),
            evidence_ids: Vec::new(),
        };
    }
    ToolExecutionResult {
        tool_name: request.tool_name.clone(),
        status: "completed".into(),
        output: format!(
            "{} 已基于 {} 条租户范围证据完成",
            request.tool_name, evidence_count
        ),
        evidence_ids: request.evidence_ids.clone(),
    }
}

pub fn plan_agent_request(request: &AgentRequest, observations: &[Observation]) -> AgentDecision {
    let evidence: Vec<&Observation> = observations
        .iter()
        .filter(|observation| {
            observation.tenant_id == request.tenant_id
                && request.observation_ids.contains(&observation.id)
        })
        .collect();
    let failed = evidence
        .iter()
        .filter(|observation| observation.status == ObservationStatus::Error)
        .count();
    let actions = if failed > 0 {
        vec![AgentAction {
            tool_name: "inspect_failure_context".into(),
            reason: format!("基于 {} 个失败观测检查上下文", failed),
            requires_approval: false,
        }]
    } else {
        vec![AgentAction {
            tool_name: "summarize_trace".into(),
            reason: "观测未显示失败，先生成执行摘要".into(),
            requires_approval: false,
        }]
    };
    AgentDecision {
        tenant_id: request.tenant_id.clone(),
        summary: format!(
            "已根据目标“{}”分析 {} 条观测",
            request.objective,
            evidence.len()
        ),
        actions,
        evidence_ids: evidence
            .into_iter()
            .map(|observation| observation.id)
            .collect(),
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, PartialOrd, Ord)]
pub enum UsageKind {
    Observation,
    AgentRun,
    ToolCall,
    ModelToken,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct UsageEntry {
    pub tenant_id: TenantId,
    pub period: String,
    pub kind: UsageKind,
    pub quantity: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum SubscriptionPlan {
    Starter,
    Growth,
    Scale,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BillingQuote {
    pub plan: SubscriptionPlan,
    pub period: String,
    pub included_observations: u64,
    pub used_observations: u64,
    pub base_price_cents: u64,
    pub overage_cents: u64,
    pub total_cents: u64,
}

pub fn quote_monthly_usage(
    ledger: &UsageLedger,
    tenant_id: &TenantId,
    period: &str,
    plan: SubscriptionPlan,
) -> BillingQuote {
    let (included, base, unit) = match plan {
        SubscriptionPlan::Starter => (100_000, 1_900, 3),
        SubscriptionPlan::Growth => (1_000_000, 9_900, 2),
        SubscriptionPlan::Scale => (10_000_000, 49_900, 1),
    };
    let used = ledger.total(tenant_id, period, &UsageKind::Observation);
    let overage_units = used.saturating_sub(included).div_ceil(1_000);
    let overage_cents = overage_units * unit;
    BillingQuote {
        plan,
        period: period.to_owned(),
        included_observations: included,
        used_observations: used,
        base_price_cents: base,
        overage_cents,
        total_cents: base + overage_cents,
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
pub struct UsageLedger {
    entries: BTreeMap<(TenantId, String, UsageKind), u64>,
}

#[derive(Debug, Clone)]
pub struct JsonlUsageStore {
    path: PathBuf,
}

pub trait UsageRepository: Send + Sync {
    fn append(&self, entry: &UsageEntry) -> Result<(), ObservationError>;
    fn load(&self) -> Result<UsageLedger, ObservationError>;
}

impl JsonlUsageStore {
    pub fn new(path: impl AsRef<Path>) -> Self {
        Self {
            path: path.as_ref().to_path_buf(),
        }
    }

    pub fn append(&self, entry: &UsageEntry) -> Result<(), ObservationError> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.path)
            .map_err(|e| ObservationError::Storage(e.to_string()))?;
        serde_json::to_writer(&mut file, entry)
            .map_err(|e| ObservationError::Storage(e.to_string()))?;
        file.write_all(b"\n")
            .map_err(|e| ObservationError::Storage(e.to_string()))
    }

    pub fn load(&self) -> Result<UsageLedger, ObservationError> {
        let file = match File::open(&self.path) {
            Ok(file) => file,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                return Ok(UsageLedger::default())
            }
            Err(error) => return Err(ObservationError::Storage(error.to_string())),
        };
        let mut ledger = UsageLedger::default();
        for line in BufReader::new(file).lines() {
            let line = line.map_err(|e| ObservationError::Storage(e.to_string()))?;
            let entry: UsageEntry = serde_json::from_str(&line)
                .map_err(|e| ObservationError::Storage(e.to_string()))?;
            ledger.record(entry);
        }
        Ok(ledger)
    }
}

impl UsageRepository for JsonlUsageStore {
    fn append(&self, entry: &UsageEntry) -> Result<(), ObservationError> {
        Self::append(self, entry)
    }

    fn load(&self) -> Result<UsageLedger, ObservationError> {
        Self::load(self)
    }
}

impl UsageLedger {
    pub fn record(&mut self, entry: UsageEntry) {
        *self
            .entries
            .entry((entry.tenant_id, entry.period, entry.kind))
            .or_default() += entry.quantity;
    }

    pub fn total(&self, tenant_id: &TenantId, period: &str, kind: &UsageKind) -> u64 {
        self.entries
            .get(&(tenant_id.clone(), period.to_owned(), kind.clone()))
            .copied()
            .unwrap_or_default()
    }

    pub fn snapshot(&self, tenant_id: &TenantId, period: &str) -> Vec<UsageEntry> {
        self.entries
            .iter()
            .filter(|((tenant, month, _), _)| tenant == tenant_id && month == period)
            .map(|((tenant, month, kind), quantity)| UsageEntry {
                tenant_id: tenant.clone(),
                period: month.clone(),
                kind: kind.clone(),
                quantity: *quantity,
            })
            .collect()
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Finding {
    pub tenant_id: TenantId,
    pub severity: FindingSeverity,
    pub title: String,
    pub evidence_ids: Vec<Uuid>,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum FindingSeverity {
    Info,
    Warning,
    Critical,
}

pub fn diagnose(tenant_id: &TenantId, observations: &[Observation]) -> Vec<Finding> {
    let errors: Vec<&Observation> = observations
        .iter()
        .filter(|o| &o.tenant_id == tenant_id && o.status == ObservationStatus::Error)
        .collect();
    if errors.is_empty() {
        return Vec::new();
    }
    vec![Finding {
        tenant_id: tenant_id.clone(),
        severity: FindingSeverity::Warning,
        title: format!("发现 {} 个失败观测", errors.len()),
        evidence_ids: errors.into_iter().map(|o| o.id).collect(),
    }]
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Observation {
        Observation {
            id: Uuid::new_v4(),
            tenant_id: TenantId(Uuid::new_v4()),
            trace_id: "t".into(),
            span_id: "s".into(),
            kind: ObservationKind::Agent,
            name: "agent.run".into(),
            status: ObservationStatus::Ok,
            started_at_ms: 0,
            duration_ms: 10,
            attributes: BTreeMap::new(),
        }
    }

    #[test]
    fn valid_observation_passes() {
        assert!(sample().validate().is_ok());
    }

    #[test]
    fn empty_name_is_rejected() {
        let mut o = sample();
        o.name = " ".into();
        assert_eq!(o.validate(), Err(ObservationError::EmptyName));
    }

    #[test]
    fn diagnosis_is_tenant_scoped() {
        let o = sample();
        let other = TenantId(Uuid::new_v4());
        assert!(diagnose(&other, &[o]).is_empty());
    }

    #[test]
    fn usage_ledger_accumulates_by_tenant_period_and_kind() {
        let tenant = TenantId(Uuid::new_v4());
        let mut ledger = UsageLedger::default();
        ledger.record(UsageEntry {
            tenant_id: tenant.clone(),
            period: "2026-08".into(),
            kind: UsageKind::Observation,
            quantity: 2,
        });
        ledger.record(UsageEntry {
            tenant_id: tenant.clone(),
            period: "2026-08".into(),
            kind: UsageKind::Observation,
            quantity: 3,
        });
        assert_eq!(ledger.total(&tenant, "2026-08", &UsageKind::Observation), 5);
        assert_eq!(ledger.snapshot(&tenant, "2026-09").len(), 0);
    }

    #[test]
    fn jsonl_store_round_trips_observations() {
        let path = std::env::temp_dir().join(format!("observability-{}.jsonl", Uuid::new_v4()));
        let store = JsonlObservationStore::new(&path);
        let observation = sample();
        store.append(&observation).unwrap();
        assert_eq!(store.list().unwrap(), vec![observation]);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn agent_plan_only_uses_requested_tenant_evidence() {
        let observation = sample();
        let request = AgentRequest {
            tenant_id: observation.tenant_id.clone(),
            objective: "解释执行失败".into(),
            observation_ids: vec![observation.id],
        };
        let decision = plan_agent_request(&request, &[observation.clone()]);
        assert_eq!(decision.evidence_ids, vec![observation.id]);
        assert_eq!(decision.actions[0].tool_name, "summarize_trace");
    }

    #[test]
    fn unsafe_or_unapproved_tool_is_not_executed() {
        let tenant = TenantId(Uuid::new_v4());
        let request = ToolExecutionRequest {
            tenant_id: tenant,
            tool_name: "shell.exec".into(),
            evidence_ids: Vec::new(),
            approved: true,
        };
        assert_eq!(execute_safe_tool(&request, &[]).status, "rejected");
    }

    #[test]
    fn monthly_quote_uses_integer_cents_and_overage() {
        let tenant = TenantId(Uuid::new_v4());
        let mut ledger = UsageLedger::default();
        ledger.record(UsageEntry {
            tenant_id: tenant.clone(),
            period: "2026-08".into(),
            kind: UsageKind::Observation,
            quantity: 101_001,
        });
        let quote = quote_monthly_usage(&ledger, &tenant, "2026-08", SubscriptionPlan::Starter);
        assert_eq!(quote.base_price_cents, 1_900);
        assert_eq!(quote.overage_cents, 6);
        assert_eq!(quote.total_cents, 1_906);
    }

    #[test]
    fn jsonl_usage_store_restores_accumulated_ledger() {
        let path = std::env::temp_dir().join(format!("usage-{}.jsonl", Uuid::new_v4()));
        let store = JsonlUsageStore::new(&path);
        let tenant = TenantId(Uuid::new_v4());
        let entry = UsageEntry {
            tenant_id: tenant.clone(),
            period: "2026-08".into(),
            kind: UsageKind::AgentRun,
            quantity: 4,
        };
        store.append(&entry).unwrap();
        store.append(&entry).unwrap();
        assert_eq!(
            store
                .load()
                .unwrap()
                .total(&tenant, "2026-08", &UsageKind::AgentRun),
            8
        );
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn model_provider_is_replaceable_and_returns_usage() {
        let provider = DeterministicModelProvider;
        let request = ModelRequest {
            tenant_id: TenantId(Uuid::new_v4()),
            model: "local-deterministic".into(),
            prompt: "总结失败原因".into(),
            evidence_ids: vec![Uuid::new_v4()],
        };
        let response = provider.complete(&request).unwrap();
        assert_eq!(response.model, "local-deterministic");
        assert!(response.output_tokens > 0);
        let observation = model_observation(&request, &response, "trace", 1, 2);
        assert_eq!(observation.kind, ObservationKind::Model);
        assert_eq!(observation.attributes["evidence_count"], "1");
    }

    #[test]
    fn bounded_queue_rejects_when_full_and_preserves_order() {
        let mut queue = BoundedObservationQueue::new(1);
        let first = sample();
        let second = sample();
        queue.try_push(first.clone()).unwrap();
        assert!(queue.try_push(second).is_err());
        assert_eq!(queue.len(), 1);
        assert_eq!(queue.pop(), Some(first));
        assert!(queue.is_empty());
    }
}
