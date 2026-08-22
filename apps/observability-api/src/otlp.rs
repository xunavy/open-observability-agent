use observability_core::{Observation, ObservationKind, ObservationStatus, TenantId};
use opentelemetry_proto::tonic::{
    collector::trace::v1::{
        ExportTracePartialSuccess, ExportTraceServiceRequest, ExportTraceServiceResponse,
    },
    common::v1::{any_value, AnyValue, KeyValue},
    trace::v1::{span, status, Span},
};
use prost::Message;
use serde_json::{Map, Number, Value};
use std::{collections::BTreeMap, fmt};
use uuid::Uuid;

const OBSERVATION_NAMESPACE: Uuid = Uuid::from_u128(0x72a1b31d_1ea6_43b5_98ec_111cd8c25642);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OtlpEncoding {
    Protobuf,
    Json,
}

impl OtlpEncoding {
    pub fn from_content_type(content_type: Option<&str>) -> Result<Self, OtlpError> {
        let media_type = content_type
            .unwrap_or_default()
            .split(';')
            .next()
            .unwrap_or_default()
            .trim()
            .to_ascii_lowercase();
        match media_type.as_str() {
            "application/x-protobuf" => Ok(Self::Protobuf),
            "application/json" => Ok(Self::Json),
            _ => Err(OtlpError::UnsupportedContentType),
        }
    }

    pub fn content_type(self) -> &'static str {
        match self {
            Self::Protobuf => "application/x-protobuf",
            Self::Json => "application/json",
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub enum OtlpError {
    UnsupportedContentType,
    InvalidPayload(String),
    ResponseEncoding(String),
}

impl fmt::Display for OtlpError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnsupportedContentType => formatter
                .write_str("content-type must be application/x-protobuf or application/json"),
            Self::InvalidPayload(message) => {
                write!(formatter, "invalid OTLP trace payload: {message}")
            }
            Self::ResponseEncoding(message) => {
                write!(formatter, "cannot encode OTLP response: {message}")
            }
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct MappedTraceBatch {
    pub observations: Vec<Observation>,
    pub rejected_spans: i64,
    pub error_message: String,
}

pub fn decode_request(
    encoding: OtlpEncoding,
    body: &[u8],
) -> Result<ExportTraceServiceRequest, OtlpError> {
    match encoding {
        OtlpEncoding::Protobuf => ExportTraceServiceRequest::decode(body)
            .map_err(|error| OtlpError::InvalidPayload(error.to_string())),
        OtlpEncoding::Json => serde_json::from_slice(body)
            .map_err(|error| OtlpError::InvalidPayload(error.to_string())),
    }
}

pub fn encode_response(
    encoding: OtlpEncoding,
    rejected_spans: i64,
    error_message: String,
) -> Result<Vec<u8>, OtlpError> {
    let has_partial_success = rejected_spans > 0 || !error_message.is_empty();
    let partial_success = has_partial_success.then_some(ExportTracePartialSuccess {
        rejected_spans,
        error_message: error_message.clone(),
    });
    let response = ExportTraceServiceResponse { partial_success };
    match encoding {
        OtlpEncoding::Protobuf => Ok(response.encode_to_vec()),
        OtlpEncoding::Json if has_partial_success => serde_json::to_vec(&serde_json::json!({
            "partialSuccess": {
                "rejectedSpans": rejected_spans.to_string(),
                "errorMessage": error_message,
            }
        }))
        .map_err(|error| OtlpError::ResponseEncoding(error.to_string())),
        OtlpEncoding::Json => Ok(b"{}".to_vec()),
    }
}

pub fn encode_error(
    encoding: OtlpEncoding,
    code: i32,
    message: &str,
) -> Result<Vec<u8>, OtlpError> {
    #[derive(Clone, PartialEq, Message)]
    struct RpcStatus {
        #[prost(int32, tag = "1")]
        code: i32,
        #[prost(string, tag = "2")]
        message: String,
    }

    match encoding {
        OtlpEncoding::Protobuf => Ok(RpcStatus {
            code,
            message: message.into(),
        }
        .encode_to_vec()),
        OtlpEncoding::Json => serde_json::to_vec(&serde_json::json!({
            "code": code,
            "message": message,
        }))
        .map_err(|error| OtlpError::ResponseEncoding(error.to_string())),
    }
}

pub fn span_count(request: &ExportTraceServiceRequest) -> usize {
    request
        .resource_spans
        .iter()
        .flat_map(|resource| &resource.scope_spans)
        .map(|scope| scope.spans.len())
        .sum()
}

pub fn map_request(request: ExportTraceServiceRequest, tenant_id: TenantId) -> MappedTraceBatch {
    let mut observations = Vec::new();
    let mut rejection_counts = BTreeMap::<&'static str, i64>::new();

    for resource_spans in request.resource_spans {
        let resource_attributes = resource_spans
            .resource
            .map(|resource| resource.attributes)
            .unwrap_or_default();
        for scope_spans in resource_spans.scope_spans {
            for span in scope_spans.spans {
                match map_span(
                    span,
                    &tenant_id,
                    &resource_attributes,
                    scope_spans.scope.as_ref(),
                ) {
                    Ok(observation) => observations.push(observation),
                    Err(reason) => *rejection_counts.entry(reason).or_default() += 1,
                }
            }
        }
    }

    let rejected_spans = rejection_counts.values().sum();
    let error_message = if rejection_counts.is_empty() {
        String::new()
    } else {
        let reasons = rejection_counts
            .into_iter()
            .map(|(reason, count)| format!("{reason}: {count}"))
            .collect::<Vec<_>>()
            .join(", ");
        format!("rejected invalid spans ({reasons})")
    };

    MappedTraceBatch {
        observations,
        rejected_spans,
        error_message,
    }
}

fn map_span(
    span: Span,
    tenant_id: &TenantId,
    resource_attributes: &[KeyValue],
    scope: Option<&opentelemetry_proto::tonic::common::v1::InstrumentationScope>,
) -> Result<Observation, &'static str> {
    if !valid_identifier(&span.trace_id, 16) {
        return Err("invalid trace_id");
    }
    if !valid_identifier(&span.span_id, 8) {
        return Err("invalid span_id");
    }
    if span.name.trim().is_empty() {
        return Err("empty span name");
    }
    if span.end_time_unix_nano < span.start_time_unix_nano {
        return Err("end time precedes start time");
    }

    let started_at_ms_u64 = span.start_time_unix_nano / 1_000_000;
    let started_at_ms = i64::try_from(started_at_ms_u64).map_err(|_| "start time is too large")?;
    let duration_ms = (span.end_time_unix_nano - span.start_time_unix_nano) / 1_000_000;
    if duration_ms > 86_400_000 {
        return Err("duration exceeds ingestion limit");
    }

    let trace_id = hex(&span.trace_id);
    let span_id = hex(&span.span_id);
    let mut identity = Vec::with_capacity(16 + span.trace_id.len() + span.span_id.len());
    identity.extend_from_slice(tenant_id.0.as_bytes());
    identity.extend_from_slice(&span.trace_id);
    identity.extend_from_slice(&span.span_id);

    let mut attributes = BTreeMap::new();
    insert_attributes(&mut attributes, resource_attributes, "resource.");
    if let Some(scope) = scope {
        if !scope.name.is_empty() {
            attributes.insert("otel.scope.name".into(), scope.name.clone());
        }
        if !scope.version.is_empty() {
            attributes.insert("otel.scope.version".into(), scope.version.clone());
        }
        insert_attributes(&mut attributes, &scope.attributes, "scope.");
    }
    insert_attributes(&mut attributes, &span.attributes, "");
    if !span.parent_span_id.is_empty() {
        attributes.insert("otel.parent_span_id".into(), hex(&span.parent_span_id));
    }
    if !span.trace_state.is_empty() {
        attributes.insert("otel.trace_state".into(), span.trace_state.clone());
    }
    attributes.insert("otel.span.kind".into(), span_kind_name(span.kind).into());
    if let Some(status) = &span.status {
        if !status.message.is_empty() {
            attributes.insert("otel.status_message".into(), status.message.clone());
        }
    }

    let kind = classify_observation(&attributes);
    let status = if span
        .status
        .as_ref()
        .is_some_and(|status| status.code == status::StatusCode::Error as i32)
    {
        ObservationStatus::Error
    } else {
        ObservationStatus::Ok
    };

    Ok(Observation {
        id: Uuid::new_v5(&OBSERVATION_NAMESPACE, &identity),
        tenant_id: tenant_id.clone(),
        trace_id,
        span_id,
        kind,
        name: span.name,
        status,
        started_at_ms,
        duration_ms,
        attributes,
    })
}

fn valid_identifier(value: &[u8], expected_length: usize) -> bool {
    value.len() == expected_length && value.iter().any(|byte| *byte != 0)
}

fn span_kind_name(kind: i32) -> &'static str {
    match span::SpanKind::try_from(kind).unwrap_or(span::SpanKind::Unspecified) {
        span::SpanKind::Internal => "internal",
        span::SpanKind::Server => "server",
        span::SpanKind::Client => "client",
        span::SpanKind::Producer => "producer",
        span::SpanKind::Consumer => "consumer",
        span::SpanKind::Unspecified => "unspecified",
    }
}

fn classify_observation(attributes: &BTreeMap<String, String>) -> ObservationKind {
    if let Some(explicit) = attributes.get("observability.kind") {
        match explicit.to_ascii_lowercase().as_str() {
            "agent" => return ObservationKind::Agent,
            "tool" => return ObservationKind::Tool,
            "model" => return ObservationKind::Model,
            "http" => return ObservationKind::Http,
            "workflow" => return ObservationKind::Workflow,
            _ => {}
        }
    }

    let open_inference_kind = attributes
        .get("openinference.span.kind")
        .map(|value| value.to_ascii_lowercase());
    let operation = attributes
        .get("gen_ai.operation.name")
        .map(|value| value.to_ascii_lowercase());
    if attributes.contains_key("gen_ai.agent.name")
        || open_inference_kind.as_deref() == Some("agent")
        || operation.as_deref() == Some("invoke_agent")
    {
        ObservationKind::Agent
    } else if attributes.contains_key("gen_ai.tool.name")
        || open_inference_kind.as_deref() == Some("tool")
        || operation.as_deref() == Some("execute_tool")
    {
        ObservationKind::Tool
    } else if attributes.contains_key("gen_ai.request.model")
        || attributes.contains_key("gen_ai.response.model")
        || attributes.contains_key("gen_ai.system")
        || open_inference_kind.as_deref() == Some("llm")
    {
        ObservationKind::Model
    } else if attributes
        .keys()
        .any(|key| key.starts_with("http.") || key.starts_with("url.") || key == "server.address")
    {
        ObservationKind::Http
    } else {
        ObservationKind::Workflow
    }
}

fn insert_attributes(target: &mut BTreeMap<String, String>, values: &[KeyValue], prefix: &str) {
    for item in values {
        if item.key.is_empty() {
            continue;
        }
        if let Some(value) = &item.value {
            target.insert(format!("{prefix}{}", item.key), any_value_to_string(value));
        }
    }
}

fn any_value_to_string(value: &AnyValue) -> String {
    match value.value.as_ref() {
        Some(any_value::Value::StringValue(value)) => value.clone(),
        Some(any_value::Value::BoolValue(value)) => value.to_string(),
        Some(any_value::Value::IntValue(value)) => value.to_string(),
        Some(any_value::Value::DoubleValue(value)) => value.to_string(),
        Some(any_value::Value::BytesValue(value)) => hex(value),
        Some(any_value::Value::ArrayValue(value)) => {
            let values = value.values.iter().map(any_value_to_json).collect();
            Value::Array(values).to_string()
        }
        Some(any_value::Value::KvlistValue(value)) => {
            let mut values = Map::new();
            for item in &value.values {
                if let Some(value) = &item.value {
                    values.insert(item.key.clone(), any_value_to_json(value));
                }
            }
            Value::Object(values).to_string()
        }
        Some(any_value::Value::StringValueStrindex(_)) | None => String::new(),
    }
}

fn any_value_to_json(value: &AnyValue) -> Value {
    match value.value.as_ref() {
        Some(any_value::Value::StringValue(value)) => Value::String(value.clone()),
        Some(any_value::Value::BoolValue(value)) => Value::Bool(*value),
        Some(any_value::Value::IntValue(value)) => Value::Number((*value).into()),
        Some(any_value::Value::DoubleValue(value)) => Number::from_f64(*value)
            .map(Value::Number)
            .unwrap_or_else(|| Value::String(value.to_string())),
        Some(any_value::Value::BytesValue(value)) => Value::String(hex(value)),
        Some(any_value::Value::ArrayValue(value)) => {
            Value::Array(value.values.iter().map(any_value_to_json).collect())
        }
        Some(any_value::Value::KvlistValue(value)) => Value::Object(
            value
                .values
                .iter()
                .filter_map(|item| {
                    item.value
                        .as_ref()
                        .map(|value| (item.key.clone(), any_value_to_json(value)))
                })
                .collect(),
        ),
        Some(any_value::Value::StringValueStrindex(_)) | None => Value::Null,
    }
}

fn hex(value: &[u8]) -> String {
    let mut encoded = String::with_capacity(value.len() * 2);
    const HEX: &[u8; 16] = b"0123456789abcdef";
    for byte in value {
        encoded.push(HEX[(byte >> 4) as usize] as char);
        encoded.push(HEX[(byte & 0x0f) as usize] as char);
    }
    encoded
}

#[cfg(test)]
mod tests {
    use super::*;
    use opentelemetry_proto::tonic::{
        common::v1::{AnyValue, InstrumentationScope, KeyValue},
        resource::v1::Resource,
        trace::v1::{status, ResourceSpans, ScopeSpans, Status},
    };

    fn string_attribute(key: &str, value: &str) -> KeyValue {
        KeyValue {
            key: key.into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(value.into())),
            }),
            key_strindex: 0,
        }
    }

    fn span(trace_byte: u8, span_byte: u8) -> Span {
        Span {
            trace_id: vec![trace_byte; 16],
            span_id: vec![span_byte; 8],
            name: "agent.invoke".into(),
            start_time_unix_nano: 1_000_000_000,
            end_time_unix_nano: 1_125_000_000,
            attributes: vec![string_attribute("gen_ai.agent.name", "support-agent")],
            status: Some(Status {
                message: "provider timeout".into(),
                code: status::StatusCode::Error as i32,
            }),
            ..Span::default()
        }
    }

    fn request(spans: Vec<Span>) -> ExportTraceServiceRequest {
        ExportTraceServiceRequest {
            resource_spans: vec![ResourceSpans {
                resource: Some(Resource {
                    attributes: vec![string_attribute("service.name", "checkout")],
                    ..Resource::default()
                }),
                scope_spans: vec![ScopeSpans {
                    scope: Some(InstrumentationScope {
                        name: "example-instrumentation".into(),
                        version: "1.0.0".into(),
                        ..InstrumentationScope::default()
                    }),
                    spans,
                    ..ScopeSpans::default()
                }],
                ..ResourceSpans::default()
            }],
        }
    }

    #[test]
    fn decodes_binary_and_json_requests() {
        let expected = request(vec![span(1, 2)]);
        let binary = expected.encode_to_vec();
        assert_eq!(
            decode_request(OtlpEncoding::Protobuf, &binary).unwrap(),
            expected
        );

        let json = serde_json::to_vec(&expected).unwrap();
        assert_eq!(decode_request(OtlpEncoding::Json, &json).unwrap(), expected);
    }

    #[test]
    fn maps_agent_span_and_preserves_evidence_fields() {
        let tenant = TenantId(Uuid::new_v4());
        let mapped = map_request(request(vec![span(1, 2)]), tenant.clone());
        assert_eq!(mapped.rejected_spans, 0);
        assert_eq!(mapped.observations.len(), 1);
        let observation = &mapped.observations[0];
        assert_eq!(observation.tenant_id, tenant);
        assert_eq!(observation.trace_id, "01010101010101010101010101010101");
        assert_eq!(observation.span_id, "0202020202020202");
        assert_eq!(observation.kind, ObservationKind::Agent);
        assert_eq!(observation.status, ObservationStatus::Error);
        assert_eq!(observation.started_at_ms, 1_000);
        assert_eq!(observation.duration_ms, 125);
        assert_eq!(
            observation.attributes.get("resource.service.name"),
            Some(&"checkout".into())
        );
        assert_eq!(
            observation.attributes.get("otel.scope.name"),
            Some(&"example-instrumentation".into())
        );
    }

    #[test]
    fn rejects_bad_span_and_reports_partial_success() {
        let tenant = TenantId(Uuid::new_v4());
        let mut invalid = span(0, 2);
        invalid.trace_id = vec![0; 16];
        let mapped = map_request(request(vec![span(1, 2), invalid]), tenant);
        assert_eq!(mapped.observations.len(), 1);
        assert_eq!(mapped.rejected_spans, 1);
        assert!(mapped.error_message.contains("invalid trace_id: 1"));

        let body = encode_response(
            OtlpEncoding::Protobuf,
            mapped.rejected_spans,
            mapped.error_message,
        )
        .unwrap();
        let response = ExportTraceServiceResponse::decode(body.as_slice()).unwrap();
        assert_eq!(response.partial_success.unwrap().rejected_spans, 1);

        let json = encode_response(OtlpEncoding::Json, 1, "rejected invalid spans".into()).unwrap();
        assert_eq!(
            serde_json::from_slice::<Value>(&json).unwrap(),
            serde_json::json!({
                "partialSuccess": {
                    "rejectedSpans": "1",
                    "errorMessage": "rejected invalid spans",
                }
            })
        );
        assert_eq!(
            encode_response(OtlpEncoding::Json, 0, String::new()).unwrap(),
            b"{}"
        );
    }

    #[test]
    fn deterministic_ids_make_export_retries_idempotent() {
        let tenant = TenantId(Uuid::new_v4());
        let first = map_request(request(vec![span(1, 2)]), tenant.clone());
        let second = map_request(request(vec![span(1, 2)]), tenant);
        assert_eq!(first.observations[0].id, second.observations[0].id);
    }
}
