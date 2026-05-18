//! Data Pipeline / Transforms
//!
//! User-defined pipelines that transform telemetry data in-flight:
//! - Parse unstructured logs into structured fields
//! - Extract metrics from log patterns
//! - Enrich events with external data
//! - Route to different storage tiers
//! - Filter/drop noisy data
//! Like Datadog's Log Pipelines but for all signal types.

use crate::api::{ApiState, StandardResponse};
use axum::{
    extract::{State, Path},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

/// A data pipeline definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Pipeline {
    pub id: String,
    pub name: String,
    pub description: String,
    pub enabled: bool,
    pub order: i32,
    /// What signal types this pipeline applies to
    pub applies_to: Vec<SignalType>,
    /// Filter: only process data matching this condition
    pub filter: Option<PipelineFilter>,
    /// Ordered list of transforms to apply
    pub transforms: Vec<Transform>,
    pub created_at_us: u64,
    pub updated_at_us: u64,
    #[serde(default)]
    pub stats: PipelineStats,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SignalType { Logs, Metrics, Traces, Events }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineFilter {
    pub field: String,
    pub operator: String,
    pub value: String,
}

/// Transform operations applied to data in-flight.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum Transform {
    /// Parse a field using a pattern (grok, regex, JSON, key-value)
    Parse { source_field: String, parser: ParserType, target_prefix: Option<String> },
    /// Extract a metric from a log field
    ExtractMetric { field: String, metric_name: String, metric_type: String, tags: Vec<String> },
    /// Add/set a field
    SetField { field: String, value: String },
    /// Remove a field
    RemoveField { field: String },
    /// Rename a field
    RenameField { from: String, to: String },
    /// Convert field type (string→number, etc.)
    TypeConvert { field: String, target_type: String },
    /// Filter: drop events matching condition
    Drop { condition: String },
    /// Route to a specific storage tier
    Route { destination: String, condition: Option<String> },
    /// Enrich with external lookup (e.g., GeoIP, user data)
    Enrich { source_field: String, lookup_table: String, target_field: String },
    /// Mask sensitive data
    Mask { field: String, pattern: String, replacement: String },
    /// Sample: keep only N% of events
    Sample { rate_percent: f64 },
    /// Add timestamp if missing
    AddTimestamp { field: String, format: Option<String> },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum ParserType {
    Json,
    Regex { pattern: String },
    Grok { pattern: String },
    KeyValue { separator: String, delimiter: String },
    Csv { headers: Vec<String> },
    Syslog,
    Apache,
    Nginx,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PipelineStats {
    pub events_processed: u64,
    pub events_dropped: u64,
    pub errors: u64,
    pub last_processed_us: u64,
}

fn now_us() -> u64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

#[derive(Debug, Default)]
pub struct PipelineStore {
    pipelines: RwLock<HashMap<String, Pipeline>>,
}

impl PipelineStore {
    pub fn new() -> Self { Self::default() }

    pub fn create(&self, mut pipeline: Pipeline) -> Pipeline {
        if pipeline.id.is_empty() { pipeline.id = uuid::Uuid::new_v4().to_string(); }
        pipeline.created_at_us = now_us();
        pipeline.updated_at_us = pipeline.created_at_us;
        self.pipelines.write().insert(pipeline.id.clone(), pipeline.clone());
        pipeline
    }

    pub fn list(&self) -> Vec<Pipeline> {
        let mut result: Vec<Pipeline> = self.pipelines.read().values().cloned().collect();
        result.sort_by_key(|p| p.order);
        result
    }

    pub fn get(&self, id: &str) -> Option<Pipeline> {
        self.pipelines.read().get(id).cloned()
    }

    pub fn update(&self, id: &str, mut pipeline: Pipeline) -> Option<Pipeline> {
        let mut store = self.pipelines.write();
        if store.contains_key(id) {
            pipeline.id = id.to_string();
            pipeline.updated_at_us = now_us();
            store.insert(id.to_string(), pipeline.clone());
            Some(pipeline)
        } else { None }
    }

    pub fn delete(&self, id: &str) -> bool {
        self.pipelines.write().remove(id).is_some()
    }

    pub fn toggle(&self, id: &str) -> Option<bool> {
        let mut store = self.pipelines.write();
        store.get_mut(id).map(|p| { p.enabled = !p.enabled; p.enabled })
    }
}

pub async fn create_pipeline(State(state): State<Arc<ApiState>>, Json(pipeline): Json<Pipeline>) -> impl IntoResponse {
    let created = state.pipeline_store.create(pipeline);
    StandardResponse::ok(serde_json::to_value(&created).unwrap_or_default(), "Pipeline created.")
}

pub async fn list_pipelines(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    StandardResponse::ok(serde_json::to_value(&state.pipeline_store.list()).unwrap_or_default(), "Pipelines.")
}

pub async fn get_pipeline(Path(id): Path<String>, State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    match state.pipeline_store.get(&id) {
        Some(p) => StandardResponse::ok(serde_json::to_value(&p).unwrap_or_default(), "Pipeline."),
        None => StandardResponse::error(serde_json::json!({}), "Not found", "NOT_FOUND", vec![]),
    }
}

pub async fn delete_pipeline(Path(id): Path<String>, State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let deleted = state.pipeline_store.delete(&id);
    StandardResponse::ok(serde_json::json!({"deleted": deleted}), "Pipeline deleted.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pipeline_crud() {
        let store = PipelineStore::new();
        let p = store.create(Pipeline {
            id: String::new(), name: "parse-nginx".into(), description: "Parse nginx access logs".into(),
            enabled: true, order: 1, applies_to: vec![SignalType::Logs],
            filter: Some(PipelineFilter { field: "service".into(), operator: "eq".into(), value: "nginx".into() }),
            transforms: vec![
                Transform::Parse { source_field: "message".into(), parser: ParserType::Nginx, target_prefix: Some("http".into()) },
                Transform::ExtractMetric { field: "http.status".into(), metric_name: "nginx.status".into(), metric_type: "counter".into(), tags: vec!["status".into()] },
                Transform::Mask { field: "http.client_ip".into(), pattern: r"\d+\.\d+\.\d+\.\d+".into(), replacement: "[REDACTED]".into() },
            ],
            created_at_us: 0, updated_at_us: 0, stats: PipelineStats::default(),
        });

        assert!(!p.id.is_empty());
        assert_eq!(store.list().len(), 1);
        assert!(store.get(&p.id).is_some());
        assert!(store.delete(&p.id));
        assert_eq!(store.list().len(), 0);
    }
}
