//! Deploy Annotations API
//!
//! Track CI/CD deployments as time-stamped markers for chart overlays.
//! POST /v1/annotations to mark a deploy, GET /v1/annotations to list them.

use crate::api::{ApiState, StandardResponse};
use axum::{
    extract::{State, Query},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use parking_lot::RwLock;

/// A deploy/event annotation for chart overlays.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Annotation {
    pub id: String,
    pub title: String,
    pub description: Option<String>,
    pub timestamp_us: u64,
    #[serde(default = "default_annotation_type")]
    pub annotation_type: String,
    #[serde(default)]
    pub tags: HashMap<String, String>,
    pub service: Option<String>,
    pub environment: Option<String>,
    /// Source: "github", "gitlab", "jenkins", "manual", etc.
    pub source: Option<String>,
    /// Commit SHA or build ID.
    pub reference: Option<String>,
}

fn default_annotation_type() -> String {
    "deploy".to_string()
}

/// In-memory annotation store.
#[derive(Debug, Default)]
pub struct AnnotationStore {
    annotations: RwLock<Vec<Annotation>>,
}

impl AnnotationStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, mut annotation: Annotation) {
        if annotation.id.is_empty() {
            annotation.id = uuid::Uuid::new_v4().to_string();
        }
        if annotation.timestamp_us == 0 {
            annotation.timestamp_us = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros() as u64;
        }
        self.annotations.write().push(annotation);
    }

    pub fn list(&self, from_us: Option<u64>, to_us: Option<u64>, service: Option<&str>) -> Vec<Annotation> {
        let annotations = self.annotations.read();
        annotations.iter()
            .filter(|a| {
                let in_range = from_us.map_or(true, |f| a.timestamp_us >= f)
                    && to_us.map_or(true, |t| a.timestamp_us <= t);
                let svc_match = service.map_or(true, |s| a.service.as_deref() == Some(s));
                in_range && svc_match
            })
            .cloned()
            .collect()
    }

    pub fn len(&self) -> usize {
        self.annotations.read().len()
    }
}

/// POST /v1/annotations — Create a deploy annotation.
pub async fn create_annotation(
    State(state): State<Arc<ApiState>>,
    Json(annotation): Json<Annotation>,
) -> impl IntoResponse {
    state.annotation_store.insert(annotation);
    StandardResponse::ok(
        serde_json::json!({ "created": true }),
        "Annotation created.",
    )
}

/// GET /v1/annotations — List annotations with optional time range filter.
pub async fn list_annotations(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let from = params.get("from").and_then(|v| v.parse().ok());
    let to = params.get("to").and_then(|v| v.parse().ok());
    let service = params.get("service").map(|s| s.as_str());

    let annotations = state.annotation_store.list(from, to, service);
    let data = serde_json::to_value(&annotations).unwrap_or_default();
    StandardResponse::ok(data, "Annotations list.")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_annotation_store() {
        let store = AnnotationStore::new();
        store.insert(Annotation {
            id: String::new(),
            title: "Deploy v1.2.3".to_string(),
            description: Some("Hotfix".to_string()),
            timestamp_us: 1000,
            annotation_type: "deploy".to_string(),
            tags: HashMap::new(),
            service: Some("api".to_string()),
            environment: Some("production".to_string()),
            source: Some("github".to_string()),
            reference: Some("abc123".to_string()),
        });

        assert_eq!(store.len(), 1);
        let all = store.list(None, None, None);
        assert_eq!(all.len(), 1);
        assert_eq!(all[0].title, "Deploy v1.2.3");
    }

    #[test]
    fn test_filter_by_service() {
        let store = AnnotationStore::new();
        store.insert(Annotation {
            id: "1".to_string(), title: "Deploy A".to_string(), description: None,
            timestamp_us: 1000, annotation_type: "deploy".to_string(),
            tags: HashMap::new(), service: Some("api".to_string()),
            environment: None, source: None, reference: None,
        });
        store.insert(Annotation {
            id: "2".to_string(), title: "Deploy B".to_string(), description: None,
            timestamp_us: 2000, annotation_type: "deploy".to_string(),
            tags: HashMap::new(), service: Some("worker".to_string()),
            environment: None, source: None, reference: None,
        });

        let api_only = store.list(None, None, Some("api"));
        assert_eq!(api_only.len(), 1);
        assert_eq!(api_only[0].title, "Deploy A");
    }
}
