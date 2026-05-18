//! Incident Management
//!
//! Full incident lifecycle: create → acknowledge → investigate → resolve → postmortem.
//! Tracks timeline of events, impacted services, and responders.

use crate::api::{ApiState, StandardResponse};
use axum::{
    extract::{State, Path},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;
use parking_lot::RwLock;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum IncidentSeverity { Sev1, Sev2, Sev3, Sev4 }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum IncidentStatus { Open, Acknowledged, Investigating, Mitigated, Resolved, Postmortem }

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Incident {
    pub id: String,
    pub title: String,
    pub description: String,
    pub severity: IncidentSeverity,
    pub status: IncidentStatus,
    pub commander: Option<String>,
    pub services: Vec<String>,
    pub created_at_us: u64,
    pub updated_at_us: u64,
    pub resolved_at_us: Option<u64>,
    pub timeline: Vec<TimelineEntry>,
    pub postmortem: Option<Postmortem>,
    #[serde(default)]
    pub labels: HashMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimelineEntry {
    pub timestamp_us: u64,
    pub author: String,
    pub action: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Postmortem {
    pub summary: String,
    pub root_cause: String,
    pub impact: String,
    pub detection_time_secs: u64,
    pub resolution_time_secs: u64,
    pub action_items: Vec<ActionItem>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActionItem {
    pub description: String,
    pub owner: String,
    pub status: String,
    pub due_date: Option<String>,
}

fn now_us() -> u64 {
    SystemTime::now().duration_since(SystemTime::UNIX_EPOCH).unwrap_or_default().as_micros() as u64
}

#[derive(Debug, Default)]
pub struct IncidentStore {
    incidents: RwLock<HashMap<String, Incident>>,
}

impl IncidentStore {
    pub fn new() -> Self { Self::default() }

    pub fn create(&self, mut incident: Incident) -> Incident {
        if incident.id.is_empty() { incident.id = uuid::Uuid::new_v4().to_string(); }
        incident.created_at_us = now_us();
        incident.updated_at_us = incident.created_at_us;
        incident.timeline.push(TimelineEntry {
            timestamp_us: incident.created_at_us,
            author: "system".to_string(),
            action: "created".to_string(),
            message: format!("Incident created: {}", incident.title),
        });
        self.incidents.write().insert(incident.id.clone(), incident.clone());
        incident
    }

    pub fn update_status(&self, id: &str, status: IncidentStatus, author: &str, message: &str) -> Option<Incident> {
        let mut store = self.incidents.write();
        if let Some(inc) = store.get_mut(id) {
            inc.status = status;
            inc.updated_at_us = now_us();
            if status == IncidentStatus::Resolved { inc.resolved_at_us = Some(now_us()); }
            inc.timeline.push(TimelineEntry {
                timestamp_us: now_us(),
                author: author.to_string(),
                action: format!("{:?}", status).to_lowercase(),
                message: message.to_string(),
            });
            Some(inc.clone())
        } else { None }
    }

    pub fn add_postmortem(&self, id: &str, postmortem: Postmortem) -> Option<Incident> {
        let mut store = self.incidents.write();
        if let Some(inc) = store.get_mut(id) {
            inc.postmortem = Some(postmortem);
            inc.status = IncidentStatus::Postmortem;
            inc.updated_at_us = now_us();
            Some(inc.clone())
        } else { None }
    }

    pub fn list(&self, status: Option<IncidentStatus>) -> Vec<Incident> {
        let store = self.incidents.read();
        let mut result: Vec<Incident> = store.values()
            .filter(|i| status.is_none() || Some(i.status) == status)
            .cloned().collect();
        result.sort_by(|a, b| b.created_at_us.cmp(&a.created_at_us));
        result
    }

    pub fn get(&self, id: &str) -> Option<Incident> {
        self.incidents.read().get(id).cloned()
    }
}

pub async fn create_incident(State(state): State<Arc<ApiState>>, Json(incident): Json<Incident>) -> impl IntoResponse {
    let created = state.incident_store.create(incident);
    let data = serde_json::to_value(&created).unwrap_or_default();
    StandardResponse::ok(data, "Incident created.")
}

pub async fn list_incidents(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let incidents = state.incident_store.list(None);
    let data = serde_json::to_value(&incidents).unwrap_or_default();
    StandardResponse::ok(data, "Incidents list.")
}

pub async fn get_incident(Path(id): Path<String>, State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    match state.incident_store.get(&id) {
        Some(inc) => { let data = serde_json::to_value(&inc).unwrap_or_default(); StandardResponse::ok(data, "Incident detail.") }
        None => StandardResponse::error(serde_json::json!({}), "Incident not found", "NOT_FOUND", vec![]),
    }
}

#[derive(Deserialize)]
pub struct StatusUpdate { pub status: IncidentStatus, pub author: String, pub message: String }

pub async fn update_incident_status(Path(id): Path<String>, State(state): State<Arc<ApiState>>, Json(update): Json<StatusUpdate>) -> impl IntoResponse {
    match state.incident_store.update_status(&id, update.status, &update.author, &update.message) {
        Some(inc) => { let data = serde_json::to_value(&inc).unwrap_or_default(); StandardResponse::ok(data, "Incident updated.") }
        None => StandardResponse::error(serde_json::json!({}), "Incident not found", "NOT_FOUND", vec![]),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_incident_lifecycle() {
        let store = IncidentStore::new();
        let inc = store.create(Incident {
            id: String::new(), title: "API Down".to_string(), description: "500 errors".to_string(),
            severity: IncidentSeverity::Sev1, status: IncidentStatus::Open, commander: Some("alice".to_string()),
            services: vec!["api-gateway".to_string()], created_at_us: 0, updated_at_us: 0,
            resolved_at_us: None, timeline: vec![], postmortem: None, labels: HashMap::new(),
        });
        assert_eq!(inc.status, IncidentStatus::Open);
        assert_eq!(inc.timeline.len(), 1);

        let updated = store.update_status(&inc.id, IncidentStatus::Investigating, "bob", "Looking into it").unwrap();
        assert_eq!(updated.status, IncidentStatus::Investigating);
        assert_eq!(updated.timeline.len(), 2);

        let resolved = store.update_status(&inc.id, IncidentStatus::Resolved, "bob", "Fixed DB config").unwrap();
        assert!(resolved.resolved_at_us.is_some());
    }

    fn mk(title: &str, sev: IncidentSeverity) -> Incident {
        Incident {
            id: String::new(), title: title.to_string(), description: String::new(),
            severity: sev, status: IncidentStatus::Open, commander: None,
            services: vec![], created_at_us: 0, updated_at_us: 0,
            resolved_at_us: None, timeline: vec![], postmortem: None, labels: HashMap::new(),
        }
    }

    #[test]
    fn test_create_assigns_id_when_empty() {
        let store = IncidentStore::new();
        let inc = store.create(mk("x", IncidentSeverity::Sev2));
        assert!(!inc.id.is_empty());
    }

    #[test]
    fn test_create_preserves_nonempty_id() {
        let store = IncidentStore::new();
        let mut i = mk("x", IncidentSeverity::Sev2);
        i.id = "custom-id".to_string();
        let created = store.create(i);
        assert_eq!(created.id, "custom-id");
    }

    #[test]
    fn test_list_multiple_incidents() {
        let store = IncidentStore::new();
        store.create(mk("a", IncidentSeverity::Sev1));
        store.create(mk("b", IncidentSeverity::Sev2));
        store.create(mk("c", IncidentSeverity::Sev3));
        assert_eq!(store.list(None).len(), 3);
    }

    #[test]
    fn test_list_filter_by_status() {
        let store = IncidentStore::new();
        let i1 = store.create(mk("a", IncidentSeverity::Sev1));
        store.create(mk("b", IncidentSeverity::Sev2));
        store.update_status(&i1.id, IncidentStatus::Resolved, "x", "done");

        assert_eq!(store.list(Some(IncidentStatus::Open)).len(), 1);
        assert_eq!(store.list(Some(IncidentStatus::Resolved)).len(), 1);
    }

    #[test]
    fn test_get_incident_found() {
        let store = IncidentStore::new();
        let i = store.create(mk("a", IncidentSeverity::Sev1));
        assert!(store.get(&i.id).is_some());
    }

    #[test]
    fn test_get_incident_not_found() {
        let store = IncidentStore::new();
        assert!(store.get("nope").is_none());
    }

    #[test]
    fn test_update_status_nonexistent() {
        let store = IncidentStore::new();
        let r = store.update_status("nope", IncidentStatus::Resolved, "x", "m");
        assert!(r.is_none());
    }

    #[test]
    fn test_timeline_grows_with_updates() {
        let store = IncidentStore::new();
        let i = store.create(mk("a", IncidentSeverity::Sev1));
        store.update_status(&i.id, IncidentStatus::Acknowledged, "u", "ack");
        store.update_status(&i.id, IncidentStatus::Investigating, "u", "inv");
        store.update_status(&i.id, IncidentStatus::Mitigated, "u", "mit");
        let final_ = store.update_status(&i.id, IncidentStatus::Resolved, "u", "res").unwrap();
        assert_eq!(final_.timeline.len(), 5); // create + 4 updates
    }

    #[test]
    fn test_add_postmortem() {
        let store = IncidentStore::new();
        let i = store.create(mk("a", IncidentSeverity::Sev1));
        let pm = Postmortem {
            summary: "s".into(), root_cause: "rc".into(), impact: "i".into(),
            detection_time_secs: 60, resolution_time_secs: 3600, action_items: vec![],
        };
        let updated = store.add_postmortem(&i.id, pm).unwrap();
        assert!(updated.postmortem.is_some());
        assert_eq!(updated.status, IncidentStatus::Postmortem);
    }

    #[test]
    fn test_add_postmortem_nonexistent() {
        let store = IncidentStore::new();
        let pm = Postmortem {
            summary: "s".into(), root_cause: "r".into(), impact: "i".into(),
            detection_time_secs: 1, resolution_time_secs: 2, action_items: vec![],
        };
        assert!(store.add_postmortem("nope", pm).is_none());
    }

    #[test]
    fn test_severity_variants() {
        assert_ne!(IncidentSeverity::Sev1, IncidentSeverity::Sev2);
        assert_eq!(IncidentSeverity::Sev3, IncidentSeverity::Sev3);
    }

    #[test]
    fn test_list_sorted_by_created_desc() {
        let store = IncidentStore::new();
        store.create(mk("first", IncidentSeverity::Sev1));
        std::thread::sleep(std::time::Duration::from_millis(2));
        store.create(mk("second", IncidentSeverity::Sev1));
        let list = store.list(None);
        assert!(list[0].created_at_us >= list[1].created_at_us);
    }

    #[test]
    fn test_resolved_sets_resolved_at() {
        let store = IncidentStore::new();
        let i = store.create(mk("a", IncidentSeverity::Sev1));
        assert!(i.resolved_at_us.is_none());
        let r = store.update_status(&i.id, IncidentStatus::Resolved, "u", "done").unwrap();
        assert!(r.resolved_at_us.is_some());
    }

    #[test]
    fn test_non_resolved_status_no_resolved_at() {
        let store = IncidentStore::new();
        let i = store.create(mk("a", IncidentSeverity::Sev1));
        let r = store.update_status(&i.id, IncidentStatus::Acknowledged, "u", "m").unwrap();
        assert!(r.resolved_at_us.is_none());
    }
}
