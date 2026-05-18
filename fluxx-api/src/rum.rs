//! Real User Monitoring (RUM) API
//!
//! Ingests browser performance data from the Fluxx RUM JavaScript SDK.
//! Tracks: page loads, web vitals (LCP/FID/CLS/FCP/TTFB), JS errors,
//! network requests, user actions, sessions, and custom events.

use crate::api::{ApiState, StandardResponse};
use axum::{
    extract::{State, Query},
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;
use tracing::info;

/// A single RUM event from the browser SDK.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RumEvent {
    #[serde(rename = "type")]
    pub event_type: String,
    pub timestamp_us: u64,
    pub session_id: String,
    pub page_view_id: Option<String>,
    pub application_id: String,
    pub environment: Option<String>,
    pub app_version: Option<String>,
    pub user_id: Option<String>,
    pub user_email: Option<String>,
    pub url: Option<String>,
    pub user_agent: Option<String>,
    pub screen_width: Option<u32>,
    pub screen_height: Option<u32>,
    pub viewport_width: Option<u32>,
    pub viewport_height: Option<u32>,
    // Flattened extra fields from various event types
    #[serde(flatten)]
    pub data: HashMap<String, serde_json::Value>,
}

/// Aggregated RUM session summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RumSessionSummary {
    pub session_id: String,
    pub application_id: String,
    pub user_id: Option<String>,
    pub first_url: String,
    pub page_view_count: u32,
    pub error_count: u32,
    pub action_count: u32,
    pub started_at_us: u64,
    pub last_activity_us: u64,
    pub duration_ms: u64,
    pub user_agent: String,
    pub country: Option<String>,
}

/// Web Vitals summary for an application.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WebVitalsSummary {
    pub application_id: String,
    pub lcp_p75_ms: f64,
    pub fid_p75_ms: f64,
    pub cls_p75: f64,
    pub fcp_p75_ms: f64,
    pub ttfb_p75_ms: f64,
    pub sample_count: u64,
    pub good_percent: f64,
    pub needs_improvement_percent: f64,
    pub poor_percent: f64,
}

#[derive(Debug, Default)]
pub struct RumStore {
    events: RwLock<Vec<RumEvent>>,
    sessions: RwLock<HashMap<String, RumSessionSummary>>,
}

impl RumStore {
    pub fn new() -> Self { Self::default() }

    pub fn ingest(&self, events: Vec<RumEvent>) {
        let mut sessions = self.sessions.write();

        for event in &events {
            // Update session summary
            let session = sessions.entry(event.session_id.clone()).or_insert_with(|| {
                RumSessionSummary {
                    session_id: event.session_id.clone(),
                    application_id: event.application_id.clone(),
                    user_id: event.user_id.clone(),
                    first_url: event.url.clone().unwrap_or_default(),
                    page_view_count: 0,
                    error_count: 0,
                    action_count: 0,
                    started_at_us: event.timestamp_us,
                    last_activity_us: event.timestamp_us,
                    duration_ms: 0,
                    user_agent: event.user_agent.clone().unwrap_or_default(),
                    country: None,
                }
            });

            session.last_activity_us = session.last_activity_us.max(event.timestamp_us);
            session.duration_ms = (session.last_activity_us - session.started_at_us) / 1000;

            match event.event_type.as_str() {
                "view" => session.page_view_count += 1,
                "error" => session.error_count += 1,
                "action" => session.action_count += 1,
                _ => {}
            }
        }

        self.events.write().extend(events);
    }

    pub fn get_sessions(&self, app: Option<&str>, limit: usize) -> Vec<RumSessionSummary> {
        let sessions = self.sessions.read();
        let mut result: Vec<RumSessionSummary> = sessions.values()
            .filter(|s| app.map_or(true, |a| s.application_id == a))
            .cloned()
            .collect();
        result.sort_by(|a, b| b.last_activity_us.cmp(&a.last_activity_us));
        result.truncate(limit);
        result
    }

    pub fn get_vitals(&self, app_id: &str) -> WebVitalsSummary {
        let events = self.events.read();
        let vitals: Vec<&RumEvent> = events.iter()
            .filter(|e| e.event_type == "vital" && e.application_id == app_id)
            .collect();

        let mut lcp_vals = Vec::new();
        let mut fid_vals = Vec::new();
        let mut cls_vals = Vec::new();
        let mut fcp_vals = Vec::new();
        let mut ratings = HashMap::new();

        for v in &vitals {
            let name = v.data.get("vital_name").and_then(|v| v.as_str()).unwrap_or("");
            let value = v.data.get("vital_value").and_then(|v| v.as_f64()).unwrap_or(0.0);
            let rating = v.data.get("vital_rating").and_then(|v| v.as_str()).unwrap_or("unknown");

            match name {
                "LCP" => lcp_vals.push(value),
                "FID" => fid_vals.push(value),
                "CLS" => cls_vals.push(value),
                "FCP" => fcp_vals.push(value),
                _ => {}
            }
            *ratings.entry(rating.to_string()).or_insert(0u64) += 1;
        }

        let total = ratings.values().sum::<u64>().max(1) as f64;

        WebVitalsSummary {
            application_id: app_id.to_string(),
            lcp_p75_ms: percentile_75(&mut lcp_vals),
            fid_p75_ms: percentile_75(&mut fid_vals),
            cls_p75: percentile_75(&mut cls_vals),
            fcp_p75_ms: percentile_75(&mut fcp_vals),
            ttfb_p75_ms: 0.0, // From page_load events
            sample_count: vitals.len() as u64,
            good_percent: *ratings.get("good").unwrap_or(&0) as f64 / total * 100.0,
            needs_improvement_percent: *ratings.get("needs-improvement").unwrap_or(&0) as f64 / total * 100.0,
            poor_percent: *ratings.get("poor").unwrap_or(&0) as f64 / total * 100.0,
        }
    }

    pub fn total_events(&self) -> usize { self.events.read().len() }
    pub fn total_sessions(&self) -> usize { self.sessions.read().len() }
}

fn percentile_75(vals: &mut Vec<f64>) -> f64 {
    if vals.is_empty() { return 0.0; }
    vals.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
    let idx = ((vals.len() as f64 * 0.75) as usize).min(vals.len() - 1);
    vals[idx]
}

/// POST /v1/rum — Ingest RUM events from browser SDK.
pub async fn ingest_rum(
    State(state): State<Arc<ApiState>>,
    Json(events): Json<Vec<RumEvent>>,
) -> impl IntoResponse {
    let count = events.len();

    // Feed error-typed events into the ErrorTracker so they show up in
    // the issues dashboard. We do this BEFORE handing the vec to the
    // store so we can read each event without cloning the whole list.
    // RUM SDKs don't currently post a separate error endpoint, so this
    // is the entry point for all browser-side exceptions.
    let mut tracked = 0usize;
    for ev in &events {
        if let Some(call) = error_call_from_rum_event(ev) {
            state.error_tracker.track_with(
                &call.error_type,
                &call.message,
                &call.service,
                call.stack.as_deref(),
                &call.ctx,
            );
            tracked += 1;
        }
    }
    if tracked > 0 {
        info!("RUM: forwarded {} error event(s) to ErrorTracker", tracked);
    }

    state.rum_store.ingest(events);
    info!("RUM: Ingested {} browser events", count);
    StandardResponse::ok(serde_json::json!({"ingested": count}), "RUM events ingested.")
}

/// Materialized form of an error event's tracker call. Extracted into a
/// struct so the handler can run the (potentially blocking) `track_with`
/// in a single line, and so the extraction is unit-testable.
struct ErrorTrackCall {
    error_type: String,
    message: String,
    service: String,
    stack: Option<String>,
    ctx: crate::analysis::error_tracking::TrackContext,
}

/// Extract the tracker payload from a RUM event, returning `None` for
/// non-error events. The browser SDK posts errors under
/// `event_type = "error"` with `error_*` fields flattened into `data`.
fn error_call_from_rum_event(ev: &RumEvent) -> Option<ErrorTrackCall> {
    if ev.event_type != "error" {
        return None;
    }
    let message = ev
        .data
        .get("error_message")
        .and_then(|v| v.as_str())
        .unwrap_or("(no message)")
        .to_string();
    let error_type = ev
        .data
        .get("error_type")
        .and_then(|v| v.as_str())
        .unwrap_or("Error")
        .to_string();
    let stack = ev
        .data
        .get("error_stack")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());

    // Browsers don't really have a "service" — use application_id so the
    // dashboard can group by app the way it would by backend service.
    let service = if ev.application_id.is_empty() {
        "browser".to_string()
    } else {
        ev.application_id.clone()
    };

    let mut ctx = crate::analysis::error_tracking::TrackContext::default();
    ctx.release = ev.app_version.clone().unwrap_or_default();
    ctx.environment = ev.environment.clone().unwrap_or_default();
    ctx.session_id = ev.session_id.clone();
    ctx.user_id = ev.user_id.clone().unwrap_or_default();
    ctx.url = ev.url.clone().unwrap_or_default();
    ctx.user_agent = ev.user_agent.clone().unwrap_or_default();
    ctx.project_id = ev.application_id.clone();

    // Browser breadcrumbs (the last ~30 clicks/console/http events
    // before the error). Carried opaquely as a tag so the error
    // tracker doesn't need to know the schema; the dashboard reads
    // it back from `tags["breadcrumbs"]`.
    if let Some(crumbs) = ev.data.get("breadcrumbs") {
        if !crumbs.is_null() {
            ctx.tags.insert("breadcrumbs".to_string(), crumbs.to_string());
        }
    }

    Some(ErrorTrackCall {
        error_type,
        message,
        service,
        stack,
        ctx,
    })
}

/// GET /v1/rum/sessions — List RUM sessions.
pub async fn list_rum_sessions(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let app = params.get("application_id").map(|s| s.as_str());
    let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(50);
    let sessions = state.rum_store.get_sessions(app, limit);
    StandardResponse::ok(serde_json::to_value(&sessions).unwrap_or_default(), "RUM sessions.")
}

/// GET /v1/rum/vitals — Get Web Vitals summary.
pub async fn get_web_vitals(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let app_id = params.get("application_id").map(|s| s.as_str()).unwrap_or("default");
    let vitals = state.rum_store.get_vitals(app_id);
    StandardResponse::ok(serde_json::to_value(&vitals).unwrap_or_default(), "Web Vitals.")
}

/// GET /v1/rum/stats — Get RUM overview stats.
pub async fn rum_stats(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    StandardResponse::ok(serde_json::json!({
        "total_events": state.rum_store.total_events(),
        "total_sessions": state.rum_store.total_sessions(),
    }), "RUM stats.")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_event(event_type: &str, session: &str, app: &str, ts_us: u64) -> RumEvent {
        RumEvent {
            event_type: event_type.to_string(),
            timestamp_us: ts_us,
            session_id: session.to_string(),
            page_view_id: None,
            application_id: app.to_string(),
            environment: None,
            app_version: None,
            user_id: None,
            user_email: None,
            url: Some("https://example.com/".to_string()),
            user_agent: Some("test-agent".to_string()),
            screen_width: None,
            screen_height: None,
            viewport_width: None,
            viewport_height: None,
            data: HashMap::new(),
        }
    }

    fn mk_vital(session: &str, app: &str, name: &str, value: f64, rating: &str) -> RumEvent {
        let mut e = mk_event("vital", session, app, 1);
        e.data.insert("vital_name".to_string(), serde_json::json!(name));
        e.data.insert("vital_value".to_string(), serde_json::json!(value));
        e.data.insert("vital_rating".to_string(), serde_json::json!(rating));
        e
    }

    #[test]
    fn ingest_empty_vec_no_effect() {
        let store = RumStore::new();
        store.ingest(vec![]);
        assert_eq!(store.total_sessions(), 0);
        assert_eq!(store.total_events(), 0);
    }

    #[test]
    fn ingest_single_view_creates_session() {
        let store = RumStore::new();
        store.ingest(vec![mk_event("view", "s1", "app", 100)]);
        assert_eq!(store.total_sessions(), 1);
        assert_eq!(store.total_events(), 1);
    }

    #[test]
    fn ingest_tracks_view_count() {
        let store = RumStore::new();
        store.ingest(vec![
            mk_event("view", "s1", "app", 100),
            mk_event("view", "s1", "app", 200),
            mk_event("view", "s1", "app", 300),
        ]);
        let sessions = store.get_sessions(None, 10);
        assert_eq!(sessions[0].page_view_count, 3);
    }

    #[test]
    fn ingest_tracks_error_count() {
        let store = RumStore::new();
        store.ingest(vec![
            mk_event("error", "s1", "app", 100),
            mk_event("error", "s1", "app", 200),
            mk_event("view", "s1", "app", 300),
        ]);
        let sessions = store.get_sessions(None, 10);
        assert_eq!(sessions[0].error_count, 2);
        assert_eq!(sessions[0].page_view_count, 1);
    }

    #[test]
    fn ingest_tracks_action_count() {
        let store = RumStore::new();
        store.ingest(vec![
            mk_event("action", "s1", "app", 100),
            mk_event("action", "s1", "app", 200),
        ]);
        let sessions = store.get_sessions(None, 10);
        assert_eq!(sessions[0].action_count, 2);
    }

    #[test]
    fn ingest_computes_duration() {
        let store = RumStore::new();
        store.ingest(vec![
            mk_event("view", "s1", "app", 1_000_000),
            mk_event("view", "s1", "app", 5_000_000),
        ]);
        let sessions = store.get_sessions(None, 10);
        // 4 seconds = 4000 ms
        assert_eq!(sessions[0].duration_ms, 4000);
    }

    #[test]
    fn get_sessions_filters_by_application() {
        let store = RumStore::new();
        store.ingest(vec![
            mk_event("view", "s1", "appA", 100),
            mk_event("view", "s2", "appB", 200),
        ]);
        assert_eq!(store.get_sessions(Some("appA"), 10).len(), 1);
        assert_eq!(store.get_sessions(Some("appB"), 10).len(), 1);
        assert_eq!(store.get_sessions(Some("missing"), 10).len(), 0);
    }

    #[test]
    fn get_sessions_respects_limit() {
        let store = RumStore::new();
        for i in 0..5 {
            store.ingest(vec![mk_event("view", &format!("s{}", i), "app", i)]);
        }
        assert_eq!(store.get_sessions(None, 2).len(), 2);
    }

    #[test]
    fn get_sessions_sorted_by_last_activity_desc() {
        let store = RumStore::new();
        store.ingest(vec![mk_event("view", "old", "app", 100)]);
        store.ingest(vec![mk_event("view", "new", "app", 500)]);
        let sessions = store.get_sessions(None, 10);
        assert_eq!(sessions[0].session_id, "new");
    }

    #[test]
    fn total_sessions_unique_by_session_id() {
        let store = RumStore::new();
        store.ingest(vec![
            mk_event("view", "s1", "app", 100),
            mk_event("view", "s1", "app", 200),
            mk_event("view", "s2", "app", 300),
        ]);
        assert_eq!(store.total_sessions(), 2);
    }

    #[test]
    fn total_events_counts_all_ingested() {
        let store = RumStore::new();
        store.ingest(vec![
            mk_event("view", "s1", "app", 1),
            mk_event("error", "s1", "app", 2),
            mk_event("action", "s1", "app", 3),
        ]);
        assert_eq!(store.total_events(), 3);
    }

    #[test]
    fn get_vitals_empty_for_unknown_app() {
        let store = RumStore::new();
        let v = store.get_vitals("nope");
        assert_eq!(v.sample_count, 0);
        assert_eq!(v.lcp_p75_ms, 0.0);
    }

    #[test]
    fn get_vitals_aggregates_lcp() {
        let store = RumStore::new();
        store.ingest(vec![
            mk_vital("s1", "app", "LCP", 1000.0, "good"),
            mk_vital("s1", "app", "LCP", 2000.0, "good"),
            mk_vital("s1", "app", "LCP", 3000.0, "good"),
            mk_vital("s1", "app", "LCP", 4000.0, "poor"),
        ]);
        let v = store.get_vitals("app");
        assert_eq!(v.sample_count, 4);
        assert!(v.lcp_p75_ms >= 3000.0);
    }

    // --- F-105: RUM error event → ErrorTracker extraction -----------------

    fn mk_error(
        app: &str,
        release: Option<&str>,
        message: &str,
        error_type: &str,
        stack: Option<&str>,
    ) -> RumEvent {
        let mut e = mk_event("error", "s1", app, 1);
        e.app_version = release.map(|s| s.to_string());
        e.url = Some("https://x/y".into());
        e.environment = Some("production".into());
        e.user_id = Some("u".into());
        e.data
            .insert("error_message".into(), serde_json::json!(message));
        e.data
            .insert("error_type".into(), serde_json::json!(error_type));
        if let Some(s) = stack {
            e.data.insert("error_stack".into(), serde_json::json!(s));
        }
        e
    }

    #[test]
    fn error_extractor_returns_none_for_non_error_event() {
        let ev = mk_event("view", "s1", "app", 1);
        assert!(error_call_from_rum_event(&ev).is_none());
    }

    #[test]
    fn error_extractor_pulls_message_type_and_stack() {
        let ev = mk_error("app1", Some("1.4.2"), "x is undefined", "TypeError", Some("at f (a.js:1:2)"));
        let call = error_call_from_rum_event(&ev).expect("error event extracted");
        assert_eq!(call.error_type, "TypeError");
        assert_eq!(call.message, "x is undefined");
        assert_eq!(call.stack.as_deref(), Some("at f (a.js:1:2)"));
        assert_eq!(call.service, "app1");
    }

    #[test]
    fn error_extractor_threads_app_version_into_release() {
        let ev = mk_error("app1", Some("9.9.9"), "boom", "E", None);
        let call = error_call_from_rum_event(&ev).unwrap();
        assert_eq!(call.ctx.release, "9.9.9");
        assert_eq!(call.ctx.environment, "production");
        assert_eq!(call.ctx.session_id, "s1");
        assert_eq!(call.ctx.project_id, "app1");
    }

    #[test]
    fn error_extractor_handles_missing_release_as_empty() {
        let ev = mk_error("app1", None, "boom", "E", None);
        let call = error_call_from_rum_event(&ev).unwrap();
        assert!(call.ctx.release.is_empty());
    }

    #[test]
    fn error_extractor_falls_back_when_fields_missing() {
        let mut ev = mk_event("error", "s1", "app1", 1);
        // No error_message / error_type / error_stack in data
        let call = error_call_from_rum_event(&ev).unwrap();
        assert_eq!(call.error_type, "Error"); // fallback
        assert_eq!(call.message, "(no message)"); // fallback
        assert!(call.stack.is_none());

        ev.application_id = String::new();
        let call = error_call_from_rum_event(&ev).unwrap();
        assert_eq!(call.service, "browser"); // empty application_id fallback
    }
}
