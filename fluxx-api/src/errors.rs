//! HTTP routes for the ErrorTracker. The handlers are thin wrappers
//! around the async `query_*` API on `ErrorTracker`, which transparently
//! routes to ClickHouse when a read store is configured and otherwise
//! synthesizes results from the in-memory map. The dashboard's
//! ErrorsView and ReleasesView consume these endpoints.
//!
//! Routes:
//!   GET  /v1/errors                — list error groups
//!   GET  /v1/errors/top            — top N by count
//!   GET  /v1/errors/trend          — 24-bucket hourly histogram
//!   GET  /v1/errors/stats          — total groups + occurrences
//!   GET  /v1/errors/:fingerprint   — one group
//!   POST /v1/errors/:fingerprint/resolve  — set in-memory status to Resolved
//!   POST /v1/errors/:fingerprint/mute     — set in-memory status to Muted

use crate::api::{ApiState, StandardResponse};
use axum::{
    extract::{Path, Query, State},
    response::IntoResponse,
};
use serde::Deserialize;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Debug, Deserialize, Default)]
pub struct WindowQuery {
    #[serde(default = "default_since_hours")]
    pub since_hours: u32,
}

fn default_since_hours() -> u32 {
    24
}

/// GET /v1/errors?since_hours=24
pub async fn list_errors(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<WindowQuery>,
) -> impl IntoResponse {
    let groups = state.error_tracker.query_groups(q.since_hours).await;
    StandardResponse::ok(
        serde_json::to_value(&groups).unwrap_or_default(),
        "Error groups.",
    )
}

/// GET /v1/errors/top?n=10&since_hours=24
pub async fn top_errors(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let n: usize = params
        .get("n")
        .and_then(|v| v.parse().ok())
        .unwrap_or(10)
        .min(500);
    let since_hours: u32 = params
        .get("since_hours")
        .and_then(|v| v.parse().ok())
        .unwrap_or(24);
    let groups = state.error_tracker.query_top(n, since_hours).await;
    StandardResponse::ok(
        serde_json::to_value(&groups).unwrap_or_default(),
        "Top error groups.",
    )
}

/// GET /v1/errors/trend?since_hours=24
pub async fn errors_trend(
    State(state): State<Arc<ApiState>>,
    Query(q): Query<WindowQuery>,
) -> impl IntoResponse {
    let trend: [u64; 24] = state.error_tracker.query_trend(q.since_hours).await;
    StandardResponse::ok(
        serde_json::json!({ "buckets": trend.to_vec(), "size": 24 }),
        "Hourly error trend.",
    )
}

/// GET /v1/errors/stats
pub async fn errors_stats(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let stats = state.error_tracker.query_stats().await;
    StandardResponse::ok(
        serde_json::to_value(&stats).unwrap_or_default(),
        "Error tracker stats.",
    )
}

/// GET /v1/errors/:fingerprint
pub async fn get_error(
    State(state): State<Arc<ApiState>>,
    Path(fingerprint): Path<String>,
    Query(q): Query<WindowQuery>,
) -> impl IntoResponse {
    let group = state
        .error_tracker
        .query_group(&fingerprint, q.since_hours)
        .await;
    match group {
        Some(g) => StandardResponse::ok(
            serde_json::to_value(&g).unwrap_or_default(),
            "Error group.",
        ),
        None => StandardResponse::ok(
            serde_json::json!(null),
            "Error group not found in the requested window.",
        ),
    }
}

/// POST /v1/errors/:fingerprint/resolve
///
/// Mutates only the in-memory tracker. The canonical `resolved_in_release`
/// lifecycle lives in api.fluxx.dev's Postgres `issues` table; this
/// endpoint exists so the per-node dashboard can give immediate UX
/// feedback without a round-trip to the control plane.
pub async fn resolve_error(
    State(state): State<Arc<ApiState>>,
    Path(fingerprint): Path<String>,
) -> impl IntoResponse {
    let ok = state.error_tracker.resolve(&fingerprint);
    StandardResponse::ok(
        serde_json::json!({ "resolved": ok, "fingerprint": fingerprint }),
        if ok { "Error resolved." } else { "Error not found." },
    )
}

/// GET /v1/errors/:fingerprint/events?limit=20&since_hours=168
///
/// Page of recent events for a fingerprint, newest first. The
/// dashboard's "Recent" tab paginates through these so an operator
/// can see how the same fingerprint manifested across users / URLs.
/// `limit` clamps to [1, 200].
pub async fn list_events(
    State(state): State<Arc<ApiState>>,
    Path(fingerprint): Path<String>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let since_hours: u32 = params
        .get("since_hours")
        .and_then(|v| v.parse().ok())
        .unwrap_or(24 * 7);
    let limit: u32 = params
        .get("limit")
        .and_then(|v| v.parse().ok())
        .unwrap_or(20)
        .min(200);
    let events = state
        .error_tracker
        .query_recent_events(&fingerprint, since_hours, limit)
        .await;
    StandardResponse::ok(
        serde_json::to_value(&events).unwrap_or_default(),
        "Recent events.",
    )
}

/// GET /v1/errors/:fingerprint/latest-event
///
/// Returns the most recent ClickHouse-backed event matching the
/// fingerprint, with full frame arrays and tag map. Used by the
/// dashboard's error detail panel to render the frame walker and
/// breadcrumb timeline.
pub async fn latest_event(
    State(state): State<Arc<ApiState>>,
    Path(fingerprint): Path<String>,
    Query(q): Query<WindowQuery>,
) -> impl IntoResponse {
    let ev = state
        .error_tracker
        .query_latest_event(&fingerprint, q.since_hours)
        .await;
    match ev {
        Some(e) => StandardResponse::ok(
            serde_json::to_value(&e).unwrap_or_default(),
            "Latest event.",
        ),
        None => StandardResponse::ok(
            serde_json::json!(null),
            "No event found in the requested window.",
        ),
    }
}

/// POST /v1/errors/:fingerprint/mute
pub async fn mute_error(
    State(state): State<Arc<ApiState>>,
    Path(fingerprint): Path<String>,
) -> impl IntoResponse {
    let ok = state.error_tracker.mute(&fingerprint);
    StandardResponse::ok(
        serde_json::json!({ "muted": ok, "fingerprint": fingerprint }),
        if ok { "Error muted." } else { "Error not found." },
    )
}
