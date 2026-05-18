use crate::api::{ApiState, StandardResponse};
use crate::models::log_entry::{LogEntry, LogLevel};
use crate::storage::log_store::LogQuery;
use axum::{
    extract::{ws::{WebSocket, WebSocketUpgrade, Message}, State, Query},
    response::IntoResponse,
    Json,
};
use std::sync::Arc;
use std::collections::HashMap;
use tracing::error;

/// POST /v1/logs — Ingest an array of log entries.
pub async fn ingest_logs(
    State(state): State<Arc<ApiState>>,
    Json(entries): Json<Vec<LogEntry>>,
) -> impl IntoResponse {
    let count = entries.len();
    for entry in entries {
        if let Err(e) = state.log_store.insert(entry).await {
            error!("Failed to insert log entry: {}", e);
        }
    }
    StandardResponse::ok(
        serde_json::json!({ "ingested": count }),
        "Log entries ingested successfully.",
    )
}

/// GET /v1/logs — Query log entries with filtering.
pub async fn query_logs(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    let level = params.get("level").and_then(|l| match l.to_lowercase().as_str() {
        "trace" => Some(LogLevel::Trace),
        "debug" => Some(LogLevel::Debug),
        "info" => Some(LogLevel::Info),
        "warn" => Some(LogLevel::Warn),
        "error" => Some(LogLevel::Error),
        "fatal" => Some(LogLevel::Fatal),
        _ => None,
    });

    let query = LogQuery {
        q: params.get("q").cloned(),
        service: params.get("service").cloned(),
        level,
        trace_id: params.get("trace_id").cloned(),
        from_us: params.get("from").and_then(|v| v.parse().ok()),
        to_us: params.get("to").and_then(|v| v.parse().ok()),
        limit: params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(100),
        offset: params.get("offset").and_then(|v| v.parse().ok()).unwrap_or(0),
    };

    match state.log_store.query(query).await {
        Ok(entries) => {
            let data = serde_json::to_value(&entries).unwrap_or_default();
            StandardResponse::ok(data, "Log query results.")
        }
        Err(e) => StandardResponse::error(
            serde_json::json!({}),
            &format!("Log query failed: {}", e),
            "QUERY_ERROR",
            vec![],
        ),
    }
}

/// GET /v1/logs/stream — WebSocket live tail for logs.
pub async fn stream_logs_ws(
    ws: WebSocketUpgrade,
    State(state): State<Arc<ApiState>>,
    Query(params): Query<HashMap<String, String>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_log_socket(socket, state, params))
}

async fn handle_log_socket(
    mut socket: WebSocket,
    state: Arc<ApiState>,
    filters: HashMap<String, String>,
) {
    let mut rx = state.log_bus.subscribe();

    let target_service = filters.get("service").cloned();
    let target_level = filters.get("level").and_then(|l| match l.to_lowercase().as_str() {
        "trace" => Some(LogLevel::Trace),
        "debug" => Some(LogLevel::Debug),
        "info" => Some(LogLevel::Info),
        "warn" => Some(LogLevel::Warn),
        "error" => Some(LogLevel::Error),
        "fatal" => Some(LogLevel::Fatal),
        _ => None,
    });
    let target_query = filters.get("q").cloned();

    while let Ok(entry) = rx.recv().await {
        if let Some(ref svc) = target_service {
            if entry.service.as_ref() != Some(svc) {
                continue;
            }
        }
        if let Some(ref level) = target_level {
            if entry.level != *level {
                continue;
            }
        }
        if let Some(ref q) = target_query {
            if !entry.message.to_lowercase().contains(&q.to_lowercase()) {
                continue;
            }
        }

        let msg = serde_json::to_string(&*entry).unwrap_or_default();
        if socket.send(Message::Text(msg.into())).await.is_err() {
            break;
        }
    }
}
