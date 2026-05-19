//! Canonical-path ingestion dispatchers.
//!
//! The goal: one fluxx URL per signal that any modern telemetry tool
//! already knows how to talk to. OTel SDKs, the Datadog agent's OTLP
//! exporter, Grafana Alloy, the Splunk OTel collector — all of them
//! default to `OTEL_EXPORTER_OTLP_ENDPOINT/v1/{traces,metrics,logs}`.
//! By hosting OTLP on those exact paths we become a drop-in target
//! with zero SDK-config changes on the user's side.
//!
//! For `/v1/logs` there's an unavoidable collision with the
//! fluxx-native `Vec<LogEntry>` shape we shipped earlier. Rather than
//! pick one and break the other, this module dispatches based on the
//! body's structural shape:
//!
//!   * object with `resourceLogs` → OTLP HTTP/JSON
//!   * top-level JSON array       → fluxx-native LogEntry batch
//!
//! Both paths converge on the same underlying log store.

use crate::{ApiState, StandardResponse};
use axum::{extract::State, response::IntoResponse, Json};
use fluxx_core::models::log_entry::LogEntry;
use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use serde_json::Value;
use std::sync::Arc;

/// POST /v1/logs — accepts either OTLP HTTP/JSON or a fluxx-native
/// `Vec<LogEntry>` based on body shape. Branches by inspecting the
/// parsed JSON value rather than the `Content-Type` header so the
/// caller doesn't have to set anything special.
pub async fn ingest_logs(
    State(state): State<Arc<ApiState>>,
    Json(body): Json<Value>,
) -> axum::response::Response {
    // OTLP HTTP/JSON always has a top-level object with `resourceLogs`.
    if body.is_object() && body.get("resourceLogs").is_some() {
        return match serde_json::from_value::<ExportLogsServiceRequest>(body) {
            Ok(req) => {
                let count = crate::otlp_http_handlers::write_otlp_logs(&state, req).await;
                StandardResponse::ok(
                    serde_json::json!({ "accepted_logs": count, "wire": "otlp" }),
                    "Logs ingested successfully.",
                )
                .into_response()
            }
            Err(e) => StandardResponse::error(
                Value::Null,
                &format!("Invalid OTLP logs body: {}", e),
                "INVALID_BODY",
                Vec::new(),
            )
            .into_response(),
        };
    }

    // Fluxx-native is a top-level array of LogEntry.
    if body.is_array() {
        return match serde_json::from_value::<Vec<LogEntry>>(body) {
            Ok(entries) => {
                let count = crate::logs::insert_log_entries(&state, entries).await;
                StandardResponse::ok(
                    serde_json::json!({ "ingested": count, "wire": "native" }),
                    "Log entries ingested successfully.",
                )
                .into_response()
            }
            Err(e) => StandardResponse::error(
                Value::Null,
                &format!("Invalid LogEntry array: {}", e),
                "INVALID_BODY",
                Vec::new(),
            )
            .into_response(),
        };
    }

    StandardResponse::error(
        Value::Null,
        "POST /v1/logs accepts either OTLP HTTP/JSON (object with `resourceLogs`) \
         or a fluxx-native JSON array of LogEntry.",
        "INVALID_BODY",
        Vec::new(),
    )
    .into_response()
}

#[cfg(test)]
mod tests {
    //! Shape-only unit tests. The full dispatcher hits the log store,
    //! which needs the whole ApiState wiring — that's covered by the
    //! e2e suite in `tests/`. Here we just pin the routing decision.

    use serde_json::json;

    #[test]
    fn otlp_shape_is_object_with_resourcelogs() {
        let v = json!({
            "resourceLogs": [{
                "resource": {"attributes": []},
                "scopeLogs": [{"scope": {"name": "test"}, "logRecords": []}]
            }]
        });
        assert!(v.is_object());
        assert!(v.get("resourceLogs").is_some());
    }

    #[test]
    fn native_shape_is_array() {
        let v = json!([
            {"id": "1", "timestamp_us": 0, "level": "info", "message": "x"}
        ]);
        assert!(v.is_array());
        assert!(!v.is_object());
    }

    #[test]
    fn ambiguous_object_fails_cleanly() {
        // A bare object without `resourceLogs` is neither OTLP nor native.
        let v = json!({"hello": "world"});
        assert!(v.is_object());
        assert!(v.get("resourceLogs").is_none());
        assert!(!v.is_array());
    }
}
