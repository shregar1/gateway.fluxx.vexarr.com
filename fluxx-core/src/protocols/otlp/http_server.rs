//! Canonical OTLP HTTP receiver.
//!
//! Mounts the OTel-spec paths every SDK expects:
//!
//!   POST /v1/traces
//!   POST /v1/metrics
//!   POST /v1/logs
//!
//! Each handler accepts BOTH content types the OTel spec allows:
//!
//!   • `application/x-protobuf`  — what most language SDKs send by
//!     default (Python, Node, Go, Java, Rust)
//!   • `application/json`        — JSON-encoded protobuf, used by some
//!     debugging clients and by our existing /v1/otlp/* aliases
//!
//! This is bound to a SEPARATE port from the agent's main HTTP API
//! (port 4318 by default vs 7007 for the dashboard/admin API) so a
//! standard `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318` Just
//! Works without the user knowing anything about Fluxx — including
//! the prompt we ship to other-project agents.
//!
//! The gRPC receiver on 4317 stays exactly as it was; this is purely
//! additive.

use std::sync::Arc;

use axum::{
    body::Bytes,
    extract::State,
    http::{header, HeaderMap, StatusCode},
    response::IntoResponse,
    routing::post,
    Router,
};
use prost::Message;
use tokio::sync::broadcast;
use tracing::{debug, error, info, warn};

use opentelemetry_proto::tonic::collector::logs::v1::ExportLogsServiceRequest;
use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;

use crate::models::event::Event;
use crate::models::log_entry::LogEntry;
use crate::models::metric::Metric;
use crate::protocols::otlp::convert;
use crate::storage::event_store::EventStore;
use crate::storage::log_store::LogStore;
use crate::storage::metric_store::MetricStore;

/// Lightweight state — just the stores + buses. Distinct from
/// `ApiState` so the OTLP HTTP server doesn't pull in the whole
/// dashboard surface (sessions, audit log, etc.).
#[derive(Clone)]
pub struct OtlpHttpState {
    pub event_store: Arc<dyn EventStore>,
    pub event_bus: broadcast::Sender<Arc<Event>>,
    pub metric_store: Arc<dyn MetricStore>,
    pub metric_bus: broadcast::Sender<Arc<Metric>>,
    pub log_store: Arc<dyn LogStore>,
    pub log_bus: broadcast::Sender<Arc<LogEntry>>,
}

/// Bind axum on `0.0.0.0:port` and serve `/v1/{traces,metrics,logs}`.
/// Runs forever; call from a `tokio::spawn`.
pub async fn start_otlp_http_server(
    port: u16,
    state: OtlpHttpState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let app = Router::new()
        .route("/v1/traces", post(ingest_traces))
        .route("/v1/metrics", post(ingest_metrics))
        .route("/v1/logs", post(ingest_logs))
        // Health probe — useful when wiring this up; doesn't bother
        // with auth because the listener is meant to be loopback-only.
        .route("/healthz", axum::routing::get(|| async { "ok" }))
        .with_state(state);

    let addr = std::net::SocketAddr::from(([0, 0, 0, 0], port));
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(port, "OTLP HTTP receiver listening (canonical /v1/{{traces,metrics,logs}})");
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

// ─── Content-type dispatcher ───────────────────────────────────────────────

/// Decode the request body into a protobuf message of type `M`.
///
/// Handles the three encodings real-world OTel SDKs send:
///   • `Content-Type: application/x-protobuf`  → prost decode
///   • `Content-Type: application/json`        → serde_json decode
///   • `Content-Encoding: gzip`                → ungzip first, then dispatch
///
/// SDKs default to `application/x-protobuf` and several (notably the
/// JS HTTP exporter when `compression: 'gzip'` is set) may apply
/// gzip on top. Missing gzip handling silently 400'd every JS export
/// for ~10 minutes during initial bring-up — the lesson is to be
/// generous in what we accept.
fn decode_body<M>(headers: &HeaderMap, body: &Bytes) -> Result<M, (StatusCode, String)>
where
    M: Message + Default + serde::de::DeserializeOwned,
{
    let ct = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("application/x-protobuf");
    let ce = headers
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_lowercase();

    // Optionally ungzip first — produces a fresh Vec<u8> we can decode.
    let decoded_bytes: std::borrow::Cow<[u8]> = if ce.contains("gzip") {
        use std::io::Read;
        let mut gz = flate2::read::GzDecoder::new(body.as_ref());
        let mut out = Vec::with_capacity(body.len() * 4);
        if let Err(e) = gz.read_to_end(&mut out) {
            warn!(error = %e, body_len = body.len(), "OTLP HTTP: gzip decode failed");
            return Err((
                StatusCode::BAD_REQUEST,
                format!("gzip decode failed: {}", e),
            ));
        }
        std::borrow::Cow::Owned(out)
    } else {
        std::borrow::Cow::Borrowed(body.as_ref())
    };

    // Strip parameters (e.g. "application/json; charset=utf-8").
    let ct_main = ct.split(';').next().unwrap_or("").trim();

    match ct_main {
        "application/x-protobuf" | "" => {
            M::decode(decoded_bytes.as_ref()).map_err(|e| {
                warn!(
                    error = %e,
                    body_len = decoded_bytes.len(),
                    first16_hex = %hex_prefix(decoded_bytes.as_ref(), 16),
                    "OTLP HTTP: protobuf decode failed"
                );
                (
                    StatusCode::BAD_REQUEST,
                    format!("protobuf decode failed: {}", e),
                )
            })
        }
        "application/json" => serde_json::from_slice::<M>(decoded_bytes.as_ref()).map_err(|e| {
            warn!(error = %e, "OTLP HTTP: json decode failed");
            (StatusCode::BAD_REQUEST, format!("json decode failed: {}", e))
        }),
        _ => {
            warn!(content_type = %ct, "OTLP HTTP: unsupported content type");
            Err((
                StatusCode::UNSUPPORTED_MEDIA_TYPE,
                "expected application/x-protobuf or application/json".to_string(),
            ))
        }
    }
}

fn hex_prefix(b: &[u8], n: usize) -> String {
    let take = b.iter().take(n);
    let mut s = String::with_capacity(n * 2);
    for byte in take {
        use std::fmt::Write;
        let _ = write!(&mut s, "{:02x}", byte);
    }
    s
}

// ─── Tests ──────────────────────────────────────────────────────────────────
//
// decode_body has bitten us once already: gzipped payloads silently 400'd
// every export until we added the Content-Encoding branch. These tests
// pin the matrix of (content-type × encoding × body) so a future refactor
// that breaks gzip — or assumes Content-Type is always present — fails
// loud at `cargo test` instead of in production at 3 am.

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Bytes;
    use axum::http::HeaderMap;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use opentelemetry_proto::tonic::collector::trace::v1::ExportTraceServiceRequest;
    use prost::Message;
    use std::io::Write;

    /// Construct a HeaderMap. Pass `&[]` for "no headers", or
    /// `&[("content-type", "application/x-protobuf"), ...]` for specific ones.
    fn headers(kv: &[(&str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in kv {
            h.insert(
                axum::http::HeaderName::from_bytes(k.as_bytes()).unwrap(),
                v.parse().unwrap(),
            );
        }
        h
    }

    /// An empty `ExportTraceServiceRequest` serialized as protobuf is just
    /// zero bytes — proto3 represents "all fields default" as empty wire.
    fn empty_proto_bytes() -> Vec<u8> {
        let msg = ExportTraceServiceRequest::default();
        msg.encode_to_vec()
    }

    fn gzip(b: &[u8]) -> Vec<u8> {
        let mut e = GzEncoder::new(Vec::new(), Compression::default());
        e.write_all(b).unwrap();
        e.finish().unwrap()
    }

    // ── protobuf branch ──────────────────────────────────────────────────

    #[test]
    fn decode_body_accepts_protobuf_with_explicit_content_type() {
        let body = Bytes::from(empty_proto_bytes());
        let h = headers(&[("content-type", "application/x-protobuf")]);
        let result: Result<ExportTraceServiceRequest, _> = decode_body(&h, &body);
        assert!(result.is_ok(), "protobuf path returned {:?}", result.err());
    }

    #[test]
    fn decode_body_defaults_to_protobuf_when_content_type_missing() {
        // Some SDKs omit Content-Type entirely. The OTel spec says default
        // is application/x-protobuf, so we do the same. A regression that
        // returned 415 here would break perfectly valid clients.
        let body = Bytes::from(empty_proto_bytes());
        let h = headers(&[]);
        let result: Result<ExportTraceServiceRequest, _> = decode_body(&h, &body);
        assert!(result.is_ok(), "missing content-type path returned {:?}", result.err());
    }

    #[test]
    fn decode_body_rejects_malformed_protobuf_with_400() {
        let body = Bytes::from_static(b"not-real-protobuf-bytes");
        let h = headers(&[("content-type", "application/x-protobuf")]);
        let result: Result<ExportTraceServiceRequest, _> = decode_body(&h, &body);
        let (status, _msg) = result.expect_err("expected decode failure");
        assert_eq!(status.as_u16(), 400);
    }

    // ── gzip branch ──────────────────────────────────────────────────────

    #[test]
    fn decode_body_ungzips_protobuf_when_content_encoding_is_gzip() {
        // The whole reason this test exists: SDKs that enable gzip used
        // to silently 400 because we didn't ungzip. Pin the happy path.
        let body = Bytes::from(gzip(&empty_proto_bytes()));
        let h = headers(&[
            ("content-type", "application/x-protobuf"),
            ("content-encoding", "gzip"),
        ]);
        let result: Result<ExportTraceServiceRequest, _> = decode_body(&h, &body);
        assert!(result.is_ok(), "gzip path returned {:?}", result.err());
    }

    #[test]
    fn decode_body_rejects_gzip_with_corrupt_body_as_400() {
        // If Content-Encoding says gzip but the body isn't actually gzip,
        // we want a 400 with a "gzip decode failed" message, not a panic
        // or 500.
        let body = Bytes::from_static(b"plain-bytes-not-actually-gzip");
        let h = headers(&[
            ("content-type", "application/x-protobuf"),
            ("content-encoding", "gzip"),
        ]);
        let result: Result<ExportTraceServiceRequest, _> = decode_body(&h, &body);
        let (status, msg) = result.expect_err("expected gzip-decode failure");
        assert_eq!(status.as_u16(), 400);
        assert!(msg.contains("gzip"), "expected error mentions gzip: {:?}", msg);
    }

    #[test]
    fn decode_body_is_case_insensitive_for_content_encoding() {
        // `Content-Encoding: GZIP` (uppercase) is just as valid as lowercase.
        let body = Bytes::from(gzip(&empty_proto_bytes()));
        let h = headers(&[
            ("content-type", "application/x-protobuf"),
            ("content-encoding", "GZIP"),
        ]);
        let result: Result<ExportTraceServiceRequest, _> = decode_body(&h, &body);
        assert!(result.is_ok(), "uppercase GZIP path returned {:?}", result.err());
    }

    // ── content-type variants ────────────────────────────────────────────

    #[test]
    fn decode_body_strips_content_type_parameters() {
        // "application/json; charset=utf-8" must dispatch to the JSON
        // branch, not be rejected as unsupported. Real curl + many SDKs
        // send the charset parameter. We send a minimal-but-valid
        // ExportTraceServiceRequest body so we're testing the dispatch
        // logic, not the JSON deserialization correctness (which has
        // its own dedicated tests).
        let body = Bytes::from_static(br#"{"resourceSpans":[]}"#);
        let h = headers(&[("content-type", "application/json; charset=utf-8")]);
        let result: Result<ExportTraceServiceRequest, _> = decode_body(&h, &body);
        assert!(result.is_ok(), "json with charset returned {:?}", result.err());
    }

    #[test]
    fn decode_body_accepts_plain_json() {
        // The "no charset" branch of the JSON path. A separate test
        // from the strips-params one above so a regression that breaks
        // only one branch fails only that branch's test (clearer signal).
        let body = Bytes::from_static(br#"{"resourceSpans":[]}"#);
        let h = headers(&[("content-type", "application/json")]);
        let result: Result<ExportTraceServiceRequest, _> = decode_body(&h, &body);
        assert!(result.is_ok(), "plain json returned {:?}", result.err());
    }

    #[test]
    fn decode_body_rejects_unsupported_content_type_with_415() {
        let body = Bytes::from_static(b"<xml />");
        let h = headers(&[("content-type", "application/xml")]);
        let result: Result<ExportTraceServiceRequest, _> = decode_body(&h, &body);
        let (status, _) = result.expect_err("expected 415");
        assert_eq!(status.as_u16(), 415);
    }

    // ── hex_prefix (used for diagnostic logs) ────────────────────────────

    #[test]
    fn hex_prefix_renders_first_n_bytes_lowercase() {
        assert_eq!(hex_prefix(&[0xde, 0xad, 0xbe, 0xef], 2), "dead");
        assert_eq!(hex_prefix(&[0xde, 0xad, 0xbe, 0xef], 4), "deadbeef");
    }

    #[test]
    fn hex_prefix_handles_shorter_than_requested_input() {
        // Don't panic when caller asks for more bytes than available.
        assert_eq!(hex_prefix(&[0x01, 0x02], 16), "0102");
        assert_eq!(hex_prefix(&[], 16), "");
    }
}

// ─── Handlers ──────────────────────────────────────────────────────────────

async fn ingest_traces(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let req: ExportTraceServiceRequest = match decode_body(&headers, &body) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    let events = convert::otlp_spans_to_events(&req.resource_spans);
    let count = events.len();
    for event in events {
        let arc = Arc::new(event.clone());
        let _ = state.event_bus.send(arc);
        if let Err(e) = state.event_store.insert(event).await {
            error!("OTLP HTTP trace store error: {}", e);
        }
    }
    debug!(spans = count, "OTLP HTTP: ingested traces");
    // OTel spec wants an empty ExportTraceServiceResponse on success.
    // Returning protobuf-encoded empty body is fussy; SDKs accept any
    // 200 with the right content-type signal, so we keep it simple.
    (StatusCode::OK, [(header::CONTENT_TYPE, "application/x-protobuf")], Vec::<u8>::new())
        .into_response()
}

async fn ingest_metrics(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let req: ExportMetricsServiceRequest = match decode_body(&headers, &body) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    let metrics = convert::otlp_metrics_to_metrics(&req.resource_metrics);
    let count = metrics.len();
    for metric in metrics {
        let arc = Arc::new(metric.clone());
        let _ = state.metric_bus.send(arc);
        if let Err(e) = state.metric_store.insert(metric).await {
            error!("OTLP HTTP metric store error: {}", e);
        }
    }
    debug!(metrics = count, "OTLP HTTP: ingested metrics");
    (StatusCode::OK, [(header::CONTENT_TYPE, "application/x-protobuf")], Vec::<u8>::new())
        .into_response()
}

async fn ingest_logs(
    State(state): State<OtlpHttpState>,
    headers: HeaderMap,
    body: Bytes,
) -> impl IntoResponse {
    let req: ExportLogsServiceRequest = match decode_body(&headers, &body) {
        Ok(r) => r,
        Err(e) => return e.into_response(),
    };
    let entries = convert::otlp_logs_to_entries(&req.resource_logs);
    let count = entries.len();
    for entry in entries {
        let arc = Arc::new(entry.clone());
        let _ = state.log_bus.send(arc);
        if let Err(e) = state.log_store.insert(entry).await {
            error!("OTLP HTTP log store error: {}", e);
        }
    }
    debug!(logs = count, "OTLP HTTP: ingested logs");
    (StatusCode::OK, [(header::CONTENT_TYPE, "application/x-protobuf")], Vec::<u8>::new())
        .into_response()
}
