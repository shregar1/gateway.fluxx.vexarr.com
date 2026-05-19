use crate::{ApiState, StandardResponse};
use fluxx_core::models::event::Event;
use axum::{
    extract::{ws::{WebSocket, WebSocketUpgrade, Message}, Path, State, Query},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use std::sync::Arc;
use tower_http::cors::{CorsLayer, AllowOrigin};
use tracing::{info, error};

/// Bootstrap-token auth middleware.
///
/// Accepts the configured admin token via either `Authorization:
/// Bearer <token>` or `X-API-Key: <token>` (both for compatibility
/// with curl and SDKs that already use one form). Constant-time
/// compare lives in `security::bootstrap_token::verify_token`.
///
/// Carve-outs (no auth required):
///   * `/`, `/index.html`, `/assets/*`, `/favicon*`, `/robots.txt` —
///     the bundled dashboard. The dashboard itself prompts for the
///     token on the client side and attaches it to subsequent /v1/*
///     calls.
///   * `/v1/status` — health-probe endpoint hit by container orchestrators.
///   * SDK ingest endpoints: `/v1/rum`, `/v1/logs`, `/v1/events`,
///     `/v1/otlp/*`, `/v1/prom/write`. Browser RUM has no server-side
///     secret; gating these would defeat the SDK. Operators who want
///     authenticated ingest set `FLUXX_REQUIRE_INGEST_AUTH=true`.
async fn auth_middleware(
    headers: HeaderMap,
    mut request: axum::extract::Request,
    next: Next,
) -> Result<Response, StatusCode> {
    // URL-based auth: path-prefix `/k/<KEY>/...` is rewritten to its
    // canonical path here, with the extracted key handed downstream as
    // the token. Clients that cannot set headers (embedded devices,
    // `<img>` beacons, certain telemetry SDKs) can authenticate by
    // building the key into the URL. See docs/INGEST.md.
    let path_token = strip_path_token(&mut request);

    let path = request.uri().path();
    if is_public_path(path) {
        return Ok(next.run(request).await);
    }
    let ingest_optional = is_ingest_path(path)
        && std::env::var("FLUXX_REQUIRE_INGEST_AUTH").is_err();

    // Resolve token from any URL/header mechanism. Precedence:
    //   1. /k/<KEY>/ path prefix (rewritten above)
    //   2. Authorization: Bearer <KEY>
    //   3. Authorization: Basic base64(<KEY>:)   ← userinfo URL auth
    //   4. X-API-Key: <KEY>
    let token = path_token.or_else(|| extract_token(&headers));

    if let Some(t) = &token {
        if fluxx_core::security::bootstrap_token::verify_token(t) {
            return Ok(next.run(request).await);
        }
        // Token was provided but wrong — always fail closed, even on
        // ingest paths. Submitting a *bad* key is intent to authenticate;
        // we shouldn't silently let it through.
        error!(path = %path, "Unauthorized API access attempt (bad token)");
        return Err(StatusCode::UNAUTHORIZED);
    }

    // No token. Anonymous ingest is allowed unless the operator opted in.
    if ingest_optional {
        return Ok(next.run(request).await);
    }

    error!(path = %path, "Unauthorized API access attempt");
    Err(StatusCode::UNAUTHORIZED)
}

/// If the URL starts with `/k/<KEY>/<rest>`, strip the prefix in place
/// and return the key. Used so that
///   POST /k/flx_live_abc/v1/traces?x=1
/// becomes routed as
///   POST /v1/traces?x=1
/// with `flx_live_abc` extracted as the bearer token. This is the
/// "URL-only" auth mechanism for clients that cannot configure
/// arbitrary headers.
fn strip_path_token(request: &mut axum::extract::Request) -> Option<String> {
    // Pull the bits we need out of the existing URI into owned strings
    // up front, so the subsequent `request.uri_mut()` mutable borrow
    // doesn't collide with the immutable borrows of `path` / `query`.
    let (key, path_and_query) = {
        let uri = request.uri();
        let path = uri.path();
        let rest = path.strip_prefix("/k/")?;
        let slash = rest.find('/')?;
        let key = &rest[..slash];
        if key.is_empty() {
            return None;
        }
        let remaining = &rest[slash..]; // includes leading '/'
        let pq = match uri.query() {
            Some(q) => format!("{}?{}", remaining, q),
            None => remaining.to_string(),
        };
        (key.to_string(), pq)
    };

    // Rebuild the URI preserving scheme/authority where present.
    let mut parts = request.uri().clone().into_parts();
    parts.path_and_query = path_and_query.parse().ok();
    if let Ok(new_uri) = axum::http::Uri::from_parts(parts) {
        *request.uri_mut() = new_uri;
    } else if let Ok(uri) = path_and_query.parse::<axum::http::Uri>() {
        // Fall back to a relative URI if from_parts rejects (e.g. had
        // a scheme but no authority). Path-only URIs are valid in
        // server-side handling.
        *request.uri_mut() = uri;
    }
    Some(key)
}

fn is_public_path(path: &str) -> bool {
    // /v1/auth/verify is intentionally public — it's the endpoint the
    // dashboard hits BEFORE it has a known-good key. The handler itself
    // re-validates against keyx (or accepts in dev-passthrough mode);
    // the gateway middleware just lets the request through.
    if path == "/" || path == "/index.html" || path == "/v1/status" || path == "/v1/auth/verify" {
        return true;
    }
    if path.starts_with("/assets/") {
        return true;
    }
    if path == "/favicon.ico"
        || path == "/favicon-light.svg"
        || path == "/favicon-dark.svg"
        || path == "/manifest.webmanifest"
        || path == "/robots.txt"
    {
        return true;
    }
    // SPA deep links — anything non-`/v1/*`, non-asset is presumed to
    // be a dashboard route that the React-router renders.
    !path.starts_with("/v1/") && !path.starts_with("/metrics")
}

fn is_ingest_path(path: &str) -> bool {
    matches!(
        path,
        "/v1/rum" | "/v1/logs" | "/v1/events" | "/v1/prom/write"
    ) || path.starts_with("/v1/otlp/")
}

/// Extract the API key from one of the supported HTTP auth headers:
///
/// * `Authorization: Bearer <KEY>` — preferred (OTel SDKs / dashboards)
/// * `Authorization: Basic base64(<KEY>:)` — what every HTTP client
///   sends when the URL has userinfo (e.g. `https://flx_KEY@gateway/...`).
///   Treat the username as the key; the password slot is ignored. If
///   the username is empty, fall back to the password — some libraries
///   put the secret there.
/// * `X-API-Key: <KEY>` — convenience header.
///
/// Returns an owned `String` because Basic decoding allocates.
fn extract_token(headers: &HeaderMap) -> Option<String> {
    if let Some(v) = headers.get(header::AUTHORIZATION).and_then(|v| v.to_str().ok()) {
        if let Some(rest) = v.strip_prefix("Bearer ").or_else(|| v.strip_prefix("bearer ")) {
            return Some(rest.trim().to_string());
        }
        if let Some(b64) = v.strip_prefix("Basic ").or_else(|| v.strip_prefix("basic ")) {
            use base64::engine::general_purpose::STANDARD;
            use base64::Engine;
            if let Ok(decoded) = STANDARD.decode(b64.trim()) {
                if let Ok(s) = std::str::from_utf8(&decoded) {
                    let (user, pass) = s.split_once(':').unwrap_or((s, ""));
                    let key = if !user.is_empty() { user } else { pass };
                    if !key.is_empty() {
                        return Some(key.to_string());
                    }
                }
            }
        }
    }
    headers.get("X-API-Key")
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

/// Security middleware: Add security headers
async fn security_headers_middleware(
    request: axum::extract::Request,
    next: Next,
) -> impl IntoResponse {
    let mut response = next.run(request).await;
    
    let headers = response.headers_mut();
    headers.insert("X-Content-Type-Options", header::HeaderValue::from_static("nosniff"));
    headers.insert("X-Frame-Options", header::HeaderValue::from_static("DENY"));
    headers.insert("X-XSS-Protection", header::HeaderValue::from_static("1; mode=block"));
    headers.insert("Referrer-Policy", header::HeaderValue::from_static("strict-origin-when-cross-origin"));
    
    response
}

/// Build the CORS layer from `FLUXX_CORS_ORIGIN`. See `start()` above for
/// the policy. Returned as `anyhow::Result` so misconfigured origins fail
/// loudly at startup instead of silently disabling cross-origin requests.
fn build_cors_layer() -> anyhow::Result<CorsLayer> {
    use axum::http::{header, HeaderValue, Method};

    let base = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST, Method::OPTIONS, Method::PATCH, Method::DELETE])
        .allow_headers([header::AUTHORIZATION, header::CONTENT_TYPE]);

    let Ok(raw) = std::env::var("FLUXX_CORS_ORIGIN") else {
        // No env set → same-origin only. The dashboard served from the
        // agent's own host still works (same origin); browsers from any
        // other origin get a CORS preflight failure.
        return Ok(base);
    };

    if raw.trim() == "*" {
        if std::env::var("FLUXX_CORS_ALLOW_ANY").ok().as_deref() != Some("true") {
            anyhow::bail!(
                "FLUXX_CORS_ORIGIN=* is refused unless FLUXX_CORS_ALLOW_ANY=true is also set. \
                 Wildcard CORS is almost always a misconfiguration — set a concrete origin instead."
            );
        }
        return Ok(base.allow_origin(AllowOrigin::any()));
    }

    let origins: Vec<HeaderValue> = raw
        .split(',')
        .map(|s| s.trim())
        .filter(|s| !s.is_empty())
        .map(|s| HeaderValue::from_str(s).map_err(|e| anyhow::anyhow!("bad CORS origin {s:?}: {e}")))
        .collect::<anyhow::Result<_>>()?;
    if origins.is_empty() {
        anyhow::bail!("FLUXX_CORS_ORIGIN is set but contains no valid origins");
    }
    Ok(base.allow_origin(AllowOrigin::list(origins)))
}

#[cfg(test)]
mod cors_tests {
    use super::build_cors_layer;
    use std::sync::Mutex;

    // Env-var manipulation needs serialization across tests.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    #[test]
    fn unset_returns_same_origin_only_layer() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::remove_var("FLUXX_CORS_ORIGIN");
        std::env::remove_var("FLUXX_CORS_ALLOW_ANY");
        // Should not panic and should not allow cross-origin without explicit config.
        let _ = build_cors_layer().expect("ok");
    }

    #[test]
    fn exact_origin_is_accepted() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("FLUXX_CORS_ORIGIN", "https://app.example.com");
        let _ = build_cors_layer().expect("ok");
        std::env::remove_var("FLUXX_CORS_ORIGIN");
    }

    #[test]
    fn multi_origin_csv_is_accepted() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var(
            "FLUXX_CORS_ORIGIN",
            "https://a.example.com, https://b.example.com",
        );
        let _ = build_cors_layer().expect("ok");
        std::env::remove_var("FLUXX_CORS_ORIGIN");
    }

    #[test]
    fn wildcard_refused_without_explicit_opt_in() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("FLUXX_CORS_ORIGIN", "*");
        std::env::remove_var("FLUXX_CORS_ALLOW_ANY");
        let err = build_cors_layer().unwrap_err();
        assert!(err.to_string().contains("FLUXX_CORS_ALLOW_ANY"));
        std::env::remove_var("FLUXX_CORS_ORIGIN");
    }

    #[test]
    fn wildcard_accepted_with_opt_in() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("FLUXX_CORS_ORIGIN", "*");
        std::env::set_var("FLUXX_CORS_ALLOW_ANY", "true");
        let _ = build_cors_layer().expect("ok");
        std::env::remove_var("FLUXX_CORS_ORIGIN");
        std::env::remove_var("FLUXX_CORS_ALLOW_ANY");
    }

    #[test]
    fn empty_origin_list_is_an_error() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("FLUXX_CORS_ORIGIN", "  ,  ,");
        assert!(build_cors_layer().is_err());
        std::env::remove_var("FLUXX_CORS_ORIGIN");
    }

    #[test]
    fn unparseable_origin_is_an_error() {
        let _g = ENV_LOCK.lock().unwrap();
        std::env::set_var("FLUXX_CORS_ORIGIN", "not\na\nheader\nvalue");
        assert!(build_cors_layer().is_err());
        std::env::remove_var("FLUXX_CORS_ORIGIN");
    }
}

/// ApiServer: High-performance Axum-based web server for Fluxx telemetry.
pub struct ApiServer {
    state: Arc<ApiState>,
    addr: String,
}

impl ApiServer {
    pub fn new(state: Arc<ApiState>, addr: &str) -> Self {
        Self {
            state,
            addr: addr.to_string(),
        }
    }

    pub async fn start(&self) -> anyhow::Result<()> {
        // CORS policy: default-deny for cross-origin requests.
        //
        // Three modes, in order of preference:
        //   FLUXX_CORS_ORIGIN=https://app.example.com    → exact allowlist
        //   FLUXX_CORS_ORIGIN=https://a,https://b,...    → multi-origin allowlist
        //   FLUXX_CORS_ORIGIN=*                          → opt-in wildcard
        //                                                  (only valid when
        //                                                  FLUXX_CORS_ALLOW_ANY=true)
        //   unset                                        → SAME-ORIGIN ONLY
        //
        // Closes audit finding SEC-002. Previously this fell back to
        // `CorsLayer::permissive()` whenever the env var was unset, which
        // meant any website could call the agent API from a logged-in
        // browser. The default is now "no cross-origin requests."
        let cors = build_cors_layer()?;

        let app = Router::new()
            // Auth — the gateway is the single point of contact for
            // the dashboard's key validation. It forwards to keyx
            // (when configured) or accepts in dev-passthrough mode.
            // Dashboards never call keyx directly.
            .route("/v1/auth/verify", post(crate::auth::verify_api_key))
            .route("/v1/sessions", get(list_sessions))
            .route("/v1/sessions/:id", get(get_session))
            .route("/v1/anomalies", get(list_anomalies))
            .route("/v1/events", post(ingest_event).get(list_events))
            .route("/v1/stats", get(get_stats))
            .route("/v1/stream", get(stream_ws))
            // Canonical OTel/Datadog/Grafana/Splunk ingestion path.
            // POST is the content-negotiating dispatcher: it accepts
            // OTLP HTTP/JSON (`{resourceLogs: [...]}`) and the
            // fluxx-native LogEntry array on the same URL so every
            // upstream SDK works without per-vendor path tweaks.
            // GET continues to query the log store.
            .route("/v1/logs", post(crate::ingest::ingest_logs).get(crate::logs::query_logs))
            // Fluxx-native shape only — kept for callers that want the
            // strict-array contract and don't want the dispatcher's
            // content sniff. Same handler, no shape branching.
            .route("/v1/logs/raw", post(crate::logs::ingest_logs))
            .route("/v1/logs/stream", get(crate::logs::stream_logs_ws))
            // Metrics: GET queries the metric store, POST accepts OTLP
            // HTTP/JSON — same URL every OTel SDK is preconfigured for.
            .route("/v1/metrics",
                   get(crate::metrics::query_metrics)
                       .post(crate::otlp_http_handlers::ingest_metrics))
            .route("/v1/metrics/names", get(crate::metrics::list_metric_names))
            // Prometheus-compatible scrape endpoint
            .route("/metrics", get(crate::metrics::prometheus_endpoint))
            // Service map endpoint
            .route("/v1/service-map", get(crate::service_map::get_service_map))
            // Deploy annotations
            .route("/v1/annotations", post(crate::annotations::create_annotation).get(crate::annotations::list_annotations))
            // PromQL-compatible query engine
            .route("/v1/query", get(crate::promql::query))
            // Traces: GET lists/searches stored traces, POST accepts
            // OTLP HTTP/JSON so every OTel SDK is a drop-in producer.
            .route("/v1/traces",
                   get(crate::traces::list_traces)
                       .post(crate::otlp_http_handlers::ingest_traces))
            .route("/v1/traces/:trace_id", get(crate::traces::get_trace))
            // SLO endpoints
            .route("/v1/slos", post(crate::slo::create_slo).get(crate::slo::list_slos))
            .route("/v1/slos/:id", get(crate::slo::get_slo))
            // Incidents
            .route("/v1/incidents", post(crate::incidents::create_incident).get(crate::incidents::list_incidents))
            .route("/v1/incidents/:id", get(crate::incidents::get_incident))
            .route("/v1/incidents/:id/status", post(crate::incidents::update_incident_status))
            // Notebooks
            .route("/v1/notebooks", post(crate::notebooks::create_notebook).get(crate::notebooks::list_notebooks))
            .route("/v1/notebooks/:id", get(crate::notebooks::get_notebook))
            // Workflows
            .route("/v1/workflows", post(crate::workflows::create_workflow).get(crate::workflows::list_workflows))
            .route("/v1/workflows/:id", get(crate::workflows::get_workflow))
            // Database Monitoring
            .route("/v1/dbm/queries", post(crate::dbm::ingest_slow_query).get(crate::dbm::list_slow_queries))
            .route("/v1/dbm/stats", get(crate::dbm::get_query_stats))
            // CI/CD Visibility
            .route("/v1/ci/pipelines", post(crate::ci::record_pipeline).get(crate::ci::list_pipelines))
            .route("/v1/ci/dora", get(crate::ci::get_dora_metrics))
            // Network Performance Monitoring
            .route("/v1/network/flows", post(crate::npm::record_flow).get(crate::npm::list_flows))
            .route("/v1/network/dns", post(crate::npm::record_dns))
            .route("/v1/network/summary", get(crate::npm::network_summary))
            // Real User Monitoring (RUM)
            .route("/v1/rum", post(crate::rum::ingest_rum))
            .route("/v1/rum/sessions", get(crate::rum::list_rum_sessions))
            .route("/v1/rum/vitals", get(crate::rum::get_web_vitals))
            .route("/v1/rum/stats", get(crate::rum::rum_stats))
            // Error tracking
            .route("/v1/errors", get(crate::errors::list_errors))
            .route("/v1/errors/top", get(crate::errors::top_errors))
            .route("/v1/errors/trend", get(crate::errors::errors_trend))
            .route("/v1/errors/stats", get(crate::errors::errors_stats))
            .route("/v1/errors/:fingerprint", get(crate::errors::get_error))
            .route("/v1/errors/:fingerprint/latest-event", get(crate::errors::latest_event))
            .route("/v1/errors/:fingerprint/events", get(crate::errors::list_events))
            .route("/v1/errors/:fingerprint/resolve", post(crate::errors::resolve_error))
            .route("/v1/errors/:fingerprint/mute", post(crate::errors::mute_error))
            // Sourcemap upload + list. The upload endpoint accepts a
            // multipart payload (`release`, `bundle_url`, `map`).
            .route(
                "/v1/sourcemaps",
                post(crate::sourcemaps::upload_sourcemap)
                    .get(crate::sourcemaps::list_sourcemaps),
            )
            // Cloud Security Posture Management (CSPM)
            .route("/v1/cspm/findings", post(crate::cspm::report_finding).get(crate::cspm::list_findings))
            .route("/v1/cspm/compliance", get(crate::cspm::get_compliance))
            .route("/v1/cspm/rules", get(crate::cspm::list_security_rules))
            // Cost Analytics
            .route("/v1/cost", get(crate::cost::cost_analysis))
            // Continuous Profiling
            .route("/v1/profiling", post(crate::profiling::ingest_profile).get(crate::profiling::list_profiles))
            .route("/v1/profiling/raw", post(crate::profiling::ingest_raw_profile))
            .route("/v1/profiling/:id", get(crate::profiling::get_profile))
            // AI Root Cause Analysis
            .route("/v1/rca", get(crate::ai_rca::analyze_root_cause).post(crate::ai_rca::trigger_rca))
            // Natural Language Dashboard Builder
            .route("/v1/nl/query", get(crate::nl_dashboard::nl_to_promql))
            .route("/v1/nl/panel", post(crate::nl_dashboard::create_panel_from_nl))
            // Predictive Alerting
            .route("/v1/predict", get(crate::predictive::predict_metric))
            .route("/v1/predict/alerts", get(crate::predictive::predicted_alerts))
            // Dashboard Templates
            .route("/v1/templates", get(crate::templates::list_templates))
            .route("/v1/templates/active", get(crate::templates::list_active_templates))
            .route("/v1/templates/:id/activate", post(crate::templates::activate_template))
            .route("/v1/templates/:id/deactivate", post(crate::templates::deactivate_template))
            .route("/v1/templates/:id/data", get(crate::templates::get_template_data))
            // Correlation Engine
            .route("/v1/correlate", get(crate::correlate::correlate))
            // Change Intelligence
            .route("/v1/changes", post(crate::changes::record_change).get(crate::changes::list_changes))
            .route("/v1/changes/regressions", get(crate::changes::detect_regressions))
            // Data Pipelines
            .route("/v1/pipelines", post(crate::pipelines::create_pipeline).get(crate::pipelines::list_pipelines))
            .route("/v1/pipelines/:id", get(crate::pipelines::get_pipeline).delete(crate::pipelines::delete_pipeline))
            // Quotas & Rate Limiting
            .route("/v1/quotas", post(crate::quotas::set_quota).get(crate::quotas::list_quotas))
            .route("/v1/quotas/:project_id/usage", get(crate::quotas::get_usage))
            // Cluster status
            .route("/v1/cluster", get(crate::cluster::cluster_status))
            .route("/v1/cluster/topology", get(crate::cluster::cluster_topology))
            // Prometheus remote_write endpoint
            .route("/v1/prom/write", post(crate::prometheus::remote_write))
            // Legacy `/v1/otlp/*` aliases — kept so existing fluxx
            // deployments using the old paths keep working. New
            // deployments should point at the canonical `/v1/{traces,
            // metrics,logs}` routes above, which match the
            // OTEL_EXPORTER_OTLP_ENDPOINT contract every SDK uses.
            .route("/v1/otlp/traces", post(crate::otlp_http_handlers::ingest_traces))
            .route("/v1/otlp/metrics", post(crate::otlp_http_handlers::ingest_metrics))
            .route("/v1/otlp/logs", post(crate::otlp_http_handlers::ingest_logs))
            .layer(middleware::from_fn(security_headers_middleware))
            .layer(middleware::from_fn(auth_middleware))
            .layer(cors)
            .with_state(self.state.clone());

        info!("API: Binding Fluxx layer to {} (auth required if configured)", self.addr);
        let listener = tokio::net::TcpListener::bind(&self.addr).await?;
    // Mount install endpoints (gateway-streamed installer + binary download).
    let install_state = std::sync::Arc::new(fluxx_core::install::InstallState::new());
    let app = fluxx_core::install::router(install_state).merge(app);

    // Mount the embedded per-node dashboard *after* the API routes so
    // its SPA fallback never shadows a real `/v1/*` handler. The
    // dashboard is bundled at compile time; if it wasn't built, the
    // router serves a one-line hint instead of a blank page.
    if crate::dashboard::is_bundled() {
        info!("API: Dashboard bundle embedded — serving at http://{}/", self.addr);
    } else {
        info!(
            "API: Dashboard bundle NOT embedded. Build with:\n  cd node.dashboard.fluxx.dev && npm run build && cd .. && cargo build"
        );
    }
    let app = app.merge(crate::dashboard::router());

        axum::serve(listener, app).await?;
        Ok(())
    }
}

async fn list_sessions(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let sessions = state.session_manager.list_sessions().await;
    StandardResponse::ok(sessions, "Sessions list retrieved successfully.")
}

async fn get_session(Path(id): Path<String>, State(state): State<Arc<ApiState>>) -> axum::response::Response {
    match state.session_manager.get_session(&id).await {
        Some(session) => StandardResponse::ok(session, "Session detail retrieved.").into_response(),
        None => StandardResponse::error(
            serde_json::json!({}), 
            "Session not found.", 
            "NOT_FOUND", 
            vec!["The requested session might have been evicted.".to_string()]
        ).into_response(),
    }
}

async fn list_anomalies() -> impl IntoResponse {
    StandardResponse::ok(Vec::<String>::new(), "Anomalies search results.")
}

async fn ingest_event(
    State(state): State<Arc<ApiState>>,
    Json(event): Json<Event>,
) -> impl IntoResponse {
    let event_arc = Arc::new(event);
    let _ = state.event_bus.send(event_arc.clone());
    let _ = state.event_store.insert((*event_arc).clone()).await;
    StandardResponse::ok(serde_json::json!({}), "Event ingested successfully.")
}

async fn list_events(
    State(state): State<Arc<ApiState>>,
    Query(params): Query<std::collections::HashMap<String, String>>,
) -> impl IntoResponse {
    let project_id = params.get("project_id").cloned();
    let service_id = params.get("service_id").cloned();
    let protocol = params.get("protocol").cloned();
    let severity = params.get("severity").cloned();
    
    let protocol_enum = protocol.and_then(|p| {
        match p.to_lowercase().as_str() {
            "http" => Some(fluxx_core::models::event::ProtocolType::Http),
            "ws" => Some(fluxx_core::models::event::ProtocolType::Ws),
            "tcp" => Some(fluxx_core::models::event::ProtocolType::Tcp),
            "udp" => Some(fluxx_core::models::event::ProtocolType::Udp),
            "grpc" => Some(fluxx_core::models::event::ProtocolType::Grpc),
            "quic" => Some(fluxx_core::models::event::ProtocolType::Quic),
            "ebpf" => Some(fluxx_core::models::event::ProtocolType::Ebpf),
            "mqtt" => Some(fluxx_core::models::event::ProtocolType::Mqtt),
            "modbus" => Some(fluxx_core::models::event::ProtocolType::Modbus),
            "system" => Some(fluxx_core::models::event::ProtocolType::System),
            "otlp" => Some(fluxx_core::models::event::ProtocolType::Otlp),
            "statsd" => Some(fluxx_core::models::event::ProtocolType::StatsD),
            "log" => Some(fluxx_core::models::event::ProtocolType::Log),
            _ => None,
        }
    });

    let severity_enum = severity.and_then(|s| {
        match s.to_lowercase().as_str() {
            "info" => Some(fluxx_core::models::event::EventSeverity::Info),
            "low" => Some(fluxx_core::models::event::EventSeverity::Low),
            "medium" => Some(fluxx_core::models::event::EventSeverity::Medium),
            "high" => Some(fluxx_core::models::event::EventSeverity::High),
            "critical" => Some(fluxx_core::models::event::EventSeverity::Critical),
            _ => None,
        }
    });

    let events = state.event_store.list_events(project_id, protocol_enum, severity_enum, service_id).await;
    StandardResponse::ok(events, "Events retrieval successful.")
}

async fn get_stats(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let stats = state.event_store.get_stats().await;
    StandardResponse::ok(stats, "Agent operational statistics.")
}

async fn stream_ws(
    ws: WebSocketUpgrade, 
    State(state): State<Arc<ApiState>>,
    Query(params): Query<std::collections::HashMap<String, String>>
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_socket(socket, state, params))
}

async fn handle_socket(mut socket: WebSocket, state: Arc<ApiState>, filters: std::collections::HashMap<String, String>) {
    let mut rx = state.event_bus.subscribe();
    
    let target_project = filters.get("project_id").cloned();
    let target_env = filters.get("environment_id").cloned();
    let target_service = filters.get("service_id").cloned();

    while let Ok(event) = rx.recv().await {
        if let Some(p) = &target_project { if event.project_id.as_ref() != Some(p) { continue; } }
        if let Some(e) = &target_env { if event.environment_id.as_ref() != Some(e) { continue; } }
        if let Some(s) = &target_service { if event.service_id.as_ref() != Some(s) { continue; } }

        let msg = serde_json::to_string(&*event).unwrap_or_default();
        if let Err(_) = socket.send(Message::Text(msg.into())).await {
            break;
        }
    }
}

#[cfg(test)]
mod url_auth_tests {
    //! URL-based auth — both mechanisms are tested at the unit level so
    //! we pin the exact path-rewrite + header-decode behaviour. The
    //! middleware-level wiring is covered by the e2e smoke that fires
    //! real requests in `tests/`.

    use super::{extract_token, strip_path_token};
    use axum::body::Body;
    use axum::http::{HeaderMap, HeaderValue, Request};

    fn req(uri: &str) -> Request<Body> {
        Request::builder().uri(uri).body(Body::empty()).unwrap()
    }

    // ── /k/<KEY>/... path-prefix ───────────────────────────────────

    #[test]
    fn path_prefix_extracts_key_and_rewrites_path() {
        let mut r = req("/k/flx_live_abc123/v1/traces");
        let key = strip_path_token(&mut r);
        assert_eq!(key.as_deref(), Some("flx_live_abc123"));
        assert_eq!(r.uri().path(), "/v1/traces");
    }

    #[test]
    fn path_prefix_preserves_query_string() {
        let mut r = req("/k/secret/v1/logs?level=error&service=foo");
        let key = strip_path_token(&mut r);
        assert_eq!(key.as_deref(), Some("secret"));
        assert_eq!(r.uri().path(), "/v1/logs");
        assert_eq!(r.uri().query(), Some("level=error&service=foo"));
    }

    #[test]
    fn no_prefix_returns_none_and_leaves_uri_alone() {
        let mut r = req("/v1/traces");
        assert!(strip_path_token(&mut r).is_none());
        assert_eq!(r.uri().path(), "/v1/traces");
    }

    #[test]
    fn empty_key_in_prefix_is_rejected() {
        let mut r = req("/k//v1/traces");
        assert!(strip_path_token(&mut r).is_none());
        // URI is intentionally not rewritten on rejection.
        assert_eq!(r.uri().path(), "/k//v1/traces");
    }

    #[test]
    fn prefix_without_trailing_path_is_rejected() {
        let mut r = req("/k/onlykey");
        assert!(strip_path_token(&mut r).is_none());
    }

    // ── Authorization header parsing ───────────────────────────────

    fn hdr(name: &str, value: &str) -> HeaderMap {
        let mut h = HeaderMap::new();
        h.insert(
            axum::http::HeaderName::from_bytes(name.as_bytes()).unwrap(),
            HeaderValue::from_str(value).unwrap(),
        );
        h
    }

    #[test]
    fn bearer_token_is_extracted() {
        let h = hdr("authorization", "Bearer flx_live_abc");
        assert_eq!(extract_token(&h).as_deref(), Some("flx_live_abc"));
    }

    #[test]
    fn bearer_case_insensitive_prefix() {
        let h = hdr("authorization", "bearer flx_live_abc");
        assert_eq!(extract_token(&h).as_deref(), Some("flx_live_abc"));
    }

    #[test]
    fn x_api_key_is_extracted() {
        let h = hdr("x-api-key", "flx_live_xyz");
        assert_eq!(extract_token(&h).as_deref(), Some("flx_live_xyz"));
    }

    /// HTTP Basic auth — the canonical URL-userinfo mechanism. A client
    /// hitting `https://flx_KEY@gateway/v1/traces` automatically sends
    /// `Authorization: Basic base64(flx_KEY:)`. Username = key, password
    /// is unused.
    #[test]
    fn basic_userinfo_username_is_extracted() {
        // base64("flx_live_user:") = "Zmx4X2xpdmVfdXNlcjo="
        let h = hdr("authorization", "Basic Zmx4X2xpdmVfdXNlcjo=");
        assert_eq!(extract_token(&h).as_deref(), Some("flx_live_user"));
    }

    /// Some HTTP libraries put the secret in the password slot
    /// (`https://:flx_KEY@gateway/...`). We accept that too.
    #[test]
    fn basic_userinfo_password_fallback() {
        // base64(":flx_live_pass") = "OmZseF9saXZlX3Bhc3M="
        let h = hdr("authorization", "Basic OmZseF9saXZlX3Bhc3M=");
        assert_eq!(extract_token(&h).as_deref(), Some("flx_live_pass"));
    }

    #[test]
    fn basic_garbage_returns_none() {
        let h = hdr("authorization", "Basic !!!not-base64!!!");
        assert!(extract_token(&h).is_none());
    }

    #[test]
    fn no_auth_header_returns_none() {
        let h = HeaderMap::new();
        assert!(extract_token(&h).is_none());
    }
}

