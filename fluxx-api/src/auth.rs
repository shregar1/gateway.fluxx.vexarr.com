//! Auth verification — the gateway brokers between the dashboard and
//! the upstream auth server (auth server). The dashboard NEVER talks to auth server
//! directly: it sends `POST /v1/auth/verify { api_key }` to this
//! gateway, and the gateway is responsible for validating the key
//! against auth server, mapping the result, and returning a stable envelope.
//!
//! ## Trust anchor: baked in, not env-configurable
//!
//! `AUTH_URL` is the **full endpoint URL** the gateway POSTs to when
//! it forwards `/v1/auth/verify` — e.g.
//! `https://auth.vexarr.com/v1/keys/verify`. It is captured at
//! COMPILE TIME via `env!("AUTH_URL")` and must be supplied at
//! build time — there is no built-in default. We deliberately refuse
//! to bake a fallback so a misconfigured CI can't silently ship a
//! binary pointed at the wrong auth host. If you forgot to set it,
//! the build fails loudly.
//!
//! AUTH_URL is **not** read from the process env at runtime — otherwise
//! any operator (or attacker who can flip an env var) could redirect
//! the gateway to a fake auth server that returns "valid" for every
//! key, silently bypassing real validation. The URL is the trust
//! anchor; it travels with the binary.
//!
//! Build-time matrix:
//!
//! | Build command                                                                  | Baked URL                              | Mode           |
//! |--------------------------------------------------------------------------------|----------------------------------------|----------------|
//! | `AUTH_URL=https://auth.vexarr.com/v1/keys/verify cargo build --release`        | https://auth.vexarr.com/v1/keys/verify | live (prod)    |
//! | `AUTH_URL=https://auth.staging.vexarr.com/v1/keys/verify cargo build`          | …/v1/keys/verify                       | live (staging) |
//! | `AUTH_URL= cargo build`                                                        | (empty)                                | passthrough    |
//! | `cargo build` (AUTH_URL unset)                                                 | **build fails**                        | —              |
//!
//! Empty `AUTH_URL=""` at build time selects local "passthrough" mode:
//! any non-empty key is accepted. Use it for offline dev and tests.
//!
//! ## What's still runtime-configurable
//!
//! `FLUXX_API_KEY`. One env var, two jobs:
//!   1. The edge auth middleware (`fluxx_core::security::bootstrap_token`)
//!      uses it as the bootstrap bearer for `/v1/*`.
//!   2. THIS module uses it as the gateway's own credential when it
//!      forwards `/v1/auth/verify` to auth server — same value, no second
//!      secret to mint or rotate.
//! Without it, the gateway falls back to passthrough mode even on a
//! release build (and logs a warning).
//!
//! Defense in depth: when `FLUXX_API_KEY` is set, the gateway also
//! checks that the dashboard-submitted key equals it (constant-time)
//! before forwarding to auth server. That short-circuits auth server round-trips
//! for obviously-wrong keys and removes the "any key the operator
//! pastes is forwarded as-is to auth server" footgun.
//!
//! ## Wire shape
//!
//! All responses use the standard envelope on HTTP 200; callers branch
//! on `responseKey` (OK / INVALID_API_KEY / AUTH_UNAVAILABLE /
//! MISSING_API_KEY) rather than HTTP status.

use crate::{ApiState, StandardResponse};
use axum::{extract::State, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::Arc;
use std::time::Duration;
use tracing::warn;

/// Compile-time auth-server endpoint URL. Captured via `env!`, so the
/// value travels inside the binary and cannot be replaced at runtime.
///
/// This is the **full endpoint URL** the gateway POSTs to — including
/// the `/v1/keys/verify` path — not a base. Whatever the operator
/// baked at build time is what we hit verbatim.
///
/// **AUTH_URL must be set at build time** — there is no default. We
/// refuse to bake a fallback so a misconfigured CI can't silently
/// ship a binary pointed at the wrong auth host. If you forgot to
/// set it, the build fails with the message below.
///
/// Build-time matrix:
/// - `AUTH_URL=https://auth.vexarr.com/v1/keys/verify`         → live (prod)
/// - `AUTH_URL=https://auth.staging.vexarr.com/v1/keys/verify` → live (staging)
/// - `AUTH_URL=http://127.0.0.1:9100/v1/keys/verify`           → local auth-stub
/// - `AUTH_URL=` (empty)                                       → passthrough mode (dev / offline)
/// - unset                                                     → **build fails**
pub const AUTH_URL: &str = env!(
    "AUTH_URL",
    "AUTH_URL is required at compile time and was not set. Pass it on \
     the cargo command, e.g. `AUTH_URL=https://auth.vexarr.com/v1/keys/verify \
     cargo build --release` for production, or \
     `AUTH_URL=http://127.0.0.1:9100/v1/keys/verify cargo run` for local \
     auth-stub testing. There is intentionally NO default — this prevents \
     shipping a binary that points at the wrong auth backend."
);

#[derive(Debug, Deserialize)]
pub struct VerifyRequest {
    /// The raw API key the dashboard wants to validate. We never echo
    /// it back in the response.
    pub api_key: String,
    /// Optional project slug forwarded to auth server. Defaults to "fluxx".
    #[serde(default)]
    pub project: Option<String>,
}

#[derive(Debug, Serialize)]
pub struct VerifyOk {
    pub valid: bool,
    /// Minimal principal surface the dashboard can display. Filled
    /// when auth server returned a principal; empty in dev-passthrough mode.
    pub principal: Option<VerifiedPrincipal>,
    /// "live" when we forwarded to auth server, "dev-passthrough" when the
    /// gateway isn't configured for auth server (no env vars set).
    pub mode: &'static str,
}

#[derive(Debug, Serialize, Default)]
pub struct VerifiedPrincipal {
    pub tenant_urn: Option<String>,
    pub api_key_id: Option<u64>,
    pub scopes: Vec<String>,
    pub environment: Option<String>,
}

/// POST /v1/auth/verify
pub async fn verify_api_key(
    State(_state): State<Arc<ApiState>>,
    Json(req): Json<VerifyRequest>,
) -> impl IntoResponse {
    let trimmed = req.api_key.trim();
    if trimmed.is_empty() {
        return StandardResponse::error(
            Value::Null,
            "api_key is required",
            "MISSING_API_KEY",
            Vec::new(),
        )
        .into_response();
    }

    // Trust anchor: compile-time const. Single operator secret: runtime.
    let fluxx_api_key = std::env::var("FLUXX_API_KEY")
        .ok()
        .filter(|s| !s.is_empty());

    // Live mode requires both:
    //   - the binary was built with a non-empty AUTH_URL baked in
    //     (AUTH_URL is required at build time; passing `AUTH_URL=`
    //     empty selects passthrough — anything non-empty is live)
    //   - the operator set FLUXX_API_KEY at runtime
    // Anything else → passthrough.
    if !AUTH_URL.is_empty() {
        if let Some(operator_key) = fluxx_api_key {
            // Defense-in-depth fast path: the submitted key must match
            // the operator's FLUXX_API_KEY before we bother the auth
            // server. Constant-time compare so timing doesn't leak
            // which prefix matches.
            if !constant_time_eq(trimmed.as_bytes(), operator_key.as_bytes()) {
                return StandardResponse::error(
                    Value::Null,
                    "Key does not match this node's configured FLUXX_API_KEY.",
                    "INVALID_API_KEY",
                    Vec::new(),
                )
                .into_response();
            }
            // Same value serves as the gateway's bearer upstream. One
            // env var, two jobs.
            return verify_with_auth(
                AUTH_URL.to_string(),
                operator_key,
                trimmed,
                req.project,
            )
            .await;
        }
    }

    // Passthrough fallback: AUTH_URL was built empty, or FLUXX_API_KEY
    // is missing. Accept any non-empty key so local dashboards still
    // onboard cleanly. Log loudly so the operator notices a misconfig.
    if AUTH_URL.is_empty() {
        warn!(
            "auth.verify: this binary was built with AUTH_URL=\"\" — running \
             in passthrough mode. For production, rebuild with the full \
             endpoint URL baked in (e.g. \
             AUTH_URL=https://auth.vexarr.com/v1/keys/verify cargo build --release)."
        );
    } else {
        warn!(
            "auth.verify: FLUXX_API_KEY not set at runtime — falling back \
             to passthrough. Set FLUXX_API_KEY to your auth-server PAT to \
             enable real validation."
        );
    }
    StandardResponse::ok(
        serde_json::to_value(VerifyOk {
            valid: true,
            principal: None,
            mode: "dev-passthrough",
        })
        .unwrap_or(Value::Null),
        "API key accepted (dev passthrough — auth not configured).",
    )
    .into_response()
}

fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for i in 0..a.len() {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

async fn verify_with_auth(
    endpoint_url: String,
    service_key: String,
    api_key: &str,
    project: Option<String>,
) -> axum::response::Response {
    // AUTH_URL is treated as the FULL endpoint URL the gateway POSTs to
    // (e.g. https://auth.vexarr.com/v1/keys/verify). We do not append a
    // path here — what the operator baked at build time is what we hit
    // verbatim. Trailing slashes are trimmed for hygiene only.
    let url = endpoint_url.trim_end_matches('/').to_string();
    let body = serde_json::json!({
        "api_key": api_key,
        "project": project.unwrap_or_else(|| "fluxx".to_string()),
    });

    let client = match reqwest::Client::builder()
        .timeout(Duration::from_secs(10))
        .build()
    {
        Ok(c) => c,
        Err(e) => {
            return StandardResponse::error(
                Value::Null,
                &format!("Failed to build HTTP client: {}", e),
                "AUTH_UNAVAILABLE",
                Vec::new(),
            )
            .into_response();
        }
    };

    let res = match client
        .post(&url)
        .bearer_auth(&service_key)
        .header("Accept", "application/json")
        .json(&body)
        .send()
        .await
    {
        Ok(r) => r,
        Err(e) => {
            return StandardResponse::error(
                Value::Null,
                &format!("auth server unreachable: {}", e),
                "AUTH_UNAVAILABLE",
                Vec::new(),
            )
            .into_response();
        }
    };

    let status = res.status();
    if status.as_u16() == 401 || status.as_u16() == 403 {
        return StandardResponse::error(
            Value::Null,
            "auth server rejected this key",
            "INVALID_API_KEY",
            Vec::new(),
        )
        .into_response();
    }
    if !status.is_success() {
        return StandardResponse::error(
            Value::Null,
            &format!("auth server returned HTTP {}", status.as_u16()),
            "AUTH_UNAVAILABLE",
            Vec::new(),
        )
        .into_response();
    }

    // auth server returns either `{ principal: {...} }` or the principal at
    // the root. Handle both for forward compatibility.
    let parsed: Value = match res.json().await {
        Ok(v) => v,
        Err(e) => {
            return StandardResponse::error(
                Value::Null,
                &format!("auth server returned non-JSON body: {}", e),
                "AUTH_UNAVAILABLE",
                Vec::new(),
            )
            .into_response();
        }
    };
    let p_obj = parsed.get("principal").cloned().unwrap_or(parsed.clone());

    let principal = VerifiedPrincipal {
        tenant_urn: p_obj
            .get("tenant_urn")
            .and_then(|v| v.as_str())
            .map(String::from),
        api_key_id: p_obj.get("api_key_id").and_then(|v| v.as_u64()),
        scopes: p_obj
            .get("scopes")
            .and_then(|v| v.as_array())
            .map(|a| a.iter().filter_map(|x| x.as_str().map(String::from)).collect())
            .unwrap_or_default(),
        environment: p_obj
            .get("environment")
            .and_then(|v| v.as_str())
            .map(String::from),
    };

    StandardResponse::ok(
        serde_json::to_value(VerifyOk {
            valid: true,
            principal: Some(principal),
            mode: "live",
        })
        .unwrap_or(Value::Null),
        "API key verified.",
    )
    .into_response()
}

#[cfg(test)]
mod tests {
    //! End-to-end wiring tests for the gateway broker. A local axum
    //! server stands in for the upstream auth host so we can observe
    //! exactly what URL/headers/body the gateway forwards.

    use axum::extract::{Json, State};
    use axum::http::HeaderMap;
    use axum::routing::post;
    use axum::Router;
    use serde_json::{json, Value};
    use std::sync::Arc;
    use std::sync::Mutex;
    use tokio::net::TcpListener;

    #[derive(Default)]
    struct StubState {
        last_path: Mutex<Option<String>>,
        last_bearer: Mutex<Option<String>>,
        last_body: Mutex<Option<Value>>,
    }

    async fn stub_handler(
        State(state): State<Arc<StubState>>,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> Json<Value> {
        // Record what hit us so the test can assert on it.
        *state.last_path.lock().unwrap() = Some("/v1/keys/verify".to_string());
        *state.last_bearer.lock().unwrap() = headers
            .get(axum::http::header::AUTHORIZATION)
            .and_then(|v| v.to_str().ok())
            .map(String::from);
        *state.last_body.lock().unwrap() = Some(body.clone());
        Json(json!({
            "principal": {
                "tenant_urn": "urn:fluxx:tenant:42",
                "api_key_id": 7,
                "scopes": ["events:write"],
                "environment": "production"
            }
        }))
    }

    /// The full-endpoint AUTH_URL must be POSTed to verbatim — no path
    /// appending, no rewriting. This guards against the historical
    /// double-`/v1/keys/verify` bug.
    #[tokio::test]
    async fn gateway_posts_to_auth_url_verbatim() {
        let state = Arc::new(StubState::default());
        let app = Router::new()
            .route("/v1/keys/verify", post(stub_handler))
            .with_state(state.clone());

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        // AUTH_URL points at the FULL endpoint, mirroring the build-time
        // contract: `https://auth.vexarr.com/v1/keys/verify`.
        let endpoint = format!("http://{}/v1/keys/verify", addr);
        let _resp = super::verify_with_auth(
            endpoint.clone(),
            "test-service-key".to_string(),
            "submitted-key",
            None,
        )
        .await;

        // Stub recorded a hit at the exact path — no doubling, no rewrite.
        assert_eq!(
            state.last_path.lock().unwrap().as_deref(),
            Some("/v1/keys/verify"),
            "gateway should POST to AUTH_URL verbatim, not append a path"
        );
        // Bearer is the gateway's service key (FLUXX_API_KEY in production).
        assert_eq!(
            state.last_bearer.lock().unwrap().as_deref(),
            Some("Bearer test-service-key")
        );
        // Body carries the dashboard-submitted key and the default project.
        let body = state.last_body.lock().unwrap().clone().unwrap();
        assert_eq!(body["api_key"], "submitted-key");
        assert_eq!(body["project"], "fluxx");
    }
}
