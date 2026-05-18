//! Sourcemap HTTP routes.
//!
//!   POST /v1/sourcemaps  — multipart upload: fields `release`,
//!                          `bundle_url`, `map` (the `.map` file).
//!   GET  /v1/sourcemaps  — list registered (release, bundle_url) pairs.
//!
//! Auth: gated by the global bearer-token middleware. CI typically
//! posts these via `fluxx sourcemaps upload`, which carries the
//! token from `FLUXX_ADMIN_TOKEN`.

use crate::api::{ApiState, StandardResponse};
use axum::{
    extract::{Multipart, State},
    response::IntoResponse,
};
use std::sync::Arc;
use tracing::warn;

fn bad(msg: &str) -> StandardResponse<serde_json::Value> {
    StandardResponse::error(
        serde_json::Value::Null,
        msg,
        "VALIDATION_ERROR",
        vec![msg.to_string()],
    )
}

/// POST /v1/sourcemaps
pub async fn upload_sourcemap(
    State(state): State<Arc<ApiState>>,
    mut multipart: Multipart,
) -> impl IntoResponse {
    let mut release: Option<String> = None;
    let mut bundle_url: Option<String> = None;
    let mut body: Option<Vec<u8>> = None;

    while let Ok(Some(field)) = multipart.next_field().await {
        match field.name() {
            Some("release") => release = field.text().await.ok(),
            Some("bundle_url") => bundle_url = field.text().await.ok(),
            Some("map") => body = field.bytes().await.ok().map(|b| b.to_vec()),
            _ => {}
        }
    }

    let Some(release) = release else {
        return bad("missing field: release");
    };
    let Some(bundle_url) = bundle_url else {
        return bad("missing field: bundle_url");
    };
    let Some(body) = body else {
        return bad("missing field: map");
    };

    match state.sourcemap_store.put(&release, &bundle_url, &body) {
        Ok(meta) => StandardResponse::ok(
            serde_json::to_value(&meta).unwrap_or_default(),
            "Sourcemap uploaded.",
        ),
        Err(e) => {
            warn!(error = %e, "sourcemap upload: write failed");
            bad(&format!("Failed to persist map: {}", e))
        }
    }
}

/// GET /v1/sourcemaps
pub async fn list_sourcemaps(
    State(state): State<Arc<ApiState>>,
) -> impl IntoResponse {
    let list = state.sourcemap_store.list();
    StandardResponse::ok(
        serde_json::to_value(&list).unwrap_or_default(),
        "Sourcemaps.",
    )
}
