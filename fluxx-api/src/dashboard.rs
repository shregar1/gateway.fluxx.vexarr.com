//! Embedded per-node dashboard for fluxx-api.
//!
//! The Vite-built bundle in `../node.dashboard.fluxx.vexarr.com/dist/`
//! is compiled into the api crate when the `bundled-dashboard` feature
//! is on. The feature is OFF by default because the dashboard repo
//! is a sibling, not a submodule, and the bundle may not exist in
//! every developer checkout. With the feature off:
//!
//!   * `is_bundled()` returns `false`
//!   * `router()` serves a 404 stub instructing operators how to
//!     enable the bundle
//!
//! The api keeps booting either way — bundling failure must not take
//! `/v1/*` down.
//!
//! Mounted by `server.rs` after the api routes so the SPA fallback
//! never shadows a real `/v1/*` handler.

use axum::{
    body::Body,
    extract::Path,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};

#[cfg(feature = "bundled-dashboard")]
#[derive(rust_embed::Embed)]
#[folder = "../node.dashboard.fluxx.vexarr.com/dist/"]
struct DashboardAssets;

/// Build the static-file router. Designed to be `.merge()`'d into
/// the main `Router` *after* the `/v1/*` routes.
pub fn router() -> Router {
    Router::new()
        .route("/", get(serve_index))
        .route("/index.html", get(serve_index))
        .route("/assets/:file", get(serve_asset))
        .route("/favicon.ico", get(|| async { serve_path("favicon.ico") }))
        .route(
            "/manifest.webmanifest",
            get(|| async { serve_path("manifest.webmanifest") }),
        )
        .route("/robots.txt", get(|| async { serve_path("robots.txt") }))
        // SPA fallback: anything we don't have a real handler for and
        // isn't an asset path renders the dashboard shell so the
        // client-side router can take over.
        .fallback(get(serve_spa_fallback))
}

async fn serve_index() -> Response {
    serve_path("index.html")
}

async fn serve_asset(Path(file): Path<String>) -> Response {
    let path = format!("assets/{}", file);
    let mut resp = serve_path(&path).into_response();
    if resp.status() == StatusCode::OK {
        // Vite content-hashes asset filenames, so "immutable" is safe.
        resp.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=31536000, immutable"),
        );
    }
    resp
}

async fn serve_spa_fallback() -> Response {
    serve_path("index.html")
}

#[cfg(feature = "bundled-dashboard")]
fn serve_path(path: &str) -> Response {
    match DashboardAssets::get(path) {
        Some(content) => {
            let ct = content_type_for(path);
            let mut resp = Response::new(Body::from(content.data.into_owned()));
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_static(ct),
            );
            resp
        }
        None => not_found(),
    }
}

#[cfg(not(feature = "bundled-dashboard"))]
fn serve_path(_path: &str) -> Response {
    not_bundled_stub()
}

#[cfg(feature = "bundled-dashboard")]
fn not_found() -> Response {
    let mut resp = Response::new(Body::from("Not Found"));
    *resp.status_mut() = StatusCode::NOT_FOUND;
    resp
}

#[cfg(not(feature = "bundled-dashboard"))]
fn not_bundled_stub() -> Response {
    let mut resp = Response::new(Body::from(
        "Fluxx api is running but the dashboard bundle was not built into this binary.\n\
         To enable, build the dashboard and rebuild with the feature on:\n  \
           cd ../node.dashboard.fluxx.vexarr.com && npm install && npm run build\n  \
           cargo build -p fluxx-api --features bundled-dashboard\n",
    ));
    *resp.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
    resp.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/plain; charset=utf-8"),
    );
    resp
}

/// `true` when the bundle was embedded at build time. Surfaced in the
/// boot log so operators can see "dashboard bundled" / "dashboard
/// missing" without having to curl the endpoint.
#[cfg(feature = "bundled-dashboard")]
pub fn is_bundled() -> bool {
    DashboardAssets::iter().next().is_some()
}

#[cfg(not(feature = "bundled-dashboard"))]
pub fn is_bundled() -> bool {
    false
}

/// Minimal MIME-type lookup for the file extensions Vite emits.
/// Inlined to avoid pulling `mime_guess` into fluxx-api.
#[cfg(feature = "bundled-dashboard")]
fn content_type_for(path: &str) -> &'static str {
    let ext = path.rsplit('.').next().unwrap_or("");
    match ext {
        "html" | "htm" => "text/html; charset=utf-8",
        "js" | "mjs" => "application/javascript; charset=utf-8",
        "css" => "text/css; charset=utf-8",
        "json" => "application/json",
        "svg" => "image/svg+xml",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "ico" => "image/x-icon",
        "woff" => "font/woff",
        "woff2" => "font/woff2",
        "ttf" => "font/ttf",
        "txt" => "text/plain; charset=utf-8",
        "webmanifest" => "application/manifest+json",
        "map" => "application/json",
        _ => "application/octet-stream",
    }
}
