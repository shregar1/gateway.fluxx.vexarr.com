//! Embed the per-node dashboard's `dist/` bundle into the agent
//! binary so a self-hosted user runs **one** `fluxx` process — no
//! separate Vite dev server, no proxy.
//!
//! At build time `rust-embed` walks
//! `node.dashboard.fluxx.dev/dist/` and inlines every file. At
//! runtime this module mounts:
//!
//!   GET /                  → embedded `index.html`
//!   GET /assets/<file>     → embedded asset, served with the right
//!                            mime + an `immutable` cache header
//!                            (Vite stamps content hashes into the
//!                            filename so we can be aggressive).
//!   GET /<anything else>   → falls through to `index.html` so the
//!                            React-router/SPA routes deep-link.
//!
//! If `dist/` is missing at compile time, every request 404s with a
//! "build the dashboard first" hint. The agent keeps booting either
//! way — bundling failure must not take the API down.

use axum::{
    body::Body,
    extract::Path,
    http::{header, HeaderValue, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use rust_embed::RustEmbed;

#[derive(RustEmbed)]
#[folder = "node.dashboard.fluxx.dev/dist/"]
struct DashboardAssets;

/// Build the static-file router for the dashboard. Designed to be
/// `.merge()`'d into the main `Router` *after* the `/v1/*` API routes
/// so that the SPA fallback never shadows real endpoints.
pub fn router() -> Router {
    Router::new()
        .route("/", get(serve_index))
        .route("/index.html", get(serve_index))
        .route("/assets/:file", get(serve_asset))
        // Other dashboard top-level files (favicon, manifest, robots).
        .route("/favicon.ico", get(serve_named("favicon.ico")))
        .route("/manifest.webmanifest", get(serve_named("manifest.webmanifest")))
        .route("/robots.txt", get(serve_named("robots.txt")))
        // Catch-all for deep links into the SPA. This must be the
        // last route attached to the dashboard sub-router so it
        // doesn't compete with the asset routes above.
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
        // 1 year max-age matches the convention.
        resp.headers_mut().insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=31536000, immutable"),
        );
    }
    resp
}

async fn serve_spa_fallback() -> Response {
    // SPA routing: anything we don't have a real handler for and that
    // isn't an asset path should render the dashboard shell so the
    // client-side router can take over.
    serve_path("index.html")
}

/// Builder for one-off static handlers like `favicon.ico`.
fn serve_named(file: &'static str) -> impl Fn() -> std::pin::Pin<Box<dyn std::future::Future<Output = Response> + Send>> + Clone {
    move || {
        let f = file.to_string();
        Box::pin(async move { serve_path(&f) })
    }
}

fn serve_path(path: &str) -> Response {
    match DashboardAssets::get(path) {
        Some(content) => {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            let mut resp = Response::new(Body::from(content.data.into_owned()));
            resp.headers_mut().insert(
                header::CONTENT_TYPE,
                HeaderValue::from_str(mime.as_ref()).unwrap_or_else(|_| {
                    HeaderValue::from_static("application/octet-stream")
                }),
            );
            resp
        }
        None => not_found_or_hint(),
    }
}

/// When the bundle is absent (e.g. the dashboard wasn't built before
/// the binary was compiled) we serve a one-line plain-text hint at `/`
/// so operators don't get a confusing blank page. API endpoints under
/// `/v1/*` keep working regardless.
fn not_found_or_hint() -> Response {
    if DashboardAssets::iter().next().is_none() {
        let mut resp = Response::new(Body::from(
            "Fluxx agent is running but the dashboard bundle was not built into this binary.\n\
             To fix:\n  cd node.dashboard.fluxx.dev && npm install && npm run build\n  cargo build\n",
        ));
        *resp.status_mut() = StatusCode::SERVICE_UNAVAILABLE;
        resp.headers_mut().insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static("text/plain; charset=utf-8"),
        );
        return resp;
    }
    let mut resp = Response::new(Body::from("Not Found"));
    *resp.status_mut() = StatusCode::NOT_FOUND;
    resp
}

/// `true` when the bundle was embedded at build time. Surfaced in the
/// boot log so operators can see "dashboard bundled" / "dashboard
/// missing" without having to curl the endpoint.
pub fn is_bundled() -> bool {
    DashboardAssets::iter().next().is_some()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use tower::ServiceExt;

    #[test]
    fn is_bundled_reflects_dist_state() {
        // We can't assert the value because it depends on whether the
        // dashboard was built before `cargo test`, but the function
        // must not panic and must return a definite bool.
        let _ = is_bundled();
    }

    fn req(path: &str) -> Request<Body> {
        Request::builder().uri(path).body(Body::empty()).unwrap()
    }

    #[tokio::test]
    async fn root_serves_index_when_bundled() {
        if !is_bundled() {
            // Without `dist/`, the router serves a 503 hint instead of
            // the real index — that branch is covered by
            // `missing_bundle_returns_503_hint` below.
            return;
        }
        let resp = router().oneshot(req("/")).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let ct = resp.headers().get(header::CONTENT_TYPE).cloned();
        let body = to_bytes(resp.into_body(), 1 << 20).await.unwrap();
        assert!(
            ct.as_ref().map(|v| v.to_str().unwrap_or("")).unwrap_or("").starts_with("text/html"),
            "/ should serve text/html, got {ct:?}",
        );
        let html = std::str::from_utf8(&body).unwrap();
        assert!(html.contains("<html"), "/ body should be the dashboard shell");
    }

    #[tokio::test]
    async fn spa_fallback_serves_index_for_deep_links() {
        if !is_bundled() {
            return;
        }
        let resp = router().oneshot(req("/keys/some-deep-link")).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "deep links must fall through to index.html so the SPA router can take over",
        );
    }

    #[tokio::test]
    async fn assets_are_served_with_immutable_cache_header() {
        if !is_bundled() {
            return;
        }
        // Find a real hashed asset name from the embedded bundle.
        let asset_path = DashboardAssets::iter()
            .find(|p| p.starts_with("assets/"))
            .map(|p| p.to_string());
        let Some(path) = asset_path else { return };
        let uri = format!("/{path}");
        let resp = router().oneshot(req(&uri)).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "asset {uri} should serve 200");
        let cc = resp
            .headers()
            .get(header::CACHE_CONTROL)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        assert!(
            cc.contains("immutable") && cc.contains("max-age=31536000"),
            "hashed assets must be served immutable+1yr, got {cc:?}",
        );
    }

    #[tokio::test]
    async fn missing_asset_returns_404() {
        let resp = router().oneshot(req("/assets/does-not-exist.js")).await.unwrap();
        // If the bundle is absent altogether we return 503; with a real
        // bundle a missing asset is a plain 404.
        let expected = if is_bundled() { StatusCode::NOT_FOUND } else { StatusCode::SERVICE_UNAVAILABLE };
        assert_eq!(resp.status(), expected);
    }
}
