use axum::{
    http::{header, StatusCode, Uri},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use rust_embed::Embed;
use tracing::info;

/// Embed the dashboard static assets at compile time.
#[derive(Embed)]
#[folder = "dashboard/"]
struct DashboardAssets;

/// Serve the embedded dashboard on a stable port (API port + 1).
/// Returns the actual port the server bound to.
pub async fn start_dashboard_server(api_port: u16) -> anyhow::Result<u16> {
    let app = Router::new()
        // Serve a dynamic config script that tells the SPA where the API is
        .route("/config.js", get(move || async move {
            let js = format!(
                "window.__FLUXX_API_BASE__ = 'http://localhost:{}/v1';",
                api_port
            );
            Response::builder()
                .header(header::CONTENT_TYPE, "application/javascript")
                .body(js)
                .unwrap()
        }))
        // Root serves index.html
        .route("/", get(serve_root))
        // Everything else: try static file, then SPA fallback
        .fallback(get(serve_any));

    // Bind to API port + 1 for a stable, predictable URL
    let dash_port = api_port + 1;
    let listener = match tokio::net::TcpListener::bind(format!("0.0.0.0:{}", dash_port)).await {
        Ok(l) => l,
        Err(_) => {
            // Fallback to random port if preferred port is taken
            tokio::net::TcpListener::bind("0.0.0.0:0").await?
        }
    };
    let port = listener.local_addr()?.port();

    info!("Dashboard: Serving on http://localhost:{}", port);

    tokio::spawn(async move {
        axum::serve(listener, app).await.ok();
    });

    Ok(port)
}

/// Fallback handler: uses the full URI path to find embedded files.
async fn serve_any(uri: Uri) -> impl IntoResponse {
    let path = uri.path().trim_start_matches('/');

    // Try exact file match
    if !path.is_empty() {
        if let Some(file) = DashboardAssets::get(path) {
            let mime = mime_guess::from_path(path).first_or_octet_stream();
            return Response::builder()
                .header(header::CONTENT_TYPE, mime.as_ref())
                .header(header::CACHE_CONTROL, "public, max-age=31536000, immutable")
                .body(axum::body::Body::from(file.data.to_vec()))
                .unwrap();
        }
    }

    // SPA fallback: serve index.html for any unmatched route
    serve_index_html()
}

/// Serve the root index.html with the config script injected.
pub async fn serve_root() -> impl IntoResponse {
    serve_index_html()
}

fn serve_index_html() -> Response {
    match DashboardAssets::get("index.html") {
        Some(file) => {
            let html = String::from_utf8_lossy(&file.data);
            // Inject config.js script before the closing </head> tag
            let injected = html.replace(
                "</head>",
                r#"<script src="/config.js"></script></head>"#,
            );
            Response::builder()
                .header(header::CONTENT_TYPE, "text/html; charset=utf-8")
                .header(header::CACHE_CONTROL, "no-cache")
                .body(axum::body::Body::from(injected))
                .unwrap()
        }
        None => Response::builder()
            .status(StatusCode::NOT_FOUND)
            .body(axum::body::Body::from("Dashboard not found"))
            .unwrap(),
    }
}
