//! `fluxx release` — declare an application release to api.fluxx.dev.
//!
//! The agent's per-event release tag (`service.version` resource attr
//! or `RumEvent.app_version`) is what populates the dashboard. CI
//! typically also wants to attach commit/author/ref metadata to a
//! release once — this subcommand is the entry point.
//!
//! POSTs to `<FLUXX_API_URL>/api/v1/app-releases` with API-key auth.
//! Auth source: `FLUXX_API_KEY` env var. The endpoint is idempotent on
//! `(org, project, version)` so re-running with richer flags enriches
//! the existing row.

use clap::{Args, Subcommand};
use serde::Deserialize;
use serde_json::json;

#[derive(Debug, Args)]
pub struct ReleaseArgs {
    #[command(subcommand)]
    pub cmd: ReleaseCmd,
}

#[derive(Debug, Subcommand)]
pub enum ReleaseCmd {
    /// Declare a release. Idempotent on (project, version).
    Declare(DeclareArgs),
    /// Upload JS sourcemaps to the local agent so JS frames get
    /// symbolicated on ingest. Auth via FLUXX_ADMIN_TOKEN.
    Sourcemaps(UploadSourcemapsArgs),
}

#[derive(Debug, Args)]
pub struct UploadSourcemapsArgs {
    /// Project (release) tag the sourcemap belongs to. Must match
    /// the `release` your SDK reports.
    #[arg(short, long)]
    pub release: String,

    /// One or more <bundle_url>=<path> pairs. The bundle URL is what
    /// the SDK reports in `error_filename` (typically the asset URL
    /// the browser fetched). The path is the local `.map` file.
    ///
    /// Example: --pair https://app/static/main.abc.js=dist/main.abc.js.map
    #[arg(short, long, value_name = "URL=PATH")]
    pub pair: Vec<String>,

    /// Override the local agent URL. Defaults to FLUXX_AGENT_URL
    /// or http://127.0.0.1:7007.
    #[arg(long)]
    pub url: Option<String>,
}

#[derive(Debug, Args)]
pub struct DeclareArgs {
    /// Project identifier. Matches the SDK `projectId` you initialise with.
    #[arg(short, long)]
    pub project: String,

    /// Release version string. e.g. "1.4.2", "build-9023", or a git SHA.
    #[arg(short, long)]
    pub version: String,

    /// Commit SHA (any length the host accepts; full or short is fine).
    #[arg(long)]
    pub commit_sha: Option<String>,

    /// Author of the release (email, name, or anything human-readable).
    #[arg(long)]
    pub author: Option<String>,

    /// Git ref the release was cut from, e.g. "main" or "release/1.4.x".
    #[arg(long)]
    pub r#ref: Option<String>,

    /// Override the control-plane URL. Defaults to FLUXX_API_URL,
    /// then https://api.fluxx.dev.
    #[arg(long)]
    pub api_url: Option<String>,
}

#[derive(Debug, Deserialize)]
struct AppRelease {
    urn: String,
    version: String,
    #[serde(default)]
    commit_sha: Option<String>,
    #[serde(default)]
    deployed_at: String,
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    data: T,
    status: String,
    #[serde(rename = "responseMessage", default)]
    response_message: String,
}

pub async fn run(args: ReleaseArgs) -> anyhow::Result<()> {
    match args.cmd {
        ReleaseCmd::Declare(d) => declare(d).await,
        ReleaseCmd::Sourcemaps(u) => upload_sourcemaps(u).await,
    }
}

/// Upload each `<URL>=<PATH>` pair as a multipart form post to the
/// local agent's `/v1/sourcemaps`. Each pair is one HTTP request; the
/// server dedupes by content hash so re-running is cheap.
async fn upload_sourcemaps(args: UploadSourcemapsArgs) -> anyhow::Result<()> {
    if args.pair.is_empty() {
        anyhow::bail!("No --pair arguments supplied. Usage: --pair https://app/main.abc.js=dist/main.abc.js.map");
    }

    let base = if let Some(u) = args.url {
        u.trim_end_matches('/').to_string()
    } else if let Ok(u) = std::env::var("FLUXX_AGENT_URL") {
        u.trim_end_matches('/').to_string()
    } else {
        "http://127.0.0.1:7007".to_string()
    };
    let token = std::env::var("FLUXX_ADMIN_TOKEN").ok();

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(30))
        .build()?;

    let mut ok = 0usize;
    let mut bad = 0usize;
    for pair in &args.pair {
        let (bundle_url, path) = match pair.split_once('=') {
            Some(p) => p,
            None => {
                eprintln!("✘ skipping malformed pair: {} (expected URL=PATH)", pair);
                bad += 1;
                continue;
            }
        };
        let body = match std::fs::read(path) {
            Ok(b) => b,
            Err(e) => {
                eprintln!("✘ {}: cannot read {}: {}", bundle_url, path, e);
                bad += 1;
                continue;
            }
        };
        let form = reqwest::multipart::Form::new()
            .text("release", args.release.clone())
            .text("bundle_url", bundle_url.to_string())
            .part(
                "map",
                reqwest::multipart::Part::bytes(body)
                    .file_name(path.split('/').next_back().unwrap_or("map").to_string())
                    .mime_str("application/json")?,
            );
        let mut req = http.post(format!("{}/v1/sourcemaps", base)).multipart(form);
        if let Some(t) = &token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().await
            .map_err(|e| anyhow::anyhow!("Failed to reach agent at {}: {}", base, e))?;
        if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
            anyhow::bail!("Agent returned 401. Set FLUXX_ADMIN_TOKEN to the agent's admin token.");
        }
        if resp.status().is_success() {
            println!("✔ {}  ←  {}", bundle_url, path);
            ok += 1;
        } else {
            let status = resp.status();
            let body = resp.text().await.unwrap_or_default();
            eprintln!("✘ {} → {}: {}", bundle_url, status, body);
            bad += 1;
        }
    }
    println!("uploaded {} ok, {} failed", ok, bad);
    if bad > 0 {
        std::process::exit(1);
    }
    Ok(())
}

async fn declare(args: DeclareArgs) -> anyhow::Result<()> {
    let base = resolve_api_url(args.api_url.as_deref());
    let api_key = std::env::var("FLUXX_API_KEY").map_err(|_| {
        anyhow::anyhow!(
            "FLUXX_API_KEY is not set. Generate a key in the dashboard \
             (Account → API Keys) and export it: \
             export FLUXX_API_KEY=<key>"
        )
    })?;

    let url = format!("{}/api/v1/app-releases", base);
    let mut body = json!({
        "project_id": args.project,
        "version":    args.version,
    });
    if let Some(s) = &args.commit_sha {
        body["commit_sha"] = json!(s);
    }
    if let Some(a) = &args.author {
        body["author"] = json!(a);
    }
    if let Some(r) = &args.r#ref {
        body["ref"] = json!(r);
    }

    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(15))
        .build()?;

    let resp = http
        .post(&url)
        .header("Authorization", format!("Bearer {}", api_key))
        .json(&body)
        .send()
        .await
        .map_err(|e| anyhow::anyhow!("Failed to reach control plane at {}: {}", url, e))?;

    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Control plane returned {}: {}", status, body);
    }

    let body = resp.text().await?;
    let env: Envelope<AppRelease> =
        serde_json::from_str(&body).map_err(|e| anyhow::anyhow!("Bad response shape: {}", e))?;

    if env.status != "SUCCESS" {
        anyhow::bail!("Release declare failed: {}", env.response_message);
    }

    println!("✔ Release {} declared", env.data.version);
    println!("  urn         {}", env.data.urn);
    if let Some(c) = env.data.commit_sha {
        println!("  commit_sha  {}", c);
    }
    if !env.data.deployed_at.is_empty() {
        println!("  deployed_at {}", env.data.deployed_at);
    }
    Ok(())
}

fn resolve_api_url(flag: Option<&str>) -> String {
    if let Some(u) = flag {
        return u.trim_end_matches('/').to_string();
    }
    if let Ok(u) = std::env::var("FLUXX_API_URL") {
        if !u.is_empty() {
            return u.trim_end_matches('/').to_string();
        }
    }
    "https://api.fluxx.dev".to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resolve_api_url_prefers_flag_over_env() {
        assert_eq!(resolve_api_url(Some("https://staging.api/")), "https://staging.api");
    }

    #[test]
    fn resolve_api_url_strips_trailing_slash() {
        assert_eq!(resolve_api_url(Some("https://x/")), "https://x");
    }
}
