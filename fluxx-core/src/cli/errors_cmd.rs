//! `fluxx errors` — terminal access to the local agent's `/v1/errors/*`.
//!
//! Designed for CI gates and quick triage from the shell. Output is
//! plain text aligned columns, not JSON, because the primary consumer
//! is a human at a terminal. `--json` is available on every subcommand
//! when a machine wants to consume the raw envelope.
//!
//! The base URL is taken from `--url`, otherwise the `FLUXX_AGENT_URL`
//! env var, otherwise `http://127.0.0.1:7007`. Agent listens on this
//! port by default (see `src/api/server.rs` mount).

use clap::{Args, Subcommand};
use serde::Deserialize;

#[derive(Debug, Args)]
pub struct ErrorsArgs {
    /// Override the agent URL. Defaults to FLUXX_AGENT_URL or 127.0.0.1:7007.
    #[arg(long, global = true)]
    pub url: Option<String>,

    /// Print the raw JSON envelope instead of the aligned-columns view.
    #[arg(long, global = true)]
    pub json: bool,

    #[command(subcommand)]
    pub cmd: ErrorsCmd,
}

#[derive(Debug, Subcommand)]
pub enum ErrorsCmd {
    /// List error groups in the window (default 24h).
    List {
        /// Window in hours. Defaults to 24.
        #[arg(long, default_value_t = 24)]
        since_hours: u32,
    },
    /// Top N error groups by occurrence count.
    Top {
        /// Number of rows. Defaults to 10.
        #[arg(short, long, default_value_t = 10)]
        n: usize,
        /// Window in hours.
        #[arg(long, default_value_t = 24)]
        since_hours: u32,
    },
    /// Hour-of-day distribution as a sparkline.
    Trend {
        #[arg(long, default_value_t = 24)]
        since_hours: u32,
    },
    /// Aggregate counters.
    Stats,
}

#[derive(Debug, Deserialize)]
struct GroupSnapshot {
    fingerprint: String,
    error_type: String,
    message: String,
    // first/last_seen_us are part of the wire shape but not displayed
    // in the table view (the relative "Xm ago" math lives in the UI).
    // last_release is similarly carried for compatibility; only
    // first_release shows up in the columns.
    #[serde(default)]
    #[allow(dead_code)]
    first_seen_us: u64,
    #[serde(default)]
    #[allow(dead_code)]
    last_seen_us: u64,
    count: u64,
    #[serde(default)]
    services: Vec<String>,
    #[serde(default)]
    first_release: String,
    #[serde(default)]
    #[allow(dead_code)]
    last_release: String,
}

#[derive(Debug, Deserialize)]
struct Trend {
    buckets: Vec<u64>,
}

#[derive(Debug, Deserialize)]
struct Stats {
    total_groups: u64,
    total_occurrences: u64,
}

#[derive(Debug, Deserialize)]
struct Envelope<T> {
    data: T,
    // Carried for forward-compat with the wire envelope; the CLI
    // currently only displays the unwrapped `data`.
    #[serde(default)]
    #[allow(dead_code)]
    status: String,
    #[serde(default)]
    #[allow(dead_code)]
    response_message: String,
}

pub async fn run(args: ErrorsArgs) -> anyhow::Result<()> {
    let base = resolve_base_url(args.url.as_deref());
    let http = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()?;

    match args.cmd {
        ErrorsCmd::List { since_hours } => {
            let url = format!("{}/v1/errors?since_hours={}", base, since_hours);
            let body = fetch_raw(&http, &url).await?;
            if args.json {
                println!("{}", body);
                return Ok(());
            }
            let env: Envelope<Vec<GroupSnapshot>> = serde_json::from_str(&body)?;
            print_groups(&env.data);
        }
        ErrorsCmd::Top { n, since_hours } => {
            let url = format!("{}/v1/errors/top?n={}&since_hours={}", base, n, since_hours);
            let body = fetch_raw(&http, &url).await?;
            if args.json {
                println!("{}", body);
                return Ok(());
            }
            let env: Envelope<Vec<GroupSnapshot>> = serde_json::from_str(&body)?;
            print_groups(&env.data);
        }
        ErrorsCmd::Trend { since_hours } => {
            let url = format!("{}/v1/errors/trend?since_hours={}", base, since_hours);
            let body = fetch_raw(&http, &url).await?;
            if args.json {
                println!("{}", body);
                return Ok(());
            }
            let env: Envelope<Trend> = serde_json::from_str(&body)?;
            print_trend(&env.data.buckets);
        }
        ErrorsCmd::Stats => {
            let url = format!("{}/v1/errors/stats", base);
            let body = fetch_raw(&http, &url).await?;
            if args.json {
                println!("{}", body);
                return Ok(());
            }
            let env: Envelope<Stats> = serde_json::from_str(&body)?;
            println!("groups       {}", env.data.total_groups);
            println!("occurrences  {}", env.data.total_occurrences);
        }
    }

    Ok(())
}

fn resolve_base_url(flag: Option<&str>) -> String {
    if let Some(u) = flag {
        return u.trim_end_matches('/').to_string();
    }
    if let Ok(u) = std::env::var("FLUXX_AGENT_URL") {
        if !u.is_empty() {
            return u.trim_end_matches('/').to_string();
        }
    }
    "http://127.0.0.1:7007".to_string()
}

async fn fetch_raw(http: &reqwest::Client, url: &str) -> anyhow::Result<String> {
    let mut req = http.get(url);
    if let Ok(token) = std::env::var("FLUXX_ADMIN_TOKEN") {
        if !token.is_empty() {
            req = req.bearer_auth(token);
        }
    }
    let resp = req.send().await
        .map_err(|e| anyhow::anyhow!("Failed to reach agent at {}: {}", url, e))?;
    if resp.status() == reqwest::StatusCode::UNAUTHORIZED {
        anyhow::bail!(
            "Agent returned 401 Unauthorized. Set FLUXX_ADMIN_TOKEN to \
             the token printed by `fluxx start` (or stored in \
             /var/lib/fluxx/admin.token.sha256's matching plaintext)."
        );
    }
    if !resp.status().is_success() {
        let status = resp.status();
        let body = resp.text().await.unwrap_or_default();
        anyhow::bail!("Agent returned {}: {}", status, body);
    }
    let body = resp.text().await?;
    Ok(body)
}

fn print_groups(groups: &[GroupSnapshot]) {
    if groups.is_empty() {
        println!("(no errors in the requested window)");
        return;
    }

    // Header
    println!(
        "{:<8}  {:<5}  {:<10}  {:<22}  {:<18}  {}",
        "FP", "COUNT", "FIRST_REL", "TYPE", "SERVICES", "MESSAGE"
    );
    for g in groups {
        let fp_short = if g.fingerprint.len() >= 8 { &g.fingerprint[..8] } else { &g.fingerprint };
        let services = if g.services.is_empty() {
            "-".to_string()
        } else {
            g.services.join(",")
        };
        let services_trunc = truncate(&services, 18);
        let type_trunc = truncate(&g.error_type, 22);
        let msg_trunc = truncate(&g.message, 60);
        let first_rel = if g.first_release.is_empty() { "-" } else { g.first_release.as_str() };
        println!(
            "{:<8}  {:<5}  {:<10}  {:<22}  {:<18}  {}",
            fp_short, g.count, first_rel, type_trunc, services_trunc, msg_trunc
        );
    }
}

fn print_trend(buckets: &[u64]) {
    if buckets.is_empty() {
        println!("(no data)");
        return;
    }
    let max = buckets.iter().copied().max().unwrap_or(0).max(1);
    let blocks = [' ', '\u{2581}', '\u{2582}', '\u{2583}', '\u{2584}',
                  '\u{2585}', '\u{2586}', '\u{2587}', '\u{2588}'];
    let line: String = buckets
        .iter()
        .map(|&v| {
            let idx = ((v as f64 / max as f64) * 8.0).round() as usize;
            blocks[idx.min(8)]
        })
        .collect();
    println!("{}", line);
    println!("00          06          12          18          23");
    let total: u64 = buckets.iter().sum();
    println!("total {} events, peak bucket = {} events", total, max);
}

fn truncate(s: &str, max: usize) -> String {
    if s.chars().count() <= max {
        s.to_string()
    } else {
        let truncated: String = s.chars().take(max.saturating_sub(1)).collect();
        format!("{}…", truncated)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn truncate_respects_max() {
        assert_eq!(truncate("short", 10), "short");
        let t = truncate("abcdefghij", 5);
        assert!(t.chars().count() <= 5, "got {} chars", t.chars().count());
    }

    #[test]
    fn resolve_base_url_prefers_flag() {
        assert_eq!(resolve_base_url(Some("http://x:1/")), "http://x:1");
        assert_eq!(resolve_base_url(Some("http://x:1")), "http://x:1");
    }

    #[test]
    fn print_trend_handles_empty_buckets() {
        // Just confirm no panic on a tiny histogram.
        print_trend(&[]);
        print_trend(&[0, 1, 2, 0]);
    }
}
