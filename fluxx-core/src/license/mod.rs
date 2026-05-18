//! Fluxx license & runtime-grant client surface.
//!
//! The classic free-tier license verify lives in this file (it predates the
//! grant subsystem). Premium agent operations should go through the new
//! grant client in [`grant`] — see that module's docs for the full lifecycle.

pub mod cache;
pub mod cert_pin;
pub mod fingerprint;
pub mod grant;
pub mod signing;

use serde::{Deserialize, Serialize};
use tracing::{info, warn};

const DEFAULT_API_URL: &str = "https://api.fluxx.dev";

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LicenseInfo {
    #[serde(default = "_default_valid")]
    pub valid: bool,
    pub tier: Option<String>,
    pub limits: Option<TierLimits>,
    pub expires_at: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TierLimits {
    pub agents: i64,
    pub events_per_day: i64,
    pub retention_days: i64,
    pub integrations: i64,
}

#[derive(Debug, Deserialize)]
struct ApiEnvelope {
    status: String,
    data: Option<LicenseInfo>,
    #[serde(rename = "responseMessage")]
    response_message: String,
}

fn _default_valid() -> bool { true }

pub async fn validate_subscription() -> Result<LicenseInfo, String> {
    let api_key = std::env::var("FLUXX_API_KEY")
        .map_err(|_| "FLUXX_API_KEY environment variable is not set.\n\nTo use Fluxx, you need an active subscription.\n  1. Sign up at https://dashboard.fluxx.dev\n  2. Choose a plan (Free tier available)\n  3. Copy your API key from the dashboard\n  4. Set it: export FLUXX_API_KEY=flx_your_key_here".to_string())?;

    if api_key.is_empty() {
        return Err("FLUXX_API_KEY is empty. Set a valid API key to continue.".to_string());
    }

    let api_url = std::env::var("FLUXX_API_URL")
        .unwrap_or_else(|_| DEFAULT_API_URL.to_string());

    let url = format!("{}/api/v1/license/verify?key={}", api_url, urlencoding::encode(&api_key));
    info!("Validating subscription against {}", api_url);

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| format!("Failed to create HTTP client: {}", e))?;

    let response = client
        .get(&url)
        .send()
        .await
        .map_err(|e| {
            if std::env::var("FLUXX_OFFLINE_MODE").is_ok() {
                warn!("License validation failed (offline mode enabled, continuing): {}", e);
                return "OFFLINE_OK".to_string();
            }
            format!(
                "Cannot reach Fluxx API at {}.\n  Check your network connection or set FLUXX_API_URL if self-hosting.\n  Error: {}",
                api_url, e
            )
        })?;

    if !response.status().is_success() {
        return Err(format!(
            "License validation failed with HTTP {}.\n  Ensure your API key is correct.",
            response.status()
        ));
    }

    let envelope: ApiEnvelope = response
        .json()
        .await
        .map_err(|e| format!("Invalid response from license server: {}", e))?;

    if envelope.status != "SUCCESS" {
        return Err(format!("License check failed: {}", envelope.response_message));
    }

    let license = envelope.data.ok_or("No license data in response")?;

    if !license.valid {
        return Err(
            "Your API key is invalid or your subscription is not active.\n\n  Manage your subscription at https://dashboard.fluxx.dev/settings".to_string()
        );
    }

    info!(
        "Subscription validated: tier={}, expires={}",
        license.tier.as_deref().unwrap_or("unknown"),
        license.expires_at.as_deref().unwrap_or("unknown")
    );

    Ok(license)
}

pub fn print_license_error(msg: &str) {
    eprintln!();
    eprintln!("  \x1b[1;31mSubscription Required\x1b[0m");
    eprintln!("  {}", "─".repeat(40));
    for line in msg.lines() {
        eprintln!("  {}", line);
    }
    eprintln!();
    eprintln!("  \x1b[2mFluxx requires an active subscription to run.");
    eprintln!("  Sign up free at https://dashboard.fluxx.dev\x1b[0m");
    eprintln!();
}
