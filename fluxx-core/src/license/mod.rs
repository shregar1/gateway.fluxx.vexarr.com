//! Fluxx license & runtime-grant client surface.
//!
//! The classic free-tier license verify lives in this file (it predates the
//! grant subsystem). Premium agent operations should go through the new
//! grant client in [`grant`] — see that module's docs for the full lifecycle.
//!
//! The HTTP plumbing (env-var resolution, URL building, offline fallback,
//! ANSI error banner) now lives in `utilx::license::simple`. This file
//! pins the fluxx-specific shape (`LicenseInfo` / `TierLimits` types,
//! `FLUXX_API_KEY` / `FLUXX_API_URL` env names) and the human-readable
//! error messages.

pub mod cache;
pub mod cert_pin;
pub mod fingerprint;
pub mod grant;
pub mod signing;

use serde::{Deserialize, Serialize};
use tracing::info;
use utilx::license::simple::{SimpleConfig, SimpleVerifyError, verify_simple};

const DEFAULT_API_URL: &str = "https://api.fluxx.dev";

/// Fluxx-shaped licence payload (what the server returns inside the
/// `data` field of the StandardResponse envelope).
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

fn _default_valid() -> bool {
    true
}

fn fluxx_simple_cfg() -> SimpleConfig {
    SimpleConfig::for_product(
        "FLUXX_API_KEY",
        "FLUXX_API_URL",
        "FLUXX_OFFLINE_MODE",
        DEFAULT_API_URL,
    )
}

pub async fn validate_subscription() -> Result<LicenseInfo, String> {
    let cfg = fluxx_simple_cfg();
    info!(
        "Validating subscription against {}",
        std::env::var(cfg.env_api_url).unwrap_or_else(|_| cfg.default_api_url.to_string())
    );

    match verify_simple::<LicenseInfo>(&cfg).await {
        Ok(license) => {
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
        Err(SimpleVerifyError::MissingApiKey { .. }) => Err(
            "FLUXX_API_KEY environment variable is not set.\n\nTo use Fluxx, you need an active subscription.\n  1. Sign up at https://dashboard.fluxx.dev\n  2. Choose a plan (Free tier available)\n  3. Copy your API key from the dashboard\n  4. Set it: export FLUXX_API_KEY=flx_your_key_here".to_string()
        ),
        Err(SimpleVerifyError::EmptyApiKey { .. }) => Err(
            "FLUXX_API_KEY is empty. Set a valid API key to continue.".to_string(),
        ),
        Err(SimpleVerifyError::HttpClient(e)) => {
            Err(format!("Failed to create HTTP client: {e}"))
        }
        Err(SimpleVerifyError::OfflineOk(_)) => Err("OFFLINE_OK".to_string()),
        Err(SimpleVerifyError::Network { url, cause }) => Err(format!(
            "Cannot reach Fluxx API at {url}.\n  Check your network connection or set FLUXX_API_URL if self-hosting.\n  Error: {cause}"
        )),
        Err(SimpleVerifyError::BadStatus(s)) => Err(format!(
            "License validation failed with HTTP {s}.\n  Ensure your API key is correct."
        )),
        Err(SimpleVerifyError::BadJson(e)) => {
            Err(format!("Invalid response from license server: {e}"))
        }
        Err(SimpleVerifyError::ServerFailed(msg)) => {
            Err(format!("License check failed: {msg}"))
        }
        Err(SimpleVerifyError::MissingData) => Err("No license data in response".to_string()),
        // `SimpleVerifyError` is `#[non_exhaustive]` so this catch-all
        // future-proofs us against utilx adding new variants.
        Err(other) => Err(format!("License check failed: {other}")),
    }
}

/// Print the fluxx-styled subscription-required banner to stderr.
pub fn print_license_error(msg: &str) {
    utilx::license::simple::print_error_banner(
        "Subscription Required",
        msg,
        "Fluxx requires an active subscription to run.\n  Sign up free at https://dashboard.fluxx.dev",
    );
}
