//! Runtime grant client.
//!
//! Lifecycle:
//!   1. `mint(api_key)` calls `POST /v1/runtime/grant`. The server returns
//!      `{ grant_token, hmac_key, expires_at, features, quotas }`. We keep
//!      both in memory — the token to send back on every premium call, the
//!      key to sign per-request HMACs with.
//!   2. `current()` returns the live grant if one exists and isn't within
//!      the refresh window.
//!   3. `start_refresher(...)` spawns a background tokio task that calls
//!      `POST /v1/runtime/grant/refresh` every `refresh_interval` until
//!      cancelled.
//!   4. Premium handlers call `with_signed_request(...)` to attach the
//!      `Authorization`, `X-Fluxx-Nonce`, `X-Fluxx-Timestamp`, and
//!      `X-Fluxx-Signature` headers to their outbound request.
//!
//! In-memory only. The grant token never touches disk; it's short-lived by
//! design (~15 minutes) and re-mintable on demand.

use crate::license::fingerprint;
use crate::license::signing::{self, Signature};
use parking_lot::RwLock;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use tracing::{debug, warn};

/// Refresh window: refresh whenever the grant has less than this much
/// remaining lifetime. The control plane mints 15-minute grants, so this
/// gives us a 5-minute safety margin.
const REFRESH_BEFORE_EXPIRY: Duration = Duration::from_secs(5 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GrantTokenSet {
    pub grant_urn: String,
    pub grant_token: String,
    /// Per-grant HMAC key, hex. The agent uses this to sign every premium
    /// request body via `signing::sign(...)`.
    pub hmac_key: String,
    pub expires_at: String,
    pub ttl_seconds: u64,
    pub tier: String,
    pub features: Vec<String>,
    pub quotas: std::collections::HashMap<String, i64>,
    /// Local expiry tracking — `expires_at` parsed to a unix timestamp.
    /// Computed from `ttl_seconds` at mint time so we don't depend on
    /// parsing the server's ISO 8601 format.
    ///
    /// `pub(crate)` so the in-tree tests in
    /// `src/ai/diagnose_remote.rs` can construct one via a struct
    /// literal. External callers should still go through
    /// `build_grant` / `refresh_grant`.
    #[serde(skip)]
    pub(crate) expires_unix: u64,
}

impl GrantTokenSet {
    fn is_fresh_enough(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.expires_unix > now + REFRESH_BEFORE_EXPIRY.as_secs()
    }

    /// Does the grant include `feature`?
    pub fn has_feature(&self, feature: &str) -> bool {
        self.features.iter().any(|f| f == "*" || f == feature)
    }
}

#[derive(Debug, thiserror::Error)]
pub enum GrantError {
    #[error("Network error: {0}")]
    Network(String),
    #[error("Server rejected grant request ({status}): {message}")]
    ServerRejected { status: u16, message: String },
    #[error("Server returned malformed grant response: {0}")]
    Malformed(String),
    #[error("No active grant; mint one first")]
    NoActiveGrant,
}

/// Thread-safe in-memory store for the current grant. Use one per agent.
#[derive(Clone, Default)]
pub struct GrantStore {
    inner: Arc<RwLock<Option<GrantTokenSet>>>,
}

impl GrantStore {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn current(&self) -> Option<GrantTokenSet> {
        self.inner.read().clone()
    }

    pub fn replace(&self, grant: GrantTokenSet) {
        *self.inner.write() = Some(grant);
    }

    pub fn clear(&self) {
        *self.inner.write() = None;
    }
}

/// Configuration for the grant client. Pulled from env in production; tests
/// construct it directly.
#[derive(Debug, Clone)]
pub struct GrantConfig {
    pub api_base: String,
    pub api_key: String,
    pub agent_version: Option<String>,
    /// `reqwest::Client` HTTP timeout per call.
    pub http_timeout: Duration,
}

impl GrantConfig {
    pub fn from_env() -> Result<Self, GrantError> {
        let api_key = std::env::var("FLUXX_API_KEY")
            .map_err(|_| GrantError::ServerRejected {
                status: 0,
                message: "FLUXX_API_KEY not set".into(),
            })?;
        if api_key.is_empty() {
            return Err(GrantError::ServerRejected {
                status: 0,
                message: "FLUXX_API_KEY is empty".into(),
            });
        }
        let api_base = std::env::var("FLUXX_API_URL").unwrap_or_else(|_| "https://api.fluxx.dev".to_string());
        let agent_version = option_env!("CARGO_PKG_VERSION").map(|s| s.to_string());
        Ok(Self {
            api_base,
            api_key,
            agent_version,
            http_timeout: Duration::from_secs(10),
        })
    }
}

#[derive(Deserialize)]
struct GrantEnvelope {
    status: String,
    #[serde(rename = "responseMessage")]
    response_message: String,
    data: Option<RawGrant>,
}

#[derive(Deserialize)]
struct RawGrant {
    grant_urn: String,
    grant_token: String,
    hmac_key: String,
    expires_at: String,
    ttl_seconds: u64,
    tier: String,
    features: Vec<String>,
    quotas: std::collections::HashMap<String, i64>,
}

/// Mint a fresh grant. Replaces any existing one in the store.
pub async fn mint(client: &Client, cfg: &GrantConfig, store: &GrantStore) -> Result<GrantTokenSet, GrantError> {
    let url = format!("{}/api/v1/runtime/grant", cfg.api_base.trim_end_matches('/'));
    let body = serde_json::json!({
        "machine_fingerprint_sha256": fingerprint::current(),
        "agent_version": cfg.agent_version,
    });
    let resp = client
        .post(&url)
        .header("X-API-Key", &cfg.api_key)
        .json(&body)
        .timeout(cfg.http_timeout)
        .send()
        .await
        .map_err(|e| GrantError::Network(e.to_string()))?;
    let status = resp.status().as_u16();
    let envelope: GrantEnvelope = resp
        .json()
        .await
        .map_err(|e| GrantError::Malformed(e.to_string()))?;
    if envelope.status != "SUCCESS" {
        return Err(GrantError::ServerRejected { status, message: envelope.response_message });
    }
    let raw = envelope.data.ok_or_else(|| GrantError::Malformed("missing data".into()))?;
    let grant = build_grant(raw);
    store.replace(grant.clone());
    Ok(grant)
}

/// Refresh the existing grant. If the previous grant has gone too stale to
/// refresh (server returns FINGERPRINT_MISMATCH etc.), the caller should
/// fall back to `mint`.
pub async fn refresh(
    client: &Client,
    cfg: &GrantConfig,
    store: &GrantStore,
) -> Result<GrantTokenSet, GrantError> {
    let previous = store.current().ok_or(GrantError::NoActiveGrant)?;
    let url = format!("{}/api/v1/runtime/grant/refresh", cfg.api_base.trim_end_matches('/'));
    let body = serde_json::json!({
        "previous_grant_token": previous.grant_token,
        "machine_fingerprint_sha256": fingerprint::current(),
        "agent_version": cfg.agent_version,
    });
    let resp = client
        .post(&url)
        .header("X-API-Key", &cfg.api_key)
        .json(&body)
        .timeout(cfg.http_timeout)
        .send()
        .await
        .map_err(|e| GrantError::Network(e.to_string()))?;
    let status = resp.status().as_u16();
    let envelope: GrantEnvelope = resp
        .json()
        .await
        .map_err(|e| GrantError::Malformed(e.to_string()))?;
    if envelope.status != "SUCCESS" {
        return Err(GrantError::ServerRejected { status, message: envelope.response_message });
    }
    let raw = envelope.data.ok_or_else(|| GrantError::Malformed("missing data".into()))?;
    let grant = build_grant(raw);
    store.replace(grant.clone());
    Ok(grant)
}

fn build_grant(raw: RawGrant) -> GrantTokenSet {
    let now = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_secs()).unwrap_or(0);
    GrantTokenSet {
        grant_urn: raw.grant_urn,
        grant_token: raw.grant_token,
        hmac_key: raw.hmac_key,
        expires_at: raw.expires_at,
        ttl_seconds: raw.ttl_seconds,
        tier: raw.tier,
        features: raw.features,
        quotas: raw.quotas,
        expires_unix: now + raw.ttl_seconds,
    }
}

/// Spawn a background refresh task. The task ticks every 60 s, and refreshes
/// whenever the grant has less than `REFRESH_BEFORE_EXPIRY` remaining. If
/// refresh fails (e.g. revoked), the store is cleared so the next premium
/// call surfaces the error instead of using a stale grant.
pub fn start_refresher(
    client: Client,
    cfg: GrantConfig,
    store: GrantStore,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(60));
        loop {
            tick.tick().await;
            let needs_refresh = store
                .current()
                .map(|g| !g.is_fresh_enough())
                .unwrap_or(true);
            if !needs_refresh {
                continue;
            }
            let result = if store.current().is_some() {
                refresh(&client, &cfg, &store).await
            } else {
                mint(&client, &cfg, &store).await
            };
            match result {
                Ok(g) => debug!(
                    grant_urn = %g.grant_urn,
                    tier = %g.tier,
                    expires_in = g.ttl_seconds,
                    "refreshed runtime grant"
                ),
                Err(e) => {
                    warn!(error = %e, "grant refresh failed; clearing in-memory grant");
                    store.clear();
                }
            }
        }
    })
}

/// Wrap an HTTP RequestBuilder with the grant + signing headers. Use this
/// from every premium call:
///
/// ```ignore
/// let body = serde_json::to_vec(&payload)?;
/// let req = client.post(url).body(body.clone());
/// let req = with_signed_request(req, &grant, &body)?;
/// let resp = req.send().await?;
/// ```
///
/// The caller is responsible for serializing the body and passing the
/// exact same bytes to both the body and the signer. Re-serialization
/// elsewhere will produce a mismatched signature.
pub fn with_signed_request(
    req: reqwest::RequestBuilder,
    grant: &GrantTokenSet,
    body: &[u8],
) -> Result<reqwest::RequestBuilder, signing::SigningError> {
    let sig: Signature = signing::sign(body, &grant.hmac_key)?;
    Ok(req
        .header("Authorization", format!("Bearer {}", grant.grant_token))
        .header("X-Fluxx-Nonce", sig.nonce)
        .header("X-Fluxx-Timestamp", sig.timestamp.to_string())
        .header("X-Fluxx-Signature", sig.signature))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_raw(features: Vec<&str>) -> RawGrant {
        RawGrant {
            grant_urn: "urn:fluxx:grant:1".into(),
            grant_token: "flxg.v1.payload.sig".into(),
            hmac_key: "00".repeat(32),
            expires_at: "2099-01-01T00:00:00Z".into(),
            ttl_seconds: 900,
            tier: "pro".into(),
            features: features.into_iter().map(String::from).collect(),
            quotas: std::collections::HashMap::new(),
        }
    }

    #[test]
    fn store_replace_and_clear_round_trip() {
        let store = GrantStore::new();
        assert!(store.current().is_none());
        store.replace(build_grant(fake_raw(vec!["ai_diagnostics"])));
        assert!(store.current().is_some());
        store.clear();
        assert!(store.current().is_none());
    }

    #[test]
    fn has_feature_matches_listed_feature() {
        let g = build_grant(fake_raw(vec!["ai_diagnostics", "auto_remediation"]));
        assert!(g.has_feature("ai_diagnostics"));
        assert!(g.has_feature("auto_remediation"));
        assert!(!g.has_feature("custom_models"));
    }

    #[test]
    fn wildcard_feature_grants_any() {
        let g = build_grant(fake_raw(vec!["*"]));
        assert!(g.has_feature("any_thing"));
        assert!(g.has_feature("ai_diagnostics"));
    }

    #[test]
    fn freshly_minted_grant_is_fresh_enough() {
        let g = build_grant(fake_raw(vec!["ai_diagnostics"]));
        assert!(g.is_fresh_enough());
    }

    #[test]
    fn near_expiry_grant_is_not_fresh_enough() {
        let mut g = build_grant(fake_raw(vec!["ai_diagnostics"]));
        // Pretend it was minted ~14 minutes ago, so only ~1 minute remains.
        g.expires_unix = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
            + 60;
        assert!(!g.is_fresh_enough());
    }

    #[test]
    fn with_signed_request_attaches_required_headers() {
        let client = reqwest::Client::new();
        let grant = build_grant(fake_raw(vec!["ai_diagnostics"]));
        let body = b"{\"q\":\"why\"}";
        let req = client.post("https://api.fluxx.dev/api/v1/ai/diagnose");
        let req = with_signed_request(req, &grant, body).unwrap();
        let built = req.build().unwrap();
        let headers = built.headers();
        assert!(headers.get("Authorization").is_some());
        assert!(headers.get("X-Fluxx-Nonce").is_some());
        assert!(headers.get("X-Fluxx-Timestamp").is_some());
        assert!(headers.get("X-Fluxx-Signature").is_some());
        assert!(
            headers
                .get("Authorization")
                .unwrap()
                .to_str()
                .unwrap()
                .starts_with("Bearer flxg.v1.")
        );
    }
}
