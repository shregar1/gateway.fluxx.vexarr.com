//! Worked example: how a premium agent operation calls the control plane.
//!
//! The agent's local AI handlers (RCA, auto-remediation, advanced anomaly,
//! etc.) all follow the same pattern:
//!
//! 1. Resolve the current runtime grant from [`GrantStore`]. Mint a fresh
//!    one if there isn't one yet (or if the previous mint failed).
//! 2. Check the grant carries the feature flag this operation needs.
//!    `requireFeature("ai_diagnostics")` on the server is the second line
//!    of defense; checking here gives a clean local error.
//! 3. Serialize the request body, compute the HMAC signature using the
//!    grant's `hmac_key`, attach the four headers (`Authorization`,
//!    `X-Fluxx-Nonce`, `X-Fluxx-Timestamp`, `X-Fluxx-Signature`), POST.
//! 4. Surface server errors as typed [`DiagnoseError`] variants so the
//!    CLI / dashboard can map them to user-facing messages.
//!
//! The same pattern works for `/v1/auto-remediation`, `/v1/mesh/federate`,
//! etc. — just change the URL, the body type, and the feature name.

use crate::license::grant::{self, GrantStore, GrantConfig, GrantError, with_signed_request};
use reqwest::Client;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize)]
pub struct DiagnoseRequest {
    pub question: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub events: Option<Vec<DiagnoseEvent>>,
}

#[derive(Debug, Clone, Serialize)]
pub struct DiagnoseEvent {
    pub timestamp: String,
    pub message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub level: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub service: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DiagnoseResponse {
    pub diagnosis: Diagnosis,
    pub usage: Usage,
    pub tier: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Diagnosis {
    pub summary: String,
    pub root_cause: String,
    pub evidence: Vec<String>,
    pub suggested_fix: String,
    pub confidence: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Usage {
    pub tokens_used: u32,
    pub latency_ms: u32,
    pub model: String,
}

#[derive(Debug, thiserror::Error)]
pub enum DiagnoseError {
    #[error("Grant not available: {0}")]
    GrantUnavailable(#[from] GrantError),
    #[error("This agent's grant does not include feature `{feature}` (tier `{tier}`). Upgrade your subscription to enable AI diagnostics.")]
    FeatureNotGranted { feature: String, tier: String },
    #[error("Failed to sign the request: {0}")]
    SignatureFailed(String),
    #[error("Network error: {0}")]
    Network(String),
    #[error("Server rejected the request ({status}): {message}")]
    Rejected { status: u16, message: String },
    #[error("Server returned malformed response: {0}")]
    Malformed(String),
}

#[derive(Deserialize)]
struct AiEnvelope {
    status: String,
    #[serde(rename = "responseMessage")]
    response_message: String,
    #[serde(rename = "responseKey")]
    response_key: String,
    data: Option<DiagnoseResponse>,
}

/// Call `POST /v1/ai/diagnose` with a signed grant.
///
/// On first call (and after a refresh failure), this lazily mints a grant.
/// In production, call [`grant::start_refresher`] once at agent startup so
/// the grant is always warm and this call doesn't pay a mint round-trip.
pub async fn diagnose(
    client: &Client,
    cfg: &GrantConfig,
    store: &GrantStore,
    req: &DiagnoseRequest,
) -> Result<DiagnoseResponse, DiagnoseError> {
    // 1. Resolve a grant.
    let mut grant_opt = store.current();
    if grant_opt.is_none() {
        grant_opt = Some(grant::mint(client, cfg, store).await?);
    }
    let grant = grant_opt.expect("just minted if missing");

    // 2. Local feature gate.
    if !grant.has_feature("ai_diagnostics") {
        return Err(DiagnoseError::FeatureNotGranted {
            feature: "ai_diagnostics".into(),
            tier: grant.tier.clone(),
        });
    }

    // 3. Serialize once — body bytes are signed AND sent. Re-serializing
    // would break the signature because key ordering and whitespace differ.
    let body_bytes = serde_json::to_vec(req)
        .map_err(|e| DiagnoseError::Malformed(e.to_string()))?;

    let url = format!("{}/api/v1/ai/diagnose", cfg.api_base.trim_end_matches('/'));
    let request = client.post(&url).body(body_bytes.clone());
    let request = with_signed_request(request, &grant, &body_bytes)
        .map_err(|e| DiagnoseError::SignatureFailed(e.to_string()))?
        .header("Content-Type", "application/json");

    let response = request
        .send()
        .await
        .map_err(|e| DiagnoseError::Network(e.to_string()))?;
    let status = response.status().as_u16();
    let envelope: AiEnvelope = response
        .json()
        .await
        .map_err(|e| DiagnoseError::Malformed(e.to_string()))?;
    if envelope.status != "SUCCESS" {
        return Err(DiagnoseError::Rejected {
            status,
            message: format!("[{}] {}", envelope.response_key, envelope.response_message),
        });
    }
    envelope
        .data
        .ok_or_else(|| DiagnoseError::Malformed("envelope missing data".into()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::license::grant::GrantTokenSet;

    fn fake_cfg() -> GrantConfig {
        GrantConfig {
            api_base: "http://localhost".into(),
            api_key: "flx_live_test".into(),
            agent_version: Some("test".into()),
            http_timeout: std::time::Duration::from_secs(1),
        }
    }

    fn fake_grant(features: &[&str], tier: &str) -> GrantTokenSet {
        // Build via a small helper that mirrors what the real client does
        // without contacting a server.
        let raw = serde_json::json!({
            "grant_urn": "urn:fluxx:grant:test",
            "grant_token": "flxg.v1.payload.sig",
            "hmac_key": "00".repeat(32),
            "expires_at": "2099-01-01T00:00:00Z",
            "ttl_seconds": 900,
            "tier": tier,
            "features": features,
            "quotas": {},
        });
        // Round-trip through the API envelope shape so we exercise the same
        // deserializer path as production.
        #[derive(serde::Deserialize)]
        struct Raw {
            grant_urn: String,
            grant_token: String,
            hmac_key: String,
            expires_at: String,
            ttl_seconds: u64,
            tier: String,
            features: Vec<String>,
            quotas: std::collections::HashMap<String, i64>,
        }
        let parsed: Raw = serde_json::from_value(raw).unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        GrantTokenSet {
            grant_urn: parsed.grant_urn,
            grant_token: parsed.grant_token,
            hmac_key: parsed.hmac_key,
            expires_at: parsed.expires_at,
            ttl_seconds: parsed.ttl_seconds,
            tier: parsed.tier,
            features: parsed.features,
            quotas: parsed.quotas,
            // Tests only need this to be non-zero/in the future; precise
            // expiry math is exercised in `license::grant` itself.
            expires_unix: now + parsed.ttl_seconds,
        }
    }

    #[tokio::test]
    async fn rejects_when_feature_not_granted() {
        let client = Client::new();
        let store = GrantStore::new();
        // Free tier — no features.
        store.replace(fake_grant(&[], "free"));
        let cfg = fake_cfg();
        let req = DiagnoseRequest {
            question: "x".into(),
            service: None,
            events: None,
        };
        let err = diagnose(&client, &cfg, &store, &req).await.unwrap_err();
        match err {
            DiagnoseError::FeatureNotGranted { feature, tier } => {
                assert_eq!(feature, "ai_diagnostics");
                assert_eq!(tier, "free");
            }
            other => panic!("expected FeatureNotGranted, got: {other:?}"),
        }
    }

    #[tokio::test]
    async fn accepts_wildcard_grant() {
        // Internal tier has "*" — should pass the local check and then fail
        // on the network call (since we point at an unroutable URL).
        let client = Client::new();
        let store = GrantStore::new();
        store.replace(fake_grant(&["*"], "internal"));
        let cfg = GrantConfig {
            api_base: "http://127.0.0.1:1".into(), // intentional connect-refused
            api_key: "flx_live_test".into(),
            agent_version: Some("t".into()),
            http_timeout: std::time::Duration::from_millis(200),
        };
        let req = DiagnoseRequest {
            question: "x".into(),
            service: None,
            events: None,
        };
        let err = diagnose(&client, &cfg, &store, &req).await.unwrap_err();
        // We expect Network (not FeatureNotGranted) because the wildcard
        // grant passes the local check.
        assert!(matches!(err, DiagnoseError::Network(_)), "got: {err:?}");
    }

    #[tokio::test]
    async fn signature_attached_to_outbound_request() {
        // Spin up a single-connection mock TCP server that captures the
        // request and replies with a stock envelope. Verify the agent
        // attached the expected headers.
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut buf = vec![0u8; 4096];
            let n = sock.read(&mut buf).await.unwrap();
            let req_text = String::from_utf8_lossy(&buf[..n]).to_string();
            let body = serde_json::json!({
                "transactionUrn": "urn:fluxx:txn:t",
                "status": "SUCCESS",
                "responseMessage": "OK",
                "responseKey": "SUCCESS",
                "errors": [],
                "timestamp": "2099-01-01T00:00:00Z",
                "metadata": {},
                "reference_urn": "urn:fluxx:ref:t",
                "data": {
                    "diagnosis": {
                        "summary": "ok",
                        "root_cause": "unknown",
                        "evidence": [],
                        "suggested_fix": "x",
                        "confidence": "low"
                    },
                    "usage": { "tokens_used": 1, "latency_ms": 1, "model": "stub" },
                    "tier": "pro"
                }
            }).to_string();
            let resp = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n{}",
                body.len(), body
            );
            sock.write_all(resp.as_bytes()).await.unwrap();
            sock.shutdown().await.unwrap();
            req_text
        });

        let client = Client::new();
        let store = GrantStore::new();
        store.replace(fake_grant(&["ai_diagnostics"], "pro"));
        let cfg = GrantConfig {
            api_base: format!("http://{addr}"),
            api_key: "flx_live_test".into(),
            agent_version: Some("t".into()),
            http_timeout: std::time::Duration::from_secs(2),
        };
        let req = DiagnoseRequest {
            question: "why is it slow?".into(),
            service: None,
            events: None,
        };
        let result = diagnose(&client, &cfg, &store, &req).await.unwrap();
        assert_eq!(result.diagnosis.summary, "ok");
        let req_text = server.await.unwrap();
        assert!(req_text.contains("POST /api/v1/ai/diagnose"));
        assert!(req_text.to_lowercase().contains("authorization: bearer flxg.v1."));
        assert!(req_text.to_lowercase().contains("x-fluxx-nonce:"));
        assert!(req_text.to_lowercase().contains("x-fluxx-timestamp:"));
        assert!(req_text.to_lowercase().contains("x-fluxx-signature:"));
    }
}
