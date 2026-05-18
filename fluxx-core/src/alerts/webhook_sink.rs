//! Outbound webhook for issue events.
//!
//! Configured by `FLUXX_WEBHOOK_URL`; optional `FLUXX_WEBHOOK_SECRET`
//! enables HMAC-SHA256 signing in `X-Fluxx-Signature`. Receivers
//! (Slack incoming-webhook URL, generic JSON endpoints) get a payload
//! of the form:
//!
//! ```json
//! {
//!   "event": "new_issue" | "regression" | "spike",
//!   "fingerprint": "abcd1234abcd1234",
//!   "error_type": "TypeError",
//!   "message": "x is undefined",
//!   "service": "frontend",
//!   "release": "1.4.2",
//!   "count_last_5m": 87,
//!   "url": "https://app/some/page",
//!   "fired_at": "2026-05-17T03:14:15.926Z"
//! }
//! ```
//!
//! Failures are logged and counted, never retried — webhook sinks are
//! a notification convenience, not a reliable delivery channel. For
//! durable routing use the upstream ingest path + a real workflow tool.

use parking_lot::Mutex;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use tracing::{debug, warn};

#[derive(Debug, Clone, Serialize)]
pub struct WebhookPayload {
    /// "new_issue" | "regression" | "spike"
    pub event: String,
    pub fingerprint: String,
    pub error_type: String,
    pub message: String,
    pub service: String,
    pub release: String,
    pub count_last_5m: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub url: Option<String>,
    pub fired_at: String,
}

#[derive(Debug)]
pub struct WebhookSink {
    url: String,
    secret: Option<String>,
    http: reqwest::Client,
    sent: AtomicU64,
    dropped: AtomicU64,
    errors: AtomicU64,
    /// Fingerprints we've already fired `new_issue` for. Prevents
    /// duplicate notifications when the same fingerprint keeps
    /// appearing — only the *first* observation per restart fires.
    seen: Mutex<HashSet<String>>,
}

impl WebhookSink {
    /// Open from env. Returns `None` if no URL is configured.
    pub fn from_env() -> Option<Arc<Self>> {
        let url = std::env::var("FLUXX_WEBHOOK_URL").ok()?;
        if url.is_empty() {
            return None;
        }
        let secret = std::env::var("FLUXX_WEBHOOK_SECRET").ok().filter(|s| !s.is_empty());
        let http = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(5))
            .build()
            .ok()?;
        Some(Arc::new(Self {
            url,
            secret,
            http,
            sent: AtomicU64::new(0),
            dropped: AtomicU64::new(0),
            errors: AtomicU64::new(0),
            seen: Mutex::new(HashSet::new()),
        }))
    }

    /// Fire a webhook if this fingerprint hasn't been seen this run.
    /// Spawned as a fire-and-forget task so the ingest path doesn't
    /// block on the network call.
    pub fn maybe_fire_new_issue(self: &Arc<Self>, payload: WebhookPayload) {
        // Guard with the dedupe set first so we don't spin up a task
        // we'll immediately drop.
        {
            let mut seen = self.seen.lock();
            if seen.contains(&payload.fingerprint) {
                self.dropped.fetch_add(1, Ordering::Relaxed);
                return;
            }
            seen.insert(payload.fingerprint.clone());
        }
        let me = self.clone();
        tokio::spawn(async move {
            me.fire(payload).await;
        });
    }

    /// Fire unconditionally — for regressions / spikes the upstream
    /// caller has already decided this is interesting.
    pub fn fire_event(self: &Arc<Self>, payload: WebhookPayload) {
        let me = self.clone();
        tokio::spawn(async move {
            me.fire(payload).await;
        });
    }

    async fn fire(&self, payload: WebhookPayload) {
        let body = match serde_json::to_vec(&payload) {
            Ok(b) => b,
            Err(e) => {
                warn!(error = %e, "webhook: payload serialize failed");
                self.errors.fetch_add(1, Ordering::Relaxed);
                return;
            }
        };

        let mut req = self
            .http
            .post(&self.url)
            .header("Content-Type", "application/json");

        if let Some(secret) = &self.secret {
            let mut h = Sha256::new();
            h.update(secret.as_bytes());
            h.update(&body);
            let sig: String = h
                .finalize()
                .iter()
                .map(|b| format!("{:02x}", b))
                .collect();
            req = req.header("X-Fluxx-Signature", format!("sha256={}", sig));
        }

        match req.body(body).send().await {
            Ok(r) if r.status().is_success() => {
                self.sent.fetch_add(1, Ordering::Relaxed);
                debug!(url = %self.url, "webhook: sent");
            }
            Ok(r) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                warn!(status = %r.status(), "webhook: non-2xx");
            }
            Err(e) => {
                self.errors.fetch_add(1, Ordering::Relaxed);
                warn!(error = %e, "webhook: request failed");
            }
        }
    }

    pub fn stats(&self) -> WebhookStats {
        WebhookStats {
            sent: self.sent.load(Ordering::Relaxed),
            dropped: self.dropped.load(Ordering::Relaxed),
            errors: self.errors.load(Ordering::Relaxed),
            tracked_fingerprints: self.seen.lock().len() as u64,
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct WebhookStats {
    pub sent: u64,
    pub dropped: u64,
    pub errors: u64,
    pub tracked_fingerprints: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample(fp: &str) -> WebhookPayload {
        WebhookPayload {
            event: "new_issue".into(),
            fingerprint: fp.into(),
            error_type: "TypeError".into(),
            message: "boom".into(),
            service: "svc".into(),
            release: "1.0.0".into(),
            count_last_5m: 1,
            url: None,
            fired_at: "2026-05-17T03:14:15Z".into(),
        }
    }

    #[test]
    fn from_env_returns_none_without_url() {
        std::env::remove_var("FLUXX_WEBHOOK_URL");
        assert!(WebhookSink::from_env().is_none());
    }

    #[test]
    fn from_env_returns_some_with_url() {
        std::env::set_var("FLUXX_WEBHOOK_URL", "http://example.com/hook");
        let sink = WebhookSink::from_env().unwrap();
        assert_eq!(sink.url, "http://example.com/hook");
        std::env::remove_var("FLUXX_WEBHOOK_URL");
    }

    #[tokio::test]
    async fn dedupe_skips_repeated_fingerprints() {
        std::env::set_var("FLUXX_WEBHOOK_URL", "http://127.0.0.1:1/never");
        let sink = WebhookSink::from_env().unwrap();
        sink.maybe_fire_new_issue(sample("abc"));
        sink.maybe_fire_new_issue(sample("abc"));
        sink.maybe_fire_new_issue(sample("def"));
        // Allow the spawned tasks to register before checking stats.
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let s = sink.stats();
        assert_eq!(s.tracked_fingerprints, 2);
        assert_eq!(s.dropped, 1, "second `abc` should be dropped");
        std::env::remove_var("FLUXX_WEBHOOK_URL");
    }
}
