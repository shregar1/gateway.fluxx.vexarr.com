//! Tier-1 integration wiring.
//!
//! Reads a small set of environment variables at startup, builds an
//! `IntegrationManager` from them, and runs a periodic collection loop
//! that converts each integration's `Metric` / `Event` into a fluxx
//! `Event` and inserts it into the agent's `EventStore` — same path
//! every other event takes, so the cloud forwarder picks them up
//! without any extra wiring.
//!
//! Env vars (all optional, comma-separated lists):
//!
//!   FLUXX_INTEGRATIONS_POSTGRES   one or more libpq URIs
//!     e.g. "postgres://fluxx@127.0.0.1:5432/fluxx,postgres://taskx@127.0.0.1:5433/taskx"
//!
//!   FLUXX_INTEGRATIONS_REDIS      one or more host:port pairs
//!     e.g. "127.0.0.1:6379,127.0.0.1:6380"
//!
//!   FLUXX_INTEGRATIONS_DOCKER     "true" to enable
//!
//!   FLUXX_INTEGRATIONS_HTTP_PROBES   one or more URLs to GET on a tick
//!     e.g. "http://127.0.0.1:8090/healthz,http://127.0.0.1:8004/healthz"
//!
//!   FLUXX_INTEGRATIONS_INTERVAL_SECS   override the 30s default
//!
//! All wiring is best-effort: if the env var is missing or a single
//! integration fails to construct, we log a warning and continue. We
//! never fail-fast on integration setup — the rest of the agent must
//! stay up.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use tokio::time::interval;
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::integrations::framework::{
    Event as IntegEvent, EventSeverity as IntegSeverity, Metric as IntegMetric, MetricValue,
};
use crate::integrations::IntegrationManager;
use crate::models::event::{Event, EventSeverity, Metadata, ProtocolType};
use crate::storage::event_store::EventStore;

const DEFAULT_INTERVAL_SECS: u64 = 30;

/// Spawn the Tier-1 integration loop. No-op if no env var is set.
pub fn spawn(event_store: Arc<dyn EventStore>) {
    // Build the manager up-front so a misconfigured env var produces
    // a single warning at boot instead of repeating every interval.
    let mut manager = IntegrationManager::new();
    let mut enabled: Vec<String> = Vec::new();

    if let Ok(uris) = std::env::var("FLUXX_INTEGRATIONS_POSTGRES") {
        for uri in split_csv(&uris) {
            manager.with_postgres(&uri);
            enabled.push(format!("postgres({})", redact_creds(&uri)));
        }
    }

    if let Ok(hosts) = std::env::var("FLUXX_INTEGRATIONS_REDIS") {
        for hp in split_csv(&hosts) {
            match parse_host_port(&hp) {
                Some((h, p)) => {
                    manager.with_redis(&h, p);
                    enabled.push(format!("redis({}:{})", h, p));
                }
                None => warn!(value = %hp, "FLUXX_INTEGRATIONS_REDIS: skip malformed host:port"),
            }
        }
    }

    if matches!(
        std::env::var("FLUXX_INTEGRATIONS_DOCKER")
            .unwrap_or_default()
            .to_lowercase()
            .as_str(),
        "true" | "1" | "yes"
    ) {
        manager.with_docker();
        enabled.push("docker".into());
    }

    let probe_urls: Vec<String> = std::env::var("FLUXX_INTEGRATIONS_HTTP_PROBES")
        .map(|v| split_csv(&v))
        .unwrap_or_default();

    if enabled.is_empty() && probe_urls.is_empty() {
        debug!("integration_spawner: no integrations configured, skipping");
        return;
    }

    let interval_secs = std::env::var("FLUXX_INTEGRATIONS_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse::<u64>().ok())
        .unwrap_or(DEFAULT_INTERVAL_SECS);

    info!(
        integrations = ?enabled,
        probes = probe_urls.len(),
        interval_secs,
        "integration_spawner: starting Tier-1 integration loop"
    );

    let manager = Arc::new(manager);

    // Main collection loop: poll integrations, write events.
    {
        let store = event_store.clone();
        let manager = manager.clone();
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(interval_secs));
            // Skip the immediate tick — let the rest of the agent settle first.
            ticker.tick().await;
            loop {
                ticker.tick().await;
                run_one_pass(&manager, &store).await;
            }
        });
    }

    // HTTP-probe loop: separate from IntegrationManager (it doesn't
    // model "GET this URL and time it" as an Integration, and we
    // don't want to add a one-off integration type for it).
    if !probe_urls.is_empty() {
        let store = event_store.clone();
        let probes = probe_urls;
        tokio::spawn(async move {
            let mut ticker = interval(Duration::from_secs(interval_secs));
            ticker.tick().await;
            let client = reqwest::Client::builder()
                .timeout(Duration::from_secs(5))
                .build()
                .unwrap_or_else(|_| reqwest::Client::new());
            loop {
                ticker.tick().await;
                for url in &probes {
                    let ev = probe_one(&client, url).await;
                    if let Err(e) = store.insert(ev).await {
                        warn!(error = %e, url = %url, "http_probe: insert failed");
                    }
                }
            }
        });
    }
}

/// One collection pass: pull metrics + events from every integration,
/// convert each into a fluxx Event, write to the store.
async fn run_one_pass(manager: &Arc<IntegrationManager>, store: &Arc<dyn EventStore>) {
    let metrics = manager.collect_metrics().await;
    let events = manager.collect_events().await;

    debug!(
        metrics = metrics.len(),
        events = events.len(),
        "integration_spawner: collection pass"
    );

    for m in metrics {
        let ev = metric_to_event(m);
        if let Err(e) = store.insert(ev).await {
            warn!(error = %e, "integration_spawner: metric insert failed");
        }
    }
    for e in events {
        let ev = integ_event_to_event(e);
        if let Err(e) = store.insert(ev).await {
            warn!(error = %e, "integration_spawner: event insert failed");
        }
    }
}

/// Convert an integration `Metric` into the agent's `Event` shape.
///
/// We tag protocol=System (it's a derived/synthetic metric, not OTLP)
/// and stash the metric name + tags in `metadata.extra` so the cloud's
/// /service-map and /events endpoints can read them. The metric value
/// goes into `payload` as a short "name=value" string for free-text
/// search ("redis evictions=1234").
fn metric_to_event(m: IntegMetric) -> Event {
    let value_str = match &m.value {
        MetricValue::Integer(v) => v.to_string(),
        MetricValue::Float(v) => format!("{:.6}", v),
        MetricValue::String(s) => s.clone(),
        MetricValue::Boolean(b) => b.to_string(),
        MetricValue::Histogram(_) => "<histogram>".to_string(),
        // The MetricValue enum has variants we don't care to render
        // verbatim; the catch-all keeps us forward-compatible.
        _ => "<value>".to_string(),
    };

    let mut extra: HashMap<String, String> = m.tags.into_iter().collect();
    extra.insert("metric_name".into(), m.name.clone());
    extra.insert("metric_value".into(), value_str.clone());
    extra.insert("metric_source".into(), m.source.clone());
    if let Some(unit) = &m.unit {
        extra.insert("metric_unit".into(), unit.clone());
    }
    extra.insert("metric_type".into(), format!("{:?}", m.metric_type));

    Event {
        id: Uuid::new_v4().to_string(),
        session_id: None,
        project_id: None,
        environment_id: None,
        // Each integration name (postgres, redis, docker) becomes
        // its own service node on the dashboard's /service-map.
        service_id: Some(m.source.clone()),
        timestamp_us: timestamp_us(m.timestamp),
        protocol: ProtocolType::System,
        latency_us: 0,
        severity: EventSeverity::Info,
        payload: Some(format!("{}={}", m.name, value_str)),
        metadata: Metadata { extra, ..Default::default() },
    }
}

fn integ_event_to_event(e: IntegEvent) -> Event {
    let severity = match e.severity {
        IntegSeverity::Info => EventSeverity::Info,
        IntegSeverity::Warning => EventSeverity::Medium,
        IntegSeverity::Error => EventSeverity::High,
        IntegSeverity::Critical => EventSeverity::Critical,
    };
    let mut extra: HashMap<String, String> = e.metadata;
    extra.insert("event_type".into(), e.event_type);
    extra.insert("title".into(), e.title.clone());

    Event {
        id: Uuid::new_v4().to_string(),
        session_id: None,
        project_id: None,
        environment_id: None,
        service_id: Some(e.source),
        timestamp_us: timestamp_us(e.timestamp),
        protocol: ProtocolType::System,
        latency_us: 0,
        severity,
        payload: Some(format!("{}: {}", e.title, e.description)),
        metadata: Metadata { extra, ..Default::default() },
    }
}

/// HTTP probe: GET a URL, time the round-trip, surface as an event.
async fn probe_one(client: &reqwest::Client, url: &str) -> Event {
    let start = std::time::Instant::now();
    let result = client.get(url).send().await;
    let elapsed = start.elapsed();
    let now_us = timestamp_us(SystemTime::now());

    let (severity, status, ok_str) = match &result {
        Ok(r) if r.status().is_success() => (EventSeverity::Info, r.status().as_u16(), "ok"),
        Ok(r) => (EventSeverity::Medium, r.status().as_u16(), "non_2xx"),
        Err(_) => (EventSeverity::High, 0, "error"),
    };

    let mut extra = HashMap::new();
    extra.insert("probe_url".into(), url.to_string());
    extra.insert("probe_status".into(), status.to_string());
    extra.insert("probe_result".into(), ok_str.to_string());
    extra.insert(
        "duration_us".into(),
        (elapsed.as_micros() as u64).to_string(),
    );

    let host = url
        .split("://")
        .nth(1)
        .and_then(|rest| rest.split('/').next())
        .unwrap_or(url);

    Event {
        id: Uuid::new_v4().to_string(),
        session_id: None,
        project_id: None,
        environment_id: None,
        service_id: Some(format!("probe:{}", host)),
        timestamp_us: now_us,
        protocol: ProtocolType::Http,
        latency_us: elapsed.as_micros() as u64,
        severity,
        payload: Some(format!("GET {} → {} ({}us)", url, status, elapsed.as_micros())),
        metadata: Metadata { extra, ..Default::default() },
    }
}

// ─── Helpers ────────────────────────────────────────────────────────────────

fn split_csv(s: &str) -> Vec<String> {
    s.split(',')
        .map(|x| x.trim())
        .filter(|x| !x.is_empty())
        .map(|x| x.to_string())
        .collect()
}

fn parse_host_port(s: &str) -> Option<(String, u16)> {
    let (h, p) = s.rsplit_once(':')?;
    Some((h.to_string(), p.parse().ok()?))
}

/// Best-effort credential redaction for the boot log line.
fn redact_creds(uri: &str) -> String {
    if let Some(at) = uri.rfind('@') {
        if let Some(scheme_end) = uri.find("://") {
            return format!("{}://***@{}", &uri[..scheme_end], &uri[at + 1..]);
        }
    }
    uri.to_string()
}

fn timestamp_us(t: SystemTime) -> u64 {
    t.duration_since(SystemTime::UNIX_EPOCH)
        .map(|d| d.as_micros() as u64)
        .unwrap_or(0)
}

// ─── Tests ──────────────────────────────────────────────────────────────────
//
// Pure-function tests for the env-parsing + redaction helpers. The
// async spawn paths (event_store inserts, HTTP probes) aren't covered
// here — they're exercised by the integration tests under tests/.

#[cfg(test)]
mod tests {
    use super::*;

    // ── split_csv ────────────────────────────────────────────────────────
    #[test]
    fn split_csv_handles_empty_string() {
        assert_eq!(split_csv(""), Vec::<String>::new());
    }

    #[test]
    fn split_csv_single_item_no_comma() {
        assert_eq!(split_csv("foo"), vec!["foo"]);
    }

    #[test]
    fn split_csv_trims_whitespace_around_items() {
        // Production env vars often arrive with stray spaces around commas;
        // a missed trim would produce a redis host of "  127.0.0.1:6379".
        assert_eq!(
            split_csv(" a, b ,c "),
            vec!["a", "b", "c"]
        );
    }

    #[test]
    fn split_csv_drops_empty_segments() {
        // Avoids the silent gotcha of `FLUXX_INTEGRATIONS_REDIS=,` turning
        // into a list with an empty host that then fails parse_host_port.
        assert_eq!(split_csv(",a,,b,"), vec!["a", "b"]);
    }

    // ── parse_host_port ──────────────────────────────────────────────────
    #[test]
    fn parse_host_port_simple_ipv4() {
        assert_eq!(
            parse_host_port("127.0.0.1:6379"),
            Some(("127.0.0.1".into(), 6379))
        );
    }

    #[test]
    fn parse_host_port_hostname() {
        assert_eq!(
            parse_host_port("redis.internal:6380"),
            Some(("redis.internal".into(), 6380))
        );
    }

    #[test]
    fn parse_host_port_rejects_missing_port() {
        assert_eq!(parse_host_port("127.0.0.1"), None);
    }

    #[test]
    fn parse_host_port_rejects_non_numeric_port() {
        assert_eq!(parse_host_port("redis:six-three-eight-zero"), None);
    }

    #[test]
    fn parse_host_port_rejects_port_above_u16() {
        // Anything above 65535 is not a valid TCP port and `p.parse::<u16>()`
        // correctly rejects it. Pin so a future refactor that switches to u32
        // doesn't silently accept garbage.
        assert_eq!(parse_host_port("127.0.0.1:99999"), None);
    }

    #[test]
    fn parse_host_port_uses_rsplit_for_ipv6_safety() {
        // IPv6 addresses contain colons too. rsplit_once on the LAST colon
        // means "[::1]:6379" parses to host="[::1]", port=6379 — the right
        // thing. Even if we don't enable IPv6 today, the behaviour shouldn't
        // be that "::1:6379" interprets the wrong colon as the port boundary.
        assert_eq!(
            parse_host_port("[::1]:6379"),
            Some(("[::1]".into(), 6379))
        );
    }

    // ── redact_creds ─────────────────────────────────────────────────────
    #[test]
    fn redact_creds_strips_userinfo_from_postgres_uri() {
        // Most common case: "postgres://user:pass@host/db" must NOT leak
        // creds to logs.
        assert_eq!(
            redact_creds("postgres://fluxx:hunter2@127.0.0.1:5432/db"),
            "postgres://***@127.0.0.1:5432/db"
        );
    }

    #[test]
    fn redact_creds_passes_through_uri_with_no_userinfo() {
        // No '@' means nothing to redact. The function must not corrupt
        // the URI in that case (early-returning unchanged).
        assert_eq!(
            redact_creds("postgres://127.0.0.1:5432/db"),
            "postgres://127.0.0.1:5432/db"
        );
    }

    #[test]
    fn redact_creds_passes_through_non_uri_string() {
        // Inputs without "://" should be returned verbatim — the function
        // is best-effort, not a URI validator.
        assert_eq!(redact_creds("not a uri"), "not a uri");
    }

    #[test]
    fn redact_creds_uses_last_at_so_passwords_with_at_dont_leak() {
        // Passwords legitimately contain '@'. rfind('@') keeps the right
        // split (the userinfo terminator '@' is always the LAST '@' before
        // the host). A naive find('@') would leak everything after the
        // first '@'.
        assert_eq!(
            redact_creds("postgres://user:p@ss@host/db"),
            "postgres://***@host/db"
        );
    }

    // ── timestamp_us ─────────────────────────────────────────────────────
    #[test]
    fn timestamp_us_for_unix_epoch_is_zero() {
        assert_eq!(timestamp_us(SystemTime::UNIX_EPOCH), 0);
    }

    #[test]
    fn timestamp_us_grows_monotonically() {
        // Sanity: two SystemTime::now() reads, the second is >= the first
        // by the elapsed micros. Avoids the regression where the function
        // started returning constant 0 due to an Err in duration_since
        // (which IS possible on systems with clock skew that pushes "now"
        // before the epoch — we treat that as 0 deliberately).
        let a = timestamp_us(SystemTime::now());
        std::thread::sleep(std::time::Duration::from_micros(50));
        let b = timestamp_us(SystemTime::now());
        assert!(
            b > a,
            "expected b ({}) > a ({}), spread = {}us",
            b,
            a,
            b as i128 - a as i128
        );
    }

    // ── hex_prefix is in http_server.rs; not duplicated here. ────────────
}
