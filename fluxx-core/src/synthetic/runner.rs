//! Synthetic Check Runner
//!
//! Executes synthetic checks and reports results as metrics.

use crate::synthetic::checks::*;
use crate::synthetic::ssl;
use crate::models::metric::{Metric, MetricType};
use crate::storage::metric_store::MetricStore;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::broadcast;
use tracing::{info, warn, error};

/// Runs synthetic checks on their configured intervals.
pub struct SyntheticRunner {
    checks: Vec<SyntheticCheck>,
    metric_store: Arc<dyn MetricStore>,
    metric_bus: broadcast::Sender<Arc<Metric>>,
    http_client: reqwest::Client,
}

impl SyntheticRunner {
    pub fn new(
        checks: Vec<SyntheticCheck>,
        metric_store: Arc<dyn MetricStore>,
        metric_bus: broadcast::Sender<Arc<Metric>>,
    ) -> Self {
        Self {
            checks,
            metric_store,
            metric_bus,
            http_client: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .danger_accept_invalid_certs(false)
                .build()
                .unwrap_or_default(),
        }
    }

    /// Start all check loops.
    pub async fn run(&self) {
        if self.checks.is_empty() {
            info!("SyntheticRunner: No checks configured");
            return;
        }
        info!("SyntheticRunner: Starting {} checks", self.checks.len());

        let mut handles = Vec::new();
        for check in &self.checks {
            if !check.enabled {
                continue;
            }
            let check = check.clone();
            let client = self.http_client.clone();
            let store = self.metric_store.clone();
            let bus = self.metric_bus.clone();

            handles.push(tokio::spawn(async move {
                let mut interval = tokio::time::interval(Duration::from_secs(check.interval_secs));
                loop {
                    interval.tick().await;
                    let result = execute_check(&check, &client).await;
                    report_result(&result, &store, &bus).await;
                }
            }));
        }

        // Wait for all (they run forever)
        for h in handles {
            let _ = h.await;
        }
    }
}

/// Execute a single check.
async fn execute_check(check: &SyntheticCheck, client: &reqwest::Client) -> CheckResult {
    let timeout = Duration::from_secs(check.timeout_secs);

    match &check.check_type {
        CheckType::Http { url, method, expected_status, headers, body } => {
            execute_http_check(check, client, url, method, *expected_status, headers, body.as_deref(), timeout).await
        }
        CheckType::ApiTest { steps } => {
            execute_api_test(check, client, steps, timeout).await
        }
        CheckType::Ssl { hostname, port, warn_days, critical_days } => {
            execute_ssl_check(check, hostname, *port, *warn_days, *critical_days).await
        }
    }
}

async fn execute_http_check(
    check: &SyntheticCheck,
    client: &reqwest::Client,
    url: &str,
    method: &str,
    expected_status: u16,
    headers: &HashMap<String, String>,
    body: Option<&str>,
    timeout: Duration,
) -> CheckResult {
    let start = Instant::now();

    let mut req = match method.to_uppercase().as_str() {
        "GET" => client.get(url),
        "POST" => client.post(url),
        "PUT" => client.put(url),
        "DELETE" => client.delete(url),
        "HEAD" => client.head(url),
        _ => client.get(url),
    };

    for (k, v) in headers {
        req = req.header(k.as_str(), v.as_str());
    }
    if let Some(b) = body {
        req = req.body(b.to_string());
    }
    req = req.timeout(timeout);

    match req.send().await {
        Ok(resp) => {
            let status = resp.status().as_u16();
            let latency = start.elapsed();
            if status == expected_status {
                CheckResult::success(check, status, latency)
            } else {
                let mut result = CheckResult::failure(
                    check,
                    format!("Expected status {}, got {}", expected_status, status),
                    latency,
                );
                result.status_code = Some(status);
                result
            }
        }
        Err(e) => CheckResult::failure(check, format!("Request failed: {}", e), start.elapsed()),
    }
}

async fn execute_api_test(
    check: &SyntheticCheck,
    client: &reqwest::Client,
    steps: &[ApiTestStep],
    timeout: Duration,
) -> CheckResult {
    let start = Instant::now();

    for (i, step) in steps.iter().enumerate() {
        let mut req = match step.method.to_uppercase().as_str() {
            "GET" => client.get(&step.url),
            "POST" => client.post(&step.url),
            "PUT" => client.put(&step.url),
            "DELETE" => client.delete(&step.url),
            _ => client.get(&step.url),
        };

        for (k, v) in &step.headers {
            req = req.header(k.as_str(), v.as_str());
        }
        if let Some(b) = &step.body {
            req = req.body(b.clone());
        }
        req = req.timeout(timeout);

        match req.send().await {
            Ok(resp) => {
                let status = resp.status().as_u16();
                if status != step.expected_status {
                    return CheckResult::failure(
                        check,
                        format!("Step {} '{}': expected status {}, got {}", i + 1, step.name, step.expected_status, status),
                        start.elapsed(),
                    );
                }
            }
            Err(e) => {
                return CheckResult::failure(
                    check,
                    format!("Step {} '{}': {}", i + 1, step.name, e),
                    start.elapsed(),
                );
            }
        }
    }

    CheckResult::success(check, 200, start.elapsed())
}

async fn execute_ssl_check(
    check: &SyntheticCheck,
    hostname: &str,
    port: u16,
    warn_days: u32,
    critical_days: u32,
) -> CheckResult {
    let start = Instant::now();

    match ssl::check_certificate(hostname, port).await {
        Ok(days_remaining) => {
            let mut result = CheckResult::success(check, 200, start.elapsed());
            result.details.insert("days_remaining".to_string(), days_remaining.to_string());
            result.details.insert("hostname".to_string(), hostname.to_string());

            if days_remaining < critical_days as i64 {
                result.success = false;
                result.error = Some(format!(
                    "SSL cert expires in {} days (critical threshold: {})",
                    days_remaining, critical_days
                ));
            } else if days_remaining < warn_days as i64 {
                result.error = Some(format!(
                    "SSL cert expires in {} days (warning threshold: {})",
                    days_remaining, warn_days
                ));
            }
            result
        }
        Err(e) => CheckResult::failure(check, format!("SSL check failed: {}", e), start.elapsed()),
    }
}

/// Report a check result as metrics.
async fn report_result(
    result: &CheckResult,
    store: &Arc<dyn MetricStore>,
    bus: &broadcast::Sender<Arc<Metric>>,
) {
    let tags = HashMap::from([
        ("check_id".to_string(), result.check_id.clone()),
        ("check_name".to_string(), result.check_name.clone()),
    ]);

    // Latency metric
    let latency = Metric::new(
        format!("synthetic.{}.latency_ms", result.check_id),
        result.latency_ms,
        MetricType::Gauge,
    ).with_tags(tags.clone());

    // Success metric (1.0 or 0.0)
    let success = Metric::new(
        format!("synthetic.{}.success", result.check_id),
        if result.success { 1.0 } else { 0.0 },
        MetricType::Gauge,
    ).with_tags(tags);

    for m in [latency, success] {
        let m_arc = Arc::new(m.clone());
        let _ = bus.send(m_arc);
        if let Err(e) = store.insert(m).await {
            error!("SyntheticRunner: Failed to store metric: {}", e);
        }
    }

    if !result.success {
        warn!(
            "SyntheticRunner: Check '{}' FAILED: {}",
            result.check_name,
            result.error.as_deref().unwrap_or("unknown")
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_result_success() {
        let check = SyntheticCheck {
            id: "test".to_string(),
            name: "Test Check".to_string(),
            check_type: CheckType::Http {
                url: "http://example.com".to_string(),
                method: "GET".to_string(),
                expected_status: 200,
                headers: HashMap::new(),
                body: None,
            },
            interval_secs: 60,
            timeout_secs: 10,
            enabled: true,
            tags: HashMap::new(),
        };

        let result = CheckResult::success(&check, 200, Duration::from_millis(150));
        assert!(result.success);
        assert_eq!(result.status_code, Some(200));
        assert!(result.latency_ms > 100.0);
    }

    #[test]
    fn test_check_result_failure() {
        let check = SyntheticCheck {
            id: "test".to_string(),
            name: "Test Check".to_string(),
            check_type: CheckType::Http {
                url: "http://example.com".to_string(),
                method: "GET".to_string(),
                expected_status: 200,
                headers: HashMap::new(),
                body: None,
            },
            interval_secs: 60,
            timeout_secs: 10,
            enabled: true,
            tags: HashMap::new(),
        };

        let result = CheckResult::failure(&check, "timeout".to_string(), Duration::from_secs(10));
        assert!(!result.success);
        assert_eq!(result.error, Some("timeout".to_string()));
    }
}
