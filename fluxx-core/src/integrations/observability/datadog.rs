//! Datadog Integration
//!
//! Monitor Datadog monitors, metrics, and events.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Datadog integration
pub struct DatadogIntegration {
    config: IntegrationConfig,
    api_key: String,
    app_key: String,
    site: String,
}

impl DatadogIntegration {
    /// Create new Datadog integration
    pub fn new(api_key: &str, app_key: &str) -> Result<Self, IntegrationError> {
        Self::with_site(api_key, app_key, "datadoghq.com")
    }

    /// Create with custom site (e.g., datadoghq.eu)
    pub fn with_site(api_key: &str, app_key: &str, site: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("observability_type".to_string(), "datadog".to_string());

        let config = IntegrationConfig {
            name: "datadog".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            api_key: api_key.to_string(),
            app_key: app_key.to_string(),
            site: site.to_string(),
        })
    }

    /// Check if Datadog API is accessible
    async fn check_datadog_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("https://api.{}/api/v1/validate", self.site);

        match client
            .get(&url)
            .header("DD-API-KEY", &self.api_key)
            .header("DD-APPLICATION-KEY", &self.app_key)
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            Ok(response) => response.status().is_success(),
            Err(_) => false,
        }
    }

    /// Collect monitor metrics
    async fn collect_monitor_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("https://api.{}/api/v1/monitor", self.site);

        let response = client
            .get(&url)
            .header("DD-API-KEY", &self.api_key)
            .header("DD-APPLICATION-KEY", &self.app_key)
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !response.status().is_success() {
            return Ok(metrics);
        }

        let monitors: Vec<serde_json::Value> = response
            .json()
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        let mut ok_count = 0u64;
        let mut alert_count = 0u64;
        let mut warn_count = 0u64;
        let mut no_data_count = 0u64;

        for monitor in &monitors {
            if let Some(overall_state) = monitor["overall_state"].as_str() {
                match overall_state {
                    "OK" => ok_count += 1,
                    "Alert" => alert_count += 1,
                    "Warn" => warn_count += 1,
                    "No Data" => no_data_count += 1,
                    _ => {}
                }
            }
        }

        metrics.push(Metric::gauge(
            "datadog.monitors.total",
            monitors.len() as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "datadog.monitors.ok",
            ok_count as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "datadog.monitors.alert",
            alert_count as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "datadog.monitors.warn",
            warn_count as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "datadog.monitors.no_data",
            no_data_count as f64,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect all Datadog metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_datadog_access().await {
            all_metrics.push(Metric::gauge(
                "datadog.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "datadog.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_monitor_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect monitor metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Datadog events
    async fn collect_datadog_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_datadog_access().await {
            events.push(
                Event::new(
                    "Datadog API Unavailable",
                    "Cannot connect to Datadog API".to_string(),
                    EventSeverity::Critical,
                    self.name(),
                    "api_unavailable",
                ),
            );
            return Ok(events);
        }

        // Check for alerting monitors
        let client = reqwest::Client::new();
        let url = format!("https://api.{}/api/v1/monitor", self.site);

        if let Ok(response) = client
            .get(&url)
            .header("DD-API-KEY", &self.api_key)
            .header("DD-APPLICATION-KEY", &self.app_key)
            .query(&[("group_states", "alert,warn")])
            .timeout(std::time::Duration::from_secs(30))
            .send()
            .await
        {
            if response.status().is_success() {
                if let Ok(monitors) = response.json::<Vec<serde_json::Value>>().await {
                    for monitor in monitors {
                        if let (Some(name), Some(state)) = (
                            monitor["name"].as_str(),
                            monitor["overall_state"].as_str(),
                        ) {
                            if state == "Alert" {
                                events.push(
                                    Event::new(
                                        "Datadog Monitor Alert",
                                        format!("Monitor '{}' is alerting", name),
                                        EventSeverity::Critical,
                                        self.name(),
                                        "monitor_alert",
                                    )
                                    .with_metadata("monitor_name", name)
                                    .with_metadata("state", state),
                                );
                            } else if state == "Warn" {
                                events.push(
                                    Event::new(
                                        "Datadog Monitor Warning",
                                        format!("Monitor '{}' has warning", name),
                                        EventSeverity::Warning,
                                        self.name(),
                                        "monitor_warn",
                                    )
                                    .with_metadata("monitor_name", name)
                                    .with_metadata("state", state),
                                );
                            }
                        }
                    }
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for DatadogIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_datadog_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_datadog_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "Datadog API keys invalid".to_string()
            ))
        }
    }

    fn config(&self) -> &IntegrationConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_datadog_integration_creation() {
        let datadog = DatadogIntegration::new("api_key", "app_key");
        assert!(datadog.is_ok());
    }
}
