//! HashiCorp Vault Integration
//!
//! Monitor Vault health, seals, and audit logs.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Vault integration
pub struct VaultIntegration {
    config: IntegrationConfig,
    address: String,
    token: Option<String>,
}

impl VaultIntegration {
    /// Create new Vault integration
    pub fn new(address: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("security_type".to_string(), "vault".to_string());

        let config = IntegrationConfig {
            name: "vault".to_string(),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            address: address.trim_end_matches('/').to_string(),
            token: None,
        })
    }

    /// Create with authentication token
    pub fn with_token(mut self, token: &str) -> Self {
        self.token = Some(token.to_string());
        self
    }

    /// Check if Vault is accessible
    async fn check_vault_access(&self) -> bool {
        let client = reqwest::Client::new();
        let url = format!("{}/v1/sys/health", self.address);

        let mut request = client.get(&url);
        if let Some(ref token) = self.token {
            request = request.header("X-Vault-Token", token);
        }

        match request
            .timeout(std::time::Duration::from_secs(5))
            .send()
            .await
        {
            Ok(response) => {
                // Vault returns specific status codes:
                // 200 - initialized, unsealed, active
                // 429 - unsealed, standby
                // 472 - disaster recovery mode
                // 473 - performance standby
                // 501 - not initialized
                // 503 - sealed
                let status = response.status();
                status.is_success() || status.as_u16() == 429 || status.as_u16() == 472 || status.as_u16() == 473
            }
            Err(_) => false,
        }
    }

    /// Collect health metrics
    async fn collect_health_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/v1/sys/health", self.address);

        let mut request = client.get(&url);
        if let Some(ref token) = self.token {
            request = request.header("X-Vault-Token", token);
        }

        let response = request
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        let status = response.status().as_u16();

        // Health status
        let health_value = match status {
            200 => 1.0,  // Healthy
            429 => 0.5,  // Standby
            472 => 0.5,  // DR mode
            473 => 0.5,  // Performance standby
            501 => 0.0,  // Not initialized
            503 => 0.0,  // Sealed
            _ => 0.0,
        };

        metrics.push(Metric::gauge(
            "vault.health.status",
            health_value,
            self.name(),
        ));

        // Parse response body if successful
        if let Ok(data) = response.json::<serde_json::Value>().await {
            if let Some(sealed) = data["sealed"].as_bool() {
                metrics.push(Metric::gauge(
                    "vault.sealed",
                    if sealed { 1.0 } else { 0.0 },
                    self.name(),
                ));
            }
            if let Some(initialized) = data["initialized"].as_bool() {
                metrics.push(Metric::gauge(
                    "vault.initialized",
                    if initialized { 1.0 } else { 0.0 },
                    self.name(),
                ));
            }
            if let Some(standby) = data["standby"].as_bool() {
                metrics.push(Metric::gauge(
                    "vault.standby",
                    if standby { 1.0 } else { 0.0 },
                    self.name(),
                ));
            }
            if let Some(version) = data["version"].as_str() {
                metrics.push(Metric::gauge(
                    "vault.version",
                    1.0,
                    self.name(),
                ).with_tag("version", version));
            }
        }

        Ok(metrics)
    }

    /// Collect all Vault metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        if !self.check_vault_access().await {
            all_metrics.push(Metric::gauge(
                "vault.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(all_metrics);
        }

        all_metrics.push(Metric::gauge(
            "vault.connection.status",
            1.0,
            self.name(),
        ));

        match self.collect_health_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect health metrics: {}", e),
        }

        Ok(all_metrics)
    }

    /// Collect Vault events
    async fn collect_vault_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        let client = reqwest::Client::new();
        let url = format!("{}/v1/sys/health", self.address);

        let mut request = client.get(&url);
        if let Some(ref token) = self.token {
            request = request.header("X-Vault-Token", token);
        }

        if let Ok(response) = request
            .timeout(std::time::Duration::from_secs(10))
            .send()
            .await
        {
            let status = response.status().as_u16();

            match status {
                503 => {
                    events.push(
                        Event::new(
                            "Vault Sealed",
                            "Vault is currently sealed - cannot serve requests",
                            EventSeverity::Critical,
                            self.name(),
                            "vault_sealed",
                        ),
                    );
                }
                501 => {
                    events.push(
                        Event::new(
                            "Vault Not Initialized",
                            "Vault has not been initialized",
                            EventSeverity::Critical,
                            self.name(),
                            "vault_not_initialized",
                        ),
                    );
                }
                200 => {} // Healthy
                429 => {
                    events.push(
                        Event::new(
                            "Vault Standby",
                            "Vault is in standby mode",
                            EventSeverity::Info,
                            self.name(),
                            "vault_standby",
                        ),
                    );
                }
                _ => {}
            }

            // Check seal status
            if let Ok(data) = response.json::<serde_json::Value>().await {
                if let Some(sealed) = data["sealed"].as_bool() {
                    if sealed {
                        if let Some(threshold) = data["t"].as_u64() {
                            if let Some(shares) = data["n"].as_u64() {
                                events.push(
                                    Event::new(
                                        "Vault Seal Status",
                                        format!("Vault sealed: {}/{} unseal keys needed", shares, threshold),
                                        EventSeverity::Critical,
                                        self.name(),
                                        "seal_status",
                                    )
                                    .with_metadata("threshold", &threshold.to_string())
                                    .with_metadata("shares", &shares.to_string()),
                                );
                            }
                        }
                    }
                }
            }
        } else {
            events.push(
                Event::new(
                    "Vault Unavailable",
                    format!("Cannot connect to Vault at {}", self.address),
                    EventSeverity::Critical,
                    self.name(),
                    "vault_unavailable",
                ),
            );
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for VaultIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_vault_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_vault_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Vault at {}",
                self.address
            )))
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
    fn test_vault_integration_creation() {
        let vault = VaultIntegration::new("http://localhost:8200");
        assert!(vault.is_ok());
    }
}
