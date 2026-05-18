//! LDAP Integration
//!
//! Monitor LDAP/Active Directory server health and connections.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// LDAP integration
pub struct LdapIntegration {
    config: IntegrationConfig,
    host: String,
    port: u16,
    use_ssl: bool,
    bind_dn: Option<String>,
    bind_password: Option<String>,
}

impl LdapIntegration {
    /// Create new LDAP integration
    pub fn new(host: &str, port: u16) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("security_type".to_string(), "ldap".to_string());

        let config = IntegrationConfig {
            name: format!("ldap_{}", host),
            interval_secs: 60,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            host: host.to_string(),
            port,
            use_ssl: port == 636 || port == 6360,
            bind_dn: None,
            bind_password: None,
        })
    }

    /// Create with SSL
    pub fn with_ssl(mut self) -> Self {
        self.use_ssl = true;
        self.port = 636;
        self
    }

    /// Create with authentication
    pub fn with_auth(mut self, bind_dn: &str, bind_password: &str) -> Self {
        self.bind_dn = Some(bind_dn.to_string());
        self.bind_password = Some(bind_password.to_string());
        self
    }

    /// Check if LDAP is accessible
    async fn check_ldap_access(&self) -> bool {
        let timeout = tokio::time::Duration::from_secs(5);
        tokio::time::timeout(
            timeout,
            tokio::net::TcpStream::connect((self.host.as_str(), self.port)),
        )
        .await
        .is_ok_and(|r| r.is_ok())
    }

    /// Collect all LDAP metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        if !self.check_ldap_access().await {
            metrics.push(Metric::gauge(
                "ldap.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(metrics);
        }

        metrics.push(Metric::gauge(
            "ldap.connection.status",
            1.0,
            self.name(),
        ));

        metrics.push(Metric::gauge(
            "ldap.ssl",
            if self.use_ssl { 1.0 } else { 0.0 },
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect LDAP events
    async fn collect_ldap_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        if !self.check_ldap_access().await {
            events.push(
                Event::new(
                    "LDAP Unavailable",
                    format!("Cannot connect to LDAP at {}:{}", self.host, self.port),
                    EventSeverity::Critical,
                    self.name(),
                    "ldap_unavailable",
                ),
            );
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for LdapIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_ldap_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_ldap_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to LDAP at {}:{}",
                self.host, self.port
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
    fn test_ldap_integration_creation() {
        let ldap = LdapIntegration::new("localhost", 389);
        assert!(ldap.is_ok());
    }

    #[test]
    fn test_ldap_with_ssl() {
        let ldap = LdapIntegration::new("localhost", 636).unwrap().with_ssl();
        assert!(ldap.use_ssl);
    }
}
