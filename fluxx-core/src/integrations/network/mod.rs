//! Network Performance Monitoring
//!
//! Monitor TCP/UDP flows, DNS resolution, and network latency using eBPF.

use super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Network integration
pub struct NetworkIntegration {
    config: IntegrationConfig,
}

impl NetworkIntegration {
    /// Create new network integration
    pub fn new() -> Result<Self, IntegrationError> {
        let config = IntegrationConfig {
            name: "network".to_string(),
            interval_secs: 30,
            tags: [
                ("category".to_string(), "network".to_string()),
            ]
            .into_iter()
            .collect(),
            timeout_secs: 30,
        };

        Ok(Self { config })
    }

    /// Collect TCP connection metrics using /proc/net/tcp
    async fn collect_tcp_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];
        
        // Parse /proc/net/tcp for connection stats
        let tcp_content = tokio::fs::read_to_string("/proc/net/tcp")
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        let mut established = 0u64;
        let mut time_wait = 0u64;
        let mut close_wait = 0u64;
        let mut syn_sent = 0u64;

        for line in tcp_content.lines().skip(1) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 4 {
                continue;
            }

            match parts[3] {
                "01" => established += 1,  // ESTABLISHED
                "06" => time_wait += 1,     // TIME_WAIT
                "08" => close_wait += 1,    // CLOSE_WAIT
                "02" => syn_sent += 1,      // SYN_SENT
                _ => {}
            }
        }

        metrics.push(Metric::gauge(
            "net.tcp.connections.established",
            established as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "net.tcp.connections.time_wait",
            time_wait as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "net.tcp.connections.close_wait",
            close_wait as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "net.tcp.connections.syn_sent",
            syn_sent as f64,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect network interface statistics
    async fn collect_interface_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let net_dev = tokio::fs::read_to_string("/proc/net/dev")
            .await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        for line in net_dev.lines().skip(2) {
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() < 9 {
                continue;
            }

            let iface = parts[0].trim_end_matches(':');
            if iface == "lo" {
                continue;  // Skip loopback
            }

            let rx_bytes: u64 = parts[1].parse().unwrap_or(0);
            let rx_packets: u64 = parts[2].parse().unwrap_or(0);
            let rx_errors: u64 = parts[3].parse().unwrap_or(0);
            let rx_drops: u64 = parts[4].parse().unwrap_or(0);
            let tx_bytes: u64 = parts[9].parse().unwrap_or(0);
            let tx_packets: u64 = parts[10].parse().unwrap_or(0);
            let tx_errors: u64 = parts[11].parse().unwrap_or(0);
            let tx_drops: u64 = parts[12].parse().unwrap_or(0);

            let tags: HashMap<String, String> = [("interface".to_string(), iface.to_string())]
                .into_iter()
                .collect();

            metrics.push(
                Metric::counter("net.interface.rx_bytes", rx_bytes, self.name())
                    .with_tags(tags.clone()),
            );
            metrics.push(
                Metric::counter("net.interface.rx_packets", rx_packets, self.name())
                    .with_tags(tags.clone()),
            );
            metrics.push(
                Metric::counter("net.interface.rx_errors", rx_errors, self.name())
                    .with_tags(tags.clone()),
            );
            metrics.push(
                Metric::counter("net.interface.rx_drops", rx_drops, self.name())
                    .with_tags(tags.clone()),
            );
            metrics.push(
                Metric::counter("net.interface.tx_bytes", tx_bytes, self.name())
                    .with_tags(tags.clone()),
            );
            metrics.push(
                Metric::counter("net.interface.tx_packets", tx_packets, self.name())
                    .with_tags(tags.clone()),
            );
            metrics.push(
                Metric::counter("net.interface.tx_errors", tx_errors, self.name())
                    .with_tags(tags.clone()),
            );
            metrics.push(
                Metric::counter("net.interface.tx_drops", tx_drops, self.name())
                    .with_tags(tags),
            );
        }

        Ok(metrics)
    }

    /// Collect socket statistics
    async fn collect_socket_stats(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Try to read sockstat
        if let Ok(sockstat) = tokio::fs::read_to_string("/proc/net/sockstat").await {
            for line in sockstat.lines() {
                if line.starts_with("TCP:") {
                    // Parse: "TCP: inuse 15 orphan 0 tw 0 alloc 16 mem 10"
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    for i in 0..parts.len() {
                        match parts[i] {
                            "inuse" => {
                                if let Ok(val) = parts[i + 1].parse::<u64>() {
                                    metrics.push(Metric::gauge(
                                        "net.sockets.tcp_inuse",
                                        val as f64,
                                        self.name(),
                                    ));
                                }
                            }
                            "orphan" => {
                                if let Ok(val) = parts[i + 1].parse::<u64>() {
                                    metrics.push(Metric::gauge(
                                        "net.sockets.tcp_orphan",
                                        val as f64,
                                        self.name(),
                                    ));
                                }
                            }
                            "tw" => {
                                if let Ok(val) = parts[i + 1].parse::<u64>() {
                                    metrics.push(Metric::gauge(
                                        "net.sockets.tcp_timewait",
                                        val as f64,
                                        self.name(),
                                    ));
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }
        }

        Ok(metrics)
    }

    /// Perform DNS resolution check
    async fn check_dns_resolution(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Check DNS resolution time for common services
        let hosts = vec!["google.com", "cloudflare.com", "github.com"];
        
        for host in hosts {
            let start = std::time::Instant::now();
            let result = tokio::net::lookup_host(format!("{}:80", host)).await;
            let duration_ms = start.elapsed().as_millis() as f64;

            let tags: HashMap<String, String> = [("host".to_string(), host.to_string())]
                .into_iter()
                .collect();

            metrics.push(
                Metric::gauge("net.dns.resolution_time_ms", duration_ms, self.name())
                    .with_tags(tags.clone()),
            );
            metrics.push(
                Metric::gauge(
                    "net.dns.resolution_success",
                    if result.is_ok() { 1.0 } else { 0.0 },
                    self.name(),
                )
                .with_tags(tags),
            );
        }

        Ok(metrics)
    }
}

#[async_trait]
impl Integration for NetworkIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        match self.collect_tcp_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect TCP metrics: {}", e),
        }

        match self.collect_interface_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect interface metrics: {}", e),
        }

        match self.collect_socket_stats().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect socket stats: {}", e),
        }

        match self.check_dns_resolution().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to check DNS: {}", e),
        }

        Ok(all_metrics)
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check for high connection counts
        let tcp_content = tokio::fs::read_to_string("/proc/net/tcp")
            .await
            .unwrap_or_default();
        
        let established = tcp_content.lines().skip(1).count() as f64;
        
        if established > 10000.0 {
            events.push(
                Event::new(
                    "High TCP Connection Count",
                    format!("{} established TCP connections", established),
                    EventSeverity::Warning,
                    self.name(),
                    "high_connection_count",
                )
                .with_metadata("connections", &established.to_string()),
            );
        }

        // Check for packet errors
        if let Ok(net_dev) = tokio::fs::read_to_string("/proc/net/dev").await {
            let mut total_rx_errors = 0u64;
            let mut total_tx_errors = 0u64;

            for line in net_dev.lines().skip(2) {
                let parts: Vec<&str> = line.split_whitespace().collect();
                if parts.len() >= 12 {
                    total_rx_errors += parts[3].parse::<u64>().unwrap_or(0);
                    total_tx_errors += parts[11].parse::<u64>().unwrap_or(0);
                }
            }

            if total_rx_errors > 1000 || total_tx_errors > 1000 {
                events.push(
                    Event::new(
                        "Network Packet Errors",
                        format!(
                            "RX errors: {}, TX errors: {}",
                            total_rx_errors, total_tx_errors
                        ),
                        EventSeverity::Warning,
                        self.name(),
                        "packet_errors",
                    )
                    .with_metadata("rx_errors", &total_rx_errors.to_string())
                    .with_metadata("tx_errors", &total_tx_errors.to_string()),
                );
            }
        }

        Ok(events)
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        // Check if we can read network stats
        tokio::fs::read_to_string("/proc/net/dev")
            .await
            .map_err(|e| IntegrationError::Other(format!("Cannot read network stats: {}", e)))?;

        Ok(())
    }

    fn config(&self) -> &IntegrationConfig {
        &self.config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_network_integration_creation() {
        let net = NetworkIntegration::new();
        assert!(net.is_ok());
    }
}
