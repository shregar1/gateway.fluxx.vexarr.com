//! Memcached Integration
//!
//! Monitor Memcached server statistics, connections, and cache efficiency.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Validate hostname/IP to prevent command injection
/// Only allows alphanumeric, dots, dashes, and colons (for IPv6)
fn is_valid_host(host: &str) -> bool {
    if host.is_empty() || host.len() > 253 {
        return false;
    }
    
    // Check for shell metacharacters
    let forbidden_chars = [';', '|', '&', '$', '`', '(', ')', '{', '}', '<', '>', 
                          '\n', '\r', '\t', ' ', '\'', '"', '\\'];
    if host.chars().any(|c| forbidden_chars.contains(&c)) {
        return false;
    }
    
    // Must be valid characters only
    host.chars().all(|c| {
        c.is_alphanumeric() || c == '.' || c == '-' || c == ':' || c == '_'
    })
}

/// Memcached integration
pub struct MemcachedIntegration {
    config: IntegrationConfig,
    host: String,
    port: u16,
}

impl MemcachedIntegration {
    /// Create new Memcached integration
    pub fn new(host: &str, port: u16) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cache_type".to_string(), "memcached".to_string());

        let config = IntegrationConfig {
            name: format!("memcached_{}_{}", host.replace('.', "_"), port),
            interval_secs: 30,
            tags,
            timeout_secs: 5,
        };

        Ok(Self {
            config,
            host: host.to_string(),
            port,
        })
    }

    /// Check if Memcached is reachable
    async fn check_memcached_access(&self) -> bool {
        let timeout = tokio::time::Duration::from_secs(5);
        
        tokio::time::timeout(
            timeout,
            tokio::net::TcpStream::connect((self.host.as_str(), self.port)),
        )
        .await
        .is_ok_and(|r| r.is_ok())
    }

    /// Execute stats command via direct TCP connection (secure, no shell injection)
    async fn get_stats(&self) -> Result<HashMap<String, String>, IntegrationError> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        
        let mut stats = HashMap::new();

        // Validate host - only allow valid hostnames/IPs (no shell metacharacters)
        if !is_valid_host(&self.host) {
            return Err(IntegrationError::Config(
                format!("Invalid host: {}", self.host)
            ));
        }

        // Connect directly via TCP (no shell command)
        let addr = format!("{}:{}", self.host, self.port);
        let timeout = tokio::time::Duration::from_secs(5);
        
        let stream = tokio::time::timeout(
            timeout,
            tokio::net::TcpStream::connect(&addr)
        )
        .await
        .map_err(|_| IntegrationError::Connection("Connection timeout".to_string()))?
        .map_err(|e| IntegrationError::Connection(format!("Failed to connect: {}", e)))?;
        
        // Send "stats" command directly
        let (mut reader, mut writer) = stream.into_split();
        writer.write_all(b"stats\r\nquit\r\n").await
            .map_err(|e| IntegrationError::Connection(format!("Write failed: {}", e)))?;
        drop(writer); // Close write half
        
        // Read response
        let mut buffer = Vec::new();
        tokio::time::timeout(
            timeout,
            reader.read_to_end(&mut buffer)
        )
        .await
        .map_err(|_| IntegrationError::Connection("Read timeout".to_string()))?
        .map_err(|e| IntegrationError::Connection(format!("Read failed: {}", e)))?;
        
        let response = String::from_utf8_lossy(&buffer);
        
        for line in response.lines() {
            // Parse lines like: STAT <name> <value>
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 3 && parts[0] == "STAT" {
                let name = parts[1];
                let value = parts[2];
                stats.insert(name.to_string(), value.to_string());
            }
        }

        Ok(stats)
    }

    /// Collect Memcached metrics
    async fn collect_memcached_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Check connectivity first
        if !self.check_memcached_access().await {
            metrics.push(Metric::gauge(
                "memcached.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(metrics);
        }

        metrics.push(Metric::gauge(
            "memcached.connection.status",
            1.0,
            self.name(),
        ));

        let stats = self.get_stats().await?;

        // Connection metrics
        if let Some(val) = stats.get("curr_connections") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.connections.current",
                    v,
                    self.name(),
                ));
            }
        }
        if let Some(val) = stats.get("total_connections") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.connections.total",
                    v,
                    self.name(),
                ));
            }
        }

        // Cache metrics
        if let Some(val) = stats.get("curr_items") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.items.current",
                    v,
                    self.name(),
                ));
            }
        }
        if let Some(val) = stats.get("total_items") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.items.total",
                    v,
                    self.name(),
                ));
            }
        }

        // Memory metrics
        if let Some(val) = stats.get("bytes") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.memory.bytes_used",
                    v,
                    self.name(),
                ));
            }
        }
        if let Some(val) = stats.get("limit_maxbytes") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.memory.limit_bytes",
                    v,
                    self.name(),
                ));
            }
        }

        // Command metrics
        if let Some(val) = stats.get("cmd_get") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.commands.get",
                    v,
                    self.name(),
                ));
            }
        }
        if let Some(val) = stats.get("cmd_set") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.commands.set",
                    v,
                    self.name(),
                ));
            }
        }
        if let Some(val) = stats.get("cmd_flush") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.commands.flush",
                    v,
                    self.name(),
                ));
            }
        }

        // Get/Set rates
        if let (Some(get_hits), Some(get_misses)) = (
            stats.get("get_hits"),
            stats.get("get_misses"),
        ) {
            if let (Ok(hits), Ok(misses)) = (get_hits.parse::<f64>(), get_misses.parse::<f64>()) {
                metrics.push(Metric::gauge(
                    "memcached.gets.hits",
                    hits,
                    self.name(),
                ));
                metrics.push(Metric::gauge(
                    "memcached.gets.misses",
                    misses,
                    self.name(),
                ));

                // Hit ratio
                let total = hits + misses;
                if total > 0.0 {
                    let hit_ratio = (hits / total) * 100.0;
                    metrics.push(Metric::gauge(
                        "memcached.gets.hit_ratio",
                        hit_ratio,
                        self.name(),
                    ));
                }
            }
        }

        // Delete metrics
        if let (Some(del_hits), Some(del_misses)) = (
            stats.get("delete_hits"),
            stats.get("delete_misses"),
        ) {
            if let (Ok(hits), Ok(misses)) = (del_hits.parse::<f64>(), del_misses.parse::<f64>()) {
                metrics.push(Metric::gauge(
                    "memcached.deletes.hits",
                    hits,
                    self.name(),
                ));
                metrics.push(Metric::gauge(
                    "memcached.deletes.misses",
                    misses,
                    self.name(),
                ));
            }
        }

        // Eviction metrics
        if let Some(val) = stats.get("evictions") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.evictions",
                    v,
                    self.name(),
                ));
            }
        }
        if let Some(val) = stats.get("reclaimed") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.reclaimed",
                    v,
                    self.name(),
                ));
            }
        }

        // Network metrics
        if let Some(val) = stats.get("bytes_read") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.network.bytes_read",
                    v,
                    self.name(),
                ));
            }
        }
        if let Some(val) = stats.get("bytes_written") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.network.bytes_written",
                    v,
                    self.name(),
                ));
            }
        }

        // Thread metrics
        if let Some(val) = stats.get("threads") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.threads",
                    v,
                    self.name(),
                ));
            }
        }

        // Uptime
        if let Some(val) = stats.get("uptime") {
            if let Ok(v) = val.parse::<f64>() {
                metrics.push(Metric::gauge(
                    "memcached.uptime_seconds",
                    v,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect Memcached events
    async fn collect_memcached_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check connectivity
        if !self.check_memcached_access().await {
            events.push(
                Event::new(
                    "Memcached Unavailable",
                    format!("Cannot connect to Memcached at {}:{}", self.host, self.port),
                    EventSeverity::Critical,
                    self.name(),
                    "memcached_unavailable",
                ),
            );
            return Ok(events);
        }

        let stats = self.get_stats().await?;

        // Check hit ratio
        if let (Some(get_hits), Some(get_misses)) = (
            stats.get("get_hits"),
            stats.get("get_misses"),
        ) {
            if let (Ok(hits), Ok(misses)) = (get_hits.parse::<f64>(), get_misses.parse::<f64>()) {
                let total = hits + misses;
                if total > 0.0 {
                    let hit_ratio = (hits / total) * 100.0;
                    if hit_ratio < 50.0 && total > 1000.0 {
                        events.push(
                            Event::new(
                                "Low Memcached Hit Ratio",
                                format!("Hit ratio is {:.1}% ({} hits, {} misses)", hit_ratio, hits, misses),
                                EventSeverity::Warning,
                                self.name(),
                                "low_hit_ratio",
                            )
                            .with_metadata("hit_ratio", &hit_ratio.to_string())
                            .with_metadata("hits", &hits.to_string())
                            .with_metadata("misses", &misses.to_string()),
                        );
                    }
                }
            }
        }

        // Check evictions
        if let Some(val) = stats.get("evictions") {
            if let Ok(evictions) = val.parse::<f64>() {
                if evictions > 0.0 {
                    events.push(
                        Event::new(
                            "Memcached Evictions",
                            format!("{} items evicted from cache", evictions),
                            EventSeverity::Warning,
                            self.name(),
                            "evictions",
                        )
                        .with_metadata("evictions", &evictions.to_string()),
                    );
                }
            }
        }

        // Check memory usage
        if let (Some(bytes), Some(limit)) = (
            stats.get("bytes"),
            stats.get("limit_maxbytes"),
        ) {
            if let (Ok(used), Ok(max)) = (bytes.parse::<f64>(), limit.parse::<f64>()) {
                if max > 0.0 {
                    let usage_pct = (used / max) * 100.0;
                    if usage_pct > 90.0 {
                        events.push(
                            Event::new(
                                "High Memcached Memory Usage",
                                format!("Memory usage is {:.1}%", usage_pct),
                                EventSeverity::Warning,
                                self.name(),
                                "high_memory_usage",
                            )
                            .with_metadata("usage_percent", &usage_pct.to_string())
                            .with_metadata("used_bytes", &used.to_string())
                            .with_metadata("limit_bytes", &max.to_string()),
                        );
                    }
                }
            }
        }

        // Check connections
        if let Some(val) = stats.get("curr_connections") {
            if let Ok(connections) = val.parse::<f64>() {
                if connections > 1000.0 {
                    events.push(
                        Event::new(
                            "High Memcached Connections",
                            format!("{} current connections", connections),
                            EventSeverity::Info,
                            self.name(),
                            "high_connections",
                        )
                        .with_metadata("connections", &connections.to_string()),
                    );
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for MemcachedIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_memcached_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_memcached_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_memcached_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(format!(
                "Cannot connect to Memcached at {}:{}",
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
    fn test_memcached_integration_creation() {
        let memcached = MemcachedIntegration::new("localhost", 11211);
        assert!(memcached.is_ok());
    }
}
