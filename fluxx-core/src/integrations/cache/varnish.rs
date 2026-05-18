//! Varnish Integration
//!
//! Monitor Varnish Cache hit rates, backend health, and performance.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// Varnish integration
pub struct VarnishIntegration {
    config: IntegrationConfig,
    instance_name: Option<String>,
}

impl VarnishIntegration {
    /// Create new Varnish integration
    pub fn new() -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cache_type".to_string(), "varnish".to_string());

        let config = IntegrationConfig {
            name: "varnish".to_string(),
            interval_secs: 30,
            tags,
            timeout_secs: 10,
        };

        Ok(Self {
            config,
            instance_name: None,
        })
    }

    /// Create with specific instance name
    pub fn with_instance(name: &str) -> Result<Self, IntegrationError> {
        let mut this = Self::new()?;
        this.instance_name = Some(name.to_string());
        this.config.name = format!("varnish_{}", name);
        Ok(this)
    }

    /// Check if varnishstat is available
    async fn check_varnish_access(&self) -> bool {
        let mut cmd = tokio::process::Command::new("varnishstat");
        cmd.arg("-1");
        
        if let Some(ref name) = self.instance_name {
            cmd.args(["-n", name]);
        }

        match cmd.output().await {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    /// Get varnishstat output
    async fn get_stats(&self) -> Result<HashMap<String, f64>, IntegrationError> {
        let mut stats = HashMap::new();

        let mut cmd = tokio::process::Command::new("varnishstat");
        cmd.arg("-1");
        
        if let Some(ref name) = self.instance_name {
            cmd.args(["-n", name]);
        }

        let output = cmd.output().await
            .map_err(|e| IntegrationError::Connection(e.to_string()))?;

        if !output.status.success() {
            return Ok(stats);
        }

        let stdout = String::from_utf8_lossy(&output.stdout);
        
        for line in stdout.lines() {
            // Parse lines like: <name> <value> <description>
            let parts: Vec<&str> = line.split_whitespace().collect();
            if parts.len() >= 2 {
                let name = parts[0];
                if let Ok(value) = parts[1].parse::<f64>() {
                    stats.insert(name.to_string(), value);
                }
            }
        }

        Ok(stats)
    }

    /// Collect Varnish metrics
    async fn collect_varnish_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Check access first
        if !self.check_varnish_access().await {
            metrics.push(Metric::gauge(
                "varnish.connection.status",
                0.0,
                self.name(),
            ));
            return Ok(metrics);
        }

        metrics.push(Metric::gauge(
            "varnish.connection.status",
            1.0,
            self.name(),
        ));

        let stats = self.get_stats().await?;

        // Client request metrics
        if let Some(v) = stats.get("MAIN.client_req") {
            metrics.push(Metric::gauge(
                "varnish.client.requests",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.client_req_400") {
            metrics.push(Metric::gauge(
                "varnish.client.requests_400",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.client_req_417") {
            metrics.push(Metric::gauge(
                "varnish.client.requests_417",
                *v,
                self.name(),
            ));
        }

        // Cache hit/miss metrics
        if let Some(v) = stats.get("MAIN.cache_hit") {
            metrics.push(Metric::gauge(
                "varnish.cache.hits",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.cache_hitpass") {
            metrics.push(Metric::gauge(
                "varnish.cache.hitpass",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.cache_miss") {
            metrics.push(Metric::gauge(
                "varnish.cache.misses",
                *v,
                self.name(),
            ));
        }

        // Calculate hit ratio
        let hits = stats.get("MAIN.cache_hit").copied().unwrap_or(0.0);
        let misses = stats.get("MAIN.cache_miss").copied().unwrap_or(0.0);
        let total = hits + misses;
        if total > 0.0 {
            let hit_ratio = (hits / total) * 100.0;
            metrics.push(Metric::gauge(
                "varnish.cache.hit_ratio",
                hit_ratio,
                self.name(),
            ));
        }

        // Backend metrics
        if let Some(v) = stats.get("MAIN.backend_conn") {
            metrics.push(Metric::gauge(
                "varnish.backend.connections",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.backend_unhealthy") {
            metrics.push(Metric::gauge(
                "varnish.backend.unhealthy",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.backend_busy") {
            metrics.push(Metric::gauge(
                "varnish.backend.busy",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.backend_fail") {
            metrics.push(Metric::gauge(
                "varnish.backend.failures",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.backend_reuse") {
            metrics.push(Metric::gauge(
                "varnish.backend.reuse",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.backend_recycle") {
            metrics.push(Metric::gauge(
                "varnish.backend.recycle",
                *v,
                self.name(),
            ));
        }

        // Session metrics
        if let Some(v) = stats.get("MAIN.s_sess") {
            metrics.push(Metric::gauge(
                "varnish.sessions.total",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.s_req") {
            metrics.push(Metric::gauge(
                "varnish.sessions.requests",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.s_pipe") {
            metrics.push(Metric::gauge(
                "varnish.sessions.pipe",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.s_pass") {
            metrics.push(Metric::gauge(
                "varnish.sessions.pass",
                *v,
                self.name(),
            ));
        }

        // Thread metrics
        if let Some(v) = stats.get("MAIN.threads") {
            metrics.push(Metric::gauge(
                "varnish.threads",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.threads_created") {
            metrics.push(Metric::gauge(
                "varnish.threads.created",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.threads_failed") {
            metrics.push(Metric::gauge(
                "varnish.threads.failed",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.threads_limited") {
            metrics.push(Metric::gauge(
                "varnish.threads.limited",
                *v,
                self.name(),
            ));
        }

        // Object metrics
        if let Some(v) = stats.get("MAIN.n_object") {
            metrics.push(Metric::gauge(
                "varnish.objects",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.n_objectcore") {
            metrics.push(Metric::gauge(
                "varnish.objects.core",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.n_objecthead") {
            metrics.push(Metric::gauge(
                "varnish.objects.head",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.n_expired") {
            metrics.push(Metric::gauge(
                "varnish.objects.expired",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.n_lru_nuked") {
            metrics.push(Metric::gauge(
                "varnish.objects.lru_nuked",
                *v,
                self.name(),
            ));
        }

        // Ban metrics
        if let Some(v) = stats.get("MAIN.bans") {
            metrics.push(Metric::gauge(
                "varnish.bans.total",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.bans_completed") {
            metrics.push(Metric::gauge(
                "varnish.bans.completed",
                *v,
                self.name(),
            ));
        }

        // ESI metrics
        if let Some(v) = stats.get("MAIN.esi_errors") {
            metrics.push(Metric::gauge(
                "varnish.esi.errors",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("MAIN.esi_warnings") {
            metrics.push(Metric::gauge(
                "varnish.esi.warnings",
                *v,
                self.name(),
            ));
        }

        // MSE (Massive Storage Engine) metrics if available
        if let Some(v) = stats.get("MSE.storage") {
            metrics.push(Metric::gauge(
                "varnish.mse.storage_bytes",
                *v,
                self.name(),
            ));
        }

        // SMA (malloc storage) metrics
        if let Some(v) = stats.get("SMA.s0.g_bytes") {
            metrics.push(Metric::gauge(
                "varnish.sma.bytes_allocated",
                *v,
                self.name(),
            ));
        }
        if let Some(v) = stats.get("SMA.s0.g_space") {
            metrics.push(Metric::gauge(
                "varnish.sma.bytes_free",
                *v,
                self.name(),
            ));
        }

        Ok(metrics)
    }

    /// Collect Varnish events
    async fn collect_varnish_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check access
        if !self.check_varnish_access().await {
            events.push(
                Event::new(
                    "Varnish Unavailable",
                    "varnishstat command not available or no Varnish instance running".to_string(),
                    EventSeverity::Critical,
                    self.name(),
                    "varnish_unavailable",
                ),
            );
            return Ok(events);
        }

        let stats = self.get_stats().await?;

        // Check hit ratio
        let hits = stats.get("MAIN.cache_hit").copied().unwrap_or(0.0);
        let misses = stats.get("MAIN.cache_miss").copied().unwrap_or(0.0);
        let total = hits + misses;
        
        if total > 1000.0 {
            let hit_ratio = (hits / total) * 100.0;
            if hit_ratio < 50.0 {
                events.push(
                    Event::new(
                        "Low Varnish Hit Ratio",
                        format!("Hit ratio is {:.1}%", hit_ratio),
                        EventSeverity::Warning,
                        self.name(),
                        "low_hit_ratio",
                    )
                    .with_metadata("hit_ratio", &hit_ratio.to_string()),
                );
            }
        }

        // Check backend health
        if let Some(failures) = stats.get("MAIN.backend_fail") {
            if *failures > 0.0 {
                events.push(
                    Event::new(
                        "Varnish Backend Failures",
                        format!("{} backend failures detected", failures),
                        EventSeverity::Warning,
                        self.name(),
                        "backend_failures",
                    )
                    .with_metadata("failures", &failures.to_string()),
                );
            }
        }

        if let Some(unhealthy) = stats.get("MAIN.backend_unhealthy") {
            if *unhealthy > 0.0 {
                events.push(
                    Event::new(
                        "Varnish Unhealthy Backends",
                        format!("{} backends marked as unhealthy", unhealthy),
                        EventSeverity::Critical,
                        self.name(),
                        "unhealthy_backends",
                    )
                    .with_metadata("unhealthy_count", &unhealthy.to_string()),
                );
            }
        }

        // Check thread pool
        if let Some(failed) = stats.get("MAIN.threads_failed") {
            if *failed > 0.0 {
                events.push(
                    Event::new(
                        "Varnish Thread Failures",
                        format!("{} threads failed to create", failed),
                        EventSeverity::Warning,
                        self.name(),
                        "thread_failures",
                    )
                    .with_metadata("failed", &failed.to_string()),
                );
            }
        }

        // Check ESI errors
        if let Some(esi_errors) = stats.get("MAIN.esi_errors") {
            if *esi_errors > 0.0 {
                events.push(
                    Event::new(
                        "Varnish ESI Errors",
                        format!("{} ESI errors detected", esi_errors),
                        EventSeverity::Warning,
                        self.name(),
                        "esi_errors",
                    )
                    .with_metadata("errors", &esi_errors.to_string()),
                );
            }
        }

        // Check LRU nuked objects (cache eviction)
        if let Some(nuked) = stats.get("MAIN.n_lru_nuked") {
            if *nuked > 1000.0 {
                events.push(
                    Event::new(
                        "Varnish LRU Eviction",
                        format!("{} objects evicted from cache", nuked),
                        EventSeverity::Info,
                        self.name(),
                        "lru_eviction",
                    )
                    .with_metadata("nuked", &nuked.to_string()),
                );
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for VarnishIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_varnish_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_varnish_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_varnish_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Connection(
                "varnishstat not available or no Varnish instance running".to_string()
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
    fn test_varnish_integration_creation() {
        let varnish = VarnishIntegration::new();
        assert!(varnish.is_ok());
    }
}
