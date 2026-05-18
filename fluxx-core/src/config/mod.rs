use serde::{Deserialize, Serialize};
use anyhow::{Result, Context};
use std::env;

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub agent_name: String,
    pub log_level: String,
    pub server: ServerConfig,
    #[serde(default)]
    pub monitoring: MonitoringConfig,
    #[serde(default)]
    pub otlp: OtlpConfig,
    #[serde(default)]
    pub logs: LogConfig,
    #[serde(default)]
    pub statsd: StatsdConfig,
    #[serde(default)]
    pub metric_alerts: MetricAlertConfig,
    #[serde(default)]
    pub clickhouse_error_events: ClickHouseErrorEventsConfig,
}

/// Configuration for persisting error events to ClickHouse.
///
/// Opt-in: disabled by default. When enabled, the ErrorTracker writes
/// every tracked occurrence into `fluxx.error_events` via the HTTP
/// interface, batched. See `src/storage/clickhouse_error_events.rs`.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClickHouseErrorEventsConfig {
    pub enabled: bool,
    pub host: String,
    pub port: u16,
    pub database: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub flush_interval_secs: u64,
    pub flush_batch_size: usize,
    pub queue_capacity: usize,
    pub http_timeout_secs: u64,
}

impl Default for ClickHouseErrorEventsConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            host: "localhost".to_string(),
            port: 8123,
            database: "fluxx".to_string(),
            username: None,
            password: None,
            flush_interval_secs: 5,
            flush_batch_size: 1000,
            queue_capacity: 100_000,
            http_timeout_secs: 30,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MetricAlertConfig {
    pub enabled: bool,
    pub check_interval_secs: u64,
    pub lookback_secs: u64,
}

impl Default for MetricAlertConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            check_interval_secs: 30,
            lookback_secs: 300,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct OtlpConfig {
    pub enabled: bool,
    pub grpc_port: u16,
    pub http_enabled: bool,
    /// Canonical OTLP HTTP listener port. The OTel ecosystem defaults
    /// to 4318 for HTTP exporters, so SDKs that ship `localhost:4318`
    /// as the user-set endpoint Just Work.
    #[serde(default = "default_http_port")]
    pub http_port: u16,
}

fn default_http_port() -> u16 { 4318 }

impl Default for OtlpConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            grpc_port: 4317,
            http_enabled: true,
            http_port: 4318,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogConfig {
    pub enabled: bool,
    pub max_entries: usize,
    pub retention_secs: u64,
    /// Log store backend: "memory", "file", "wal"
    #[serde(default = "default_log_backend")]
    pub backend: String,
    /// File backend config (used when backend = "file")
    #[serde(default)]
    pub file: LogFileConfig,
    /// WAL backend config (used when backend = "wal")
    #[serde(default)]
    pub wal: LogWalConfig,
}

fn default_log_backend() -> String { "memory".to_string() }

/// File log store configuration.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogFileConfig {
    pub directory: String,
    pub max_file_bytes: u64,
    pub max_files: usize,
    pub prefix: String,
}

impl Default for LogFileConfig {
    fn default() -> Self {
        Self {
            directory: "/var/lib/fluxx/logs".to_string(),
            max_file_bytes: 100 * 1024 * 1024,
            max_files: 10,
            prefix: "fluxx-logs".to_string(),
        }
    }
}

/// WAL log store configuration.
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct LogWalConfig {
    pub wal_dir: String,
    pub compress: bool,
}

impl Default for LogWalConfig {
    fn default() -> Self {
        Self {
            wal_dir: "/var/lib/fluxx/log-wal".to_string(),
            compress: false,
        }
    }
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            enabled: true,
            max_entries: 1_000_000,
            retention_secs: 86400,
            backend: "memory".to_string(),
            file: LogFileConfig::default(),
            wal: LogWalConfig::default(),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct StatsdConfig {
    pub enabled: bool,
    pub bind_address: String,
    pub flush_interval_secs: u64,
}

impl Default for StatsdConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            bind_address: "0.0.0.0:8125".to_string(),
            flush_interval_secs: 10,
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct MonitoringConfig {
    pub enable_system: bool,
    pub enable_pcap: bool,
}

impl Default for MonitoringConfig {
    fn default() -> Self {
        Self {
            enable_system: false, // Disabled by default as requested
            enable_pcap: false,   // Disabled by default as requested
        }
    }
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ServerConfig {
    pub host: String,
    pub port: u16,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            agent_name: "fluxx-agent-01".into(),
            log_level: "info".into(),
            server: ServerConfig {
                host: "127.0.0.1".into(),
                port: 7007,
            },
            monitoring: MonitoringConfig::default(),
            otlp: OtlpConfig::default(),
            logs: LogConfig::default(),
            statsd: StatsdConfig::default(),
            metric_alerts: MetricAlertConfig::default(),
            clickhouse_error_events: ClickHouseErrorEventsConfig::default(),
        }
    }
}

impl Config {
    pub fn load() -> Result<Self> {
        let run_mode = env::var("RUN_MODE").unwrap_or_else(|_| "development".into());

        let s = config::Config::builder()
            // Start off by merging in the "default" configuration file
            .add_source(config::File::with_name("config/default").required(false))
            // Add in the current environment file
            // Default to 'development' env
            // Note that this file is _optional_
            .add_source(
                config::File::with_name(&format!("config/{}", run_mode))
                    .required(false),
            )
            // Settings from environment vars, prefix `APP_`. Nested
            // keys use `__` so `APP_SERVER__PORT=7007` sets
            // `server.port`. `prefix_separator` makes the strip explicit
            // so the crate doesn't get confused by single-underscore
            // top-level keys like `APP_AGENT_NAME`. `try_parsing` lets
            // numeric/boolean fields take their typed value.
            .add_source(
                config::Environment::with_prefix("APP")
                    .prefix_separator("_")
                    .separator("__")
                    .try_parsing(true),
            )
            .build()?;

        s.try_deserialize().context("Failed to deserialize configuration")
    }

    pub fn load_from_path(path: &str) -> Result<Self> {
        let s = config::Config::builder()
            .add_source(config::File::with_name(path))
            .build()
            .map_err(|e| anyhow::anyhow!("Config build failed: {}", e))?;

        s.try_deserialize().context("Failed to deserialize configuration from path")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 71: Config default values
    #[test]
    fn test_config_default() {
        let config = Config::default();
        assert_eq!(config.agent_name, "fluxx-agent-01");
        assert_eq!(config.log_level, "info");
        assert_eq!(config.server.host, "127.0.0.1");
        assert_eq!(config.server.port, 7007);
    }

    // Test 72: ServerConfig default
    #[test]
    fn test_server_config_default() {
        let server: ServerConfig = Config::default().server;
        assert_eq!(server.host, "127.0.0.1");
        assert_eq!(server.port, 7007);
    }

    // Test 73: Config clone
    #[test]
    fn test_config_clone() {
        let config = Config::default();
        let cloned = config.clone();
        assert_eq!(config.agent_name, cloned.agent_name);
        assert_eq!(config.log_level, cloned.log_level);
    }

    // Test 74: Config serialization
    #[test]
    fn test_config_serialization() {
        let config = Config::default();
        let json = serde_json::to_string(&config).unwrap();
        assert!(json.contains("fluxx-agent-01"));
        assert!(json.contains("7007"));
    }

    // Test 75: ServerConfig serialization
    #[test]
    fn test_server_config_serialization() {
        let server = ServerConfig { host: "0.0.0.0".to_string(), port: 3000 };
        let json = serde_json::to_string(&server).unwrap();
        assert!(json.contains("0.0.0.0"));
        assert!(json.contains("3000"));
    }

    // Test 76: Config deserialization
    #[test]
    fn test_config_deserialization() {
        let json = r#"{"agent_name":"test-agent","log_level":"debug","server":{"host":"0.0.0.0","port":3000}}"#;
        let config: Config = serde_json::from_str(json).unwrap();
        assert_eq!(config.agent_name, "test-agent");
        assert_eq!(config.log_level, "debug");
        assert_eq!(config.server.port, 3000);
    }

    // Test 77: Custom config values
    #[test]
    fn test_custom_config() {
        let config = Config {
            agent_name: "custom-agent".to_string(),
            log_level: "debug".to_string(),
            server: ServerConfig { host: "0.0.0.0".to_string(), port: 9090 },
            monitoring: MonitoringConfig::default(),
            otlp: OtlpConfig::default(),
            logs: LogConfig::default(),
            statsd: StatsdConfig::default(),
            metric_alerts: MetricAlertConfig::default(),
            clickhouse_error_events: ClickHouseErrorEventsConfig::default(),
        };
        assert_eq!(config.agent_name, "custom-agent");
        assert_eq!(config.server.port, 9090);
    }
}
