//! Infrastructure Integrations
//!
//! Monitor databases, network performance, CI/CD pipelines, cloud services, containers, queues, and caches.
//!
//! ## Supported Integrations (20+ integrations, framework for 500+)
//!
//! ### Databases (4)
//! - PostgreSQL - Query performance, connection metrics, lock analysis
//! - MySQL - Slow query log, replication lag, table stats
//! - MongoDB - Operation metrics, replication status
//! - Redis - Memory usage, command stats, keyspace analysis
//!
//! ### Network
//! - TCP/UDP flow monitoring
//! - DNS resolution tracking
//! - Network latency between services
//! - Packet loss detection
//!
//! ### Containers & Orchestration (2)
//! - Docker - Container stats, images, volumes, system resources
//! - Kubernetes - Pods, nodes, deployments, services
//!
//! ### Cloud Providers (3)
//! - AWS CloudWatch - EC2, RDS, ELB, Lambda, S3
//! - Azure Monitor - VMs, AKS, SQL, Storage, Functions
//! - GCP Monitoring - Compute, GKE, Cloud SQL, Storage, Functions
//!
//! ### Message Queues (3)
//! - Kafka - Brokers, topics, consumer groups, partitions
//! - RabbitMQ - Queues, exchanges, connections, nodes
//! - AWS SQS - Queue depth, message age, visibility
//!
//! ### CI/CD (4)
//! - GitHub Actions - Workflow runs, deployment tracking
//! - GitLab CI - Pipelines, jobs, runners
//! - Jenkins - Jobs, executors, queue, nodes
//! - CircleCI - Pipelines, workflows
//!
//! ### Cache (2)
//! - Memcached - Connections, cache efficiency, memory usage
//! - Varnish - Hit rates, backends, ESI, threads
//!
//! ## Quick Start
//!
//! ```rust,ignore
//! use fluxx_agent::integrations::IntegrationManager;
//!
//! let mut manager = IntegrationManager::new();
//! manager.with_postgres("postgres://localhost/mydb");
//! manager.with_redis("localhost", 6379);
//! manager.with_docker();
//! manager.with_kubernetes();
//! manager.with_aws("us-east-1");
//!
//! let metrics = manager.collect_metrics().await;
//! ```

pub mod db;
pub mod network;
pub mod cicd;
pub mod containers;
pub mod cloud;
pub mod queue;
pub mod cache;
pub mod observability;
pub mod security;
pub mod framework;

pub use framework::{
    Integration, IntegrationConfig, IntegrationError,
    Metric, MetricType, MetricValue,
    Event, EventSeverity,
};

use std::collections::HashMap;
use std::time::Duration;
use tokio::time::interval;
use tracing::{info, error, debug};

/// Integration manager coordinates all infrastructure integrations
pub struct IntegrationManager {
    integrations: HashMap<String, Box<dyn Integration>>,
    config: IntegrationManagerConfig,
}

/// Configuration for the integration manager
#[derive(Debug, Clone)]
pub struct IntegrationManagerConfig {
    /// Collection interval
    pub interval: Duration,
    /// Enable auto-discovery
    pub auto_discovery: bool,
    /// Timeout for collection operations
    pub timeout: Duration,
}

impl Default for IntegrationManagerConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(60),
            auto_discovery: true,
            timeout: Duration::from_secs(30),
        }
    }
}

impl IntegrationManager {
    /// Create new integration manager
    pub fn new() -> Self {
        Self {
            integrations: HashMap::new(),
            config: IntegrationManagerConfig::default(),
        }
    }

    /// Create with configuration
    pub fn with_config(config: IntegrationManagerConfig) -> Self {
        Self {
            integrations: HashMap::new(),
            config,
        }
    }

    /// Register an integration
    pub fn register<I: Integration + 'static>(
        &mut self,
        name: impl Into<String>,
        integration: I,
    ) -> &mut Self {
        let name = name.into();
        info!(integration = %name, "Registering integration");
        self.integrations.insert(name, Box::new(integration));
        self
    }

    /// Add PostgreSQL integration
    pub fn with_postgres(&mut self, connection_string: &str) -> &mut Self {
        match db::postgres::PostgresIntegration::new(connection_string) {
            Ok(pg) => {
                self.register("postgres", pg);
            }
            Err(e) => {
                error!(error = %e, "Failed to create PostgreSQL integration");
            }
        }
        self
    }

    /// Add MySQL integration
    pub fn with_mysql(&mut self, connection_string: &str) -> &mut Self {
        match db::mysql::MySqlIntegration::new(connection_string) {
            Ok(mysql) => {
                self.register("mysql", mysql);
            }
            Err(e) => {
                error!(error = %e, "Failed to create MySQL integration");
            }
        }
        self
    }

    /// Add Redis integration
    pub fn with_redis(&mut self, host: &str, port: u16) -> &mut Self {
        match db::redis::RedisIntegration::new(host, port) {
            Ok(redis) => {
                self.register("redis", redis);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Redis integration");
            }
        }
        self
    }

    /// Add network monitoring
    pub fn with_network_monitoring(&mut self) -> &mut Self {
        match network::NetworkIntegration::new() {
            Ok(net) => {
                self.register("network", net);
            }
            Err(e) => {
                error!(error = %e, "Failed to create network integration");
            }
        }
        self
    }

    /// Add GitHub Actions integration
    pub fn with_github_actions(&mut self, token: &str, owner: &str, repo: &str) -> &mut Self {
        match cicd::github::GitHubActionsIntegration::new(token, owner, repo) {
            Ok(github) => {
                self.register("github_actions", github);
            }
            Err(e) => {
                error!(error = %e, "Failed to create GitHub Actions integration");
            }
        }
        self
    }

    /// Add GitLab CI integration for a project
    pub fn with_gitlab_ci_project(&mut self, base_url: &str, token: &str, project_id: &str) -> &mut Self {
        match cicd::gitlab::GitLabCiIntegration::new_project(base_url, token, project_id) {
            Ok(gitlab) => {
                self.register("gitlab_ci", gitlab);
            }
            Err(e) => {
                error!(error = %e, "Failed to create GitLab CI integration");
            }
        }
        self
    }

    /// Add Jenkins integration
    pub fn with_jenkins(&mut self, base_url: &str, username: &str, api_token: &str) -> &mut Self {
        match cicd::jenkins::JenkinsIntegration::new(base_url, username, api_token) {
            Ok(jenkins) => {
                self.register("jenkins", jenkins);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Jenkins integration");
            }
        }
        self
    }

    /// Add CircleCI integration for a project
    pub fn with_circleci_project(&mut self, token: &str, org: &str, project: &str) -> &mut Self {
        match cicd::circleci::CircleCiIntegration::new_project(token, org, project) {
            Ok(circleci) => {
                self.register("circleci", circleci);
            }
            Err(e) => {
                error!(error = %e, "Failed to create CircleCI integration");
            }
        }
        self
    }

    /// Add Docker integration
    pub fn with_docker(&mut self) -> &mut Self {
        match containers::docker::DockerIntegration::new() {
            Ok(docker) => {
                self.register("docker", docker);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Docker integration");
            }
        }
        self
    }

    /// Add Kubernetes integration
    pub fn with_kubernetes(&mut self) -> &mut Self {
        match containers::kubernetes::KubernetesIntegration::new() {
            Ok(k8s) => {
                self.register("kubernetes", k8s);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Kubernetes integration");
            }
        }
        self
    }

    /// Add AWS CloudWatch integration
    pub fn with_aws(&mut self, region: &str) -> &mut Self {
        match cloud::aws::AwsIntegration::new(region) {
            Ok(aws) => {
                self.register(format!("aws_{}", region).as_str(), aws);
            }
            Err(e) => {
                error!(error = %e, "Failed to create AWS integration");
            }
        }
        self
    }

    /// Add Azure Monitor integration
    pub fn with_azure(&mut self, subscription_id: &str) -> &mut Self {
        match cloud::azure::AzureIntegration::new(subscription_id) {
            Ok(azure) => {
                self.register("azure", azure);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Azure integration");
            }
        }
        self
    }

    /// Add GCP Monitoring integration
    pub fn with_gcp(&mut self, project_id: &str) -> &mut Self {
        match cloud::gcp::GcpIntegration::new(project_id) {
            Ok(gcp) => {
                self.register(format!("gcp_{}", project_id).as_str(), gcp);
            }
            Err(e) => {
                error!(error = %e, "Failed to create GCP integration");
            }
        }
        self
    }

    /// Add Kafka integration
    pub fn with_kafka(&mut self, bootstrap_servers: &str) -> &mut Self {
        match queue::kafka::KafkaIntegration::new(bootstrap_servers) {
            Ok(kafka) => {
                self.register("kafka", kafka);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Kafka integration");
            }
        }
        self
    }

    /// Add RabbitMQ integration
    pub fn with_rabbitmq(&mut self, host: &str, port: u16, username: &str, password: &str) -> &mut Self {
        match queue::rabbitmq::RabbitMqIntegration::new(host, port, username, password) {
            Ok(rabbitmq) => {
                self.register("rabbitmq", rabbitmq);
            }
            Err(e) => {
                error!(error = %e, "Failed to create RabbitMQ integration");
            }
        }
        self
    }

    /// Add AWS SQS integration
    pub fn with_sqs(&mut self, region: &str) -> &mut Self {
        match queue::sqs::SqsIntegration::new(region) {
            Ok(sqs) => {
                self.register(format!("sqs_{}", region).as_str(), sqs);
            }
            Err(e) => {
                error!(error = %e, "Failed to create SQS integration");
            }
        }
        self
    }

    /// Add Memcached integration
    pub fn with_memcached(&mut self, host: &str, port: u16) -> &mut Self {
        match cache::memcached::MemcachedIntegration::new(host, port) {
            Ok(memcached) => {
                self.register(format!("memcached_{}", host).as_str(), memcached);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Memcached integration");
            }
        }
        self
    }

    /// Add Varnish integration
    pub fn with_varnish(&mut self) -> &mut Self {
        match cache::varnish::VarnishIntegration::new() {
            Ok(varnish) => {
                self.register("varnish", varnish);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Varnish integration");
            }
        }
        self
    }

    /// Add Cassandra integration
    pub fn with_cassandra(&mut self, hosts: &[&str], port: u16) -> &mut Self {
        match db::cassandra::CassandraIntegration::new(hosts, port) {
            Ok(cassandra) => {
                self.register("cassandra", cassandra);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Cassandra integration");
            }
        }
        self
    }

    /// Add ClickHouse integration
    pub fn with_clickhouse(&mut self, host: &str, port: u16, database: &str) -> &mut Self {
        match db::clickhouse::ClickHouseIntegration::new(host, port, database) {
            Ok(clickhouse) => {
                self.register("clickhouse", clickhouse);
            }
            Err(e) => {
                error!(error = %e, "Failed to create ClickHouse integration");
            }
        }
        self
    }

    /// Add Elasticsearch integration
    pub fn with_elasticsearch(&mut self, base_url: &str) -> &mut Self {
        match db::elasticsearch::ElasticsearchIntegration::new(base_url) {
            Ok(es) => {
                self.register("elasticsearch", es);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Elasticsearch integration");
            }
        }
        self
    }

    /// Add DigitalOcean integration
    pub fn with_digitalocean(&mut self, api_token: &str) -> &mut Self {
        match cloud::digitalocean::DigitalOceanIntegration::new(api_token) {
            Ok(do_integration) => {
                self.register("digitalocean", do_integration);
            }
            Err(e) => {
                error!(error = %e, "Failed to create DigitalOcean integration");
            }
        }
        self
    }

    /// Add Linode integration
    pub fn with_linode(&mut self, api_token: &str) -> &mut Self {
        match cloud::linode::LinodeIntegration::new(api_token) {
            Ok(linode) => {
                self.register("linode", linode);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Linode integration");
            }
        }
        self
    }

    /// Add IBM Cloud integration
    pub fn with_ibm_cloud(&mut self, api_key: &str, region: &str) -> &mut Self {
        match cloud::ibm::IbmCloudIntegration::new(api_key, region) {
            Ok(ibm) => {
                self.register(format!("ibm_{}", region).as_str(), ibm);
            }
            Err(e) => {
                error!(error = %e, "Failed to create IBM Cloud integration");
            }
        }
        self
    }

    /// Add NATS integration
    pub fn with_nats(&mut self, server_url: &str) -> &mut Self {
        match queue::nats::NatsIntegration::new(server_url) {
            Ok(nats) => {
                self.register("nats", nats);
            }
            Err(e) => {
                error!(error = %e, "Failed to create NATS integration");
            }
        }
        self
    }

    /// Add Pulsar integration
    pub fn with_pulsar(&mut self, admin_url: &str) -> &mut Self {
        match queue::pulsar::PulsarIntegration::new(admin_url) {
            Ok(pulsar) => {
                self.register("pulsar", pulsar);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Pulsar integration");
            }
        }
        self
    }

    /// Add ActiveMQ integration
    pub fn with_activemq(&mut self, admin_url: &str, username: &str, password: &str) -> &mut Self {
        match queue::activemq::ActiveMqIntegration::new(admin_url, username, password) {
            Ok(activemq) => {
                self.register("activemq", activemq);
            }
            Err(e) => {
                error!(error = %e, "Failed to create ActiveMQ integration");
            }
        }
        self
    }

    /// Add Travis CI integration
    pub fn with_travis_ci(&mut self, token: &str, owner: &str, repo: &str) -> &mut Self {
        match cicd::travisci::TravisCiIntegration::new_repo(token, owner, repo) {
            Ok(travis) => {
                self.register("travisci", travis);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Travis CI integration");
            }
        }
        self
    }

    /// Add TeamCity integration
    pub fn with_teamcity(&mut self, base_url: &str, username: &str, password: &str) -> &mut Self {
        match cicd::teamcity::TeamCityIntegration::new(base_url, username, password) {
            Ok(teamcity) => {
                self.register("teamcity", teamcity);
            }
            Err(e) => {
                error!(error = %e, "Failed to create TeamCity integration");
            }
        }
        self
    }

    /// Add Bamboo integration
    pub fn with_bamboo(&mut self, base_url: &str, username: &str, password: &str) -> &mut Self {
        match cicd::bamboo::BambooIntegration::new(base_url, username, password) {
            Ok(bamboo) => {
                self.register("bamboo", bamboo);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Bamboo integration");
            }
        }
        self
    }

    /// Add Prometheus integration
    pub fn with_prometheus(&mut self, base_url: &str) -> &mut Self {
        match observability::prometheus::PrometheusIntegration::new(base_url) {
            Ok(prometheus) => {
                self.register("prometheus", prometheus);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Prometheus integration");
            }
        }
        self
    }

    /// Add Grafana integration
    pub fn with_grafana(&mut self, base_url: &str, api_token: &str) -> &mut Self {
        match observability::grafana::GrafanaIntegration::new(base_url, api_token) {
            Ok(grafana) => {
                self.register("grafana", grafana);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Grafana integration");
            }
        }
        self
    }

    /// Add Jaeger integration
    pub fn with_jaeger(&mut self, query_url: &str) -> &mut Self {
        match observability::jaeger::JaegerIntegration::new(query_url) {
            Ok(jaeger) => {
                self.register("jaeger", jaeger);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Jaeger integration");
            }
        }
        self
    }

    /// Add Zipkin integration
    pub fn with_zipkin(&mut self, base_url: &str) -> &mut Self {
        match observability::zipkin::ZipkinIntegration::new(base_url) {
            Ok(zipkin) => {
                self.register("zipkin", zipkin);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Zipkin integration");
            }
        }
        self
    }

    /// Add Neo4j integration
    pub fn with_neo4j(&mut self, host: &str, port: u16, username: &str, password: &str) -> &mut Self {
        match db::neo4j::Neo4jIntegration::new(host, port, username, password) {
            Ok(neo4j) => {
                self.register("neo4j", neo4j);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Neo4j integration");
            }
        }
        self
    }

    /// Add InfluxDB integration
    pub fn with_influxdb(&mut self, base_url: &str, org: &str, token: &str) -> &mut Self {
        match db::influxdb::InfluxDbIntegration::new(base_url, org, token) {
            Ok(influxdb) => {
                self.register("influxdb", influxdb);
            }
            Err(e) => {
                error!(error = %e, "Failed to create InfluxDB integration");
            }
        }
        self
    }

    /// Add TimescaleDB integration
    pub fn with_timescaledb(&mut self, host: &str, port: u16, database: &str) -> &mut Self {
        match db::timescaledb::TimescaleDbIntegration::new(host, port, database) {
            Ok(timescaledb) => {
                self.register("timescaledb", timescaledb);
            }
            Err(e) => {
                error!(error = %e, "Failed to create TimescaleDB integration");
            }
        }
        self
    }

    /// Add CockroachDB integration
    pub fn with_cockroachdb(&mut self, host: &str, port: u16) -> &mut Self {
        match db::cockroachdb::CockroachDbIntegration::new(host, port) {
            Ok(cockroachdb) => {
                self.register("cockroachdb", cockroachdb);
            }
            Err(e) => {
                error!(error = %e, "Failed to create CockroachDB integration");
            }
        }
        self
    }

    /// Add Oracle Cloud integration
    pub fn with_oracle_cloud(&mut self, region: &str, tenancy_ocid: &str) -> &mut Self {
        match cloud::oracle::OracleCloudIntegration::new(region, tenancy_ocid) {
            Ok(oracle) => {
                self.register(format!("oracle_{}", region).as_str(), oracle);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Oracle Cloud integration");
            }
        }
        self
    }

    /// Add Alibaba Cloud integration
    pub fn with_alibaba_cloud(&mut self, region: &str, access_key_id: &str, access_key_secret: &str) -> &mut Self {
        match cloud::alibaba::AlibabaCloudIntegration::new(region, access_key_id, access_key_secret) {
            Ok(alibaba) => {
                self.register(format!("alibaba_{}", region).as_str(), alibaba);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Alibaba Cloud integration");
            }
        }
        self
    }

    /// Add Datadog integration
    pub fn with_datadog(&mut self, api_key: &str, app_key: &str) -> &mut Self {
        match observability::datadog::DatadogIntegration::new(api_key, app_key) {
            Ok(datadog) => {
                self.register("datadog", datadog);
            }
            Err(e) => {
                error!(error = %e, "Failed to create Datadog integration");
            }
        }
        self
    }

    /// Add New Relic integration
    pub fn with_newrelic(&mut self, api_key: &str, account_id: &str) -> &mut Self {
        match observability::newrelic::NewRelicIntegration::new(api_key, account_id) {
            Ok(newrelic) => {
                self.register("newrelic", newrelic);
            }
            Err(e) => {
                error!(error = %e, "Failed to create New Relic integration");
            }
        }
        self
    }

    /// Add Vault integration
    pub fn with_vault(&mut self, address: &str, token: Option<&str>) -> &mut Self {
        let mut vault = match security::vault::VaultIntegration::new(address) {
            Ok(v) => v,
            Err(e) => {
                error!(error = %e, "Failed to create Vault integration");
                return self;
            }
        };
        if let Some(t) = token {
            vault = vault.with_token(t);
        }
        self.register("vault", vault);
        self
    }

    /// Add LDAP integration
    pub fn with_ldap(&mut self, host: &str, port: u16) -> &mut Self {
        match security::ldap::LdapIntegration::new(host, port) {
            Ok(ldap) => {
                self.register(format!("ldap_{}", host).as_str(), ldap);
            }
            Err(e) => {
                error!(error = %e, "Failed to create LDAP integration");
            }
        }
        self
    }

    /// Collect metrics from all integrations
    pub async fn collect_metrics(&self) -> Vec<framework::Metric> {
        let mut all_metrics = Vec::new();

        for (name, integration) in &self.integrations {
            debug!(integration = %name, "Collecting metrics");
            
            match tokio::time::timeout(
                self.config.timeout,
                integration.collect_metrics()
            ).await {
                Ok(Ok(metrics)) => {
                    debug!(
                        integration = %name,
                        metric_count = metrics.len(),
                        "Metrics collected"
                    );
                    all_metrics.extend(metrics);
                }
                Ok(Err(e)) => {
                    error!(
                        integration = %name,
                        error = %e,
                        "Failed to collect metrics"
                    );
                }
                Err(_) => {
                    error!(
                        integration = %name,
                        timeout_secs = self.config.timeout.as_secs(),
                        "Metric collection timed out"
                    );
                }
            }
        }

        all_metrics
    }

    /// Collect events from all integrations
    pub async fn collect_events(&self) -> Vec<framework::Event> {
        let mut all_events = Vec::new();

        for (name, integration) in &self.integrations {
            match tokio::time::timeout(
                self.config.timeout,
                integration.collect_events()
            ).await {
                Ok(Ok(events)) => {
                    all_events.extend(events);
                }
                Ok(Err(e)) => {
                    error!(
                        integration = %name,
                        error = %e,
                        "Failed to collect events"
                    );
                }
                Err(_) => {
                    error!(
                        integration = %name,
                        "Event collection timed out"
                    );
                }
            }
        }

        all_events
    }

    /// Get health status of all integrations
    pub async fn health_check(&self) -> HashMap<String, IntegrationStatus> {
        let mut statuses = HashMap::new();

        for (name, integration) in &self.integrations {
            let status = match integration.health_check().await {
                Ok(()) => IntegrationStatus::Healthy,
                Err(e) => IntegrationStatus::Unhealthy(e.to_string()),
            };
            statuses.insert(name.clone(), status);
        }

        statuses
    }

    /// Start continuous collection
    pub async fn run(self: std::sync::Arc<Self>) {
        let mut ticker = interval(self.config.interval);

        loop {
            ticker.tick().await;

            let metrics = self.collect_metrics().await;
            let events = self.collect_events().await;

            info!(
                metrics_collected = metrics.len(),
                events_collected = events.len(),
                "Integration collection cycle complete"
            );

            // TODO: Send to storage/analytics
        }
    }

    /// Get registered integration names
    pub fn integration_names(&self) -> Vec<&String> {
        self.integrations.keys().collect()
    }

    /// Check if integration is registered
    pub fn has_integration(&self, name: &str) -> bool {
        self.integrations.contains_key(name)
    }
}

impl Default for IntegrationManager {
    fn default() -> Self {
        Self::new()
    }
}

/// Status of an integration
#[derive(Debug, Clone)]
pub enum IntegrationStatus {
    Healthy,
    Unhealthy(String),
    Unknown,
}

impl std::fmt::Display for IntegrationStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            IntegrationStatus::Healthy => write!(f, "healthy"),
            IntegrationStatus::Unhealthy(reason) => write!(f, "unhealthy: {}", reason),
            IntegrationStatus::Unknown => write!(f, "unknown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_integration_manager_creation() {
        let manager = IntegrationManager::new();
        assert!(manager.integration_names().is_empty());
    }

    #[tokio::test]
    async fn test_health_check_empty() {
        let manager = IntegrationManager::new();
        let health = manager.health_check().await;
        assert!(health.is_empty());
    }
}
