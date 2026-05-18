//! Database Integrations

pub mod cassandra;
pub mod clickhouse;
pub mod cockroachdb;
pub mod elasticsearch;
pub mod influxdb;
pub mod mysql;
pub mod neo4j;
pub mod postgres;
pub mod redis;
pub mod mongodb;
pub mod timescaledb;

pub use cassandra::CassandraIntegration;
pub use clickhouse::ClickHouseIntegration;
pub use cockroachdb::CockroachDbIntegration;
pub use elasticsearch::ElasticsearchIntegration;
pub use influxdb::InfluxDbIntegration;
pub use mysql::MySqlIntegration;
pub use neo4j::Neo4jIntegration;
pub use postgres::PostgresIntegration;
pub use redis::RedisIntegration;
pub use mongodb::MongoDbIntegration;
pub use timescaledb::TimescaleDbIntegration;

use super::framework::*;
use async_trait::async_trait;

/// Database-specific integration trait
#[async_trait]
pub trait DatabaseIntegration: Integration {
    /// Get connection pool size
    async fn get_connection_pool_size(&self) -> Result<u32, IntegrationError>;
    
    /// Get slow query count
    async fn get_slow_query_count(&self, threshold_ms: u64) -> Result<u64, IntegrationError>;
}

/// Database connector factory
pub struct DatabaseConnector;

impl DatabaseConnector {
    /// Create a database integration based on connection string
    pub fn from_connection_string(db_type: &str, connection_string: &str) -> Result<Box<dyn Integration>, IntegrationError> {
        match db_type {
            "postgresql" | "postgres" => {
                Ok(Box::new(PostgresIntegration::new(connection_string)?))
            }
            "mysql" => {
                Ok(Box::new(MySqlIntegration::new(connection_string)?))
            }
            "mongodb" => {
                Ok(Box::new(MongoDbIntegration::new(connection_string)?))
            }
            _ => Err(IntegrationError::Other(format!(
                "Unknown database type: {}",
                db_type
            ))),
        }
    }

    /// Create Redis integration
    pub fn redis(host: &str, port: u16) -> Result<Box<dyn Integration>, IntegrationError> {
        Ok(Box::new(RedisIntegration::new(host, port)?))
    }
}

/// Connection pool status
#[derive(Debug, Clone)]
pub struct PoolStatus {
    pub active_connections: u32,
    pub idle_connections: u32,
    pub max_connections: u32,
    pub waiting_count: u32,
}

impl PoolStatus {
    /// Get pool utilization percentage
    pub fn utilization_pct(&self) -> f64 {
        if self.max_connections == 0 {
            return 0.0;
        }
        (self.active_connections as f64 / self.max_connections as f64) * 100.0
    }

    /// Check if pool is saturated
    pub fn is_saturated(&self, threshold: f64) -> bool {
        self.utilization_pct() >= threshold
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_pool_status() {
        let status = PoolStatus {
            active_connections: 8,
            idle_connections: 2,
            max_connections: 10,
            waiting_count: 0,
        };

        assert_eq!(status.utilization_pct(), 80.0);
        assert!(status.is_saturated(75.0));
        assert!(!status.is_saturated(90.0));
    }
}
