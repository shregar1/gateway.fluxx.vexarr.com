//! AWS SQS Integration
//!
//! Monitor Amazon Simple Queue Service queues and message metrics.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// AWS SQS integration
pub struct SqsIntegration {
    config: IntegrationConfig,
    region: String,
    profile: Option<String>,
}

impl SqsIntegration {
    /// Create new SQS integration
    pub fn new(region: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("queue_type".to_string(), "sqs".to_string());
        tags.insert("cloud_provider".to_string(), "aws".to_string());

        let config = IntegrationConfig {
            name: format!("sqs_{}", region),
            interval_secs: 60,
            tags,
            timeout_secs: 30,
        };

        Ok(Self {
            config,
            region: region.to_string(),
            profile: None,
        })
    }

    /// Create with AWS profile
    pub fn with_profile(mut self, profile: &str) -> Self {
        self.profile = Some(profile.to_string());
        self
    }

    /// Check if AWS CLI is available and credentials are configured
    async fn check_aws_access(&self) -> bool {
        let mut cmd = tokio::process::Command::new("aws");
        cmd.args(["sts", "get-caller-identity", "--region", &self.region]);
        
        if let Some(ref profile) = self.profile {
            cmd.args(["--profile", profile]);
        }

        match cmd.output().await {
            Ok(output) => output.status.success(),
            Err(_) => false,
        }
    }

    /// List all SQS queues
    async fn list_queues(&self) -> Result<Vec<String>, IntegrationError> {
        let mut cmd = tokio::process::Command::new("aws");
        cmd.args([
            "sqs", "list-queues",
            "--region", &self.region,
            "--query", "QueueUrls",
            "--output", "json",
        ]);
        
        if let Some(ref profile) = self.profile {
            cmd.args(["--profile", profile]);
        }

        let output = cmd.output().await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        if !output.status.success() {
            return Ok(vec![]);
        }

        let queues: Vec<String> = serde_json::from_slice(&output.stdout)
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        Ok(queues)
    }

    /// Get queue attributes
    async fn get_queue_attributes(&self, queue_url: &str) -> Result<HashMap<String, String>, IntegrationError> {
        let mut cmd = tokio::process::Command::new("aws");
        cmd.args([
            "sqs", "get-queue-attributes",
            "--region", &self.region,
            "--queue-url", queue_url,
            "--attribute-names", "All",
            "--query", "Attributes",
            "--output", "json",
        ]);
        
        if let Some(ref profile) = self.profile {
            cmd.args(["--profile", profile]);
        }

        let output = cmd.output().await
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        if !output.status.success() {
            return Ok(HashMap::new());
        }

        let attrs: HashMap<String, String> = serde_json::from_slice(&output.stdout)
            .map_err(|e| IntegrationError::Other(e.to_string()))?;

        Ok(attrs)
    }

    /// Collect SQS metrics
    async fn collect_sqs_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let queues = self.list_queues().await?;
        
        metrics.push(Metric::gauge(
            "aws.sqs.queues.total",
            queues.len() as f64,
            self.name(),
        ));

        let mut total_messages: i64 = 0;
        let mut total_visible: i64 = 0;
        let mut total_delayed: i64 = 0;
        let mut total_not_visible: i64 = 0;

        for queue_url in queues {
            let attrs = self.get_queue_attributes(&queue_url).await?;
            
            // Extract queue name from URL
            let queue_name = queue_url.split('/').last().unwrap_or("unknown");

            // ApproximateNumberOfMessages
            if let Some(val) = attrs.get("ApproximateNumberOfMessages") {
                if let Ok(count) = val.parse::<i64>() {
                    total_visible += count;
                    metrics.push(Metric::gauge(
                        "aws.sqs.queue.messages_visible",
                        count as f64,
                        self.name(),
                    ).with_tag("queue", queue_name));
                }
            }

            // ApproximateNumberOfMessagesDelayed
            if let Some(val) = attrs.get("ApproximateNumberOfMessagesDelayed") {
                if let Ok(count) = val.parse::<i64>() {
                    total_delayed += count;
                    metrics.push(Metric::gauge(
                        "aws.sqs.queue.messages_delayed",
                        count as f64,
                        self.name(),
                    ).with_tag("queue", queue_name));
                }
            }

            // ApproximateNumberOfMessagesNotVisible
            if let Some(val) = attrs.get("ApproximateNumberOfMessagesNotVisible") {
                if let Ok(count) = val.parse::<i64>() {
                    total_not_visible += count;
                    metrics.push(Metric::gauge(
                        "aws.sqs.queue.messages_not_visible",
                        count as f64,
                        self.name(),
                    ).with_tag("queue", queue_name));
                }
            }

            // VisibilityTimeout
            if let Some(val) = attrs.get("VisibilityTimeout") {
                if let Ok(timeout) = val.parse::<f64>() {
                    metrics.push(Metric::gauge(
                        "aws.sqs.queue.visibility_timeout_seconds",
                        timeout,
                        self.name(),
                    ).with_tag("queue", queue_name));
                }
            }

            // MaximumMessageSize
            if let Some(val) = attrs.get("MaximumMessageSize") {
                if let Ok(size) = val.parse::<f64>() {
                    metrics.push(Metric::gauge(
                        "aws.sqs.queue.max_message_size_bytes",
                        size,
                        self.name(),
                    ).with_tag("queue", queue_name));
                }
            }

            // MessageRetentionPeriod
            if let Some(val) = attrs.get("MessageRetentionPeriod") {
                if let Ok(period) = val.parse::<f64>() {
                    metrics.push(Metric::gauge(
                        "aws.sqs.queue.retention_period_seconds",
                        period,
                        self.name(),
                    ).with_tag("queue", queue_name));
                }
            }

            // Calculate total messages for this queue
            let queue_total = attrs
                .get("ApproximateNumberOfMessages")
                .and_then(|v| v.parse::<i64>().ok())
                .unwrap_or(0)
                + attrs
                    .get("ApproximateNumberOfMessagesDelayed")
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0)
                + attrs
                    .get("ApproximateNumberOfMessagesNotVisible")
                    .and_then(|v| v.parse::<i64>().ok())
                    .unwrap_or(0);
            
            total_messages += queue_total;

            metrics.push(Metric::gauge(
                "aws.sqs.queue.messages_total",
                queue_total as f64,
                self.name(),
            ).with_tag("queue", queue_name));
        }

        // Aggregate metrics across all queues
        metrics.push(Metric::gauge(
            "aws.sqs.messages.visible",
            total_visible as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "aws.sqs.messages.delayed",
            total_delayed as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "aws.sqs.messages.not_visible",
            total_not_visible as f64,
            self.name(),
        ));
        metrics.push(Metric::gauge(
            "aws.sqs.messages.total",
            total_messages as f64,
            self.name(),
        ));

        Ok(metrics)
    }

    /// Collect SQS events
    async fn collect_sqs_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check AWS access
        if !self.check_aws_access().await {
            events.push(
                Event::new(
                    "AWS SQS Access Failed",
                    format!("Cannot access AWS SQS in region {}", self.region),
                    EventSeverity::Critical,
                    self.name(),
                    "sqs_access_failed",
                ),
            );
            return Ok(events);
        }

        // Check for queues with high message count
        let queues = self.list_queues().await?;
        
        for queue_url in queues {
            let attrs = self.get_queue_attributes(&queue_url).await?;
            let queue_name = queue_url.split('/').last().unwrap_or("unknown");

            // Check visible messages
            if let Some(val) = attrs.get("ApproximateNumberOfMessages") {
                if let Ok(count) = val.parse::<f64>() {
                    if count > 10000.0 {
                        events.push(
                            Event::new(
                                "High SQS Queue Depth",
                                format!("Queue {} has {} visible messages", queue_name, count),
                                EventSeverity::Warning,
                                self.name(),
                                "high_queue_depth",
                            )
                            .with_metadata("queue", queue_name)
                            .with_metadata("count", &count.to_string())
                            .with_metadata("region", &self.region),
                        );
                    }
                }
            }

            // Check age of oldest message
            if let Some(val) = attrs.get("ApproximateAgeOfOldestMessage") {
                if let Ok(age) = val.parse::<f64>() {
                    if age > 3600.0 {
                        // Older than 1 hour
                        events.push(
                            Event::new(
                                "Old SQS Messages",
                                format!(
                                    "Queue {} has messages older than {} seconds",
                                    queue_name, age
                                ),
                                EventSeverity::Warning,
                                self.name(),
                                "old_messages",
                            )
                            .with_metadata("queue", queue_name)
                            .with_metadata("age_seconds", &age.to_string()),
                        );
                    }
                }
            }

            // Check for dead letter queue configuration
            if let Some(redrive_policy) = attrs.get("RedrivePolicy") {
                if redrive_policy.is_empty() {
                    events.push(
                        Event::new(
                            "SQS No Dead Letter Queue",
                            format!("Queue {} has no dead letter queue configured", queue_name),
                            EventSeverity::Info,
                            self.name(),
                            "no_dlq",
                        )
                        .with_metadata("queue", queue_name),
                    );
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for SqsIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_sqs_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_sqs_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_aws_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "AWS credentials not configured".to_string()
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
    fn test_sqs_integration_creation() {
        let sqs = SqsIntegration::new("us-east-1");
        assert!(sqs.is_ok());
        assert_eq!(sqs.unwrap().name(), "sqs_us-east-1");
    }
}
