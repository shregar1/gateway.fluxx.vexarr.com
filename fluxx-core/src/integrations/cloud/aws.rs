//! AWS CloudWatch Integration
//!
//! Monitor AWS resources via CloudWatch metrics.

use super::super::framework::*;
use async_trait::async_trait;
use std::collections::HashMap;

/// AWS CloudWatch integration
pub struct AwsIntegration {
    config: IntegrationConfig,
    region: String,
    profile: Option<String>,
}

impl AwsIntegration {
    /// Create new AWS integration
    pub fn new(region: &str) -> Result<Self, IntegrationError> {
        let mut tags = HashMap::new();
        tags.insert("cloud_provider".to_string(), "aws".to_string());
        tags.insert("region".to_string(), region.to_string());

        let config = IntegrationConfig {
            name: format!("aws_{}", region),
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

    /// Collect EC2 metrics
    async fn collect_ec2_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // List running instances
        let mut cmd = tokio::process::Command::new("aws");
        cmd.args([
            "ec2", "describe-instances",
            "--region", &self.region,
            "--filters", "Name=instance-state-name,Values=running",
            "--query", "Reservations[*].Instances[*].InstanceId",
            "--output", "text",
        ]);
        
        if let Some(ref profile) = self.profile {
            cmd.args(["--profile", profile]);
        }

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let instance_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "aws.ec2.instances.running",
                    instance_count,
                    self.name(),
                ));
            }
        }

        // Get CloudWatch metrics for CPU utilization (average)
        let mut cmd = tokio::process::Command::new("aws");
        cmd.args([
            "cloudwatch", "get-metric-statistics",
            "--region", &self.region,
            "--namespace", "AWS/EC2",
            "--metric-name", "CPUUtilization",
            "--statistics", "Average",
            "--start-time", "$(date -u -d '5 minutes ago' +%Y-%m-%dT%H:%M:%SZ)",
            "--end-time", "$(date -u +%Y-%m-%dT%H:%M:%SZ)",
            "--period", "300",
            "--query", "Datapoints[0].Average",
            "--output", "text",
        ]);
        
        if let Some(ref profile) = self.profile {
            cmd.args(["--profile", profile]);
        }

        // Note: This is a placeholder - actual CloudWatch queries would need proper timestamp handling
        // For production, would use AWS SDK instead of CLI

        Ok(metrics)
    }

    /// Collect RDS metrics
    async fn collect_rds_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("aws");
        cmd.args([
            "rds", "describe-db-instances",
            "--region", &self.region,
            "--query", "DBInstances[*].DBInstanceIdentifier",
            "--output", "text",
        ]);
        
        if let Some(ref profile) = self.profile {
            cmd.args(["--profile", profile]);
        }

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let db_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "aws.rds.databases.total",
                    db_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect ELB metrics
    async fn collect_elb_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        // Application Load Balancers
        let mut cmd = tokio::process::Command::new("aws");
        cmd.args([
            "elbv2", "describe-load-balancers",
            "--region", &self.region,
            "--query", "LoadBalancers[*].LoadBalancerArn",
            "--output", "text",
        ]);
        
        if let Some(ref profile) = self.profile {
            cmd.args(["--profile", profile]);
        }

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let alb_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "aws.elbv2.load_balancers.total",
                    alb_count,
                    self.name(),
                ));
            }
        }

        // Classic Load Balancers
        let mut cmd = tokio::process::Command::new("aws");
        cmd.args([
            "elb", "describe-load-balancers",
            "--region", &self.region,
            "--query", "LoadBalancerDescriptions[*].LoadBalancerName",
            "--output", "text",
        ]);
        
        if let Some(ref profile) = self.profile {
            cmd.args(["--profile", profile]);
        }

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let elb_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "aws.elb.load_balancers.total",
                    elb_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect Lambda metrics
    async fn collect_lambda_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("aws");
        cmd.args([
            "lambda", "list-functions",
            "--region", &self.region,
            "--query", "Functions[*].FunctionName",
            "--output", "text",
        ]);
        
        if let Some(ref profile) = self.profile {
            cmd.args(["--profile", profile]);
        }

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let function_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "aws.lambda.functions.total",
                    function_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect S3 metrics
    async fn collect_s3_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut metrics = vec![];

        let mut cmd = tokio::process::Command::new("aws");
        cmd.args([
            "s3api", "list-buckets",
            "--query", "Buckets[*].Name",
            "--output", "text",
        ]);
        
        if let Some(ref profile) = self.profile {
            cmd.args(["--profile", profile]);
        }

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let bucket_count = stdout.lines().filter(|l| !l.is_empty()).count() as f64;
                
                metrics.push(Metric::gauge(
                    "aws.s3.buckets.total",
                    bucket_count,
                    self.name(),
                ));
            }
        }

        Ok(metrics)
    }

    /// Collect all AWS metrics
    async fn collect_all_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        let mut all_metrics = vec![];

        match self.collect_ec2_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect EC2 metrics: {}", e),
        }

        match self.collect_rds_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect RDS metrics: {}", e),
        }

        match self.collect_elb_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect ELB metrics: {}", e),
        }

        match self.collect_lambda_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect Lambda metrics: {}", e),
        }

        match self.collect_s3_metrics().await {
            Ok(metrics) => all_metrics.extend(metrics),
            Err(e) => tracing::warn!("Failed to collect S3 metrics: {}", e),
        }

        // Add region tag to all metrics
        for metric in &mut all_metrics {
            metric.tags.insert("region".to_string(), self.region.clone());
        }

        Ok(all_metrics)
    }

    /// Collect AWS events
    async fn collect_aws_events(&self) -> Result<Vec<Event>, IntegrationError> {
        let mut events = vec![];

        // Check AWS access
        if !self.check_aws_access().await {
            events.push(
                Event::new(
                    "AWS Access Failed",
                    format!("Cannot access AWS region {}", self.region),
                    EventSeverity::Critical,
                    self.name(),
                    "aws_access_failed",
                ),
            );
            return Ok(events);
        }

        // Check for EC2 instances in stopped state (potential cost issue)
        let mut cmd = tokio::process::Command::new("aws");
        cmd.args([
            "ec2", "describe-instances",
            "--region", &self.region,
            "--filters", "Name=instance-state-name,Values=stopped",
            "--query", "Reservations[*].Instances[*].InstanceId",
            "--output", "text",
        ]);
        
        if let Some(ref profile) = self.profile {
            cmd.args(["--profile", profile]);
        }

        if let Ok(output) = cmd.output().await {
            if output.status.success() {
                let stdout = String::from_utf8_lossy(&output.stdout);
                let stopped_count = stdout.lines().filter(|l| !l.is_empty()).count();
                
                if stopped_count > 0 {
                    events.push(
                        Event::new(
                            "Stopped EC2 Instances",
                            format!("{} EC2 instances are stopped (incurring EBS costs)", stopped_count),
                            EventSeverity::Info,
                            self.name(),
                            "stopped_instances",
                        )
                        .with_metadata("count", &stopped_count.to_string())
                        .with_metadata("region", &self.region),
                    );
                }
            }
        }

        Ok(events)
    }
}

#[async_trait]
impl Integration for AwsIntegration {
    fn name(&self) -> &str {
        &self.config.name
    }

    async fn collect_metrics(&self) -> Result<Vec<Metric>, IntegrationError> {
        self.collect_all_metrics().await
    }

    async fn collect_events(&self) -> Result<Vec<Event>, IntegrationError> {
        self.collect_aws_events().await
    }

    async fn health_check(&self) -> Result<(), IntegrationError> {
        if self.check_aws_access().await {
            Ok(())
        } else {
            Err(IntegrationError::Authentication(
                "AWS credentials not configured or invalid".to_string()
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
    fn test_aws_integration_creation() {
        let aws = AwsIntegration::new("us-east-1");
        assert!(aws.is_ok());
        assert_eq!(aws.unwrap().name(), "aws_us-east-1");
    }
}
