//! Cloud Security Posture Management (CSPM)
//!
//! Scans cloud infrastructure configurations for security misconfigurations.
//! Supports: AWS, GCP, Azure security checks.
//! Tracks compliance against CIS benchmarks, SOC2, HIPAA, PCI-DSS.

use crate::api::{ApiState, StandardResponse};
use axum::{extract::{State, Query}, response::IntoResponse, Json};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use parking_lot::RwLock;

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FindingSeverity { Critical, High, Medium, Low, Info }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum FindingStatus { Open, Suppressed, Resolved }

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
pub enum ComplianceFramework { Cis, Soc2, Hipaa, PciDss, Gdpr, Iso27001, Nist }

/// A security misconfiguration finding.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityFinding {
    pub id: String,
    pub rule_id: String,
    pub rule_name: String,
    pub description: String,
    pub severity: FindingSeverity,
    pub status: FindingStatus,
    pub cloud_provider: String,
    pub resource_type: String,
    pub resource_id: String,
    pub region: String,
    pub account_id: Option<String>,
    pub frameworks: Vec<ComplianceFramework>,
    pub remediation: String,
    pub detected_at_us: u64,
    #[serde(default)]
    pub tags: HashMap<String, String>,
}

/// Compliance posture summary.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompliancePosture {
    pub framework: ComplianceFramework,
    pub total_rules: u32,
    pub passing: u32,
    pub failing: u32,
    pub score_percent: f64,
}

/// CSPM security rule definition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SecurityRule {
    pub id: String,
    pub name: String,
    pub description: String,
    pub severity: FindingSeverity,
    pub cloud_provider: String,
    pub resource_type: String,
    pub check_type: String,
    pub frameworks: Vec<ComplianceFramework>,
    pub remediation: String,
    pub enabled: bool,
}

#[allow(dead_code)]
fn now_us() -> u64 { std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_micros() as u64 }

/// Built-in security rules.
pub fn default_rules() -> Vec<SecurityRule> {
    vec![
        SecurityRule { id: "aws-s3-public".into(), name: "S3 Bucket Public Access".into(),
            description: "S3 buckets should not allow public access".into(), severity: FindingSeverity::Critical,
            cloud_provider: "aws".into(), resource_type: "s3_bucket".into(), check_type: "public_access".into(),
            frameworks: vec![ComplianceFramework::Cis, ComplianceFramework::Soc2, ComplianceFramework::PciDss],
            remediation: "Enable S3 Block Public Access at the bucket level".into(), enabled: true },
        SecurityRule { id: "aws-sg-open".into(), name: "Security Group Open to World".into(),
            description: "Security groups should not allow unrestricted inbound access".into(), severity: FindingSeverity::High,
            cloud_provider: "aws".into(), resource_type: "security_group".into(), check_type: "open_ingress".into(),
            frameworks: vec![ComplianceFramework::Cis, ComplianceFramework::PciDss],
            remediation: "Restrict security group inbound rules to specific CIDR ranges".into(), enabled: true },
        SecurityRule { id: "aws-rds-public".into(), name: "RDS Instance Publicly Accessible".into(),
            description: "RDS instances should not be publicly accessible".into(), severity: FindingSeverity::Critical,
            cloud_provider: "aws".into(), resource_type: "rds_instance".into(), check_type: "public_access".into(),
            frameworks: vec![ComplianceFramework::Cis, ComplianceFramework::Hipaa],
            remediation: "Set PubliclyAccessible to false for the RDS instance".into(), enabled: true },
        SecurityRule { id: "aws-iam-mfa".into(), name: "IAM User Without MFA".into(),
            description: "IAM users should have MFA enabled".into(), severity: FindingSeverity::High,
            cloud_provider: "aws".into(), resource_type: "iam_user".into(), check_type: "mfa_enabled".into(),
            frameworks: vec![ComplianceFramework::Cis, ComplianceFramework::Soc2, ComplianceFramework::Nist],
            remediation: "Enable MFA for the IAM user".into(), enabled: true },
        SecurityRule { id: "aws-encryption".into(), name: "Unencrypted EBS Volume".into(),
            description: "EBS volumes should be encrypted at rest".into(), severity: FindingSeverity::Medium,
            cloud_provider: "aws".into(), resource_type: "ebs_volume".into(), check_type: "encryption".into(),
            frameworks: vec![ComplianceFramework::Hipaa, ComplianceFramework::PciDss, ComplianceFramework::Gdpr],
            remediation: "Enable encryption for EBS volumes".into(), enabled: true },
        SecurityRule { id: "gcp-fw-open".into(), name: "GCP Firewall Open to Internet".into(),
            description: "Firewall rules should not allow 0.0.0.0/0".into(), severity: FindingSeverity::High,
            cloud_provider: "gcp".into(), resource_type: "firewall_rule".into(), check_type: "open_ingress".into(),
            frameworks: vec![ComplianceFramework::Cis],
            remediation: "Restrict source ranges in firewall rules".into(), enabled: true },
        SecurityRule { id: "azure-nsg-open".into(), name: "Azure NSG Open Inbound Rule".into(),
            description: "NSGs should not have unrestricted inbound rules".into(), severity: FindingSeverity::High,
            cloud_provider: "azure".into(), resource_type: "nsg".into(), check_type: "open_ingress".into(),
            frameworks: vec![ComplianceFramework::Cis, ComplianceFramework::Iso27001],
            remediation: "Restrict NSG inbound rules".into(), enabled: true },
    ]
}

#[derive(Debug, Default)]
pub struct CspmStore {
    findings: RwLock<Vec<SecurityFinding>>,
    rules: RwLock<Vec<SecurityRule>>,
}

impl CspmStore {
    pub fn new() -> Self {
        let store = Self::default();
        *store.rules.write() = default_rules();
        store
    }

    pub fn add_finding(&self, finding: SecurityFinding) { self.findings.write().push(finding); }

    pub fn get_findings(&self, severity: Option<FindingSeverity>, provider: Option<&str>, status: Option<FindingStatus>, limit: usize) -> Vec<SecurityFinding> {
        self.findings.read().iter().rev()
            .filter(|f| severity.map_or(true, |s| f.severity == s))
            .filter(|f| provider.map_or(true, |p| f.cloud_provider == p))
            .filter(|f| status.map_or(true, |s| f.status == s))
            .take(limit).cloned().collect()
    }

    pub fn get_compliance(&self, framework: ComplianceFramework) -> CompliancePosture {
        let rules = self.rules.read();
        let findings = self.findings.read();
        let framework_rules: Vec<&SecurityRule> = rules.iter()
            .filter(|r| r.frameworks.contains(&framework) && r.enabled).collect();
        let total = framework_rules.len() as u32;
        let failing_ids: std::collections::HashSet<&str> = findings.iter()
            .filter(|f| f.status == FindingStatus::Open && f.frameworks.contains(&framework))
            .map(|f| f.rule_id.as_str()).collect();
        let failing = failing_ids.len() as u32;
        let passing = total.saturating_sub(failing);
        CompliancePosture {
            framework, total_rules: total, passing, failing,
            score_percent: if total > 0 { passing as f64 / total as f64 * 100.0 } else { 100.0 },
        }
    }

    pub fn get_rules(&self) -> Vec<SecurityRule> { self.rules.read().clone() }
}

pub async fn report_finding(State(state): State<Arc<ApiState>>, Json(finding): Json<SecurityFinding>) -> impl IntoResponse {
    state.cspm_store.add_finding(finding);
    StandardResponse::ok(serde_json::json!({"recorded": true}), "Finding recorded.")
}

pub async fn list_findings(State(state): State<Arc<ApiState>>, Query(params): Query<HashMap<String, String>>) -> impl IntoResponse {
    let provider = params.get("provider").map(|s| s.as_str());
    let limit: usize = params.get("limit").and_then(|v| v.parse().ok()).unwrap_or(50);
    let findings = state.cspm_store.get_findings(None, provider, None, limit);
    StandardResponse::ok(serde_json::to_value(&findings).unwrap_or_default(), "Security findings.")
}

pub async fn get_compliance(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    let frameworks = vec![ComplianceFramework::Cis, ComplianceFramework::Soc2, ComplianceFramework::Hipaa,
        ComplianceFramework::PciDss, ComplianceFramework::Gdpr, ComplianceFramework::Iso27001, ComplianceFramework::Nist];
    let postures: Vec<CompliancePosture> = frameworks.iter().map(|f| state.cspm_store.get_compliance(*f)).collect();
    StandardResponse::ok(serde_json::to_value(&postures).unwrap_or_default(), "Compliance posture.")
}

pub async fn list_security_rules(State(state): State<Arc<ApiState>>) -> impl IntoResponse {
    StandardResponse::ok(serde_json::to_value(&state.cspm_store.get_rules()).unwrap_or_default(), "Security rules.")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mk_finding(id: &str, rule_id: &str, severity: FindingSeverity, status: FindingStatus, provider: &str, frameworks: Vec<ComplianceFramework>) -> SecurityFinding {
        SecurityFinding {
            id: id.to_string(),
            rule_id: rule_id.to_string(),
            rule_name: "rule".to_string(),
            description: "d".to_string(),
            severity,
            status,
            cloud_provider: provider.to_string(),
            resource_type: "r".to_string(),
            resource_id: "res-1".to_string(),
            region: "us-east-1".to_string(),
            account_id: None,
            frameworks,
            remediation: "fix it".to_string(),
            detected_at_us: 0,
            tags: HashMap::new(),
        }
    }

    #[test]
    fn default_rules_non_empty() {
        let rules = default_rules();
        assert!(!rules.is_empty());
    }

    #[test]
    fn default_rules_contain_aws_s3_public() {
        let rules = default_rules();
        assert!(rules.iter().any(|r| r.id == "aws-s3-public"));
    }

    #[test]
    fn new_store_has_default_rules() {
        let store = CspmStore::new();
        assert!(!store.get_rules().is_empty());
    }

    #[test]
    fn new_store_has_no_findings() {
        let store = CspmStore::new();
        assert!(store.get_findings(None, None, None, 100).is_empty());
    }

    #[test]
    fn add_finding_stores_it() {
        let store = CspmStore::new();
        store.add_finding(mk_finding("f1", "aws-s3-public", FindingSeverity::Critical, FindingStatus::Open, "aws", vec![ComplianceFramework::Cis]));
        assert_eq!(store.get_findings(None, None, None, 100).len(), 1);
    }

    #[test]
    fn get_findings_filters_by_severity() {
        let store = CspmStore::new();
        store.add_finding(mk_finding("f1", "r1", FindingSeverity::Critical, FindingStatus::Open, "aws", vec![]));
        store.add_finding(mk_finding("f2", "r2", FindingSeverity::Low, FindingStatus::Open, "aws", vec![]));
        let result = store.get_findings(Some(FindingSeverity::Critical), None, None, 100);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].severity, FindingSeverity::Critical);
    }

    #[test]
    fn get_findings_filters_by_provider() {
        let store = CspmStore::new();
        store.add_finding(mk_finding("f1", "r1", FindingSeverity::High, FindingStatus::Open, "aws", vec![]));
        store.add_finding(mk_finding("f2", "r2", FindingSeverity::High, FindingStatus::Open, "gcp", vec![]));
        let result = store.get_findings(None, Some("aws"), None, 100);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn get_findings_filters_by_status() {
        let store = CspmStore::new();
        store.add_finding(mk_finding("f1", "r1", FindingSeverity::High, FindingStatus::Open, "aws", vec![]));
        store.add_finding(mk_finding("f2", "r2", FindingSeverity::High, FindingStatus::Resolved, "aws", vec![]));
        let result = store.get_findings(None, None, Some(FindingStatus::Resolved), 100);
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn get_findings_respects_limit() {
        let store = CspmStore::new();
        for i in 0..5 {
            store.add_finding(mk_finding(&format!("f{}", i), "r", FindingSeverity::High, FindingStatus::Open, "aws", vec![]));
        }
        assert_eq!(store.get_findings(None, None, None, 2).len(), 2);
    }

    #[test]
    fn get_compliance_empty_findings_perfect_score() {
        let store = CspmStore::new();
        let p = store.get_compliance(ComplianceFramework::Cis);
        assert_eq!(p.failing, 0);
        assert_eq!(p.score_percent, 100.0);
        assert!(p.total_rules > 0);
    }

    #[test]
    fn get_compliance_failing_reduces_score() {
        let store = CspmStore::new();
        store.add_finding(mk_finding("f1", "aws-s3-public", FindingSeverity::Critical, FindingStatus::Open, "aws", vec![ComplianceFramework::Cis]));
        let p = store.get_compliance(ComplianceFramework::Cis);
        assert!(p.failing >= 1);
        assert!(p.score_percent < 100.0);
    }

    #[test]
    fn get_compliance_suppressed_not_failing() {
        let store = CspmStore::new();
        store.add_finding(mk_finding("f1", "aws-s3-public", FindingSeverity::Critical, FindingStatus::Suppressed, "aws", vec![ComplianceFramework::Cis]));
        let p = store.get_compliance(ComplianceFramework::Cis);
        assert_eq!(p.failing, 0);
    }

    #[test]
    fn get_compliance_framework_scoped() {
        let store = CspmStore::new();
        // Finding only applies to SOC2, not HIPAA
        store.add_finding(mk_finding("f1", "aws-s3-public", FindingSeverity::Critical, FindingStatus::Open, "aws", vec![ComplianceFramework::Soc2]));
        let soc2 = store.get_compliance(ComplianceFramework::Soc2);
        let hipaa = store.get_compliance(ComplianceFramework::Hipaa);
        assert!(soc2.failing >= 1);
        assert_eq!(hipaa.failing, 0);
    }

    #[test]
    fn get_compliance_dedupes_findings_by_rule() {
        let store = CspmStore::new();
        store.add_finding(mk_finding("f1", "aws-s3-public", FindingSeverity::Critical, FindingStatus::Open, "aws", vec![ComplianceFramework::Cis]));
        store.add_finding(mk_finding("f2", "aws-s3-public", FindingSeverity::Critical, FindingStatus::Open, "aws", vec![ComplianceFramework::Cis]));
        let p = store.get_compliance(ComplianceFramework::Cis);
        // Both findings share the same rule_id, so they count as 1 failing rule.
        assert_eq!(p.failing, 1);
    }
}
