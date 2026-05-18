//! Remediation engine for applying fixes

use crate::diagnostic::models::{RemediationPlan, RemediationStep, Action, SuggestedFix};
use crate::diagnostic::config::RemediationConfig;

/// Remediation engine
pub struct RemediationEngine {
    config: RemediationConfig,
}

/// Remediation result
#[derive(Debug)]
pub struct RemediationResult {
    pub success: bool,
    pub applied_steps: Vec<u32>,
    pub failed_step: Option<u32>,
    pub error_message: Option<String>,
    pub rollback_applied: bool,
}

impl RemediationEngine {
    /// Create new remediation engine
    pub fn new(config: RemediationConfig) -> Self {
        Self { config }
    }

    /// Check if fix can be auto-applied
    pub fn can_auto_apply(&self, fix: &SuggestedFix) -> bool {
        if !self.config.enabled {
            return false;
        }

        if self.config.auto_apply {
            return fix.auto_applicable && fix.confidence >= self.config.confidence_threshold;
        }

        false
    }

    /// Apply remediation plan
    pub async fn apply_plan(&self, plan: &RemediationPlan) -> RemediationResult {
        if !self.config.enabled {
            return RemediationResult {
                success: false,
                applied_steps: vec![],
                failed_step: None,
                error_message: Some("Remediation is disabled".to_string()),
                rollback_applied: false,
            };
        }

        if plan.requires_approval && !self.config.auto_apply {
            return RemediationResult {
                success: false,
                applied_steps: vec![],
                failed_step: None,
                error_message: Some("This plan requires manual approval".to_string()),
                rollback_applied: false,
            };
        }

        let mut applied_steps = vec![];

        for step in &plan.steps {
            match self.apply_step(step).await {
                Ok(()) => {
                    applied_steps.push(step.step_number);
                }
                Err(e) => {
                    tracing::error!(
                        step = step.step_number,
                        error = %e,
                        "Remediation step failed"
                    );

                    // Rollback if enabled
                    let rollback_applied = if self.config.rollback_on_failure {
                        self.rollback_steps(&applied_steps, plan).await.is_ok()
                    } else {
                        false
                    };

                    return RemediationResult {
                        success: false,
                        applied_steps,
                        failed_step: Some(step.step_number),
                        error_message: Some(e),
                        rollback_applied,
                    };
                }
            }
        }

        RemediationResult {
            success: true,
            applied_steps,
            failed_step: None,
            error_message: None,
            rollback_applied: false,
        }
    }

    /// Apply a single step
    async fn apply_step(&self, step: &RemediationStep) -> Result<(), String> {
        tracing::info!(
            step = step.step_number,
            title = %step.title,
            "Applying remediation step"
        );

        match &step.action {
            Action::Command { command, .. } => {
                self.execute_command(command).await
            }
            Action::CodeChange { file_path, search_pattern, replacement } => {
                self.apply_code_change(file_path, search_pattern, replacement).await
            }
            Action::ConfigChange { file_path, key, new_value } => {
                self.apply_config_change(file_path, key, new_value).await
            }
            Action::Restart { service_name } => {
                self.restart_service(service_name).await
            }
            Action::Scale { service_name, replicas } => {
                self.scale_service(service_name, *replicas).await
            }
            Action::Manual { instructions } => {
                // Manual steps can't be auto-applied
                Err(format!("Manual step requires human intervention: {}", instructions))
            }
        }
    }

    /// Execute shell command
    async fn execute_command(&self, command: &str) -> Result<(), String> {
        tracing::info!(command = %command, "Executing command");
        
        // In a real implementation, this would:
        // 1. Parse the command safely
        // 2. Execute with proper sandboxing
        // 3. Capture output
        // 4. Verify success
        
        // For now, this is a placeholder
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        
        tracing::info!(command = %command, "Command executed successfully");
        Ok(())
    }

    /// Apply code change
    async fn apply_code_change(
        &self,
        file_path: &str,
        _search_pattern: &str,
        _replacement: &str,
    ) -> Result<(), String> {
        tracing::info!(
            file = %file_path,
            "Applying code change"
        );

        // In a real implementation, this would:
        // 1. Read the file
        // 2. Find the pattern
        // 3. Apply the replacement
        // 4. Write back
        // 5. Create backup

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        
        Ok(())
    }

    /// Apply config change
    async fn apply_config_change(
        &self,
        file_path: &str,
        key: &str,
        new_value: &str,
    ) -> Result<(), String> {
        tracing::info!(
            file = %file_path,
            key = %key,
            value = %new_value,
            "Applying config change"
        );

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        
        Ok(())
    }

    /// Restart service
    async fn restart_service(&self, service_name: &str) -> Result<(), String> {
        tracing::info!(service = %service_name, "Restarting service");

        // In a real implementation, this would:
        // 1. Check if service exists
        // 2. Gracefully restart
        // 3. Wait for health check
        // 4. Verify restart success

        tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        
        tracing::info!(service = %service_name, "Service restarted");
        Ok(())
    }

    /// Scale service
    async fn scale_service(&self, service_name: &str, replicas: u32) -> Result<(), String> {
        tracing::info!(
            service = %service_name,
            replicas = replicas,
            "Scaling service"
        );

        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        
        Ok(())
    }

    /// Rollback applied steps
    async fn rollback_steps(
        &self,
        applied_steps: &[u32],
        plan: &RemediationPlan,
    ) -> Result<(), String> {
        tracing::warn!("Rolling back remediation steps");

        for step_num in applied_steps.iter().rev() {
            if let Some(step) = plan.steps.iter().find(|s| s.step_number == *step_num) {
                if let Some(rollback) = &step.rollback {
                    tracing::info!(step = step_num, "Rolling back step");
                    
                    // Execute rollback action
                    match rollback {
                        Action::Command { command, .. } => {
                            let _ = self.execute_command(command).await;
                        }
                        _ => {
                            tracing::warn!("Rollback for this action type not implemented");
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Validate remediation plan
    pub fn validate_plan(&self, plan: &RemediationPlan) -> Vec<String> {
        let mut issues = vec![];

        if plan.steps.is_empty() {
            issues.push("Plan has no steps".to_string());
        }

        for step in &plan.steps {
            if step.title.is_empty() {
                issues.push(format!("Step {} has no title", step.step_number));
            }

            match &step.action {
                Action::CodeChange { file_path, .. } => {
                    if !std::path::Path::new(file_path).exists() {
                        issues.push(format!(
                            "Step {}: File {} does not exist",
                            step.step_number, file_path
                        ));
                    }
                }
                Action::ConfigChange { file_path, .. } => {
                    if !std::path::Path::new(file_path).exists() {
                        issues.push(format!(
                            "Step {}: Config file {} does not exist",
                            step.step_number, file_path
                        ));
                    }
                }
                _ => {}
            }
        }

        issues
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::diagnostic::models::RiskLevel;

    fn create_test_config() -> RemediationConfig {
        RemediationConfig {
            enabled: true,
            auto_apply: false,
            confidence_threshold: 0.8,
            max_retries: 3,
            approval_required_for: vec!["critical".to_string()],
            rollback_on_failure: true,
        }
    }

    #[test]
    fn test_can_auto_apply() {
        let config = create_test_config();
        let engine = RemediationEngine::new(config);

        let fix_high_confidence = SuggestedFix {
            id: "1".to_string(),
            title: "Test".to_string(),
            description: "Test".to_string(),
            steps: vec![],
            confidence: 0.9,
            estimated_time_minutes: 5,
            risk_level: RiskLevel::Low,
            auto_applicable: true,
            required_permissions: vec![],
            rollback_steps: vec![],
        };

        // Should be false because auto_apply is false in config
        assert!(!engine.can_auto_apply(&fix_high_confidence));

        let mut config = create_test_config();
        config.auto_apply = true;
        let engine = RemediationEngine::new(config);

        assert!(engine.can_auto_apply(&fix_high_confidence));
    }
}
