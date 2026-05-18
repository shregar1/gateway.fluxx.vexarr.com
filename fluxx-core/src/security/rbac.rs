//! Role-Based Access Control (RBAC) + Multi-Tenancy
//!
//! API key scoping per project for team isolation.
//! Each API key is bound to a project_id and has a permission set.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use parking_lot::RwLock;

/// Permission levels for API operations.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Permission {
    /// Read events, logs, metrics
    Read,
    /// Write/ingest events, logs, metrics
    Write,
    /// Manage alerts, rules, configuration
    Admin,
    /// Full access (superuser)
    SuperAdmin,
}

/// Role definition with a set of permissions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Role {
    pub name: String,
    pub permissions: HashSet<Permission>,
}

impl Role {
    pub fn viewer() -> Self {
        Self {
            name: "viewer".to_string(),
            permissions: HashSet::from([Permission::Read]),
        }
    }

    pub fn editor() -> Self {
        Self {
            name: "editor".to_string(),
            permissions: HashSet::from([Permission::Read, Permission::Write]),
        }
    }

    pub fn admin() -> Self {
        Self {
            name: "admin".to_string(),
            permissions: HashSet::from([Permission::Read, Permission::Write, Permission::Admin]),
        }
    }

    pub fn super_admin() -> Self {
        Self {
            name: "super_admin".to_string(),
            permissions: HashSet::from([
                Permission::Read, Permission::Write, Permission::Admin, Permission::SuperAdmin,
            ]),
        }
    }

    pub fn has_permission(&self, perm: Permission) -> bool {
        self.permissions.contains(&perm) || self.permissions.contains(&Permission::SuperAdmin)
    }
}

/// An API key with project scoping and role assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ApiKeyEntry {
    pub key_hash: String,
    pub name: String,
    pub project_id: Option<String>,
    pub environment_id: Option<String>,
    pub role: Role,
    pub created_at: u64,
    pub expires_at: Option<u64>,
    pub enabled: bool,
}

impl ApiKeyEntry {
    /// Check if this key grants access to a specific project.
    pub fn can_access_project(&self, project_id: &str) -> bool {
        if !self.enabled {
            return false;
        }
        // SuperAdmin can access all projects
        if self.role.has_permission(Permission::SuperAdmin) {
            return true;
        }
        // If key has no project scope, it can access all
        match &self.project_id {
            None => true,
            Some(pid) => pid == project_id,
        }
    }

    /// Check if this key grants access to a specific environment.
    pub fn can_access_environment(&self, env_id: &str) -> bool {
        match &self.environment_id {
            None => true,
            Some(eid) => eid == env_id,
        }
    }

    /// Check if the key is expired.
    pub fn is_expired(&self) -> bool {
        if let Some(expires) = self.expires_at {
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            now > expires
        } else {
            false
        }
    }
}

/// RBAC Manager: stores API keys and validates access.
#[derive(Debug)]
pub struct RbacManager {
    /// API key hash → entry
    keys: RwLock<HashMap<String, ApiKeyEntry>>,
    /// Whether RBAC enforcement is enabled
    enabled: bool,
}

impl RbacManager {
    pub fn new(enabled: bool) -> Self {
        Self {
            keys: RwLock::new(HashMap::new()),
            enabled,
        }
    }

    /// Register a new API key.
    pub fn register_key(&self, key: String, entry: ApiKeyEntry) {
        let hash = hash_key(&key);
        let mut keys = self.keys.write();
        keys.insert(hash, entry);
    }

    /// Validate an API key and return its entry if valid.
    pub fn validate_key(&self, key: &str) -> Option<ApiKeyEntry> {
        if !self.enabled {
            // When RBAC is disabled, return a super admin entry
            return Some(ApiKeyEntry {
                key_hash: String::new(),
                name: "default".to_string(),
                project_id: None,
                environment_id: None,
                role: Role::super_admin(),
                created_at: 0,
                expires_at: None,
                enabled: true,
            });
        }

        let hash = hash_key(key);
        let keys = self.keys.read();
        keys.get(&hash).and_then(|entry| {
            if !entry.enabled || entry.is_expired() {
                None
            } else {
                Some(entry.clone())
            }
        })
    }

    /// Check if an API key has a specific permission.
    pub fn check_permission(&self, key: &str, permission: Permission) -> bool {
        match self.validate_key(key) {
            Some(entry) => entry.role.has_permission(permission),
            None => !self.enabled, // If RBAC disabled, allow all
        }
    }

    /// Check if an API key can access a specific project.
    pub fn check_project_access(&self, key: &str, project_id: &str) -> bool {
        match self.validate_key(key) {
            Some(entry) => entry.can_access_project(project_id),
            None => !self.enabled,
        }
    }

    /// List all registered keys (without the actual key values).
    pub fn list_keys(&self) -> Vec<ApiKeyEntry> {
        let keys = self.keys.read();
        keys.values().cloned().collect()
    }

    /// Revoke a key by its hash.
    pub fn revoke_key(&self, key_hash: &str) -> bool {
        let mut keys = self.keys.write();
        if let Some(entry) = keys.get_mut(key_hash) {
            entry.enabled = false;
            true
        } else {
            false
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.enabled
    }
}

/// Simple hash function for API keys (use a proper hash in production).
fn hash_key(key: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    key.hash(&mut hasher);
    format!("{:016x}", hasher.finish())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_role_permissions() {
        let viewer = Role::viewer();
        assert!(viewer.has_permission(Permission::Read));
        assert!(!viewer.has_permission(Permission::Write));

        let editor = Role::editor();
        assert!(editor.has_permission(Permission::Read));
        assert!(editor.has_permission(Permission::Write));
        assert!(!editor.has_permission(Permission::Admin));

        let admin = Role::admin();
        assert!(admin.has_permission(Permission::Admin));

        let super_admin = Role::super_admin();
        assert!(super_admin.has_permission(Permission::Admin));
        assert!(super_admin.has_permission(Permission::SuperAdmin));
    }

    #[test]
    fn test_api_key_project_scoping() {
        let entry = ApiKeyEntry {
            key_hash: "test".to_string(),
            name: "scoped key".to_string(),
            project_id: Some("project-a".to_string()),
            environment_id: None,
            role: Role::editor(),
            created_at: 0,
            expires_at: None,
            enabled: true,
        };

        assert!(entry.can_access_project("project-a"));
        assert!(!entry.can_access_project("project-b"));
    }

    #[test]
    fn test_api_key_superadmin_access() {
        let entry = ApiKeyEntry {
            key_hash: "test".to_string(),
            name: "super key".to_string(),
            project_id: Some("project-a".to_string()),
            environment_id: None,
            role: Role::super_admin(),
            created_at: 0,
            expires_at: None,
            enabled: true,
        };

        // SuperAdmin can access any project
        assert!(entry.can_access_project("project-a"));
        assert!(entry.can_access_project("project-b"));
    }

    #[test]
    fn test_rbac_manager_register_and_validate() {
        let manager = RbacManager::new(true);

        let entry = ApiKeyEntry {
            key_hash: String::new(),
            name: "test key".to_string(),
            project_id: None,
            environment_id: None,
            role: Role::editor(),
            created_at: 0,
            expires_at: None,
            enabled: true,
        };

        manager.register_key("my-secret-key".to_string(), entry);
        assert!(manager.validate_key("my-secret-key").is_some());
        assert!(manager.validate_key("wrong-key").is_none());
    }

    #[test]
    fn test_rbac_disabled_allows_all() {
        let manager = RbacManager::new(false);
        assert!(manager.check_permission("any-key", Permission::Admin));
        assert!(manager.check_project_access("any-key", "any-project"));
    }

    #[test]
    fn test_key_revocation() {
        let manager = RbacManager::new(true);
        let entry = ApiKeyEntry {
            key_hash: String::new(),
            name: "revokable".to_string(),
            project_id: None,
            environment_id: None,
            role: Role::editor(),
            created_at: 0,
            expires_at: None,
            enabled: true,
        };

        manager.register_key("key123".to_string(), entry);
        assert!(manager.validate_key("key123").is_some());

        let hash = hash_key("key123");
        manager.revoke_key(&hash);
        assert!(manager.validate_key("key123").is_none());
    }
}
