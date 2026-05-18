//! SSO / SAML / OIDC Authentication
//!
//! Supports external identity providers for enterprise authentication:
//! - SAML 2.0 (Okta, Azure AD, OneLogin)
//! - OIDC (Google, Auth0, Keycloak)
//! - LDAP (Active Directory)

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::SystemTime;
use parking_lot::RwLock;
use tracing::info;

/// SSO configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SsoConfig {
    pub enabled: bool,
    pub provider: SsoProvider,
    /// Session duration in seconds.
    pub session_ttl_secs: u64,
    /// Allow local API key auth alongside SSO.
    pub allow_api_keys: bool,
}

impl Default for SsoConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            provider: SsoProvider::None,
            session_ttl_secs: 28800, // 8 hours
            allow_api_keys: true,
        }
    }
}

/// SSO provider configuration variants.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SsoProvider {
    None,
    Saml {
        idp_metadata_url: String,
        entity_id: String,
        acs_url: String,
        certificate: Option<String>,
        attribute_mapping: AttributeMapping,
    },
    Oidc {
        issuer_url: String,
        client_id: String,
        client_secret: String,
        redirect_uri: String,
        scopes: Vec<String>,
    },
    Ldap {
        server_url: String,
        bind_dn: String,
        bind_password: String,
        base_dn: String,
        user_filter: String,
        group_filter: Option<String>,
    },
}

/// Maps IdP attributes to Fluxx user fields.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AttributeMapping {
    pub email: String,
    pub name: String,
    pub groups: Option<String>,
    pub role: Option<String>,
}

impl Default for AttributeMapping {
    fn default() -> Self {
        Self {
            email: "email".to_string(),
            name: "displayName".to_string(),
            groups: Some("groups".to_string()),
            role: None,
        }
    }
}

/// Authenticated user session.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SsoSession {
    pub session_id: String,
    pub user_id: String,
    pub email: String,
    pub name: String,
    pub groups: Vec<String>,
    pub role: String,
    pub provider: String,
    pub created_at: u64,
    pub expires_at: u64,
    pub metadata: HashMap<String, String>,
}

impl SsoSession {
    pub fn is_expired(&self) -> bool {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        now >= self.expires_at
    }
}

/// SSO session manager.
pub struct SsoManager {
    config: SsoConfig,
    sessions: RwLock<HashMap<String, SsoSession>>,
}

impl SsoManager {
    pub fn new(config: SsoConfig) -> Self {
        Self {
            config,
            sessions: RwLock::new(HashMap::new()),
        }
    }

    /// Create a session after successful authentication.
    pub fn create_session(
        &self,
        user_id: &str,
        email: &str,
        name: &str,
        groups: Vec<String>,
        role: &str,
        provider: &str,
    ) -> SsoSession {
        let now = SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let session = SsoSession {
            session_id: uuid::Uuid::new_v4().to_string(),
            user_id: user_id.to_string(),
            email: email.to_string(),
            name: name.to_string(),
            groups,
            role: role.to_string(),
            provider: provider.to_string(),
            created_at: now,
            expires_at: now + self.config.session_ttl_secs,
            metadata: HashMap::new(),
        };

        let mut sessions = self.sessions.write();
        sessions.insert(session.session_id.clone(), session.clone());
        info!("SSO: Session created for {} ({})", email, provider);
        session
    }

    /// Validate a session token.
    pub fn validate_session(&self, session_id: &str) -> Option<SsoSession> {
        let sessions = self.sessions.read();
        sessions.get(session_id).and_then(|s| {
            if s.is_expired() {
                None
            } else {
                Some(s.clone())
            }
        })
    }

    /// Destroy a session (logout).
    pub fn destroy_session(&self, session_id: &str) -> bool {
        let mut sessions = self.sessions.write();
        sessions.remove(session_id).is_some()
    }

    /// Clean up expired sessions.
    pub fn cleanup_expired(&self) -> usize {
        let mut sessions = self.sessions.write();
        let before = sessions.len();
        sessions.retain(|_, s| !s.is_expired());
        before - sessions.len()
    }

    /// Get all active sessions.
    pub fn active_sessions(&self) -> Vec<SsoSession> {
        let sessions = self.sessions.read();
        sessions.values().filter(|s| !s.is_expired()).cloned().collect()
    }

    /// Generate SAML auth request URL (placeholder for full implementation).
    pub fn saml_auth_url(&self) -> Result<String, String> {
        match &self.config.provider {
            SsoProvider::Saml { idp_metadata_url, acs_url, .. } => {
                // In production: generate AuthnRequest XML, deflate, base64, build redirect URL
                Ok(format!(
                    "{}?SAMLRequest=placeholder&RelayState={}",
                    idp_metadata_url, acs_url
                ))
            }
            _ => Err("SAML not configured".to_string()),
        }
    }

    /// Generate OIDC authorization URL.
    pub fn oidc_auth_url(&self) -> Result<String, String> {
        match &self.config.provider {
            SsoProvider::Oidc { issuer_url, client_id, redirect_uri, scopes, .. } => {
                let state = uuid::Uuid::new_v4().to_string();
                let scope = scopes.join("+");
                Ok(format!(
                    "{}/authorize?client_id={}&redirect_uri={}&response_type=code&scope={}&state={}",
                    issuer_url, client_id, redirect_uri, scope, state
                ))
            }
            _ => Err("OIDC not configured".to_string()),
        }
    }

    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_lifecycle() {
        let config = SsoConfig {
            enabled: true,
            session_ttl_secs: 3600,
            ..Default::default()
        };
        let manager = SsoManager::new(config);

        let session = manager.create_session(
            "user-1", "user@company.com", "Test User",
            vec!["engineering".to_string()], "editor", "saml",
        );

        assert!(manager.validate_session(&session.session_id).is_some());
        assert!(manager.destroy_session(&session.session_id));
        assert!(manager.validate_session(&session.session_id).is_none());
    }

    #[test]
    fn test_expired_session() {
        let config = SsoConfig {
            enabled: true,
            session_ttl_secs: 0, // Expire immediately
            ..Default::default()
        };
        let manager = SsoManager::new(config);
        let session = manager.create_session("u", "e@co.com", "N", vec![], "viewer", "oidc");
        // Session created with expires_at = now + 0, so it's already expired
        assert!(manager.validate_session(&session.session_id).is_none());
    }

    #[test]
    fn test_cleanup() {
        let config = SsoConfig {
            enabled: true,
            session_ttl_secs: 0,
            ..Default::default()
        };
        let manager = SsoManager::new(config);
        manager.create_session("u1", "a@b.com", "A", vec![], "viewer", "ldap");
        manager.create_session("u2", "c@d.com", "B", vec![], "viewer", "ldap");
        let cleaned = manager.cleanup_expired();
        assert_eq!(cleaned, 2);
    }

    #[test]
    fn test_oidc_auth_url() {
        let config = SsoConfig {
            enabled: true,
            provider: SsoProvider::Oidc {
                issuer_url: "https://auth.example.com".to_string(),
                client_id: "fluxx-app".to_string(),
                client_secret: "secret".to_string(),
                redirect_uri: "http://localhost:7007/auth/callback".to_string(),
                scopes: vec!["openid".to_string(), "email".to_string(), "profile".to_string()],
            },
            ..Default::default()
        };
        let manager = SsoManager::new(config);
        let url = manager.oidc_auth_url().unwrap();
        assert!(url.contains("auth.example.com"));
        assert!(url.contains("fluxx-app"));
    }
}
