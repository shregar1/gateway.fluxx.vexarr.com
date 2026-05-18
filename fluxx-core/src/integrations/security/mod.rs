//! Security Integrations
//!
//! Monitor HashiCorp Vault, LDAP, and other security systems.

pub mod ldap;
pub mod vault;

pub use ldap::LdapIntegration;
pub use vault::VaultIntegration;
