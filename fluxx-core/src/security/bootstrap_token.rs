//! Bootstrap admin token for self-hosted deployments.
//!
//! On first start the agent generates a 32-byte cryptographically
//! random token, stores its sha256 hash to disk, and prints the raw
//! token to stdout exactly once. The operator copies it into the
//! dashboard's "Sign in" prompt or pastes it into `Authorization:
//! Bearer …` headers.
//!
//! Subsequent boots load the hash from disk; the raw token never
//! lives on the filesystem. If the token file is deleted, the next
//! boot mints a fresh one.
//!
//! Override paths:
//!   * `FLUXX_ADMIN_TOKEN`   — supply a pre-shared token; the file
//!                             is updated to match its hash and the
//!                             boot banner is silent.
//!   * `FLUXX_DISABLE_AUTH`  — kill switch, dev-only. Auth middleware
//!                             waves every request through.
//!
//! Token file default: `/var/lib/fluxx/admin.token.sha256`. Override
//! with `FLUXX_ADMIN_TOKEN_FILE`. The directory is created (0700) and
//! the file is written (0600) on POSIX.

use rand::RngCore;
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::sync::OnceLock;

const DEFAULT_TOKEN_DIR: &str = "/var/lib/fluxx";
const TOKEN_FILE_BASENAME: &str = "admin.token.sha256";

/// Lazily-initialised global token hash. Set once at boot by
/// `init_admin_token`; read by the auth middleware on every request.
static TOKEN_HASH: OnceLock<Option<[u8; 32]>> = OnceLock::new();

/// Public: outcome the boot path emits for telemetry / banner copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapOutcome {
    /// First boot — token was minted and printed to stdout.
    Minted,
    /// Token loaded from disk; not displayed.
    Loaded,
    /// `FLUXX_ADMIN_TOKEN` env override was active.
    FromEnv,
    /// `FLUXX_DISABLE_AUTH=1` was set — auth bypassed.
    Disabled,
}

/// Initialise the admin token. Call exactly once per process at boot.
/// Subsequent calls are no-ops (returns the cached outcome).
pub fn init_admin_token() -> BootstrapOutcome {
    if std::env::var("FLUXX_DISABLE_AUTH").is_ok() {
        let _ = TOKEN_HASH.set(None);
        return BootstrapOutcome::Disabled;
    }
    if let Ok(token) = std::env::var("FLUXX_ADMIN_TOKEN") {
        if !token.is_empty() {
            let hash = sha256(token.as_bytes());
            let _ = TOKEN_HASH.set(Some(hash));
            // Best-effort persist so a missing file doesn't re-mint
            // next boot. Failure here is non-fatal — the env wins.
            let _ = write_hash_file(&hash);
            return BootstrapOutcome::FromEnv;
        }
    }
    // Try to load existing hash.
    if let Some(hash) = load_hash_file() {
        let _ = TOKEN_HASH.set(Some(hash));
        return BootstrapOutcome::Loaded;
    }
    // Mint a new one.
    let raw = mint_raw_token();
    let hash = sha256(raw.as_bytes());
    let _ = TOKEN_HASH.set(Some(hash));
    let _ = write_hash_file(&hash);
    println!();
    println!("  \x1b[1;33m🔑 Fluxx admin token (copy now — shown only once):\x1b[0m");
    println!("     \x1b[1m{}\x1b[0m", raw);
    println!("  \x1b[2mUse it as the bearer in `Authorization: Bearer …` or paste");
    println!("  it into the dashboard sign-in prompt. Hash is persisted to");
    println!("  {}.\x1b[0m", token_file_path().display());
    println!();
    BootstrapOutcome::Minted
}

/// Constant-time bearer-or-key check used by the auth middleware.
/// Returns `true` when the presented token matches the configured one,
/// or when auth is globally disabled.
pub fn verify_token(presented: &str) -> bool {
    match TOKEN_HASH.get() {
        Some(None) => true, // FLUXX_DISABLE_AUTH was on
        Some(Some(stored)) => {
            let got = sha256(presented.as_bytes());
            constant_time_eq_32(&got, stored)
        }
        None => {
            // Init not yet called — fail closed.
            false
        }
    }
}

pub fn is_disabled() -> bool {
    matches!(TOKEN_HASH.get(), Some(None))
}

fn mint_raw_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    // url-safe base64 without padding — fits in a single shell-friendly token.
    base64_url_no_pad(&bytes)
}

fn base64_url_no_pad(b: &[u8]) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    URL_SAFE_NO_PAD.encode(b)
}

fn sha256(b: &[u8]) -> [u8; 32] {
    let mut h = Sha256::new();
    h.update(b);
    let out = h.finalize();
    let mut arr = [0u8; 32];
    arr.copy_from_slice(&out);
    arr
}

fn constant_time_eq_32(a: &[u8; 32], b: &[u8; 32]) -> bool {
    let mut diff = 0u8;
    for i in 0..32 {
        diff |= a[i] ^ b[i];
    }
    diff == 0
}

fn token_file_path() -> PathBuf {
    if let Ok(p) = std::env::var("FLUXX_ADMIN_TOKEN_FILE") {
        if !p.is_empty() {
            return PathBuf::from(p);
        }
    }
    PathBuf::from(DEFAULT_TOKEN_DIR).join(TOKEN_FILE_BASENAME)
}

fn load_hash_file() -> Option<[u8; 32]> {
    let path = token_file_path();
    let hex = std::fs::read_to_string(&path).ok()?;
    let hex = hex.trim();
    if hex.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for i in 0..32 {
        out[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16).ok()?;
    }
    Some(out)
}

fn write_hash_file(hash: &[u8; 32]) -> std::io::Result<()> {
    let path = token_file_path();
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
        // 0700 on the dir if we can; ignore EPERM elsewhere.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = std::fs::set_permissions(parent, std::fs::Permissions::from_mode(0o700));
        }
    }
    let hex: String = hash.iter().map(|b| format!("{:02x}", b)).collect();
    std::fs::write(&path, hex)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    // Locks so the env-var-mutating tests don't race each other.
    static ENV_LOCK: Mutex<()> = Mutex::new(());

    fn clear_token_state() {
        // OnceLock has no take(); spawn-per-test would be the proper
        // isolation. For these focused tests we reach in via the file
        // path override and confirm functional bits that don't depend
        // on resetting the OnceLock.
        std::env::remove_var("FLUXX_ADMIN_TOKEN");
        std::env::remove_var("FLUXX_DISABLE_AUTH");
    }

    #[test]
    fn sha256_is_stable_and_unique() {
        assert_eq!(sha256(b"abc"), sha256(b"abc"));
        assert_ne!(sha256(b"abc"), sha256(b"abd"));
    }

    #[test]
    fn constant_time_eq_matches_only_equal_arrays() {
        let a = sha256(b"x");
        let b = sha256(b"x");
        assert!(constant_time_eq_32(&a, &b));
        let c = sha256(b"y");
        assert!(!constant_time_eq_32(&a, &c));
    }

    #[test]
    fn mint_raw_token_produces_url_safe_string() {
        let t = mint_raw_token();
        assert_eq!(t.len(), 43, "32 bytes → 43 chars URL-safe-no-pad");
        assert!(t.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn write_then_load_roundtrips() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_token_state();
        let tmp = std::env::temp_dir().join(format!("fluxx-token-test-{}", uuid::Uuid::new_v4()));
        std::env::set_var("FLUXX_ADMIN_TOKEN_FILE", &tmp);
        let h = sha256(b"hello");
        write_hash_file(&h).unwrap();
        let loaded = load_hash_file().unwrap();
        assert_eq!(loaded, h);
        let _ = std::fs::remove_file(&tmp);
        std::env::remove_var("FLUXX_ADMIN_TOKEN_FILE");
    }

    #[test]
    fn load_returns_none_for_garbage_file() {
        let _g = ENV_LOCK.lock().unwrap();
        clear_token_state();
        let tmp = std::env::temp_dir().join(format!("fluxx-token-bad-{}", uuid::Uuid::new_v4()));
        std::fs::write(&tmp, "not-hex").unwrap();
        std::env::set_var("FLUXX_ADMIN_TOKEN_FILE", &tmp);
        assert!(load_hash_file().is_none());
        let _ = std::fs::remove_file(&tmp);
        std::env::remove_var("FLUXX_ADMIN_TOKEN_FILE");
    }
}
