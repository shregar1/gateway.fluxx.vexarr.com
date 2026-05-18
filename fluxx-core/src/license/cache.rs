//! 7-day on-disk license cache.
//!
//! Stored at `~/.fluxx/license.cache.json` (or a path override for tests).
//! The cache file is **only** consulted when the network is unreachable —
//! the happy path is always to call `/v1/license/verify` on startup.
//!
//! Why a cache: agents on customer infrastructure sometimes lose outbound
//! connectivity for short windows (DNS failures, captive portals, egress
//! firewall blips). Without a cache the agent would refuse to start every
//! single time. With a cache we tolerate up to 7 days of offline.
//!
//! The cache is keyed by the sha256 fingerprint of the API key. Multiple
//! keys can share the same `.fluxx/` directory without colliding.

use crate::license::LicenseInfo;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::path::PathBuf;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// Default cache TTL: 7 days.
pub const DEFAULT_TTL: Duration = Duration::from_secs(7 * 24 * 60 * 60);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct CacheEntry {
    /// Unix seconds when this entry was written.
    written_at: u64,
    /// sha256 of the api key, hex.
    api_key_hash: String,
    /// The verified license info from the server.
    license: LicenseInfo,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
struct CacheFile {
    entries: Vec<CacheEntry>,
}

pub struct Cache {
    path: PathBuf,
    ttl: Duration,
}

impl Cache {
    /// Cache at `~/.fluxx/license.cache.json`. Returns `None` if no home dir
    /// can be resolved (e.g. headless CI).
    pub fn default_location() -> Option<Self> {
        let mut p = dirs::home_dir()?;
        p.push(".fluxx");
        p.push("license.cache.json");
        Some(Self { path: p, ttl: DEFAULT_TTL })
    }

    /// Test/CI helper — pin the path and (optionally) the TTL.
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into(), ttl: DEFAULT_TTL }
    }

    pub fn with_ttl(mut self, ttl: Duration) -> Self {
        self.ttl = ttl;
        self
    }

    pub fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Read a cached license for `api_key`. Returns `None` if missing, expired,
    /// or the file can't be parsed.
    pub fn get(&self, api_key: &str) -> Option<LicenseInfo> {
        let bytes = std::fs::read(&self.path).ok()?;
        let file: CacheFile = serde_json::from_slice(&bytes).ok()?;
        let hash = hash_key(api_key);
        let entry = file.entries.iter().find(|e| e.api_key_hash == hash)?;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();
        if now < entry.written_at {
            // Clock skew — pretend the entry is fresh; system clock moving
            // backwards isn't our problem to solve.
            return Some(entry.license.clone());
        }
        if now.saturating_sub(entry.written_at) >= self.ttl.as_secs() {
            return None;
        }
        Some(entry.license.clone())
    }

    /// Insert or replace the entry for `api_key`. Best-effort: if the file
    /// can't be written, the error is swallowed (we still have the
    /// in-memory license).
    pub fn put(&self, api_key: &str, license: &LicenseInfo) -> std::io::Result<()> {
        if let Some(parent) = self.path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let mut file: CacheFile = std::fs::read(&self.path)
            .ok()
            .and_then(|b| serde_json::from_slice(&b).ok())
            .unwrap_or_default();
        let hash = hash_key(api_key);
        file.entries.retain(|e| e.api_key_hash != hash);
        let written_at = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        file.entries.push(CacheEntry {
            written_at,
            api_key_hash: hash,
            license: license.clone(),
        });
        // Limit to 16 entries — well above the typical "1 API key per host"
        // pattern, but bounded so a misuse can't blow up the file.
        while file.entries.len() > 16 {
            file.entries.remove(0);
        }
        let json = serde_json::to_vec_pretty(&file)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        atomic_write(&self.path, &json)
    }

    /// Drop the entry (e.g. on revocation).
    pub fn drop(&self, api_key: &str) -> std::io::Result<()> {
        let bytes = match std::fs::read(&self.path) {
            Ok(b) => b,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
            Err(e) => return Err(e),
        };
        let mut file: CacheFile = serde_json::from_slice(&bytes)
            .unwrap_or_default();
        let hash = hash_key(api_key);
        let before = file.entries.len();
        file.entries.retain(|e| e.api_key_hash != hash);
        if file.entries.len() == before {
            return Ok(());
        }
        let json = serde_json::to_vec_pretty(&file)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
        atomic_write(&self.path, &json)
    }
}

fn hash_key(api_key: &str) -> String {
    let mut h = Sha256::new();
    h.update(api_key.as_bytes());
    hex::encode(h.finalize())
}

fn atomic_write(path: &std::path::Path, bytes: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("cache.tmp");
    std::fs::write(&tmp, bytes)?;
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn fake_license() -> LicenseInfo {
        LicenseInfo {
            valid: true,
            tier: Some("pro".into()),
            limits: None,
            expires_at: Some("2099-01-01T00:00:00Z".into()),
        }
    }

    fn tempfile() -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "fluxx-cache-test-{}.json",
            uuid::Uuid::new_v4()
        ));
        dir
    }

    #[test]
    fn round_trips_one_entry() {
        let path = tempfile();
        let cache = Cache::at(&path);
        let lic = fake_license();
        cache.put("flx_live_aaa", &lic).unwrap();
        let got = cache.get("flx_live_aaa").unwrap();
        assert_eq!(got.tier, lic.tier);
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn returns_none_for_unknown_key() {
        let path = tempfile();
        let cache = Cache::at(&path);
        cache.put("flx_live_aaa", &fake_license()).unwrap();
        assert!(cache.get("flx_live_bbb").is_none());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn entries_expire_after_ttl() {
        // The on-disk timestamp is second-resolution to keep the format
        // simple. TTL=1 second + a 2-second sleep + the `>=` comparator
        // gives unambiguous boundary behavior.
        let path = tempfile();
        let cache = Cache::at(&path).with_ttl(Duration::from_secs(1));
        cache.put("flx_live_aaa", &fake_license()).unwrap();
        std::thread::sleep(Duration::from_secs(2));
        assert!(cache.get("flx_live_aaa").is_none());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn entries_returned_within_ttl_window() {
        let path = tempfile();
        // 60-second TTL — should never expire during the test.
        let cache = Cache::at(&path).with_ttl(Duration::from_secs(60));
        cache.put("flx_live_aaa", &fake_license()).unwrap();
        assert!(cache.get("flx_live_aaa").is_some());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn replace_overwrites_existing_entry() {
        let path = tempfile();
        let cache = Cache::at(&path);
        cache.put("flx_live_aaa", &fake_license()).unwrap();
        let mut updated = fake_license();
        updated.tier = Some("enterprise".into());
        cache.put("flx_live_aaa", &updated).unwrap();
        assert_eq!(cache.get("flx_live_aaa").unwrap().tier, Some("enterprise".into()));
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn drop_removes_only_the_target_entry() {
        let path = tempfile();
        let cache = Cache::at(&path);
        cache.put("flx_live_a", &fake_license()).unwrap();
        cache.put("flx_live_b", &fake_license()).unwrap();
        cache.drop("flx_live_a").unwrap();
        assert!(cache.get("flx_live_a").is_none());
        assert!(cache.get("flx_live_b").is_some());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn corrupt_file_returns_none() {
        let path = tempfile();
        std::fs::write(&path, "not-json").unwrap();
        let cache = Cache::at(&path);
        assert!(cache.get("anything").is_none());
        std::fs::remove_file(&path).ok();
    }

    #[test]
    fn drop_on_missing_file_is_ok() {
        let path = tempfile();
        let cache = Cache::at(&path);
        // No file written; drop is a no-op.
        cache.drop("anything").unwrap();
    }

    #[test]
    fn cap_at_16_entries() {
        let path = tempfile();
        let cache = Cache::at(&path);
        for i in 0..20 {
            cache.put(&format!("flx_live_{i}"), &fake_license()).unwrap();
        }
        // The 20th entry kicked out the first four — confirm:
        let bytes = std::fs::read(&path).unwrap();
        let file: CacheFile = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(file.entries.len(), 16);
        std::fs::remove_file(&path).ok();
    }
}
