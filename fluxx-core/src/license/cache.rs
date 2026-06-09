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
//! ## Implementation
//!
//! The atomic-write / sha256-key-fingerprint / TTL plumbing lives in
//! `utilx::cache::disk_json::DiskJsonCache<T>`. This file is a thin wrapper
//! that fixes `T = LicenseInfo` and pins the fluxx-specific cache path
//! (`~/.fluxx/license.cache.json`) so 100% of call-site behaviour stays
//! identical.

use crate::license::LicenseInfo;
use std::path::PathBuf;
use std::time::Duration;

/// Default cache TTL: 7 days.
pub const DEFAULT_TTL: Duration = utilx::cache::disk_json::DEFAULT_TTL;

/// 7-day on-disk cache for verified license info.
pub struct Cache {
    inner: utilx::cache::disk_json::DiskJsonCache<LicenseInfo>,
}

impl Cache {
    /// Cache at `~/.fluxx/license.cache.json`. Returns `None` if no home dir
    /// can be resolved (e.g. headless CI).
    pub fn default_location() -> Option<Self> {
        utilx::cache::disk_json::DiskJsonCache::default_at_home(".fluxx", "license.cache.json")
            .map(|inner| Self { inner })
    }

    /// Test/CI helper — pin the path and (optionally) the TTL.
    pub fn at(path: impl Into<PathBuf>) -> Self {
        Self {
            inner: utilx::cache::disk_json::DiskJsonCache::at(path),
        }
    }

    /// Override the TTL (builder).
    pub fn with_ttl(self, ttl: Duration) -> Self {
        Self {
            inner: self.inner.with_ttl(ttl),
        }
    }

    /// Return the on-disk path.
    pub fn path(&self) -> &std::path::Path {
        self.inner.path()
    }

    /// Read a cached license for `api_key`. Returns `None` if missing, expired,
    /// or the file can't be parsed.
    pub fn get(&self, api_key: &str) -> Option<LicenseInfo> {
        self.inner.get(api_key)
    }

    /// Insert or replace the entry for `api_key`. Best-effort: if the file
    /// can't be written, the error is surfaced so the caller can decide to
    /// proceed with the in-memory license.
    pub fn put(&self, api_key: &str, license: &LicenseInfo) -> std::io::Result<()> {
        self.inner.put(api_key, license)
    }

    /// Drop the entry (e.g. on revocation). No-op when missing.
    pub fn drop(&self, api_key: &str) -> std::io::Result<()> {
        self.inner.drop(api_key)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fake_license() -> LicenseInfo {
        LicenseInfo {
            valid: true,
            tier: Some("pro".into()),
            limits: None,
            expires_at: Some("2099-01-01T00:00:00Z".into()),
        }
    }

    fn tempfile() -> PathBuf {
        std::env::temp_dir().join(format!(
            "fluxx-cache-test-{}.json",
            uuid::Uuid::new_v4()
        ))
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
        assert_eq!(
            cache.get("flx_live_aaa").unwrap().tier,
            Some("enterprise".into())
        );
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
    fn drop_on_missing_file_is_ok() {
        let path = tempfile();
        let cache = Cache::at(&path);
        cache.drop("anything").unwrap();
    }
}
