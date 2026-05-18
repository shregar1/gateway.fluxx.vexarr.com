use tokio_util::sync::CancellationToken;
use tracing::info;

#[derive(Debug, Clone)]
pub struct Lifecycle {
    token: CancellationToken,
}

impl Lifecycle {
    pub fn new() -> Self {
        Self {
            token: CancellationToken::new(),
        }
    }

    pub async fn wait_for_shutdown(&self) {
        self.token.cancelled().await;
        info!("Lifecycle: Shutdown finalized.");
    }

    pub fn shutdown(&self) {
        info!("Lifecycle: Initiating shutdown...");
        self.token.cancel();
    }

    pub fn token(&self) -> CancellationToken {
        self.token.clone()
    }
}

impl Default for Lifecycle {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test 130: Lifecycle new
    #[test]
    fn test_lifecycle_new() {
        let lifecycle = Lifecycle::new();
        assert!(!lifecycle.token.is_cancelled());
    }

    // Test 131: Lifecycle default
    #[test]
    fn test_lifecycle_default() {
        let lifecycle: Lifecycle = Default::default();
        assert!(!lifecycle.token.is_cancelled());
    }

    // Test 132: Lifecycle shutdown
    #[test]
    fn test_lifecycle_shutdown() {
        let lifecycle = Lifecycle::new();
        lifecycle.shutdown();
        assert!(lifecycle.token.is_cancelled());
    }

    // Test 133: Lifecycle token clone
    #[test]
    fn test_lifecycle_token() {
        let lifecycle = Lifecycle::new();
        let token = lifecycle.token();
        assert!(!token.is_cancelled());
    }

    // Test 134: Lifecycle shared state
    #[test]
    fn test_lifecycle_shared_state() {
        let lifecycle1 = Lifecycle::new();
        let lifecycle2 = lifecycle1.clone();
        lifecycle1.shutdown();
        assert!(lifecycle2.token.is_cancelled());
    }

    // Test 135: Lifecycle wait_for_shutdown (cancelled path)
    #[tokio::test]
    async fn test_lifecycle_wait_shutdown() {
        let lifecycle = Lifecycle::new();
        lifecycle.shutdown();
        lifecycle.wait_for_shutdown().await;
        assert!(lifecycle.token.is_cancelled());
    }

    // Test 136: Lifecycle clone preserves token
    #[test]
    fn test_lifecycle_clone() {
        let lifecycle = Lifecycle::new();
        let cloned = lifecycle.clone();
        assert!(!cloned.token.is_cancelled());
    }
}
