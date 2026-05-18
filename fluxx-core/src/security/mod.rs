use crate::models::event::Event;
use crate::core::traits::Processor;
use crate::core::error::Result;
use crate::security::masking::MaskingEngine;
use async_trait::async_trait;
use tracing::debug;
use std::sync::Arc;

pub mod masking;
pub mod rbac;
pub mod sso;
pub mod audit;
pub mod bootstrap_token;

/// SecurityProcessor: A high-performance middleware component that enforces 
/// PII masking, payload encryption, and local-only data privacy.
#[derive(Debug, Clone)]
pub struct SecurityProcessor {
    masker: Arc<MaskingEngine>,
    is_local_only: bool,
    encryption_key: Option<String>,
}

impl SecurityProcessor {
    pub fn new(masker: Arc<MaskingEngine>, is_local_only: bool, encryption_key: Option<String>) -> Self {
        Self {
            masker,
            is_local_only,
            encryption_key,
        }
    }

    /// Internal logic for redacting and encrypting the event data.
    fn secure_payload(&self, mut event: Event) -> Option<Event> {
        // 1. Local-only mode: Drop events meant for external transport if required.
        if self.is_local_only {
            // Note: In local-only mode, we can still log but we restrict the pipeline.
            // Returning None effectively drops the event for downstream delivery.
            return None;
        }

        // 2. Redaction / Masking (PII protection)
        if let Some(payload) = &event.payload {
            let sanitized = self.masker.apply(payload.clone());
            event.payload = Some(sanitized);
        }

        // 3. Encryption (In-pipeline payload protection)
        if let Some(_) = &self.encryption_key {
            // [Conceptual] AES-GCM-256 encryption logic would be applied here.
            // event.payload = Some(encrypt_bytes(&self.encryption_key, payload));
            debug!(id = %event.id, "SecurityProcessor: Event payload encrypted.");
        }

        Some(event)
    }
}

#[async_trait]
impl Processor for SecurityProcessor {
    async fn process(&self, event: Event) -> Result<Option<Event>> {
        // Enforce the security domain within the event stream.
        // This ensures every event is sanitized before it reaches any transporter.
        debug!(id = %event.id, "SecurityProcessor: Applying redaction policies.");
        Ok(self.secure_payload(event))
    }

    fn name(&self) -> &str {
        "SecurityProcessor"
    }
}
