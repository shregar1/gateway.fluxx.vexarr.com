use async_trait::async_trait;
use crate::core::error::{Error, Result};
use crate::core::traits::Capturer;
use crate::models::event::{Event, ProtocolType};
use tracing::info;

#[derive(Debug)]
pub struct HttpCapturer {
    url: String,
    name: String,
    client: reqwest::Client,
}

impl HttpCapturer {
    pub fn new(url: &str, name: &str) -> Self {
        Self {
            url: url.to_string(),
            name: name.to_string(),
            client: reqwest::Client::builder()
                .user_agent("fluxx-agent/0.1.0")
                .build()
                .unwrap_or_default(),
        }
    }
}

#[async_trait]
impl Capturer for HttpCapturer {
    async fn capture(&self) -> Result<Event> {
        info!("Fetching data from {}", self.url);
        // Realistic implement: sleep or use a timer to avoid spamming in a tight loop.
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        let res = self.client.get(&self.url)
            .send()
            .await
            .map_err(|e| Error::Capture(format!("HTTP request failed: {}", e)))?;

        let body = res
            .bytes()
            .await
            .map_err(|e| Error::Capture(format!("Fetch body failed: {}", e)))?;

        let mut event = Event::new(uuid::Uuid::new_v4().to_string(), ProtocolType::Http);
        event.payload = Some(String::from_utf8_lossy(&body).to_string());
        
        // Use structured, deduplicated metadata
        event.metadata.user_agent = Some("fluxx-agent/0.1.0".to_string());
        event.metadata.add("url", &self.url);
        
        Ok(event)
    }

    fn name(&self) -> &str {
        &self.name
    }
}
