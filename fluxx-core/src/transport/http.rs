use async_trait::async_trait;
use crate::core::error::{Error, Result};
use crate::transport::network::NetworkLayer;
use tracing::debug;
use reqwest::Client;

#[derive(Debug)]
pub struct HttpTransport {
    url: String,
    client: Client,
}

impl HttpTransport {
    pub fn new(url: &str) -> Self {
        Self {
            url: url.to_string(),
            client: Client::builder()
                .timeout(std::time::Duration::from_secs(5))
                .default_headers({
                    let mut headers = reqwest::header::HeaderMap::new();
                    headers.insert("Content-Type", "application/json".parse().unwrap());
                    headers
                })
                .build()
                .unwrap_or_default(),
        }
    }
}

#[async_trait]
impl NetworkLayer for HttpTransport {
    async fn write(&self, data: Vec<u8>) -> Result<()> {
        debug!("HTTP Delivery: Posting bytes to {}", self.url);
        
        let mut request = self.client.post(&self.url)
            .header("Content-Type", "application/json")
            .body(data);
        if let Ok(key) = std::env::var("FLUXX_API_KEY") {
            request = request.header("X-API-Key", key);
        }

        let response = request
            .send()
            .await
            .map_err(|e| Error::Transport(format!("HTTP POST failed: {}", e)))?;

        if !response.status().is_success() {
            return Err(Error::Transport(format!(
                "HTTP endpoint error: {}", response.status()
            )));
        }

        Ok(())
    }

    fn protocol_name(&self) -> &str {
        "http"
    }
}
