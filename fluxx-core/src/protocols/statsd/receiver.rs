use crate::protocols::statsd::parser;
use crate::protocols::statsd::aggregator::MetricAggregator;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tracing::{info, warn, error};

/// StatsD/DogStatsD UDP receiver.
pub struct StatsdReceiver {
    bind_address: String,
    aggregator: Arc<MetricAggregator>,
}

impl StatsdReceiver {
    pub fn new(bind_address: &str, aggregator: Arc<MetricAggregator>) -> Self {
        Self {
            bind_address: bind_address.to_string(),
            aggregator,
        }
    }

    /// Run the UDP receiver loop.
    pub async fn run(&self) -> anyhow::Result<()> {
        let socket = UdpSocket::bind(&self.bind_address).await?;
        info!("StatsD: Listening on UDP {}", self.bind_address);

        // 64KB receive buffer for high-throughput scenarios
        let mut buf = vec![0u8; 65536];

        loop {
            match socket.recv_from(&mut buf).await {
                Ok((len, _addr)) => {
                    let data = match std::str::from_utf8(&buf[..len]) {
                        Ok(s) => s,
                        Err(_) => {
                            warn!("StatsD: Received non-UTF8 datagram, skipping");
                            continue;
                        }
                    };

                    let results = parser::parse_statsd_datagram(data);
                    for result in results {
                        match result {
                            Ok(metric) => self.aggregator.record(&metric),
                            Err(e) => warn!("StatsD: Parse error: {}", e),
                        }
                    }
                }
                Err(e) => {
                    error!("StatsD: UDP recv error: {}", e);
                }
            }
        }
    }
}
