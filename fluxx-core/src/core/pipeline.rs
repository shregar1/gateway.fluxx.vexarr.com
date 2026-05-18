use crate::core::error::Result;
use crate::core::traits::{Capturer, Processor, Transporter};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info};

pub struct Pipeline {
    capturers: Vec<Arc<dyn Capturer>>,
    processors: Vec<Arc<dyn Processor>>,
    transporters: Vec<Arc<dyn Transporter>>,
}

impl Pipeline {
    pub fn new(
        capturers: Vec<Arc<dyn Capturer>>,
        processors: Vec<Arc<dyn Processor>>,
        transporters: Vec<Arc<dyn Transporter>>,
    ) -> Self {
        Self {
            capturers,
            processors,
            transporters,
        }
    }

    pub async fn run(&self, token: CancellationToken) -> Result<()> {
        info!("Pipeline: Initiating data flow...");
        let (tx, mut rx) = mpsc::channel(1024);

        // Capture Layer Orchestration
        for capturer in &self.capturers {
            let tx = tx.clone();
            let token = token.clone();
            let cap = capturer.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = token.cancelled() => {
                            info!("Pipeline: Capturer '{}' task terminated.", cap.name());
                            break;
                        }
                        event_res = cap.capture() => {
                            match event_res {
                                Ok(event) => {
                                    if let Err(e) = tx.send(event).await {
                                        error!("Pipeline: Channel closed, stopping capturer {}: {}", cap.name(), e);
                                        break;
                                    }
                                }
                                Err(e) => error!("Pipeline: Capture error from {}: {}", cap.name(), e),
                            }
                        }
                    }
                }
            });
        }

        // Processing & Transport Layer Orchestration
        let processors = self.processors.clone();
        let transporters = self.transporters.clone();
        let loop_token = token.clone();

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = loop_token.cancelled() => {
                        info!("Pipeline: Processing task terminated.");
                        break;
                    }
                    Some(event) = rx.recv() => {
                        let processors = processors.clone();
                        let transporters = transporters.clone();

                        tokio::spawn(async move {
                            let mut processed_event = Some(event);
                            
                            // Sequential Processing (Middleware pattern)
                            for processor in processors {
                                if let Some(ev) = processed_event {
                                    match processor.process(ev).await {
                                        Ok(res) => processed_event = res,
                                        Err(e) => {
                                            error!("Pipeline: Processor error from {}: {}", processor.name(), e);
                                            processed_event = None;
                                            break;
                                        }
                                    }
                                }
                            }

                            // Fan-out Transport
                            if let Some(ev) = processed_event {
                                for transporter in transporters {
                                    if let Err(e) = transporter.send(ev.clone()).await {
                                        error!("Pipeline: Transport error from {}: {}", transporter.name(), e);
                                    }
                                }
                            }
                        });
                    }
                }
            }
        });

        Ok(())
    }
}
