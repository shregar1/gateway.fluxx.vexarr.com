use crate::core::error::Result;
use crate::core::traits::{Capturer, Processor, Transporter};
use crate::models::event::Event;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};
use tokio::sync::Semaphore;

/// Pipeline orchestrates high-performance concurrent ingestion, processing 
/// and fan-out delivery. Optimized for backpressure and worker pooling.
pub struct Pipeline {
    capturers: Vec<Arc<dyn Capturer>>,
    processors: Vec<Arc<dyn Processor>>,
    transporters: Vec<Arc<dyn Transporter>>,
    worker_limit: usize,
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
            worker_limit: 100, // Balanced concurrency for processing tasks
        }
    }

    pub async fn run(&self, token: CancellationToken) -> Result<()> {
        info!(workers=%self.worker_limit, "Pipeline: Initiated high-performance data flow...");
        
        // Bounded channel provides native backpressure between ingestion and processing
        let (tx, mut rx) = mpsc::channel::<Event>(2048);
        let semaphore = Arc::new(Semaphore::new(self.worker_limit));

        // 1. Capture Layer Orchestration
        for capturer in &self.capturers {
            let tx = tx.clone();
            let token = token.clone();
            let cap = capturer.clone();

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = token.cancelled() => {
                            info!(cap=cap.name(), "Pipeline: Capturer task terminated.");
                            break;
                        }
                        event_res = cap.capture() => {
                            match event_res {
                                Ok(event) => {
                                    // tx.send() waits if rx buffer is full, establishing backpressure
                                    if let Err(_) = tx.send(event).await {
                                        warn!(cap=cap.name(), "Pipeline: Channel closed, stopping capturer.");
                                        break;
                                    }
                                }
                                Err(e) => error!(error=%e, cap=cap.name(), "Pipeline: Capture error."),
                            }
                        }
                    }
                }
            });
        }

        // 2. Processing & Fan-out Orchestration (Worker Pool)
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
                        let sem = semaphore.clone();

                        // Acquire permit before spawning to enforce worker pool limits
                        let permit = match sem.acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => break, // Semaphore closed
                        };

                        tokio::spawn(async move {
                            let _permit = permit; // Drop permit only when processing finishes
                            let mut processed_event = Some(event);
                            
                            // Sequential Processing (Middleware pattern)
                            for processor in processors {
                                if let Some(ev) = processed_event {
                                    match processor.process(ev).await {
                                        Ok(res) => processed_event = res,
                                        Err(e) => {
                                            error!(error=%e, proc=processor.name(), "Pipeline: Processor failure.");
                                            processed_event = None;
                                            break;
                                        }
                                    }
                                }
                            }

                            // Fan-out Transport (Zero-copy payload sharing across all sinks)
                            if let Some(ev) = processed_event {
                                let shared_event = Arc::new(ev);
                                
                                for transporter in &transporters {
                                    let trans = transporter.clone();
                                    let shared = shared_event.clone();
                                    
                                    tokio::spawn(async move {
                                        if let Err(e) = trans.send(shared).await {
                                            error!(error=%e, transport=trans.name(), "Pipeline: Event delivery failed.");
                                        }
                                    });
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
