use crate::models::event::Event;
use crate::core::traits::Processor;
use crate::core::error::Result;
use async_trait::async_trait;
use tracing::debug;

/// TopologyProcessor: High-fidelity middleware that enriches every event with its 
/// organizational context (Project/Environment/Service) for advanced log filtering.
#[derive(Debug, Clone)]
pub struct TopologyProcessor {
    project_id: String,
    environment_id: String,
    service_id: String,
}

impl TopologyProcessor {
    pub fn new(project: &str, environment: &str, service: &str) -> Self {
        Self {
            project_id: project.to_string(),
            environment_id: environment.to_string(),
            service_id: service.to_string(),
        }
    }
}

#[async_trait]
impl Processor for TopologyProcessor {
    async fn process(&self, mut event: Event) -> Result<Option<Event>> {
        debug!(id = %event.id, service = %self.service_id, "TopologyProcessor: Finalizing enrichment.");
        
        // Enforce the organizational hierarchy on the event stream.
        event.project_id = Some(self.project_id.clone());
        event.environment_id = Some(self.environment_id.clone());
        event.service_id = Some(self.service_id.clone());

        Ok(Some(event))
    }

    fn name(&self) -> &str {
        "TopologyProcessor"
    }
}
