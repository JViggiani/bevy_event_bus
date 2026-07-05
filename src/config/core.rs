//! Core configuration traits and shared configuration value types.

/// Core trait for event bus configurations that enables type-safe backend inference
pub trait EventBusConfig: Send + Sync + Clone + 'static {
    /// The backend type this configuration is for (e.g., Kafka, InMemory)
    type Backend: super::markers::BackendMarker;

    /// Get topics this config applies to
    fn topics(&self) -> &[String];

    /// Get a unique identifier for this configuration instance
    fn config_id(&self) -> String;

    /// Allow downcasting to concrete config types
    fn as_any(&self) -> &dyn std::any::Any;
}

/// Controls whether topology elements should be provisioned or only validated at runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TopologyMode {
    /// Create or update the topology element if it does not already exist.
    Provision,
    /// Require the topology element to exist and fail if it is missing or inaccessible.
    Validate,
}

/// Configuration for frame-level processing limits
#[derive(Debug, Clone)]
pub struct ProcessingLimits {
    /// Maximum events to process per frame
    pub max_events_per_frame: Option<usize>,
    /// Maximum time to spend draining events per frame
    pub max_drain_millis: Option<u64>,
}

impl Default for ProcessingLimits {
    fn default() -> Self {
        Self {
            max_events_per_frame: Some(100),
            max_drain_millis: Some(10),
        }
    }
}
