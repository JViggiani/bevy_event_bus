//! Configuration system for backend-agnostic event bus readers and writers
//!
//! This module provides a type-safe configuration-driven approach where backend types
//! are inferred from configuration objects at compile time. This enables clean system
//! signatures without explicit backend type parameters while maintaining full type safety.

pub mod kafka;

/// Core trait for event bus configurations that enables type-safe backend inference
pub trait EventBusConfig: Send + Sync + Clone + 'static {
    /// The backend type this configuration is for (e.g., Kafka, InMemory)
    type Backend: BackendMarker;

    /// Get topics this config applies to
    fn topics(&self) -> &[String];

    /// Get a unique identifier for this configuration instance
    fn config_id(&self) -> String;
}

/// Marker trait for backend types to enable compile-time dispatch
pub trait BackendMarker: Send + Sync + 'static {}

/// Backend marker types for compile-time type inference
#[derive(Debug, Clone, Copy)]
pub struct Kafka;
impl BackendMarker for Kafka {}

#[derive(Debug, Clone, Copy)]
pub struct InMemory;
impl BackendMarker for InMemory {}

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
