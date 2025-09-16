//! bevy_event_bus: Connect Bevy events to external message brokers
//!
//! This crate provides an interface similar to Bevy's EventReader/EventWriter
//! but connected to external message brokers like Kafka or AMQP.

// Core modules
pub mod backends;
mod error;
mod event;
mod plugin;
mod readers;
pub mod registration; // internal use by derive
mod resources;
mod runtime;
mod writers;

// Re-exports
pub use backends::{EventBusBackend, EventBusBackendExt};
pub use error::EventBusError;
pub use event::BusEvent;
pub use plugin::{BackendDownEvent, BackendReadyEvent, BackendStatus};
pub use plugin::{EventBusPlugin, EventBusPlugins, PreconfiguredTopics};
pub use readers::event_bus_reader::EventBusReader;
pub use resources::{
    ConsumerMetrics, DeliveryEvent, DrainMetricsEvent, DrainedTopicBuffers, EventBusConsumerConfig,
    IncomingMessage, MessageQueue, OutboundMessage, OutboundMessageQueue,
};
pub use writers::event_bus_writer::EventBusWriter;

// Re-export backends
#[cfg(feature = "kafka")]
pub use backends::kafka_backend::{KafkaConfig, KafkaEventBusBackend};

// Re-export the derive macro
pub use bevy_event_bus_derive::ExternalBusEvent;
pub use registration::EVENT_REGISTRY; // hidden but available
pub use runtime::{SharedRuntime, ensure_runtime};
pub use runtime::{block_on, runtime};

/// Re-export common items for convenience
pub mod prelude {
    pub use crate::{
        BusEvent, ConsumerMetrics, DeliveryEvent, DrainMetricsEvent, DrainedTopicBuffers, EventBusBackend,
        EventBusBackendExt, EventBusConsumerConfig, EventBusError, EventBusPlugin, EventBusPlugins,
        EventBusReader, EventBusWriter, ExternalBusEvent, IncomingMessage, MessageQueue,
        OutboundMessage, OutboundMessageQueue,
    };

    #[cfg(feature = "kafka")]
    pub use crate::{KafkaConfig, KafkaEventBusBackend};
}

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;
