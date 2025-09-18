//! bevy_event_bus: Connect Bevy events to external message brokers
//!
//! This crate provides an interface similar to Bevy's EventReader/EventWriter
//! but connected to external message brokers like Kafka.

// Core modules
pub mod app_ext;
pub mod backends;
pub mod decoder;
mod error;
mod event;
mod plugin;
mod readers;
pub mod registration; // internal use by derive
mod resources;
mod runtime;
mod writers;

// Re-exports
pub use app_ext::EventBusAppExt;
pub use backends::{EventBusBackend, EventBusBackendExt};
pub use decoder::{DecoderRegistry, DecoderFn, TypedDecoder, DecodedEvent};
pub use error::{EventBusError, EventBusErrorType, EventBusDecodeError};
pub use event::BusEvent;
pub use plugin::{BackendDownEvent, BackendReadyEvent, BackendStatus};
pub use plugin::{EventBusPlugin, EventBusPlugins, PreconfiguredTopics};
pub use readers::event_bus_reader::EventBusReader;
pub use resources::{
    ConsumerMetrics, DecodedEventBuffer, DeliveryEvent, DrainMetricsEvent, DrainedTopicMetadata, EventBusConsumerConfig,
    EventMetadata, ExternalEvent, IncomingMessage, MessageQueue, OutboundMessage, OutboundMessageQueue, ProcessedMessage,
    TopicDecodedEvents, TypeErasedEvent,
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
        app_ext::EventBusAppExt,
        BusEvent, ConsumerMetrics, DecodedEvent, DecodedEventBuffer, DecoderRegistry, DeliveryEvent, DrainMetricsEvent, DrainedTopicMetadata, EventBusBackend,
        EventBusBackendExt, EventBusConsumerConfig, EventBusError, EventBusErrorType, EventBusDecodeError, EventBusPlugin, EventBusPlugins,
        EventBusReader, EventBusWriter, EventMetadata, ExternalBusEvent, ExternalEvent, IncomingMessage, MessageQueue,
        OutboundMessage, OutboundMessageQueue, ProcessedMessage, TopicDecodedEvents, TypedDecoder, TypeErasedEvent,
    };

    #[cfg(feature = "kafka")]
    pub use crate::{KafkaConfig, KafkaEventBusBackend};
}

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;
