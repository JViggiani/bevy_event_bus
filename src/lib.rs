//! bevy_event_bus: Connect Bevy events to external message brokers
//!
//! This crate provides an interface similar to Bevy's EventReader/EventWriter
//! but connected to external message brokers like Kafka.

extern crate self as bevy_event_bus;

// Core modules
pub mod app_ext;
pub mod backends;
pub mod config;
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
pub use backends::EventBusBackend;
pub use config::{BackendMarker, EventBusConfig, InMemory, Kafka, ProcessingLimits};
pub use decoder::{DecodedEvent, DecoderFn, DecoderRegistry, TypedDecoder};
pub use error::{EventBusDecodeError, EventBusError, EventBusErrorType};
pub use event::BusEvent;
pub use plugin::{BackendDownEvent, BackendReadyEvent, BackendStatus};
pub use plugin::{EventBusPlugin, EventBusPlugins};
pub use readers::BusEventReader;
#[cfg(feature = "kafka")]
pub use readers::{KafkaEventReader, KafkaReaderError};
pub use resources::{
    BackendMetadata, ConsumerMetrics, DecodedEventBuffer, DrainMetricsEvent, DrainedTopicMetadata,
    EventBusConsumerConfig, EventMetadata, EventWrapper, KafkaMetadata, ProcessedMessage,
    TopicDecodedEvents,
};
pub use writers::{BusEventWriter, EventBusErrorQueue};
#[cfg(feature = "kafka")]
pub use writers::{KafkaEventWriter, KafkaWriterError};

// Re-export backends
#[cfg(feature = "kafka")]
pub use backends::kafka_backend::KafkaEventBusBackend;
#[cfg(feature = "kafka")]
pub use config::kafka::{
    KafkaBackendConfig, KafkaConnectionConfig, KafkaConsumerConfig, KafkaConsumerGroupSpec,
    KafkaEventMetadata, KafkaInitialOffset, KafkaProducerConfig, KafkaTopicSpec,
    KafkaTopologyConfig, UncommittedEvent,
};

// Re-export the derive macro
pub use bevy_event_bus_derive::ExternalBusEvent;
pub use registration::EVENT_REGISTRY; // hidden but available for derive macro
pub use runtime::{SharedRuntime, ensure_runtime};
pub use runtime::{block_on, runtime};

/// Re-export common items for convenience
pub mod prelude {
    pub use bevy_event_bus::{
        BusEvent, BusEventReader, BusEventWriter, ConsumerMetrics, DecodedEvent,
        DecodedEventBuffer, DecoderRegistry, EventBusBackend, EventBusConsumerConfig,
        EventBusDecodeError, EventBusError, EventBusErrorType, EventBusPlugin, EventBusPlugins,
        EventMetadata, EventWrapper, ExternalBusEvent, ProcessedMessage, TopicDecodedEvents,
        TypedDecoder,
        app_ext::EventBusAppExt,
        config::{BackendMarker, EventBusConfig, InMemory, Kafka, ProcessingLimits},
    };

    #[cfg(feature = "kafka")]
    pub use bevy_event_bus::{
        KafkaEventBusBackend, KafkaEventReader, KafkaEventWriter, KafkaReaderError,
        KafkaWriterError,
        config::kafka::{
            KafkaBackendConfig, KafkaConnectionConfig, KafkaConsumerConfig, KafkaConsumerGroupSpec,
            KafkaEventMetadata, KafkaInitialOffset, KafkaProducerConfig, KafkaTopicSpec,
            KafkaTopologyConfig, UncommittedEvent,
        },
    };
}

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;
