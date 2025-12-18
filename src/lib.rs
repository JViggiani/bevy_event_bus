//! bevy_event_bus: Connect Bevy events to external message brokers
//!
//! This crate provides an interface similar to Bevy's MessageReader/MessageWriter
//! but connected to external message brokers like Kafka.

extern crate self as bevy_event_bus;

// Core modules
pub mod backends;
pub mod config;
pub mod decoder;
mod errors;
mod error;
mod event;
mod plugin;
mod readers;
mod resources;
mod runtime;
mod writers;

// Re-exports
pub use backends::EventBusBackend;
pub use backends::event_bus_backend::StreamTrimStrategy;
#[cfg(feature = "kafka")]
pub use config::Kafka;
pub use errors::{BusErrorCallback, BusErrorContext, BusErrorKind};
pub use config::{BackendMarker, EventBusConfig, InMemory, ProcessingLimits, TopologyMode};
pub use decoder::{DecodedEvent, DecoderFn, DecoderRegistry, TypedDecoder};
pub use error::{EventBusDecodeError, EventBusError, EventBusErrorType};
pub use event::BusEvent;
pub use plugin::{BackendCapabilities, BackendDownMessage, BackendReadyMessage, BackendStatus};
pub use plugin::{EventBusPlugin, EventBusPlugins};
pub use readers::BusMessageReader;
#[cfg(feature = "kafka")]
pub use readers::{KafkaMessageReader, KafkaReaderError};
#[cfg(feature = "redis")]
pub use readers::{RedisMessageReader, RedisReaderError};
pub use resources::{
    BackendMetadata, ConsumerMetrics, DecodedEventBuffer, DrainMetricsMessage,
    DrainedTopicMetadata, EventBusConsumerConfig, KafkaMetadata, MessageMetadata, MessageWrapper,
    ProcessedMessage, ProvisionedTopology, TopicDecodedEvents,
};
pub use writers::BusMessageWriter;
#[cfg(feature = "kafka")]
pub use writers::{KafkaMessageWriter, KafkaWriterError};
#[cfg(feature = "redis")]
pub use writers::{RedisMessageWriter, RedisWriterError};

// Re-export backends
#[cfg(feature = "kafka")]
pub use backends::kafka_backend::KafkaEventBusBackend;
#[cfg(feature = "redis")]
pub use backends::redis_backend::{RedisAckWorkerStats, RedisEventBusBackend};
#[cfg(feature = "kafka")]
pub use config::kafka::{
    KafkaBackendConfig, KafkaChannelCapacities, KafkaConnectionConfig, KafkaConsumerConfig,
    KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaMessageMetadata, KafkaProducerConfig,
    KafkaTopicSpec, KafkaTopologyConfig, UncommittedEvent,
};

#[cfg(feature = "redis")]
pub use config::redis::{
    RedisBackendConfig, RedisConnectionConfig, RedisConsumerConfig, RedisConsumerGroupSpec,
    RedisProducerConfig, RedisRuntimeTuning, RedisStreamSpec, RedisTopologyBuilder,
    RedisTopologyConfig, RedisTopologyEventBinding, TrimStrategy,
};

pub use runtime::{SharedRuntime, ensure_runtime};
pub use runtime::{block_on, runtime};

/// Re-export common items for convenience
pub mod prelude {
    pub use bevy_event_bus::{
        BusEvent, BusMessageReader, BusMessageWriter, ConsumerMetrics, DecodedEvent,
        DecodedEventBuffer, DecoderRegistry, EventBusBackend, EventBusConsumerConfig,
        EventBusDecodeError, EventBusError, EventBusErrorType, EventBusPlugin, EventBusPlugins,
        MessageMetadata, MessageWrapper, ProcessedMessage, StreamTrimStrategy, TopicDecodedEvents,
        TypedDecoder,
        config::{BackendMarker, EventBusConfig, InMemory, ProcessingLimits, TopologyMode},
    };

    #[cfg(feature = "redis")]
    pub use bevy_event_bus::config::Redis;

    #[cfg(feature = "kafka")]
    pub use bevy_event_bus::{
        KafkaEventBusBackend, KafkaMessageReader, KafkaMessageWriter, KafkaReaderError,
        KafkaWriterError,
        config::Kafka,
        config::kafka::{
            KafkaBackendConfig, KafkaChannelCapacities, KafkaConnectionConfig, KafkaConsumerConfig,
            KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaMessageMetadata, KafkaProducerConfig,
            KafkaTopicSpec, KafkaTopologyConfig, UncommittedEvent,
        },
    };

    #[cfg(feature = "redis")]
    pub use bevy_event_bus::config::redis::{
        RedisBackendConfig, RedisConnectionConfig, RedisConsumerConfig, RedisConsumerGroupSpec,
        RedisProducerConfig, RedisRuntimeTuning, RedisStreamSpec, RedisTopologyBuilder,
        RedisTopologyConfig, TrimStrategy,
    };

    #[cfg(feature = "redis")]
    pub use bevy_event_bus::{
        RedisAckWorkerStats, RedisEventBusBackend, RedisMessageReader, RedisMessageWriter,
        RedisReaderError, RedisWriterError,
    };
}

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;
