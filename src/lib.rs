//! bevy_event_bus: Connect Bevy events to external message brokers
//!
//! This crate provides an interface similar to Bevy's MessageReader/MessageWriter
//! but connected to external message brokers like Kafka.

extern crate self as bevy_event_bus;

// Core modules
pub mod backends;
pub mod bus_message;
pub mod config;
pub mod decoder;
mod error;
pub mod plugins;
mod readers;
mod resources;
mod runtime;
mod writers;

// Re-exports
pub use backends::EventBusBackend;
pub use backends::event_bus_backend::StreamTrimStrategy;
pub use bus_message::BusMessage;
#[cfg(feature = "kafka")]
pub use config::Kafka;
pub use config::{BackendMarker, EventBusConfig, InMemory, ProcessingLimits, TopologyMode};
pub use decoder::{DecodedEvent, DecoderFn, DecoderRegistry, TypedDecoder};
pub use error::{BusErrorCallback, BusErrorContext, EventBusError, EventBusErrorType};
pub use plugins::event_bus::{
    BackendCapabilities, BackendDownMessage, BackendReadyMessage, BackendStatus, EventBusPlugin,
};
pub use readers::BusMessageReader;
pub use readers::{KafkaMessageReader, KafkaReaderError};
#[cfg(feature = "redis")]
pub use readers::{RedisMessageReader, RedisReaderError};
pub use resources::{
    BackendMetadata, ConsumerMetrics, DEFAULT_MAX_RETAINED_PER_TOPIC, DrainMetricsMessage,
    DrainedTopicMetadata, EventBusConsumerConfig, KafkaMetadata, MessageMetadata, MessageQueue,
    MessageWrapper, ProcessedMessage, ProvisionedTopology,
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
        BusMessage, BusMessageReader, BusMessageWriter, ConsumerMetrics, DecodedEvent,
        DecoderRegistry, EventBusBackend, EventBusConsumerConfig, EventBusError, EventBusErrorType,
        EventBusPlugin, MessageMetadata, MessageWrapper, ProcessedMessage,
        StreamTrimStrategy, TypedDecoder,
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
