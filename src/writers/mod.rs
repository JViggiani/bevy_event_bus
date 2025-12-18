#[cfg(feature = "kafka")]
mod kafka;
#[cfg(feature = "redis")]
mod redis;

pub mod outbound_bridge;

use bevy::prelude::*;

use bevy_event_bus::BusEvent;
use bevy_event_bus::config::EventBusConfig;
use bevy_event_bus::errors::BusErrorCallback;

#[cfg(feature = "kafka")]
pub use kafka::{KafkaMessageWriter, KafkaWriterError};
#[cfg(feature = "redis")]
pub use redis::{RedisMessageWriter, RedisWriterError};

/// Common interface shared by all outbound message writers.
pub trait BusMessageWriter<T: BusEvent + Message> {
    /// Send a single message using the provided configuration.
    fn write<C: EventBusConfig>(
        &mut self,
        config: &C,
        event: T,
        error_callback: Option<BusErrorCallback>,
    );

    /// Send a batch of messages using the provided configuration.
    fn write_batch<C, I>(
        &mut self,
        config: &C,
        events: I,
        error_callback: Option<BusErrorCallback>,
    ) where
        C: EventBusConfig,
        I: IntoIterator<Item = T>;
}
