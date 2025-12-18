use bevy::prelude::Message;
use bevy_event_bus::BusEvent;
use bevy_event_bus::config::EventBusConfig;
use bevy_event_bus::resources::MessageWrapper;

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "kafka")]
pub use kafka::{KafkaMessageReader, KafkaReaderError};

#[cfg(feature = "redis")]
pub mod redis;
#[cfg(feature = "redis")]
pub use redis::{RedisMessageReader, RedisReaderError};

/// Common capabilities shared by all bus message readers.
pub trait BusMessageReader<T: BusEvent + Message> {
    /// Drain the buffered messages for the supplied configuration.
    fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<MessageWrapper<T>>;
}
