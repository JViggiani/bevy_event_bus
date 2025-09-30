use bevy_event_bus::BusEvent;
use bevy_event_bus::config::EventBusConfig;
use bevy_event_bus::resources::EventWrapper;

#[cfg(feature = "kafka")]
pub mod kafka;

#[cfg(feature = "kafka")]
pub use kafka::{KafkaEventReader, KafkaReaderError};

/// Common capabilities shared by all bus event readers.
pub trait BusEventReader<T: BusEvent> {
    /// Drain the buffered events for the supplied configuration.
    fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<EventWrapper<T>>;
}
