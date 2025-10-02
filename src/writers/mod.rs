#[cfg(feature = "kafka")]
mod kafka;

pub mod outbound_bridge;

use std::sync::Mutex;

use bevy::prelude::*;

use bevy_event_bus::config::EventBusConfig;
use bevy_event_bus::{BusEvent, EventBusError};

#[cfg(feature = "kafka")]
pub use kafka::{KafkaEventWriter, KafkaWriterError};

/// Queue of errors emitted by writers so they can be flushed outside of the system parameter borrow.
#[derive(Resource, Default)]
pub struct EventBusErrorQueue {
    pending_errors: Mutex<Vec<Box<dyn Fn(&mut World) + Send + Sync>>>,
}

impl EventBusErrorQueue {
    pub fn add_error<T: BusEvent + Event>(&self, error: EventBusError<T>) {
        if let Ok(mut pending) = self.pending_errors.lock() {
            pending.push(Box::new(move |world: &mut World| {
                world.send_event(error.clone());
            }));
        }
    }

    pub fn drain_pending(&self) -> Vec<Box<dyn Fn(&mut World) + Send + Sync>> {
        if let Ok(mut pending) = self.pending_errors.lock() {
            std::mem::take(&mut *pending)
        } else {
            Vec::new()
        }
    }
}

/// Common interface shared by all outbound event writers.
pub trait BusEventWriter<T: BusEvent> {
    /// Send a single event using the provided configuration.
    fn write<C: EventBusConfig>(&mut self, config: &C, event: T);

    /// Send a batch of events using the provided configuration.
    fn write_batch<C, I>(&mut self, config: &C, events: I)
    where
        C: EventBusConfig,
        I: IntoIterator<Item = T>;
}
