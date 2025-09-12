//! bevy_event_bus: Connect Bevy events to external message brokers
//!
//! This crate provides an interface similar to Bevy's EventReader/EventWriter
//! but connected to external message brokers like Kafka or AMQP.

// Core modules
mod backends;
mod error;
mod event;
mod plugin;
mod readers;
pub mod registration;
mod writers;
mod runtime;

// Re-exports
pub use backends::{EventBusBackend, EventBusBackendExt};
pub use error::EventBusError;
pub use event::BusEvent;
pub use plugin::{EventBusPlugin, EventBusPlugins, EventBusAppExt};
pub use readers::event_bus_reader::EventBusReader;
pub use writers::event_bus_writer::EventBusWriter;

// Re-export backends
#[cfg(feature = "kafka")]
pub use backends::kafka_backend::{KafkaEventBusBackend, KafkaConfig};

// Re-export the derive macro
pub use bevy_event_bus_derive::ExternalBusEvent;
pub use registration::{EVENT_REGISTRY};
pub use runtime::{block_on, runtime};

/// Re-export common items for convenience
pub mod prelude {
    pub use crate::{
        BusEvent,
        EventBusReader,
        EventBusWriter,
        EventBusPlugin,
        EventBusPlugins,
        EventBusError,
        ExternalBusEvent,
        EventBusBackend,
        EventBusBackendExt,
    };
    
    #[cfg(feature = "kafka")]
    pub use crate::{KafkaEventBusBackend, KafkaConfig};
}

// Re-export for macro use
#[doc(hidden)]
pub use once_cell;
