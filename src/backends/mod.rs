pub mod backend_resource;
pub mod event_bus_backend;

#[cfg(feature = "kafka")]
pub mod kafka_backend;

pub use backend_resource::EventBusBackendResource;
pub use event_bus_backend::EventBusBackend;

#[cfg(feature = "kafka")]
pub use kafka_backend::{
    KafkaCommitRequest, KafkaCommitResult, KafkaEventBusBackend, KafkaLagCache,
};

// Producer flush helper removed; frame-level system now guarantees delivery.
