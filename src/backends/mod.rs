pub mod backend_resource;
pub mod event_bus_backend;

#[cfg(feature = "kafka")]
pub mod kafka_backend;
#[cfg(feature = "redis")]
pub mod redis_backend;

pub use backend_resource::EventBusBackendResource;
pub use event_bus_backend::EventBusBackend;

#[cfg(feature = "kafka")]
pub use kafka_backend::{
    KafkaCommitOutcome, KafkaCommitRequest, KafkaCommitResult, KafkaCommitResultEvent,
    KafkaCommitResultStats, KafkaEventBusBackend, KafkaLagCache,
};

#[cfg(feature = "redis")]
pub use redis_backend::{RedisAckWorkerStats, RedisEventBusBackend};

// Producer flush helper removed; frame-level system now guarantees delivery.
