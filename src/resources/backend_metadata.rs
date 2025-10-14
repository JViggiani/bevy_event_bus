use std::any::Any;
use std::collections::HashMap;
use std::time::Instant;

/// Generic event metadata that works across all backends
#[derive(Debug, Clone)]
pub struct EventMetadata {
    pub source: String, // topic/queue/stream name
    pub timestamp: Instant,
    pub key: Option<String>, // Generic message key
    pub backend_specific: Option<Box<dyn BackendMetadata>>,
}

/// Trait for backend-specific metadata extensions
pub trait BackendMetadata: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn clone_box(&self) -> Box<dyn BackendMetadata>;
}

// Implement Clone for Box<dyn BackendMetadata>
impl Clone for Box<dyn BackendMetadata> {
    fn clone(&self) -> Self {
        self.clone_box()
    }
}

/// Kafka-specific metadata
#[derive(Debug, Clone)]
pub struct KafkaMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub consumer_group: Option<String>,
    pub manual_commit: bool,
    pub headers: HashMap<String, String>,
}

impl BackendMetadata for KafkaMetadata {
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn clone_box(&self) -> Box<dyn BackendMetadata> {
        Box::new(self.clone())
    }
}

/// Redis-specific metadata
#[derive(Debug, Clone)]
pub struct RedisMetadata {
    pub stream: String,
    pub entry_id: String,
    pub consumer_group: Option<String>,
    pub manual_ack: bool,
}

impl BackendMetadata for RedisMetadata {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn clone_box(&self) -> Box<dyn BackendMetadata> {
        Box::new(self.clone())
    }
}

/// Extension methods for easy access to backend-specific metadata
impl EventMetadata {
    /// Create new metadata with backend-specific data
    pub fn new(
        source: String,
        timestamp: Instant,
        key: Option<String>,
        backend_specific: Option<Box<dyn BackendMetadata>>,
    ) -> Self {
        Self {
            source,
            timestamp,
            key,
            backend_specific,
        }
    }

    /// Get Kafka-specific metadata if available
    pub fn kafka_metadata(&self) -> Option<&KafkaMetadata> {
        self.backend_specific
            .as_ref()?
            .as_any()
            .downcast_ref::<KafkaMetadata>()
    }

    /// Get Redis-specific metadata if available
    pub fn redis_metadata(&self) -> Option<&RedisMetadata> {
        self.backend_specific
            .as_ref()?
            .as_any()
            .downcast_ref::<RedisMetadata>()
    }
}
