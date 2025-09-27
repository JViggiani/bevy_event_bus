use bevy::prelude::*;
use crossbeam_channel::Receiver;
use std::collections::HashMap;
use std::time::Instant;
use crate::BusEvent;
use crate::resources::backend_metadata::EventMetadata;

/// Raw incoming message captured by background consumer task
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub key: Option<Vec<u8>>,
    pub payload: Vec<u8>,
    pub timestamp: Instant,
    pub headers: HashMap<String, String>,
    pub consumer_group: Option<String>, // Which background consumer group received this (if any)
}

impl EventMetadata {
    /// Get the key as a UTF-8 string if possible
    pub fn key_as_string(&self) -> Option<String> {
        self.key.clone()
    }
    
    /// Get a human-readable representation of the key
    pub fn key_display(&self) -> Option<String> {
        self.key.clone()
    }
}

/// Unified event wrapper that provides direct access to event data with optional metadata.
/// External events from message brokers include metadata while internal Bevy events do not.
/// The wrapper can be used directly as the event thanks to Deref implementation.
#[derive(Debug, Clone)]
pub struct EventWrapper<T: BusEvent> {
    event: T,
    metadata: Option<EventMetadata>,
}

impl<T: BusEvent> EventWrapper<T> {
    /// Create a new EventWrapper for an external event with metadata
    pub fn new_external(event: T, metadata: EventMetadata) -> Self {
        Self {
            event,
            metadata: Some(metadata),
        }
    }
    
    /// Create a new EventWrapper for an internal event without metadata
    pub fn new_internal(event: T) -> Self {
        Self {
            event,
            metadata: None,
        }
    }
    
    /// Get the event data regardless of source
    pub fn event(&self) -> &T {
        &self.event
    }
    
    /// Get metadata if this is an external event
    pub fn metadata(&self) -> Option<&EventMetadata> {
        self.metadata.as_ref()
    }
    
    /// Check if this event came from an external source
    pub fn is_external(&self) -> bool {
        self.metadata.is_some()
    }
    
    /// Check if this event came from internal Bevy events
    pub fn is_internal(&self) -> bool {
        self.metadata.is_none()
    }
    
    /// Extract the inner event, consuming the wrapper
    pub fn into_event(self) -> T {
        self.event
    }
    
    /// Extract both event and metadata (if available), consuming the wrapper
    pub fn into_parts(self) -> (T, Option<EventMetadata>) {
        (self.event, self.metadata)
    }
}

impl<T: BusEvent> std::ops::Deref for EventWrapper<T> {
    type Target = T;
    
    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

/// Configuration controlling how many events are drained each frame
#[derive(Resource, Debug, Clone)]
#[derive(Default)]
pub struct EventBusConsumerConfig {
    /// Maximum events to drain per frame (None = unlimited)
    pub max_events_per_frame: Option<usize>,
    /// Optional millisecond budget for drain loop (None = no time limit)
    pub max_drain_millis: Option<u64>,
}

/// Channel receiver resource for background consumer -> main thread
#[derive(Resource)]
pub struct MessageQueue {
    pub receiver: Receiver<IncomingMessage>,
}

/// Pre-processed message with payload and metadata already converted for efficient reading
#[derive(Clone, Debug)]
pub struct ProcessedMessage {
    pub payload: Vec<u8>,
    pub metadata: EventMetadata,
}

/// Per-topic metadata-aware message buffers filled by drain system each frame
#[derive(Resource, Default, Debug)]
pub struct DrainedTopicMetadata {
    pub topics: std::collections::HashMap<String, Vec<ProcessedMessage>>,
    pub decode_errors: Vec<crate::EventBusDecodeError>,
}

/// Basic consumer metrics (frame-scoped counters + cumulative stats)
#[derive(Resource, Debug, Clone)]
#[derive(Default)]
pub struct ConsumerMetrics {
    pub drained_last_frame: usize,
    pub remaining_channel_after_drain: usize,
    pub dropped_messages: usize,
    pub total_drained: usize,
    pub queue_len_start: usize,
    pub queue_len_end: usize,
    pub drain_duration_us: u128,
    pub idle_frames: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::resources::backend_metadata::KafkaMetadata;
    use std::time::Instant;

    #[test]
    fn test_key_parsing() {
        // Test basic key operations with new metadata structure
        let mut metadata = EventMetadata::new(
            "test_topic".to_string(),
            Instant::now(),
            std::collections::HashMap::new(),
            None,
            None,
        );

        // Test None key
        assert_eq!(metadata.key_as_string(), None);
        assert_eq!(metadata.key_display(), None);

        // Test valid UTF-8 string key
        metadata.key = Some("user_123".to_string());
        assert_eq!(metadata.key_as_string(), Some("user_123".to_string()));
        assert_eq!(metadata.key_display(), Some("user_123".to_string()));

        // Test partition key for routing
        metadata.key = Some("world_456".to_string());
        assert_eq!(metadata.key_as_string(), Some("world_456".to_string()));
        assert_eq!(metadata.key_display(), Some("world_456".to_string()));

        // Test Kafka-specific metadata integration
        let kafka_metadata = KafkaMetadata {
            topic: "test_topic".to_string(),
            partition: 0,
            offset: 42,
            kafka_timestamp: Some(1234567890),
        };
        metadata.backend_specific = Some(Box::new(kafka_metadata));
        
        if let Some(kafka_meta) = metadata.kafka_metadata() {
            assert_eq!(kafka_meta.topic, "test_topic");
            assert_eq!(kafka_meta.partition, 0);
            assert_eq!(kafka_meta.offset, 42);
        } else {
            panic!("Expected Kafka metadata");
        }

        // Test headers functionality
        let mut headers = std::collections::HashMap::new();
        headers.insert("correlation_id".to_string(), "abc-123".to_string());
        headers.insert("source_service".to_string(), "world_simulator".to_string());
        
        let metadata_with_headers = EventMetadata::new(
            "events_topic".to_string(),
            Instant::now(),
            headers.clone(),
            Some("event_key_789".to_string()),
            Some(Box::new(KafkaMetadata {
                topic: "events_topic".to_string(),
                partition: 1,
                offset: 456,
                kafka_timestamp: Some(9876543210),
            })),
        );
        
        assert_eq!(metadata_with_headers.source, "events_topic");
        assert_eq!(metadata_with_headers.key_as_string(), Some("event_key_789".to_string()));
        assert_eq!(metadata_with_headers.headers.get("correlation_id"), Some(&"abc-123".to_string()));
        assert_eq!(metadata_with_headers.headers.get("source_service"), Some(&"world_simulator".to_string()));
    }

    #[test]
    fn test_backend_metadata_downcasting() {
        // Test that we can properly downcast backend-specific metadata
        let kafka_meta = KafkaMetadata {
            topic: "kafka_topic".to_string(),
            partition: 1,
            offset: 100,
            kafka_timestamp: Some(1122334455),
        };
        
        let metadata = EventMetadata::new(
            "kafka_topic".to_string(),
            Instant::now(),
            std::collections::HashMap::new(),
            Some("partition_key".to_string()),
            Some(Box::new(kafka_meta)),
        );
        
        // Test the helper method
        assert!(metadata.kafka_metadata().is_some());
        if let Some(k_meta) = metadata.kafka_metadata() {
            assert_eq!(k_meta.topic, "kafka_topic");
            assert_eq!(k_meta.partition, 1);
            assert_eq!(k_meta.offset, 100);
        }
    }
}

/// Event emitted after each drain with snapshot metrics (optional for user systems)
#[derive(Event, Debug, Clone)]
pub struct DrainMetricsEvent {
    pub drained: usize,
    pub remaining: usize,
    pub total_drained: usize,
    pub dropped: usize,
    pub drain_duration_us: u128,
}

/// Multi-decoded event storage that groups all successfully decoded events by topic
/// This replaces DrainedTopicMetadata for the new multi-decoder pipeline
#[derive(Resource, Default)]
pub struct DecodedEventBuffer {
    /// Maps topic name to lists of decoded events (organized by type)
    pub topics: HashMap<String, TopicDecodedEvents>,
}

/// Storage for all decoded events from a single topic, organized by event type
#[derive(Default)]
pub struct TopicDecodedEvents {
    /// Maps TypeId to a vector of type-erased decoded events with metadata
    pub events_by_type: HashMap<std::any::TypeId, Vec<TypeErasedEvent>>,
    
    /// Total number of raw messages processed for this topic
    pub total_processed: usize,
    
    /// Number of messages that failed to decode with any decoder
    pub decode_failures: usize,
}

/// Type-erased event with metadata, used for storage before type-specific retrieval
pub struct TypeErasedEvent {
    /// The decoded event as a type-erased box
    pub event: Box<dyn std::any::Any + Send + Sync>,
    
    /// Metadata associated with this event
    pub metadata: EventMetadata,
    
    /// Name of the decoder that produced this event (for debugging)
    pub decoder_name: String,
}

impl TopicDecodedEvents {
    pub fn new() -> Self {
        Self::default()
    }
    
    /// Add a decoded event to the appropriate type bucket
    pub fn add_event<T: 'static + Send + Sync>(&mut self, event: T, metadata: EventMetadata, decoder_name: String) {
        let type_id = std::any::TypeId::of::<T>();
        let type_erased = TypeErasedEvent {
            event: Box::new(event),
            metadata,
            decoder_name,
        };
        
        self.events_by_type
            .entry(type_id)
            .or_insert_with(Vec::new)
            .push(type_erased);
    }
    
    /// Get events of a specific type, converting them back from type-erased storage
    pub fn get_events<T: BusEvent>(&self) -> Vec<EventWrapper<T>> {
        let type_id = std::any::TypeId::of::<T>();
        
        if let Some(type_erased_events) = self.events_by_type.get(&type_id) {
            type_erased_events
                .iter()
                .filter_map(|te| {
                    te.event
                        .downcast_ref::<T>()
                        .map(|event| EventWrapper::new_external(event.clone(), te.metadata.clone()))
                })
                .collect()
        } else {
            Vec::new()
        }
    }
    
    /// Get the number of events of a specific type
    pub fn count_events<T: 'static>(&self) -> usize {
        let type_id = std::any::TypeId::of::<T>();
        self.events_by_type.get(&type_id).map(|v| v.len()).unwrap_or(0)
    }
    
    /// Get total number of successfully decoded events across all types
    pub fn total_events(&self) -> usize {
        self.events_by_type.values().map(|v| v.len()).sum()
    }
    
    /// Clear all events (typically called after processing)
    pub fn clear(&mut self) {
        self.events_by_type.clear();
        self.total_processed = 0;
        self.decode_failures = 0;
    }
    
    /// Get decode success rate as a percentage
    pub fn success_rate(&self) -> f32 {
        if self.total_processed == 0 {
            0.0
        } else {
            (self.total_events() as f32 / self.total_processed as f32) * 100.0
        }
    }
}
