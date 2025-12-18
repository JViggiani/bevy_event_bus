use bevy::prelude::*;
use crossbeam_channel::Receiver;
use std::collections::HashMap;
use std::time::Instant;

use crate::BusEvent;
use crate::resources::backend_metadata::{BackendMetadata, MessageMetadata};
#[cfg(test)]
use crate::resources::backend_metadata::{KafkaMetadata, RedisMetadata};

/// Raw incoming message captured by background consumer task
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub source: String,
    pub payload: Vec<u8>,
    pub key: Option<String>,
    pub timestamp: Instant,
    pub backend_metadata: Option<Box<dyn BackendMetadata>>,
}

impl MessageMetadata {
    /// Get the key as a UTF-8 string if possible
    pub fn key_as_string(&self) -> Option<String> {
        self.key.clone()
    }

    /// Get a human-readable representation of the key
    pub fn key_display(&self) -> Option<String> {
        self.key.clone()
    }
}

impl IncomingMessage {
    /// Construct an `IncomingMessage` using the current instant.
    pub fn plain(
        source: impl Into<String>,
        payload: Vec<u8>,
        key: Option<String>,
        backend_metadata: Option<Box<dyn BackendMetadata>>,
    ) -> Self {
        Self {
            source: source.into(),
            payload,
            key,
            timestamp: Instant::now(),
            backend_metadata,
        }
    }
}

/// Unified message wrapper that provides direct access to message data along with its metadata.
/// Wrappers are only produced for externally sourced messages, so metadata is always present.
/// The wrapper can be used directly as the message thanks to the `Deref` implementation.
#[derive(Debug, Clone)]
pub struct MessageWrapper<T: BusEvent> {
    event: T,
    metadata: MessageMetadata,
}

impl<T: BusEvent> MessageWrapper<T> {
    /// Create a new `MessageWrapper` for a message coming from the external bus.
    pub fn new(event: T, metadata: MessageMetadata) -> Self {
        Self { event, metadata }
    }

    /// Get the message data regardless of source
    pub fn event(&self) -> &T {
        &self.event
    }

    /// Get metadata associated with this message
    pub fn metadata(&self) -> &MessageMetadata {
        &self.metadata
    }

    /// Extract the inner message, consuming the wrapper
    pub fn into_event(self) -> T {
        self.event
    }

    /// Extract both message and metadata, consuming the wrapper
    pub fn into_parts(self) -> (T, MessageMetadata) {
        (self.event, self.metadata)
    }
}

impl<T: BusEvent> std::ops::Deref for MessageWrapper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

/// Configuration controlling how many events are drained each frame
#[derive(Resource, Debug, Clone, Default)]
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
    pub metadata: MessageMetadata,
}

/// Per-topic metadata-aware message buffers filled by drain system each frame
#[derive(Resource, Default, Debug)]
pub struct DrainedTopicMetadata {
    pub topics: HashMap<String, Vec<ProcessedMessage>>,
    pub decode_errors: Vec<crate::EventBusDecodeError>,
}

/// Basic consumer metrics (frame-scoped counters + cumulative stats)
#[derive(Resource, Debug, Clone, Default)]
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

/// Message emitted after each drain with snapshot metrics (optional for user systems)
#[derive(Message, Debug, Clone)]
pub struct DrainMetricsMessage {
    pub drained: usize,
    pub remaining: usize,
    pub total_drained: usize,
    pub dropped: usize,
    pub drain_duration_us: u128,
}

/// Multi-decoded event storage that groups all successfully decoded events by topic
/// This replaces `DrainedTopicMetadata` for the new multi-decoder pipeline
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
    pub metadata: MessageMetadata,

    /// Name of the decoder that produced this event (for debugging)
    pub decoder_name: &'static str,
}

impl TopicDecodedEvents {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a decoded event to the appropriate type bucket
    pub fn add_event<T: 'static + Send + Sync>(
        &mut self,
        event: T,
        metadata: MessageMetadata,
        decoder_name: &'static str,
    ) {
        let type_id = std::any::TypeId::of::<T>();
        let type_erased = TypeErasedEvent {
            event: Box::new(event),
            metadata,
            decoder_name,
        };

        self.events_by_type
            .entry(type_id)
            .or_default()
            .push(type_erased);
    }

    /// Get events of a specific type, converting them back from type-erased storage
    pub fn get_events<T: BusEvent>(&self) -> Vec<MessageWrapper<T>> {
        let type_id = std::any::TypeId::of::<T>();

        if let Some(type_erased_events) = self.events_by_type.get(&type_id) {
            type_erased_events
                .iter()
                .filter_map(|te| {
                    te.event
                        .downcast_ref::<T>()
                        .map(|event| MessageWrapper::new(event.clone(), te.metadata.clone()))
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get the number of events of a specific type
    pub fn count_events<T: 'static>(&self) -> usize {
        let type_id = std::any::TypeId::of::<T>();
        self.events_by_type
            .get(&type_id)
            .map(|v| v.len())
            .unwrap_or(0)
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Instant;

    #[test]
    fn test_key_parsing() {
        let mut metadata =
            MessageMetadata::new("test_topic".to_string(), Instant::now(), None, None);

        assert_eq!(metadata.key_as_string(), None);
        assert_eq!(metadata.key_display(), None);

        metadata.key = Some("user_123".to_string());
        assert_eq!(metadata.key_as_string(), Some("user_123".to_string()));
        assert_eq!(metadata.key_display(), Some("user_123".to_string()));

        metadata.key = Some("world_456".to_string());
        assert_eq!(metadata.key_as_string(), Some("world_456".to_string()));
        assert_eq!(metadata.key_display(), Some("world_456".to_string()));

        let kafka_metadata = KafkaMetadata {
            topic: "test_topic".to_string(),
            partition: 0,
            offset: 42,
            consumer_group: Some("group_a".to_string()),
            manual_commit: false,
            headers: HashMap::new(),
        };
        metadata.backend_specific = Some(Box::new(kafka_metadata));

        if let Some(kafka_meta) = metadata.kafka_metadata() {
            assert_eq!(kafka_meta.topic, "test_topic");
            assert_eq!(kafka_meta.partition, 0);
            assert_eq!(kafka_meta.offset, 42);
        } else {
            panic!("Expected Kafka metadata");
        }

        let mut headers = HashMap::new();
        headers.insert("correlation_id".to_string(), "abc-123".to_string());
        headers.insert("source_service".to_string(), "world_simulator".to_string());

        let metadata_with_headers = MessageMetadata::new(
            "events_topic".to_string(),
            Instant::now(),
            Some("event_key_789".to_string()),
            Some(Box::new(KafkaMetadata {
                topic: "events_topic".to_string(),
                partition: 1,
                offset: 456,
                consumer_group: Some("group_b".to_string()),
                manual_commit: true,
                headers: headers.clone(),
            })),
        );

        assert_eq!(metadata_with_headers.source, "events_topic");
        assert_eq!(
            metadata_with_headers.key_as_string(),
            Some("event_key_789".to_string())
        );
        let kafka_headers = metadata_with_headers
            .kafka_metadata()
            .expect("expected kafka metadata");
        assert_eq!(
            kafka_headers.headers.get("correlation_id"),
            Some(&"abc-123".to_string())
        );
        assert_eq!(
            kafka_headers.headers.get("source_service"),
            Some(&"world_simulator".to_string())
        );
    }

    #[test]
    fn test_backend_metadata_downcasting() {
        let kafka_meta = KafkaMetadata {
            topic: "kafka_topic".to_string(),
            partition: 1,
            offset: 100,
            consumer_group: Some("group_c".to_string()),
            manual_commit: true,
            headers: HashMap::new(),
        };

        let metadata = MessageMetadata::new(
            "kafka_topic".to_string(),
            Instant::now(),
            Some("partition_key".to_string()),
            Some(Box::new(kafka_meta)),
        );

        assert!(metadata.kafka_metadata().is_some());
        if let Some(k_meta) = metadata.kafka_metadata() {
            assert_eq!(k_meta.topic, "kafka_topic");
            assert_eq!(k_meta.partition, 1);
            assert_eq!(k_meta.offset, 100);
        }

        let redis_meta = RedisMetadata {
            stream: "redis_stream".to_string(),
            entry_id: "1-0".to_string(),
            consumer_group: Some("redis_group".to_string()),
            manual_ack: true,
        };

        let metadata = MessageMetadata::new(
            "redis_stream".to_string(),
            Instant::now(),
            None,
            Some(Box::new(redis_meta)),
        );

        if let Some(r_meta) = metadata.redis_metadata() {
            assert_eq!(r_meta.stream, "redis_stream");
            assert_eq!(r_meta.entry_id, "1-0");
            assert_eq!(r_meta.consumer_group.as_deref(), Some("redis_group"));
            assert!(r_meta.manual_ack);
        } else {
            panic!("Expected Redis metadata");
        }
    }
}
