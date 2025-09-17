use bevy::prelude::*;
use crossbeam_channel::Receiver;
use std::collections::HashMap;
use std::time::Instant;
use crate::BusEvent;

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
}

/// Metadata associated with an event received from an external message broker
#[derive(Debug, Clone)]
pub struct EventMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Instant,
    pub headers: HashMap<String, String>,
    pub key: Option<Vec<u8>>,
}

impl EventMetadata {
    /// Get the key as a UTF-8 string if possible
    pub fn key_as_string(&self) -> Option<String> {
        self.key.as_ref().and_then(|k| String::from_utf8(k.clone()).ok())
    }
    
    /// Get the key as a lossy UTF-8 string, replacing invalid sequences with replacement characters
    pub fn key_as_string_lossy(&self) -> Option<String> {
        self.key.as_ref().map(|k| String::from_utf8_lossy(k).into_owned())
    }
    
    /// Get the key as a hex string for debugging/logging non-UTF-8 keys
    pub fn key_as_hex(&self) -> Option<String> {
        self.key.as_ref().map(|k| {
            k.iter().map(|b| format!("{:02x}", b)).collect::<Vec<_>>().join("")
        })
    }
    
    /// Get a human-readable representation of the key, trying UTF-8 first, then hex fallback
    pub fn key_display(&self) -> Option<String> {
        if let Some(utf8_key) = self.key_as_string() {
            Some(utf8_key)
        } else {
            self.key_as_hex().map(|hex| format!("0x{}", hex))
        }
    }
}

/// Enhanced event wrapper that includes both the event data and metadata
/// 
/// This wrapper implements `Deref<Target = T>` so you can access the inner event
/// fields directly while having metadata available when needed.
#[derive(Debug, Clone)]
pub struct ExternalEvent<T: BusEvent> {
    pub event: T,
    pub metadata: EventMetadata,
}

impl<T: BusEvent> std::ops::Deref for ExternalEvent<T> {
    type Target = T;
    
    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

impl<T: BusEvent> ExternalEvent<T> {
    /// Create a new external event with metadata
    pub fn new(event: T, metadata: EventMetadata) -> Self {
        Self { event, metadata }
    }
    
    /// Access the metadata for this event
    pub fn metadata(&self) -> &EventMetadata {
        &self.metadata
    }
    
    /// Extract the inner event, consuming the wrapper
    pub fn into_event(self) -> T {
        self.event
    }
    
    /// Get a reference to the inner event
    pub fn event(&self) -> &T {
        &self.event
    }
}

/// Outbound message queued for delivery to external broker
#[derive(Debug, Clone)]
pub struct OutboundMessage {
    pub topic: String,
    pub payload: Vec<u8>,
    /// Unique ID for tracking delivery success/failure
    pub delivery_id: u64,
}

/// Events fired when messages are successfully delivered or fail
#[derive(Event, Debug, Clone)]
pub enum DeliveryEvent {
    Success { delivery_id: u64, topic: String },
    Failed { delivery_id: u64, topic: String, error: String },
}

/// Queue of outbound messages waiting to be sent
#[derive(Resource, Debug)]
pub struct OutboundMessageQueue {
    pub messages: std::sync::Mutex<std::collections::VecDeque<OutboundMessage>>,
    pub next_delivery_id: std::sync::atomic::AtomicU64,
}

impl Default for OutboundMessageQueue {
    fn default() -> Self {
        Self {
            messages: std::sync::Mutex::new(std::collections::VecDeque::new()),
            next_delivery_id: std::sync::atomic::AtomicU64::new(1),
        }
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
    use std::time::Instant;

    #[test]
    fn test_key_parsing() {
        let mut metadata = EventMetadata {
            topic: "test".to_string(),
            partition: 0,
            offset: 0,
            timestamp: Instant::now(),
            headers: std::collections::HashMap::new(),
            key: None,
        };

        // Test None key
        assert_eq!(metadata.key_as_string(), None);
        assert_eq!(metadata.key_as_string_lossy(), None);
        assert_eq!(metadata.key_as_hex(), None);
        assert_eq!(metadata.key_display(), None);

        // Test valid UTF-8 key
        metadata.key = Some(b"hello".to_vec());
        assert_eq!(metadata.key_as_string(), Some("hello".to_string()));
        assert_eq!(metadata.key_as_string_lossy(), Some("hello".to_string()));
        assert_eq!(metadata.key_as_hex(), Some("68656c6c6f".to_string()));
        assert_eq!(metadata.key_display(), Some("hello".to_string()));

        // Test invalid UTF-8 key (contains invalid byte sequence)
        metadata.key = Some(vec![0xFF, 0xFE, 0xFD]);
        assert_eq!(metadata.key_as_string(), None); // Should fail UTF-8 validation
        assert!(metadata.key_as_string_lossy().is_some()); // Should work with replacement chars
        assert_eq!(metadata.key_as_hex(), Some("fffefd".to_string()));
        assert_eq!(metadata.key_display(), Some("0xfffefd".to_string())); // Should fallback to hex

        // Test mixed UTF-8 and invalid bytes
        metadata.key = Some(vec![b'h', b'e', 0xFF, b'o']);
        assert_eq!(metadata.key_as_string(), None); // Should fail UTF-8 validation
        let lossy = metadata.key_as_string_lossy().unwrap();
        assert!(lossy.contains('h') && lossy.contains('e') && lossy.contains('o')); // Should preserve valid chars
        assert_eq!(metadata.key_as_hex(), Some("6865ff6f".to_string()));
        assert_eq!(metadata.key_display(), Some("0x6865ff6f".to_string())); // Should fallback to hex
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
    pub fn get_events<T: BusEvent>(&self) -> Vec<ExternalEvent<T>> {
        let type_id = std::any::TypeId::of::<T>();
        
        if let Some(type_erased_events) = self.events_by_type.get(&type_id) {
            type_erased_events
                .iter()
                .filter_map(|te| {
                    te.event
                        .downcast_ref::<T>()
                        .map(|event| ExternalEvent::new(event.clone(), te.metadata.clone()))
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
