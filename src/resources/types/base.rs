use bevy::prelude::*;
use std::time::Instant;

use crate::BusMessage;
use crate::resources::backend_metadata::{BackendMetadata, MessageMetadata};
#[cfg(test)]
use crate::resources::backend_metadata::{KafkaMetadata, RedisMetadata};
#[cfg(test)]
use std::collections::HashMap;

/// Raw incoming message captured by background consumer task.
#[derive(Debug, Clone)]
pub struct IncomingMessage {
    pub source: String,
    pub payload: Vec<u8>,
    pub key: Option<String>,
    pub timestamp: Instant,
    pub backend_metadata: Option<Box<dyn BackendMetadata>>,
}

impl MessageMetadata {
    pub fn key_as_string(&self) -> Option<String> {
        self.key.clone()
    }

    pub fn key_display(&self) -> Option<String> {
        self.key.clone()
    }
}

impl IncomingMessage {
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
#[derive(Debug, Clone)]
pub struct MessageWrapper<T: BusMessage> {
    event: T,
    metadata: MessageMetadata,
}

impl<T: BusMessage> MessageWrapper<T> {
    pub fn new(event: T, metadata: MessageMetadata) -> Self {
        Self { event, metadata }
    }

    pub fn event(&self) -> &T {
        &self.event
    }

    pub fn metadata(&self) -> &MessageMetadata {
        &self.metadata
    }

    pub fn into_event(self) -> T {
        self.event
    }

    pub fn into_parts(self) -> (T, MessageMetadata) {
        (self.event, self.metadata)
    }
}

impl<T: BusMessage> std::ops::Deref for MessageWrapper<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}

/// Pre-processed message with payload and metadata already converted for efficient reading.
#[derive(Clone, Debug)]
pub struct ProcessedMessage {
    pub payload: Vec<u8>,
    pub metadata: MessageMetadata,
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
