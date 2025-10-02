use async_trait::async_trait;
use bevy::prelude::App;
use bevy_event_bus::BusEvent;
use std::any::Any;
use std::collections::HashMap;
use std::error::Error;
use std::fmt::{Debug, Display};

/// Error returned when a backend configuration cannot be applied.
#[derive(Debug, Clone)]
pub struct BackendConfigError {
    backend: &'static str,
    reason: String,
}

impl BackendConfigError {
    pub fn new(backend: &'static str, reason: impl Into<String>) -> Self {
        Self {
            backend,
            reason: reason.into(),
        }
    }

    pub fn backend(&self) -> &'static str {
        self.backend
    }

    pub fn reason(&self) -> &str {
        &self.reason
    }
}

impl Display for BackendConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Backend '{}' configuration error: {}",
            self.backend, self.reason
        )
    }
}

impl Error for BackendConfigError {}

/// Trait implemented by backend-specific configuration objects to enable dynamic dispatch.
pub trait EventBusBackendConfig: Send + Sync + 'static {
    fn as_any(&self) -> &dyn Any;
}

/// Trait defining the common interface for event bus backends
///
/// Backends should be simple and never panic. Connection/send failures
/// are handled gracefully by returning success/failure indicators.
#[async_trait]
pub trait EventBusBackend: Send + Sync + 'static + Debug {
    fn clone_box(&self) -> Box<dyn EventBusBackend>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;

    /// Apply backend-specific configuration prior to connecting.
    fn configure(&mut self, _config: &dyn EventBusBackendConfig) -> Result<(), BackendConfigError> {
        let _ = _config;
        Ok(())
    }

    /// Apply topology-defined event registrations to the provided Bevy `App`.
    fn apply_event_bindings(&self, _app: &mut App) {}

    /// Connect to the backend. Returns true if successful.
    async fn connect(&mut self) -> bool;

    /// Disconnect from the backend. Returns true if successful.
    async fn disconnect(&mut self) -> bool;

    /// Non-blocking send that queues the event for delivery.
    /// Returns true if the event was queued successfully, false if it failed.
    fn try_send_serialized(&self, event_json: &[u8], topic: &str) -> bool;

    /// Non-blocking send with headers that queues the event for delivery.
    /// Returns true if the event was queued successfully, false if it failed.
    fn try_send_serialized_with_headers(
        &self,
        event_json: &[u8],
        topic: &str,
        headers: &HashMap<String, String>,
    ) -> bool;

    /// Receive serialized messages from a topic.
    /// Returns empty vec if no messages or on error.
    async fn receive_serialized(&self, topic: &str) -> Vec<Vec<u8>>;

    /// Subscribe to a topic. Returns true if successful.
    async fn subscribe(&mut self, topic: &str) -> bool;

    /// Unsubscribe from a topic. Returns true if successful.
    async fn unsubscribe(&mut self, topic: &str) -> bool;

    /// Send with partition key for message ordering (Kafka-specific, optional for others)
    fn try_send_serialized_with_partition_key(
        &self,
        event_json: &[u8],
        topic: &str,
        key: &str,
    ) -> bool {
        // Default implementation falls back to regular send
        let _ = key; // Suppress unused parameter warning
        self.try_send_serialized(event_json, topic)
    }

    /// Send with both partition key and headers
    fn try_send_serialized_with_key_and_headers(
        &self,
        event_json: &[u8],
        topic: &str,
        key: &str,
        headers: &HashMap<String, String>,
    ) -> bool {
        // Default implementation falls back to headers only
        let _ = key; // Suppress unused parameter warning
        self.try_send_serialized_with_headers(event_json, topic, headers)
    }

    /// Create a consumer group with specific configuration
    async fn create_consumer_group(
        &mut self,
        topics: &[String],
        group_id: &str,
    ) -> Result<(), String> {
        let _ = (topics, group_id); // Suppress unused parameter warnings
        Ok(()) // Default no-op for backends that don't support multiple consumer groups
    }

    /// Consume from specific consumer group
    async fn receive_serialized_with_group(&self, topic: &str, group_id: &str) -> Vec<Vec<u8>> {
        let _ = group_id; // Suppress unused parameter warning
        // Default implementation falls back to regular receive
        self.receive_serialized(topic).await
    }

    /// Enable manual offset commits for reliable processing
    async fn enable_manual_commits(&mut self, group_id: &str) -> Result<(), String> {
        let _ = group_id; // Suppress unused parameter warning
        Ok(()) // Default no-op for backends that don't support manual commits
    }

    /// Manually commit a specific message offset
    async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) -> Result<(), String> {
        let _ = (topic, partition, offset); // Suppress unused parameter warnings
        Ok(()) // Default no-op
    }

    /// Get consumer lag for monitoring
    async fn get_consumer_lag(&self, topic: &str, group_id: &str) -> Result<i64, String> {
        let _ = (topic, group_id); // Suppress unused parameter warnings
        Ok(0) // Default no lag
    }

    /// Flush all pending messages (producer-side)
    async fn flush(&self) -> Result<(), String> {
        Ok(()) // Default no-op
    }
}

/// Helper methods for working with EventBusBackend trait objects
impl dyn EventBusBackend {
    /// Non-blocking send that queues the event for delivery.
    /// Returns true if the event was serialized and queued successfully.
    pub fn try_send<T: BusEvent>(&self, event: &T, topic: &str) -> bool {
        match serde_json::to_vec(event) {
            Ok(serialized) => self.try_send_serialized(&serialized, topic),
            Err(_) => false, // Serialization failed
        }
    }

    /// Non-blocking send with headers that queues the event for delivery.
    /// Returns true if the event was serialized and queued successfully.
    pub fn try_send_with_headers<T: BusEvent>(
        &self,
        event: &T,
        topic: &str,
        headers: &HashMap<String, String>,
    ) -> bool {
        match serde_json::to_vec(event) {
            Ok(serialized) => self.try_send_serialized_with_headers(&serialized, topic, headers),
            Err(_) => false, // Serialization failed
        }
    }

    /// Send with partition key for message ordering
    pub fn try_send_with_partition_key<T: BusEvent>(
        &self,
        event: &T,
        topic: &str,
        key: &str,
    ) -> bool {
        match serde_json::to_vec(event) {
            Ok(serialized) => self.try_send_serialized_with_partition_key(&serialized, topic, key),
            Err(_) => false, // Serialization failed
        }
    }

    /// Send with both partition key and headers
    pub fn try_send_with_key_and_headers<T: BusEvent>(
        &self,
        event: &T,
        topic: &str,
        key: &str,
        headers: &HashMap<String, String>,
    ) -> bool {
        match serde_json::to_vec(event) {
            Ok(serialized) => {
                self.try_send_serialized_with_key_and_headers(&serialized, topic, key, headers)
            }
            Err(_) => false, // Serialization failed
        }
    }

    /// Receive and deserialize messages from a topic.
    /// Returns empty vec if no messages or on error.
    pub async fn receive<T: BusEvent>(&self, topic: &str) -> Vec<T> {
        let serialized_messages = self.receive_serialized(topic).await;
        let mut result = Vec::with_capacity(serialized_messages.len());
        for message in serialized_messages {
            if let Ok(deserialized) = serde_json::from_slice(&message) {
                result.push(deserialized);
            }
            // Silently skip messages that fail to deserialize
        }
        result
    }

    /// Receive and deserialize messages from a topic with specific consumer group
    pub async fn receive_with_group<T: BusEvent>(&self, topic: &str, group_id: &str) -> Vec<T> {
        let serialized_messages = self.receive_serialized_with_group(topic, group_id).await;
        let mut result = Vec::with_capacity(serialized_messages.len());
        for message in serialized_messages {
            if let Ok(deserialized) = serde_json::from_slice(&message) {
                result.push(deserialized);
            }
            // Silently skip messages that fail to deserialize
        }
        result
    }
}
