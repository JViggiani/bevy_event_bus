use crate::BusEvent;
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;

/// Trait defining the common interface for event bus backends
/// 
/// Backends should be simple and never panic. Connection/send failures
/// are handled gracefully by returning success/failure indicators.
#[async_trait]
pub trait EventBusBackend: Send + Sync + 'static + Debug {
    fn clone_box(&self) -> Box<dyn EventBusBackend>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    
    /// Connect to the backend. Returns true if successful.
    async fn connect(&mut self) -> bool;
    
    /// Disconnect from the backend. Returns true if successful.
    async fn disconnect(&mut self) -> bool;
    
    /// Non-blocking send that queues the event for delivery.
    /// Returns true if the event was queued successfully, false if it failed.
    fn try_send_serialized(&self, event_json: &[u8], topic: &str) -> bool;
    
    /// Non-blocking send with headers that queues the event for delivery.
    /// Returns true if the event was queued successfully, false if it failed.
    fn try_send_serialized_with_headers(&self, event_json: &[u8], topic: &str, headers: &HashMap<String, String>) -> bool;
    
    /// Receive serialized messages from a topic.
    /// Returns empty vec if no messages or on error.
    async fn receive_serialized(&self, topic: &str) -> Vec<Vec<u8>>;
    
    /// Subscribe to a topic. Returns true if successful.
    async fn subscribe(&mut self, topic: &str) -> bool;
    
    /// Unsubscribe from a topic. Returns true if successful.
    async fn unsubscribe(&mut self, topic: &str) -> bool;
}

// Extension methods for the EventBusBackend trait
#[async_trait]
pub trait EventBusBackendExt: EventBusBackend {
    /// Non-blocking send that queues the event for delivery.
    /// Returns true if the event was serialized and queued successfully.
    fn try_send<T: BusEvent>(&self, event: &T, topic: &str) -> bool {
        match serde_json::to_vec(event) {
            Ok(serialized) => self.try_send_serialized(&serialized, topic),
            Err(_) => false, // Serialization failed
        }
    }
    
    /// Non-blocking send with headers that queues the event for delivery.
    /// Returns true if the event was serialized and queued successfully.
    fn try_send_with_headers<T: BusEvent>(&self, event: &T, topic: &str, headers: &HashMap<String, String>) -> bool {
        match serde_json::to_vec(event) {
            Ok(serialized) => self.try_send_serialized_with_headers(&serialized, topic, headers),
            Err(_) => false, // Serialization failed
        }
    }
    
    /// Receive and deserialize messages from a topic.
    /// Returns empty vec if no messages or on error.
    async fn receive<T: BusEvent>(&self, topic: &str) -> Vec<T> {
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
}

// Implement EventBusBackendExt for all EventBusBackend implementors
impl<T: EventBusBackend + ?Sized> EventBusBackendExt for T {}
