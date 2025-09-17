use crate::{BusEvent, EventBusError};
use async_trait::async_trait;
use std::collections::HashMap;
use std::fmt::Debug;

/// Trait defining the common interface for event bus backends
#[async_trait]
pub trait EventBusBackend: Send + Sync + 'static + Debug {
    fn clone_box(&self) -> Box<dyn EventBusBackend>;
    fn as_any(&self) -> &dyn std::any::Any;
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any;
    async fn connect(&mut self) -> Result<(), EventBusError>;
    async fn disconnect(&mut self) -> Result<(), EventBusError>;
    /// Non-blocking send that queues the event for delivery
    fn try_send_serialized(&self, event_json: &[u8], topic: &str) -> Result<(), EventBusError>;
    /// Non-blocking send with headers that queues the event for delivery
    fn try_send_serialized_with_headers(&self, event_json: &[u8], topic: &str, headers: &HashMap<String, String>) -> Result<(), EventBusError>;
    async fn receive_serialized(&self, topic: &str) -> Result<Vec<Vec<u8>>, EventBusError>;
    async fn subscribe(&mut self, topic: &str) -> Result<(), EventBusError>;
    async fn unsubscribe(&mut self, topic: &str) -> Result<(), EventBusError>;
}

// Extension methods for the EventBusBackend trait
#[async_trait]
pub trait EventBusBackendExt: EventBusBackend {
    /// Non-blocking send that queues the event for delivery
    fn try_send<T: BusEvent>(&self, event: &T, topic: &str) -> Result<(), EventBusError> {
        let serialized =
            serde_json::to_vec(event).map_err(|e| EventBusError::Serialization(e.to_string()))?;
        self.try_send_serialized(&serialized, topic)
    }
    /// Non-blocking send with headers that queues the event for delivery
    fn try_send_with_headers<T: BusEvent>(&self, event: &T, topic: &str, headers: &HashMap<String, String>) -> Result<(), EventBusError> {
        let serialized =
            serde_json::to_vec(event).map_err(|e| EventBusError::Serialization(e.to_string()))?;
        self.try_send_serialized_with_headers(&serialized, topic, headers)
    }
    async fn receive<T: BusEvent>(&self, topic: &str) -> Result<Vec<T>, EventBusError> {
        let serialized_messages = self.receive_serialized(topic).await?;
        let mut result = Vec::with_capacity(serialized_messages.len());
        for message in serialized_messages {
            match serde_json::from_slice(&message) {
                Ok(deserialized) => result.push(deserialized),
                Err(e) => return Err(EventBusError::Serialization(e.to_string())),
            }
        }
        Ok(result)
    }
}

// Implement EventBusBackendExt for all EventBusBackend implementors
impl<T: EventBusBackend + ?Sized> EventBusBackendExt for T {}
