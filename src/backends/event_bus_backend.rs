use std::fmt::Debug;

use crate::{BusEvent, EventBusError};

/// Trait defining the common interface for event bus backends
pub trait EventBusBackend: Send + Sync + 'static + Debug {
    /// Create a new instance of this backend
    fn clone_box(&self) -> Box<dyn EventBusBackend>;
    
    /// Connect to the event broker
    fn connect(&mut self) -> Result<(), EventBusError>;
    
    /// Disconnect from the event broker
    fn disconnect(&mut self) -> Result<(), EventBusError>;
    
    /// Send a message to the specified topic using type erasure
    fn send_serialized(&self, event_json: &[u8], topic: &str) -> Result<(), EventBusError>;
    
    /// Receive messages from the specified topic as raw bytes
    fn receive_serialized(&self, topic: &str) -> Result<Vec<Vec<u8>>, EventBusError>;
    
    /// Subscribe to the specified topic
    fn subscribe(&mut self, topic: &str) -> Result<(), EventBusError>;
    
    /// Unsubscribe from the specified topic
    fn unsubscribe(&mut self, topic: &str) -> Result<(), EventBusError>;
}

// Extension methods for the EventBusBackend trait
pub trait EventBusBackendExt: EventBusBackend {
    /// Send a message to the specified topic
    fn send<T: BusEvent>(&self, event: &T, topic: &str) -> Result<(), EventBusError> {
        let serialized = serde_json::to_vec(event)
            .map_err(|e| EventBusError::Serialization(e.to_string()))?;
        self.send_serialized(&serialized, topic)
    }
    
    /// Receive messages from the specified topic
    fn receive<T: BusEvent>(&self, topic: &str) -> Result<Vec<T>, EventBusError> {
        let serialized_messages = self.receive_serialized(topic)?;
        
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
