use async_trait::async_trait;
use std::fmt::Debug;
use crate::BusEvent;

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
    
    /// Non-blocking send that queues the event for delivery using producer configuration.
    /// Returns true if the event was queued successfully, false if it failed.
    fn try_send_serialized(&self, event_json: &[u8], config: &dyn std::any::Any) -> bool;
    
    /// Poll and deserialize messages using configuration - polls with 0ms timeout
    /// Returns messages immediately without background systems or queues
    /// Config specifies topics, consumer group, and other settings.
    /// Populates the provided vector with deserialized messages.
    /// IMPORTANT: Consumer groups must be created explicitly before calling this method.
    /// Returns Ok(()) on success or Err(String) on error.
    async fn poll_messages(&self, config: &crate::config::kafka::KafkaReadConfig, messages: &mut Vec<Vec<u8>>) -> Result<(), String>;
    
    /// Flush all pending messages (synchronous delivery)
    /// 
    /// This is a BLOCKING operation that should only be called during shutdown
    /// or other situations where blocking is acceptable. Never call this from
    /// the Bevy update loop or any performance-critical path.
    /// 
    /// # Arguments
    /// * `timeout` - Maximum time to wait for all messages to be delivered
    /// 
    /// # Returns
    /// * `true` if all messages were successfully flushed
    /// * `false` if timeout occurred or other error happened
    fn flush(&self, timeout: std::time::Duration) -> bool;
}

/// Helper methods for working with EventBusBackend trait objects
impl dyn EventBusBackend {
    /// Non-blocking send that queues the event for delivery using producer configuration.
    /// Returns true if the event was serialized and queued successfully.
    pub fn try_send<T: BusEvent, C: crate::config::EventBusConfig + 'static>(
        &self, 
        event: &T, 
        config: &C
    ) -> bool {
        match serde_json::to_vec(event) {
            Ok(serialized) => self.try_send_serialized(&serialized, config as &dyn std::any::Any),
            Err(_) => false, // Serialization failed
        }
    }

    /// Helper method for receiving events with automatic deserialization
    /// This combines poll_messages and deserialization into a single convenient method
    pub async fn receive<T: BusEvent>(&self, config: &crate::config::kafka::KafkaReadConfig, messages: &mut Vec<T>) -> Result<(), String> {
        let mut raw_messages = Vec::new();
        self.poll_messages(config, &mut raw_messages).await?;
        
        for raw_payload in raw_messages {
            match serde_json::from_slice::<T>(&raw_payload) {
                Ok(event) => messages.push(event),
                Err(e) => tracing::warn!("Failed to deserialize event: {}", e),
            }
        }
        
        Ok(())
    }
}
