use bevy_event_bus::EventBusBackend;
use bevy_event_bus::config::kafka::{KafkaReadConfig, KafkaWriteConfig};
use async_trait::async_trait;

use std::fmt::Debug;
use std::sync::{Arc, Mutex};

/// Mock backend for testing
#[derive(Debug, Clone)]
pub struct MockEventBusBackend {
    /// Simulate delivery failures for specific topics
    pub(crate) fail_topics: Arc<Mutex<Vec<String>>>,
}

impl MockEventBusBackend {
    pub fn new() -> Self {
        Self {
            fail_topics: Arc::new(Mutex::new(Vec::new())),
        }
    }
    
    /// Configure the mock to simulate delivery failures for a specific topic
    pub fn simulate_delivery_failure_for_topic(&mut self, topic: &str) {
        let mut fail_topics = self.fail_topics.lock().unwrap();
        fail_topics.push(topic.to_string());
    }
    
    /// Check if this topic should fail
    fn should_fail(&self, topic: &str) -> bool {
        let fail_topics = self.fail_topics.lock().unwrap();
        fail_topics.contains(&topic.to_string())
    }
}

#[async_trait]
impl EventBusBackend for MockEventBusBackend {
    fn clone_box(&self) -> Box<dyn EventBusBackend> {
        Box::new(self.clone())
    }
    
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
    
    async fn connect(&mut self) -> bool {
        true
    }
    
    fn try_send_serialized(&self, _event_json: &[u8], config: &dyn std::any::Any) -> bool {
        // Try to downcast config to extract topic for failure simulation
        if let Some(kafka_config) = config.downcast_ref::<KafkaWriteConfig>() {
            // Check if the topic in this config should fail
            if self.should_fail(&kafka_config.topic) {
                return false; // Simulate failure
            }
        }
        true // Success by default
    }
    
    async fn poll_messages(&self, _config: &KafkaReadConfig, _messages: &mut Vec<Vec<u8>>) -> Result<(), String> {
        // Mock backend doesn't actually store/receive messages for this test
        Ok(())
    }
    
    fn flush(&self, _timeout: std::time::Duration) -> bool {
        // Mock backend always succeeds flush
        true
    }
}