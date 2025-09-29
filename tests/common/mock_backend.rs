use async_trait::async_trait;
use bevy_event_bus::EventBusBackend;
use std::collections::HashMap;
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

    async fn disconnect(&mut self) -> bool {
        true
    }

    fn try_send_serialized(&self, _event_json: &[u8], topic: &str) -> bool {
        // Check if we should simulate a failure for this topic
        if self.should_fail(topic) {
            return false; // Simulate failure
        }

        // Return true to simulate successful queueing
        true
    }

    fn try_send_serialized_with_headers(
        &self,
        event_json: &[u8],
        topic: &str,
        _headers: &HashMap<String, String>,
    ) -> bool {
        // Delegate to the main send method
        self.try_send_serialized(event_json, topic)
    }

    async fn receive_serialized(&self, _topic: &str) -> Vec<Vec<u8>> {
        // Mock backend doesn't actually store/receive messages for this test
        Vec::new()
    }

    async fn subscribe(&mut self, _topic: &str) -> bool {
        true
    }

    async fn unsubscribe(&mut self, _topic: &str) -> bool {
        true
    }
}
