use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use async_trait::async_trait;
use bevy_event_bus::{EventBusBackend, EventBusError};

#[derive(Clone, Default, Debug)]
pub struct MockBackendInner {
    pub topics: HashMap<String, VecDeque<Vec<u8>>>,
    pub subs: Vec<String>,
}

#[derive(Clone, Default, Debug)]
pub struct MockEventBusBackend(Arc<Mutex<MockBackendInner>>);

impl MockEventBusBackend {
    pub fn new() -> Self { Self::default() }
}

#[async_trait]
impl EventBusBackend for MockEventBusBackend {
    fn clone_box(&self) -> Box<dyn EventBusBackend> { Box::new(self.clone()) }

    async fn connect(&mut self) -> Result<(), EventBusError> { Ok(()) }
    async fn disconnect(&mut self) -> Result<(), EventBusError> { Ok(()) }
    async fn send_serialized(&self, event_json: &[u8], topic: &str) -> Result<(), EventBusError> {
        let mut guard = self.0.lock().unwrap();
        let q = guard.topics.entry(topic.to_string()).or_default();
        q.push_back(event_json.to_vec());
        Ok(())
    }
    async fn receive_serialized(&self, topic: &str) -> Result<Vec<Vec<u8>>, EventBusError> {
        let mut guard = self.0.lock().unwrap();
        let mut out = Vec::new();
        if let Some(q) = guard.topics.get_mut(topic) { while let Some(v) = q.pop_front() { out.push(v); } }
        Ok(out)
    }
    async fn subscribe(&mut self, topic: &str) -> Result<(), EventBusError> {
        let mut g = self.0.lock().unwrap();
        if !g.subs.contains(&topic.to_string()) { g.subs.push(topic.to_string()); }
        Ok(())
    }
    async fn unsubscribe(&mut self, topic: &str) -> Result<(), EventBusError> {
        let mut g = self.0.lock().unwrap();
        g.subs.retain(|t| t != topic);
        Ok(())
    }
}
