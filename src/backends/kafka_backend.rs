use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, BaseConsumer},
    producer::{BaseProducer, BaseRecord, Producer},
    message::Message,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, error};
use std::fmt::Debug;

use crate::{EventBusBackend, EventBusError};
use async_trait::async_trait;

/// Configuration for Kafka backend
#[derive(Clone, Debug)]
pub struct KafkaConfig {
    /// List of Kafka bootstrap servers
    pub bootstrap_servers: String,
    /// Group ID for consumer
    pub group_id: String,
    /// Client ID for Kafka
    pub client_id: Option<String>,
    /// Connection timeout
    pub timeout_ms: i32,
    /// Additional Kafka configuration
    pub additional_config: HashMap<String, String>,
}

impl Default for KafkaConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            group_id: "bevy_event_bus".to_string(),
            client_id: None,
            timeout_ms: 10000,
            additional_config: HashMap::new(),
        }
    }
}

/// Kafka implementation of the EventBusBackend
pub struct KafkaEventBusBackend {
    config: KafkaConfig,
    producer: Arc<BaseProducer>,
    consumer: Arc<BaseConsumer>,
    subscriptions: Arc<Mutex<HashSet<String>>>,
}

impl std::fmt::Debug for KafkaEventBusBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaEventBusBackend")
            .field("config", &self.config)
            .field("subscriptions_count", &self.subscriptions.lock().unwrap().len())
            .finish()
    }
}

impl KafkaEventBusBackend {
    pub fn new(config: KafkaConfig) -> Self {
        // Configure producer
        let mut producer_config = ClientConfig::new();
        producer_config.set("bootstrap.servers", &config.bootstrap_servers);
        producer_config.set("message.timeout.ms", config.timeout_ms.to_string());
        
        if let Some(client_id) = &config.client_id {
            producer_config.set("client.id", client_id);
        }
        
        // Add any additional configuration
        for (key, value) in &config.additional_config {
            producer_config.set(key, value);
        }
        
    let producer: BaseProducer = producer_config
            .create()
            .expect("Failed to create Kafka producer");
        
        // Configure consumer
        let mut consumer_config = ClientConfig::new();
        consumer_config.set("bootstrap.servers", &config.bootstrap_servers);
        consumer_config.set("group.id", &config.group_id);
        consumer_config.set("enable.auto.commit", "true");
        consumer_config.set("auto.offset.reset", "earliest");
        consumer_config.set("session.timeout.ms", "6000");
        
        if let Some(client_id) = &config.client_id {
            consumer_config.set("client.id", client_id);
        }
        
        // Add any additional configuration
        for (key, value) in &config.additional_config {
            consumer_config.set(key, value);
        }
        
    let consumer: BaseConsumer = consumer_config
            .create()
            .expect("Failed to create Kafka consumer");
        
        Self {
            config,
            producer: Arc::new(producer),
            consumer: Arc::new(consumer),
            subscriptions: Arc::new(Mutex::new(HashSet::new())),
        }
    }
}

impl Clone for KafkaEventBusBackend {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            producer: self.producer.clone(),
            consumer: self.consumer.clone(),
            subscriptions: self.subscriptions.clone(),
        }
    }
}

#[async_trait]
impl EventBusBackend for KafkaEventBusBackend {
    fn clone_box(&self) -> Box<dyn EventBusBackend> { Box::new(self.clone()) }
    async fn connect(&mut self) -> Result<(), EventBusError> {
        info!("Initializing Kafka backend (lazy connect) for {}", self.config.bootstrap_servers);
        // rdkafka lazily connects on first use; we simply attempt a lightweight operation by fetching metadata, but
        // ignore errors for now to allow tests to spin up the container concurrently.
        match self.producer.client().fetch_metadata(None, Duration::from_millis(500)) {
            Ok(md) => info!("Kafka metadata brokers={}", md.brokers().len()),
            Err(e) => info!("Kafka metadata fetch deferred: {}", e),
        }
        // Re-subscribe to any existing topics
        let existing = self.subscriptions.lock().unwrap().clone();
        if !existing.is_empty() {
            let topics: Vec<&str> = existing.iter().map(|s| s.as_str()).collect();
            if let Err(e) = self.consumer.subscribe(&topics) {
                return Err(EventBusError::Connection(format!("Failed to subscribe to topics: {}", e)));
            }
        }
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<(), EventBusError> {
        info!("Disconnecting from Kafka");
        // Nothing special needed for disconnection, Kafka clients clean up on drop
        Ok(())
    }
    async fn send_serialized(&self, event_json: &[u8], topic: &str) -> Result<(), EventBusError> {
        let record: BaseRecord<'_, (), [u8]> = BaseRecord::to(topic).payload(event_json);
        if let Err((e, _)) = self.producer.send(record) {
            return Err(EventBusError::Other(format!("Failed to enqueue message: {}", e)));
        }
        // Fast-path flush: poll a few short times instead of full timeout_ms.
        for _ in 0..20 { // ~200ms total
            self.producer.poll(Duration::from_millis(10));
        }
        Ok(())
    }
    async fn receive_serialized(&self, topic: &str) -> Result<Vec<Vec<u8>>, EventBusError> {
        // Auto-subscribe if not already
        let mut need_subscribe = false;
        {
            let subs = self.subscriptions.lock().unwrap();
            if !subs.contains(topic) { need_subscribe = true; }
        }
        if need_subscribe {
            // We need a mutable consumer subscribe; safe because subscribe only adds topic list
            let mut subs_guard = self.subscriptions.lock().unwrap();
            let mut all: Vec<&str> = subs_guard.iter().map(|s| s.as_str()).collect();
            all.push(topic);
            self.consumer.subscribe(&all)
                .map_err(|e| EventBusError::Topic(format!("Failed to auto-subscribe to {}: {}", topic, e)))?;
            subs_guard.insert(topic.to_string());
            info!("Auto-subscribed to Kafka topic: {}", topic);
        }
        
        let mut result = Vec::new();
        let start = std::time::Instant::now();
        let timeout = Duration::from_millis(500); // reduced for test responsiveness
        while start.elapsed() < timeout {
            match self.consumer.as_ref().poll(Duration::from_millis(50)) {
                None => { /* no message this tick */ }
                Some(Ok(message)) => {
                    if message.topic() == topic {
                        if let Some(payload) = message.payload() { result.push(payload.to_vec()); }
                    }
                }
                Some(Err(e)) => {
                    error!("Error receiving Kafka message: {}", e);
                }
            }
        }
        Ok(result)
    }
    async fn subscribe(&mut self, topic: &str) -> Result<(), EventBusError> {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        
        // Only subscribe if we haven't already
        if !subscriptions.contains(topic) {
            // Get current subscriptions
            let current_topics: Vec<&str> = subscriptions.iter().map(|s| s.as_str()).collect();
            
            // Add the new topic
            let mut new_topics = current_topics.clone();
            new_topics.push(topic);
            
            // Subscribe to all topics
            self.consumer
                .subscribe(&new_topics)
                .map_err(|e| EventBusError::Topic(format!("Failed to subscribe to topic {}: {}", topic, e)))?;
                
            // Update subscriptions
            subscriptions.insert(topic.to_string());
            
            info!("Subscribed to Kafka topic: {}", topic);
        }
        
        Ok(())
    }
    async fn unsubscribe(&mut self, topic: &str) -> Result<(), EventBusError> {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        if subscriptions.remove(topic) {
            let remaining: Vec<&str> = subscriptions.iter().map(|s| s.as_str()).collect();
            if remaining.is_empty() {
                // rdkafka's unsubscribe returns (), so no error handling required
                self.consumer.unsubscribe();
            } else if let Err(e) = self.consumer.subscribe(&remaining) {
                return Err(EventBusError::Topic(format!("Failed to update subscriptions: {}", e)));
            }
            info!("Unsubscribed from Kafka topic: {}", topic);
        }
    Ok(())
    }
}
