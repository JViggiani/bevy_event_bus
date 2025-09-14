use rdkafka::{
    config::ClientConfig,
    consumer::{Consumer, BaseConsumer},
    producer::{BaseProducer, BaseRecord, Producer},
    message::Message,
};
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{info, error, warn, debug};
use std::fmt::Debug;

use crate::{EventBusBackend, EventBusError, resources::IncomingMessage};
use crossbeam_channel::{bounded, Sender, Receiver};
use std::sync::atomic::{AtomicBool, Ordering};
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
    bg_running: Arc<AtomicBool>,
    sender: Option<Sender<IncomingMessage>>,
    receiver: Option<Receiver<IncomingMessage>>,
    dropped: Arc<std::sync::atomic::AtomicUsize>,
    // Tokio task handle for background consumer
    task_abort: Option<tokio::task::AbortHandle>,
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
            bg_running: Arc::new(AtomicBool::new(false)),
            sender: None,
            receiver: None,
            dropped: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            task_abort: None,
        }
    }

    /// Access to underlying config bootstrap servers (needed for admin topic creation in plugin)
    pub fn bootstrap_servers(&self) -> &str { &self.config.bootstrap_servers }
    #[allow(dead_code)]
    pub fn group_id(&self) -> &str { &self.config.group_id }
    pub fn current_subscriptions(&self) -> Vec<String> { self.subscriptions.lock().unwrap().iter().cloned().collect() }
}

impl Clone for KafkaEventBusBackend {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            producer: self.producer.clone(),
            consumer: self.consumer.clone(),
            subscriptions: self.subscriptions.clone(),
            bg_running: self.bg_running.clone(),
            sender: self.sender.clone(),
            receiver: None, // receiver cannot be cloned (single consumer side)
            dropped: self.dropped.clone(),
            task_abort: None, // task handle not cloned
        }
    }
}

impl KafkaEventBusBackend {
    pub fn take_receiver(&mut self) -> Option<Receiver<IncomingMessage>> { self.receiver.take() }
    pub fn dropped_count(&self) -> usize { self.dropped.load(Ordering::Relaxed) }
}

impl Drop for KafkaEventBusBackend {
    fn drop(&mut self) {
        // Ensure background thread stops cleanly to avoid late shutdown error spam
        if self.bg_running.swap(false, Ordering::SeqCst) {
            if let Some(abort) = self.task_abort.take() { abort.abort(); }
        }
        // Best-effort flush (ignore errors)
        let _ = self.producer.flush(Duration::from_millis(200));
    }
}

#[async_trait]
impl EventBusBackend for KafkaEventBusBackend {
    fn clone_box(&self) -> Box<dyn EventBusBackend> { Box::new(self.clone()) }
    fn as_any(&self) -> &dyn std::any::Any { self }
    fn as_any_mut(&mut self) -> &mut dyn std::any::Any { self }
    async fn connect(&mut self) -> Result<(), EventBusError> {
        if self.bg_running.load(Ordering::Relaxed) {
            debug!("Kafka backend connect() called but background already running");
            return Ok(());
        }
        info!("Initializing Kafka backend (lazy connect) for {}", self.config.bootstrap_servers);
        // Attempt metadata readiness loop (bounded) before spawning background thread to reduce noisy errors.
        let start = std::time::Instant::now();
        let deadline = Duration::from_secs(5);
        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
    while start.elapsed() < deadline {
            attempt += 1;
            match self.producer.client().fetch_metadata(None, Duration::from_millis(700)) {
                Ok(md) => {
                    info!(brokers = md.brokers().len(), attempts = attempt, "Kafka metadata ready pre-spawn");
                    break;
                }
                Err(e) => {
                    let msg = e.to_string();
                    if last_err.as_ref() != Some(&msg) {
                        debug!(attempt, err = %msg, "Kafka metadata attempt failed");
                    }
                    last_err = Some(msg);
            tokio::time::sleep(Duration::from_millis(120)).await;
                }
            }
        }
        if start.elapsed() >= deadline {
            warn!(elapsed_ms = start.elapsed().as_millis(), "Proceeding without confirmed metadata (will retry in background)");
        }
        // Re-subscribe to any existing topics
        let existing = self.subscriptions.lock().unwrap().clone();
        if !existing.is_empty() {
            let topics: Vec<&str> = existing.iter().map(|s| s.as_str()).collect();
            if let Err(e) = self.consumer.subscribe(&topics) {
                return Err(EventBusError::Connection(format!("Failed to subscribe to topics: {}", e)));
            }
        }
        // Spawn background consumer task once (Tokio)
        if !self.bg_running.swap(true, Ordering::SeqCst) {
            let (tx, rx) = bounded::<IncomingMessage>(10_000);
            self.sender = Some(tx.clone());
            self.receiver = Some(rx);
            let consumer = self.consumer.clone();
            let subs = self.subscriptions.clone();
            let running = self.bg_running.clone();
            let dropped_counter = self.dropped.clone();
            let bootstrap = self.config.bootstrap_servers.clone();
            let rt = crate::runtime::runtime();
            // spawn_blocking not needed; poll is non-blocking with small timeout, but we use spawn to keep simple
            let task = rt.spawn(async move {
                let mut last_err: Option<String> = None;
                let mut repeated: u32 = 0;
                let mut error_backoff = Duration::from_millis(100);
                let max_error = Duration::from_millis(1000);
                while running.load(Ordering::Relaxed) {
                    match consumer.as_ref().poll(Duration::from_millis(50)) {
                        None => { /* timeout, loop */ }
                        Some(Ok(m)) => {
                            let topic = m.topic().to_string();
                            { subs.lock().unwrap().insert(topic.clone()); }
                            if let Some(payload) = m.payload() {
                                let msg = IncomingMessage {
                                    topic,
                                    partition: m.partition(),
                                    offset: m.offset(),
                                    key: m.key().map(|k| k.to_vec()),
                                    payload: payload.to_vec(),
                                    timestamp: std::time::Instant::now(),
                                };
                                if tx.try_send(msg).is_err() { dropped_counter.fetch_add(1, Ordering::Relaxed); }
                            }
                        }
                        Some(Err(e)) => {
                            let msg = e.to_string();
                            if last_err.as_ref() == Some(&msg) {
                                repeated += 1;
                                if repeated % 10 == 0 { warn!(repeats = repeated, err = %msg, bootstrap = %bootstrap, "Repeating Kafka consume error"); }
                            } else {
                                error!(err = %msg, bootstrap = %bootstrap, "Background Kafka consume error");
                                last_err = Some(msg);
                                repeated = 0;
                            }
                            tokio::time::sleep(error_backoff).await;
                            error_backoff = std::cmp::min(error_backoff * 2, max_error);
                        }
                    }
                }
            });
            self.task_abort = Some(task.abort_handle());
        }
        Ok(())
    }
    async fn disconnect(&mut self) -> Result<(), EventBusError> {
        info!("Disconnecting from Kafka");
        self.bg_running.store(false, Ordering::SeqCst);
        if let Some(abort) = self.task_abort.take() { abort.abort(); }
        // Nothing special needed for disconnection, Kafka clients clean up on drop
        Ok(())
    }
    async fn send_serialized(&self, event_json: &[u8], topic: &str) -> Result<(), EventBusError> {
        let record: BaseRecord<'_, (), [u8]> = BaseRecord::to(topic).payload(event_json);
        if let Err((e, _)) = self.producer.send(record) {
            return Err(EventBusError::Other(format!("Failed to enqueue message: {}", e)));
        }
        // Fast-path flush: poll more times to improve delivery reliability for burst batches in tests.
        for _ in 0..40 { // ~400ms upper bound (usually exits earlier internally)
            self.producer.poll(Duration::from_millis(10));
        }
        // Allow producer background delivery progress (slightly longer)
        let _ = self.producer.flush(Duration::from_millis(150));
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
        // Fast, non-blocking style receive: perform a small number of short polls (overall <100ms)
        // to satisfy startup fallback path without stalling frames for idle topics.
        for i in 0..2 { // at most ~40ms but likely exit on first iteration
            match self.consumer.as_ref().poll(Duration::from_millis(5)) {
                None => { break; }
                Some(Ok(message)) => {
                    if message.topic() == topic {
                        if let Some(payload) = message.payload() { result.push(payload.to_vec()); }
                    }
                    // If we got something, allow a second quick poll for possible batch continuation
                    if i == 0 { continue; } else { break; }
                }
                Some(Err(e)) => { error!("Error receiving Kafka message: {}", e); break; }
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
