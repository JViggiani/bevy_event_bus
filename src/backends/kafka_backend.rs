use rdkafka::{
    ClientContext, Offset, TopicPartitionList,
    config::ClientConfig,
    consumer::{BaseConsumer, CommitMode, Consumer},
    message::{Headers, Message},
    producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext},
};
use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::{EventBusBackend, resources::IncomingMessage};
use async_trait::async_trait;
use crossbeam_channel::{Receiver, Sender, bounded};
use std::sync::atomic::{AtomicBool, Ordering};

/// Configuration for Kafka backend
/// Connection configuration for Kafka backend initialization
#[derive(Clone, Debug)]
pub struct KafkaConnection {
    /// List of Kafka bootstrap servers
    pub bootstrap_servers: String,
    /// Client ID for Kafka
    pub client_id: Option<String>,
    /// Connection timeout
    pub timeout_ms: i32,
    /// Additional Kafka configuration
    pub additional_config: HashMap<String, String>,
}

impl Default for KafkaConnection {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            client_id: None,
            timeout_ms: 10000,
            additional_config: HashMap::new(),
        }
    }
}

/// Consumer group configuration and state
struct ConsumerGroup {
    consumer: BaseConsumer,
    topics: HashSet<String>,
    manual_commit_enabled: bool,
    _group_id: String, // Stored for debugging/logging but not actively used
}

impl ConsumerGroup {
    fn new(
        config: &KafkaConnection,
        group_id: String,
        manual_commit: bool,
    ) -> Result<Self, String> {
        let mut consumer_config = ClientConfig::new();
        consumer_config.set("bootstrap.servers", &config.bootstrap_servers);
        consumer_config.set("group.id", &group_id);

        // Configure auto-commit based on manual_commit flag
        consumer_config.set(
            "enable.auto.commit",
            if manual_commit { "false" } else { "true" },
        );
        consumer_config.set("session.timeout.ms", "6000");
        // For explicit consumer groups, use "earliest" so they can read all messages from the beginning
        // This is important for testing and for applications that want to process all messages
        consumer_config.set("auto.offset.reset", "earliest");

        if let Some(client_id) = &config.client_id {
            consumer_config.set("client.id", &format!("{}_{}", client_id, group_id));
        }

        // Add any additional configuration
        for (key, value) in &config.additional_config {
            consumer_config.set(key, value);
        }

        let consumer: BaseConsumer = consumer_config
            .create()
            .map_err(|e| format!("Failed to create consumer for group {}: {}", group_id, e))?;

        Ok(Self {
            consumer,
            topics: HashSet::new(),
            manual_commit_enabled: manual_commit,
            _group_id: group_id,
        })
    }
}

/// Custom producer context for Kafka delivery reporting
#[derive(Clone)]
struct EventBusProducerContext;

impl ClientContext for EventBusProducerContext {}

impl ProducerContext for EventBusProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, delivery_result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        match delivery_result {
            Err((kafka_error, owned_message)) => {
                warn!(
                    topic = %owned_message.topic(),
                    partition = owned_message.partition(),
                    error = %kafka_error,
                    "Kafka message delivery failed"
                );
                // TODO: Fire EventBusError<T> event when we have access to the event type
            }
            Ok(delivery) => {
                debug!(
                    topic = %delivery.topic(),
                    partition = delivery.partition(),
                    offset = delivery.offset(),
                    "Kafka message delivered successfully"
                );
            }
        }
    }
}

/// Kafka implementation of the EventBusBackend
pub struct KafkaEventBusBackend {
    config: KafkaConnection,
    producer: Arc<BaseProducer<EventBusProducerContext>>,
    // Default consumer for backward compatibility and basic operations
    default_consumer: Arc<BaseConsumer>,
    // Multiple consumer groups support
    consumer_groups: Arc<Mutex<HashMap<String, ConsumerGroup>>>,
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
            .field(
                "subscriptions_count",
                &self.subscriptions.lock().unwrap().len(),
            )
            .finish()
    }
}

impl KafkaEventBusBackend {
    pub fn new(config: KafkaConnection) -> Self {
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

        // Create custom producer context for delivery callbacks
        let producer_context = EventBusProducerContext;

        let producer: BaseProducer<EventBusProducerContext> = producer_config
            .create_with_context(producer_context)
            .expect("Failed to create Kafka producer");

        // Configure default consumer - basic consumer for backward compatibility
        let mut consumer_config = ClientConfig::new();
        consumer_config.set("bootstrap.servers", &config.bootstrap_servers);

        // Use unique consumer group for each backend instance to avoid message sharing
        use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
        static GROUP_COUNTER: AtomicUsize = AtomicUsize::new(0);
        let unique_group = GROUP_COUNTER.fetch_add(1, AtomicOrdering::SeqCst);
        consumer_config.set(
            "group.id",
            &format!("bevy_event_bus_default_{}", unique_group),
        );

        // Set sensible defaults that user can override
        consumer_config.set("enable.auto.commit", "true");
        consumer_config.set("session.timeout.ms", "6000");
        // Default to "latest" for real-time applications (games) - user can override to "earliest"
        consumer_config.set("auto.offset.reset", "latest");

        if let Some(client_id) = &config.client_id {
            consumer_config.set("client.id", &format!("{}_default", client_id));
        }

        // Add any additional configuration (this can override our defaults)
        for (key, value) in &config.additional_config {
            consumer_config.set(key, value);
        }

        let default_consumer: BaseConsumer = consumer_config
            .create()
            .expect("Failed to create default Kafka consumer");

        Self {
            config,
            producer: Arc::new(producer),
            default_consumer: Arc::new(default_consumer),
            consumer_groups: Arc::new(Mutex::new(HashMap::new())),
            subscriptions: Arc::new(Mutex::new(HashSet::new())),
            bg_running: Arc::new(AtomicBool::new(false)),
            sender: None,
            receiver: None,
            dropped: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            task_abort: None,
        }
    }

    /// Access to underlying config bootstrap servers (needed for admin topic creation in plugin)
    pub fn bootstrap_servers(&self) -> &str {
        &self.config.bootstrap_servers
    }

    /// Get all current subscriptions across all consumer groups
    pub fn current_subscriptions(&self) -> Vec<String> {
        self.subscriptions.lock().unwrap().iter().cloned().collect()
    }

    /// Get all active consumer groups
    pub fn active_consumer_groups(&self) -> Vec<String> {
        self.consumer_groups
            .lock()
            .unwrap()
            .keys()
            .cloned()
            .collect()
    }
}

impl Clone for KafkaEventBusBackend {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            producer: self.producer.clone(),
            default_consumer: self.default_consumer.clone(),
            consumer_groups: self.consumer_groups.clone(),
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
    pub fn take_receiver(&mut self) -> Option<Receiver<IncomingMessage>> {
        self.receiver.take()
    }

    pub fn dropped_count(&self) -> usize {
        self.dropped.load(Ordering::Relaxed)
    }
    pub fn poll_producer(&self) {
        self.producer.poll(Duration::from_millis(0));
    }
}

impl Drop for KafkaEventBusBackend {
    fn drop(&mut self) {
        // Ensure background thread stops cleanly to avoid late shutdown error spam
        if self.bg_running.swap(false, Ordering::SeqCst) {
            if let Some(abort) = self.task_abort.take() {
                abort.abort();
            }
        }
        // Best-effort flush (ignore errors). Extra polls attempt to drive any pending delivery callbacks.
        for _ in 0..10 {
            self.producer.poll(Duration::from_millis(10));
        }
        let _ = self.producer.flush(Duration::from_millis(250));
    }
}

#[async_trait]
impl EventBusBackend for KafkaEventBusBackend {
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
        // Use compare_exchange to atomically check and set - prevents race condition
        if self
            .bg_running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            debug!("Kafka backend connect() called but background already running");
            return true;
        }
        info!(
            "Initializing Kafka backend (lazy connect) for {}",
            self.config.bootstrap_servers
        );
        // Attempt metadata readiness loop (bounded) before spawning background thread to reduce noisy errors.
        let start = std::time::Instant::now();
        let deadline = Duration::from_secs(5);
        let mut attempt: u32 = 0;
        let mut last_err: Option<String> = None;
        while start.elapsed() < deadline {
            attempt += 1;
            match self
                .producer
                .client()
                .fetch_metadata(None, Duration::from_millis(700))
            {
                Ok(md) => {
                    info!(
                        brokers = md.brokers().len(),
                        attempts = attempt,
                        "Kafka metadata ready pre-spawn"
                    );
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
            warn!(
                elapsed_ms = start.elapsed().as_millis(),
                "Proceeding without confirmed metadata (will retry in background)"
            );
        }
        // Re-subscribe to any existing topics
        let existing = self.subscriptions.lock().unwrap().clone();
        if !existing.is_empty() {
            let topics: Vec<&str> = existing.iter().map(|s| s.as_str()).collect();
            if let Err(e) = self.default_consumer.subscribe(&topics) {
                error!("Failed to subscribe to topics: {}", e);
                return false;
            }
        }
        // Spawn background consumer task once (Tokio)
        let (tx, rx) = bounded::<IncomingMessage>(10_000);
        self.sender = Some(tx.clone());
        self.receiver = Some(rx);
        let consumer = self.default_consumer.clone();
        let producer = self.producer.clone();
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
                // Drive producer delivery callbacks
                producer.poll(Duration::from_millis(0));
                match consumer.as_ref().poll(Duration::from_millis(50)) {
                    None => { /* timeout, loop */ }
                    Some(Ok(m)) => {
                        let topic = m.topic().to_string();
                        { subs.lock().unwrap().insert(topic.clone()); }
                        if let Some(payload) = m.payload() {
                            // Extract headers from Kafka message
                            let mut headers = std::collections::HashMap::new();
                            if let Some(msg_headers) = m.headers() {
                                for header in msg_headers.iter() {
                                    if let Some(value_bytes) = header.value {
                                        // Convert header value to string (UTF-8 only for now)
                                        if let Ok(value_str) = String::from_utf8(value_bytes.to_vec()) {
                                            headers.insert(header.key.to_string(), value_str);
                                        }
                                        // Skip non-UTF-8 headers for simplicity
                                    }
                                }
                            }

                            let msg = IncomingMessage {
                                topic,
                                partition: m.partition(),
                                offset: m.offset(),
                                key: m.key().map(|k| k.to_vec()),
                                payload: payload.to_vec(),
                                timestamp: std::time::Instant::now(),
                                headers,
                            };
                            if tx.try_send(msg).is_err() { dropped_counter.fetch_add(1, Ordering::Relaxed); }
                        }
                    }
                    Some(Err(e)) => {
                        let msg = e.to_string();
                        if last_err.as_ref() == Some(&msg) {
                            repeated += 1;
                            if repeated % 10 == 0 {
                                warn!(repeats = repeated, err = %msg, bootstrap = %bootstrap, "Repeating Kafka consume error");
                            }
                        } else {
                            error!(err = %msg, bootstrap = %bootstrap, "Background Kafka consume error");
                            last_err = Some(msg);
                            repeated = 0;
                        }
                        tokio::time::sleep(error_backoff).await;
                        error_backoff = std::cmp::min(error_backoff * 2, max_error);
                    }
                }
                // Light additional producer progress after consumer activity to reduce tail latency.
                for _ in 0..2 { producer.poll(Duration::from_millis(0)); }
            }
        });
        self.task_abort = Some(task.abort_handle());
        true
    }
    async fn disconnect(&mut self) -> bool {
        info!("Disconnecting from Kafka");
        self.bg_running.store(false, Ordering::SeqCst);
        if let Some(abort) = self.task_abort.take() {
            abort.abort();
        }
        // Nothing special needed for disconnection, Kafka clients clean up on drop
        true
    }
    fn try_send_serialized(&self, event_json: &[u8], topic: &str) -> bool {
        tracing::info!(
            "try_send_serialized called: topic={}, payload_size={}",
            topic,
            event_json.len()
        );
        let record: BaseRecord<'_, (), [u8]> = BaseRecord::to(topic).payload(event_json);
        if let Err((e, _)) = self.producer.send(record) {
            error!("Failed to enqueue message: {}", e);
            return false;
        }
        tracing::info!("Message successfully enqueued for topic: {}", topic);
        // Non-blocking send; delivery progress advanced by background producer polling.
        true
    }

    fn try_send_serialized_with_headers(
        &self,
        event_json: &[u8],
        topic: &str,
        headers: &std::collections::HashMap<String, String>,
    ) -> bool {
        use rdkafka::message::OwnedHeaders;

        let mut record: BaseRecord<'_, (), [u8]> = BaseRecord::to(topic).payload(event_json);

        // Add headers if any
        if !headers.is_empty() {
            let mut owned_headers = OwnedHeaders::new();
            for (key, value) in headers {
                owned_headers = owned_headers.insert(rdkafka::message::Header {
                    key,
                    value: Some(value),
                });
            }
            record = record.headers(owned_headers);
        }

        if let Err((e, _)) = self.producer.send(record) {
            debug!("Failed to enqueue message with headers: {}", e);
            return false;
        }
        // Non-blocking send; delivery progress advanced by background producer polling.
        true
    }
    async fn receive_serialized(&self, topic: &str) -> Vec<Vec<u8>> {
        // Auto-subscribe if not already
        let mut need_subscribe = false;
        {
            let subs = self.subscriptions.lock().unwrap();
            if !subs.contains(topic) {
                need_subscribe = true;
            }
        }
        if need_subscribe {
            // We need a mutable consumer subscribe; safe because subscribe only adds topic list
            let mut subs_guard = self.subscriptions.lock().unwrap();
            let mut all: Vec<&str> = subs_guard.iter().map(|s| s.as_str()).collect();
            all.push(topic);
            if let Err(e) = self.default_consumer.subscribe(&all) {
                error!("Failed to auto-subscribe to {}: {}", topic, e);
                return Vec::new();
            }
            subs_guard.insert(topic.to_string());
            info!("Auto-subscribed to Kafka topic: {}", topic);
        }

        let mut result = Vec::new();
        // Fast, non-blocking style receive: perform a small number of short polls (overall <100ms)
        // to satisfy startup fallback path without stalling frames for idle topics.
        for i in 0..2 {
            // at most ~40ms but likely exit on first iteration
            match self
                .default_consumer
                .as_ref()
                .poll(Duration::from_millis(5))
            {
                None => {
                    break;
                }
                Some(Ok(message)) => {
                    if message.topic() == topic {
                        if let Some(payload) = message.payload() {
                            result.push(payload.to_vec());
                        }
                    }
                    // If we got something, allow a second quick poll for possible batch continuation
                    if i == 0 {
                        continue;
                    } else {
                        break;
                    }
                }
                Some(Err(e)) => {
                    error!("Error receiving Kafka message: {}", e);
                    break;
                }
            }
        }
        result
    }
    async fn subscribe(&mut self, topic: &str) -> bool {
        let mut subscriptions = self.subscriptions.lock().unwrap();

        // Only subscribe if we haven't already
        if !subscriptions.contains(topic) {
            // Get current subscriptions
            let current_topics: Vec<&str> = subscriptions.iter().map(|s| s.as_str()).collect();

            // Add the new topic
            let mut new_topics = current_topics.clone();
            new_topics.push(topic);

            // Subscribe to all topics
            if let Err(e) = self.default_consumer.subscribe(&new_topics) {
                error!("Failed to subscribe to topic {}: {}", topic, e);
                return false;
            }

            // Update subscriptions
            subscriptions.insert(topic.to_string());

            info!("Subscribed to Kafka topic: {}", topic);
        }

        true
    }
    async fn unsubscribe(&mut self, topic: &str) -> bool {
        let mut subscriptions = self.subscriptions.lock().unwrap();
        if subscriptions.remove(topic) {
            let remaining: Vec<&str> = subscriptions.iter().map(|s| s.as_str()).collect();
            if remaining.is_empty() {
                // rdkafka's unsubscribe returns (), so no error handling required
                self.default_consumer.unsubscribe();
            } else if let Err(e) = self.default_consumer.subscribe(&remaining) {
                error!("Failed to update subscriptions: {}", e);
                return false;
            }
            info!("Unsubscribed from Kafka topic: {}", topic);
        }
        true
    }

    /// Send with partition key for message ordering (Kafka-specific implementation)
    fn try_send_serialized_with_partition_key(
        &self,
        event_json: &[u8],
        topic: &str,
        key: &str,
    ) -> bool {
        let record: BaseRecord<str, [u8]> = BaseRecord::to(topic).payload(event_json).key(key);

        match self.producer.send(record) {
            Ok(_) => {
                debug!(
                    "Event queued for delivery to Kafka topic '{}' with key '{}'",
                    topic, key
                );
                true
            }
            Err((e, _)) => {
                error!(
                    "Failed to queue event for topic '{}' with key '{}': {}",
                    topic, key, e
                );
                false
            }
        }
    }

    /// Send with both partition key and headers
    fn try_send_serialized_with_key_and_headers(
        &self,
        event_json: &[u8],
        topic: &str,
        key: &str,
        headers: &HashMap<String, String>,
    ) -> bool {
        // Create rdkafka headers
        let mut rdkafka_headers = rdkafka::message::OwnedHeaders::new();
        for (key_h, value_h) in headers {
            rdkafka_headers = rdkafka_headers.insert(rdkafka::message::Header {
                key: key_h,
                value: Some(value_h.as_bytes()),
            });
        }

        let record: BaseRecord<str, [u8]> = BaseRecord::to(topic)
            .payload(event_json)
            .key(key)
            .headers(rdkafka_headers);

        match self.producer.send(record) {
            Ok(_) => {
                debug!(
                    "Event queued for delivery to Kafka topic '{}' with key '{}' and {} headers",
                    topic,
                    key,
                    headers.len()
                );
                true
            }
            Err((e, _)) => {
                error!(
                    "Failed to queue event for topic '{}' with key '{}' and headers: {}",
                    topic, key, e
                );
                false
            }
        }
    }

    /// Create a consumer group with specific configuration
    async fn create_consumer_group(
        &mut self,
        topics: &[String],
        group_id: &str,
    ) -> Result<(), String> {
        let mut consumer_groups = self.consumer_groups.lock().unwrap();

        // Check if group already exists
        if consumer_groups.contains_key(group_id) {
            warn!("Consumer group '{}' already exists", group_id);
            return Ok(());
        }

        // Create new consumer group
        let mut consumer_group = ConsumerGroup::new(&self.config, group_id.to_string(), false)?;

        // Subscribe to topics
        if !topics.is_empty() {
            let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
            if let Err(e) = consumer_group.consumer.subscribe(&topic_refs) {
                return Err(format!(
                    "Failed to subscribe consumer group '{}' to topics: {}",
                    group_id, e
                ));
            }
            consumer_group.topics.extend(topics.iter().cloned());
        }

        consumer_groups.insert(group_id.to_string(), consumer_group);
        info!(
            "Created consumer group '{}' with topics: {:?}",
            group_id, topics
        );
        Ok(())
    }

    /// Consume from specific consumer group
    async fn receive_serialized_with_group(&self, topic: &str, group_id: &str) -> Vec<Vec<u8>> {
        // Check if consumer group exists first
        let needs_creation = {
            let consumer_groups = self.consumer_groups.lock().unwrap();
            !consumer_groups.contains_key(group_id)
        };

        // Create consumer group if needed - this is a limitation that we can't create from immutable method
        if needs_creation {
            warn!(
                "Consumer group '{}' does not exist. Use create_consumer_group() first.",
                group_id
            );
            return Vec::new();
        }

        let mut consumer_groups = self.consumer_groups.lock().unwrap();
        let consumer_group = match consumer_groups.get_mut(group_id) {
            Some(group) => group,
            None => {
                error!("Consumer group '{}' not found", group_id);
                return Vec::new();
            }
        };

        // Auto-subscribe to topic if not already subscribed
        if !consumer_group.topics.contains(topic) {
            let mut all_topics: Vec<&str> =
                consumer_group.topics.iter().map(|s| s.as_str()).collect();
            all_topics.push(topic);

            if let Err(e) = consumer_group.consumer.subscribe(&all_topics) {
                error!(
                    "Failed to subscribe consumer group '{}' to topic '{}': {}",
                    group_id, topic, e
                );
                return Vec::new();
            }

            consumer_group.topics.insert(topic.to_string());
            info!(
                "Auto-subscribed consumer group '{}' to topic '{}'",
                group_id, topic
            );
        }

        let mut result = Vec::new();

        // If no partitions assigned initially, do a longer poll to trigger rebalancing
        if let Ok(assignment) = consumer_group.consumer.assignment() {
            if assignment.count() == 0 {
                // Try a longer poll to allow for partition assignment/rebalancing
                if let Some(Ok(msg)) = consumer_group.consumer.poll(Duration::from_millis(2000)) {
                    // For consumer groups subscribed to multiple topics, return all messages
                    if consumer_group.topics.len() > 1 || msg.topic() == topic {
                        if let Some(payload) = msg.payload() {
                            result.push(payload.to_vec());
                        }
                    }
                }
            }
        }

        // Poll for messages from this specific consumer group with sufficient timeout for rebalancing
        for attempt in 0..10 {
            // More attempts to handle Kafka rebalancing
            match consumer_group.consumer.poll(Duration::from_millis(200)) {
                None => {
                    if attempt < 7 {
                        // Continue polling for more attempts to handle rebalancing
                        continue;
                    } else {
                        break;
                    }
                }
                Some(Ok(message)) => {
                    // For consumer groups subscribed to multiple topics, we should return all messages
                    // The topic parameter can be used for filtering, but we shouldn't drop messages from other topics
                    // as that would make them lost forever for this consumer group
                    if consumer_group.topics.len() > 1 || message.topic() == topic {
                        if let Some(payload) = message.payload() {
                            result.push(payload.to_vec());
                        }
                    }
                }
                Some(Err(e)) => {
                    tracing::error!("Consumer group '{}' poll error: {}", group_id, e);
                    error!("Error receiving from consumer group '{}': {}", group_id, e);
                    break;
                }
            }
        }

        tracing::info!(
            "receive_serialized_with_group finished: topic={}, group_id={}, received_count={}",
            topic,
            group_id,
            result.len()
        );
        result
    }

    /// Enable manual offset commits for reliable processing
    async fn enable_manual_commits(&mut self, group_id: &str) -> Result<(), String> {
        let mut consumer_groups = self.consumer_groups.lock().unwrap();

        // Check if group exists
        if !consumer_groups.contains_key(group_id) {
            // Create a new consumer group with manual commits enabled
            let consumer_group = ConsumerGroup::new(&self.config, group_id.to_string(), true)?;
            consumer_groups.insert(group_id.to_string(), consumer_group);
            info!(
                "Created new consumer group '{}' with manual commits enabled",
                group_id
            );
        } else {
            // Check if manual commits are already enabled
            let group = consumer_groups.get(group_id).unwrap();
            if group.manual_commit_enabled {
                info!(
                    "Manual commits already enabled for consumer group '{}'",
                    group_id
                );
                return Ok(());
            } else {
                // Need to recreate the consumer with manual commits
                let topics: Vec<String> = group.topics.iter().cloned().collect();
                drop(consumer_groups); // Release lock

                // Remove old group and create new one with manual commits
                self.consumer_groups.lock().unwrap().remove(group_id);
                let mut new_group = ConsumerGroup::new(&self.config, group_id.to_string(), true)?;

                // Re-subscribe to topics
                if !topics.is_empty() {
                    let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
                    if let Err(e) = new_group.consumer.subscribe(&topic_refs) {
                        return Err(format!(
                            "Failed to re-subscribe consumer group '{}' to topics: {}",
                            group_id, e
                        ));
                    }
                    new_group.topics.extend(topics);
                }

                self.consumer_groups
                    .lock()
                    .unwrap()
                    .insert(group_id.to_string(), new_group);
                info!(
                    "Reconfigured consumer group '{}' with manual commits enabled",
                    group_id
                );
            }
        }

        Ok(())
    }

    /// Manually commit a specific message offset
    async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) -> Result<(), String> {
        let consumer_groups = self.consumer_groups.lock().unwrap();

        // Find the consumer group that has this topic
        let mut found_group = None;
        for (group_id, group) in consumer_groups.iter() {
            if group.topics.contains(topic) && group.manual_commit_enabled {
                found_group = Some((group_id.clone(), &group.consumer));
                break;
            }
        }

        let (group_id, consumer) = match found_group {
            Some((id, consumer)) => (id, consumer),
            None => {
                return Err(format!(
                    "No consumer group with manual commits enabled found for topic '{}'",
                    topic
                ));
            }
        };

        // Create TopicPartitionList for the specific offset
        let mut tpl = TopicPartitionList::new();
        if let Err(e) = tpl.add_partition_offset(topic, partition, Offset::Offset(offset + 1)) {
            return Err(format!("Failed to add partition offset: {}", e));
        }

        // Commit the offset
        match consumer.commit(&tpl, CommitMode::Sync) {
            Ok(()) => {
                debug!(
                    "Successfully committed offset {} for topic '{}' partition {} in group '{}'",
                    offset, topic, partition, group_id
                );
                Ok(())
            }
            Err(e) => {
                error!(
                    "Failed to commit offset {} for topic '{}' partition {} in group '{}': {}",
                    offset, topic, partition, group_id, e
                );
                Err(format!("Commit failed: {}", e))
            }
        }
    }

    /// Get consumer lag for monitoring
    async fn get_consumer_lag(&self, topic: &str, group_id: &str) -> Result<i64, String> {
        use rdkafka::consumer::Consumer;

        let consumer_groups = self.consumer_groups.lock().unwrap();

        let consumer = match consumer_groups.get(group_id) {
            Some(group) => &group.consumer,
            None => {
                return Err(format!("Consumer group '{}' not found", group_id));
            }
        };

        // Get metadata for the topic to find partitions
        let metadata = match consumer.fetch_metadata(Some(topic), Duration::from_secs(5)) {
            Ok(md) => md,
            Err(e) => {
                return Err(format!(
                    "Failed to fetch metadata for topic '{}': {}",
                    topic, e
                ));
            }
        };

        let topic_metadata = match metadata.topics().iter().find(|t| t.name() == topic) {
            Some(tm) => tm,
            None => {
                return Err(format!("Topic '{}' not found in metadata", topic));
            }
        };

        let mut total_lag = 0i64;

        // Calculate lag for each partition
        for partition in topic_metadata.partitions() {
            let partition_id = partition.id();

            // Get high watermark (latest offset available)
            let (low, high) =
                match consumer.fetch_watermarks(topic, partition_id, Duration::from_secs(5)) {
                    Ok((l, h)) => (l, h),
                    Err(e) => {
                        warn!(
                            "Failed to fetch watermarks for topic '{}' partition {}: {}",
                            topic, partition_id, e
                        );
                        continue;
                    }
                };

            // Get committed offset for this consumer group
            let mut tpl = TopicPartitionList::new();
            if let Err(e) = tpl.add_partition_offset(topic, partition_id, Offset::Invalid) {
                warn!("Failed to add partition to TopicPartitionList: {}", e);
                continue;
            }

            let committed_offsets = match consumer.committed_offsets(tpl, Duration::from_secs(5)) {
                Ok(offsets) => offsets,
                Err(e) => {
                    warn!(
                        "Failed to get committed offsets for topic '{}' partition {}: {}",
                        topic, partition_id, e
                    );
                    continue;
                }
            };

            // Calculate lag for this partition
            if let Some(elem) = committed_offsets.elements().first() {
                match elem.offset() {
                    Offset::Offset(committed) => {
                        let lag = high - committed;
                        total_lag += lag.max(0); // Don't count negative lag
                        debug!(
                            "Partition {} lag: {} (high: {}, committed: {})",
                            partition_id, lag, high, committed
                        );
                    }
                    _ => {
                        // No committed offset yet, lag is the total available
                        let lag = high - low;
                        total_lag += lag.max(0);
                        debug!(
                            "Partition {} no committed offset, lag: {} (high: {}, low: {})",
                            partition_id, lag, high, low
                        );
                    }
                }
            }
        }

        debug!(
            "Total consumer lag for topic '{}' group '{}': {}",
            topic, group_id, total_lag
        );
        Ok(total_lag)
    }

    /// Flush all pending messages (producer-side)
    async fn flush(&self) -> Result<(), String> {
        match self.producer.flush(Duration::from_secs(10)) {
            Ok(_) => {
                debug!("Successfully flushed all pending Kafka messages");
                Ok(())
            }
            Err(e) => {
                error!("Failed to flush Kafka producer: {}", e);
                Err(format!("Flush failed: {}", e))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_context_creation() {
        // Test that we can create a producer context
        let context = EventBusProducerContext;

        // The context should exist and be ready to use
        assert_eq!(std::mem::size_of_val(&context), 0); // Empty struct
    }
}
