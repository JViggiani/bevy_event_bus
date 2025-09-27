#[cfg(feature = "kafka")]
use rdkafka::{
    config::ClientConfig,
    consumer::{BaseConsumer, Consumer, CommitMode},
    message::{Message, Headers},
    producer::{BaseProducer, BaseRecord, Producer, DeliveryResult, ProducerContext},
    ClientContext, TopicPartitionList, Offset,
};
use std::collections::{HashMap, HashSet};

use std::sync::{Arc, Mutex};
use std::time::Duration;
use tracing::{debug, error, info, warn};

use crate::{EventBusBackend, config::EventBusConfig, BusEvent};
#[cfg(feature = "kafka")]
use crate::config::kafka::{KafkaWriteConfig, KafkaReadConfig, KafkaConnection};
#[cfg(feature = "kafka")]
use async_trait::async_trait;

#[cfg(feature = "kafka")]
/// Consumer group configuration and state
struct ConsumerGroup {
    consumer: Arc<BaseConsumer>,
    topics: HashSet<String>,
    manual_commit_enabled: bool,
}

impl ConsumerGroup {
    
    /// Create consumer group with full Kafka consumer configuration
    #[cfg(feature = "kafka")]
    fn from_kafka_config(
        kafka_config: &KafkaReadConfig,
        connection_config: &KafkaConnection,
    ) -> Result<Self, String> {
        let group_id = kafka_config.consumer_group();
        let mut consumer_config = ClientConfig::new();
        
        // Use connection config for basic settings
        consumer_config.set("bootstrap.servers", &connection_config.bootstrap_servers);
        consumer_config.set("group.id", group_id);
        
        // Use KafkaReadConfig settings for consumer-specific configuration
        let offset_reset = match kafka_config.offset_reset {
            crate::config::OffsetReset::Earliest => "earliest",
            crate::config::OffsetReset::Latest => "latest",
        };
        consumer_config.set("auto.offset.reset", offset_reset);
        consumer_config.set("enable.auto.commit", &kafka_config.enable_auto_commit.to_string());
        if let Some(timeout) = kafka_config.session_timeout_ms {
            consumer_config.set("session.timeout.ms", &timeout.to_string());
        }
        // Note: max.poll.records is not a valid property in rdkafka, it's Java-specific
        
        // Connection-level settings
        if let Some(client_id) = &connection_config.client_id {
            consumer_config.set("client.id", &format!("{}_{}", client_id, group_id));
        }
        
        // Add connection additional config first (can be overridden)
        for (key, value) in &connection_config.additional_config {
            consumer_config.set(key, value);
        }
        
        // Add consumer-specific additional config (takes precedence)
        for (key, value) in &kafka_config.additional_settings {
            consumer_config.set(key, value);
        }
        
        let consumer: BaseConsumer = consumer_config
            .create()
            .map_err(|e| format!("Failed to create consumer for group {}: {}", group_id, e))?;

        Ok(Self {
            consumer: Arc::new(consumer),
            topics: HashSet::new(),
            manual_commit_enabled: !kafka_config.enable_auto_commit,
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
    // Multiple consumer groups support
    consumer_groups: Arc<Mutex<HashMap<String, ConsumerGroup>>>,
}

impl std::fmt::Debug for KafkaEventBusBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KafkaEventBusBackend")
            .field("config", &self.config)
            .field(
                "consumer_groups_count",
                &self.consumer_groups.lock().unwrap().len(),
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

        Self {
            config,
            producer: Arc::new(producer),
            consumer_groups: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Access to underlying config bootstrap servers (needed for admin topic creation in plugin)
    pub fn bootstrap_servers(&self) -> &str {
        &self.config.bootstrap_servers
    }
    
    /// Get all active consumer groups
    pub fn active_consumer_groups(&self) -> Vec<String> {
        self.consumer_groups.lock().unwrap().keys().cloned().collect()
    }
    
    /// Shutdown the Kafka backend with configurable timeouts
    /// 
    /// This method handles the orderly shutdown of the Kafka producer by:
    /// 1. Polling multiple times to process any pending delivery callbacks
    /// 2. Flushing all remaining messages with a configurable timeout
    /// 
    /// The automatic `Drop` implementation calls this with conservative defaults.
    /// For explicit shutdown, you can use longer timeouts for better guarantees.
    /// 
    /// # Arguments
    /// * `poll_timeout` - How long to wait for each producer poll operation
    /// * `flush_timeout` - How long to wait for the final flush operation  
    /// * `poll_attempts` - How many times to poll for delivery callbacks
    /// 
    /// # Why Poll Multiple Times?
    /// Kafka producers use asynchronous delivery callbacks. When you send messages,
    /// Kafka acknowledges delivery through callbacks that need to be processed.
    /// Polling gives these callbacks a chance to execute before shutdown.
    /// 
    /// # Example
    /// ```rust
    /// // Quick shutdown (automatic Drop behavior)
    /// backend.shutdown(Duration::from_millis(10), Duration::from_millis(250), 3);
    /// 
    /// // Graceful shutdown with longer timeouts
    /// backend.shutdown(Duration::from_millis(50), Duration::from_secs(2), 5);
    /// ```
    pub fn shutdown(&mut self, poll_timeout: Duration, flush_timeout: Duration, poll_attempts: u32) {
        tracing::debug!("Starting Kafka shutdown with poll_timeout={:?}, flush_timeout={:?}, attempts={}", 
                       poll_timeout, flush_timeout, poll_attempts);
        
        // Drive any pending delivery callbacks
        for attempt in 0..poll_attempts {
            tracing::trace!("Shutdown poll attempt {}/{}", attempt + 1, poll_attempts);
            self.producer.poll(poll_timeout);
        }
        
        // Best-effort flush (ignore errors during cleanup)
        match self.producer.flush(flush_timeout) {
            Ok(_) => tracing::debug!("Shutdown flush completed successfully"),
            Err(e) => tracing::warn!("Shutdown flush failed (ignoring): {}", e),
        }
    }

    /// Create a consumer group with full configuration (get-or-create pattern)
    pub async fn create_consumer_group(&mut self, config: &KafkaReadConfig) -> Result<(), String> {
        let group_id = config.consumer_group();
        
        // FAST PATH: Check cache first (no network overhead)
        {
            let consumer_groups = self.consumer_groups.lock().unwrap();
            if consumer_groups.contains_key(group_id) {
                return Ok(()); // Already exists - no network overhead
            }
        }
        
        // SLOW PATH: Create consumer group with full configuration
        info!("Creating new consumer group '{}' with full config - this may take a moment...", group_id);
        
        let mut consumer_group = ConsumerGroup::from_kafka_config(config, &self.config)?;
        
        // Subscribe to topics from config (using EventBusConfig trait method) 
        let topics = config.topics();
        if !topics.is_empty() {
            let topic_refs: Vec<&str> = topics.iter().map(|s| s.as_str()).collect();
            if let Err(e) = consumer_group.consumer.subscribe(&topic_refs) {
                return Err(format!("Failed to subscribe consumer group '{}' to topics: {}", group_id, e));
            }
            consumer_group.topics.extend(topics.iter().cloned());
        }
        
        // Cache for future use
        self.consumer_groups.lock().unwrap().insert(group_id.to_string(), consumer_group);
        info!("Consumer group '{}' created and cached with topics: {:?}", group_id, topics);
        Ok(())
    }

    /// Manually commit a specific message offset
    pub async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) -> Result<(), String> {
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
                debug!("Successfully committed offset {} for topic '{}' partition {} in group '{}'", 
                       offset, topic, partition, group_id);
                Ok(())
            }
            Err(e) => {
                error!("Failed to commit offset {} for topic '{}' partition {} in group '{}': {}", 
                       offset, topic, partition, group_id, e);
                Err(format!("Commit failed: {}", e))
            }
        }
    }

    /// Get consumer lag for monitoring
    pub async fn get_consumer_lag(&self, topic: &str, group_id: &str) -> Result<i64, String> {
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
                return Err(format!("Failed to fetch metadata for topic '{}': {}", topic, e));
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
            let (low, high) = match consumer.fetch_watermarks(topic, partition_id, Duration::from_secs(5)) {
                Ok((l, h)) => (l, h),
                Err(e) => {
                    warn!("Failed to fetch watermarks for topic '{}' partition {}: {}", topic, partition_id, e);
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
                    warn!("Failed to get committed offsets for topic '{}' partition {}: {}", topic, partition_id, e);
                    continue;
                }
            };
            
            // Calculate lag for this partition
            if let Some(elem) = committed_offsets.elements().first() {
                match elem.offset() {
                    Offset::Offset(committed) => {
                        let lag = high - committed;
                        total_lag += lag.max(0); // Don't count negative lag
                        debug!("Partition {} lag: {} (high: {}, committed: {})", partition_id, lag, high, committed);
                    }
                    _ => {
                        // No committed offset yet, lag is the total available
                        let lag = high - low;
                        total_lag += lag.max(0);
                        debug!("Partition {} no committed offset, lag: {} (high: {}, low: {})", partition_id, lag, high, low);
                    }
                }
            }
        }
        
        debug!("Total consumer lag for topic '{}' group '{}': {}", topic, group_id, total_lag);
        Ok(total_lag)
    }

    /// Receive events with their Kafka metadata preserved
    /// Returns EventWrapper instances with complete metadata
    pub async fn receive<T: BusEvent>(&self, config: &KafkaReadConfig, messages: &mut Vec<crate::resources::EventWrapper<T>>) -> Result<(), String> {
        let group_id = config.consumer_group();
        let topics = &config.topics;
        
        // Get the consumer group
        let mut consumer_groups = self.consumer_groups.lock().unwrap();
        let consumer_group = match consumer_groups.get_mut(group_id) {
            Some(group) => group,
            None => {
                let error_msg = format!("Consumer group '{}' not found. You must create consumer groups explicitly using create_consumer_group() before calling receive()", group_id);
                error!("{}", error_msg);
                return Err(error_msg);
            }
        };
        
        // Check if partitions are assigned
        if let Ok(assignment) = consumer_group.consumer.assignment() {
            if assignment.count() == 0 {
                // No partitions assigned yet - trigger rebalancing
                consumer_group.consumer.poll(Duration::from_millis(0));
                return Ok(());
            }
        }
        
        // Poll messages and preserve metadata
        loop {
            match consumer_group.consumer.poll(Duration::from_millis(0)) {
                None => break, // No more messages
                Some(Ok(message)) => {
                    // Only process messages from topics specified in config
                    if topics.contains(&message.topic().to_string()) {
                        if let Some(payload) = message.payload() {
                            match serde_json::from_slice::<T>(payload) {
                                Ok(event) => {
                                    // Extract headers from Kafka message
                                    let mut headers = std::collections::HashMap::new();
                                    if let Some(message_headers) = message.headers() {
                                        for i in 0..message_headers.count() {
                                            let header = message_headers.get(i);
                                            if let Some(value) = header.value {
                                                if let Ok(value_str) = std::str::from_utf8(value) {
                                                    headers.insert(header.key.to_string(), value_str.to_string());
                                                }
                                            }
                                        }
                                    }
                                    
                                    // Extract key from Kafka message
                                    let key = message.key().and_then(|k| std::str::from_utf8(k).ok().map(|s| s.to_string()));
                                    
                                    // Get Kafka timestamp if available
                                    let kafka_timestamp = message.timestamp().to_millis();
                                    
                                    // Create Kafka-specific metadata
                                    let kafka_metadata = crate::resources::KafkaMetadata {
                                        topic: message.topic().to_string(),
                                        partition: message.partition(),
                                        offset: message.offset(),
                                        kafka_timestamp,
                                    };
                                    
                                    // Create EventMetadata with all the extracted information
                                    let metadata = crate::resources::EventMetadata::new(
                                        message.topic().to_string(),
                                        std::time::Instant::now(),
                                        headers,
                                        key,
                                        Some(Box::new(kafka_metadata)),
                                    );
                                    
                                    // Create EventWrapper with metadata
                                    messages.push(crate::resources::EventWrapper::new_external(event, metadata));
                                }
                                Err(e) => {
                                    tracing::warn!("Failed to deserialize event from topic {} partition {} offset {}: {}", 
                                        message.topic(), message.partition(), message.offset(), e);
                                }
                            }
                        }
                    }
                }
                Some(Err(e)) => {
                    let error_msg = format!("Error receiving from consumer group '{}': {}", group_id, e);
                    error!("{}", error_msg);
                    return Err(error_msg);
                }
            }
        }
        
        debug!("Received {} messages with metadata from consumer group '{}' for topics: {:?}", messages.len(), group_id, topics);
        Ok(())
    }

    /// Read events without auto-committing (manual commit mode) - Kafka specific
    /// 
    /// This method polls messages and returns them with commit callbacks for manual offset management.
    /// Only works with consumer groups that have manual commit enabled.
    pub async fn read_uncommitted<T: BusEvent>(&self, config: &KafkaReadConfig) -> Result<Vec<crate::config::kafka::UncommittedEvent<T>>, String> {
    if config.enable_auto_commit {
        return Err("Manual commit operations require enable_auto_commit=false in KafkaReadConfig".to_string());
    }

    let mut events: Vec<crate::resources::EventWrapper<T>> = Vec::new();
    self.receive(config, &mut events).await?;
    
    let backend_ref = std::sync::Arc::new(std::sync::Mutex::new(self.clone()));
    
    let mut uncommitted_events = Vec::new();
    
    for (index, event_wrapper) in events.into_iter().enumerate() {
        let _topics = config.topics();
        let topic = if let Some(metadata) = event_wrapper.metadata() {
            metadata.source.clone()
        } else {
            return Err(format!("Event {} lacks any metadata - events from Kafka should always have metadata", index));
        };
        
        // Extract Kafka metadata from the EventWrapper - it must be present for events from Kafka
        let kafka_metadata = if let Some(metadata) = event_wrapper.metadata() {
            if let Some(kafka_meta) = metadata.kafka_metadata() {
                crate::config::kafka::KafkaEventMetadata {
                    topic: kafka_meta.topic.clone(),
                    partition: kafka_meta.partition,
                    offset: kafka_meta.offset,
                    timestamp: kafka_meta.kafka_timestamp,  // Use actual Kafka timestamp
                    key: metadata.key.clone(),
                    headers: metadata.headers.clone(),
                }
            } else {
                return Err(format!("Event {} lacks Kafka metadata - cannot create uncommitted event for manual commit", index));
            }
        } else {
            return Err(format!("Event {} lacks any metadata - cannot create uncommitted event for manual commit", index));
        };
        
        let _backend_clone = backend_ref.clone();
        let topic_clone = topic.clone();
        
        let uncommitted_event = crate::config::kafka::UncommittedEvent::new(
            event_wrapper.into_event(), // Extract the actual event from the wrapper
            kafka_metadata,
            move || {
                // This is where we'd implement actual commit logic
                // For now, it's a placeholder that would need actual partition/offset info
                tracing::warn!("Manual commit not fully implemented - would commit for topic: {}", topic_clone);
                Ok(())
            }
        );
        
        uncommitted_events.push(uncommitted_event);
    }
        
    Ok(uncommitted_events)
}
}

impl Clone for KafkaEventBusBackend {
    fn clone(&self) -> Self {
        Self {
            config: self.config.clone(),
            producer: self.producer.clone(),
            consumer_groups: self.consumer_groups.clone(),
        }
    }
}

impl Drop for KafkaEventBusBackend {
    fn drop(&mut self) {
        // Automatic cleanup with conservative defaults
        // This is called automatically by Rust when the backend is destroyed
        self.shutdown(
            Duration::from_millis(10),   // poll_timeout 
            Duration::from_millis(250),  // flush_timeout
            3                            // poll_attempts
        );
        tracing::debug!("KafkaEventBusBackend automatic cleanup completed during destruction");
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
        // Background consumer tasks no longer needed with direct polling
        true
    }
    
    fn try_send_serialized(&self, event_json: &[u8], config: &dyn std::any::Any) -> bool {
        // Downcast to KafkaWriteConfig
        let kafka_config = match config.downcast_ref::<KafkaWriteConfig>() {
            Some(config) => config,
            None => {
                error!("Invalid config type for Kafka backend - expected KafkaWriteConfig");
                return false;
            }
        };
        use rdkafka::message::OwnedHeaders;
        
        // Extract topic from config
        let topic = &kafka_config.topic;
        
        tracing::info!("try_send_serialized called: topic={}, payload_size={}", topic, event_json.len());
        
        let headers = kafka_config.headers();
        let partition_key = &kafka_config.partition_key;
        
        // Handle the different cases separately to avoid type issues
        let success = match partition_key {
            Some(key) => {
                // Create record with key
                let mut record: BaseRecord<str, [u8]> = BaseRecord::to(topic)
                    .payload(event_json)
                    .key(key);
                
                // Add headers if provided
                if !headers.is_empty() {
                    let mut owned_headers = OwnedHeaders::new();
                    for (hkey, value) in headers {
                        owned_headers = owned_headers.insert(rdkafka::message::Header {
                            key: hkey.as_str(),
                            value: Some(value.as_str()),
                        });
                    }
                    record = record.headers(owned_headers);
                }
                
                match self.producer.send(record) {
                    Ok(_) => true,
                    Err((e, _)) => {
                        error!("Failed to enqueue message: {}", e);
                        false
                    }
                }
            }
            None => {
                // Create record without key
                let mut record: BaseRecord<(), [u8]> = BaseRecord::to(topic).payload(event_json);
                
                // Add headers if provided
                if !headers.is_empty() {
                    let mut owned_headers = OwnedHeaders::new();
                    for (hkey, value) in headers {
                        owned_headers = owned_headers.insert(rdkafka::message::Header {
                            key: hkey.as_str(),
                            value: Some(value.as_str()),
                        });
                    }
                    record = record.headers(owned_headers);
                }
                
                match self.producer.send(record) {
                    Ok(_) => true,
                    Err((e, _)) => {
                        error!("Failed to enqueue message: {}", e);
                        false
                    }
                }
            }
        };
        
        if success {
            tracing::info!("Message successfully enqueued for topic: {}", topic);
        }
        
        success
    }
    
    async fn poll_messages(&self, config: &KafkaReadConfig, messages: &mut Vec<Vec<u8>>) -> Result<(), String> {
        let group_id = config.consumer_group();
        let topics = &config.topics;
        
        // Get the consumer group and receive messages
        let mut consumer_groups = self.consumer_groups.lock().unwrap();
        let consumer_group = match consumer_groups.get_mut(group_id) {
            Some(group) => group,
            None => {
                let error_msg = format!("Consumer group '{}' not found. You must create consumer groups explicitly using create_consumer_group() before calling poll_messages()", group_id);
                error!("{}", error_msg);
                return Err(error_msg);
            }
        };
        
        // Handle multi-topic case - receive from all topics the consumer group is subscribed to
        
        // Check if partitions are assigned - if not, we can't receive messages yet
        if let Ok(assignment) = consumer_group.consumer.assignment() {
            if assignment.count() == 0 {
                // No partitions assigned yet - trigger rebalancing with a single poll
                // This is needed when consumer first joins the group
                consumer_group.consumer.poll(Duration::from_millis(0));
                // Return early - next call should have assignments ready
                return Ok(());
            }
        }
        
        // Poll all available messages with zero timeout (non-blocking)
        // Keep polling until no more messages are immediately available
        loop {
            match consumer_group.consumer.poll(Duration::from_millis(0)) {
                None => {
                    // No more messages available right now - break the loop
                    break;
                },
                Some(Ok(message)) => {
                    // Only collect messages from topics specified in config
                    if topics.contains(&message.topic().to_string()) {
                        if let Some(payload) = message.payload() {
                            messages.push(payload.to_vec());
                        }
                    }
                    // Continue polling for more messages
                }
                Some(Err(e)) => {
                    let error_msg = format!("Error receiving from consumer group '{}': {}", group_id, e);
                    error!("{}", error_msg);
                    return Err(error_msg);
                }
            }
        }
        
        debug!("Received {} messages from consumer group '{}' for topics: {:?}", messages.len(), group_id, topics);
        Ok(())
    }

    /// Flush all pending messages to Kafka broker
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
    fn flush(&self, timeout: Duration) -> bool {
        match self.producer.flush(timeout) {
            Ok(_) => {
                tracing::debug!("Successfully flushed all pending Kafka messages within {:?}", timeout);
                true
            }
            Err(e) => {
                tracing::error!("Failed to flush Kafka producer within {:?}: {}", timeout, e);
                false
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
