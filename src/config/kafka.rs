//! Kafka-specific configuration objects for type-safe backend inference

use super::{EventBusConfig, Kafka, ProcessingLimits};
use std::collections::HashMap;

/// Configuration for Kafka consumers with production-ready options
#[derive(Clone, Debug)]
pub struct KafkaConsumerConfig {
    bootstrap_servers: String,
    consumer_group: String,
    topics: Vec<String>,
    auto_offset_reset: String,
    enable_auto_commit: bool,
    session_timeout_ms: u32,
    max_poll_records: u32,
    processing_limits: ProcessingLimits,
    additional_config: HashMap<String, String>,
}

impl KafkaConsumerConfig {
    /// Create a new Kafka consumer configuration
    pub fn new<I, T>(
        bootstrap_servers: impl Into<String>,
        consumer_group: impl Into<String>,
        topics: I,
    ) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            consumer_group: consumer_group.into(),
            topics: topics.into_iter().map(Into::into).collect(),
            auto_offset_reset: "latest".to_string(),
            enable_auto_commit: true,
            session_timeout_ms: 30000,
            max_poll_records: 500,
            processing_limits: ProcessingLimits::default(),
            additional_config: HashMap::new(),
        }
    }

    /// Set the consumer group for this configuration
    pub fn consumer_group(mut self, group: impl Into<String>) -> Self {
        self.consumer_group = group.into();
        self
    }

    /// Set the Kafka bootstrap servers
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.bootstrap_servers = servers.into();
        self
    }

    /// Set the topics this configuration applies to
    pub fn topics<I, T>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.topics = topics.into_iter().map(Into::into).collect();
        self
    }

    /// Set the auto offset reset behavior ("earliest", "latest", "none")
    pub fn auto_offset_reset(mut self, reset: impl Into<String>) -> Self {
        self.auto_offset_reset = reset.into();
        self
    }

    /// Enable or disable auto-commit (for manual commit control)
    pub fn enable_auto_commit(mut self, enable: bool) -> Self {
        self.enable_auto_commit = enable;
        self
    }

    /// Set session timeout in milliseconds
    pub fn session_timeout_ms(mut self, timeout: u32) -> Self {
        self.session_timeout_ms = timeout;
        self
    }

    /// Set maximum records to poll in a single request
    pub fn max_poll_records(mut self, records: u32) -> Self {
        self.max_poll_records = records;
        self
    }

    /// Set frame-level processing limits
    pub fn processing_limits(mut self, limits: ProcessingLimits) -> Self {
        self.processing_limits = limits;
        self
    }

    /// Add additional Kafka consumer configuration
    pub fn additional_config<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.additional_config.insert(key.into(), value.into());
        self
    }

    /// Get whether auto-commit is enabled
    pub fn is_auto_commit_enabled(&self) -> bool {
        self.enable_auto_commit
    }

    /// Get session timeout
    pub fn get_session_timeout_ms(&self) -> u32 {
        self.session_timeout_ms
    }

    /// Get max poll records
    pub fn get_max_poll_records(&self) -> u32 {
        self.max_poll_records
    }

    /// Get processing limits
    pub fn get_processing_limits(&self) -> &ProcessingLimits {
        &self.processing_limits
    }

    /// Get additional config
    pub fn get_additional_config(&self) -> &HashMap<String, String> {
        &self.additional_config
    }

    /// Get auto offset reset setting
    pub fn get_auto_offset_reset(&self) -> &str {
        &self.auto_offset_reset
    }

    /// Get bootstrap servers
    pub fn get_bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Get consumer group
    pub fn get_consumer_group(&self) -> &str {
        &self.consumer_group
    }
}

impl EventBusConfig for KafkaConsumerConfig {
    type Backend = Kafka;

    fn topics(&self) -> &[String] {
        &self.topics
    }

    fn config_id(&self) -> String {
        format!("kafka_consumer_{}", self.consumer_group)
    }
}

/// Configuration for Kafka producers with production-ready options
#[derive(Clone, Debug)]
pub struct KafkaProducerConfig {
    bootstrap_servers: String,
    topics: Vec<String>,
    acks: String,
    retries: u32,
    compression_type: String,
    batch_size: u32,
    linger_ms: u32,
    request_timeout_ms: u32,
    additional_config: HashMap<String, String>,
}

impl KafkaProducerConfig {
    /// Create a new Kafka producer configuration
    pub fn new<I, T>(bootstrap_servers: impl Into<String>, topics: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            topics: topics.into_iter().map(Into::into).collect(),
            acks: "1".to_string(), // Wait for leader acknowledgment
            retries: 3,
            compression_type: "none".to_string(),
            batch_size: 16384,
            linger_ms: 0,
            request_timeout_ms: 30000,
            additional_config: HashMap::new(),
        }
    }

    /// Set the Kafka bootstrap servers
    pub fn bootstrap_servers(mut self, servers: impl Into<String>) -> Self {
        self.bootstrap_servers = servers.into();
        self
    }

    /// Set the topics this producer will write to
    pub fn topics<I, T>(mut self, topics: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.topics = topics.into_iter().map(Into::into).collect();
        self
    }

    /// Set acknowledgment requirement ("0", "1", "all")
    pub fn acks(mut self, acks: impl Into<String>) -> Self {
        self.acks = acks.into();
        self
    }

    /// Set number of retries
    pub fn retries(mut self, retries: u32) -> Self {
        self.retries = retries;
        self
    }

    /// Set compression type ("none", "gzip", "snappy", "lz4", "zstd")
    pub fn compression_type(mut self, compression: impl Into<String>) -> Self {
        self.compression_type = compression.into();
        self
    }

    /// Set batch size in bytes
    pub fn batch_size(mut self, size: u32) -> Self {
        self.batch_size = size;
        self
    }

    /// Set linger time in milliseconds
    pub fn linger_ms(mut self, linger: u32) -> Self {
        self.linger_ms = linger;
        self
    }

    /// Set request timeout in milliseconds
    pub fn request_timeout_ms(mut self, timeout: u32) -> Self {
        self.request_timeout_ms = timeout;
        self
    }

    /// Add additional Kafka producer configuration
    pub fn additional_config<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.additional_config.insert(key.into(), value.into());
        self
    }

    /// Get acknowledgment setting
    pub fn get_acks(&self) -> &str {
        &self.acks
    }

    /// Get retries setting
    pub fn get_retries(&self) -> u32 {
        self.retries
    }

    /// Get compression type
    pub fn get_compression_type(&self) -> &str {
        &self.compression_type
    }

    /// Get batch size
    pub fn get_batch_size(&self) -> u32 {
        self.batch_size
    }

    /// Get linger time
    pub fn get_linger_ms(&self) -> u32 {
        self.linger_ms
    }

    /// Get request timeout
    pub fn get_request_timeout_ms(&self) -> u32 {
        self.request_timeout_ms
    }

    /// Get additional config
    pub fn get_additional_config(&self) -> &HashMap<String, String> {
        &self.additional_config
    }

    /// Get bootstrap servers
    pub fn get_bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }
}

impl Default for KafkaProducerConfig {
    fn default() -> Self {
        Self::new("localhost:9092", Vec::<String>::new())
    }
}

impl EventBusConfig for KafkaProducerConfig {
    type Backend = Kafka;

    fn topics(&self) -> &[String] {
        &self.topics
    }

    fn config_id(&self) -> String {
        "kafka_producer".to_string()
    }
}

/// Kafka-specific event metadata for manual commit functionality
#[derive(Debug, Clone)]
pub struct KafkaEventMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<String>,
    pub headers: HashMap<String, String>,
}

/// Event with manual commit capability (Kafka-only)
pub struct UncommittedEvent<T> {
    event: T,
    metadata: KafkaEventMetadata,
    commit_fn: Option<Box<dyn FnOnce() -> Result<(), String> + Send + Sync>>,
}

impl<T> std::fmt::Debug for UncommittedEvent<T>
where
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UncommittedEvent")
            .field("event", &self.event)
            .field("metadata", &self.metadata)
            .field("commit_fn", &self.commit_fn.is_some())
            .finish()
    }
}

impl<T> UncommittedEvent<T> {
    /// Create a new uncommitted event
    pub fn new(
        event: T,
        metadata: KafkaEventMetadata,
        commit_fn: impl FnOnce() -> Result<(), String> + Send + Sync + 'static,
    ) -> Self {
        Self {
            event,
            metadata,
            commit_fn: Some(Box::new(commit_fn)),
        }
    }

    /// Get the event data
    pub fn event(&self) -> &T {
        &self.event
    }

    /// Get the Kafka metadata
    pub fn metadata(&self) -> &KafkaEventMetadata {
        &self.metadata
    }

    /// Manually commit this event's offset
    pub fn commit(mut self) -> Result<(), String> {
        if let Some(commit_fn) = self.commit_fn.take() {
            commit_fn()
        } else {
            Err("Event already committed".to_string())
        }
    }

    /// Check if this event needs manual commit
    pub fn needs_commit(&self) -> bool {
        self.commit_fn.is_some()
    }
}

impl<T> std::ops::Deref for UncommittedEvent<T> {
    type Target = T;

    fn deref(&self) -> &Self::Target {
        &self.event
    }
}
