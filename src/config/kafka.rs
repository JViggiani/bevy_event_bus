//! Kafka-specific configuration objects for direct polling architecture

use super::{EventBusConfig, Kafka, OffsetReset, CompressionType, SecurityProtocol};
use std::collections::HashMap;

/// Configuration for Kafka read operations (consumer-focused)
#[derive(Clone, Debug)]
pub struct KafkaReadConfig {
    pub consumer_group: String,
    pub topics: Vec<String>,
    pub offset_reset: OffsetReset,
    pub enable_auto_commit: bool,
    pub session_timeout_ms: Option<u32>,
    pub additional_settings: HashMap<String, String>, // Advanced Kafka client settings
}

impl KafkaReadConfig {
    /// Create a new Kafka read configuration
    pub fn new(consumer_group: &str) -> Self {
        Self {
            consumer_group: consumer_group.to_string(),
            topics: Vec::new(),
            offset_reset: OffsetReset::Latest,
            enable_auto_commit: true,
            session_timeout_ms: None,
            additional_settings: HashMap::new(),
        }
    }
    
    /// Set topics to read from
    pub fn topics<I, T>(mut self, topics: I) -> Self 
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        self.topics = topics.into_iter().map(Into::into).collect();
        self
    }
    
    /// Set offset reset behavior
    pub fn offset_reset(mut self, offset: OffsetReset) -> Self {
        self.offset_reset = offset;
        self
    }
    
    /// Set auto-commit behavior
    pub fn auto_commit(mut self, enable: bool) -> Self {
        self.enable_auto_commit = enable;
        self
    }
    
    /// Set session timeout in milliseconds
    pub fn session_timeout_ms(mut self, timeout: u32) -> Self {
        self.session_timeout_ms = Some(timeout);
        self
    }
    
    /// Add additional Kafka client setting
    pub fn additional_setting(mut self, key: &str, value: &str) -> Self {
        self.additional_settings.insert(key.to_string(), value.to_string());
        self
    }
    
    /// Get consumer group
    pub fn consumer_group(&self) -> &str {
        &self.consumer_group
    }
}

impl EventBusConfig for KafkaReadConfig {
    type Backend = Kafka;
    
    fn topics(&self) -> &[String] {
        &self.topics
    }
}

/// Configuration for Kafka write operations (producer-focused)
#[derive(Clone, Debug)]
pub struct KafkaWriteConfig {
    pub topic: String,
    pub partition_key: Option<String>,
    pub headers: HashMap<String, String>,
    pub compression: Option<CompressionType>,
    pub additional_settings: HashMap<String, String>, // Advanced Kafka client settings
}

impl KafkaWriteConfig {
    /// Create a new Kafka write configuration
    pub fn new(topic: &str) -> Self {
        Self {
            topic: topic.to_string(),
            partition_key: None,
            headers: HashMap::new(),
            compression: None,
            additional_settings: HashMap::new(),
        }
    }
    
    /// Set partition key for message routing
    pub fn partition_key(mut self, key: &str) -> Self {
        self.partition_key = Some(key.to_string());
        self
    }
    
    /// Add a header
    pub fn header(mut self, key: &str, value: &str) -> Self {
        self.headers.insert(key.to_string(), value.to_string());
        self
    }
    
    /// Set compression type
    pub fn compression(mut self, compression: CompressionType) -> Self {
        self.compression = Some(compression);
        self
    }
    
    /// Add additional Kafka client setting
    pub fn additional_setting(mut self, key: &str, value: &str) -> Self {
        self.additional_settings.insert(key.to_string(), value.to_string());
        self
    }
    
    /// Get topic
    pub fn topic(&self) -> &str {
        &self.topic
    }
    
    /// Get headers
    pub fn headers(&self) -> &HashMap<String, String> {
        &self.headers
    }
    
    /// Set headers (for compatibility)
    pub fn with_headers(mut self, headers: HashMap<String, String>) -> Self {
        self.headers = headers;
        self
    }
    
    /// Add compression type (for compatibility)
    pub fn compression_type(mut self, compression: &str) -> Self {
        self.compression = match compression {
            "none" => None,
            "gzip" => Some(CompressionType::Gzip),
            "snappy" => Some(CompressionType::Snappy),
            "lz4" => Some(CompressionType::Lz4),
            "zstd" => Some(CompressionType::Zstd),
            _ => None,
        };
        self
    }
    
    /// Set partition key (for compatibility)
    pub fn with_partition_key(mut self, key: &str) -> Self {
        self.partition_key = Some(key.to_string());
        self
    }
}

impl EventBusConfig for KafkaWriteConfig {
    type Backend = Kafka;
    
    fn topics(&self) -> &[String] {
        std::slice::from_ref(&self.topic)
    }
}

/// Connection configuration for Kafka backend (plugin-level)
#[derive(Clone, Debug)]
pub struct KafkaConnection {
    pub bootstrap_servers: String,
    pub client_id: Option<String>,
    pub security_protocol: SecurityProtocol,
    pub timeout_ms: i32,
    pub additional_config: HashMap<String, String>, // SSL certs, SASL, auth settings
}

// Removed Default implementation - users must provide bootstrap_servers explicitly

impl KafkaConnection {
    /// Create a new Kafka connection configuration
    pub fn new(bootstrap_servers: &str) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.to_string(),
            client_id: None,
            security_protocol: SecurityProtocol::Plaintext,
            timeout_ms: 10000,
            additional_config: HashMap::new(),
        }
    }
    
    /// Set client ID
    pub fn client_id(mut self, client_id: &str) -> Self {
        self.client_id = Some(client_id.to_string());
        self
    }
    
    /// Set security protocol
    pub fn security_protocol(mut self, protocol: SecurityProtocol) -> Self {
        self.security_protocol = protocol;
        self
    }
    
    /// Set connection timeout in milliseconds
    pub fn timeout_ms(mut self, timeout: i32) -> Self {
        self.timeout_ms = timeout;
        self
    }
    
    /// Add additional connection-level configuration
    pub fn additional_config(mut self, key: &str, value: &str) -> Self {
        self.additional_config.insert(key.to_string(), value.to_string());
        self
    }
}

/// Kafka event metadata
#[derive(Debug, Clone)]
pub struct KafkaEventMetadata {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
    pub key: Option<String>,
    pub headers: HashMap<String, String>,
}

/// Uncommitted event wrapper
#[derive(Clone)]
pub struct UncommittedEvent<T> {
    pub event: T,
    pub metadata: KafkaEventMetadata,
    commit_fn: Option<fn() -> Result<(), String>>,
}

impl<T> std::fmt::Debug for UncommittedEvent<T> 
where 
    T: std::fmt::Debug,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UncommittedEvent")
            .field("event", &self.event)
            .field("metadata", &self.metadata)
            .field("has_commit_fn", &self.commit_fn.is_some())
            .finish()
    }
}

impl<T> UncommittedEvent<T> {
    pub fn new<F>(event: T, metadata: KafkaEventMetadata, _commit_fn: F) -> Self 
    where
        F: Fn() -> Result<(), String> + Send + Sync + 'static,
    {
        Self {
            event,
            metadata,
            commit_fn: Some(|| Ok(())), // Simplified for now
        }
    }
    
    pub fn commit(self) -> Result<(), String> {
        if let Some(commit_fn) = self.commit_fn {
            commit_fn()
        } else {
            Err("No commit function available".to_string())
        }
    }
}