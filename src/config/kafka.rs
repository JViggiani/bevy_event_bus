//! Kafka-specific configuration objects for type-safe backend inference

use super::{EventBusConfig, Kafka, ProcessingLimits, TopologyMode};
use crate::{
    BusEvent, EventBusError, backends::event_bus_backend::EventBusBackendConfig,
    decoder::DecoderRegistry,
};
use bevy::prelude::{App, Message};
use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// Channel capacity configuration for the Kafka backend's internal queues.
#[derive(Clone, Debug)]
pub struct KafkaChannelCapacities {
    pub message: usize,
    pub commit: usize,
    pub result: usize,
}

impl KafkaChannelCapacities {
    pub fn new(message: usize, commit: usize, result: usize) -> Self {
        Self {
            message,
            commit,
            result,
        }
    }

    pub fn message_capacity(mut self, capacity: usize) -> Self {
        self.message = capacity;
        self
    }

    pub fn commit_capacity(mut self, capacity: usize) -> Self {
        self.commit = capacity;
        self
    }

    pub fn result_capacity(mut self, capacity: usize) -> Self {
        self.result = capacity;
        self
    }
}

impl Default for KafkaChannelCapacities {
    fn default() -> Self {
        Self {
            message: 10_000,
            commit: 2_048,
            result: 1_024,
        }
    }
}

/// Connection configuration for Kafka backend initialization
#[derive(Clone, Debug)]
pub struct KafkaConnectionConfig {
    bootstrap_servers: String,
    client_id: Option<String>,
    timeout_ms: i32,
    additional_config: HashMap<String, String>,
}

impl KafkaConnectionConfig {
    /// Create a new connection configuration with required bootstrap servers.
    pub fn new(bootstrap_servers: impl Into<String>) -> Self {
        Self {
            bootstrap_servers: bootstrap_servers.into(),
            client_id: None,
            timeout_ms: 10_000,
            additional_config: HashMap::new(),
        }
    }

    /// Builder-style setter for client id.
    pub fn set_client_id(mut self, client_id: impl Into<String>) -> Self {
        self.client_id = Some(client_id.into());
        self
    }

    /// Builder-style setter for timeout in milliseconds.
    pub fn set_timeout_ms(mut self, timeout_ms: i32) -> Self {
        self.timeout_ms = timeout_ms;
        self
    }

    /// Builder-style setter for additional key/value configuration pairs.
    pub fn insert_additional_config<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.additional_config.insert(key.into(), value.into());
        self
    }

    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    pub fn client_id(&self) -> Option<&str> {
        self.client_id.as_deref()
    }

    pub fn timeout_ms(&self) -> i32 {
        self.timeout_ms
    }

    pub fn additional_config(&self) -> &HashMap<String, String> {
        &self.additional_config
    }
}

impl Default for KafkaConnectionConfig {
    fn default() -> Self {
        Self::new("localhost:9092")
    }
}

#[derive(Clone, Debug)]
pub struct KafkaTopologyEventBinding {
    topics: Vec<String>,
    register_fn: fn(&mut App, &[String]),
}

impl KafkaTopologyEventBinding {
    pub fn new<T: BusEvent + Message>(topics: Vec<String>) -> Self {
        Self {
            topics,
            register_fn: register_event_binding::<T>,
        }
    }

    pub fn apply(&self, app: &mut App) {
        (self.register_fn)(app, &self.topics);
    }

    pub fn topics(&self) -> &[String] {
        &self.topics
    }
}

fn register_event_binding<T: BusEvent + Message>(app: &mut App, topics: &[String]) {
    App::add_message::<T>(app);
    App::add_message::<EventBusError<T>>(app);

    if !app.world().contains_resource::<DecoderRegistry>() {
        app.world_mut().insert_resource(DecoderRegistry::new());
    }

    let mut registry = app.world_mut().resource_mut::<DecoderRegistry>();
    for topic in topics {
        registry.register_json_decoder::<T>(topic);
    }

    crate::writers::outbound_bridge::ensure_bridge::<T>(app, topics);
}

/// Controls the topics, consumer groups and behaviour that the backend prepares at startup.
#[derive(Clone, Debug, Default)]
pub struct KafkaTopologyConfig {
    topics: Vec<KafkaTopicSpec>,
    consumer_groups: HashMap<String, KafkaConsumerGroupSpec>,
    event_bindings: Vec<KafkaTopologyEventBinding>,
}

impl KafkaTopologyConfig {
    pub fn new(
        topics: Vec<KafkaTopicSpec>,
        consumer_groups: HashMap<String, KafkaConsumerGroupSpec>,
        event_bindings: Vec<KafkaTopologyEventBinding>,
    ) -> Self {
        Self {
            topics,
            consumer_groups,
            event_bindings,
        }
    }

    pub fn builder() -> KafkaTopologyBuilder {
        KafkaTopologyBuilder::default()
    }

    pub fn topics(&self) -> &[KafkaTopicSpec] {
        &self.topics
    }

    pub fn consumer_groups(&self) -> &HashMap<String, KafkaConsumerGroupSpec> {
        &self.consumer_groups
    }

    pub fn topic_names(&self) -> HashSet<String> {
        self.topics.iter().map(|t| t.name.clone()).collect()
    }

    pub fn event_bindings(&self) -> &[KafkaTopologyEventBinding] {
        &self.event_bindings
    }
}

#[derive(Default)]
pub struct KafkaTopologyBuilder {
    topics: Vec<KafkaTopicSpec>,
    consumer_groups: HashMap<String, KafkaConsumerGroupSpec>,
    event_bindings: Vec<KafkaTopologyEventBinding>,
}

impl KafkaTopologyBuilder {
    pub fn add_topic(&mut self, topic: KafkaTopicSpec) -> &mut Self {
        self.topics.push(topic);
        self
    }

    pub fn add_topics<T: IntoIterator<Item = KafkaTopicSpec>>(&mut self, topics: T) -> &mut Self {
        self.topics.extend(topics);
        self
    }

    pub fn add_consumer_group(
        &mut self,
        id: impl Into<String>,
        spec: KafkaConsumerGroupSpec,
    ) -> &mut Self {
        self.consumer_groups.insert(id.into(), spec);
        self
    }

    pub fn add_event<T: BusEvent + Message>(
        &mut self,
        topics: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self {
        let topics_vec: Vec<String> = topics.into_iter().map(Into::into).collect();
        self.event_bindings
            .push(KafkaTopologyEventBinding::new::<T>(topics_vec));
        self
    }

    pub fn add_event_single<T: BusEvent + Message>(
        &mut self,
        topic: impl Into<String>,
    ) -> &mut Self {
        self.add_event::<T>([topic.into()])
    }

    pub fn build(self) -> KafkaTopologyConfig {
        KafkaTopologyConfig::new(self.topics, self.consumer_groups, self.event_bindings)
    }
}

/// Topic configuration used when provisioning Kafka at startup.
#[derive(Clone, Debug)]
pub struct KafkaTopicSpec {
    pub name: String,
    pub partitions: Option<i32>,
    pub replication: Option<i16>,
    pub mode: TopologyMode,
}

impl KafkaTopicSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            partitions: None,
            replication: None,
            mode: TopologyMode::Provision,
        }
    }

    pub fn partitions(mut self, partitions: i32) -> Self {
        self.partitions = Some(partitions);
        self
    }

    pub fn replication(mut self, replication: i16) -> Self {
        self.replication = Some(replication);
        self
    }

    /// Override the topology mode used when preparing this topic.
    pub fn mode(mut self, mode: TopologyMode) -> Self {
        self.mode = mode;
        self
    }
}

/// Enumeration describing how consumers should position when starting.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum KafkaInitialOffset {
    Earliest,
    Latest,
    None,
}

#[derive(Clone, Debug)]
pub struct KafkaConsumerGroupSpec {
    pub topics: Vec<String>,
    pub manual_commits: bool,
    pub initial_offset: KafkaInitialOffset,
    pub mode: TopologyMode,
}

impl KafkaConsumerGroupSpec {
    pub fn new<T: IntoIterator<Item = impl Into<String>>>(topics: T) -> Self {
        Self {
            topics: topics.into_iter().map(Into::into).collect(),
            manual_commits: false,
            initial_offset: KafkaInitialOffset::Latest,
            mode: TopologyMode::Provision,
        }
    }

    pub fn manual_commits(mut self, manual: bool) -> Self {
        self.manual_commits = manual;
        self
    }

    pub fn initial_offset(mut self, offset: KafkaInitialOffset) -> Self {
        self.initial_offset = offset;
        self
    }

    /// Override the topology mode used when preparing this consumer group.
    pub fn mode(mut self, mode: TopologyMode) -> Self {
        self.mode = mode;
        self
    }
}

/// Aggregate configuration consumed by the Kafka backend during construction.
#[derive(Clone, Debug)]
pub struct KafkaBackendConfig {
    pub connection: KafkaConnectionConfig,
    pub topology: KafkaTopologyConfig,
    pub consumer_lag_poll_interval: Duration,
    pub channel_capacities: KafkaChannelCapacities,
}

impl KafkaBackendConfig {
    pub fn new(
        connection: KafkaConnectionConfig,
        topology: KafkaTopologyConfig,
        consumer_lag_poll_interval: Duration,
    ) -> Self {
        Self {
            connection,
            topology,
            consumer_lag_poll_interval,
            channel_capacities: KafkaChannelCapacities::default(),
        }
    }

    pub fn channel_capacities(mut self, capacities: KafkaChannelCapacities) -> Self {
        self.channel_capacities = capacities;
        self
    }

    pub fn get_channel_capacities(&self) -> &KafkaChannelCapacities {
        &self.channel_capacities
    }
}

impl EventBusBackendConfig for KafkaBackendConfig {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

/// Configuration for Kafka consumers with production-ready options
#[derive(Clone, Debug)]
pub struct KafkaConsumerConfig {
    consumer_group: String,
    topics: Vec<String>,
    auto_offset_reset: String,
    enable_auto_commit: bool,
    session_timeout: Duration,
    max_poll_records: u32,
    processing_limits: ProcessingLimits,
    additional_config: HashMap<String, String>,
}

impl KafkaConsumerConfig {
    /// Create a new Kafka consumer configuration
    pub fn new<I, T>(consumer_group: impl Into<String>, topics: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self {
            consumer_group: consumer_group.into(),
            topics: topics.into_iter().map(Into::into).collect(),
            auto_offset_reset: "latest".to_string(),
            enable_auto_commit: true,
            session_timeout: Duration::from_millis(30_000),
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

    /// Set the consumer session timeout.
    pub fn session_timeout(mut self, timeout: Duration) -> Self {
        self.session_timeout = timeout;
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

    /// Get the consumer session timeout.
    pub fn get_session_timeout(&self) -> Duration {
        self.session_timeout
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Configuration for Kafka producers with production-ready options
#[derive(Clone, Debug)]
pub struct KafkaProducerConfig {
    topics: Vec<String>,
    acks: String,
    retries: u32,
    compression_type: String,
    batch_size: u32,
    linger_ms: u32,
    request_timeout_ms: u32,
    partition_key: Option<String>,
    headers: HashMap<String, String>,
    additional_config: HashMap<String, String>,
}

impl KafkaProducerConfig {
    /// Create a new Kafka producer configuration
    pub fn new<I, T>(topics: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: Into<String>,
    {
        Self {
            topics: topics.into_iter().map(Into::into).collect(),
            acks: "1".to_string(), // Wait for leader acknowledgment
            retries: 3,
            compression_type: "none".to_string(),
            batch_size: 16384,
            linger_ms: 0,
            request_timeout_ms: 30000,
            partition_key: None,
            headers: HashMap::new(),
            additional_config: HashMap::new(),
        }
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

    /// Set a partition key that will be applied to every message produced with this config.
    pub fn partition_key(mut self, key: impl Into<String>) -> Self {
        self.partition_key = Some(key.into());
        self
    }

    /// Clear any configured partition key.
    pub fn clear_partition_key(mut self) -> Self {
        self.partition_key = None;
        self
    }

    /// Add or replace a single header that will accompany produced messages.
    pub fn header(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Replace all headers with the provided map.
    pub fn headers_map<I, K, V>(mut self, headers: I) -> Self
    where
        I: IntoIterator<Item = (K, V)>,
        K: Into<String>,
        V: Into<String>,
    {
        self.headers.clear();
        for (key, value) in headers {
            self.headers.insert(key.into(), value.into());
        }
        self
    }

    /// Remove all configured headers.
    pub fn clear_headers(mut self) -> Self {
        self.headers.clear();
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

    /// Get the configured partition key for produced messages, if any.
    pub fn get_partition_key(&self) -> Option<&str> {
        self.partition_key.as_deref()
    }

    /// Get the configured headers that will be attached to produced messages.
    pub fn get_headers(&self) -> &HashMap<String, String> {
        &self.headers
    }

    /// Get additional config
    pub fn get_additional_config(&self) -> &HashMap<String, String> {
        &self.additional_config
    }
}

impl Default for KafkaProducerConfig {
    fn default() -> Self {
        Self::new(Vec::<String>::new())
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

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Kafka-specific event metadata for manual commit functionality
#[derive(Debug, Clone)]
pub struct KafkaMessageMetadata {
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
    metadata: KafkaMessageMetadata,
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
        metadata: KafkaMessageMetadata,
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
    pub fn metadata(&self) -> &KafkaMessageMetadata {
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
