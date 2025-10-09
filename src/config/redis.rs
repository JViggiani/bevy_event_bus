//! Redis-specific configuration objects for topology-driven backend setup

use super::{EventBusConfig, Redis};
use crate::{
    BusEvent, EventBusError, backends::event_bus_backend::EventBusBackendConfig,
    decoder::DecoderRegistry,
};
use bevy::prelude::{App, Event};
use std::collections::{HashMap, HashSet};
use std::time::Duration;

/// Connection configuration for the Redis backend
#[derive(Clone, Debug)]
pub struct RedisConnectionConfig {
    connection_string: String,
    username: Option<String>,
    password: Option<String>,
    pool_size: usize,
    connect_timeout: Duration,
    additional_config: HashMap<String, String>,
}

impl RedisConnectionConfig {
    /// Create a new connection configuration pointing at the supplied connection string.
    pub fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
            username: None,
            password: None,
            pool_size: 8,
            connect_timeout: Duration::from_secs(5),
            additional_config: HashMap::new(),
        }
    }

    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    pub fn username(&self) -> Option<&str> {
        self.username.as_deref()
    }

    pub fn password(&self) -> Option<&str> {
        self.password.as_deref()
    }

    pub fn pool_size(&self) -> usize {
        self.pool_size
    }

    pub fn connect_timeout(&self) -> Duration {
        self.connect_timeout
    }

    pub fn additional_config(&self) -> &HashMap<String, String> {
        &self.additional_config
    }

    pub fn set_username(mut self, username: impl Into<String>) -> Self {
        self.username = Some(username.into());
        self
    }

    pub fn set_password(mut self, password: impl Into<String>) -> Self {
        self.password = Some(password.into());
        self
    }

    pub fn set_pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = pool_size;
        self
    }

    pub fn set_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    pub fn insert_additional_config<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.additional_config.insert(key.into(), value.into());
        self
    }
}

#[derive(Clone, Debug)]
pub struct RedisTopologyEventBinding {
    streams: Vec<String>,
    register_fn: fn(&mut App, &[String]),
}

impl RedisTopologyEventBinding {
    pub fn new<T: BusEvent + Event>(streams: Vec<String>) -> Self {
        Self {
            streams,
            register_fn: register_event_binding::<T>,
        }
    }

    pub fn apply(&self, app: &mut App) {
        (self.register_fn)(app, &self.streams);
    }

    pub fn streams(&self) -> &[String] {
        &self.streams
    }
}

fn register_event_binding<T: BusEvent + Event>(app: &mut App, streams: &[String]) {
    App::add_event::<T>(app);
    App::add_event::<EventBusError<T>>(app);

    if !app.world().contains_resource::<DecoderRegistry>() {
        app.world_mut().insert_resource(DecoderRegistry::new());
    }

    let mut registry = app.world_mut().resource_mut::<DecoderRegistry>();
    for stream in streams {
        registry.register_json_decoder::<T>(stream);
    }

    crate::writers::outbound_bridge::ensure_bridge::<T>(app, streams);
}

/// Stream specification used when provisioning Redis Streams.
#[derive(Clone, Debug)]
pub struct RedisStreamSpec {
    pub name: String,
    pub maxlen: Option<usize>,
}

impl RedisStreamSpec {
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            maxlen: None,
        }
    }

    pub fn maxlen(mut self, maxlen: usize) -> Self {
        self.maxlen = Some(maxlen);
        self
    }
}

/// Consumer group specification for Redis Streams.
#[derive(Clone, Debug)]
pub struct RedisConsumerGroupSpec {
    pub streams: Vec<String>,
    pub consumer_group: String,
    pub consumer_name: String,
    pub manual_ack: bool,
    pub start_id: String,
}

impl RedisConsumerGroupSpec {
    /// Construct a specification for a Redis consumer group that will be provisioned as part of
    /// the topology. The consumer will only observe new messages by default; use
    /// [`Self::start_id`] to opt into backlog replay.
    pub fn new<I, S>(
        streams: I,
        consumer_group: impl Into<String>,
        consumer_name: impl Into<String>,
    ) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let consumer_name = consumer_name.into();
        assert!(
            !consumer_name.trim().is_empty(),
            "consumer_name must not be empty when defining a Redis consumer group"
        );
        let consumer_group = consumer_group.into();
        assert!(
            !consumer_group.trim().is_empty(),
            "consumer_group must not be empty when defining a Redis consumer group"
        );
        Self {
            streams: streams.into_iter().map(Into::into).collect(),
            consumer_group,
            consumer_name,
            manual_ack: false,
            start_id: "$".to_string(),
        }
    }

    pub fn manual_ack(mut self, manual: bool) -> Self {
        self.manual_ack = manual;
        self
    }

    /// Override the stream position used when creating the consumer group.
    ///
    /// By default this is set to `$`, which instructs Redis to deliver only messages appended
    /// after the group is created. To replay the full backlog instead, set the value to
    /// `"0-0"` or any other valid Redis stream entry identifier.
    pub fn start_id(mut self, id: impl Into<String>) -> Self {
        self.start_id = id.into();
        self
    }
}

/// Aggregate configuration consumed by the Redis backend during construction.
#[derive(Clone, Debug)]
pub struct RedisTopologyConfig {
    streams: Vec<RedisStreamSpec>,
    consumer_groups: HashMap<String, RedisConsumerGroupSpec>,
    event_bindings: Vec<RedisTopologyEventBinding>,
}

impl RedisTopologyConfig {
    pub fn new(
        streams: Vec<RedisStreamSpec>,
        consumer_groups: HashMap<String, RedisConsumerGroupSpec>,
        event_bindings: Vec<RedisTopologyEventBinding>,
    ) -> Self {
        Self {
            streams,
            consumer_groups,
            event_bindings,
        }
    }

    pub fn builder() -> RedisTopologyBuilder {
        RedisTopologyBuilder::default()
    }

    pub fn streams(&self) -> &[RedisStreamSpec] {
        &self.streams
    }

    pub fn consumer_groups(&self) -> &HashMap<String, RedisConsumerGroupSpec> {
        &self.consumer_groups
    }

    pub fn stream_names(&self) -> HashSet<String> {
        self.streams.iter().map(|s| s.name.clone()).collect()
    }

    pub fn event_bindings(&self) -> &[RedisTopologyEventBinding] {
        &self.event_bindings
    }
}

impl Default for RedisTopologyConfig {
    fn default() -> Self {
        Self {
            streams: Vec::new(),
            consumer_groups: HashMap::new(),
            event_bindings: Vec::new(),
        }
    }
}

#[derive(Default)]
pub struct RedisTopologyBuilder {
    streams: Vec<RedisStreamSpec>,
    consumer_groups: HashMap<String, RedisConsumerGroupSpec>,
    event_bindings: Vec<RedisTopologyEventBinding>,
}

impl RedisTopologyBuilder {
    pub fn add_stream(&mut self, stream: RedisStreamSpec) -> &mut Self {
        self.streams.push(stream);
        self
    }

    pub fn add_streams<I>(&mut self, streams: I) -> &mut Self
    where
        I: IntoIterator<Item = RedisStreamSpec>,
    {
        self.streams.extend(streams);
        self
    }

    pub fn add_consumer_group(&mut self, spec: RedisConsumerGroupSpec) -> &mut Self {
        let key = spec.consumer_group.clone();
        assert!(
            !self.consumer_groups.contains_key(&key),
            "consumer group '{key}' is already defined in this topology"
        );
        self.consumer_groups.insert(key, spec);
        self
    }

    pub fn add_event<T: BusEvent + Event>(
        &mut self,
        streams: impl IntoIterator<Item = impl Into<String>>,
    ) -> &mut Self {
        let streams_vec: Vec<String> = streams.into_iter().map(Into::into).collect();
        self.event_bindings
            .push(RedisTopologyEventBinding::new::<T>(streams_vec));
        self
    }

    pub fn add_event_single<T: BusEvent + Event>(
        &mut self,
        stream: impl Into<String>,
    ) -> &mut Self {
        self.add_event::<T>([stream.into()])
    }

    pub fn build(self) -> RedisTopologyConfig {
        RedisTopologyConfig::new(self.streams, self.consumer_groups, self.event_bindings)
    }
}

/// Runtime tuning parameters controlling buffering and polling behaviour for the Redis backend.
#[derive(Clone, Debug)]
pub struct RedisRuntimeTuning {
    pub message_channel_capacity: usize,
    pub ack_channel_capacity: usize,
    pub trim_channel_capacity: usize,
    pub read_batch_size: usize,
    pub empty_read_backoff: Duration,
    pub ack_batch_size: usize,
    pub ack_flush_interval: Duration,
}

impl RedisRuntimeTuning {
    pub fn new(
        message_channel_capacity: usize,
        ack_channel_capacity: usize,
        trim_channel_capacity: usize,
        read_batch_size: usize,
        empty_read_backoff: Duration,
        ack_batch_size: usize,
        ack_flush_interval: Duration,
    ) -> Self {
        Self {
            message_channel_capacity,
            ack_channel_capacity,
            trim_channel_capacity,
            read_batch_size,
            empty_read_backoff,
            ack_batch_size,
            ack_flush_interval,
        }
    }

    pub fn message_channel_capacity(mut self, capacity: usize) -> Self {
        self.message_channel_capacity = capacity;
        self
    }

    pub fn ack_channel_capacity(mut self, capacity: usize) -> Self {
        self.ack_channel_capacity = capacity;
        self
    }

    pub fn trim_channel_capacity(mut self, capacity: usize) -> Self {
        self.trim_channel_capacity = capacity;
        self
    }

    pub fn read_batch_size(mut self, batch: usize) -> Self {
        self.read_batch_size = batch;
        self
    }

    pub fn empty_read_backoff(mut self, backoff: Duration) -> Self {
        self.empty_read_backoff = backoff;
        self
    }

    pub fn ack_batch_size(mut self, batch: usize) -> Self {
        self.ack_batch_size = batch;
        self
    }

    pub fn ack_flush_interval(mut self, interval: Duration) -> Self {
        self.ack_flush_interval = interval;
        self
    }
}

impl Default for RedisRuntimeTuning {
    fn default() -> Self {
        Self {
            message_channel_capacity: 10_000,
            ack_channel_capacity: 2_048,
            trim_channel_capacity: 128,
            read_batch_size: 128,
            empty_read_backoff: Duration::from_millis(5),
            ack_batch_size: 128,
            ack_flush_interval: Duration::from_millis(2),
        }
    }
}

/// Backend configuration wrapper used to construct the Redis backend.
#[derive(Clone, Debug)]
pub struct RedisBackendConfig {
    pub connection: RedisConnectionConfig,
    pub topology: RedisTopologyConfig,
    pub read_block_timeout: Duration,
    pub runtime: RedisRuntimeTuning,
}

impl RedisBackendConfig {
    pub fn new(
        connection: RedisConnectionConfig,
        topology: RedisTopologyConfig,
        read_block_timeout: Duration,
    ) -> Self {
        Self {
            connection,
            topology,
            read_block_timeout,
            runtime: RedisRuntimeTuning::default(),
        }
    }

    pub fn runtime_tuning(mut self, runtime: RedisRuntimeTuning) -> Self {
        self.runtime = runtime;
        self
    }

    pub fn get_runtime_tuning(&self) -> &RedisRuntimeTuning {
        &self.runtime
    }
}

impl EventBusBackendConfig for RedisBackendConfig {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Consumer configuration for Redis-backed readers.
#[derive(Clone, Debug)]
pub struct RedisConsumerConfig {
    streams: Vec<String>,
    consumer_group: Option<String>,
    consumer_name: Option<String>,
    read_block_timeout: Duration,
}

impl RedisConsumerConfig {
    /// Create a new consumer configuration bound to the supplied consumer group and streams.
    ///
    /// This mirrors the API style of [`KafkaConsumerConfig::new`], requiring the caller to
    /// specify both the logical group identifier and the set of streams that should be polled.
    pub fn new<G, N, I, S>(consumer_group: G, consumer_name: N, streams: I) -> Self
    where
        G: Into<String>,
        N: Into<String>,
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let consumer_group = consumer_group.into();
        assert!(
            !consumer_group.trim().is_empty(),
            "consumer_group must not be empty when constructing a Redis consumer config"
        );

        let consumer_name = consumer_name.into();
        assert!(
            !consumer_name.trim().is_empty(),
            "consumer_name must not be empty when constructing a Redis consumer config"
        );

        let streams: Vec<String> = streams.into_iter().map(Into::into).collect();
        validate_streams(&streams);
        Self {
            streams,
            consumer_group: Some(consumer_group),
            consumer_name: Some(consumer_name),
            read_block_timeout: Duration::from_millis(100),
        }
    }

    /// Create a consumer configuration without a consumer group, suitable for scenarios where
    /// the reader should receive every message regardless of group membership.
    pub fn ungrouped<I, S>(streams: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        let streams: Vec<String> = streams.into_iter().map(Into::into).collect();
        validate_streams(&streams);
        Self {
            streams,
            consumer_group: None,
            consumer_name: None,
            read_block_timeout: Duration::from_millis(100),
        }
    }

    /// Replace the consumer group associated with this configuration.
    pub fn set_consumer_group(mut self, consumer_group: impl Into<String>) -> Self {
        let consumer_group = consumer_group.into();
        assert!(
            !consumer_group.trim().is_empty(),
            "consumer_group must not be empty when calling set_consumer_group"
        );
        self.consumer_group = Some(consumer_group);
        self
    }

    /// Override the blocking read timeout used when polling Redis Streams.
    pub fn read_block_timeout(mut self, timeout: Duration) -> Self {
        self.read_block_timeout = timeout;
        self
    }

    /// Return the list of streams targeted by this configuration.
    pub fn streams(&self) -> &[String] {
        &self.streams
    }

    /// Convenience accessor returning the first configured stream, if any.
    pub fn primary_stream(&self) -> Option<&str> {
        self.streams.first().map(String::as_str)
    }

    /// Return the configured consumer group, if any.
    pub fn consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    /// Return the configured consumer name, if any.
    pub fn consumer_name(&self) -> Option<&str> {
        self.consumer_name.as_deref()
    }

    /// Return the blocking read timeout.
    pub fn block_timeout(&self) -> Duration {
        self.read_block_timeout
    }
}

fn validate_streams(streams: &[String]) {
    assert!(
        !streams.is_empty(),
        "at least one stream must be specified when constructing a Redis consumer config"
    );
    assert!(
        streams.iter().all(|stream| !stream.trim().is_empty()),
        "stream names must not be empty or whitespace when constructing a Redis consumer config"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_requires_non_empty_consumer_group() {
        let result = std::panic::catch_unwind(|| {
            RedisConsumerConfig::new("  ", "reader", ["stream"]);
        });
        assert!(result.is_err());
    }

    #[test]
    fn new_requires_non_empty_stream_list() {
        let result = std::panic::catch_unwind(|| {
            RedisConsumerConfig::new("group", "reader", Vec::<String>::new());
        });
        assert!(result.is_err());
    }

    #[test]
    fn ungrouped_disallows_empty_stream_name() {
        let result = std::panic::catch_unwind(|| {
            RedisConsumerConfig::ungrouped([" "]);
        });
        assert!(result.is_err());
    }

    #[test]
    fn set_consumer_group_rejects_empty_value() {
        let result = std::panic::catch_unwind(|| {
            RedisConsumerConfig::ungrouped(["stream"]).set_consumer_group(" ");
        });
        assert!(result.is_err());
    }

    #[test]
    fn valid_configuration_succeeds() {
        let config = RedisConsumerConfig::new("group", "reader", ["stream"]);
        assert_eq!(config.consumer_group(), Some("group"));
        assert_eq!(config.consumer_name(), Some("reader"));
        assert_eq!(config.streams(), &[String::from("stream")]);
    }
}

impl EventBusConfig for RedisConsumerConfig {
    type Backend = Redis;

    fn topics(&self) -> &[String] {
        &self.streams
    }

    fn config_id(&self) -> String {
        let mut id = format!("redis:{}", self.streams.join(","));
        if let Some(group) = &self.consumer_group {
            id.push(':');
            id.push_str(group);
        }
        id
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Producer configuration for Redis-backed writers.
#[derive(Clone, Debug)]
pub struct RedisProducerConfig {
    stream: String,
    maxlen: Option<usize>,
    trim_strategy: TrimStrategy,
}

impl RedisProducerConfig {
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            maxlen: None,
            trim_strategy: TrimStrategy::Approximate,
        }
    }

    pub fn maxlen(mut self, maxlen: usize) -> Self {
        self.maxlen = Some(maxlen);
        self
    }

    pub fn set_trim_strategy(mut self, strategy: TrimStrategy) -> Self {
        self.trim_strategy = strategy;
        self
    }

    pub fn stream(&self) -> &str {
        &self.stream
    }

    pub fn maxlen_value(&self) -> Option<usize> {
        self.maxlen
    }

    pub fn trim_strategy(&self) -> TrimStrategy {
        self.trim_strategy
    }
}

impl EventBusConfig for RedisProducerConfig {
    type Backend = Redis;

    fn topics(&self) -> &[String] {
        std::slice::from_ref(&self.stream)
    }

    fn config_id(&self) -> String {
        format!("redis_producer:{}", self.stream)
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

/// Trimming strategies supported when writing to Redis Streams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrimStrategy {
    Exact,
    Approximate,
}
