//! Redis-specific configuration objects for topology-driven backend setup

use super::{BackendMarker, EventBusConfig, Redis};
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
    pub consumer_name: Option<String>,
    pub manual_ack: bool,
    pub start_id: String,
}

impl RedisConsumerGroupSpec {
    pub fn new<I, S>(streams: I, consumer_group: impl Into<String>) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        Self {
            streams: streams.into_iter().map(Into::into).collect(),
            consumer_group: consumer_group.into(),
            consumer_name: None,
            manual_ack: false,
            start_id: "0-0".to_string(),
        }
    }

    pub fn consumer_name(mut self, name: impl Into<String>) -> Self {
        self.consumer_name = Some(name.into());
        self
    }

    pub fn manual_ack(mut self, manual: bool) -> Self {
        self.manual_ack = manual;
        self
    }

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

    pub fn add_consumer_group(
        &mut self,
        id: impl Into<String>,
        spec: RedisConsumerGroupSpec,
    ) -> &mut Self {
        self.consumer_groups.insert(id.into(), spec);
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
    pub read_batch_size: usize,
    pub empty_read_backoff: Duration,
}

impl RedisRuntimeTuning {
    pub fn new(
        message_channel_capacity: usize,
        ack_channel_capacity: usize,
        read_batch_size: usize,
        empty_read_backoff: Duration,
    ) -> Self {
        Self {
            message_channel_capacity,
            ack_channel_capacity,
            read_batch_size,
            empty_read_backoff,
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

    pub fn read_batch_size(mut self, batch: usize) -> Self {
        self.read_batch_size = batch;
        self
    }

    pub fn empty_read_backoff(mut self, backoff: Duration) -> Self {
        self.empty_read_backoff = backoff;
        self
    }
}

impl Default for RedisRuntimeTuning {
    fn default() -> Self {
        Self {
            message_channel_capacity: 10_000,
            ack_channel_capacity: 2_048,
            read_batch_size: 128,
            empty_read_backoff: Duration::from_millis(5),
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
    stream: String,
    consumer_group: Option<String>,
    consumer_name: Option<String>,
    read_block_timeout: Duration,
}

impl RedisConsumerConfig {
    pub fn new(stream: impl Into<String>) -> Self {
        Self {
            stream: stream.into(),
            consumer_group: None,
            consumer_name: None,
            read_block_timeout: Duration::from_millis(100),
        }
    }

    pub fn consumer_group(mut self, group: impl Into<String>) -> Self {
        self.consumer_group = Some(group.into());
        self
    }

    pub fn consumer_name(mut self, name: impl Into<String>) -> Self {
        self.consumer_name = Some(name.into());
        self
    }

    pub fn read_block_timeout(mut self, timeout: Duration) -> Self {
        self.read_block_timeout = timeout;
        self
    }

    pub fn stream(&self) -> &str {
        &self.stream
    }

    pub fn consumer_group(&self) -> Option<&str> {
        self.consumer_group.as_deref()
    }

    pub fn consumer_name(&self) -> Option<&str> {
        self.consumer_name.as_deref()
    }

    pub fn block_timeout(&self) -> Duration {
        self.read_block_timeout
    }
}

impl EventBusConfig for RedisConsumerConfig {
    type Backend = Redis;

    fn topics(&self) -> &[String] {
        std::slice::from_ref(&self.stream)
    }

    fn config_id(&self) -> String {
        format!("redis:{}", self.stream)
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

    pub fn trim_strategy(mut self, strategy: TrimStrategy) -> Self {
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
}

/// Trimming strategies supported when writing to Redis Streams.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TrimStrategy {
    Exact,
    Approximate,
}
