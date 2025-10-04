use anyhow::Result;

#[cfg(feature = "kafka")]
use bevy_event_bus::backends::KafkaEventBusBackend;
#[cfg(feature = "redis")]
use bevy_event_bus::backends::RedisEventBusBackend;

#[cfg(feature = "kafka")]
use bevy_event_bus::config::kafka::{
    KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaTopicSpec,
};
#[cfg(feature = "redis")]
use bevy_event_bus::config::redis::{
    RedisConsumerGroupSpec, RedisStreamSpec, RedisTopologyBuilder,
};

use crate::common::helpers::{unique_consumer_group, unique_topic};
#[cfg(feature = "redis")]
use crate::common::redis_setup::{self, RedisTestContext};
#[cfg(feature = "kafka")]
use crate::common::setup;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum TestBackend {
    #[cfg(feature = "kafka")]
    Kafka,
    #[cfg(feature = "redis")]
    Redis,
}

/// Enumerate which backends should run for the current test invocation.
///
/// Environment toggles:
/// * `BEB_ONLY_REDIS=1` – run Redis-only tests (requires `redis` feature).
/// * `BEB_ONLY_KAFKA=1` – run Kafka-only tests.
/// * `BEB_SKIP_REDIS=1` – skip Redis even if enabled (useful locally).
pub fn configured_backends() -> Vec<TestBackend> {
    let mut backends = Vec::new();

    let only_redis = std::env::var_os("BEB_ONLY_REDIS").is_some();
    let only_kafka = std::env::var_os("BEB_ONLY_KAFKA").is_some();
    if only_redis {
        #[cfg(feature = "redis")]
        {
            backends.push(TestBackend::Redis);
        }
        return backends;
    }

    if only_kafka {
        #[cfg(feature = "kafka")]
        {
            backends.push(TestBackend::Kafka);
        }
        return backends;
    }

    #[cfg(feature = "kafka")]
    {
        backends.push(TestBackend::Kafka);
    }

    #[cfg(feature = "redis")]
    {
        let skip_redis = std::env::var_os("BEB_SKIP_REDIS").is_some();
        if !skip_redis {
            backends.push(TestBackend::Redis);
        }
    }

    backends
}

#[derive(Clone)]
pub enum BackendVariant {
    #[cfg(feature = "kafka")]
    Kafka(KafkaEventBusBackend),
    #[cfg(feature = "redis")]
    Redis(RedisEventBusBackend),
}

pub struct BackendHandle {
    pub kind: TestBackend,
    pub topic: String,
    pub consumer_group: Option<String>,
    pub writer_backend: BackendVariant,
    pub reader_backend: BackendVariant,
}

pub enum TestContext {
    #[cfg(feature = "kafka")]
    Kafka,
    #[cfg(feature = "redis")]
    Redis(RedisTestContext),
}

impl TestContext {
    #[cfg(feature = "redis")]
    pub fn as_redis(&self) -> Option<&RedisTestContext> {
        match self {
            TestContext::Redis(ctx) => Some(ctx),
            _ => None,
        }
    }
}

pub fn setup_backend(kind: TestBackend) -> Result<(BackendHandle, TestContext)> {
    match kind {
        #[cfg(feature = "kafka")]
        TestBackend::Kafka => setup_kafka_backend(),
        #[cfg(feature = "redis")]
        TestBackend::Redis => setup_redis_backend(),
    }
}

#[cfg(feature = "kafka")]
fn setup_kafka_backend() -> Result<(BackendHandle, TestContext)> {
    let topic = unique_topic("matrix");
    let consumer_group = unique_consumer_group("matrix_reader");

    let topic_writer = topic.clone();
    let (writer_backend, _bootstrap_writer) = setup::setup("earliest", move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(topic_writer.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_event_single::<crate::common::events::TestEvent>(topic_writer.clone());
    });
    let topic_reader = topic.clone();
    let group_reader = consumer_group.clone();
    let (reader_backend, _bootstrap_reader) = setup::setup("earliest", move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(topic_reader.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_consumer_group(
            group_reader.clone(),
            KafkaConsumerGroupSpec::new([topic_reader.clone()])
                .initial_offset(KafkaInitialOffset::Earliest),
        );
        builder.add_event_single::<crate::common::events::TestEvent>(topic_reader.clone());
    });

    Ok((
        BackendHandle {
            kind: TestBackend::Kafka,
            topic,
            consumer_group: Some(consumer_group),
            writer_backend: BackendVariant::Kafka(writer_backend),
            reader_backend: BackendVariant::Kafka(reader_backend),
        },
        TestContext::Kafka,
    ))
}

#[cfg(feature = "redis")]
fn setup_redis_backend() -> Result<(BackendHandle, TestContext)> {
    let stream = unique_topic("redis-matrix");
    let group = unique_consumer_group("redis-matrix");

    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], group.clone()).manual_ack(true),
        )
        .add_event_single::<crate::common::events::TestEvent>(stream.clone());

    let (backend, ctx) = redis_setup::setup_with_builder(builder)?;
    let reader_backend = backend.clone();

    Ok((
        BackendHandle {
            kind: TestBackend::Redis,
            topic: stream,
            consumer_group: Some(group),
            writer_backend: BackendVariant::Redis(backend.clone()),
            reader_backend: BackendVariant::Redis(reader_backend),
        },
        TestContext::Redis(ctx),
    ))
}

