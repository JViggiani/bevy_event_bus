use bevy::prelude::*;
use bevy_event_bus::EventBusPlugins;
use integration_tests::common::backend_factory::{
    configured_backends, setup_backend, BackendHandle, BackendVariant, TestBackend,
};
use integration_tests::common::helpers::update_until;
use integration_tests::common::TestEvent;
use tracing::{info, info_span};
use tracing_subscriber::EnvFilter;

#[cfg(feature = "kafka")]
use bevy_event_bus::{
    backends::KafkaEventBusBackend,
    config::kafka::{KafkaConsumerConfig, KafkaProducerConfig},
    KafkaEventReader, KafkaEventWriter,
};

#[cfg(feature = "redis")]
use bevy_event_bus::{
    backends::RedisEventBusBackend,
    config::redis::{RedisConsumerConfig, RedisProducerConfig},
    RedisEventReader, RedisEventWriter,
};

#[derive(Resource, Clone)]
struct Topic(String);

#[derive(Resource, Clone)]
struct ConsumerGroup(Option<String>);

#[derive(Resource, Clone)]
struct ToSend(TestEvent);

#[derive(Resource, Default)]
struct Collected(Vec<TestEvent>);

#[test]
fn test_basic_event_bus_across_backends() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    for backend in configured_backends() {
        let span = info_span!("basic_test.backend", ?backend);
        let _guard = span.enter();
        info!("initialising backend under test");

        let (handle, context) = setup_backend(backend).expect("backend setup successful");
        run_basic_case(handle);
        drop(context);
    }
}

fn run_basic_case(handle: BackendHandle) {
    let BackendHandle {
        kind,
        topic,
        consumer_group,
        writer_backend,
        reader_backend,
        ..
    } = handle;

    match kind {
        #[cfg(feature = "kafka")]
        TestBackend::Kafka => match (writer_backend, reader_backend) {
            (BackendVariant::Kafka(writer_backend), BackendVariant::Kafka(reader_backend)) => {
                run_basic_kafka(
                    topic,
                    consumer_group.expect("Kafka requires consumer group"),
                    writer_backend,
                    reader_backend,
                );
            }
            #[cfg(all(feature = "kafka", feature = "redis"))]
            _ => unreachable!("Kafka test expected Kafka backends"),
        },
        #[cfg(feature = "redis")]
        TestBackend::Redis => match (writer_backend, reader_backend) {
            (BackendVariant::Redis(writer_backend), BackendVariant::Redis(reader_backend)) => {
                run_basic_redis(topic, consumer_group, writer_backend, reader_backend);
            }
            #[cfg(all(feature = "kafka", feature = "redis"))]
            _ => unreachable!("Redis test expected Redis backends"),
        },
        #[allow(unreachable_patterns)]
        _ => panic!("Unsupported backend for basic test"),
    }
}

#[cfg(feature = "kafka")]
fn run_basic_kafka(
    topic: String,
    consumer_group: String,
    writer_backend: KafkaEventBusBackend,
    reader_backend: KafkaEventBusBackend,
) {
    let event_to_send = TestEvent {
        message: "From Bevy!".into(),
        value: 100,
    };

    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(writer_backend));
    writer_app.insert_resource(ToSend(event_to_send.clone()));
    writer_app.insert_resource(Topic(topic.clone()));
    writer_app.add_systems(Update, kafka_writer_system);
    writer_app.update();

    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(reader_backend));
    reader_app.insert_resource(Topic(topic.clone()));
    reader_app.insert_resource(ConsumerGroup(Some(consumer_group)));
    reader_app.insert_resource(Collected::default());
    reader_app.add_systems(Update, kafka_reader_system);

    let (received, _frames) = update_until(&mut reader_app, 7000, |app| {
        app.world().resource::<Collected>().0.len() >= 1
    });

    assert!(received, "Expected to receive at least one event for Kafka backend");
    let collected = reader_app.world().resource::<Collected>();
    assert!(
        collected
            .0
            .iter()
            .any(|event| event == &event_to_send),
        "Kafka backend did not deliver expected event: {:?}",
        collected.0
    );
}

#[cfg(feature = "redis")]
fn run_basic_redis(
    topic: String,
    consumer_group: Option<String>,
    writer_backend: RedisEventBusBackend,
    reader_backend: RedisEventBusBackend,
) {
    let event_to_send = TestEvent {
        message: "From Bevy!".into(),
        value: 100,
    };

    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(reader_backend));
    reader_app.insert_resource(Topic(topic.clone()));
    reader_app.insert_resource(ConsumerGroup(consumer_group.clone()));
    reader_app.insert_resource(Collected::default());
    reader_app.add_systems(Update, redis_reader_system);

    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(writer_backend));
    writer_app.insert_resource(ToSend(event_to_send.clone()));
    writer_app.insert_resource(Topic(topic.clone()));
    writer_app.add_systems(Update, redis_writer_system);
    writer_app.update();

    let (received, _frames) = update_until(&mut reader_app, 7000, |app| {
        app.world().resource::<Collected>().0.len() >= 1
    });

    assert!(received, "Expected to receive at least one event for Redis backend");
    let collected = reader_app.world().resource::<Collected>();
    assert!(
        collected
            .0
            .iter()
            .any(|event| event == &event_to_send),
        "Redis backend did not deliver expected event: {:?}",
        collected.0
    );
}

#[cfg(feature = "kafka")]
fn kafka_writer_system(mut writer: KafkaEventWriter, to_send: Res<ToSend>, topic: Res<Topic>) {
    let config = KafkaProducerConfig::new([topic.0.clone()]);
    writer.write(&config, to_send.0.clone());
}

#[cfg(feature = "kafka")]
fn kafka_reader_system(
    mut reader: KafkaEventReader<TestEvent>,
    topic: Res<Topic>,
    group: Res<ConsumerGroup>,
    mut collected: ResMut<Collected>,
) {
    let group_id = group
        .0
        .clone()
        .expect("Kafka backend requires a consumer group");
    let config = KafkaConsumerConfig::new(group_id, [topic.0.clone()]);
    for wrapper in reader.read(&config) {
        collected.0.push(wrapper.event().clone());
    }
}

#[cfg(feature = "redis")]
fn redis_writer_system(mut writer: RedisEventWriter, to_send: Res<ToSend>, topic: Res<Topic>) {
    let config = RedisProducerConfig::new(topic.0.clone());
    writer.write(&config, to_send.0.clone());
}

#[cfg(feature = "redis")]
fn redis_reader_system(
    mut reader: RedisEventReader<TestEvent>,
    topic: Res<Topic>,
    group: Res<ConsumerGroup>,
    mut collected: ResMut<Collected>,
) {
    let mut config = RedisConsumerConfig::new(topic.0.clone());
    if let Some(group_id) = group.0.as_ref() {
        config = config.set_consumer_group(group_id.clone());
    }

    for wrapper in reader.read(&config) {
        collected.0.push(wrapper.event().clone());
    }
}
