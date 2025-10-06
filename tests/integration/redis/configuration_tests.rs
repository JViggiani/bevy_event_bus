#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec, TrimStrategy,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_topic, update_until,
};
use integration_tests::utils::redis_setup;
use std::time::Duration;

#[test]
fn configuration_with_readers_writers_works() {
    let stream = unique_topic("config_test");
    let consumer_group = unique_consumer_group("config_reader_group");

    let shared_db = redis_setup::ensure_shared_redis().expect("Redis backend setup successful");

    let stream_clone = stream.clone();
    let consumer_group_clone = consumer_group.clone();
    let (backend, _context) = redis_setup::setup(&shared_db, move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()))
            .add_consumer_group(
                consumer_group_clone.clone(),
                RedisConsumerGroupSpec::new([stream_clone.clone()], consumer_group_clone.clone()),
            )
            .add_event_single::<TestEvent>(stream_clone.clone());
    })
    .expect("Redis backend setup successful");

    let backend_reader = backend.clone();
    let backend_writer = backend;

    // Reader app (start first to ensure it's ready)
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(backend_reader));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader_app.insert_resource(Collected::default());
    #[derive(Resource, Clone)]
    struct Stream(String);
    #[derive(Resource, Clone)]
    struct ConsumerGroup(String);
    reader_app.insert_resource(Stream(stream.clone()));
    reader_app.insert_resource(ConsumerGroup(consumer_group.clone()));

    fn reader_system(
        mut reader: RedisEventReader<TestEvent>,
        stream: Res<Stream>,
        group: Res<ConsumerGroup>,
        mut collected: ResMut<Collected>,
    ) {
        let config = RedisConsumerConfig::new(stream.0.clone()).set_consumer_group(group.0.clone());
        let events = reader.read(&config);
        for wrapper in events {
            collected.0.push(wrapper.event().clone());
        }
    }
    reader_app.add_systems(Update, reader_system);

    // Writer app
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(backend_writer));

    #[derive(Resource, Clone)]
    struct ToSend(TestEvent, String);

    let event_to_send = TestEvent {
        message: "config_test".to_string(),
        value: 42,
    };
    writer_app.insert_resource(ToSend(event_to_send.clone(), stream.clone()));

    fn writer_system(mut writer: RedisEventWriter, data: Res<ToSend>, mut sent: Local<bool>) {
        if !*sent {
            *sent = true;
            let config = RedisProducerConfig::new(data.1.clone());
            writer.write(&config, data.0.clone());
        }
    }
    writer_app.add_systems(Update, writer_system);
    writer_app.update();

    // Poll until message received or timeout
    let expected_event = event_to_send.clone();
    let (received, _) = update_until(&mut reader_app, 10_000, |app| {
        let collected = app.world().resource::<Collected>();
        collected.0.iter().any(|item| item == &expected_event)
    });

    assert!(received, "Expected to receive event within timeout");

    let collected = reader_app.world().resource::<Collected>();
    let stored = collected
        .0
        .iter()
        .find(|event| event.message == "config_test")
        .expect("Expected to find sent event in collected list");
    assert_eq!(stored.value, 42, "Expected event payload to match");
}

/// Test that Redis-specific methods work with configurations
#[test]
fn redis_specific_methods_work() {
    let stream = unique_topic("redis_methods");
    let consumer_group = unique_consumer_group("redis_methods_group");

    let shared_db = redis_setup::ensure_shared_redis().expect("Redis backend setup successful");

    let stream_clone = stream.clone();
    let consumer_group_clone = consumer_group.clone();
    let (backend, _context) = redis_setup::setup(&shared_db, move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()).maxlen(1000))
            .add_consumer_group(
                consumer_group_clone.clone(),
                RedisConsumerGroupSpec::new([stream_clone.clone()], consumer_group_clone.clone())
                    .consumer_name("redis_consumer".to_string()),
            )
            .add_event_single::<TestEvent>(stream_clone.clone());
    })
    .expect("Redis backend setup successful");

    let mut app = App::new();
    app.add_plugins(EventBusPlugins(backend));

    let redis_producer_config = RedisProducerConfig::new(stream.clone()).maxlen(500);

    let redis_consumer_config = RedisConsumerConfig::new(stream.clone())
        .set_consumer_group(consumer_group.clone())
        .set_consumer_name("redis_consumer".to_string())
        .read_block_timeout(Duration::from_millis(100));

    #[derive(Resource)]
    struct TestConfigs {
        producer: RedisProducerConfig,
        consumer: RedisConsumerConfig,
    }

    app.insert_resource(TestConfigs {
        producer: redis_producer_config,
        consumer: redis_consumer_config,
    });

    // Writer system that uses Redis-specific write methods
    fn test_redis_write_methods(mut writer: RedisEventWriter, configs: Res<TestConfigs>) {
        writer
            .trim_stream(configs.producer.stream(), 250, TrimStrategy::Exact)
            .expect("Expected trim scheduling to succeed");
        writer.write(
            &configs.producer,
            TestEvent {
                message: "redis_test".to_string(),
                value: 999,
            },
        );
        writer.flush().expect("Expected Redis flush to succeed");
    }

    // Reader system that uses Redis-specific read methods
    fn test_redis_read_methods(mut reader: RedisEventReader<TestEvent>, configs: Res<TestConfigs>) {
        // Test Redis-specific read methods
        let _events = reader.read(&configs.consumer);
    }

    app.add_systems(Update, (test_redis_write_methods, test_redis_read_methods));

    run_app_updates(&mut app, 5);

    // If we get here without panicking, the Redis-specific methods work
}

/// Test that the builder pattern works correctly for configurations
#[test]
fn builder_pattern_works() {
    // Test that we can build consumer config
    let stream = "test_stream".to_string();
    let consumer_group = "test_group".to_string();

    let consumer_config = RedisConsumerConfig::new(stream.clone())
        .set_consumer_group(consumer_group.clone())
        .set_consumer_name("test_consumer".to_string())
        .read_block_timeout(std::time::Duration::from_millis(1000));

    // Test that trait methods work using explicit syntax
    use bevy_event_bus::EventBusConfig;
    assert!(!EventBusConfig::topics(&consumer_config).is_empty());

    // Test that specific config getters work
    assert_eq!(consumer_config.consumer_group(), Some(&*consumer_group));
    assert_eq!(consumer_config.consumer_name(), Some("test_consumer"));

    // Test that we can build producer config
    let producer_config = RedisProducerConfig::new(stream.clone()).maxlen(10000);

    // Test that producer getters work
    assert_eq!(producer_config.maxlen_value(), Some(10000));
}

/// Test that clean system signatures work without explicit backend types
#[test]
fn clean_system_signatures() {
    // This test demonstrates that systems can have clean signatures
    // without explicitly mentioning backend types

    fn clean_producer_system(mut writer: RedisEventWriter) {
        // Configuration can be injected from resource or built inline
        let config = RedisProducerConfig::new("test_stream".to_string());
        writer.write(
            &config,
            TestEvent {
                message: "clean".to_string(),
                value: 123,
            },
        );
    }

    fn clean_consumer_system(mut reader: RedisEventReader<TestEvent>) {
        // Using inline configuration
        let config = RedisConsumerConfig::new("test_stream".to_string())
            .set_consumer_group("clean_group".to_string());
        let _events = reader.read(&config);
    }

    // If this compiles, then clean signatures work
    let _ = clean_producer_system;
    let _ = clean_consumer_system;
}
