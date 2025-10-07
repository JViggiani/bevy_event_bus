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

    let stream_for_writer = stream.clone();
    let (backend_writer, _writer_context) = shared_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(stream_for_writer.clone()))
                .add_event_single::<TestEvent>(stream_for_writer.clone());
        })
        .expect("Writer Redis backend setup successful");

    let stream_for_reader = stream.clone();
    let consumer_group_for_reader = consumer_group.clone();
    let (backend_reader, _reader_context) = shared_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(stream_for_reader.clone()))
                .add_consumer_group(
                    consumer_group_for_reader.clone(),
                    RedisConsumerGroupSpec::new(
                        [stream_for_reader.clone()],
                        consumer_group_for_reader.clone(),
                    ),
                )
                .add_event_single::<TestEvent>(stream_for_reader.clone());
        })
        .expect("Reader Redis backend setup successful");

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);

    // Consumer app styled after Kafka configuration test
    let mut reader_app = {
        let mut app = App::new();
        app.add_plugins(EventBusPlugins(backend_reader));

        let stream_clone = stream.clone();
        let consumer_group_clone = consumer_group.clone();
        let consumer_system = move |
            mut reader: RedisEventReader<TestEvent>,
            mut collected: ResMut<Collected>,
        | {
            let config = RedisConsumerConfig::new(
                consumer_group_clone.clone(),
                [stream_clone.clone()],
            );
            let events = reader.read(&config);
            for wrapper in events {
                collected.0.push(wrapper.event().clone());
            }
        };

        app.insert_resource(Collected::default());
        app.add_systems(Update, consumer_system);
        app
    };

    // Producer app styled after Kafka configuration test
    let mut writer_app = {
        let mut app = App::new();
        app.add_plugins(EventBusPlugins(backend_writer));

        let stream_clone = stream.clone();
        let event_payload = TestEvent {
            message: "config_test".to_string(),
            value: 42,
        };

        let producer_system = move |mut writer: RedisEventWriter| {
            let config = RedisProducerConfig::new(stream_clone.clone());
            writer.write(&config, event_payload.clone());
        };

        app.add_systems(Update, producer_system);
        app
    };

    // Run producer first
    writer_app.update();

    // Then run consumer and verify
    let (success, _frames) = update_until(&mut reader_app, 10_000, |app| {
        let collected = app.world().get_resource::<Collected>().unwrap();
        !collected.0.is_empty()
    });

    assert!(
        success,
        "Should have received at least one event within timeout"
    );

    let collected = reader_app
        .world()
        .get_resource::<Collected>()
        .expect("Collected resource should exist");
    assert!(
        !collected.0.is_empty(),
        "Should have received at least one event"
    );

    let event = &collected.0[0];
    assert_eq!(event.message, "config_test");
    assert_eq!(event.value, 42);
}

/// Test that Redis-specific methods work with configurations
#[test]
fn redis_specific_methods_work() {
    let stream = unique_topic("redis_methods");
    let consumer_group = unique_consumer_group("redis_methods_group");

    let stream_clone = stream.clone();
    let consumer_group_clone = consumer_group.clone();
    let (backend, _context) = redis_setup::prepare_backend(move |builder| {
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

    let redis_consumer_config = RedisConsumerConfig::new(consumer_group.clone(), [stream.clone()])
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

    let consumer_config = RedisConsumerConfig::new(consumer_group.clone(), [stream.clone()])
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
        let config = RedisConsumerConfig::new("clean_group".to_string(), ["test_stream".to_string()]);
        let _events = reader.read(&config);
    }

    // If this compiles, then clean signatures work
    let _ = clean_producer_system;
    let _ = clean_consumer_system;
}
