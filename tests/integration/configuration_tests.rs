use std::collections::HashMap;

use bevy::prelude::*;
use bevy_event_bus::config::kafka::{KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaTopicSpec};
use bevy_event_bus::{
    EventBusPlugins, KafkaConsumerConfig, KafkaEventBusBackend, KafkaEventReader,
    KafkaEventWriter, KafkaProducerConfig,
};
use integration_tests::common::events::TestEvent;
use integration_tests::common::helpers::{
    DEFAULT_KAFKA_BOOTSTRAP, kafka_backend_config_for_tests, kafka_consumer_config,
    kafka_producer_config, run_app_updates, unique_consumer_group, unique_topic, update_until,
};
use integration_tests::common::setup::setup;

#[derive(Resource, Default)]
struct Collected(Vec<TestEvent>);

/// Test that configurations work with readers and writers
#[test]
fn configuration_with_readers_writers_works() {
    let topic = unique_topic("config_test");
    let consumer_group = unique_consumer_group("config_reader_group");

    // Create separate backends for writer and reader to simulate separate machines
    let topic_for_writer = topic.clone();
    let (backend_writer, _bootstrap_writer) = setup("earliest", move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(topic_for_writer.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_event_single::<TestEvent>(topic_for_writer.clone());
    });

    let topic_for_reader = topic.clone();
    let group_for_reader = consumer_group.clone();
    let (backend_reader, _bootstrap_reader) = setup("earliest", move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(topic_for_reader.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_consumer_group(
            group_for_reader.clone(),
            KafkaConsumerGroupSpec::new([topic_for_reader.clone()])
                .initial_offset(KafkaInitialOffset::Earliest),
        );
        builder.add_event_single::<TestEvent>(topic_for_reader.clone());
    });

    // Producer app
    let mut producer_app = {
        let mut app = App::new();
        app.add_plugins(EventBusPlugins(backend_writer));

        // Producer system using configuration
        let topic_clone = topic.clone();
        let producer_system = move |mut writer: KafkaEventWriter| {
            // Write using configuration - producers specify topics
            let config = kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&topic_clone])
                .compression_type("none");
            writer.write(
                &config,
                TestEvent {
                    message: "config_test".to_string(),
                    value: 42,
                },
            );
        };

        app.add_systems(Update, producer_system);
        app
    };

    // Consumer app
    let mut consumer_app = {
        let mut app = App::new();
        app.add_plugins(EventBusPlugins(backend_reader));

        // Consumer system using configuration
        let topic_clone = topic.clone();
        let consumer_group_clone = consumer_group.clone();
        let consumer_system =
            move |mut reader: KafkaEventReader<TestEvent>, mut collected: ResMut<Collected>| {
                // Read using configuration
                let config = kafka_consumer_config(
                    DEFAULT_KAFKA_BOOTSTRAP,
                    &consumer_group_clone,
                    [&topic_clone],
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

    // Run producer first
    producer_app.update();

    // Then run consumer and verify
    let (success, _frames) = update_until(
        &mut consumer_app,
        10000, // 10 second timeout
        |app| {
            let collected = app.world().get_resource::<Collected>().unwrap();
            collected.0.len() >= 1
        },
    );

    assert!(
        success,
        "Should have received at least one event within timeout"
    );

    let collected = consumer_app.world().get_resource::<Collected>().unwrap();
    assert!(
        !collected.0.is_empty(),
        "Should have received at least one event"
    );

    let event = &collected.0[0];
    assert_eq!(event.message, "config_test");
    assert_eq!(event.value, 42);
}

/// Test that Kafka-specific methods work with configurations
#[test]
fn kafka_specific_methods_work() {
    let topic = unique_topic("kafka_methods");
    let (_backend, bootstrap) = setup("latest", |_| {});
    let consumer_group = unique_consumer_group("kafka_methods");

    let topic_for_config = topic.clone();
    let consumer_for_config = consumer_group.clone();

    let backend = KafkaEventBusBackend::new(kafka_backend_config_for_tests(
        &bootstrap,
        None,
        move |builder| {
            builder
                .add_topic(
                    KafkaTopicSpec::new(topic_for_config.clone())
                        .partitions(1)
                        .replication(1),
                )
                .add_consumer_group(
                    consumer_for_config.clone(),
                    KafkaConsumerGroupSpec::new([topic_for_config.clone()])
                        .initial_offset(KafkaInitialOffset::Earliest),
                )
                .add_event_single::<TestEvent>(topic_for_config.clone());
        },
    ));
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(backend));

    let kafka_producer_config =
        kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, Vec::<String>::new())
            .compression_type("none")
            .acks("all");

    let kafka_consumer_config =
        kafka_consumer_config(DEFAULT_KAFKA_BOOTSTRAP, consumer_group.clone(), [&topic]);

    #[derive(Resource)]
    struct TestConfigs {
        producer: KafkaProducerConfig,
        consumer: KafkaConsumerConfig,
    }

    app.insert_resource(TestConfigs {
        producer: kafka_producer_config,
        consumer: kafka_consumer_config,
    });

    // Writer system that uses Kafka-specific write methods
    fn test_kafka_write_methods(mut writer: KafkaEventWriter, configs: Res<TestConfigs>) {
        // Test Kafka-specific write methods
        let _metadata = writer.write_with_key(
            &configs.producer,
            TestEvent {
                message: "key_test".to_string(),
                value: 1,
            },
            "partition_key",
        );

        let _metadata = writer.write_with_headers(
            &configs.producer,
            TestEvent {
                message: "headers_test".to_string(),
                value: 2,
            },
            HashMap::from([
                ("header1".to_string(), "value1".to_string()),
                ("header2".to_string(), "value2".to_string()),
            ]),
        );

        let _flush_result = writer.flush(std::time::Duration::from_secs(1));
    }

    // Reader system that uses Kafka-specific read methods
    fn test_kafka_read_methods(mut reader: KafkaEventReader<TestEvent>, configs: Res<TestConfigs>) {
        // Test Kafka-specific read methods
        let _events = reader.read(&configs.consumer);
        let _lag = reader.consumer_lag(&configs.consumer);
    }

    app.add_systems(Update, (test_kafka_write_methods, test_kafka_read_methods));

    // Run a few updates to execute the test system
    run_app_updates(&mut app, 5);

    // If we get here without panicking, the Kafka-specific methods work
}

/// Test that the builder pattern works correctly for configurations
#[test]
fn builder_pattern_works() {
    // Test that we can build consumer config
    let consumer_config =
        kafka_consumer_config(DEFAULT_KAFKA_BOOTSTRAP, "test_group", ["topic1", "topic2"])
            .auto_offset_reset("earliest")
            .enable_auto_commit(false)
            .session_timeout_ms(6000);

    // Test that trait methods work using explicit syntax
    use bevy_event_bus::EventBusConfig;
    assert!(!EventBusConfig::topics(&consumer_config).is_empty());
    assert!(consumer_config.get_consumer_group() == "test_group");

    // Test that specific config getters work
    assert!(!consumer_config.is_auto_commit_enabled());
    assert_eq!(consumer_config.get_session_timeout_ms(), 6000);

    // Test that we can build producer config
    let producer_config = kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, Vec::<String>::new())
        .compression_type("gzip")
        .acks("all")
        .retries(3)
        .batch_size(16384);

    // Test that producer getters work
    assert_eq!(producer_config.get_compression_type(), "gzip");
    assert_eq!(producer_config.get_acks(), "all");
    assert_eq!(producer_config.get_retries(), 3);
    assert_eq!(producer_config.get_batch_size(), 16384);
}

/// Test that clean system signatures work without explicit backend types
#[test]
fn clean_system_signatures() {
    // This test demonstrates that systems can have clean signatures
    // without explicitly mentioning backend types

    fn clean_producer_system(mut writer: KafkaEventWriter) {
        // Configuration can be injected from resource or built inline
        let config = kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, Vec::<String>::new())
            .compression_type("none");
        writer.write(
            &config,
            TestEvent {
                message: "clean".to_string(),
                value: 123,
            },
        );
    }

    fn clean_consumer_system(mut reader: KafkaEventReader<TestEvent>) {
        // Using inline configuration
        let config = kafka_consumer_config(DEFAULT_KAFKA_BOOTSTRAP, "clean_group", ["clean_test"]);
        let _events = reader.read(&config);
    }

    // If this compiles, then clean signatures work
    let _ = clean_producer_system;
    let _ = clean_consumer_system;
}
