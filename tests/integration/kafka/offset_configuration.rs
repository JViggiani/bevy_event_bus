use std::time::Duration;

use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{
    EventBusBackend, EventBusPlugins, KafkaEventBusBackend, KafkaEventReader, KafkaEventWriter,
    backends::event_bus_backend::SendOptions,
};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{
    kafka_backend_config_for_tests, unique_consumer_group, unique_topic, update_until,
    wait_for_consumer_group_ready, wait_for_messages_in_group,
};
use integration_tests::utils::kafka_setup::{self, SetupOptions};

/// Test that consumers with "earliest" offset receive historical events
#[test]
fn offset_configuration_earliest_receives_historical_events() {
    let topic = unique_topic("offset_test_earliest");

    // Create topic and ensure it's ready before proceeding
    let (_backend_setup, bootstrap) =
        kafka_setup::prepare_backend(kafka_setup::build_topology(|_| {}));
    let topic_ready = kafka_setup::ensure_topic_ready(
        &bootstrap,
        &topic,
        1, // partitions
        Duration::from_secs(5),
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);

    // Send historical events using a temporary backend producer
    {
        let (mut backend_producer, _bootstrap) =
            kafka_setup::prepare_backend(kafka_setup::build_topology(|_| {}));
        assert!(
            bevy_event_bus::block_on(backend_producer.connect()),
            "Failed to connect producer backend"
        );

        for i in 0..3 {
            let event = TestEvent {
                message: format!("historical_{}", i),
                value: i,
            };
            let payload = serde_json::to_vec(&event).expect("serialize historical event");
            assert!(
                backend_producer.try_send_serialized(&payload, &topic, SendOptions::default()),
                "Failed to enqueue historical event"
            );
        }

        backend_producer
            .flush(Duration::from_secs(2))
            .expect("flush historical events");
        let _ = bevy_event_bus::block_on(backend_producer.disconnect());
    }

    // Configure consumer group to start at the earliest offset.
    let consumer_group = unique_consumer_group("earliest_test");
    let topic_for_config = topic.clone();
    let consumer_for_config = consumer_group.clone();

    let earliest_builder = kafka_setup::build_topology(move |builder| {
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
    });
    let (backend_earliest, _bootstrap_override) =
        kafka_setup::prepare_backend((earliest_builder, SetupOptions::earliest()));

    let mut backend = backend_earliest.clone();
    assert!(
        bevy_event_bus::block_on(backend.connect()),
        "Failed to connect earliest backend"
    );

    let ready = bevy_event_bus::block_on(wait_for_consumer_group_ready(
        &backend,
        &topic,
        &consumer_group,
        10_000,
    ));
    assert!(ready, "Earliest consumer group was not ready in time");

    let messages = bevy_event_bus::block_on(wait_for_messages_in_group(
        &backend,
        &topic,
        &consumer_group,
        3,
        10_000,
    ));

    assert!(
        messages.len() >= 3,
        "Expected at least 3 historical events, got {}",
        messages.len()
    );

    let historical_messages: Vec<TestEvent> = messages
        .iter()
        .filter_map(|payload| serde_json::from_slice::<TestEvent>(payload).ok())
        .collect();
    let historical_count = historical_messages
        .iter()
        .filter(|event| event.message.starts_with("historical_"))
        .count();

    assert!(
        historical_count >= 3,
        "Consumer with 'earliest' should receive historical events. Got {:?}",
        historical_messages
    );

    let _ = bevy_event_bus::block_on(backend.disconnect());
}

/// Test that consumers with "latest" offset ignore historical events
#[test]
fn offset_configuration_latest_ignores_historical_events() {
    let topic = unique_topic("offset_test_latest");

    // Create topic and ensure it's ready before proceeding
    let (_backend_setup, bootstrap) =
        kafka_setup::prepare_backend(kafka_setup::build_topology(|_| {}));
    let topic_ready = kafka_setup::ensure_topic_ready(
        &bootstrap,
        &topic,
        1, // partitions
        std::time::Duration::from_secs(5),
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);

    // Send historical events first
    {
        let topic_for_binding = topic.clone();
        let producer_builder = kafka_setup::build_topology(move |builder| {
            builder
                .add_topic(
                    KafkaTopicSpec::new(topic_for_binding.clone())
                        .partitions(1)
                        .replication(1),
                )
                .add_event_single::<TestEvent>(topic_for_binding.clone());
        });
        let (backend_producer, _bootstrap) =
            kafka_setup::prepare_backend((producer_builder, SetupOptions::latest()));
        let mut producer_app = App::new();
        producer_app.add_plugins(EventBusPlugins(backend_producer));

        let topic_clone = topic.clone();
        producer_app.add_systems(Update, move |mut w: KafkaEventWriter| {
            // Send 3 historical events that the latest consumer should NOT see
            let config = KafkaProducerConfig::new([topic_clone.clone()]);
            for i in 0..3 {
                let _ = w.write(
                    &config,
                    TestEvent {
                        message: format!("historical_{}", i),
                        value: i,
                    },
                );
            }
        });
        producer_app.update(); // Send the historical events
    }

    // Create consumer with "latest" offset - should NOT see historical events
    let consumer_group = unique_consumer_group("latest_test");
    let topic_for_config = topic.clone();
    let consumer_for_config = consumer_group.clone();

    let mut latest_config = kafka_backend_config_for_tests(&bootstrap, None, move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_for_config.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_consumer_group(
                consumer_for_config.clone(),
                KafkaConsumerGroupSpec::new([topic_for_config.clone()])
                    .initial_offset(KafkaInitialOffset::Latest),
            )
            .add_event_single::<TestEvent>(topic_for_config.clone());
    });
    latest_config.connection = latest_config
        .connection
        .insert_additional_config("auto.offset.reset", "latest");

    let backend_latest = KafkaEventBusBackend::new(latest_config)
        .expect("Kafka backend initialization failed for latest offset test");
    let mut latest_app = App::new();
    latest_app.add_plugins(EventBusPlugins(backend_latest));

    #[derive(Resource, Default)]
    struct CollectedLatest(Vec<TestEvent>);
    latest_app.insert_resource(CollectedLatest::default());

    let topic_read = topic.clone();
    latest_app.add_systems(
        Update,
        move |mut r: KafkaEventReader<TestEvent>, mut c: ResMut<CollectedLatest>| {
            let config = KafkaConsumerConfig::new(consumer_group.clone(), [&topic_read]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Now add a writer to the same app to send new events AFTER consumer is established
    let topic_send = topic.clone();
    latest_app.add_systems(Update, move |mut w: KafkaEventWriter| {
        // Send new event after consumer is established
        let config = KafkaProducerConfig::new([topic_send.clone()]);
        let _ = w.write(
            &config,
            TestEvent {
                message: "new_event".to_string(),
                value: 999,
            },
        );
    });

    // Wait until we receive the new event (but not historical ones)
    let (ok, _frames) = update_until(&mut latest_app, 5000, |app| {
        let c = app.world().resource::<CollectedLatest>();
        c.0.iter().any(|e| e.message == "new_event")
    });

    assert!(
        ok,
        "Consumer with 'latest' offset should receive new events"
    );

    let collected = latest_app.world().resource::<CollectedLatest>();

    // Should have received the new event
    let new_events: Vec<&TestEvent> = collected
        .0
        .iter()
        .filter(|e| e.message == "new_event")
        .collect();
    assert!(
        !new_events.is_empty(),
        "Expected to receive new_event, got: {:?}",
        collected.0
    );

    // Should NOT have received historical events
    let historical_events: Vec<&TestEvent> = collected
        .0
        .iter()
        .filter(|e| e.message.starts_with("historical_"))
        .collect();
    assert!(
        historical_events.is_empty(),
        "Consumer with 'latest' should NOT receive historical events, but got: {:?}",
        historical_events
    );
}

/// Test default behavior (should be "latest")
#[test]
fn default_offset_configuration_is_latest() {
    let topic = unique_topic("default_offset");

    // Create topic and ensure it's ready before proceeding
    let (_backend_setup, bootstrap) =
        kafka_setup::prepare_backend(kafka_setup::build_topology(|_| {}));
    let topic_ready = kafka_setup::ensure_topic_ready(
        &bootstrap,
        &topic,
        1, // partitions
        std::time::Duration::from_secs(5),
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);

    // Send historical events first
    {
        let topic_for_binding = topic.clone();
        let producer_builder = kafka_setup::build_topology(move |builder| {
            builder
                .add_topic(
                    KafkaTopicSpec::new(topic_for_binding.clone())
                        .partitions(1)
                        .replication(1),
                )
                .add_event_single::<TestEvent>(topic_for_binding.clone());
        });
        let (backend_producer, _bootstrap) =
            kafka_setup::prepare_backend((producer_builder, SetupOptions::latest()));
        let mut producer_app = App::new();
        producer_app.add_plugins(EventBusPlugins(backend_producer));

        let topic_clone = topic.clone();
        producer_app.add_systems(Update, move |mut w: KafkaEventWriter| {
            let config = KafkaProducerConfig::new([topic_clone.clone()]);
            let _ = w.write(
                &config,
                TestEvent {
                    message: "should_not_see_this".to_string(),
                    value: 42,
                },
            );
        });
        producer_app.update();
    }

    // Consumer with default config (no overrides) - should use "latest" behavior
    let consumer_group = unique_consumer_group("default_offset");
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
                        .initial_offset(KafkaInitialOffset::Latest),
                )
                .add_event_single::<TestEvent>(topic_for_config.clone());
        },
    ))
    .expect("Kafka backend initialization failed for default offset test");
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(backend));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    app.insert_resource(Collected::default());

    let topic_read = topic.clone();
    app.add_systems(
        Update,
        move |mut r: KafkaEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config = KafkaConsumerConfig::new(consumer_group.clone(), [&topic_read]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Run briefly to see if any historical events come through (they shouldn't)
    // Use a short timeout since we don't expect to find anything
    let (_ok, _frames) = update_until(&mut app, 1000, |app| {
        let c = app.world().resource::<Collected>();
        // Return true if we find the historical event (which would be bad)
        c.0.iter().any(|e| e.message == "should_not_see_this")
    });

    let collected = app.world().resource::<Collected>();
    let historical_count = collected
        .0
        .iter()
        .filter(|e| e.message == "should_not_see_this")
        .count();

    assert_eq!(
        historical_count, 0,
        "Default configuration should behave like 'latest' (not receive historical events)"
    );
}
