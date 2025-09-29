use crate::common::events::TestEvent;
use crate::common::helpers::{
    DEFAULT_KAFKA_BOOTSTRAP, kafka_consumer_config, kafka_producer_config, unique_consumer_group,
    unique_topic, update_until,
};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{
    EventBusAppExt, EventBusPlugins, EventBusReader, EventBusWriter, KafkaConnection,
    KafkaEventBusBackend,
};
use std::collections::HashMap;

/// Test that consumers with "earliest" offset receive historical events
#[test]
fn offset_configuration_earliest_receives_historical_events() {
    let topic = unique_topic("offset_test_earliest");

    // Create topic and ensure it's ready before proceeding
    let (_backend_setup, bootstrap) = setup();
    let topic_ready = crate::common::setup::ensure_topic_ready(
        &bootstrap,
        &topic,
        1, // partitions
        std::time::Duration::from_secs(5),
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);

    // First, send some events to the topic using a temporary producer
    {
        let (backend_producer, _bootstrap) = setup();
        let mut producer_app = App::new();
        producer_app.add_plugins(EventBusPlugins(
            backend_producer,
            bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
        ));
        producer_app.add_bus_event::<TestEvent>(&topic);

        let topic_clone = topic.clone();
        producer_app.add_systems(Update, move |mut w: EventBusWriter| {
            // Send 3 historical events
            for i in 0..3 {
                let config = kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&topic_clone]);
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

    // Test: Consumer with "earliest" should see historical events
    let mut connection = KafkaConnection::default();
    connection.bootstrap_servers = bootstrap;
    // TODO: This should be in KafkaConsumerConfig, but current architecture applies connection-level config to all consumers
    connection
        .additional_config
        .insert("auto.offset.reset".to_string(), "earliest".to_string());

    let backend_earliest = KafkaEventBusBackend::new(connection);
    let mut earliest_app = App::new();
    earliest_app.add_plugins(EventBusPlugins(
        backend_earliest,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    earliest_app.add_bus_event::<TestEvent>(&topic);

    #[derive(Resource, Default)]
    struct CollectedEarliest(Vec<TestEvent>);
    earliest_app.insert_resource(CollectedEarliest::default());

    let topic_read = topic.clone();
    let consumer_group = unique_consumer_group("earliest_test");
    earliest_app.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut c: ResMut<CollectedEarliest>| {
            let config = kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group.as_str(),
                [&topic_read],
            );
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Should eventually receive the 3 historical events
    let (ok, _frames) = update_until(&mut earliest_app, 5000, |app| {
        let c = app.world().resource::<CollectedEarliest>();
        c.0.len() >= 3
    });

    assert!(
        ok,
        "Consumer with 'earliest' should receive historical events within timeout"
    );

    let collected = earliest_app.world().resource::<CollectedEarliest>();
    assert!(
        collected.0.len() >= 3,
        "Consumer with 'earliest' should receive historical events. Got {} events: {:?}",
        collected.0.len(),
        collected.0
    );

    // Verify we got the historical events
    let historical_messages: Vec<String> = collected
        .0
        .iter()
        .map(|e| e.message.clone())
        .filter(|m| m.starts_with("historical_"))
        .collect();
    assert!(
        historical_messages.len() >= 3,
        "Expected at least 3 historical events, got: {:?}",
        historical_messages
    );
}

/// Test that consumers with "latest" offset ignore historical events
#[test]
fn offset_configuration_latest_ignores_historical_events() {
    let topic = unique_topic("offset_test_latest");

    // Create topic and ensure it's ready before proceeding
    let (_backend_setup, bootstrap) = setup();
    let topic_ready = crate::common::setup::ensure_topic_ready(
        &bootstrap,
        &topic,
        1, // partitions
        std::time::Duration::from_secs(5),
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);

    // Send historical events first
    {
        let (backend_producer, _bootstrap) = setup();
        let mut producer_app = App::new();
        producer_app.add_plugins(EventBusPlugins(
            backend_producer,
            bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
        ));
        producer_app.add_bus_event::<TestEvent>(&topic);

        let topic_clone = topic.clone();
        producer_app.add_systems(Update, move |mut w: EventBusWriter| {
            // Send 3 historical events that the latest consumer should NOT see
            for i in 0..3 {
                let config = kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&topic_clone]);
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
    let mut connection = KafkaConnection::default();
    connection.bootstrap_servers = bootstrap.clone();
    // TODO: This should be in KafkaConsumerConfig, but current architecture applies connection-level config to all consumers
    connection
        .additional_config
        .insert("auto.offset.reset".to_string(), "latest".to_string());

    let backend_latest = KafkaEventBusBackend::new(connection);
    let mut latest_app = App::new();
    latest_app.add_plugins(EventBusPlugins(
        backend_latest,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    latest_app.add_bus_event::<TestEvent>(&topic);

    #[derive(Resource, Default)]
    struct CollectedLatest(Vec<TestEvent>);
    latest_app.insert_resource(CollectedLatest::default());

    let topic_read = topic.clone();
    let consumer_group = unique_consumer_group("latest_test");
    latest_app.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut c: ResMut<CollectedLatest>| {
            let config = kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group.as_str(),
                [&topic_read],
            );
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Now add a writer to the same app to send new events AFTER consumer is established
    let topic_send = topic.clone();
    latest_app.add_systems(Update, move |mut w: EventBusWriter| {
        // Send new event after consumer is established
        let config = kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&topic_send]);
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
    let (_backend_setup, bootstrap) = setup();
    let topic_ready = crate::common::setup::ensure_topic_ready(
        &bootstrap,
        &topic,
        1, // partitions
        std::time::Duration::from_secs(5),
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);

    // Send historical events first
    {
        let (backend_producer, _bootstrap) = setup();
        let mut producer_app = App::new();
        producer_app.add_plugins(EventBusPlugins(
            backend_producer,
            bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
        ));
        producer_app.add_bus_event::<TestEvent>(&topic);

        let topic_clone = topic.clone();
        producer_app.add_systems(Update, move |mut w: EventBusWriter| {
            let config = kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&topic_clone]);
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

    // Consumer with default config (no additional_config specified) - should use "latest" behavior
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: None,
        timeout_ms: 5000,
        additional_config: HashMap::new(), // No overrides - use defaults
    };

    let backend = KafkaEventBusBackend::new(config);
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(
        backend,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    app.add_bus_event::<TestEvent>(&topic);

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    app.insert_resource(Collected::default());

    let topic_read = topic.clone();
    let consumer_group = unique_consumer_group("default_offset");
    app.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut c: ResMut<Collected>| {
            let config = kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group.as_str(),
                [&topic_read],
            );
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
