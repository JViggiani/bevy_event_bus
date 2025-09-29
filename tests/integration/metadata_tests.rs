use crate::common::events::TestEvent;
use crate::common::helpers::{
    DEFAULT_KAFKA_BOOTSTRAP, kafka_consumer_config, kafka_producer_config, run_app_updates,
    unique_consumer_group, unique_topic, wait_for_events,
};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{
    EventBusAppExt, EventBusPlugins, EventBusReader, EventBusWriter, EventWrapper,
};
use std::collections::HashMap;
use tracing::{info, info_span};

#[test]
fn metadata_propagation_from_kafka_to_bevy() {
    let _span = info_span!("metadata_propagation_test").entered();

    let (backend_w, _b1) = setup();
    let (backend_r, _b2) = setup();
    let topic = unique_topic("metadata");

    let mut writer = App::new();
    let mut reader = App::new();

    writer.add_plugins(EventBusPlugins(
        backend_w,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    writer.add_bus_event::<TestEvent>(&topic);

    reader.add_plugins(EventBusPlugins(
        backend_r,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    reader.add_bus_event::<TestEvent>(&topic);

    // Create test event
    let test_event = TestEvent {
        message: "metadata test".to_string(),
        value: 42,
    };

    // Send event
    let topic_clone = topic.clone();
    writer.add_systems(
        Update,
        move |mut w: EventBusWriter<TestEvent>, mut sent: Local<bool>| {
            if !*sent {
                *sent = true;
                let _ = w.write(
                    &kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&topic_clone]),
                    test_event.clone(),
                );
            }
        },
    );

    #[derive(Resource, Default)]
    struct ReceivedEvents(Vec<EventWrapper<TestEvent>>);

    reader.insert_resource(ReceivedEvents::default());
    let tr = topic.clone();
    let consumer_group = unique_consumer_group("metadata_propagation");
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut events: ResMut<ReceivedEvents>| {
            for wrapper in r.read(&kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group.as_str(),
                [&tr],
            )) {
                if wrapper.is_external() {
                    events.0.push(wrapper.clone());
                }
            }
        },
    );

    // Send the event
    writer.update();
    writer.update();

    // Wait for the event to arrive
    let received = wait_for_events(&mut reader, &topic, 5000, 1, |app| {
        let events = app.world().resource::<ReceivedEvents>();
        events.0.clone()
    });

    assert_eq!(
        received.len(),
        1,
        "Should receive exactly one event with metadata"
    );

    let external_event = &received[0];
    // Thanks to Deref, we can access event fields directly
    assert_eq!(external_event.message, "metadata test");
    assert_eq!(external_event.value, 42);

    // Verify metadata using the metadata() method
    let metadata = external_event
        .metadata()
        .expect("External event should have metadata");
    assert_eq!(metadata.source, topic);
    assert!(metadata.timestamp > std::time::Instant::now() - std::time::Duration::from_secs(10));

    // Get Kafka-specific metadata
    if let Some(backend_meta) = &metadata.backend_specific {
        if let Some(kafka_meta) = backend_meta
            .as_any()
            .downcast_ref::<bevy_event_bus::KafkaMetadata>()
        {
            assert_eq!(kafka_meta.topic, topic);
            assert!(kafka_meta.partition >= 0);
            assert!(kafka_meta.offset >= 0);

            info!(
                topic = %kafka_meta.topic,
                partition = kafka_meta.partition,
                offset = kafka_meta.offset,
                "Metadata verification successful"
            );
        } else {
            panic!("Expected Kafka metadata, but got different backend type");
        }
    } else {
        panic!("No backend-specific metadata found");
    }
}

#[test]
fn header_forwarding_producer_to_consumer() {
    let _span = info_span!("header_forwarding_test").entered();

    let (backend_w, _b1) = setup();
    let (backend_r, _b2) = setup();
    let topic = unique_topic("headers");

    let mut writer = App::new();
    let mut reader = App::new();

    writer.add_plugins(EventBusPlugins(
        backend_w,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    writer.add_bus_event::<TestEvent>(&topic);

    reader.add_plugins(EventBusPlugins(
        backend_r,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    reader.add_bus_event::<TestEvent>(&topic);

    // Create test event
    let test_event = TestEvent {
        message: "header test".to_string(),
        value: 123,
    };

    let mut headers = HashMap::new();
    headers.insert("trace-id".to_string(), "abc-123".to_string());
    headers.insert("user-id".to_string(), "user-456".to_string());
    headers.insert("correlation-id".to_string(), "corr-789".to_string());

    // Send event with headers
    let topic_clone = topic.clone();
    writer.add_systems(
        Update,
        move |mut w: EventBusWriter<TestEvent>, mut sent: Local<bool>| {
            if !*sent {
                *sent = true;
                let _ = w.write_with_headers(
                    &kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&topic_clone]),
                    test_event.clone(),
                    headers.clone(),
                );
            }
        },
    );

    #[derive(Resource, Default)]
    struct ReceivedEvents(Vec<EventWrapper<TestEvent>>);

    reader.insert_resource(ReceivedEvents::default());
    let tr = topic.clone();
    let consumer_group_name = unique_consumer_group("metadata_headers");
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut events: ResMut<ReceivedEvents>| {
            for wrapper in r.read(&kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group_name.as_str(),
                [&tr],
            )) {
                if wrapper.is_external() {
                    events.0.push(wrapper.clone());
                }
            }
        },
    );

    // Send the event and let it propagate
    run_app_updates(&mut writer, 2);

    // Wait for events to be received
    let received_events = wait_for_events(
        &mut reader,
        "metadata_propagation_test",
        5000, // 5 seconds timeout
        1,    // Wait for at least 1 event
        |app| {
            let events = app.world().resource::<ReceivedEvents>();
            events.0.clone()
        },
    );

    // Verify the event was received with headers
    let received_event = &received_events[0];
    assert_eq!(received_event.event().message, "header test");
    assert_eq!(received_event.event().value, 123);

    // Verify headers were forwarded
    let metadata = received_event
        .metadata()
        .expect("External event should have metadata");
    assert_eq!(
        metadata.headers.get("trace-id"),
        Some(&"abc-123".to_string())
    );
    assert_eq!(
        metadata.headers.get("user-id"),
        Some(&"user-456".to_string())
    );
    assert_eq!(
        metadata.headers.get("correlation-id"),
        Some(&"corr-789".to_string())
    );

    // Also verify Kafka-specific metadata exists
    if let Some(_kafka_meta) = metadata.kafka_metadata() {
        // Kafka metadata exists, which is good
    } else {
        panic!("Expected Kafka metadata with headers");
    }

    info!("Header forwarding test completed successfully");
}

#[test]
fn timestamp_accuracy_for_latency_measurement() {
    let _span = info_span!("timestamp_accuracy_test").entered();

    let (backend_w, _b1) = setup();
    let (backend_r, _b2) = setup();
    let topic = unique_topic("timestamp");

    let mut writer = App::new();
    let mut reader = App::new();

    writer.add_plugins(EventBusPlugins(
        backend_w,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    writer.add_bus_event::<TestEvent>(&topic);

    reader.add_plugins(EventBusPlugins(
        backend_r,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    reader.add_bus_event::<TestEvent>(&topic);

    let send_time = std::time::Instant::now();

    // Send event and capture send time
    let topic_clone = topic.clone();
    writer.add_systems(
        Update,
        move |mut w: EventBusWriter<TestEvent>, mut sent: Local<bool>| {
            if !*sent {
                *sent = true;
                let event = TestEvent {
                    message: "timestamp test".to_string(),
                    value: 999,
                };
                let _ = w.write(
                    &kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&topic_clone]),
                    event,
                );
            }
        },
    );

    #[derive(Resource, Default)]
    struct ReceivedEvents(Vec<EventWrapper<TestEvent>>);

    reader.insert_resource(ReceivedEvents::default());
    let tr = topic.clone();
    let consumer_group = unique_consumer_group("metadata_timestamp");
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut events: ResMut<ReceivedEvents>| {
            for wrapper in r.read(&kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group.as_str(),
                [&tr],
            )) {
                if wrapper.is_external() {
                    events.0.push(wrapper.clone());
                }
            }
        },
    );

    // Send the event
    writer.update();
    writer.update();

    // Wait for the event to arrive
    let received = wait_for_events(&mut reader, &topic, 5000, 1, |app| {
        let events = app.world().resource::<ReceivedEvents>();
        events.0.clone()
    });

    assert_eq!(received.len(), 1, "Should receive exactly one event");

    let external_event = &received[0];
    let receive_time = std::time::Instant::now();

    // Get metadata for timestamp verification
    let metadata = external_event
        .metadata()
        .expect("External event should have metadata");

    // Verify timestamp is reasonable (between send time and receive time)
    assert!(
        metadata.timestamp >= send_time,
        "Event timestamp should be after send time"
    );
    assert!(
        metadata.timestamp <= receive_time,
        "Event timestamp should be before receive time"
    );

    // Calculate and verify latency is reasonable (should be less than 1 second for local test)
    let latency = receive_time.saturating_duration_since(metadata.timestamp);
    assert!(
        latency < std::time::Duration::from_secs(1),
        "Latency should be less than 1 second, got: {:?}",
        latency
    );

    info!(
        latency_ms = latency.as_millis(),
        "Timestamp accuracy verification successful"
    );
}

#[test]
fn mixed_metadata_and_regular_reading() {
    let _span = info_span!("mixed_reading_test").entered();

    let (backend_w, _b1) = setup();
    let (backend_r1, _b2) = setup();
    let (backend_r2, _b3) = setup();
    let topic = unique_topic("mixed");

    let mut writer = App::new();
    let mut regular_reader = App::new();
    let mut metadata_reader = App::new();

    writer.add_plugins(EventBusPlugins(
        backend_w,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    writer.add_bus_event::<TestEvent>(&topic);

    regular_reader.add_plugins(EventBusPlugins(
        backend_r1,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    regular_reader.add_bus_event::<TestEvent>(&topic);

    metadata_reader.add_plugins(EventBusPlugins(
        backend_r2,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    metadata_reader.add_bus_event::<TestEvent>(&topic);

    // Send events
    let topic_clone = topic.clone();
    writer.add_systems(
        Update,
        move |mut w: EventBusWriter<TestEvent>, mut sent: Local<bool>| {
            if !*sent {
                *sent = true;
                for i in 0..3 {
                    let event = TestEvent {
                        message: format!("mixed-{}", i),
                        value: i,
                    };
                    let _ = w.write(
                        &kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&topic_clone]),
                        event,
                    );
                }
            }
        },
    );

    #[derive(Resource, Default)]
    struct RegularEvents(Vec<TestEvent>);
    #[derive(Resource, Default)]
    struct MetadataEvents(Vec<EventWrapper<TestEvent>>);

    regular_reader.insert_resource(RegularEvents::default());
    metadata_reader.insert_resource(MetadataEvents::default());

    let tr1 = topic.clone();
    let tr2 = topic.clone();
    let regular_group = unique_consumer_group("metadata_regular");
    let metadata_group = unique_consumer_group("metadata_reader");

    // Regular reader using read()
    regular_reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut events: ResMut<RegularEvents>| {
            for wrapper in r.read(&kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                regular_group.as_str(),
                [&tr1],
            )) {
                events.0.push(wrapper.event().clone());
            }
        },
    );

    // Metadata reader using read() with External filtering
    metadata_reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut events: ResMut<MetadataEvents>| {
            for wrapper in r.read(&kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                metadata_group.as_str(),
                [&tr2],
            )) {
                if wrapper.is_external() {
                    events.0.push(wrapper.clone());
                }
            }
        },
    );

    // Send the events
    writer.update();
    writer.update();

    // Wait for events to arrive - use separate readers
    let regular = wait_for_events(&mut regular_reader, &topic, 5000, 3, |app| {
        let events = app.world().resource::<RegularEvents>();
        events.0.clone()
    });

    let metadata = wait_for_events(&mut metadata_reader, &topic, 5000, 3, |app| {
        let events = app.world().resource::<MetadataEvents>();
        events.0.clone()
    });

    // Both methods should see the same events
    assert_eq!(regular.len(), 3, "Regular reader should receive 3 events");
    assert_eq!(metadata.len(), 3, "Metadata reader should receive 3 events");

    // Verify that both readers got the same event data
    for (i, (regular_event, metadata_event)) in regular.iter().zip(metadata.iter()).enumerate() {
        assert_eq!(regular_event.message, format!("mixed-{}", i));
        assert_eq!(regular_event.value, i as i32);

        // Metadata version should have the same event data (via Deref)
        assert_eq!(metadata_event.message, format!("mixed-{}", i));
        assert_eq!(metadata_event.value, i as i32);

        // Verify Kafka metadata using the metadata() method
        let metadata = metadata_event
            .metadata()
            .expect("External event should have metadata");
        if let Some(kafka_meta) = metadata.kafka_metadata() {
            assert_eq!(kafka_meta.topic, topic);
        } else {
            panic!("Expected Kafka metadata for event {}", i);
        }
    }

    info!("Mixed reading verification successful");
}
