use crate::common::events::TestEvent;
use crate::common::helpers::{unique_topic, wait_for_events};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusReader, EventBusWriter, ExternalEvent, EventBusAppExt};
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
                let _ = w.write(&topic_clone, test_event.clone());
            }
        },
    );

    #[derive(Resource, Default)]
    struct ReceivedEvents(Vec<ExternalEvent<TestEvent>>);

    reader.insert_resource(ReceivedEvents::default());
    let tr = topic.clone();
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut events: ResMut<ReceivedEvents>| {
            for external_event in r.read_with_metadata(&tr) {
                events.0.push(external_event.clone());
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

    assert_eq!(received.len(), 1, "Should receive exactly one event with metadata");
    
    let external_event = &received[0];
    assert_eq!(external_event.event.message, "metadata test");
    assert_eq!(external_event.event.value, 42);
    
    // Verify metadata
    assert_eq!(external_event.metadata.topic, topic);
    assert!(external_event.metadata.partition >= 0);
    assert!(external_event.metadata.offset >= 0);
    assert!(external_event.metadata.timestamp > std::time::Instant::now() - std::time::Duration::from_secs(10));
    
    info!(
        topic = %external_event.metadata.topic,
        partition = external_event.metadata.partition,
        offset = external_event.metadata.offset,
        "Metadata verification successful"
    );
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

    // Create test event with headers
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
                let _ = w.write_with_headers(&topic_clone, test_event.clone(), headers.clone());
            }
        },
    );

    #[derive(Resource, Default)]
    struct ReceivedEvents(Vec<ExternalEvent<TestEvent>>);

    reader.insert_resource(ReceivedEvents::default());
    let tr = topic.clone();
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut events: ResMut<ReceivedEvents>| {
            for external_event in r.read_with_metadata(&tr) {
                events.0.push(external_event.clone());
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

    assert_eq!(received.len(), 1, "Should receive exactly one event with headers");
    
    let external_event = &received[0];
    assert_eq!(external_event.event.message, "header test");
    assert_eq!(external_event.event.value, 123);
    
    // Verify headers
    assert_eq!(external_event.metadata.headers.get("trace-id"), Some(&"abc-123".to_string()));
    assert_eq!(external_event.metadata.headers.get("user-id"), Some(&"user-456".to_string()));
    assert_eq!(external_event.metadata.headers.get("correlation-id"), Some(&"corr-789".to_string()));
    
    info!(
        headers = ?external_event.metadata.headers,
        "Header forwarding verification successful"
    );
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
                let _ = w.write(&topic_clone, event);
            }
        },
    );

    #[derive(Resource, Default)]
    struct ReceivedEvents(Vec<ExternalEvent<TestEvent>>);

    reader.insert_resource(ReceivedEvents::default());
    let tr = topic.clone();
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut events: ResMut<ReceivedEvents>| {
            for external_event in r.read_with_metadata(&tr) {
                events.0.push(external_event.clone());
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
    
    // Verify timestamp is reasonable (between send time and receive time)
    assert!(external_event.metadata.timestamp >= send_time, 
            "Event timestamp should be after send time");
    assert!(external_event.metadata.timestamp <= receive_time, 
            "Event timestamp should be before receive time");
    
    // Calculate and verify latency is reasonable (should be less than 1 second for local test)
    let latency = receive_time.saturating_duration_since(external_event.metadata.timestamp);
    assert!(latency < std::time::Duration::from_secs(1), 
            "Latency should be less than 1 second, got: {:?}", latency);
    
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
                    let _ = w.write(&topic_clone, event);
                }
            }
        },
    );

    #[derive(Resource, Default)]
    struct RegularEvents(Vec<TestEvent>);
    #[derive(Resource, Default)]  
    struct MetadataEvents(Vec<ExternalEvent<TestEvent>>);

    regular_reader.insert_resource(RegularEvents::default());
    metadata_reader.insert_resource(MetadataEvents::default());
    
    let tr1 = topic.clone();
    let tr2 = topic.clone();
    
    // Regular reader using read()
    regular_reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut events: ResMut<RegularEvents>| {
            for event in r.read(&tr1) {
                events.0.push(event.clone());
            }
        },
    );
    
    // Metadata reader using read_with_metadata()
    metadata_reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut events: ResMut<MetadataEvents>| {
            for external_event in r.read_with_metadata(&tr2) {
                events.0.push(external_event.clone());
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
        assert_eq!(metadata_event.event.message, format!("mixed-{}", i));
        assert_eq!(metadata_event.event.value, i as i32);
        assert_eq!(metadata_event.metadata.topic, topic);
    }
    
    info!("Mixed reading verification successful");
}