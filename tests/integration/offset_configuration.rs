use crate::common::events::TestEvent;
use crate::common::helpers::{unique_string, update_until, wait_for_events, unique_consumer_group};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{EventBusReader, EventBusWriter, EventBusAppExt, KafkaReadConfig, KafkaWriteConfig};

/// Test that consumers with "earliest" offset receive historical events
#[test] 
fn offset_configuration_earliest_receives_historical_events() {
    #[derive(Resource, Default)]
    struct CollectedEarliest(Vec<TestEvent>);
    
    let topic = unique_string("offset_test_earliest");
    
    // Create topic and ensure it's ready before proceeding
    let (_backend_setup, bootstrap) = setup(None);
    let topic_ready = crate::common::setup::ensure_topic_ready(
        &bootstrap, 
        &topic, 
        1, // partitions
        std::time::Duration::from_secs(5)
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);
    
    // First, send some events to the topic using a temporary producer
    {
        let (backend_producer, _bootstrap) = setup(None);
        let mut producer_app = crate::common::setup::build_app(backend_producer, None, |app| {
            app.add_bus_event::<TestEvent>(&topic);
        });
        
        let write_config = KafkaWriteConfig::new(&topic);
        
        producer_app.add_systems(Update, move |mut w: EventBusWriter<TestEvent>| {
            // Send 3 historical events
            for i in 0..3 {
                let _ = w.write(&write_config, TestEvent {
                    message: format!("historical_{}", i),
                    value: i,
                });
            }
        });
        producer_app.update(); // Send the historical events
    }
    
    // Test: Consumer with "earliest" should see historical events
    let consumer_group = format!("earliest_test_{}", unique_string("group"));
    
    let read_config = KafkaReadConfig::new(&consumer_group).topics([&topic]);
    
    let (backend_reader, _bootstrap_reader) = setup(Some("earliest"));
    let mut earliest_app = crate::common::setup::build_app(
        backend_reader,
        Some(&[read_config.clone()]), 
        |app| {
            app.add_bus_event::<TestEvent>(&topic);
            
            app.insert_resource(CollectedEarliest::default());
            
            app.add_systems(
                Update,
                move |mut r: EventBusReader<TestEvent>, mut c: ResMut<CollectedEarliest>| {
                    for wrapper in r.read(&read_config) {
                        c.0.push(wrapper.event().clone());
                    }
                },
            );
        },
    );
    
    // Should eventually receive the 3 historical events
    let (ok, _frames) = update_until(&mut earliest_app, 5000, |app| {
        let c = app.world().resource::<CollectedEarliest>();
        c.0.len() >= 3
    });
    
    assert!(ok, "Consumer with 'earliest' should receive historical events within timeout");
    
    let collected = earliest_app.world().resource::<CollectedEarliest>();
    assert!(collected.0.len() >= 3, 
        "Consumer with 'earliest' should receive historical events. Got {} events: {:?}", 
        collected.0.len(), collected.0);
    
    // Verify we got the historical events
    let historical_messages: Vec<String> = collected.0.iter()
        .map(|e| e.message.clone())
        .filter(|m| m.starts_with("historical_"))
        .collect();
    assert!(historical_messages.len() >= 3, 
           "Expected at least 3 historical events, got: {:?}", historical_messages);
}

/// Test that consumers with "latest" offset ignore historical events
#[test]
fn offset_configuration_latest_ignores_historical_events() {
    #[derive(Resource, Default)]
    struct CollectedLatest(Vec<TestEvent>);
    
    let topic = unique_string("offset_test_latest");
    
    // Create topic and ensure it's ready before proceeding
    let (_backend_setup, bootstrap) = setup(None);
    let topic_ready = crate::common::setup::ensure_topic_ready(
        &bootstrap, 
        &topic, 
        1, // partitions
        std::time::Duration::from_secs(5)
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);
    
    // Send historical events first
    {
        let (backend_producer, _bootstrap) = setup(None);
        let mut producer_app = crate::common::setup::build_app(backend_producer, None, |app| {
            app.add_bus_event::<TestEvent>(&topic);
        });
        
        let write_config = KafkaWriteConfig::new(&topic);
        producer_app.add_systems(Update, move |mut w: EventBusWriter<TestEvent>| {
            // Send 3 historical events that the latest consumer should NOT see
            for i in 0..3 {
                let _ = w.write(&write_config, TestEvent {
                    message: format!("historical_{}", i),
                    value: i,
                });
            }
        });
        producer_app.update(); // Send the historical events
    }
    
    // Create consumer with "latest" offset - should NOT see historical events
    let consumer_group = format!("latest_test_{}", unique_string("group"));
    let read_config = KafkaReadConfig::new(&consumer_group).topics([&topic]);
    
    let (backend_reader, _bootstrap_reader) = setup(Some("latest"));
    let mut latest_app = crate::common::setup::build_app(
        backend_reader,
        Some(&[read_config.clone()]), 
        |app| {
            app.add_bus_event::<TestEvent>(&topic);
            
            app.insert_resource(CollectedLatest::default());
            
            app.add_systems(
                Update,
                move |mut r: EventBusReader<TestEvent>, mut c: ResMut<CollectedLatest>| {
                    for wrapper in r.read(&read_config) {
                        c.0.push(wrapper.event().clone());
                    }
                },
            );
            
            // Now add a writer to send new events AFTER consumer is established
            let write_config = KafkaWriteConfig::new(&topic);
            app.add_systems(Update, move |mut w: EventBusWriter<TestEvent>| {
                // Send new event after consumer is established
                let _ = w.write(&write_config, TestEvent {
                    message: "new_event".to_string(),
                    value: 999,
                });
            });
        },
    );
    
    // Wait until we receive the new event (but not historical ones)
    let (ok, _frames) = update_until(&mut latest_app, 5000, |app| {
        let c = app.world().resource::<CollectedLatest>();
        c.0.iter().any(|e| e.message == "new_event")
    });
    
    assert!(ok, "Consumer with 'latest' offset should receive new events");
    
    let collected = latest_app.world().resource::<CollectedLatest>();
    
    // Should have received the new event
    let new_events: Vec<&TestEvent> = collected.0.iter()
        .filter(|e| e.message == "new_event")
        .collect();
    assert!(!new_events.is_empty(), 
           "Expected to receive new_event, got: {:?}", collected.0);
    
    // Should NOT have received historical events
    let historical_events: Vec<&TestEvent> = collected.0.iter()
        .filter(|e| e.message.starts_with("historical_"))
        .collect();
    assert!(historical_events.is_empty(), 
           "Consumer with 'latest' should NOT receive historical events, but got: {:?}", historical_events);
}

/// Test default behavior (should be "latest")
#[test]
fn default_offset_configuration_is_latest() {
    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    
    let topic = unique_string("default_offset");
    
    // Create topic and ensure it's ready before proceeding
    let (_backend_setup, bootstrap) = setup(None);
    let topic_ready = crate::common::setup::ensure_topic_ready(
        &bootstrap, 
        &topic, 
        1, // partitions
        std::time::Duration::from_secs(5)
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);
    
    // Send historical events first
    {
        let (backend_producer, _bootstrap) = setup(None);
        let mut producer_app = crate::common::setup::build_app(backend_producer, None, |app| {
            app.add_bus_event::<TestEvent>(&topic);
        });
        
        let write_config = KafkaWriteConfig::new(&topic);
        producer_app.add_systems(Update, move |mut w: EventBusWriter<TestEvent>| {
            let _ = w.write(&write_config, TestEvent {
                message: "should_not_see_this".to_string(),
                value: 42,
            });
        });
        producer_app.update();
    }
    
    // Consumer with default config - should use "latest" behavior by default
    let read_config = KafkaReadConfig::new(&unique_consumer_group("default_test_group")).topics([&topic]);
    
    let (backend_reader, _bootstrap_reader) = setup(None);
    let mut app = crate::common::setup::build_app(
        backend_reader,
        Some(&[read_config.clone()]), 
        |app| {
            app.add_bus_event::<TestEvent>(&topic);
            
            app.insert_resource(Collected::default());
            
            app.add_systems(
                Update,
                move |mut r: EventBusReader<TestEvent>, mut c: ResMut<Collected>| {
                    for wrapper in r.read(&read_config) {
                        c.0.push(wrapper.event().clone());
                    }
                },
            );
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
    let historical_count = collected.0.iter()
        .filter(|e| e.message == "should_not_see_this")
        .count();
    
    assert_eq!(historical_count, 0, 
              "Default configuration should behave like 'latest' (not receive historical events)");
}