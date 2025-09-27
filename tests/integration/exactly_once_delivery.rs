use crate::common::events::TestEvent;
use crate::common::helpers::{unique_topic, unique_consumer_group, update_until};
use crate::common::setup::{setup, build_app};
use bevy::prelude::*;
use bevy_event_bus::{EventBusReader, EventBusWriter, EventBusAppExt, KafkaReadConfig, KafkaWriteConfig};

/// Test that events are delivered exactly once - no duplication
#[test]
fn no_event_duplication_exactly_once_delivery() {
    let topic = unique_topic("exactly_once");
    
    // Writer app with separate backend
    let (backend_writer, _bootstrap_writer) = setup(None);
    let mut writer = crate::common::setup::build_app(backend_writer, None, |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });

    // Send exactly 10 unique events (as a resource to avoid closure issues)
    #[derive(Resource, Clone)]
    struct ToSend(Vec<TestEvent>, String);
    
    let expected_events: Vec<TestEvent> = (0..10)
        .map(|i| TestEvent {
            message: format!("unique_event_{}", i),
            value: i,
        })
        .collect();
    
    writer.insert_resource(ToSend(expected_events.clone(), topic.clone()));

    fn writer_system(mut w: EventBusWriter<TestEvent>, data: Res<ToSend>) {
        for event in &data.0 {
            let _ = w.write(&KafkaWriteConfig::new(&data.1), event.clone());
        }
    }
    writer.add_systems(Update, writer_system);

    // Reader app with pre-created consumer group
    let consumer_group = unique_consumer_group("test_group");
    let config = KafkaReadConfig::new(&consumer_group).topics([&topic]);
    let (backend, _bootstrap) = crate::common::setup::setup(None);
    let mut reader = build_app(backend, Some(&[config.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic);
        app.insert_resource(Collected::default());
    });

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    
    // Capture the same config for use in the system
    let config_for_system = config.clone();
    reader.add_systems(Update, move |mut r: EventBusReader<TestEvent>, mut collected: ResMut<Collected>| {
        // Use the SAME config that was passed to setup, not recreated
        for wrapper in r.read(&config_for_system) {
            collected.0.push(wrapper.event().clone());
        }
    });

    // Start writer to send events
    writer.update();

    // Wait for all 10 events to be received
    let (ok, _frames) = update_until(&mut reader, 5000, |app| {
        let c = app.world().resource::<Collected>();
        c.0.len() >= 10
    });

    let collected = reader.world().resource::<Collected>();
    
    assert!(ok, "Timed out waiting for events. Got {} events", collected.0.len());
    
    // Verify exactly 10 events were received (no duplication)
    assert_eq!(collected.0.len(), 10, "Expected exactly 10 events, got {}", collected.0.len());
    
    // Verify each expected event appears exactly once
    for expected_event in &expected_events {
        let count = collected.0.iter()
            .filter(|e| e.message == expected_event.message && e.value == expected_event.value)
            .count();
        assert_eq!(count, 1, "Event {:?} appeared {} times, expected exactly 1", expected_event, count);
    }
    
    // Wait a bit more and verify no additional events arrive
    let (_, _) = update_until(&mut reader, 1000, |_| false); // Just wait, don't expect anything
    
    let collected_after_wait = reader.world().resource::<Collected>();
    assert_eq!(
        collected_after_wait.0.len(),
        10,
        "Additional events appeared after waiting: expected 10, got {}",
        collected_after_wait.0.len()
    );
}
