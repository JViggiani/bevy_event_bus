use crate::common::events::TestEvent;
use crate::common::helpers::{unique_topic, update_until};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusReader, EventBusWriter};

/// Test that events are delivered exactly once - no duplication
#[test]
fn no_event_duplication_exactly_once_delivery() {
    let (backend_w, _b1, _t1) = setup();
    let (backend_r, _b2, _t2) = setup();
    let topic = unique_topic("exactly_once");
    
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(
        backend_w,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(
        backend_r,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));

    // Send exactly 10 unique events
    let expected_events: Vec<TestEvent> = (0..10)
        .map(|i| TestEvent {
            message: format!("unique_event_{}", i),
            value: i,
        })
        .collect();

    let topic_clone = topic.clone();
    let expected_clone = expected_events.clone();
    writer.add_systems(Update, move |mut w: EventBusWriter<TestEvent>| {
        for event in &expected_clone {
            let _ = w.send(&topic_clone, event.clone());
        }
    });
    
    writer.update(); // Send all events

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());
    
    let topic_read = topic.clone();
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut c: ResMut<Collected>| {
            for ev in r.try_read(&topic_read) {
                c.0.push(ev.clone());
            }
        },
    );

    // Wait for all 10 events to be received
    let (ok, _frames) = update_until(&mut reader, 5000, |app| {
        let c = app.world().resource::<Collected>();
        c.0.len() >= 10
    });

    assert!(ok, "Timed out waiting for events");
    
    let collected = reader.world().resource::<Collected>();
    
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
