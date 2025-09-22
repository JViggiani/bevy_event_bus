use crate::common::events::TestEvent;
use crate::common::helpers::{unique_topic, update_until};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusReader, EventBusWriter, EventBusAppExt};

#[test]
fn multi_topic_isolation() {
    let (backend_w, _b1) = setup();
    let (backend_r, _b2) = setup();
    let topic_a = unique_topic("topicA");
    let topic_b = unique_topic("topicB");

    // Create topics and wait for them to be fully ready
    let topic_a_ready = crate::common::setup::ensure_topic_ready(
        &_b2, 
        &topic_a, 
        1, // partitions
        std::time::Duration::from_secs(5)
    );
    let topic_b_ready = crate::common::setup::ensure_topic_ready(
        &_b2, 
        &topic_b, 
        1, // partitions
        std::time::Duration::from_secs(5)
    );
    assert!(topic_a_ready, "Topic {} not ready within timeout", topic_a);
    assert!(topic_b_ready, "Topic {} not ready within timeout", topic_b);

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(
        backend_w,
        bevy_event_bus::PreconfiguredTopics::new([topic_a.clone(), topic_b.clone()]),
    ));
    writer.add_bus_event::<TestEvent>(&topic_a);
    writer.add_bus_event::<TestEvent>(&topic_b);
    
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(
        backend_r,
        bevy_event_bus::PreconfiguredTopics::new([topic_a.clone(), topic_b.clone()]),
    ));
    reader.add_bus_event::<TestEvent>(&topic_a);
    reader.add_bus_event::<TestEvent>(&topic_b);

    let ta = topic_a.clone();
    let tb = topic_b.clone();
    writer.add_systems(Update, move |mut w: EventBusWriter<TestEvent>| {
        let _ = w.write(
            &ta,
            TestEvent {
                message: "A1".into(),
                value: 1,
            },
        );
        let _ = w.write(
            &tb,
            TestEvent {
                message: "B1".into(),
                value: 2,
            },
        );
    });
    writer.update();

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());
    let ta_r = topic_a.clone();
    let tb_r = topic_b.clone();
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut col: ResMut<Collected>| {
            for wrapper in r.read(&ta_r) {
                col.0.push(wrapper.event().clone());
            }
            for wrapper in r.read(&tb_r) {
                col.0.push(wrapper.event().clone());
            }
        },
    );

    // Spin until both messages observed or timeout
    let (ok, _frames) = update_until(&mut reader, 5000, |app| {
        let col = app.world().resource::<Collected>();
        col.0.iter().any(|e| e.message == "A1") && col.0.iter().any(|e| e.message == "B1")
    });
    assert!(ok, "Timed out waiting for both topic messages");
    let col = reader.world().resource::<Collected>();
    assert!(col.0.iter().any(|e| e.message == "A1"));
    assert!(col.0.iter().any(|e| e.message == "B1"));
    assert!(
        col.0
            .iter()
            .filter(|e| e.message.starts_with('A'))
            .all(|e| e.message == "A1")
    );
    assert!(
        col.0
            .iter()
            .filter(|e| e.message.starts_with('B'))
            .all(|e| e.message == "B1")
    );
}
