use crate::common::events::TestEvent;
use crate::common::helpers::{unique_topic, unique_consumer_group, update_until};
use bevy_event_bus::{EventBusReader, EventBusWriter, EventBusAppExt, KafkaReadConfig, KafkaWriteConfig};
use crate::common::setup::{setup, build_app};
use bevy::prelude::*;

#[test]
fn multi_topic_isolation() {
    let (backend_w, _b1) = setup(None);
    let (_backend_r, _b2) = setup(None);
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

    let mut writer = crate::common::setup::build_app(backend_w, None, |app| {
        app.add_bus_event::<TestEvent>(&topic_a);
        app.add_bus_event::<TestEvent>(&topic_b);
    });
    
    // Create consumer configurations for both topics
    let consumer_group_a = unique_consumer_group("test_group_a");
    let consumer_group_b = unique_consumer_group("test_group_b");
    let config_a = KafkaReadConfig::new(&consumer_group_a).topics([&topic_a]);
    let config_b = KafkaReadConfig::new(&consumer_group_b).topics([&topic_b]);
    
    // Build reader app with consumer groups created during setup
    let (backend_r, _bootstrap_r) = crate::common::setup::setup(None);
    let mut reader = build_app(backend_r, Some(&[config_a.clone(), config_b.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic_a);
        app.add_bus_event::<TestEvent>(&topic_b);
    });

    let ta = topic_a.clone();
    let tb = topic_b.clone();
    writer.add_systems(Update, move |mut w: EventBusWriter<TestEvent>| {
        let _ = w.write(
            &KafkaWriteConfig::new(&ta),
            TestEvent {
                message: "A1".into(),
                value: 1,
            },
        );
        let _ = w.write(
            &KafkaWriteConfig::new(&tb),
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
    
    // Simple system that just reads - no consumer group creation needed
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut col: ResMut<Collected>| {
            for wrapper in r.read(&config_a) {
                col.0.push(wrapper.event().clone());
            }
            for wrapper in r.read(&config_b) {
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
