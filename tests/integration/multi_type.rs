use crate::common::events::{TestEvent, UserLoginEvent};
use crate::common::helpers::{unique_topic, update_until};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusReader, EventBusWriter, EventBusAppExt};

#[test]
fn single_topic_multiple_types_same_frame() {
    let (backend_w, _b1) = setup();
    let (backend_r, _b2) = setup();
    let topic = unique_topic("mixed");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(
        backend_w,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    writer.add_bus_event::<TestEvent>(&topic);
    writer.add_bus_event::<UserLoginEvent>(&topic);
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(
        backend_r,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    reader.add_bus_event::<TestEvent>(&topic);
    reader.add_bus_event::<UserLoginEvent>(&topic);

    #[derive(Resource, Default)]
    struct CollectedA(Vec<TestEvent>);
    #[derive(Resource, Default)]
    struct CollectedB(Vec<UserLoginEvent>);
    reader.insert_resource(CollectedA::default());
    reader.insert_resource(CollectedB::default());
    let tr_a = topic.clone();
    reader.add_systems(
        Update,
        move |mut r1: EventBusReader<TestEvent>, mut a: ResMut<CollectedA>| {
            for ev in r1.read(&tr_a) {
                a.0.push(ev.clone());
            }
        },
    );
    let tr_b = topic.clone();
    reader.add_systems(
        Update,
        move |mut r2: EventBusReader<UserLoginEvent>, mut b: ResMut<CollectedB>| {
            for ev in r2.read(&tr_b) {
                b.0.push(ev.clone());
            }
        },
    );

    // Ensure topic is ready before proceeding
    let topic_ready = crate::common::setup::ensure_topic_ready(
        &_b2, 
        &topic, 
        1, // partitions
        std::time::Duration::from_secs(5)
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);

    // Now set up and run the writer to send events
    let tclone = topic.clone();
    writer.add_systems(
        Update,
        move |mut w1: EventBusWriter<TestEvent>,
              mut w2: EventBusWriter<UserLoginEvent>,
              mut started: Local<bool>| {
            if !*started {
                *started = true;
                return;
            }
            let _ = w1.write(
                &tclone,
                TestEvent {
                    message: "hello".into(),
                    value: 42,
                },
            );
            let _ = w2.write(
                &tclone,
                UserLoginEvent {
                    user_id: "u1".into(),
                    timestamp: 1,
                },
            );
        },
    );
    writer.update(); // warm
    writer.update(); // send

    let (ok, _frames) = update_until(&mut reader, 5000, |app| {
        let a = app.world().resource::<CollectedA>();
        let b = app.world().resource::<CollectedB>();
        !a.0.is_empty() && !b.0.is_empty()
    });
    
    assert!(ok, "Timed out waiting for both event types within timeout");
    
    let a = reader.world().resource::<CollectedA>();
    let b = reader.world().resource::<CollectedB>();
    assert_eq!(a.0.len(), 1, "Expected 1 TestEvent, got {}", a.0.len());
    assert_eq!(b.0.len(), 1, "Expected 1 UserLoginEvent, got {}", b.0.len());
}

#[test]
fn single_topic_multiple_types_interleaved_frames() {
    let (backend_w, _b1) = setup();
    let (backend_r, _b2) = setup();
    let topic = unique_topic("mixed2");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(
        backend_w,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    writer.add_bus_event::<TestEvent>(&topic);
    writer.add_bus_event::<UserLoginEvent>(&topic);
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(
        backend_r,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    reader.add_bus_event::<TestEvent>(&topic);
    reader.add_bus_event::<UserLoginEvent>(&topic);

    #[derive(Resource, Default)]
    struct CollectedA(Vec<TestEvent>);
    #[derive(Resource, Default)]
    struct CollectedB(Vec<UserLoginEvent>);
    reader.insert_resource(CollectedA::default());
    reader.insert_resource(CollectedB::default());
    let tr_a2 = topic.clone();
    reader.add_systems(
        Update,
        move |mut r1: EventBusReader<TestEvent>, mut a: ResMut<CollectedA>| {
            for ev in r1.read(&tr_a2) {
                a.0.push(ev.clone());
            }
        },
    );
    let tr_b2 = topic.clone();
    reader.add_systems(
        Update,
        move |mut r2: EventBusReader<UserLoginEvent>, mut b: ResMut<CollectedB>| {
            for ev in r2.read(&tr_b2) {
                b.0.push(ev.clone());
            }
        },
    );

    // Ensure topic is ready before proceeding
    let topic_ready = crate::common::setup::ensure_topic_ready(
        &_b2, 
        &topic, 
        1, // partitions
        std::time::Duration::from_secs(5)
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);

    // Now set up and run the writer
    #[derive(Resource)]
    struct Counter(u32);
    writer.insert_resource(Counter(0));
    let tclone = topic.clone();
    writer.add_systems(
        Update,
        move |mut w1: EventBusWriter<TestEvent>,
              mut w2: EventBusWriter<UserLoginEvent>,
              mut c: ResMut<Counter>| {
            if c.0 % 2 == 0 {
                let _ = w1.write(
                    &tclone,
                    TestEvent {
                        message: format!("m{}", c.0),
                        value: c.0 as i32,
                    },
                );
            } else {
                let _ = w2.write(
                    &tclone,
                    UserLoginEvent {
                        user_id: format!("u{}", c.0),
                        timestamp: c.0 as u64,
                    },
                );
            }
            c.0 += 1;
        },
    );
    for _ in 0..6 {
        writer.update();
    }

    // Wait for background delivery using proper polling
    let (ok, _frames) = update_until(&mut reader, 12000, |app| {
        let a = app.world().resource::<CollectedA>();
        let b = app.world().resource::<CollectedB>();
        a.0.len() >= 3 && b.0.len() >= 3
    });
    
    assert!(ok, "Timed out waiting for interleaved events within timeout");
    let a = reader.world().resource::<CollectedA>();
    let b = reader.world().resource::<CollectedB>();
    assert!(a.0.len() >= 3);
    assert!(b.0.len() >= 3);
}
