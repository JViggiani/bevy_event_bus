use crate::common::events::{TestEvent, UserLoginEvent};
use crate::common::helpers::{unique_topic, update_until};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusReader, EventBusWriter};

#[test]
fn single_topic_multiple_types_same_frame() {
    let (backend_w, _b1, _t1) = setup();
    let (backend_r, _b2, _t2) = setup();
    let topic = unique_topic("mixed");

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
            let _ = w1.send(
                &tclone,
                TestEvent {
                    message: "hello".into(),
                    value: 42,
                },
            );
            let _ = w2.send(
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
            for ev in r1.try_read(&tr_a) {
                a.0.push(ev.clone());
            }
        },
    );
    let tr_b = topic.clone();
    reader.add_systems(
        Update,
        move |mut r2: EventBusReader<UserLoginEvent>, mut b: ResMut<CollectedB>| {
            for ev in r2.try_read(&tr_b) {
                b.0.push(ev.clone());
            }
        },
    );

    let (ok, _frames) = update_until(&mut reader, 5000, |app| {
        let a = app.world().resource::<CollectedA>();
        let b = app.world().resource::<CollectedB>();
        a.0.len() >= 1 && b.0.len() >= 1
    });
    assert!(ok, "Timed out waiting for both event types");
    let a = reader.world().resource::<CollectedA>();
    let b = reader.world().resource::<CollectedB>();
    assert_eq!(a.0.len(), 1);
    assert_eq!(b.0.len(), 1);
}

#[test]
fn single_topic_multiple_types_interleaved_frames() {
    let (backend_w, _b1, _t1) = setup();
    let (backend_r, _b2, _t2) = setup();
    let topic = unique_topic("mixed2");

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
                let _ = w1.send(
                    &tclone,
                    TestEvent {
                        message: format!("m{}", c.0),
                        value: c.0 as i32,
                    },
                );
            } else {
                let _ = w2.send(
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
            for ev in r1.try_read(&tr_a2) {
                a.0.push(ev.clone());
            }
        },
    );
    let tr_b2 = topic.clone();
    reader.add_systems(
        Update,
        move |mut r2: EventBusReader<UserLoginEvent>, mut b: ResMut<CollectedB>| {
            for ev in r2.try_read(&tr_b2) {
                b.0.push(ev.clone());
            }
        },
    );
    // Eventual consistency loop for background delivery
    let start = std::time::Instant::now();
    let timeout = std::time::Duration::from_secs(12);
    loop {
        reader.update();
        std::thread::sleep(std::time::Duration::from_millis(25));
        let a = reader.world().resource::<CollectedA>();
        let b = reader.world().resource::<CollectedB>();
        if a.0.len() >= 3 && b.0.len() >= 3 {
            break;
        }
        if start.elapsed() > timeout {
            panic!("Timed out waiting for interleaved events");
        }
    }
    let a = reader.world().resource::<CollectedA>();
    let b = reader.world().resource::<CollectedB>();
    assert!(a.0.len() >= 3);
    assert!(b.0.len() >= 3);
}
