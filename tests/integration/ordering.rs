use crate::common::events::TestEvent;
use crate::common::helpers::unique_topic;
use crate::common::helpers::wait_for_events;
use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusReader, EventBusWriter};

#[test]
fn per_topic_order_preserved() {
    let (backend_w, _b1) = crate::common::setup::setup();
    let (backend_r, _b2) = crate::common::setup::setup();
    let topic = unique_topic("ordered");
    bevy_event_bus::runtime();
    crate::common::setup::ensure_topic(&_b2, &topic, 1);
    let mut writer = App::new();
    let mut reader = App::new();
    writer.add_plugins(EventBusPlugins(
        backend_w,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    reader.add_plugins(EventBusPlugins(
        backend_r,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    let tclone = topic.clone();
    writer.add_systems(
        Update,
        move |mut w: EventBusWriter<TestEvent>, mut started: Local<bool>| {
            if !*started {
                *started = true;
                return;
            }
            for i in 0..10 {
                let _ = w.send(
                    &tclone,
                    TestEvent {
                        message: format!("msg-{i}"),
                        value: i,
                    },
                );
            }
        },
    );
    writer.update(); // warm frame triggers started=true
    writer.update(); // send frame (user systems run then Last stage flushes)

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());
    let tr = topic.clone();
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut c: ResMut<Collected>| {
            for ev in r.try_read(&tr) {
                c.0.push(ev.clone());
            }
        },
    );

    // Eventual consistency loop: allow background producer task to deliver.
    let _ = wait_for_events(&mut reader, &topic, 12_000, 10, |app| {
        let c = app.world().resource::<Collected>();
        c.0.clone()
    });
    let c = reader.world().resource::<Collected>(); // still borrow for assertions
    let mut last = -1;
    for ev in &c.0 {
        assert!(ev.value > last);
        last = ev.value;
    }
    assert_eq!(c.0.len(), 10);
}

#[test]
fn cross_topic_interleave_each_ordered() {
    let (backend_w, _b1) = crate::common::setup::setup();
    let (backend_r, _b2) = crate::common::setup::setup();
    let t1 = unique_topic("t1");
    let t2 = unique_topic("t2");
    bevy_event_bus::runtime();
    crate::common::setup::ensure_topic(&_b2, &t1, 1);
    crate::common::setup::ensure_topic(&_b2, &t2, 1);
    let mut writer = App::new();
    let mut reader = App::new();
    writer.add_plugins(EventBusPlugins(
        backend_w,
        bevy_event_bus::PreconfiguredTopics::new([t1.clone(), t2.clone()]),
    ));
    reader.add_plugins(EventBusPlugins(
        backend_r,
        bevy_event_bus::PreconfiguredTopics::new([t1.clone(), t2.clone()]),
    ));
    let t1c = t1.clone();
    let t2c = t2.clone();
    // Single send frame like earlier test
    writer.add_systems(
        Update,
        move |mut w: EventBusWriter<TestEvent>, mut started: Local<bool>| {
            if !*started {
                *started = true;
                return;
            }
            for i in 0..5 {
                let _ = w.send(
                    &t1c,
                    TestEvent {
                        message: format!("A{i}"),
                        value: i,
                    },
                );
                let _ = w.send(
                    &t2c,
                    TestEvent {
                        message: format!("B{i}"),
                        value: i,
                    },
                );
            }
        },
    );
    writer.update(); // warm
    writer.update(); // send

    #[derive(Resource, Default)]
    struct CollectedT1(Vec<TestEvent>);
    #[derive(Resource, Default)]
    struct CollectedT2(Vec<TestEvent>);
    reader.insert_resource(CollectedT1::default());
    reader.insert_resource(CollectedT2::default());
    let ta = t1.clone();
    let tb = t2.clone();
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>,
              mut c1: ResMut<CollectedT1>,
              mut c2: ResMut<CollectedT2>| {
            for ev in r.try_read(&ta) {
                c1.0.push(ev.clone());
            }
            for ev in r.try_read(&tb) {
                c2.0.push(ev.clone());
            }
        },
    );

    // Wait for exact counts (no duplicates expected). If duplicates appear it's a bug.
    let a_events = wait_for_events(&mut reader, &t1, 15_000, 5, |app| {
        let c1 = app.world().resource::<CollectedT1>();
        c1.0.clone()
    });
    // Give one extra update tick after first topic completes to reduce chance of asymmetric arrival
    reader.update();
    let b_events = wait_for_events(&mut reader, &t2, 15_000, 5, |app| {
        let c2 = app.world().resource::<CollectedT2>();
        c2.0.clone()
    });
    assert_eq!(
        a_events.len(),
        5,
        "Topic {} expected 5 events got {}",
        t1,
        a_events.len()
    );
    assert_eq!(
        b_events.len(),
        5,
        "Topic {} expected 5 events got {}",
        t2,
        b_events.len()
    );
    for (i, ev) in a_events.iter().enumerate() {
        assert_eq!(ev.value, i as i32);
        assert!(ev.message.starts_with('A'));
    }
    for (i, ev) in b_events.iter().enumerate() {
        assert_eq!(ev.value, i as i32);
        assert!(ev.message.starts_with('B'));
    }
}
