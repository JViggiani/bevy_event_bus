use crate::common::events::TestEvent;
use crate::common::helpers::{unique_string, wait_for_events, unique_consumer_group};
use crate::common::setup::build_app;
use bevy::prelude::*;
use bevy_event_bus::{EventBusReader, EventBusWriter, EventBusAppExt, KafkaReadConfig, KafkaWriteConfig};

#[test]
fn per_topic_order_preserved() {
    let (backend_w, _b1) = crate::common::setup::setup(None);
    let (_backend_r, b2) = crate::common::setup::setup(None);
    let topic = unique_string("ordered");
    bevy_event_bus::runtime();
    let topic_ready = crate::common::setup::ensure_topic_ready(&b2, &topic, 1, std::time::Duration::from_secs(5));
    assert!(topic_ready, "Topic {} not ready within timeout", topic);
    let mut writer = crate::common::setup::build_app(backend_w, None, |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });
    
    // Create consumer configuration
    let consumer_group = unique_string("test_group");
    let config = KafkaReadConfig::new(&consumer_group).topics([&topic]);
    
    // Build reader app with consumer group created during setup
    let (backend_r, _bootstrap_r) = crate::common::setup::setup(None);
    let mut reader = build_app(backend_r, Some(&[config.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });
    
    let tclone = topic.clone();
    writer.add_systems(
        Update,
        move |mut w: EventBusWriter<TestEvent>, mut started: Local<bool>| {
            if !*started {
                *started = true;
                return;
            }
            for i in 0..10 {
                let _ = w.write(
                    &KafkaWriteConfig::new(&tclone),
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
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut c: ResMut<Collected>| {
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
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
    let (backend_w, _b1) = crate::common::setup::setup(None);
    let (_backend_r, b2) = crate::common::setup::setup(None);
    let t1 = unique_string("t1");
    let t2 = unique_string("t2");
    bevy_event_bus::runtime();
    let t1_ready = crate::common::setup::ensure_topic_ready(&b2, &t1, 1, std::time::Duration::from_secs(5));
    let t2_ready = crate::common::setup::ensure_topic_ready(&b2, &t2, 1, std::time::Duration::from_secs(5));
    assert!(t1_ready && t2_ready, "Topics not ready within timeout");
    let mut writer = crate::common::setup::build_app(backend_w, None, |app| {
        app.add_bus_event_topics::<TestEvent>(&[&t1, &t2]);
    });
    
    // Create consumer configurations for both topics
    let config1 = KafkaReadConfig::new(&unique_consumer_group("test_group_1")).topics([&t1]);
    let config2 = KafkaReadConfig::new(&unique_consumer_group("test_group_2")).topics([&t2]);
    
    // Build reader app with consumer groups created during setup
    let (backend_r, _bootstrap_r) = crate::common::setup::setup(None);
    let mut reader = build_app(backend_r, Some(&[config1.clone(), config2.clone()]), |app| {
        app.add_bus_event_topics::<TestEvent>(&[&t1, &t2]);
    });
    
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
                let _ = w.write(
                    &KafkaWriteConfig::new(&t1c),
                    TestEvent {
                        message: format!("A{i}"),
                        value: i,
                    },
                );
                let _ = w.write(
                    &KafkaWriteConfig::new(&t2c),
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
    
    // Simple system that just reads - no consumer group creation needed
    reader.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>,
              mut c1: ResMut<CollectedT1>,
              mut c2: ResMut<CollectedT2>| {
            for wrapper in r.read(&config1) {
                c1.0.push(wrapper.event().clone());
            }
            for wrapper in r.read(&config2) {
                c2.0.push(wrapper.event().clone());
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
