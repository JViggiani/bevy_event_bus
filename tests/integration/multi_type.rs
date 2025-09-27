use crate::common::events::{TestEvent, UserLoginEvent};
use crate::common::helpers::{unique_topic, unique_consumer_group, update_until};
use crate::common::setup::{setup, build_app};
use bevy::prelude::*;
use bevy_event_bus::{EventBusReader, EventBusWriter, EventBusAppExt, KafkaReadConfig, KafkaWriteConfig};

#[test]
fn single_topic_multiple_types_same_frame() {
    let (backend_w, _b1) = setup(None);
    let (_backend_r, _b2) = setup(None);
    let topic = unique_topic("mixed");

    let mut writer = crate::common::setup::build_app(backend_w, None, |app| {
        app.add_bus_event::<TestEvent>(&topic);
        app.add_bus_event::<UserLoginEvent>(&topic);
    });

    // Create consumer configurations for both consumer groups
    let consumer_group_a = unique_consumer_group("test_group_a");
    let consumer_group_b = unique_consumer_group("test_group_b");
    let config_a = KafkaReadConfig::new(&consumer_group_a).topics([&topic]);
    let config_b = KafkaReadConfig::new(&consumer_group_b).topics([&topic]);
    
    // Build reader app with consumer groups created during setup
    let (backend_r, _bootstrap_r) = crate::common::setup::setup(None);
    let mut reader = build_app(backend_r, Some(&[config_a.clone(), config_b.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic);
        app.add_bus_event::<UserLoginEvent>(&topic);
    });

    #[derive(Resource, Default)]
    struct CollectedA(Vec<TestEvent>);
    #[derive(Resource, Default)]
    struct CollectedB(Vec<UserLoginEvent>);
    reader.insert_resource(CollectedA::default());
    reader.insert_resource(CollectedB::default());
    
    reader.add_systems(
        Update,
        move |mut r1: EventBusReader<TestEvent>, mut a: ResMut<CollectedA>| {
            for wrapper in r1.read(&config_a) {
                a.0.push(wrapper.event().clone());
            }
        },
    );
    reader.add_systems(
        Update,
        move |mut r2: EventBusReader<UserLoginEvent>, mut b: ResMut<CollectedB>| {
            for wrapper in r2.read(&config_b) {
                b.0.push(wrapper.event().clone());
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
                &KafkaWriteConfig::new(&tclone),
                TestEvent {
                    message: "hello".into(),
                    value: 42,
                },
            );
            let _ = w2.write(
                &KafkaWriteConfig::new(&tclone),
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
    let (backend_w, _b1) = setup(None);
    let (_backend_r, _b2) = setup(None);
    let topic = unique_topic("mixed2");

    let mut writer = crate::common::setup::build_app(backend_w, None, |app| {
        app.add_bus_event::<TestEvent>(&topic);
        app.add_bus_event::<UserLoginEvent>(&topic);
    });

    // Create consumer configurations for both consumer groups  
    let consumer_group_a2 = unique_consumer_group("test_group_a2");
    let consumer_group_b2 = unique_consumer_group("test_group_b2");
    let config_a = KafkaReadConfig::new(&consumer_group_a2).topics([&topic]);
    let config_b = KafkaReadConfig::new(&consumer_group_b2).topics([&topic]);
    
    // Build reader app with consumer groups created during setup
    let (backend_r, _bootstrap_r) = crate::common::setup::setup(None);
    let mut reader = build_app(backend_r, Some(&[config_a.clone(), config_b.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic);
        app.add_bus_event::<UserLoginEvent>(&topic);
    });

    #[derive(Resource, Default)]
    struct CollectedA(Vec<TestEvent>);
    #[derive(Resource, Default)]
    struct CollectedB(Vec<UserLoginEvent>);
    reader.insert_resource(CollectedA::default());
    reader.insert_resource(CollectedB::default());
    
    // Simple systems that just read - no consumer group creation needed
    reader.add_systems(
        Update,
        move |mut r1: EventBusReader<TestEvent>, mut a: ResMut<CollectedA>| {
            for wrapper in r1.read(&config_a) {
                a.0.push(wrapper.event().clone());
            }
        },
    );
    reader.add_systems(
        Update,
        move |mut r2: EventBusReader<UserLoginEvent>, mut b: ResMut<CollectedB>| {
            for wrapper in r2.read(&config_b) {
                b.0.push(wrapper.event().clone());
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
                    &KafkaWriteConfig::new(&tclone),
                    TestEvent {
                        message: format!("m{}", c.0),
                        value: c.0 as i32,
                    },
                );
            } else {
                let _ = w2.write(
                    &KafkaWriteConfig::new(&tclone),
                    UserLoginEvent {
                        user_id: format!("u{}", c.0),
                        timestamp: c.0 as i64,
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
