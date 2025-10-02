use bevy::prelude::*;
use bevy_event_bus::config::kafka::{KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaTopicSpec};
use bevy_event_bus::{EventBusPlugins, KafkaEventReader, KafkaEventWriter};
use integration_tests::common::events::{TestEvent, UserLoginEvent};
use integration_tests::common::helpers::{
    DEFAULT_KAFKA_BOOTSTRAP, kafka_consumer_config, kafka_producer_config, unique_consumer_group,
    unique_topic, update_until,
};
use integration_tests::common::setup::setup;

#[test]
fn single_topic_multiple_types_same_frame() {
    let topic = unique_topic("mixed");
    let consumer_group = unique_consumer_group("multi_type_same_frame");

    let topic_for_writer = topic.clone();
    let (backend_w, _b1) = setup("earliest", move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_for_writer.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_event::<TestEvent>([topic_for_writer.clone()])
            .add_event::<UserLoginEvent>([topic_for_writer.clone()]);
    });

    let topic_for_reader = topic.clone();
    let group_for_reader = consumer_group.clone();
    let (backend_r, _b2) = setup("earliest", move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(topic_for_reader.clone())
                .partitions(1)
                .replication(1),
        );
        builder
            .add_consumer_group(
                group_for_reader.clone(),
                KafkaConsumerGroupSpec::new([topic_for_reader.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            )
            .add_event_single::<TestEvent>(topic_for_reader.clone())
            .add_event_single::<UserLoginEvent>(topic_for_reader.clone());
    });

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_w));
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_r));

    #[derive(Resource, Default)]
    struct CollectedA(Vec<TestEvent>);
    #[derive(Resource, Default)]
    struct CollectedB(Vec<UserLoginEvent>);
    reader.insert_resource(CollectedA::default());
    reader.insert_resource(CollectedB::default());
    let tr_a = topic.clone();
    let consumer_group_for_test = consumer_group.clone();
    let consumer_group_for_login = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r1: KafkaEventReader<TestEvent>, mut a: ResMut<CollectedA>| {
            for wrapper in r1.read(&kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group_for_test.as_str(),
                [&tr_a],
            )) {
                a.0.push(wrapper.event().clone());
            }
        },
    );
    let tr_b = topic.clone();
    reader.add_systems(
        Update,
        move |mut r2: KafkaEventReader<UserLoginEvent>, mut b: ResMut<CollectedB>| {
            for wrapper in r2.read(&kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group_for_login.as_str(),
                [&tr_b],
            )) {
                b.0.push(wrapper.event().clone());
            }
        },
    );

    // Now set up and run the writer to send events
    let tclone = topic.clone();
    writer.add_systems(
        Update,
        move |mut w1: KafkaEventWriter, mut w2: KafkaEventWriter, mut started: Local<bool>| {
            if !*started {
                *started = true;
                return;
            }
            let _ = w1.write(
                &kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&tclone]),
                TestEvent {
                    message: "hello".into(),
                    value: 42,
                },
            );
            let _ = w2.write(
                &kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&tclone]),
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
    let topic = unique_topic("mixed2");
    let consumer_group2 = unique_consumer_group("multi_type_interleaved");

    let topic_for_writer = topic.clone();
    let (backend_w, _b1) = setup("earliest", move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_for_writer.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_event::<TestEvent>([topic_for_writer.clone()])
            .add_event::<UserLoginEvent>([topic_for_writer.clone()]);
    });

    let topic_for_reader = topic.clone();
    let group_for_reader = consumer_group2.clone();
    let (backend_r, _b2) = setup("earliest", move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(topic_for_reader.clone())
                .partitions(1)
                .replication(1),
        );
        builder
            .add_consumer_group(
                group_for_reader.clone(),
                KafkaConsumerGroupSpec::new([topic_for_reader.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            )
            .add_event_single::<TestEvent>(topic_for_reader.clone())
            .add_event_single::<UserLoginEvent>(topic_for_reader.clone());
    });

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_w));
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_r));

    #[derive(Resource, Default)]
    struct CollectedA(Vec<TestEvent>);
    #[derive(Resource, Default)]
    struct CollectedB(Vec<UserLoginEvent>);
    reader.insert_resource(CollectedA::default());
    reader.insert_resource(CollectedB::default());
    let tr_a2 = topic.clone();
    let consumer_group2_for_test = consumer_group2.clone();
    let consumer_group2_for_login = consumer_group2.clone();
    reader.add_systems(
        Update,
        move |mut r1: KafkaEventReader<TestEvent>, mut a: ResMut<CollectedA>| {
            for wrapper in r1.read(&kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group2_for_test.as_str(),
                [&tr_a2],
            )) {
                a.0.push(wrapper.event().clone());
            }
        },
    );
    let tr_b2 = topic.clone();
    reader.add_systems(
        Update,
        move |mut r2: KafkaEventReader<UserLoginEvent>, mut b: ResMut<CollectedB>| {
            for wrapper in r2.read(&kafka_consumer_config(
                DEFAULT_KAFKA_BOOTSTRAP,
                consumer_group2_for_login.as_str(),
                [&tr_b2],
            )) {
                b.0.push(wrapper.event().clone());
            }
        },
    );

    // Now set up and run the writer
    #[derive(Resource)]
    struct Counter(u32);
    writer.insert_resource(Counter(0));
    let tclone = topic.clone();
    writer.add_systems(
        Update,
        move |mut w1: KafkaEventWriter, mut w2: KafkaEventWriter, mut c: ResMut<Counter>| {
            if c.0 % 2 == 0 {
                let _ = w1.write(
                    &kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&tclone]),
                    TestEvent {
                        message: format!("m{}", c.0),
                        value: c.0 as i32,
                    },
                );
            } else {
                let _ = w2.write(
                    &kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&tclone]),
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

    assert!(
        ok,
        "Timed out waiting for interleaved events within timeout"
    );
    let a = reader.world().resource::<CollectedA>();
    let b = reader.world().resource::<CollectedB>();
    assert!(a.0.len() >= 3);
    assert!(b.0.len() >= 3);
}
