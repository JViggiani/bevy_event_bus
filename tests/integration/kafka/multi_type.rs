use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{EventBusPlugins, KafkaEventReader, KafkaEventWriter};
use integration_tests::utils::events::{TestEvent, UserLoginEvent};
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic, update_until};
use integration_tests::utils::kafka_setup;

#[test]
fn single_topic_multiple_types_same_frame() {
    let topic = unique_topic("mixed");
    let consumer_group = unique_consumer_group("multi_type_same_frame");

    let topic_for_writer = topic.clone();
    let (backend_w, _b1) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_for_writer.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_event::<TestEvent>([topic_for_writer.clone()])
            .add_event::<UserLoginEvent>([topic_for_writer.clone()]);
    }));

    let topic_for_reader = topic.clone();
    let group_for_reader = consumer_group.clone();
    let (backend_r, _b2) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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
    }));

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_w));
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_r));

    #[derive(Resource, Default)]
    struct CollectedTests(Vec<TestEvent>);
    #[derive(Resource, Default)]
    struct CollectedLogins(Vec<UserLoginEvent>);
    reader.insert_resource(CollectedTests::default());
    reader.insert_resource(CollectedLogins::default());
    let tr_a = topic.clone();
    let consumer_group_for_test = consumer_group.clone();
    let consumer_group_for_login = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut reader: KafkaEventReader<TestEvent>, mut collected: ResMut<CollectedTests>| {
            let config = KafkaConsumerConfig::new(consumer_group_for_test.as_str(), [&tr_a]);
            for wrapper in reader.read(&config) {
                collected.0.push(wrapper.event().clone());
            }
        },
    );
    let tr_b = topic.clone();
    reader.add_systems(
        Update,
        move |mut reader: KafkaEventReader<UserLoginEvent>,
              mut collected: ResMut<CollectedLogins>| {
            let config = KafkaConsumerConfig::new(consumer_group_for_login.as_str(), [&tr_b]);
            for wrapper in reader.read(&config) {
                collected.0.push(wrapper.event().clone());
            }
        },
    );

    // Now set up and run the writer to send events
    let tclone = topic.clone();
    writer.add_systems(
        Update,
        move |mut writer: KafkaEventWriter, mut started: Local<bool>| {
            if !*started {
                *started = true;
                return;
            }
            let config = KafkaProducerConfig::new([tclone.clone()]);
            writer.write(
                &config,
                TestEvent {
                    message: "hello".into(),
                    value: 42,
                },
            );
            writer.write(
                &config,
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
        let a = app.world().resource::<CollectedTests>();
        let b = app.world().resource::<CollectedLogins>();
        !a.0.is_empty() && !b.0.is_empty()
    });

    assert!(ok, "Timed out waiting for both event types within timeout");

    let a = reader.world().resource::<CollectedTests>();
    let b = reader.world().resource::<CollectedLogins>();
    assert_eq!(a.0.len(), 1, "Expected 1 TestEvent, got {}", a.0.len());
    assert_eq!(b.0.len(), 1, "Expected 1 UserLoginEvent, got {}", b.0.len());
}

#[test]
fn single_topic_multiple_types_interleaved_frames() {
    let topic = unique_topic("mixed2");
    let consumer_group2 = unique_consumer_group("multi_type_interleaved");

    let topic_for_writer = topic.clone();
    let (backend_w, _b1) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_for_writer.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_event::<TestEvent>([topic_for_writer.clone()])
            .add_event::<UserLoginEvent>([topic_for_writer.clone()]);
    }));

    let topic_for_reader = topic.clone();
    let group_for_reader = consumer_group2.clone();
    let (backend_r, _b2) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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
    }));

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_w));
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_r));

    #[derive(Resource, Default)]
    struct CollectedTests(Vec<TestEvent>);
    #[derive(Resource, Default)]
    struct CollectedLogins(Vec<UserLoginEvent>);
    reader.insert_resource(CollectedTests::default());
    reader.insert_resource(CollectedLogins::default());
    let tr_a2 = topic.clone();
    let consumer_group2_for_test = consumer_group2.clone();
    let consumer_group2_for_login = consumer_group2.clone();
    reader.add_systems(
        Update,
        move |mut reader: KafkaEventReader<TestEvent>, mut collected: ResMut<CollectedTests>| {
            let config = KafkaConsumerConfig::new(consumer_group2_for_test.as_str(), [&tr_a2]);
            for wrapper in reader.read(&config) {
                collected.0.push(wrapper.event().clone());
            }
        },
    );
    let tr_b2 = topic.clone();
    reader.add_systems(
        Update,
        move |mut reader: KafkaEventReader<UserLoginEvent>,
              mut collected: ResMut<CollectedLogins>| {
            let config = KafkaConsumerConfig::new(consumer_group2_for_login.as_str(), [&tr_b2]);
            for wrapper in reader.read(&config) {
                collected.0.push(wrapper.event().clone());
            }
        },
    );

    // Now set up and run the writer
    let tclone = topic.clone();
    writer.add_systems(
        Update,
        move |mut writer: KafkaEventWriter, mut counter: Local<u32>| {
            let config = KafkaProducerConfig::new([tclone.clone()]);
            match *counter % 2 {
                0 => {
                    writer.write(
                        &config,
                        TestEvent {
                            message: format!("m{}", *counter),
                            value: *counter as i32,
                        },
                    );
                }
                _ => {
                    writer.write(
                        &config,
                        UserLoginEvent {
                            user_id: format!("u{}", *counter),
                            timestamp: *counter as u64,
                        },
                    );
                }
            }
            *counter += 1;
        },
    );
    for _ in 0..6 {
        writer.update();
    }

    // Wait for background delivery using proper polling
    let (ok, _frames) = update_until(&mut reader, 12000, |app| {
        let a = app.world().resource::<CollectedTests>();
        let b = app.world().resource::<CollectedLogins>();
        a.0.len() >= 3 && b.0.len() >= 3
    });

    assert!(
        ok,
        "Timed out waiting for interleaved events within timeout"
    );
    let a = reader.world().resource::<CollectedTests>();
    let b = reader.world().resource::<CollectedLogins>();
    assert!(a.0.len() >= 3);
    assert!(b.0.len() >= 3);
}
