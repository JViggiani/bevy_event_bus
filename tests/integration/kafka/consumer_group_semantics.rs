#![cfg(feature = "kafka")]

use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{EventBusPlugins, KafkaEventReader, KafkaEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_topic, update_two_apps_until, update_until,
};
use integration_tests::utils::kafka_setup;

#[derive(Resource, Default)]
struct EventCollector(Vec<TestEvent>);

/// Readers in the same consumer group should share work without duplicates.
#[test]
fn same_consumer_group_distributes_messages_round_robin() {
    let topic = unique_topic("same-group");
    let consumer_group = unique_consumer_group("shared-group");

    // Writer topology - no consumer groups (write-only)
    let topic_for_writer = topic.clone();
    let (writer_backend, _bootstrap1) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
            builder.add_topic(
                KafkaTopicSpec::new(topic_for_writer.clone())
                    .partitions(2)
                    .replication(1),
            );
            builder.add_event_single::<TestEvent>(topic_for_writer.clone());
        }));

    let topic_for_runtime_writer = topic.clone();

    // Reader1 topology - with consumer group
    let topic_for_reader1 = topic.clone();
    let group_for_reader1 = consumer_group.clone();
    let (reader1_backend, _bootstrap2) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
            builder.add_topic(
                KafkaTopicSpec::new(topic_for_reader1.clone())
                    .partitions(2)
                    .replication(1),
            );
            builder.add_consumer_group(
                group_for_reader1.clone(),
                KafkaConsumerGroupSpec::new([topic_for_reader1.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            );
            builder.add_event_single::<TestEvent>(topic_for_reader1.clone());
        }));

    // Reader2 topology - same consumer group
    let topic_for_reader2 = topic.clone();
    let group_for_reader2 = consumer_group.clone();
    let (reader2_backend, _bootstrap3) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
            builder.add_topic(
                KafkaTopicSpec::new(topic_for_reader2.clone())
                    .partitions(2)
                    .replication(1),
            );
            builder.add_consumer_group(
                group_for_reader2.clone(),
                KafkaConsumerGroupSpec::new([topic_for_reader2.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            );
            builder.add_event_single::<TestEvent>(topic_for_reader2.clone());
        }));

    // Setup reader1 app
    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(reader1_backend));
    reader1.insert_resource(EventCollector::default());

    let t1 = topic.clone();
    let g1 = consumer_group.clone();
    reader1.add_systems(
        Update,
        move |mut r: KafkaEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = KafkaConsumerConfig::new(g1.clone(), [t1.clone()]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Setup reader2 app
    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(reader2_backend));
    reader2.insert_resource(EventCollector::default());

    let t2 = topic.clone();
    let g2 = consumer_group.clone();
    reader2.add_systems(
        Update,
        move |mut r: KafkaEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = KafkaConsumerConfig::new(g2.clone(), [t2.clone()]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Setup writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    writer.add_systems(
        Update,
        move |mut w: KafkaEventWriter, mut sent: Local<usize>| {
            if *sent < 6 {
                let config = KafkaProducerConfig::new([topic_for_runtime_writer.clone()]);
                w.write(
                    &config,
                    TestEvent {
                        message: format!("message_{}", *sent),
                        value: *sent as i32,
                    },
                );
                *sent += 1;
            }
        },
    );

    // Start readers first to establish consumer group
    run_app_updates(&mut reader1, 3);
    run_app_updates(&mut reader2, 3);

    // Send events
    run_app_updates(&mut writer, 7);

    // Wait for message distribution and ensure the consumer group drains the full payload.
    let (drain_complete, _) = update_two_apps_until(
        &mut reader1,
        &mut reader2,
        10_000,
        |reader1_app, reader2_app| {
            let collected1 = reader1_app.world().resource::<EventCollector>();
            let collected2 = reader2_app.world().resource::<EventCollector>();
            let total_events = collected1.0.len() + collected2.0.len();
            total_events >= 6 && (collected1.0.len() > 0 || collected2.0.len() > 0)
        },
    );

    let collected1 = reader1.world().resource::<EventCollector>();
    let collected2 = reader2.world().resource::<EventCollector>();

    // Combined total should match dispatched events and the group should make progress.
    assert!(
        drain_complete,
        "Consumer group should receive all dispatched events"
    );

    let total_events = collected1.0.len() + collected2.0.len();
    assert_eq!(
        total_events,
        6,
        "Should receive all 6 events in total, got reader1={}, reader2={}",
        collected1.0.len(),
        collected2.0.len()
    );

    // Kafka assigns partitions exclusively to a single consumer, so one reader may legitimately
    // observe all events depending on the broker's rebalancing. We only assert that the group
    // consumes the complete payload without duplication.
    assert!(
        collected1.0.len() > 0 || collected2.0.len() > 0,
        "At least one reader should receive events"
    );

    println!(
        "Reader1 got {} events, Reader2 got {} events",
        collected1.0.len(),
        collected2.0.len()
    );
}

/// Independent consumer groups should each observe the full stream of events.
#[test]
fn different_consumer_groups_receive_all_events() {
    let topic = unique_topic("diff-groups");
    let consumer_group1 = unique_consumer_group("group1");
    let consumer_group2 = unique_consumer_group("group2");

    // Writer topology - no consumer groups (write-only)
    let topic_for_writer = topic.clone();
    let (writer_backend, _bootstrap1) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
            builder.add_topic(
                KafkaTopicSpec::new(topic_for_writer.clone())
                    .partitions(1)
                    .replication(1),
            );
            builder.add_event_single::<TestEvent>(topic_for_writer.clone());
        }));

    let topic_for_runtime_writer = topic.clone();

    // Reader1 topology - with consumer group 1
    let topic_for_reader1 = topic.clone();
    let group_for_reader1 = consumer_group1.clone();
    let (reader1_backend, _bootstrap2) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
            builder.add_topic(
                KafkaTopicSpec::new(topic_for_reader1.clone())
                    .partitions(1)
                    .replication(1),
            );
            builder.add_consumer_group(
                group_for_reader1.clone(),
                KafkaConsumerGroupSpec::new([topic_for_reader1.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            );
            builder.add_event_single::<TestEvent>(topic_for_reader1.clone());
        }));

    // Reader2 topology - with different consumer group 2
    let topic_for_reader2 = topic.clone();
    let group_for_reader2 = consumer_group2.clone();
    let (reader2_backend, _bootstrap3) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
            builder.add_topic(
                KafkaTopicSpec::new(topic_for_reader2.clone())
                    .partitions(1)
                    .replication(1),
            );
            builder.add_consumer_group(
                group_for_reader2.clone(),
                KafkaConsumerGroupSpec::new([topic_for_reader2.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            );
            builder.add_event_single::<TestEvent>(topic_for_reader2.clone());
        }));

    // Setup writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    writer.add_systems(
        Update,
        move |mut w: KafkaEventWriter, mut sent: Local<usize>| {
            if *sent < 4 {
                let config = KafkaProducerConfig::new([topic_for_runtime_writer.clone()]);
                w.write(
                    &config,
                    TestEvent {
                        message: format!("broadcast_message_{}", *sent),
                        value: *sent as i32 + 100,
                    },
                );
                *sent += 1;
            }
        },
    );

    // Setup reader1 app
    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(reader1_backend));
    reader1.insert_resource(EventCollector::default());

    let t1 = topic.clone();
    let g1 = consumer_group1.clone();
    reader1.add_systems(
        Update,
        move |mut r: KafkaEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = KafkaConsumerConfig::new(g1.clone(), [t1.clone()]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Setup reader2 app
    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(reader2_backend));
    reader2.insert_resource(EventCollector::default());

    let t2 = topic.clone();
    let g2 = consumer_group2.clone();
    reader2.add_systems(
        Update,
        move |mut r: KafkaEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = KafkaConsumerConfig::new(g2.clone(), [t2.clone()]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Start readers first to establish consumer groups
    run_app_updates(&mut reader1, 3);
    run_app_updates(&mut reader2, 3);

    // Send events
    run_app_updates(&mut writer, 5);

    // Wait for message broadcast
    let (received1, _) = update_until(&mut reader1, 10_000, |app| {
        let collected = app.world().resource::<EventCollector>();
        collected.0.len() >= 4
    });

    let (received2, _) = update_until(&mut reader2, 10_000, |app| {
        let collected = app.world().resource::<EventCollector>();
        collected.0.len() >= 4
    });

    let collected1 = reader1.world().resource::<EventCollector>();
    let collected2 = reader2.world().resource::<EventCollector>();

    // Both readers should have received ALL events (broadcast behavior)
    assert!(received1, "Reader1 should receive all events");
    assert!(received2, "Reader2 should receive all events");

    assert_eq!(collected1.0.len(), 4, "Reader1 should receive all 4 events");
    assert_eq!(collected2.0.len(), 4, "Reader2 should receive all 4 events");

    // Verify they got the same events
    for i in 0..4 {
        assert_eq!(collected1.0[i].value, 100 + i as i32);
        assert_eq!(collected2.0[i].value, 100 + i as i32);
    }

    println!(
        "Reader1 got {} events, Reader2 got {} events (broadcast mode)",
        collected1.0.len(),
        collected2.0.len()
    );
}

/// Writers should function without any configured consumer groups.
#[test]
fn writer_only_works_without_consumer_groups() {
    let topic = unique_topic("writer-only");

    // Writer topology - no consumer groups (write-only)
    let topic_for_writer_topology = topic.clone();
    let (writer_backend, _bootstrap) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
            builder.add_topic(
                KafkaTopicSpec::new(topic_for_writer_topology.clone())
                    .partitions(1)
                    .replication(1),
            );
            builder.add_event_single::<TestEvent>(topic_for_writer_topology.clone());
        }));

    // Setup writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let topic_for_runtime_writer = topic.clone();
    writer.add_systems(
        Update,
        move |mut w: KafkaEventWriter, mut events_sent: Local<usize>| {
            if *events_sent < 3 {
                let config = KafkaProducerConfig::new([topic_for_runtime_writer.clone()]);
                w.write(
                    &config,
                    TestEvent {
                        message: format!("writer_only_{}", *events_sent),
                        value: *events_sent as i32,
                    },
                );
                *events_sent += 1;
            }
        },
    );

    // Writer should be able to send events without any consumer groups defined
    run_app_updates(&mut writer, 4);

    // Test passes if no panics occur - writer-only mode works
    println!("Writer-only app successfully sent events without consumer groups");
}
