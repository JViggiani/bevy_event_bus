#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_consumer_group_member, unique_topic,
    update_two_apps_until, update_until,
};
use integration_tests::utils::redis_setup;

#[derive(Resource, Default)]
struct EventCollector(Vec<TestEvent>);

/// Readers in the same consumer group should share work without duplicates.
#[test]
fn same_consumer_group_distributes_messages_round_robin() {
    let stream = unique_topic("same-group");
    let consumer_group = unique_consumer_group("shared-group");

    // Generate unique consumer names for proper coordination
    let consumer1 = unique_consumer_group_member(&consumer_group);
    let consumer2 = unique_consumer_group_member(&consumer_group);

    // Writer topology - no consumer groups (write-only)
    let stream_for_writer_topology = stream.clone();
    let (writer_backend, _writer_ctx) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_writer_topology.clone()))
            .add_event_single::<TestEvent>(stream_for_writer_topology.clone());
    })
    .expect("Redis writer backend setup successful");

    // Reader1 topology - with consumer group
    let stream_for_reader1 = stream.clone();
    let group_for_reader1 = consumer_group.clone();
    let consumer_for_reader1 = consumer1.clone();
    let (reader1_backend, _reader1_ctx) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_reader1.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_reader1.clone()],
                group_for_reader1.clone(),
                consumer_for_reader1.clone(),
            ))
            .add_event_single::<TestEvent>(stream_for_reader1.clone());
    })
    .expect("Redis reader1 backend setup successful");

    // Reader2 topology - same consumer group
    let stream_for_reader2 = stream.clone();
    let group_for_reader2 = consumer_group.clone();
    let consumer_for_reader2 = consumer2.clone();
    let (reader2_backend, _reader2_ctx) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_reader2.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_reader2.clone()],
                group_for_reader2.clone(),
                consumer_for_reader2.clone(),
            ))
            .add_event_single::<TestEvent>(stream_for_reader2.clone());
    })
    .expect("Redis reader2 backend setup successful");

    // Setup reader1 app with coordinated consumer name
    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(reader1_backend));
    reader1.insert_resource(EventCollector::default());

    let s1 = stream.clone();
    let g1 = consumer_group.clone();
    let c1 = consumer1.clone();
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g1.clone(), c1.clone(), [s1.clone()]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Setup reader2 app with coordinated consumer name
    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(reader2_backend));
    reader2.insert_resource(EventCollector::default());

    let s2 = stream.clone();
    let g2 = consumer_group.clone();
    let c2 = consumer2.clone();
    reader2.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g2.clone(), c2.clone(), [s2.clone()]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Setup writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let stream_for_runtime_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<usize>| {
            if *sent < 6 {
                let config = RedisProducerConfig::new(stream_for_runtime_writer.clone());
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

    // Wait for both readers to receive their fair share and drain the work queue.
    let (drain_complete, _) = update_two_apps_until(
        &mut reader1,
        &mut reader2,
        10_000,
        |reader1_app, reader2_app| {
            let collected1 = reader1_app.world().resource::<EventCollector>();
            let collected2 = reader2_app.world().resource::<EventCollector>();
            let total_events = collected1.0.len() + collected2.0.len();
            total_events >= 6 && collected1.0.len() > 0 && collected2.0.len() > 0
        },
    );

    let collected1 = reader1.world().resource::<EventCollector>();
    let collected2 = reader2.world().resource::<EventCollector>();

    // Combined total should match dispatched events and both readers should contribute.
    assert!(
        drain_complete,
        "Both readers should receive all dispatched events"
    );

    let total_events = collected1.0.len() + collected2.0.len();
    assert_eq!(
        total_events, 6,
        "All dispatched events should be observed across the consumer group"
    );
    assert!(
        collected1.0.len() > 0,
        "Reader1 should receive at least one event via shared consumer group"
    );
    assert!(
        collected2.0.len() > 0,
        "Reader2 should receive at least one event via shared consumer group"
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
    let stream = unique_topic("broadcast-stream");
    let consumer_group1 = unique_consumer_group("group1");
    let consumer_group2 = unique_consumer_group("group2");
    let consumer_name1 = unique_consumer_group_member(&consumer_group1);
    let consumer_name2 = unique_consumer_group_member(&consumer_group2);

    // Writer topology - no consumer groups (write-only)
    let stream_for_writer_topology = stream.clone();
    let (writer_backend, _writer_ctx) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_writer_topology.clone()))
            .add_event_single::<TestEvent>(stream_for_writer_topology.clone());
    })
    .expect("Redis writer backend setup successful");

    // Reader1 topology - with consumer group 1
    let stream_for_reader1 = stream.clone();
    let group_for_reader1 = consumer_group1.clone();
    let consumer_for_reader1 = consumer_name1.clone();
    let (reader1_backend, _reader1_ctx) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_reader1.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_reader1.clone()],
                group_for_reader1.clone(),
                consumer_for_reader1.clone(),
            ))
            .add_event_single::<TestEvent>(stream_for_reader1.clone());
    })
    .expect("Reader1 Redis backend setup successful");

    // Reader2 topology - with consumer group 2
    let stream_for_reader2 = stream.clone();
    let group_for_reader2 = consumer_group2.clone();
    let consumer_for_reader2 = consumer_name2.clone();
    let (reader2_backend, _reader2_ctx) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_reader2.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_reader2.clone()],
                group_for_reader2.clone(),
                consumer_for_reader2.clone(),
            ))
            .add_event_single::<TestEvent>(stream_for_reader2.clone());
    })
    .expect("Reader2 Redis backend setup successful");

    // Setup reader1 app in group1
    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(reader1_backend));
    reader1.insert_resource(EventCollector::default());

    let s1 = stream.clone();
    let g1 = consumer_group1.clone();
    let consumer1 = consumer_name1.clone();
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g1.clone(), consumer1.clone(), [s1.clone()]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Setup reader2 app in group2
    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(reader2_backend));
    reader2.insert_resource(EventCollector::default());

    let s2 = stream.clone();
    let g2 = consumer_group2.clone();
    let consumer2 = consumer_name2.clone();
    reader2.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g2.clone(), consumer2.clone(), [s2.clone()]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Setup writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let stream_for_runtime_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<usize>| {
            if *sent < 4 {
                let config = RedisProducerConfig::new(stream_for_runtime_writer.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: format!("broadcast_message_{}", *sent),
                        value: 100 + *sent as i32,
                    },
                );
                *sent += 1;
            }
        },
    );

    // Start readers first to establish consumer groups
    run_app_updates(&mut reader1, 3);
    run_app_updates(&mut reader2, 3);

    // Send events
    run_app_updates(&mut writer, 5);

    // Wait for message distribution
    let (received1, _) = update_until(&mut reader1, 10_000, |app| {
        let collected = app.world().resource::<EventCollector>();
        collected.0.len() >= 4 // Expect all messages
    });

    let (received2, _) = update_until(&mut reader2, 10_000, |app| {
        let collected = app.world().resource::<EventCollector>();
        collected.0.len() >= 4 // Expect all messages
    });

    let collected1 = reader1.world().resource::<EventCollector>();
    let collected2 = reader2.world().resource::<EventCollector>();

    // Distinct consumer groups on the same Redis instance should each receive the full
    // set of events independently.
    assert!(
        received1,
        "Reader1 should receive all events via its consumer group"
    );
    assert!(
        received2,
        "Reader2 should receive all events via its consumer group"
    );

    assert_eq!(collected1.0.len(), 4);
    assert_eq!(collected2.0.len(), 4);

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
    let stream = unique_topic("writer-only");

    // Writer topology - no consumer groups (write-only)
    let stream_for_writer_topology = stream.clone();
    let (writer_backend, _context) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_writer_topology.clone()))
            .add_event_single::<TestEvent>(stream_for_writer_topology.clone());
    })
    .expect("Redis writer backend setup successful");

    // Setup writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let stream_for_runtime_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut events_sent: Local<usize>| {
            if *events_sent < 3 {
                let config = RedisProducerConfig::new(stream_for_runtime_writer.clone());
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
