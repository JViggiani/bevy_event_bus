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

    // Use a shared Redis database so that readers observe the same stream
    let shared_redis =
        redis_setup::ensure_shared_redis().expect("Shared Redis setup should succeed");

    let stream_for_reader1 = stream.clone();
    let group_for_reader1 = consumer_group.clone();
    let consumer_for_reader1 = consumer1.clone();
    let (reader1_backend, _reader1_ctx) = shared_redis
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(stream_for_reader1.clone()))
                .add_consumer_group(
                    group_for_reader1.clone(),
                    RedisConsumerGroupSpec::new(
                        [stream_for_reader1.clone()],
                        group_for_reader1.clone(),
                    )
                    .consumer_name(consumer_for_reader1.clone()),
                )
                .add_event_single::<TestEvent>(stream_for_reader1.clone());
        })
        .expect("Redis reader1 backend setup successful");

    let stream_for_reader2 = stream.clone();
    let group_for_reader2 = consumer_group.clone();
    let consumer_for_reader2 = consumer2.clone();
    let (reader2_backend, _reader2_ctx) = shared_redis
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(stream_for_reader2.clone()))
                .add_consumer_group(
                    group_for_reader2.clone(),
                    RedisConsumerGroupSpec::new(
                        [stream_for_reader2.clone()],
                        group_for_reader2.clone(),
                    )
                    .consumer_name(consumer_for_reader2.clone()),
                )
                .add_event_single::<TestEvent>(stream_for_reader2.clone());
        })
        .expect("Redis reader2 backend setup successful");

    // Writer backend (no consumer groups)
    let stream_for_writer_topology = stream.clone();
    let (writer_backend, _writer_ctx) = shared_redis
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(stream_for_writer_topology.clone()))
                .add_event_single::<TestEvent>(stream_for_writer_topology.clone());
        })
        .expect("Redis writer backend setup successful");

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
            let config =
                RedisConsumerConfig::new(g1.clone(), [s1.clone()]).set_consumer_name(c1.clone()); // Use SAME name as topology
            let initial_count = c.0.len();
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
            if c.0.len() > initial_count {
                println!("Reader1 total events: {}", c.0.len());
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
            let config =
                RedisConsumerConfig::new(g2.clone(), [s2.clone()]).set_consumer_name(c2.clone()); // Use SAME name as topology
            let initial_count = c.0.len();
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
            if c.0.len() > initial_count {
                println!("Reader2 total events: {}", c.0.len());
            }
        },
    );

    // Setup writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<usize>| {
            if *sent < 6 {
                let config = RedisProducerConfig::new(stream_for_writer.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: format!("message_{}", *sent),
                        value: *sent as i32,
                    },
                );
                println!("Writer sent event {}: message_{}", *sent, *sent);
                *sent += 1;
            }
        },
    );

    // Start readers first to establish consumer groups BEFORE any events are sent
    run_app_updates(&mut reader1, 3);
    run_app_updates(&mut reader2, 3);

    // Wait a moment for consumer groups to be fully established
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Send events AFTER consumer groups are established
    run_app_updates(&mut writer, 7);

    // Wait for message distribution across both readers.
    let expected_total = 6usize;
    let (completed, _) = update_two_apps_until(&mut reader1, &mut reader2, 5_000, |r1, r2| {
        let count1 = {
            let world = r1.world();
            world.resource::<EventCollector>().0.len()
        };
        let count2 = {
            let world = r2.world();
            world.resource::<EventCollector>().0.len()
        };

        (count1 + count2) >= expected_total && count1 > 0 && count2 > 0
    });

    assert!(
        completed,
        "Readers did not observe the complete event set within the allotted time"
    );

    let collected1 = reader1.world().resource::<EventCollector>();
    let collected2 = reader2.world().resource::<EventCollector>();

    // Combined total should match dispatched events and both readers should contribute.
    let total_received = collected1.0.len() + collected2.0.len();
    assert_eq!(
        total_received, 6,
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
        "Test result: Reader1 got {} events, Reader2 got {} events",
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

    // Get shared Redis connection for all backends
    let shared_redis =
        redis_setup::ensure_shared_redis().expect("Failed to setup shared Redis instance");

    // Writer backend - write-only topology (no consumer groups)
    let stream_for_writer = stream.clone();
    let (writer_backend, _writer_ctx) = shared_redis
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(stream_for_writer.clone()))
                .add_event_single::<TestEvent>(stream_for_writer.clone());
        })
        .expect("Writer Redis backend setup successful");

    // Reader1 backend - with consumer group 1
    let stream_for_reader1 = stream.clone();
    let group_for_reader1 = consumer_group1.clone();
    let (reader1_backend, _reader1_ctx) = shared_redis
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(stream_for_reader1.clone()))
                .add_consumer_group(
                    group_for_reader1.clone(),
                    RedisConsumerGroupSpec::new(
                        [stream_for_reader1.clone()],
                        group_for_reader1.clone(),
                    )
                    .start_id("0"), // Explicitly start from beginning
                )
                .add_event_single::<TestEvent>(stream_for_reader1.clone());
        })
        .expect("Reader1 Redis backend setup successful");

    // Reader2 backend - with consumer group 2
    let stream_for_reader2 = stream.clone();
    let group_for_reader2 = consumer_group2.clone();
    let (reader2_backend, _reader2_ctx) = shared_redis
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(stream_for_reader2.clone()))
                .add_consumer_group(
                    group_for_reader2.clone(),
                    RedisConsumerGroupSpec::new(
                        [stream_for_reader2.clone()],
                        group_for_reader2.clone(),
                    )
                    .start_id("0"), // Explicitly start from beginning
                )
                .add_event_single::<TestEvent>(stream_for_reader2.clone());
        })
        .expect("Reader2 Redis backend setup successful");

    // Setup reader1 app in group1
    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(reader1_backend));
    reader1.insert_resource(EventCollector::default());

    let s1 = stream.clone();
    let g1 = consumer_group1.clone();
    let consumer1 = unique_consumer_group_member(&consumer_group1);
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g1.clone(), [s1.clone()])
                .set_consumer_name(consumer1.clone());
            let initial_count = c.0.len();
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
            if c.0.len() > initial_count {
                println!("Reader1 (group1) total events: {}", c.0.len());
            }
        },
    );

    // Setup reader2 app in group2
    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(reader2_backend));
    reader2.insert_resource(EventCollector::default());

    let s2 = stream.clone();
    let g2 = consumer_group2.clone();
    let consumer2 = unique_consumer_group_member(&consumer_group2);
    reader2.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g2.clone(), [s2.clone()])
                .set_consumer_name(consumer2.clone());
            let initial_count = c.0.len();
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
            if c.0.len() > initial_count {
                println!("Reader2 (group2) total events: {}", c.0.len());
            }
        },
    );

    // Setup writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<usize>| {
            if *sent < 4 {
                let config = RedisProducerConfig::new(stream_for_writer.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: format!("broadcast_{}", *sent),
                        value: *sent as i32,
                    },
                );
                println!("Writer sent event {}: broadcast_{}", *sent, *sent);
                *sent += 1;
            }
        },
    );

    // Start readers first to establish consumer groups
    run_app_updates(&mut reader1, 3);
    run_app_updates(&mut reader2, 3);

    // Wait for consumer groups to be established
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Send events
    run_app_updates(&mut writer, 5);

    // Wait for message distribution
    let (_received1, _) = update_until(&mut reader1, 5000, |app| {
        let collected = app.world().resource::<EventCollector>();
        collected.0.len() >= 4 // Expect all messages
    });

    let (_received2, _) = update_until(&mut reader2, 5000, |app| {
        let collected = app.world().resource::<EventCollector>();
        collected.0.len() >= 4 // Expect all messages
    });

    let collected1 = reader1.world().resource::<EventCollector>();
    let collected2 = reader2.world().resource::<EventCollector>();

    // Distinct consumer groups on the same Redis instance should each receive the full
    // set of events independently.
    assert_eq!(
        collected1.0.len(),
        4,
        "Reader1 should receive the full set of events via its consumer group"
    );
    assert_eq!(
        collected2.0.len(),
        4,
        "Reader2 should receive the full set of events via its consumer group"
    );
}

/// Writers should function without any configured consumer groups.
#[test]
fn writer_only_works_without_consumer_groups() {
    let stream = unique_topic("writer-only");

    // Writer topology - no consumer groups
    let writer_db =
        redis_setup::ensure_shared_redis().expect("Redis writer backend setup successful");

    let stream_for_topology = stream.clone();
    let (writer_backend, _context) = writer_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(stream_for_topology.clone()))
                .add_event_single::<TestEvent>(stream_for_topology.clone());
        })
        .expect("Redis writer backend setup successful");

    // Setup writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut events_sent: Local<usize>| {
            if *events_sent < 3 {
                let config = RedisProducerConfig::new(stream_for_writer.clone());
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
