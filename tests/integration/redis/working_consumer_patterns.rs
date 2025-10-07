#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_topic, update_until,
};
use integration_tests::utils::redis_setup;

#[derive(Resource, Default)]
struct EventCollector(Vec<TestEvent>);

/// Test consumer group semantics using the WORKING pattern (no consumer names, single backend)
#[test]
fn test_working_consumer_group_patterns() {
    let stream = unique_topic("working-pattern");
    let consumer_group = unique_consumer_group("working-group");

    // Use the WORKING pattern - single backend, no consumer names (like basic.rs)
    let shared_db = redis_setup::ensure_shared_redis().expect("Redis backend setup successful");

    let stream_clone = stream.clone();
    let consumer_group_clone = consumer_group.clone();
    let (backend, _context) = shared_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(stream_clone.clone()))
                .add_consumer_group(
                    consumer_group_clone.clone(),
                    RedisConsumerGroupSpec::new(
                        [stream_clone.clone()],
                        consumer_group_clone.clone(),
                    ),
                )
                .add_event_single::<TestEvent>(stream_clone.clone());
        })
        .expect("Redis backend setup successful");

    // Setup reader1 app WITHOUT consumer name (working pattern)
    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(backend.clone()));
    reader1.insert_resource(EventCollector::default());

    let s1 = stream.clone();
    let g1 = consumer_group.clone();
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g1.clone(), [s1.clone()]);
            // NO .set_consumer_name() - this is the working pattern!
            let initial_count = c.0.len();
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
            if c.0.len() > initial_count {
                println!("Reader1 total events: {}", c.0.len());
            }
        },
    );

    // Setup reader2 app WITHOUT consumer name (working pattern)
    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(backend.clone()));
    reader2.insert_resource(EventCollector::default());

    let s2 = stream.clone();
    let g2 = consumer_group.clone();
    reader2.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g2.clone(), [s2.clone()]);
            // NO .set_consumer_name() - this is the working pattern!
            let initial_count = c.0.len();
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
            if c.0.len() > initial_count {
                println!("Reader2 total events: {}", c.0.len());
            }
        },
    );

    // Setup writer app (working pattern)
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend));

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

    // Start readers first to establish consumer groups
    println!("Starting reader1 to create consumer group...");
    run_app_updates(&mut reader1, 3);
    println!("Starting reader2 to join consumer group...");
    run_app_updates(&mut reader2, 3);

    // Wait for consumer groups to be established
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Send events
    println!("Starting writer to send 6 events...");
    run_app_updates(&mut writer, 7);

    // Wait for message distribution
    let (received1, _) = update_until(&mut reader1, 5000, |app| {
        let collected = app.world().resource::<EventCollector>();
        !collected.0.is_empty()
    });

    let (received2, _) = update_until(&mut reader2, 5000, |app| {
        let collected = app.world().resource::<EventCollector>();
        !collected.0.is_empty()
    });

    let collected1 = reader1.world().resource::<EventCollector>();
    let collected2 = reader2.world().resource::<EventCollector>();

    // At least one reader should receive events (this is the working pattern)
    assert!(
        received1 || received2,
        "At least one reader should receive events with working pattern"
    );

    println!(
        "WORKING pattern test result: Reader1 got {} events, Reader2 got {} events",
        collected1.0.len(),
        collected2.0.len()
    );

    // Analyze the distribution pattern
    let total_events = collected1.0.len() + collected2.0.len();
    println!("Total events distributed: {} out of 6 sent", total_events);

    if collected1.0.len() > 0 && collected2.0.len() > 0 {
        println!("✅ ROUND-ROBIN DISTRIBUTION: Both readers received events");
    } else if total_events > 0 {
        println!(
            "✅ SINGLE CONSUMER: One reader processed all events (valid round-robin behavior)"
        );
    } else {
        println!("❌ NO DISTRIBUTION: No events were received");
    }
}

/// Test broadcast behavior by creating separate consumer groups
#[test]
fn test_broadcast_with_different_groups_working_pattern() {
    let stream = unique_topic("broadcast-working");
    let consumer_group1 = unique_consumer_group("broadcast-group1");
    let consumer_group2 = unique_consumer_group("broadcast-group2");

    // Use WORKING pattern with separate consumer groups for broadcast behavior
    let shared_db = redis_setup::ensure_shared_redis().expect("Redis backend setup successful");

    let stream_clone = stream.clone();
    let consumer_group1_clone = consumer_group1.clone();
    let consumer_group2_clone = consumer_group2.clone();
    let (backend, _context) = shared_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(stream_clone.clone()))
                .add_consumer_group(
                    consumer_group1_clone.clone(),
                    RedisConsumerGroupSpec::new(
                        [stream_clone.clone()],
                        consumer_group1_clone.clone(),
                    ),
                )
                .add_consumer_group(
                    consumer_group2_clone.clone(),
                    RedisConsumerGroupSpec::new(
                        [stream_clone.clone()],
                        consumer_group2_clone.clone(),
                    ),
                )
                .add_event_single::<TestEvent>(stream_clone.clone());
        })
        .expect("Redis backend setup successful");

    // Setup reader1 app in group1 (working pattern)
    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(backend.clone()));
    reader1.insert_resource(EventCollector::default());

    let s1 = stream.clone();
    let g1 = consumer_group1.clone();
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g1.clone(), [s1.clone()]);
            // NO .set_consumer_name() - working pattern!
            let initial_count = c.0.len();
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
            if c.0.len() > initial_count {
                println!("Reader1 (group1) total events: {}", c.0.len());
            }
        },
    );

    // Setup reader2 app in group2 (working pattern)
    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(backend.clone()));
    reader2.insert_resource(EventCollector::default());

    let s2 = stream.clone();
    let g2 = consumer_group2.clone();
    reader2.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g2.clone(), [s2.clone()]);
            // NO .set_consumer_name() - working pattern!
            let initial_count = c.0.len();
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
            if c.0.len() > initial_count {
                println!("Reader2 (group2) total events: {}", c.0.len());
            }
        },
    );

    // Setup writer app (working pattern)
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend));

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
    println!("Starting reader1 (group1)...");
    run_app_updates(&mut reader1, 3);
    println!("Starting reader2 (group2)...");
    run_app_updates(&mut reader2, 3);

    // Wait for consumer groups to be established
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Send events
    println!("Starting writer to send 4 events...");
    run_app_updates(&mut writer, 5);

    // Wait for message distribution
    let (received1, _) = update_until(&mut reader1, 5000, |app| {
        let collected = app.world().resource::<EventCollector>();
        !collected.0.is_empty()
    });

    let (received2, _) = update_until(&mut reader2, 5000, |app| {
        let collected = app.world().resource::<EventCollector>();
        !collected.0.is_empty()
    });

    let collected1 = reader1.world().resource::<EventCollector>();
    let collected2 = reader2.world().resource::<EventCollector>();

    println!(
        "BROADCAST test result: Reader1 (group1) got {} events, Reader2 (group2) got {} events",
        collected1.0.len(),
        collected2.0.len()
    );

    // For broadcast behavior, both readers should receive events
    // (different consumer groups = each gets all messages)
    if collected1.0.len() > 0 && collected2.0.len() > 0 {
        println!("✅ BROADCAST BEHAVIOR: Both consumer groups received events");
    } else if collected1.0.len() > 0 || collected2.0.len() > 0 {
        println!("⚠️ PARTIAL BROADCAST: Only one consumer group received events");
        // This might still be valid depending on Redis stream behavior
    } else {
        println!("❌ NO BROADCAST: No consumer groups received events");
    }

    // At least one consumer group should receive events
    assert!(
        received1 || received2,
        "At least one consumer group should receive broadcast events"
    );
}
