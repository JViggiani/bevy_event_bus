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

/// Test without consumer names - like basic.rs test pattern
#[test]
fn test_consumer_groups_without_names() {
    let stream = unique_topic("no-names");
    let consumer_group = unique_consumer_group("shared-group");

    // Use exactly same pattern as basic.rs test - single backend for all apps
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

    // Setup reader1 app WITHOUT consumer name (like basic.rs)
    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(backend.clone()));
    reader1.insert_resource(EventCollector::default());

    let s1 = stream.clone();
    let g1 = consumer_group.clone();
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g1.clone(), [s1.clone()]);
            // NO .set_consumer_name() call - like basic.rs
            let initial_count = c.0.len();
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
            if c.0.len() > initial_count {
                println!("Reader1 total events: {}", c.0.len());
            }
        },
    );

    // Setup reader2 app WITHOUT consumer name (like basic.rs)
    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(backend.clone()));
    reader2.insert_resource(EventCollector::default());

    let s2 = stream.clone();
    let g2 = consumer_group.clone();
    reader2.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g2.clone(), [s2.clone()]);
            // NO .set_consumer_name() call - like basic.rs
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
    println!("Starting reader1 to create consumer group...");
    run_app_updates(&mut reader1, 3);
    println!("Starting reader2 to join consumer group...");
    run_app_updates(&mut reader2, 3);

    // Wait a moment for consumer groups to be fully established
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Send events AFTER consumer groups are established
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

    // At least one reader should have received events (round-robin distribution)
    assert!(
        received1 || received2,
        "At least one reader should receive events without consumer names"
    );

    println!(
        "Test result WITHOUT consumer names: Reader1 got {} events, Reader2 got {} events",
        collected1.0.len(),
        collected2.0.len()
    );
}
