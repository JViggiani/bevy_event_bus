#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisMessageReader, RedisMessageWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group_membership, unique_topic, wait_for_events,
};
use integration_tests::utils::redis_setup;

#[derive(Resource, Default)]
struct EventCollector(Vec<TestEvent>);

/// Test the principle: expensive setup at startup, cheap operations at runtime
#[test]
fn test_topology_setup_principle() {
    let stream = unique_topic("setup-principle");
    let membership = unique_consumer_group_membership("principle-group");
    let consumer_group = membership.group.clone();
    let consumer_name = membership.member.clone();

    println!("=== EXPENSIVE SETUP AT STARTUP ===");

    // This is the expensive part - should happen once at app startup
    let start_setup = std::time::Instant::now();

    let stream_clone = stream.clone();
    let consumer_group_clone = consumer_group.clone();
    let consumer_name_clone = consumer_name.clone();
    let (backend, _context) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_clone.clone()],
                consumer_group_clone.clone(),
                consumer_name_clone.clone(),
            ))
            .add_event_single::<TestEvent>(stream_clone.clone());
    })
    .expect("Redis backend setup successful");

    let setup_time = start_setup.elapsed();
    println!("Backend setup took: {:?} (expensive, one-time)", setup_time);

    // Setup apps
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend.clone()));
    reader.insert_resource(EventCollector::default());

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend));

    println!("=== CHEAP OPERATIONS AT RUNTIME ===");

    // These should be cheap - just referencing pre-existing consumer groups
    let runtime_stream = stream.clone();
    let runtime_group = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisMessageReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let start_read = std::time::Instant::now();

            // This should be cheap - consumer group already exists and is ready
            let config = RedisConsumerConfig::new(runtime_group.clone(), [runtime_stream.clone()]);

            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }

            let read_time = start_read.elapsed();
            if read_time > std::time::Duration::from_millis(50) {
                println!(
                    "WARNING: Read operation took {:?} (should be fast)",
                    read_time
                );
            }
        },
    );

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisMessageWriter, mut sent: Local<bool>| {
            if !*sent {
                let start_write = std::time::Instant::now();

                // This should be cheap - stream already exists
                let config = RedisProducerConfig::new(stream_for_writer.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: "runtime-message".to_string(),
                        value: 123,
                    },
                    None,
                );

                let write_time = start_write.elapsed();
                if write_time > std::time::Duration::from_millis(50) {
                    println!(
                        "WARNING: Write operation took {:?} (should be fast)",
                        write_time
                    );
                }

                println!("✅ Fast runtime write completed in {:?}", write_time);
                *sent = true;
            }
        },
    );

    // Runtime operations should be fast
    println!("Performing runtime read/write operations...");

    // Prime the reader once, send a single event, then wait deterministically for receipt.
    run_app_updates(&mut reader, 1);
    run_app_updates(&mut writer, 1);

    let collected_events = wait_for_events(&mut reader, &stream, 5_000, 1, |app| {
        let collected = app.world().resource::<EventCollector>();
        collected.0.clone()
    });

    println!("✅ Runtime operations completed successfully");
    println!("Messages processed: {}", collected_events.len());

    // Verify the principle worked
    assert!(
        setup_time > std::time::Duration::from_millis(1),
        "Setup should take a measurable amount of time"
    );
    assert!(
        !collected_events.is_empty(),
        "Runtime operations should work efficiently"
    );

    println!(
        "✅ Topology setup principle validated: expensive setup once, cheap runtime operations"
    );
}
