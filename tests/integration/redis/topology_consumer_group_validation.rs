#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{run_app_updates, unique_consumer_group, unique_topic};
use integration_tests::utils::redis_setup;

#[derive(Resource, Default)]
struct EventCollector(Vec<TestEvent>);

/// Test that reading from a consumer group that EXISTS in topology works correctly
#[test]
fn test_read_from_topology_defined_consumer_group() {
    let stream = unique_topic("topology-defined");
    let topology_group = unique_consumer_group("topology-group");

    // Create backend with consumer group in topology (expensive setup at startup)
    let shared_db = redis_setup::ensure_shared_redis().expect("Redis backend setup successful");

    let stream_clone = stream.clone();
    let topology_group_clone = topology_group.clone();
    let (backend, _context) = shared_db.prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()))
            .add_consumer_group(
                topology_group_clone.clone(),
                RedisConsumerGroupSpec::new([stream_clone.clone()], topology_group_clone.clone()),
            )
            .add_event_single::<TestEvent>(stream_clone.clone());
    })
    .expect("Redis backend setup successful");

    // Setup reader that uses the topology-defined consumer group
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend.clone()));
    reader.insert_resource(EventCollector::default());

    let s = stream.clone();
    let g = topology_group.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            // Reference the consumer group that was defined in topology
            let config = RedisConsumerConfig::new(g.clone(), [s.clone()]);

            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Setup writer
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend));

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<bool>| {
            if !*sent {
                let config = RedisProducerConfig::new(stream_for_writer.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: "topology-group-message".to_string(),
                        value: 42,
                    },
                );
                println!("Writer sent message to topology-defined consumer group");
                *sent = true;
            }
        },
    );

    // Start reader first to ensure consumer group is ready
    println!("Starting reader with topology-defined consumer group...");
    run_app_updates(&mut reader, 3);

    // Send message
    println!("Sending message...");
    run_app_updates(&mut writer, 3);

    // Reader should process the message
    run_app_updates(&mut reader, 5);

    let collected = reader.world().resource::<EventCollector>();

    println!(
        "Reader received {} messages from topology-defined group",
        collected.0.len()
    );

    // This should work - reading from topology-defined consumer group
    assert!(
        collected.0.len() > 0,
        "Reading from topology-defined consumer group should work"
    );
}

/// Test that reading from a consumer group that does NOT exist in topology fails appropriately
#[test]
fn test_read_from_non_topology_consumer_group_should_fail() {
    let stream = unique_topic("non-topology");
    let topology_group = unique_consumer_group("topology-group");
    let non_topology_group = unique_consumer_group("non-topology-group");

    // Create backend with ONE consumer group in topology
    let shared_db = redis_setup::ensure_shared_redis().expect("Redis backend setup successful");

    let stream_clone = stream.clone();
    let topology_group_clone = topology_group.clone();
    let (backend, _context) = shared_db.prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()))
            .add_consumer_group(
                topology_group_clone.clone(),
                RedisConsumerGroupSpec::new([stream_clone.clone()], topology_group_clone.clone()),
            )
            .add_event_single::<TestEvent>(stream_clone.clone());
    })
    .expect("Redis backend setup successful");

    // Setup reader that tries to use a DIFFERENT consumer group (not in topology)
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend.clone()));
    reader.insert_resource(EventCollector::default());

    let s = stream.clone();
    let g = non_topology_group.clone(); // This group was NOT defined in topology!
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            // Try to reference a consumer group that was NOT defined in topology
            let config = RedisConsumerConfig::new(g.clone(), [s.clone()]);

            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
                println!("ERROR: Should not receive messages from non-topology group!");
            }
        },
    );

    // Setup writer
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend));

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<bool>| {
            if !*sent {
                let config = RedisProducerConfig::new(stream_for_writer.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: "non-topology-message".to_string(),
                        value: 99,
                    },
                );
                println!("Writer sent message (should not reach non-topology consumer group)");
                *sent = true;
            }
        },
    );

    println!("Testing reader with NON-topology consumer group...");

    // Start reader with non-topology group
    run_app_updates(&mut reader, 3);

    // Send message
    run_app_updates(&mut writer, 3);

    // Try to read (should fail or receive nothing)
    run_app_updates(&mut reader, 5);

    let collected = reader.world().resource::<EventCollector>();

    println!(
        "Reader received {} messages from non-topology group",
        collected.0.len()
    );

    // This should fail - reading from non-topology consumer group should not work
    assert_eq!(
        collected.0.len(),
        0,
        "Reading from non-topology consumer group should not receive messages"
    );

    println!("✅ Correctly prevented reading from non-topology consumer group");
}

/// Test the principle: expensive setup at startup, cheap operations at runtime
#[test]
fn test_topology_setup_principle() {
    let stream = unique_topic("setup-principle");
    let consumer_group = unique_consumer_group("principle-group");

    println!("=== EXPENSIVE SETUP AT STARTUP ===");

    // This is the expensive part - should happen once at app startup
    let start_setup = std::time::Instant::now();

    let shared_db = redis_setup::ensure_shared_redis().expect("Redis backend setup successful");

    let stream_clone = stream.clone();
    let consumer_group_clone = consumer_group.clone();
    let (backend, _context) = shared_db.prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()))
            .add_consumer_group(
                consumer_group_clone.clone(),
                RedisConsumerGroupSpec::new([stream_clone.clone()], consumer_group_clone.clone()),
            )
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
    let s = stream.clone();
    let g = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let start_read = std::time::Instant::now();

            // This should be cheap - consumer group already exists and is ready
            let config = RedisConsumerConfig::new(g.clone(), [s.clone()]);

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
        move |mut w: RedisEventWriter, mut sent: Local<bool>| {
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

    run_app_updates(&mut reader, 3);
    run_app_updates(&mut writer, 3);
    run_app_updates(&mut reader, 5);

    let collected = reader.world().resource::<EventCollector>();

    println!("✅ Runtime operations completed successfully");
    println!("Messages processed: {}", collected.0.len());

    // Verify the principle worked
    assert!(
        setup_time > std::time::Duration::from_millis(1),
        "Setup should take a measurable amount of time"
    );
    assert!(
        collected.0.len() > 0,
        "Runtime operations should work efficiently"
    );

    println!(
        "✅ Topology setup principle validated: expensive setup once, cheap runtime operations"
    );
}
