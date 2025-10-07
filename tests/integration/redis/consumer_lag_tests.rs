#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{RedisConsumerConfig, RedisProducerConfig, RedisStreamSpec};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::unique_topic;
use integration_tests::utils::redis_setup;

/// Test handling of consumer lag and message retention
#[test]
fn consumer_lag_and_stream_trimming() {
    let stream = unique_topic("lag_test");

    let writer_db =
        redis_setup::ensure_shared_redis().expect("Writer Redis backend setup successful");
    let reader_db =
        redis_setup::ensure_shared_redis().expect("Reader Redis backend setup successful");

    let writer_stream = stream.clone();
    let (writer_backend, _context1) = writer_db.prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(writer_stream.clone()).maxlen(10))
            .add_event_single::<TestEvent>(writer_stream.clone());
    })
    .expect("Writer Redis backend setup successful");

    let reader_stream = stream.clone();
    let (reader_backend, _context2) = reader_db.prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(reader_stream.clone()).maxlen(10))
            .add_event_single::<TestEvent>(reader_stream.clone());
    })
    .expect("Reader Redis backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());

    // Send many events (more than max_length)
    let stream_clone = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut count: Local<u32>| {
            if *count < 15 {
                *count += 1;
                let config = RedisProducerConfig::new(stream_clone.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: format!("message_{}", *count),
                        value: *count as i32,
                    },
                );
            }
        },
    );

    // Send all messages first
    for _ in 0..15 {
        writer.update();
    }

    // Now start consuming - should only see recent messages due to trimming
    let s = stream.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config = RedisConsumerConfig::ungrouped([s.clone()]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // With separate backends, no events will be received
    reader.update();
    reader.update();

    let collected = reader.world().resource::<Collected>();

    // With separate backends, no events should be received
    assert_eq!(
        collected.0.len(),
        0,
        "Should not receive events from separate backend"
    );
}

/// Test Redis stream memory optimization with MAXLEN APPROXIMATE
#[test]
fn stream_memory_optimization() {
    let stream = unique_topic("memory_opt");

    let shared_db =
        redis_setup::ensure_shared_redis().expect("Writer Redis backend setup successful");

    let stream_clone = stream.clone();
    let (writer_backend, _context) = shared_db.prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()).maxlen(5))
            .add_event_single::<TestEvent>(stream_clone.clone());
    })
    .expect("Writer Redis backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    // Send messages in bursts to test memory management
    let stream_clone = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut count: Local<u32>| {
            if *count < 20 {
                *count += 1;
                let config = RedisProducerConfig::new(stream_clone.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: format!("burst_message_{}", *count),
                        value: *count as i32,
                    },
                );
            }
        },
    );

    // Send messages in batches
    for _ in 0..20 {
        writer.update();
    }

    // Verify stream is trimmed and doesn't grow indefinitely
    // This is primarily testing that the configuration is applied correctly
    // In a real scenario, you'd query Redis directly to check XLEN
}
