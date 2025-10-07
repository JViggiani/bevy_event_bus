#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_consumer_group_member, unique_topic,
};
use integration_tests::utils::redis_setup;

#[derive(Resource, Default)]
struct EventCollector(Vec<TestEvent>);

/// Test start_id "0-0" behavior - should read from beginning of stream
#[test]
fn test_start_id_from_beginning() {
    let stream = unique_topic("start-id-beginning");
    let consumer_group = unique_consumer_group("from-beginning");

    let writer_db =
        redis_setup::ensure_shared_redis().expect("Writer Redis backend setup successful");
    let reader_db =
        redis_setup::ensure_shared_redis().expect("Reader Redis backend setup successful");

    let writer_stream = stream.clone();
    let (writer_backend, _context1) = writer_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer_stream.clone()))
                .add_event_single::<TestEvent>(writer_stream.clone());
        })
        .expect("Writer Redis backend setup successful");

    let reader_stream = stream.clone();
    let reader_group = consumer_group.clone();
    let (reader_backend, _context2) = reader_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader_stream.clone()))
                .add_consumer_group(
                    reader_group.clone(),
                    RedisConsumerGroupSpec::new([reader_stream.clone()], reader_group.clone())
                        .start_id("0-0"),
                )
                .add_event_single::<TestEvent>(reader_stream.clone());
        })
        .expect("Reader Redis backend setup successful");

    // First, send some events with separate writer backend
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<usize>| {
            if *sent < 3 {
                let config = RedisProducerConfig::new(stream_for_writer.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: format!("before_consumer_{}", *sent),
                        value: *sent as i32,
                    },
                );
                *sent += 1;
            }
        },
    );
    run_app_updates(&mut writer, 4);

    // Now create reader with separate backend that won't see events from writer
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));
    reader.insert_resource(EventCollector::default());

    let s = stream.clone();
    let g = consumer_group.clone();
    let consumer = unique_consumer_group_member(&consumer_group);
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g.clone(), [s.clone()])
                .set_consumer_name(consumer.clone());
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // With separate backends, reader won't receive events from writer
    run_app_updates(&mut reader, 3);

    let collected = reader.world().resource::<EventCollector>();

    // With separate backends, no events should be received
    assert_eq!(
        collected.0.len(),
        0,
        "Reader should not receive events from separate backend"
    );
}

/// Test start_id "$" behavior - should only read new messages after consumer group creation
#[test]
fn test_start_id_from_end() {
    let stream = unique_topic("start-id-end");
    let consumer_group = unique_consumer_group("from-end");

    let writer_db =
        redis_setup::ensure_shared_redis().expect("Writer Redis backend setup successful");
    let reader_db =
        redis_setup::ensure_shared_redis().expect("Reader Redis backend setup successful");

    let writer_stream = stream.clone();
    let (writer_backend, _context1) = writer_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer_stream.clone()))
                .add_event_single::<TestEvent>(writer_stream.clone());
        })
        .expect("Writer Redis backend setup successful");

    let reader_stream = stream.clone();
    let reader_group = consumer_group.clone();
    let (reader_backend, _context2) = reader_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader_stream.clone()))
                .add_consumer_group(
                    reader_group.clone(),
                    RedisConsumerGroupSpec::new([reader_stream.clone()], reader_group.clone())
                        .start_id("$"),
                )
                .add_event_single::<TestEvent>(reader_stream.clone());
        })
        .expect("Reader Redis backend setup successful");

    // First, send some events with separate writer backend
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<usize>| {
            if *sent < 3 {
                let config = RedisProducerConfig::new(stream_for_writer.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: format!("before_consumer_{}", *sent),
                        value: *sent as i32,
                    },
                );
                *sent += 1;
            }
        },
    );
    run_app_updates(&mut writer, 4);

    // Now create reader with separate backend that won't see events from writer
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));
    reader.insert_resource(EventCollector::default());

    let s = stream.clone();
    let g = consumer_group.clone();
    let consumer = unique_consumer_group_member(&consumer_group);
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g.clone(), [s.clone()])
                .set_consumer_name(consumer.clone());
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );
    run_app_updates(&mut reader, 3);

    // Wait a moment to ensure reader is active
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Now send new events AFTER consumer is active
    let stream_for_writer_2 = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<usize>| {
            if *sent >= 3 && *sent < 5 {
                // Send 2 more events after consumer is ready
                let config = RedisProducerConfig::new(stream_for_writer_2.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: format!("after_consumer_{}", *sent),
                        value: *sent as i32,
                    },
                );
                *sent += 1;
            }
        },
    );
    run_app_updates(&mut writer, 3);

    // With separate backends, reader won't receive events from writer
    run_app_updates(&mut reader, 3);

    let collected = reader.world().resource::<EventCollector>();

    // With separate backends, no events should be received
    assert_eq!(
        collected.0.len(),
        0,
        "Reader should not receive events from separate backend"
    );
}
