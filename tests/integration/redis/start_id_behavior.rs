#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
    RedisTopologyBuilder,
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

    // Create backend with consumer group using "0-0" start_id (from beginning)
    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()).start_id("0-0"), // Read from beginning
        )
        .add_event_single::<TestEvent>(stream.clone());

    // Create separate writer backend without consumer groups
    let mut writer_builder = RedisTopologyBuilder::default();
    writer_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_event_single::<TestEvent>(stream.clone());

    let (writer_backend, _context1) =
        redis_setup::setup(writer_builder).expect("Writer Redis backend setup successful");

    let (reader_backend, _context2) =
        redis_setup::setup(builder).expect("Reader Redis backend setup successful");

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
            let config = RedisConsumerConfig::new(s.clone())
                .set_consumer_group(g.clone())
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

    // Create backend with consumer group using "$" start_id (only new messages)
    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()).start_id("$"), // Read only NEW messages
        )
        .add_event_single::<TestEvent>(stream.clone());

    // Create separate writer backend without consumer groups
    let mut writer_builder = RedisTopologyBuilder::default();
    writer_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_event_single::<TestEvent>(stream.clone());

    let (writer_backend, _context1) =
        redis_setup::setup(writer_builder).expect("Writer Redis backend setup successful");

    let (reader_backend, _context2) =
        redis_setup::setup(builder).expect("Reader Redis backend setup successful");

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
            let config = RedisConsumerConfig::new(s.clone())
                .set_consumer_group(g.clone())
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
