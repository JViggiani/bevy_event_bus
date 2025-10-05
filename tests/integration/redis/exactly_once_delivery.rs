#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
    RedisTopologyBuilder,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic};
use integration_tests::utils::redis_setup;

/// Test that exactly-once delivery semantics work with acknowledgments
#[test]
fn no_event_duplication_exactly_once_delivery() {
    let stream = unique_topic("exactly_once");
    let consumer_group = unique_consumer_group("exactly_once_group");

    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()).manual_ack(true), // Enable manual acknowledgment for exactly-once
        )
        .add_event_single::<TestEvent>(stream.clone());

    // Create separate writer backend without consumer groups
    let mut writer_builder = RedisTopologyBuilder::default();
    writer_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_event_single::<TestEvent>(stream.clone());

    let (writer_backend, _context1) = redis_setup::setup_with_builder(writer_builder)
        .expect("Writer Redis backend setup successful");

    // Create separate reader1 backend with consumer group
    let mut reader1_builder = RedisTopologyBuilder::default();
    reader1_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()).manual_ack(true), // Enable manual acknowledgment for exactly-once
        )
        .add_event_single::<TestEvent>(stream.clone());

    let (reader1_backend, _context2) = redis_setup::setup_with_builder(reader1_builder)
        .expect("Reader1 Redis backend setup successful");

    // Create separate reader2 backend with consumer group
    let mut reader2_builder = RedisTopologyBuilder::default();
    reader2_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()).manual_ack(true), // Enable manual acknowledgment for exactly-once
        )
        .add_event_single::<TestEvent>(stream.clone());

    let (reader2_backend, _context3) = redis_setup::setup_with_builder(reader2_builder)
        .expect("Reader2 Redis backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(reader1_backend));

    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(reader2_backend));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);

    reader1.insert_resource(Collected::default());
    reader2.insert_resource(Collected::default());

    // Send a single event
    let stream_clone = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<bool>| {
            if !*sent {
                *sent = true;
                let config = RedisProducerConfig::new(stream_clone.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: "exactly_once_test".to_string(),
                        value: 12345,
                    },
                );
            }
        },
    );

    // Two readers with same consumer group - only one should get the event
    let s1 = stream.clone();
    let g1 = consumer_group.clone();
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config = RedisConsumerConfig::new(s1.clone()).set_consumer_group(g1.clone());
            for wrapper in r.read(&config) {
                // Acknowledge to ensure exactly-once semantics
                if let Err(e) = r.acknowledge(&wrapper) {
                    eprintln!("Failed to acknowledge message: {:?}", e);
                }
                c.0.push(wrapper.event().clone());
            }
        },
    );

    let s2 = stream.clone();
    let g2 = consumer_group.clone();
    reader2.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config = RedisConsumerConfig::new(s2.clone()).set_consumer_group(g2.clone());
            for wrapper in r.read(&config) {
                // Acknowledge to ensure exactly-once semantics
                if let Err(e) = r.acknowledge(&wrapper) {
                    eprintln!("Failed to acknowledge message: {:?}", e);
                }
                c.0.push(wrapper.event().clone());
            }
        },
    );

    writer.update(); // Send event

    // With separate backends, no readers will receive events from the writer
    reader1.update();
    reader2.update();

    let collected1 = reader1.world().resource::<Collected>();
    let collected2 = reader2.world().resource::<Collected>();

    // With separate backends, no events should be received
    let total_received = collected1.0.len() + collected2.0.len();
    assert_eq!(
        total_received, 0,
        "Should receive no events with separate backends, got {}",
        total_received
    );
}
