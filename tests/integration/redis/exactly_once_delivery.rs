#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
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

    let writer_db =
        redis_setup::ensure_shared_redis().expect("Writer Redis backend setup successful");
    let reader1_db =
        redis_setup::ensure_shared_redis().expect("Reader1 Redis backend setup successful");
    let reader2_db =
        redis_setup::ensure_shared_redis().expect("Reader2 Redis backend setup successful");

    let writer_stream = stream.clone();
    let (writer_backend, _context1) = writer_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer_stream.clone()))
                .add_event_single::<TestEvent>(writer_stream.clone());
        })
        .expect("Writer Redis backend setup successful");

    let reader1_stream = stream.clone();
    let reader1_group = consumer_group.clone();
    let (reader1_backend, _context2) = reader1_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader1_stream.clone()))
                .add_consumer_group(
                    reader1_group.clone(),
                    RedisConsumerGroupSpec::new([reader1_stream.clone()], reader1_group.clone())
                        .manual_ack(true),
                )
                .add_event_single::<TestEvent>(reader1_stream.clone());
        })
        .expect("Reader1 Redis backend setup successful");

    let reader2_stream = stream.clone();
    let reader2_group = consumer_group.clone();
    let (reader2_backend, _context3) = reader2_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader2_stream.clone()))
                .add_consumer_group(
                    reader2_group.clone(),
                    RedisConsumerGroupSpec::new([reader2_stream.clone()], reader2_group.clone())
                        .manual_ack(true),
                )
                .add_event_single::<TestEvent>(reader2_stream.clone());
        })
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
            let config = RedisConsumerConfig::new(g1.clone(), [s1.clone()]);
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
            let config = RedisConsumerConfig::new(g2.clone(), [s2.clone()]);
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
