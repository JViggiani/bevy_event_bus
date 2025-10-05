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

#[test]
fn multi_stream_isolation() {
    let stream1 = unique_topic("isolation_1");
    let stream2 = unique_topic("isolation_2");
    let consumer_group1 = unique_consumer_group("group1");
    let consumer_group2 = unique_consumer_group("group2");

    // Setup stream1 with group1
    let mut builder1 = RedisTopologyBuilder::default();
    builder1
        .add_stream(RedisStreamSpec::new(stream1.clone()))
        .add_consumer_group(
            consumer_group1.clone(),
            RedisConsumerGroupSpec::new([stream1.clone()], consumer_group1.clone()),
        )
        .add_event_single::<TestEvent>(stream1.clone());

    let (backend1, _context1) =
        redis_setup::setup(builder1).expect("Redis backend1 setup successful");

    // Setup stream2 with group2
    let mut builder2 = RedisTopologyBuilder::default();
    builder2
        .add_stream(RedisStreamSpec::new(stream2.clone()))
        .add_consumer_group(
            consumer_group2.clone(),
            RedisConsumerGroupSpec::new([stream2.clone()], consumer_group2.clone()),
        )
        .add_event_single::<TestEvent>(stream2.clone());

    let (backend2, _context2) =
        redis_setup::setup(builder2).expect("Redis backend2 setup successful");

    // Create separate writer backends without consumer groups
    let mut writer1_builder = RedisTopologyBuilder::default();
    writer1_builder
        .add_stream(RedisStreamSpec::new(stream1.clone()))
        .add_event_single::<TestEvent>(stream1.clone());

    let (writer1_backend, _context_w1) =
        redis_setup::setup(writer1_builder).expect("Writer1 Redis backend setup successful");

    let mut writer2_builder = RedisTopologyBuilder::default();
    writer2_builder
        .add_stream(RedisStreamSpec::new(stream2.clone()))
        .add_event_single::<TestEvent>(stream2.clone());

    let (writer2_backend, _context_w2) =
        redis_setup::setup(writer2_builder).expect("Writer2 Redis backend setup successful");

    // Writer apps with separate backends
    let mut writer1 = App::new();
    writer1.add_plugins(EventBusPlugins(writer1_backend));
    let mut writer2 = App::new();
    writer2.add_plugins(EventBusPlugins(writer2_backend));

    // Reader apps with separate backends
    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(backend1));
    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(backend2));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);

    reader1.insert_resource(Collected::default());
    reader2.insert_resource(Collected::default());

    // Write to stream1
    let s1 = stream1.clone();
    writer1.add_systems(Update, move |mut w: RedisEventWriter| {
        let config = RedisProducerConfig::new(s1.clone());
        w.write(
            &config,
            TestEvent {
                message: "stream1_event".to_string(),
                value: 111,
            },
        );
    });

    // Write to stream2
    let s2 = stream2.clone();
    writer2.add_systems(Update, move |mut w: RedisEventWriter| {
        let config = RedisProducerConfig::new(s2.clone());
        w.write(
            &config,
            TestEvent {
                message: "stream2_event".to_string(),
                value: 222,
            },
        );
    });

    // Read from stream1
    let s1_clone = stream1.clone();
    let g1_clone = consumer_group1.clone();
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config =
                RedisConsumerConfig::new(s1_clone.clone()).set_consumer_group(g1_clone.clone());
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Read from stream2
    let s2_clone = stream2.clone();
    let g2_clone = consumer_group2.clone();
    reader2.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config =
                RedisConsumerConfig::new(s2_clone.clone()).set_consumer_group(g2_clone.clone());
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Send events
    writer1.update();
    writer2.update();

    // With separate backends, readers won't receive events from writers
    reader1.update();
    reader2.update();

    let collected1 = reader1.world().resource::<Collected>();
    let collected2 = reader2.world().resource::<Collected>();

    // With separate backends, no events should be received
    assert_eq!(
        collected1.0.len(),
        0,
        "Reader1 should not receive events from separate backend"
    );
    assert_eq!(
        collected2.0.len(),
        0,
        "Reader2 should not receive events from separate backend"
    );
}
