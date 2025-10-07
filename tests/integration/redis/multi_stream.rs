#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
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

    let reader1_db = redis_setup::ensure_shared_redis().expect("Redis backend1 setup successful");
    let reader2_db = redis_setup::ensure_shared_redis().expect("Redis backend2 setup successful");
    let writer1_db =
        redis_setup::ensure_shared_redis().expect("Writer1 Redis backend setup successful");
    let writer2_db =
        redis_setup::ensure_shared_redis().expect("Writer2 Redis backend setup successful");

    let reader1_stream = stream1.clone();
    let reader1_group = consumer_group1.clone();
    let (backend1, _context1) = reader1_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader1_stream.clone()))
                .add_consumer_group(
                    reader1_group.clone(),
                    RedisConsumerGroupSpec::new([reader1_stream.clone()], reader1_group.clone()),
                )
                .add_event_single::<TestEvent>(reader1_stream.clone());
        })
        .expect("Redis backend1 setup successful");

    let reader2_stream = stream2.clone();
    let reader2_group = consumer_group2.clone();
    let (backend2, _context2) = reader2_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader2_stream.clone()))
                .add_consumer_group(
                    reader2_group.clone(),
                    RedisConsumerGroupSpec::new([reader2_stream.clone()], reader2_group.clone()),
                )
                .add_event_single::<TestEvent>(reader2_stream.clone());
        })
        .expect("Redis backend2 setup successful");

    let writer1_stream = stream1.clone();
    let (writer1_backend, _context_w1) = writer1_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer1_stream.clone()))
                .add_event_single::<TestEvent>(writer1_stream.clone());
        })
        .expect("Writer1 Redis backend setup successful");

    let writer2_stream = stream2.clone();
    let (writer2_backend, _context_w2) = writer2_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer2_stream.clone()))
                .add_event_single::<TestEvent>(writer2_stream.clone());
        })
        .expect("Writer2 Redis backend setup successful");

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
            let config = RedisConsumerConfig::new(g1_clone.clone(), [s1_clone.clone()]);
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
            let config = RedisConsumerConfig::new(g2_clone.clone(), [s2_clone.clone()]);
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
