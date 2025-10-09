#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group_membership, unique_topic};
use integration_tests::utils::redis_setup;

#[test]
fn multi_stream_isolation() {
    let stream1 = unique_topic("isolation_1");
    let stream2 = unique_topic("isolation_2");
    let membership1 = unique_consumer_group_membership("group1");
    let membership2 = unique_consumer_group_membership("group2");

    let reader1_stream = stream1.clone();
    let reader1_group = membership1.group.clone();
    let reader1_consumer = membership1.member.clone();
    let (backend1, _context1) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(reader1_stream.clone()))
            .add_consumer_group(
                reader1_group.clone(),
                RedisConsumerGroupSpec::new(
                    [reader1_stream.clone()],
                    reader1_group.clone(),
                    reader1_consumer.clone(),
                ),
            )
            .add_event_single::<TestEvent>(reader1_stream.clone());
    })
    .expect("Redis backend1 setup successful");

    let reader2_stream = stream2.clone();
    let reader2_group = membership2.group.clone();
    let reader2_consumer = membership2.member.clone();
    let (backend2, _context2) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(reader2_stream.clone()))
            .add_consumer_group(
                reader2_group.clone(),
                RedisConsumerGroupSpec::new(
                    [reader2_stream.clone()],
                    reader2_group.clone(),
                    reader2_consumer.clone(),
                ),
            )
            .add_event_single::<TestEvent>(reader2_stream.clone());
    })
    .expect("Redis backend2 setup successful");

    let writer1_stream = stream1.clone();
    let (writer1_backend, _context_w1) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(writer1_stream.clone()))
            .add_event_single::<TestEvent>(writer1_stream.clone());
    })
    .expect("Writer1 Redis backend setup successful");

    let writer2_stream = stream2.clone();
    let (writer2_backend, _context_w2) = redis_setup::prepare_backend(move |builder| {
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
    let g1_clone = membership1.group.clone();
    let c1_clone = membership1.member.clone();
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config =
                RedisConsumerConfig::new(g1_clone.clone(), c1_clone.clone(), [s1_clone.clone()]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Read from stream2
    let s2_clone = stream2.clone();
    let g2_clone = membership2.group.clone();
    let c2_clone = membership2.member.clone();
    reader2.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config =
                RedisConsumerConfig::new(g2_clone.clone(), c2_clone.clone(), [s2_clone.clone()]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Send events
    writer1.update();
    writer2.update();

    let mut received1 = 0;
    let mut received2 = 0;
    for _ in 0..5000 {
        reader1.update();
        reader2.update();

        let collected1 = reader1.world().resource::<Collected>();
        let collected2 = reader2.world().resource::<Collected>();
        received1 = collected1.0.len();
        received2 = collected2.0.len();
        if received1 >= 1 && received2 >= 1 {
            break;
        }
    }

    assert_eq!(
        received1, 1,
        "Reader1 should receive events written to stream1"
    );
    assert_eq!(
        received2, 1,
        "Reader2 should receive events written to stream2"
    );
}
