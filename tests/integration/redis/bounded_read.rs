#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisMessageReader, RedisMessageWriter};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{
    unique_consumer_group_membership, unique_topic, update_until,
};
use integration_tests::utils::redis_setup;

#[derive(Resource, Default)]
struct Collected(Vec<TestEvent>);

#[test]
fn read_bounded_limits_stream_drain() {
    let stream = unique_topic("bounded_read");
    let membership = unique_consumer_group_membership("bounded_read_group");

    let database = redis_setup::allocate_database()
        .expect("Redis database allocation for bounded read test");

    let writer_stream = stream.clone();
    let (backend_writer, _ctx_w) = redis_setup::with_database(database, || {
        redis_setup::prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer_stream.clone()))
                .add_event_single::<TestEvent>(writer_stream.clone());
        })
    })
    .expect("Writer Redis backend setup successful");

    let reader_stream = stream.clone();
    let reader_group = membership.group.clone();
    let reader_consumer = membership.member.clone();
    let (backend_reader, _ctx_r) = redis_setup::with_database(database, || {
        redis_setup::prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader_stream.clone()))
                .add_consumer_group(
                    RedisConsumerGroupSpec::new(
                        [reader_stream.clone()],
                        reader_group.clone(),
                        reader_consumer.clone(),
                    )
                    .manual_ack(true),
                )
                .add_event_single::<TestEvent>(reader_stream.clone());
        })
    })
    .expect("Reader Redis backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins { backend: backend_writer });

    let sclone = stream.clone();
    writer.add_systems(Update, move |mut w: RedisMessageWriter| {
        let config = RedisProducerConfig::new(sclone.clone());
        for i in 0..4 {
            w.write(
                &config,
                TestEvent {
                    message: format!("v{i}"),
                    value: i,
                },
                None,
            );
        }
    });
    writer.update();

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins { backend: backend_reader });
    reader.insert_resource(Collected::default());

    let stream_for_system = stream.clone();
    let group_for_system = membership.group.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisMessageReader<TestEvent>, mut collected: ResMut<Collected>| {
            let config = RedisConsumerConfig::new(group_for_system.clone(), [stream_for_system.clone()]);
            for wrapper in r.read_bounded(&config, 2) {
                r.acknowledge(&wrapper)
                    .expect("Redis acknowledge should succeed for bounded read test");
                collected.0.push(wrapper.event().clone());
            }
        },
    );

    let (received_initial, _frames) = update_until(&mut reader, 5_000, |app| {
        let len = app.world().resource::<Collected>().0.len();
        len > 0
    });
    assert!(received_initial, "Timed out waiting for first bounded batch");
    let first_batch = reader.world().resource::<Collected>().0.len();
    assert_eq!(first_batch, 2, "first bounded read should return exactly 2 events");

    let (ok, _frames) = update_until(&mut reader, 5_000, |app| {
        let collected = app.world().resource::<Collected>();
        collected.0.len() >= 4
    });
    assert!(ok, "Timed out waiting for second bounded drain");

    let collected = reader.world().resource::<Collected>();
    assert_eq!(collected.0.len(), 4);
}
