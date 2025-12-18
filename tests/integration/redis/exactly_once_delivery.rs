#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisMessageReader, RedisMessageWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    unique_consumer_group, unique_consumer_group_member, unique_topic, update_until,
};
use integration_tests::utils::redis_setup;

/// Test that exactly-once delivery semantics work with acknowledgments
#[test]
fn no_event_duplication_exactly_once_delivery() {
    let stream = unique_topic("exactly_once");
    let consumer_group = unique_consumer_group("exactly_once_group");
    let backend_consumer = unique_consumer_group_member(&consumer_group);

    let writer_stream = stream.clone();
    let (writer_backend, _context1) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(writer_stream.clone()))
            .add_event_single::<TestEvent>(writer_stream.clone());
    })
    .expect("Writer Redis backend setup successful");

    let reader_stream = stream.clone();
    let reader_group = consumer_group.clone();
    let reader_backend_consumer = backend_consumer.clone();
    let (reader_backend, _context2) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(reader_stream.clone()))
            .add_consumer_group(
                RedisConsumerGroupSpec::new(
                    [reader_stream.clone()],
                    reader_group.clone(),
                    reader_backend_consumer.clone(),
                )
                .manual_ack(true),
            )
            .add_event_single::<TestEvent>(reader_stream.clone());
    })
    .expect("reader Redis backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    #[derive(Resource, Clone)]
    struct ToSend(Vec<TestEvent>, String);

    let expected_events: Vec<TestEvent> = (0..10)
        .map(|i| TestEvent {
            message: format!("unique_event_{}", i),
            value: i,
        })
        .collect();

    writer.insert_resource(ToSend(expected_events.clone(), stream.clone()));

    fn writer_system(mut writer: RedisMessageWriter, data: Res<ToSend>) {
        let config = RedisProducerConfig::new(data.1.clone());
        for event in &data.0 {
            writer.write(&config, event.clone(), None);
        }
    }

    writer.add_systems(Update, writer_system);

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));
    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());

    #[derive(Resource, Clone)]
    struct Stream(String);
    #[derive(Resource, Clone)]
    struct Group(String);

    reader.insert_resource(Stream(stream.clone()));
    reader.insert_resource(Group(consumer_group));

    fn reader_system(
        mut reader: RedisMessageReader<TestEvent>,
        stream: Res<Stream>,
        group: Res<Group>,
        mut collected: ResMut<Collected>,
    ) {
        let config = RedisConsumerConfig::new(group.0.clone(), [stream.0.clone()]);
        for wrapper in reader.read(&config) {
            reader
                .acknowledge(&wrapper)
                .expect("Redis acknowledge should succeed for exactly-once test");
            collected.0.push(wrapper.event().clone());
        }
    }

    reader.add_systems(Update, reader_system);

    writer.update();

    let (ok, _frames) = update_until(&mut reader, 5_000, |app| {
        let collected = app.world().resource::<Collected>();
        collected.0.len() >= 10
    });

    let collected = reader.world().resource::<Collected>();

    assert!(
        ok,
        "Timed out waiting for events. Got {} events",
        collected.0.len()
    );

    assert_eq!(
        collected.0.len(),
        10,
        "Expected exactly 10 events, got {}",
        collected.0.len()
    );

    for expected in &expected_events {
        let count = collected
            .0
            .iter()
            .filter(|event| event.message == expected.message && event.value == expected.value)
            .count();
        assert_eq!(
            count, 1,
            "Event {:?} appeared {} times, expected exactly 1",
            expected, count
        );
    }

    let (_, _) = update_until(&mut reader, 1_000, |_| false);

    let collected_after_wait = reader.world().resource::<Collected>();
    assert_eq!(
        collected_after_wait.0.len(),
        10,
        "Additional events appeared after waiting: expected 10, got {}",
        collected_after_wait.0.len()
    );
}
