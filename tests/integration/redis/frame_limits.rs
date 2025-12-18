#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{
    EventBusConsumerConfig, EventBusPlugins, RedisMessageReader, RedisMessageWriter,
};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    unique_consumer_group_membership, unique_topic, update_until,
};
use integration_tests::utils::redis_setup;

#[test]
fn frame_limit_spreads_drain() {
    let stream = unique_topic("frame_limits");
    let membership = unique_consumer_group_membership("frame_limit_group");

    let database =
        redis_setup::allocate_database().expect("Redis database allocation for frame limit test");

    let writer_stream = stream.clone();
    let (backend_writer, _context_writer) = redis_setup::with_database(database, || {
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
    let (backend_reader, _context_reader) = redis_setup::with_database(database, || {
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
    writer.add_plugins(EventBusPlugins(backend_writer));

    #[derive(Resource, Clone)]
    struct Payload(Vec<TestEvent>, String);

    let events_to_send: Vec<TestEvent> = (0..15)
        .map(|i| TestEvent {
            message: format!("v{i}"),
            value: i,
        })
        .collect();

    writer.insert_resource(Payload(events_to_send.clone(), stream.clone()));

    fn writer_system(mut writer: RedisMessageWriter, payload: Res<Payload>, mut sent: Local<bool>) {
        if *sent {
            return;
        }
        *sent = true;
        let config = RedisProducerConfig::new(payload.1.clone());
        for event in &payload.0 {
            writer.write(&config, event.clone(), None);
        }
    }

    writer.add_systems(Update, writer_system);
    writer.update();

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_reader));
    reader.insert_resource(EventBusConsumerConfig {
        max_events_per_frame: Some(5),
        max_drain_millis: None,
    });

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);

    reader.insert_resource(Collected::default());

    #[derive(Resource, Clone)]
    struct Stream(String);
    #[derive(Resource, Clone)]
    struct Group(String);

    reader.insert_resource(Stream(stream));
    reader.insert_resource(Group(membership.group));

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
                .expect("Redis acknowledge should succeed under frame limit test");
            collected.0.push(wrapper.event().clone());
        }
    }

    reader.add_systems(Update, reader_system);

    let (ok, _frames) = update_until(&mut reader, 5_000, |app| {
        let collected = app.world().resource::<Collected>();
        collected.0.len() >= 15
    });

    assert!(ok, "Timed out waiting for all events under frame limit");
    let collected = reader.world().resource::<Collected>();
    assert_eq!(collected.0.len(), 15);

    for expected in &events_to_send {
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
}
