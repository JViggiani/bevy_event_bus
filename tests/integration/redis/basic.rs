#![cfg(feature = "redis")]
#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
    RedisTopologyBuilder,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic, update_until};
use integration_tests::utils::redis_setup;

#[test]
fn test_basic_redis_event_bus() {
    let stream = unique_topic("bevy-event-bus-test");
    let consumer_group = unique_consumer_group("basic_reader_group");

    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()),
        )
        .add_event_single::<TestEvent>(stream.clone());

    let (backend, _context) = redis_setup::setup(builder).expect("Redis backend setup successful");

    let backend_reader = backend.clone();
    let backend_writer = backend;

    // Reader app (start first to ensure it's ready)
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(backend_reader));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader_app.insert_resource(Collected::default());
    #[derive(Resource, Clone)]
    struct Stream(String);
    #[derive(Resource, Clone)]
    struct ConsumerGroup(String);
    reader_app.insert_resource(Stream(stream.clone()));
    reader_app.insert_resource(ConsumerGroup(consumer_group));

    fn reader_system(
        mut reader: RedisEventReader<TestEvent>,
        stream: Res<Stream>,
        group: Res<ConsumerGroup>,
        mut collected: ResMut<Collected>,
    ) {
        let config = RedisConsumerConfig::new(stream.0.clone()).set_consumer_group(group.0.clone());
        for wrapper in reader.read(&config) {
            collected.0.push(wrapper.event().clone());
        }
    }
    reader_app.add_systems(Update, reader_system);

    // Writer app
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(backend_writer));

    #[derive(Resource, Clone)]
    struct ToSend(TestEvent, String);

    let event_to_send = TestEvent {
        message: "From Bevy!".into(),
        value: 100,
    };
    writer_app.insert_resource(ToSend(event_to_send.clone(), stream.clone()));

    fn writer_system(mut writer: RedisEventWriter, data: Res<ToSend>) {
        let config = RedisProducerConfig::new(data.1.clone());
        writer.write(&config, data.0.clone());
    }
    writer_app.add_systems(Update, writer_system);
    writer_app.update();

    // Poll until message received or timeout
    let (received, _) = update_until(&mut reader_app, 5000, |app| {
        let collected = app.world().resource::<Collected>();
        !collected.0.is_empty()
    });

    assert!(received, "Expected to receive event within timeout");

    let collected = reader_app.world().resource::<Collected>();
    assert!(
        collected.0.iter().any(|e| e == &event_to_send),
        "Expected to find sent event in collected list (collected={:?})",
        collected.0
    );
}
