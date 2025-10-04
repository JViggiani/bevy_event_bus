use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{EventBusPlugins, KafkaEventReader, KafkaEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic, update_until};
use integration_tests::utils::setup::setup;

#[test]
fn test_basic_kafka_event_bus() {
    let topic = unique_topic("bevy-event-bus-test");
    let consumer_group = unique_consumer_group("basic_reader_group");

    let topic_for_writer = topic.clone();
    let (backend_writer, _bootstrap_writer) = setup("earliest", move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(topic_for_writer.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_event_single::<TestEvent>(topic_for_writer.clone());
    });

    let topic_for_reader = topic.clone();
    let group_for_reader = consumer_group.clone();
    let (backend_reader, _bootstrap_reader) = setup("earliest", move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(topic_for_reader.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_consumer_group(
            group_for_reader.clone(),
            KafkaConsumerGroupSpec::new([topic_for_reader.clone()])
                .initial_offset(KafkaInitialOffset::Earliest),
        );
        builder.add_event_single::<TestEvent>(topic_for_reader.clone());
    });

    // Writer app
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(backend_writer));

    #[derive(Resource, Clone)]
    struct ToSend(TestEvent, String);

    let event_to_send = TestEvent {
        message: "From Bevy!".into(),
        value: 100,
    };
    writer_app.insert_resource(ToSend(event_to_send.clone(), topic.clone()));

    fn writer_system(mut w: KafkaEventWriter, data: Res<ToSend>) {
        let config = KafkaProducerConfig::new([data.1.clone()]);
        let _ = w.write(&config, data.0.clone());
    }
    writer_app.add_systems(Update, writer_system);
    writer_app.update();

    // Reader app (separate consumer group with separate backend)
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(backend_reader));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader_app.insert_resource(Collected::default());
    #[derive(Resource, Clone)]
    struct Topic(String);
    #[derive(Resource, Clone)]
    struct ConsumerGroup(String);
    reader_app.insert_resource(Topic(topic.clone()));
    reader_app.insert_resource(ConsumerGroup(consumer_group));

    fn reader_system(
        mut r: KafkaEventReader<TestEvent>,
        topic: Res<Topic>,
        group: Res<ConsumerGroup>,
        mut collected: ResMut<Collected>,
    ) {
        let config = KafkaConsumerConfig::new(group.0.clone(), [topic.0.clone()]);
        for wrapper in r.read(&config) {
            collected.0.push(wrapper.event().clone());
        }
    }
    reader_app.add_systems(Update, reader_system);

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