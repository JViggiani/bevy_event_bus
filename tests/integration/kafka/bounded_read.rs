use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{EventBusPlugins, KafkaMessageReader, KafkaMessageWriter};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic, update_until};
use integration_tests::utils::kafka_setup;

#[derive(Resource, Default)]
struct Collected(Vec<TestEvent>);

#[test]
fn read_bounded_limits_message_drain() {
    let topic = unique_topic("bounded_read");
    let consumer_group = unique_consumer_group("bounded_read_group");

    let topic_for_writer = topic.clone();
    let (backend_writer, _ctx_w) = kafka_setup::prepare_backend(kafka_setup::earliest(
        move |builder| {
            builder
                .add_topic(
                    KafkaTopicSpec::new(topic_for_writer.clone())
                        .partitions(1)
                        .replication(1),
                )
                .add_event_single::<TestEvent>(topic_for_writer.clone());
        },
    ));

    let topic_for_reader = topic.clone();
    let group_for_reader = consumer_group.clone();
    let (backend_reader, _ctx_r) = kafka_setup::prepare_backend(kafka_setup::earliest(
        move |builder| {
            builder
                .add_topic(
                    KafkaTopicSpec::new(topic_for_reader.clone())
                        .partitions(1)
                        .replication(1),
                )
                .add_consumer_group(
                    group_for_reader.clone(),
                    KafkaConsumerGroupSpec::new([topic_for_reader.clone()])
                        .initial_offset(KafkaInitialOffset::Earliest),
                )
                .add_event_single::<TestEvent>(topic_for_reader.clone());
        },
    ));

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins { backend: backend_writer });

    let tclone = topic.clone();
    writer.add_systems(Update, move |mut w: KafkaMessageWriter| {
        let config = KafkaProducerConfig::new([tclone.clone()]);
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

    let topic_for_system = topic.clone();
    let group_for_system = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: KafkaMessageReader<TestEvent>, mut collected: ResMut<Collected>| {
            let config = KafkaConsumerConfig::new(group_for_system.as_str(), [&topic_for_system]);
            for wrapper in r.read_bounded(&config, 2) {
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
