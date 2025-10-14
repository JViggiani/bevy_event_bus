use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{EventBusConsumerConfig, EventBusPlugins, KafkaEventReader, KafkaEventWriter};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic, update_until};
use integration_tests::utils::kafka_setup;

#[test]
fn frame_limit_spreads_drain() {
    let topic = unique_topic("limit");
    let consumer_group = unique_consumer_group("frame_limit_reader");

    let topic_for_writer = topic.clone();
    let (backend_w, _b1) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_for_writer.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_event_single::<TestEvent>(topic_for_writer.clone());
    }));

    let topic_for_reader = topic.clone();
    let group_for_reader = consumer_group.clone();
    let (backend_r, _b2) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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
    }));
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_w));

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_r));
    let tclone = topic.clone();
    writer.add_systems(Update, move |mut w: KafkaEventWriter| {
        let config = KafkaProducerConfig::new([tclone.clone()]);
        for i in 0..15 {
            w.write(
                &config,
                TestEvent {
                    message: format!("v{i}"),
                    value: i,
                },
            );
        }
    });
    writer.update();

    reader.insert_resource(EventBusConsumerConfig {
        max_events_per_frame: Some(5),
        max_drain_millis: None,
    });
    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());
    let tr = topic.clone();
    reader.add_systems(
        Update,
        move |mut r: KafkaEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config = KafkaConsumerConfig::new(consumer_group.as_str(), [&tr]);
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Spin until all 15 collected or timeout
    let (ok, _frames) = update_until(&mut reader, 5000, |app| {
        let c = app.world().resource::<Collected>();
        c.0.len() >= 15
    });
    assert!(ok, "Timed out waiting for all events under frame limit");
    let collected = reader.world().resource::<Collected>();
    assert_eq!(collected.0.len(), 15);
}
