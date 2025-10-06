use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{EventBusPlugins, KafkaEventReader, KafkaEventWriter};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic, update_until};
use integration_tests::utils::kafka_setup;

#[test]
fn multi_topic_isolation() {
    let topic_a = unique_topic("topicA");
    let topic_b = unique_topic("topicB");
    let consumer_group = unique_consumer_group("multi_topic_reader");

    let topic_a_writer = topic_a.clone();
    let topic_b_writer = topic_b.clone();
    let (backend_w, _b1) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_a_writer.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_event_single::<TestEvent>(topic_a_writer.clone());
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_b_writer.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_event_single::<TestEvent>(topic_b_writer.clone());
    }));

    let topic_a_reader = topic_a.clone();
    let topic_b_reader = topic_b.clone();
    let group_for_reader = consumer_group.clone();
    let (backend_r, _b2) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(topic_a_reader.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_topic(
            KafkaTopicSpec::new(topic_b_reader.clone())
                .partitions(1)
                .replication(1),
        );
        builder
            .add_consumer_group(
                group_for_reader.clone(),
                KafkaConsumerGroupSpec::new([topic_a_reader.clone(), topic_b_reader.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            )
            .add_event_single::<TestEvent>(topic_a_reader.clone())
            .add_event_single::<TestEvent>(topic_b_reader.clone());
    }));

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_w));

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_r));

    let ta = topic_a.clone();
    let tb = topic_b.clone();
    writer.add_systems(Update, move |mut w: KafkaEventWriter| {
        let config_a = KafkaProducerConfig::new([ta.clone()]);
        let _ = w.write(
            &config_a,
            TestEvent {
                message: "A1".into(),
                value: 1,
            },
        );
        let config_b = KafkaProducerConfig::new([tb.clone()]);
        let _ = w.write(
            &config_b,
            TestEvent {
                message: "B1".into(),
                value: 2,
            },
        );
    });
    writer.update();

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());
    let ta_r = topic_a.clone();
    let tb_r = topic_b.clone();
    let consumer_group_clone = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: KafkaEventReader<TestEvent>, mut col: ResMut<Collected>| {
            let config_a = KafkaConsumerConfig::new(consumer_group_clone.as_str(), [&ta_r]);
            for wrapper in r.read(&config_a) {
                col.0.push(wrapper.event().clone());
            }
            let config_b = KafkaConsumerConfig::new(consumer_group_clone.as_str(), [&tb_r]);
            for wrapper in r.read(&config_b) {
                col.0.push(wrapper.event().clone());
            }
        },
    );

    // Spin until both messages observed or timeout
    let (ok, _frames) = update_until(&mut reader, 5000, |app| {
        let col = app.world().resource::<Collected>();
        col.0.iter().any(|e| e.message == "A1") && col.0.iter().any(|e| e.message == "B1")
    });
    assert!(ok, "Timed out waiting for both topic messages");
    let col = reader.world().resource::<Collected>();
    assert!(col.0.iter().any(|e| e.message == "A1"));
    assert!(col.0.iter().any(|e| e.message == "B1"));
    assert!(
        col.0
            .iter()
            .filter(|e| e.message.starts_with('A'))
            .all(|e| e.message == "A1")
    );
    assert!(
        col.0
            .iter()
            .filter(|e| e.message.starts_with('B'))
            .all(|e| e.message == "B1")
    );
}
