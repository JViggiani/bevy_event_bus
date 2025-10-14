use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{EventBusPlugins, KafkaEventReader, KafkaEventWriter};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic, update_until};
use integration_tests::utils::kafka_setup;

/// Test that events are delivered exactly once - no duplication
#[test]
fn no_event_duplication_exactly_once_delivery() {
    let topic = unique_topic("exactly_once");
    let consumer_group = unique_consumer_group("exactly_once_reader");

    let topic_for_writer = topic.clone();
    let (backend_writer, _bootstrap_writer) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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
    let (backend_reader, bootstrap_reader) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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

    // Create topic and wait for it to be fully ready
    let topic_ready = kafka_setup::ensure_topic_ready(
        &bootstrap_reader,
        &topic,
        1, // partitions
        std::time::Duration::from_secs(5),
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);

    // Writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_writer));

    // Send exactly 10 unique events (as a resource to avoid closure issues)
    #[derive(Resource, Clone)]
    struct ToSend(Vec<TestEvent>, String);

    let expected_events: Vec<TestEvent> = (0..10)
        .map(|i| TestEvent {
            message: format!("unique_event_{}", i),
            value: i,
        })
        .collect();

    writer.insert_resource(ToSend(expected_events.clone(), topic.clone()));

    fn writer_system(mut w: KafkaEventWriter, data: Res<ToSend>) {
        let config = KafkaProducerConfig::new([data.1.clone()]);
        for event in &data.0 {
            let _ = w.write(&config, event.clone());
        }
    }
    writer.add_systems(Update, writer_system);

    // Reader app with separate backend
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_reader));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());

    #[derive(Resource, Clone)]
    struct Topic(String);
    #[derive(Resource, Clone)]
    struct ConsumerGroup(String);
    reader.insert_resource(Topic(topic.clone()));
    reader.insert_resource(ConsumerGroup(consumer_group));

    fn reader_system(
        mut r: KafkaEventReader<TestEvent>,
        topic: Res<Topic>,
        group: Res<ConsumerGroup>,
        mut collected: ResMut<Collected>,
    ) {
        let config = KafkaConsumerConfig::new(group.0.clone(), [&topic.0]);
        for wrapper in r.read(&config) {
            collected.0.push(wrapper.event().clone());
        }
    }
    reader.add_systems(Update, reader_system);

    // Start writer to send events
    writer.update();

    // Wait for all 10 events to be received
    let (ok, _frames) = update_until(&mut reader, 5000, |app| {
        let c = app.world().resource::<Collected>();
        c.0.len() >= 10
    });

    let collected = reader.world().resource::<Collected>();

    assert!(
        ok,
        "Timed out waiting for events. Got {} events",
        collected.0.len()
    );

    // Verify exactly 10 events were received (no duplication)
    assert_eq!(
        collected.0.len(),
        10,
        "Expected exactly 10 events, got {}",
        collected.0.len()
    );

    // Verify each expected event appears exactly once
    for expected_event in &expected_events {
        let count = collected
            .0
            .iter()
            .filter(|e| e.message == expected_event.message && e.value == expected_event.value)
            .count();
        assert_eq!(
            count, 1,
            "Event {:?} appeared {} times, expected exactly 1",
            expected_event, count
        );
    }

    // Wait a bit more and verify no additional events arrive
    let (_, _) = update_until(&mut reader, 1000, |_| false); // Just wait, don't expect anything

    let collected_after_wait = reader.world().resource::<Collected>();
    assert_eq!(
        collected_after_wait.0.len(),
        10,
        "Additional events appeared after waiting: expected 10, got {}",
        collected_after_wait.0.len()
    );
}
