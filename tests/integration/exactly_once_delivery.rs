use bevy::prelude::*;
use bevy_event_bus::{EventBusAppExt, EventBusPlugins, KafkaEventReader, KafkaEventWriter};
use integration_tests::common::events::TestEvent;
use integration_tests::common::helpers::{
    DEFAULT_KAFKA_BOOTSTRAP, kafka_consumer_config, kafka_producer_config, unique_consumer_group,
    unique_topic, update_until,
};
use integration_tests::common::setup::setup;

/// Test that events are delivered exactly once - no duplication
#[test]
fn no_event_duplication_exactly_once_delivery() {
    // Create separate backends for writer and reader to simulate separate machines
    let (backend_writer, _bootstrap_writer) = setup();
    let (backend_reader, _bootstrap_reader) = setup();

    let topic = unique_topic("exactly_once");

    // Create topic and wait for it to be fully ready
    let topic_ready = integration_tests::common::setup::ensure_topic_ready(
        &_bootstrap_reader,
        &topic,
        1, // partitions
        std::time::Duration::from_secs(5),
    );
    assert!(topic_ready, "Topic {} not ready within timeout", topic);

    // Writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(
        backend_writer,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    writer.add_bus_event::<TestEvent>(&topic);

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
        for event in &data.0 {
            let _ = w.write(
                &kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&data.1]),
                event.clone(),
            );
        }
    }
    writer.add_systems(Update, writer_system);

    // Reader app with separate backend
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(
        backend_reader,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    reader.add_bus_event::<TestEvent>(&topic);

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());

    #[derive(Resource, Clone)]
    struct Topic(String);
    #[derive(Resource, Clone)]
    struct ConsumerGroup(String);
    let consumer_group = unique_consumer_group("exactly_once_reader");
    reader.insert_resource(Topic(topic.clone()));
    reader.insert_resource(ConsumerGroup(consumer_group));

    fn reader_system(
        mut r: KafkaEventReader<TestEvent>,
        topic: Res<Topic>,
        group: Res<ConsumerGroup>,
        mut collected: ResMut<Collected>,
    ) {
        for wrapper in r.read(&kafka_consumer_config(
            DEFAULT_KAFKA_BOOTSTRAP,
            group.0.as_str(),
            [&topic.0],
        )) {
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
