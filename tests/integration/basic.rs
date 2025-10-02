use bevy::prelude::*;
use bevy_event_bus::config::kafka::{KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaTopicSpec};
use bevy_event_bus::{EventBusAppExt, EventBusPlugins, KafkaEventReader, KafkaEventWriter};
use integration_tests::common::TestEvent;
use integration_tests::common::helpers::{
    DEFAULT_KAFKA_BOOTSTRAP, kafka_consumer_config, kafka_producer_config, unique_consumer_group,
    unique_topic, update_until,
};
use integration_tests::common::setup::setup;
use tracing::{info, info_span};
use tracing_subscriber::EnvFilter;

#[test]
fn test_basic_kafka_event_bus() {
    // Initialize tracing subscriber once (idempotent - ignore error)
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    let total_start = std::time::Instant::now();
    let total_span = info_span!("test_basic_kafka_event_bus.total");
    let _tg = total_span.enter();
    let setup_start = std::time::Instant::now();
    let topic = unique_topic("bevy-event-bus-test");
    let consumer_group = unique_consumer_group("basic_reader_group");

    let topic_for_writer = topic.clone();
    let (backend_writer, _bootstrap_writer) = setup("earliest", move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(topic_for_writer.clone())
                .partitions(1)
                .replication(1),
        );
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
    });

    info!(
        setup_total_ms = setup_start.elapsed().as_millis(),
        "Setup complete"
    );

    // Writer app
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(backend_writer));

    // Register bus event to enable KafkaEventWriter error handling
    writer_app.add_bus_event::<TestEvent>(&topic);

    #[derive(Resource, Clone)]
    struct ToSend(TestEvent, String);

    let event_to_send = TestEvent {
        message: "From Bevy!".into(),
        value: 100,
    };
    writer_app.insert_resource(ToSend(event_to_send.clone(), topic.clone()));

    fn writer_system(mut w: KafkaEventWriter, data: Res<ToSend>) {
        let config = kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&data.1]);
        let _ = w.write(&config, data.0.clone());
    }
    writer_app.add_systems(Update, writer_system);
    let writer_span = info_span!("writer_app.update");
    {
        let _wg = writer_span.enter();
        writer_app.update();
    }

    // Reader app (separate consumer group with separate backend)
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(backend_reader));

    // Register bus event for reading
    reader_app.add_bus_event::<TestEvent>(&topic);

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
        let config = kafka_consumer_config(DEFAULT_KAFKA_BOOTSTRAP, group.0.as_str(), [&topic.0]);
        for wrapper in r.read(&config) {
            collected.0.push(wrapper.event().clone());
        }
    }
    reader_app.add_systems(Update, reader_system);

    // Poll frames until first message or timeout
    let recv_span = info_span!("reader.poll");
    let _rg = recv_span.enter();
    let start_poll = std::time::Instant::now();
    let (received, frames) = update_until(&mut reader_app, 5000, |app| {
        let collected = app.world().resource::<Collected>();
        !collected.0.is_empty()
    });

    if received {
        info!(
            frames,
            elapsed_ms = start_poll.elapsed().as_millis(),
            "Received first message"
        );
    }

    info!(
        total_frames = frames,
        poll_elapsed_ms = start_poll.elapsed().as_millis(),
        "Polling finished"
    );

    let collected = reader_app.world().resource::<Collected>();
    assert!(
        collected.0.iter().any(|e| e == &event_to_send),
        "Expected to find sent event in collected list (collected={:?})",
        collected.0
    );
    info!(
        total_elapsed_ms = total_start.elapsed().as_millis(),
        "Test complete"
    );
}
