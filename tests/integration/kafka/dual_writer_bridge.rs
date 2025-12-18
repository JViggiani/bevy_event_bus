use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{EventBusPlugins, KafkaMessageReader, KafkaMessageWriter, MessageWrapper};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_topic, wait_for_events,
};
use integration_tests::utils::kafka_setup;
use tracing::{info, info_span};
use tracing_subscriber::EnvFilter;

#[derive(Resource)]
struct WriterState {
    topic: String,
    dispatched: bool,
}

fn dual_writer_emitter(
    mut standard_messages: ResMut<Messages<TestEvent>>,
    mut bus_writer: KafkaMessageWriter,
    mut state: ResMut<WriterState>,
) {
    if state.dispatched {
        return;
    }

    state.dispatched = true;

    let bridge_event = TestEvent {
        message: "standard-writer".to_string(),
        value: 1,
    };
    standard_messages.write(bridge_event.clone());

    let bus_event = TestEvent {
        message: "event-bus-writer".to_string(),
        value: 2,
    };
    let config = KafkaProducerConfig::new([state.topic.clone()]);
    bus_writer.write(&config, bus_event, None);

    info!(topic = %state.topic, "Dispatched events via both writers");
}

#[derive(Resource)]
struct ReaderState {
    topic: String,
    consumer_group: String,
}

#[derive(Resource, Default)]
struct CapturedEvents(Vec<MessageWrapper<TestEvent>>);

fn capture_wrapped_events(
    mut reader: KafkaMessageReader<TestEvent>,
    state: Res<ReaderState>,
    mut captured: ResMut<CapturedEvents>,
) {
    let config = KafkaConsumerConfig::new(state.consumer_group.clone(), [&state.topic]);
    for wrapper in reader.read(&config) {
        captured.0.push(wrapper.clone());
    }
}

#[test]
fn external_bus_events_flow_from_both_writers() {
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .try_init();

    let _span = info_span!("dual_writer_bridge_test").entered();

    let topic = unique_topic("dual-writer");
    let consumer_group = unique_consumer_group("dual-writer-group");

    let topic_for_writer = topic.clone();
    let (backend_writer, _bootstrap_w) =
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
    let (backend_reader, _bootstrap_r) =
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

    // Writer app configuration
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(backend_writer));
    writer_app.insert_resource(WriterState {
        topic: topic.clone(),
        dispatched: false,
    });
    writer_app.add_systems(Update, dual_writer_emitter);

    // Run a few frames to allow bridge system to observe the event queue and publish externally
    run_app_updates(&mut writer_app, 4);

    // Reader app configuration
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(backend_reader));
    reader_app.insert_resource(ReaderState {
        topic: topic.clone(),
        consumer_group,
    });
    reader_app.insert_resource(CapturedEvents::default());
    reader_app.add_systems(Update, capture_wrapped_events);

    let received = wait_for_events(&mut reader_app, &topic, 12_000, 2, |app| {
        app.world().resource::<CapturedEvents>().0.clone()
    });

    assert_eq!(
        received.len(),
        2,
        "Exactly two events should arrive – one per writer path",
    );

    let mut messages: Vec<&str> = received
        .iter()
        .map(|wrapper| wrapper.message.as_str())
        .collect();
    messages.sort();
    assert_eq!(messages, vec!["event-bus-writer", "standard-writer"]);

    for wrapper in &received {
        let metadata = wrapper.metadata();
        assert_eq!(metadata.source.as_str(), topic.as_str());
        assert!(metadata.timestamp.elapsed().as_secs() < 30);
    }

    let standard = received
        .iter()
        .find(|wrapper| wrapper.message == "standard-writer")
        .expect("standard writer event missing");
    assert_eq!(standard.value, 1);

    let bus = received
        .iter()
        .find(|wrapper| wrapper.message == "event-bus-writer")
        .expect("event bus writer event missing");
    assert_eq!(bus.value, 2);
}
