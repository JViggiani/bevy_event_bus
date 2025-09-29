use crate::common::events::TestEvent;
use crate::common::helpers::{
    kafka_consumer_config, kafka_producer_config, run_app_updates, unique_consumer_group,
    unique_topic, wait_for_events,
};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{
    EventBusAppExt, EventBusPlugins, EventBusReader, EventBusWriter, EventWrapper,
    PreconfiguredTopics,
};
use tracing::{info, info_span};
use tracing_subscriber::EnvFilter;

#[derive(Resource)]
struct WriterState {
    bootstrap: String,
    topic: String,
    dispatched: bool,
}

fn dual_writer_emitter(
    mut standard_writer: EventWriter<TestEvent>,
    mut bus_writer: EventBusWriter,
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
    standard_writer.write(bridge_event.clone());

    let bus_event = TestEvent {
        message: "event-bus-writer".to_string(),
        value: 2,
    };
    bus_writer.write(
        &kafka_producer_config(&state.bootstrap, [state.topic.clone()]),
        bus_event,
    );

    info!(topic = %state.topic, "Dispatched events via both writers");
}

#[derive(Resource)]
struct ReaderState {
    bootstrap: String,
    topic: String,
    consumer_group: String,
}

#[derive(Resource, Default)]
struct CapturedEvents(Vec<EventWrapper<TestEvent>>);

fn capture_wrapped_events(
    mut reader: EventBusReader<TestEvent>,
    state: Res<ReaderState>,
    mut captured: ResMut<CapturedEvents>,
) {
    for wrapper in reader.read(&kafka_consumer_config(
        &state.bootstrap,
        &state.consumer_group,
        [&state.topic],
    )) {
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

    let (backend_writer, bootstrap_w) = setup();
    let (backend_reader, bootstrap_r) = setup();

    let topic = unique_topic("dual-writer");

    // Writer app configuration
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(
        backend_writer,
        PreconfiguredTopics::new([topic.clone()]),
    ));
    writer_app.add_bus_event::<TestEvent>(&topic);
    writer_app.insert_resource(WriterState {
        bootstrap: bootstrap_w,
        topic: topic.clone(),
        dispatched: false,
    });
    writer_app.add_systems(Update, dual_writer_emitter);

    // Run a few frames to allow bridge system to observe the event queue and publish externally
    run_app_updates(&mut writer_app, 4);

    // Reader app configuration
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(
        backend_reader,
        PreconfiguredTopics::new([topic.clone()]),
    ));
    reader_app.add_bus_event::<TestEvent>(&topic);
    reader_app.insert_resource(ReaderState {
        bootstrap: bootstrap_r,
        topic: topic.clone(),
        consumer_group: unique_consumer_group("dual-writer-group"),
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
