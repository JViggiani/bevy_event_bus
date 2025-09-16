use crate::common::TestEvent;
use crate::common::setup::setup;
use crate::common::helpers::update_until;
use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusReader, EventBusWriter};
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
    let topic = format!("bevy-event-bus-test-{}", uuid_suffix());
    info!(
        setup_total_ms = setup_start.elapsed().as_millis(),
        "Setup complete"
    );

    // Broker is assured ready by setup(); proceed.

    // Create separate backends for writer and reader to simulate separate machines
    let (backend_writer, _bootstrap_writer) = setup();
    let (backend_reader, _bootstrap_reader) = setup();

    // Writer app
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(
        backend_writer,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));

    #[derive(Resource, Clone)]
    struct ToSend(TestEvent, String);

    let event_to_send = TestEvent {
        message: "From Bevy!".into(),
        value: 100,
    };
    writer_app.insert_resource(ToSend(event_to_send.clone(), topic.clone()));

    fn writer_system(mut w: EventBusWriter<TestEvent>, data: Res<ToSend>) {
        let _ = w.send(&data.1, data.0.clone());
    }
    writer_app.add_systems(Update, writer_system);
    let writer_span = info_span!("writer_app.update");
    {
        let _wg = writer_span.enter();
        writer_app.update();
    }

    // Instead of fixed sleep loop we'll actively poll the reader app

    // Reader app (separate consumer group with separate backend)
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(
        backend_reader,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader_app.insert_resource(Collected::default());
    #[derive(Resource, Clone)]
    struct Topic(String);
    reader_app.insert_resource(Topic(topic.clone()));

    fn reader_system(
        mut r: EventBusReader<TestEvent>,
        topic: Res<Topic>,
        mut collected: ResMut<Collected>,
    ) {
        for e in r.try_read(&topic.0) {
            collected.0.push(e.clone());
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

fn uuid_suffix() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("{}", nanos)
}
