use crate::common::TestEvent;
use crate::common::setup::{setup, build_app};
use crate::common::helpers::{unique_topic, unique_consumer_group, update_until};
use bevy::prelude::*;
use bevy_event_bus::{EventBusReader, EventBusWriter, EventBusAppExt, KafkaReadConfig, KafkaWriteConfig};
use bevy_event_bus::config::OffsetReset;
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
    info!(
        setup_total_ms = setup_start.elapsed().as_millis(),
        "Setup complete"
    );

    // Broker is assured ready by setup(None); proceed.

    // Create backend for writer - reader uses separate backend via build_app_with_consumer_group
    let (backend_writer, _bootstrap_writer) = setup(None);

    // Writer app
    let mut writer_app = crate::common::setup::build_app(backend_writer, None, |app| {
        // Register bus event to enable EventBusWriter error handling
        app.add_bus_event::<TestEvent>(&topic);
    });

    #[derive(Resource, Clone)]
    struct ToSend(TestEvent, String);

    let event_to_send = TestEvent {
        message: "From Bevy!".into(),
        value: 100,
    };
    writer_app.insert_resource(ToSend(event_to_send.clone(), topic.clone()));

    fn writer_system(mut w: EventBusWriter<TestEvent>, data: Res<ToSend>) {
        let config = KafkaWriteConfig::new(&data.1);
        let _ = w.write(&config, data.0.clone());
        let _ = w.flush(&config, std::time::Duration::from_secs(5)); // Ensure message is actually sent to Kafka
    }
    writer_app.add_systems(Update, writer_system);
    let writer_span = info_span!("writer_app.update");
    {
        let _wg = writer_span.enter();
        writer_app.update();
    }

    // Instead of fixed sleep loop we'll actively poll the reader app

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);

    // Reader app with pre-created consumer group
    let consumer_group = unique_consumer_group("test_group");
    let config = KafkaReadConfig::new(&consumer_group)
        .topics([&topic])
        .offset_reset(OffsetReset::Earliest);
    
    let (backend_reader, _bootstrap_reader) = setup(None);
    let mut reader_app = build_app(backend_reader, Some(&[config.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic);
        app.insert_resource(Collected::default());
        
        app.add_systems(Update, move |mut r: EventBusReader<TestEvent>, mut collected: ResMut<Collected>| {
            // Use the SAME config that was passed to setup, not recreated
            for wrapper in r.read(&config) {
                collected.0.push(wrapper.event().clone());
            }
        });
    });

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
