use bevy::ecs::system::RunSystemOnce;
use bevy::prelude::*;
use bevy_event_bus::backends::EventBusBackendResource;
use bevy_event_bus::config::kafka::{KafkaProducerConfig, KafkaTopologyEventBinding};
use bevy_event_bus::{BusErrorCallback, BusErrorContext, BusErrorKind, EventBusPlugin, EventBusPlugins, KafkaMessageWriter};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::unique_topic;
use integration_tests::utils::mock_backend::MockEventBusBackend;
use std::sync::{Arc, Mutex};

#[derive(Resource, Default)]
struct InternalSeen(usize);

#[test]
fn writer_does_not_emit_bevy_events() {
    let mut app = App::new();

    app.add_plugins(EventBusPlugins(MockEventBusBackend::new()));

    let topic = unique_topic("external_only");
    KafkaTopologyEventBinding::new::<TestEvent>(vec![topic.clone()]).apply(&mut app);
    app.insert_resource(InternalSeen::default());

    let topic_for_writer = topic.clone();
    app.add_systems(
        Update,
        move |mut writer: KafkaMessageWriter, mut fired: Local<bool>| {
            if !*fired {
                *fired = true;
                let config = KafkaProducerConfig::new([topic_for_writer.clone()]);
                writer.write(
                    &config,
                    TestEvent {
                        message: "no-internal-bridge".to_string(),
                        value: 7,
                    },
                    None,
                );
            }
        },
    );

    app.add_systems(
        Update,
        |mut reader: MessageReader<TestEvent>, mut seen: ResMut<InternalSeen>| {
            seen.0 += reader.read().count();
        },
    );

    // Run a few frames to allow systems to execute
    for _ in 0..5 {
        app.update();
    }

    let seen = app.world().resource::<InternalSeen>();
    assert_eq!(
        seen.0, 0,
        "KafkaMessageWriter should not emit additional Bevy events for the same payload",
    );
}

#[test]
fn writer_queues_not_configured_error_when_backend_missing() {
    let mut app = App::new();

    app.add_plugins(EventBusPlugin);

    // Register events/results needed for the writer
    app.add_message::<TestEvent>();

    // Remove any backend resource to simulate a missing backend scenario
    app.world_mut().remove_resource::<EventBusBackendResource>();

    let errors: Arc<Mutex<Vec<BusErrorContext>>> = Arc::new(Mutex::new(Vec::new()));
    let callback: BusErrorCallback = {
        let sink: Arc<Mutex<Vec<BusErrorContext>>> = Arc::clone(&errors);
        std::sync::Arc::new(move |ctx| {
            sink.lock().unwrap().push(ctx);
        })
    };

    // Drive the writer once
    app.world_mut()
        .run_system_once(move |mut writer: KafkaMessageWriter| {
            let config = KafkaProducerConfig::new([unique_topic("missing_backend")]);
            writer.write(
                &config,
                TestEvent {
                    message: "should-error".to_string(),
                    value: 99,
                },
                Some(callback.clone()),
            );
        })
        .expect("writer system should run without errors");

    let errors = errors.lock().unwrap();
    assert_eq!(errors.len(), 1, "Exactly one error should be emitted");
    let error = &errors[0];
    assert_eq!(error.kind, BusErrorKind::NotConfigured);
    assert_eq!(
        error.message, "No event bus backend configured",
        "Error message should explain the missing backend",
    );
}
