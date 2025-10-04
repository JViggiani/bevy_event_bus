use bevy::ecs::system::RunSystemOnce;
use bevy::prelude::*;
use bevy_event_bus::backends::EventBusBackendResource;
use bevy_event_bus::config::kafka::{KafkaProducerConfig, KafkaTopologyEventBinding};
use bevy_event_bus::{
    EventBusError, EventBusErrorQueue, EventBusErrorType, EventBusPlugin, EventBusPlugins,
    KafkaEventWriter,
};
use integration_tests::common::events::TestEvent;
use integration_tests::common::helpers::unique_topic;
use integration_tests::common::mock_backend::MockEventBusBackend;

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
        move |mut writer: KafkaEventWriter, mut fired: Local<bool>| {
            if !*fired {
                *fired = true;
                let config = KafkaProducerConfig::new([topic_for_writer.clone()]);
                writer.write(
                    &config,
                    TestEvent {
                        message: "no-internal-bridge".to_string(),
                        value: 7,
                    },
                );
            }
        },
    );

    app.add_systems(
        Update,
        |mut reader: EventReader<TestEvent>, mut seen: ResMut<InternalSeen>| {
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
        "KafkaEventWriter should not emit additional Bevy events for the same payload",
    );
}

#[test]
fn writer_queues_not_configured_error_when_backend_missing() {
    let mut app = App::new();

    app.add_plugins(EventBusPlugin);
    app.insert_resource(EventBusErrorQueue::default());

    // Register events/results needed for the writer and error queue
    app.add_event::<TestEvent>();
    app.add_event::<EventBusError<TestEvent>>();

    // Remove any backend resource to simulate a missing backend scenario
    app.world_mut().remove_resource::<EventBusBackendResource>();

    // Drive the writer once
    app.world_mut()
        .run_system_once(|mut writer: KafkaEventWriter| {
            let config = KafkaProducerConfig::new([unique_topic("missing_backend")]);
            writer.write(
                &config,
                TestEvent {
                    message: "should-error".to_string(),
                    value: 99,
                },
            );
        })
        .expect("writer system should run without errors");

    // Flush queued error events into the world
    let pending = {
        let queue = app.world().resource::<EventBusErrorQueue>();
        queue.drain_pending()
    };
    for callback in pending {
        callback(app.world_mut());
    }

    // Capture emitted errors
    let errors: Vec<EventBusError<TestEvent>> = app
        .world_mut()
        .run_system_once(|mut reader: EventReader<EventBusError<TestEvent>>| {
            reader.read().cloned().collect()
        })
        .expect("error reader should run");

    assert_eq!(errors.len(), 1, "Exactly one error should be emitted");
    let error = &errors[0];
    assert_eq!(error.error_type, EventBusErrorType::NotConfigured);
    assert_eq!(
        error.error_message, "No event bus backend configured",
        "Error message should explain the missing backend",
    );
}
