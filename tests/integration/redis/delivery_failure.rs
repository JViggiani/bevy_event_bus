use bevy::prelude::*;
use bevy_event_bus::config::redis::RedisProducerConfig;
use bevy_event_bus::{BusErrorCallback, BusErrorContext, BusErrorKind, EventBusPlugins, RedisMessageWriter};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{unique_topic, update_until};
use integration_tests::utils::redis_setup;
use std::sync::{Arc, Mutex};

#[test]
fn redis_producer_emits_delivery_failure_error_for_missing_stream() {
    let missing_stream = unique_topic("redis_delivery_failure_missing");

    // Provision no streams so writes target an unknown stream, forcing a delivery failure.
    let (backend, _ctx) = redis_setup::prepare_backend(|_| {}).expect("Redis backend setup");

    let mut app = App::new();
    app.add_plugins(EventBusPlugins(backend));

    // Ensure event type is registered for the test payload.
    app.add_message::<TestEvent>();

    // Allow writer to target an unprovisioned stream by removing topology validation.
    app.world_mut()
        .remove_resource::<bevy_event_bus::ProvisionedTopology>();

    let errors: Arc<Mutex<Vec<BusErrorContext>>> = Arc::new(Mutex::new(Vec::new()));
    let callback: BusErrorCallback = {
        let sink: Arc<Mutex<Vec<BusErrorContext>>> = Arc::clone(&errors);
        std::sync::Arc::new(move |ctx| {
            sink.lock().unwrap().push(ctx);
        })
    };

    let stream_for_writer = missing_stream.clone();
    app.add_systems(
        Update,
        move |mut writer: RedisMessageWriter, mut fired: Local<bool>| {
            if !*fired {
                *fired = true;
                let config = RedisProducerConfig::new(stream_for_writer.clone());
                writer.write(
                    &config,
                    TestEvent {
                        message: "should-fail".to_string(),
                        value: 1,
                    },
                    Some(callback.clone()),
                );
                let _ = writer.flush();
            }
        },
    );

    // Drive the app until we observe a delivery failure error.
    let (found, _frames) = update_until(&mut app, 10_000, |_app| {
        errors
            .lock()
            .unwrap()
            .iter()
            .any(|err| err.kind == BusErrorKind::DeliveryFailure)
    });

    assert!(found, "Expected delivery failure error to be emitted");
    assert!(
        errors
            .lock()
            .unwrap()
            .iter()
            .any(|err| err.kind == BusErrorKind::DeliveryFailure),
        "Delivery failure should be classified correctly",
    );
    assert!(
        errors
            .lock()
            .unwrap()
            .iter()
            .any(|err| err.topic == missing_stream),
        "Error should reference the missing stream",
    );
}
