use std::time::Duration;

use bevy::prelude::*;
use bevy_event_bus::config::kafka::KafkaProducerConfig;
use bevy_event_bus::{BusErrorCallback, BusErrorContext, BusErrorKind, EventBusPlugins, KafkaMessageWriter};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{unique_topic, update_until};
use integration_tests::utils::kafka_setup::{self, SetupOptions};
use std::sync::{Arc, Mutex};

#[test]
fn kafka_producer_emits_delivery_failure_error_for_missing_topic() {
    let missing_topic = unique_topic("kafka_delivery_failure_missing");

    // Provision no topics so the broker rejects production to the missing topic.
    let options = SetupOptions::earliest()
        .insert_connection_config("allow.auto.create.topics", "false")
        .insert_connection_config("bootstrap.servers", "127.0.0.1:65531")
        .insert_connection_config("message.timeout.ms", "1000");
    let (backend, _bootstrap) =
        kafka_setup::prepare_backend(kafka_setup::build_request(options, |_| {}));

    let mut app = App::new();
    app.add_plugins(EventBusPlugins(backend));

    // Ensure event type is registered for the test payload.
    app.add_message::<TestEvent>();

    // Allow writer to target an unprovisioned topic by removing topology validation.
    app.world_mut()
        .remove_resource::<bevy_event_bus::ProvisionedTopology>();

    let errors: Arc<Mutex<Vec<BusErrorContext>>> = Arc::new(Mutex::new(Vec::new()));
    let callback: BusErrorCallback = {
        let sink: Arc<Mutex<Vec<BusErrorContext>>> = Arc::clone(&errors);
        std::sync::Arc::new(move |ctx| {
            sink.lock().unwrap().push(ctx);
        })
    };

    let topic_for_writer = missing_topic.clone();
    app.add_systems(
        Update,
        move |mut writer: KafkaMessageWriter, mut fired: Local<bool>| {
            if !*fired {
                *fired = true;
                let config = KafkaProducerConfig::new([topic_for_writer.clone()]);
                writer.write(
                    &config,
                    TestEvent {
                        message: "should-fail".to_string(),
                        value: 1,
                    },
                    Some(callback.clone()),
                );
                let _ = writer.flush(Duration::from_secs(5));
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
            .any(|err| err.topic == missing_topic),
        "Error should reference the missing topic",
    );
}
