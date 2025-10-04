#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{RedisConsumerConfig, RedisProducerConfig};
use bevy_event_bus::{EventBusErrorQueue, EventBusPlugins, RedisAckWorkerStats, RedisEventReader, RedisEventWriter};
use integration_tests::common::backend_factory::{setup_backend, BackendVariant, TestBackend};
use integration_tests::common::helpers::{run_app_updates, update_until};
use integration_tests::common::TestEvent;

#[derive(Resource, Clone)]
struct Topic(String);

#[derive(Resource, Clone)]
struct ConsumerGroup(Option<String>);

#[derive(Resource, Default)]
struct Received {
    events: Vec<TestEvent>,
    ack_attempts: usize,
}

#[derive(Resource)]
struct WriterPayload {
    event: TestEvent,
    topic: String,
    dispatched: bool,
}

fn redis_writer_emit_once(mut writer: RedisEventWriter, mut payload: ResMut<WriterPayload>) {
    if payload.dispatched {
        return;
    }

    let config = RedisProducerConfig::new(payload.topic.clone());
    writer.write(&config, payload.event.clone());
    payload.dispatched = true;
}

fn redis_reader_ack_system(
    mut reader: RedisEventReader<TestEvent>,
    topic: Res<Topic>,
    group: Res<ConsumerGroup>,
    mut received: ResMut<Received>,
) {
    let mut config = RedisConsumerConfig::new(topic.0.clone());
    if let Some(group_id) = group.0.as_ref() {
        config = config.set_consumer_group(group_id.clone());
    }

    for wrapper in reader.read(&config) {
        reader
            .acknowledge(&wrapper)
            .expect("acknowledgement should succeed");
        received.events.push(wrapper.event().clone());
        received.ack_attempts += 1;
    }
}

#[test]
fn manual_ack_clears_messages_and_tracks_success() {
    let (handle, context) = setup_backend(TestBackend::Redis).expect("redis backend setup");

    let BackendVariant::Redis(writer_backend) = handle.writer_backend else {
        panic!("expected redis writer backend");
    };
    let BackendVariant::Redis(reader_backend) = handle.reader_backend else {
        panic!("expected redis reader backend");
    };

    let topic = handle.topic.clone();
    let consumer_group = handle
        .consumer_group
        .clone()
        .expect("redis backend should include a consumer group");

    // Reader app drains events and acknowledges them immediately. Install the backend here first
    // so that the message stream and manual acknowledgement resources bind to the reader app.
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(reader_backend));
    reader_app.insert_resource(Topic(topic.clone()));
    reader_app.insert_resource(ConsumerGroup(Some(consumer_group)));
    reader_app.insert_resource(Received::default());
    reader_app.add_systems(Update, redis_reader_ack_system);

    // Writer app dispatches a single event once. Installing the backend after the reader ensures
    // writer-only responsibilities do not steal the shared message queue.
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(writer_backend));
    writer_app.insert_resource(WriterPayload {
        event: TestEvent {
            message: "redis-manual-ack".to_string(),
            value: 7,
        },
    topic: topic.clone(),
        dispatched: false,
    });
    writer_app.add_systems(Update, redis_writer_emit_once);
    run_app_updates(&mut writer_app, 2);

    let pending_errors = {
        let queue = writer_app.world_mut().resource::<EventBusErrorQueue>();
        queue.drain_pending()
    };
    assert!(
        pending_errors.is_empty(),
        "redis writer emitted {} errors",
        pending_errors.len()
    );

    // Spin frames until we receive at least one event, then allow a few extra frames for acking.
    let (received_any, _frames) = update_until(&mut reader_app, 7_000, |app| {
        !app.world().resource::<Received>().events.is_empty()
    });
    assert!(received_any, "expected to receive at least one redis event");
    run_app_updates(&mut reader_app, 4);

    // Verify acknowledgement statistics are recorded.
    let (acked, _) = update_until(&mut reader_app, 7_000, |app| {
        let stats = app
            .world()
            .get_resource::<RedisAckWorkerStats>()
            .expect("ack worker stats should be available");
        stats.acknowledged() >= 1
    });
    assert!(acked, "expected redis ack worker to record a successful acknowledgement");
    let stats = reader_app
        .world()
        .get_resource::<RedisAckWorkerStats>()
        .expect("ack worker stats should be available");
    assert_eq!(stats.failed(), 0);

    // Ensure the event was only received once.
    let received = reader_app.world().resource::<Received>();
    assert_eq!(received.events.len(), 1);
    assert_eq!(received.ack_attempts, 1);

    drop(context);
}
