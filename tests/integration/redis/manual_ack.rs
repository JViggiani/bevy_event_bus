#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{
    EventBusErrorQueue, EventBusPlugins, RedisAckWorkerStats, RedisEventReader, RedisEventWriter,
};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_topic, update_until,
};
use integration_tests::utils::redis_setup;

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

#[derive(Resource)]
struct WriterBatchPayload {
    events: Vec<TestEvent>,
    topic: String,
    dispatched: usize,
}

fn redis_writer_emit_once(mut writer: RedisEventWriter, mut payload: ResMut<WriterPayload>) {
    if payload.dispatched {
        return;
    }

    let config = RedisProducerConfig::new(payload.topic.clone());
    writer.write(&config, payload.event.clone());
    payload.dispatched = true;
}

fn redis_writer_emit_batch(mut writer: RedisEventWriter, mut payload: ResMut<WriterBatchPayload>) {
    if payload.dispatched >= payload.events.len() {
        return;
    }

    let config = RedisProducerConfig::new(payload.topic.clone());
    let remaining = payload.events.len() - payload.dispatched;
    let batch = remaining.min(128);
    let start = payload.dispatched;
    let end = start + batch;

    for event in payload.events[start..end].iter().cloned() {
        writer.write(&config, event);
    }

    payload.dispatched = end;
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
    let stream = unique_topic("redis-manual-ack");
    let group = unique_consumer_group("redis-manual-ack-group");

    let shared_db = redis_setup::ensure_shared_redis().expect("Redis backend setup successful");

    let stream_clone = stream.clone();
    let group_clone = group.clone();
    let (backend, context) = shared_db.prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()))
            .add_consumer_group(
                group_clone.clone(),
                RedisConsumerGroupSpec::new([stream_clone.clone()], group_clone.clone())
                    .manual_ack(true),
            )
            .add_event_single::<TestEvent>(stream_clone.clone());
    })
    .expect("Redis backend setup successful");

    let writer_backend = backend.clone();
    let reader_backend = backend;

    let topic = stream;
    let consumer_group = group;

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
    assert!(
        acked,
        "expected redis ack worker to record a successful acknowledgement"
    );
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

#[test]
fn manual_ack_batches_multiple_messages() {
    const TOTAL_MESSAGES: usize = 64;

    let stream = unique_topic("redis-manual-ack-batch");
    let group = unique_consumer_group("redis-manual-ack-batch-group");

    let shared_db = redis_setup::ensure_shared_redis().expect("Redis backend setup successful");

    let stream_clone = stream.clone();
    let group_clone = group.clone();
    let (backend, context) = shared_db.prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()))
            .add_consumer_group(
                group_clone.clone(),
                RedisConsumerGroupSpec::new([stream_clone.clone()], group_clone.clone())
                    .manual_ack(true),
            )
            .add_event_single::<TestEvent>(stream_clone.clone());
    })
    .expect("Redis backend setup successful");

    let reader_backend = backend.clone();
    let writer_backend = backend;

    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(reader_backend));
    reader_app.insert_resource(Topic(stream.clone()));
    reader_app.insert_resource(ConsumerGroup(Some(group.clone())));
    reader_app.insert_resource(Received::default());
    reader_app.add_systems(Update, redis_reader_ack_system);

    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(writer_backend));
    writer_app.insert_resource(WriterBatchPayload {
        events: (0..TOTAL_MESSAGES)
            .map(|value| TestEvent {
                message: format!("redis-manual-ack-batch-{value}"),
                value: value as i32,
            })
            .collect(),
        topic: stream.clone(),
        dispatched: 0,
    });
    writer_app.add_systems(Update, redis_writer_emit_batch);

    while writer_app
        .world()
        .resource::<WriterBatchPayload>()
        .dispatched
        < TOTAL_MESSAGES
    {
        writer_app.update();
    }
    run_app_updates(&mut writer_app, 6);

    let pending_errors = {
        let queue = writer_app.world_mut().resource::<EventBusErrorQueue>();
        queue.drain_pending()
    };
    assert!(
        pending_errors.is_empty(),
        "redis writer emitted {} errors",
        pending_errors.len()
    );

    let (received_all, _) = update_until(&mut reader_app, 15_000, |app| {
        app.world().resource::<Received>().events.len() >= TOTAL_MESSAGES
    });
    assert!(
        received_all,
        "expected to receive all redis events for batch acknowledgement"
    );
    let (acks_complete, _) = update_until(&mut reader_app, 15_000, |app| {
        app.world()
            .get_resource::<RedisAckWorkerStats>()
            .map(|stats| stats.acknowledged() >= TOTAL_MESSAGES)
            .unwrap_or(false)
    });
    assert!(
        acks_complete,
        "expected redis ack worker to flush all batched acknowledgements"
    );

    let stats = reader_app
        .world()
        .get_resource::<RedisAckWorkerStats>()
        .expect("ack worker stats should be available");
    assert_eq!(stats.acknowledged(), TOTAL_MESSAGES);
    assert_eq!(stats.failed(), 0);

    let received = reader_app.world().resource::<Received>();
    assert_eq!(received.events.len(), TOTAL_MESSAGES);
    assert_eq!(received.ack_attempts, TOTAL_MESSAGES);

    drop(context);
}
