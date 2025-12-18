#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{
    EventBusPlugins, RedisAckWorkerStats, RedisMessageReader, RedisMessageWriter,
};
use bevy_event_bus::{BusErrorCallback, BusErrorContext};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    ConsumerGroupMembership, run_app_updates, unique_consumer_group_membership, unique_topic,
    update_until,
};
use integration_tests::utils::redis_setup;
use std::sync::{Arc, Mutex};

#[derive(Resource, Clone)]
struct Topic(String);

#[derive(Resource, Clone)]
struct ConsumerMembership(Option<ConsumerGroupMembership>);

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

#[derive(Resource, Clone)]
struct WriterErrorCallback(pub BusErrorCallback);

fn redis_writer_emit_once(
    mut writer: RedisMessageWriter,
    mut payload: ResMut<WriterPayload>,
    callback: Res<WriterErrorCallback>,
) {
    if payload.dispatched {
        return;
    }

    let config = RedisProducerConfig::new(payload.topic.clone());
    writer.write(&config, payload.event.clone(), Some(callback.0.clone()));
    payload.dispatched = true;
}

fn redis_writer_emit_batch(
    mut writer: RedisMessageWriter,
    mut payload: ResMut<WriterBatchPayload>,
    callback: Res<WriterErrorCallback>,
) {
    if payload.dispatched >= payload.events.len() {
        return;
    }

    let config = RedisProducerConfig::new(payload.topic.clone());
    let remaining = payload.events.len() - payload.dispatched;
    let batch = remaining.min(128);
    let start = payload.dispatched;
    let end = start + batch;

    for event in payload.events[start..end].iter().cloned() {
        writer.write(&config, event, Some(callback.0.clone()));
    }

    payload.dispatched = end;
}

fn redis_reader_ack_system(
    mut reader: RedisMessageReader<TestEvent>,
    topic: Res<Topic>,
    membership: Res<ConsumerMembership>,
    mut received: ResMut<Received>,
) {
    let config = match membership.0.as_ref() {
        Some(group_membership) => {
            RedisConsumerConfig::new(group_membership.group.clone(), [topic.0.clone()])
        }
        None => RedisConsumerConfig::ungrouped([topic.0.clone()]),
    };

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
    let membership = unique_consumer_group_membership("redis-manual-ack-group");
    let group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let stream_clone = stream.clone();
    let group_clone = group.clone();
    let consumer_clone = consumer_name.clone();
    let (backend, context) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()))
            .add_consumer_group(
                RedisConsumerGroupSpec::new(
                    [stream_clone.clone()],
                    group_clone.clone(),
                    consumer_clone.clone(),
                )
                .manual_ack(true),
            )
            .add_event_single::<TestEvent>(stream_clone.clone());
    })
    .expect("Redis backend setup successful");

    let writer_backend = backend.clone();
    let reader_backend = backend;

    let topic = stream;

    // Reader app drains events and acknowledges them immediately. Install the backend here first
    // so that the message stream and manual acknowledgement resources bind to the reader app.
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(reader_backend));
    reader_app.insert_resource(Topic(topic.clone()));
    reader_app.insert_resource(ConsumerMembership(Some(membership.clone())));
    reader_app.insert_resource(Received::default());
    reader_app.add_systems(Update, redis_reader_ack_system);

    // Writer app dispatches a single event once. Installing the backend after the reader ensures
    // writer-only responsibilities do not steal the shared message queue.
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(writer_backend));
    let writer_errors: Arc<Mutex<Vec<BusErrorContext>>> = Arc::new(Mutex::new(Vec::new()));
    let writer_callback: BusErrorCallback = {
        let sink: Arc<Mutex<Vec<BusErrorContext>>> = Arc::clone(&writer_errors);
        Arc::new(move |ctx| {
            sink.lock().unwrap().push(ctx);
        })
    };
    writer_app.insert_resource(WriterErrorCallback(writer_callback));
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

    assert!(
        writer_errors.lock().unwrap().is_empty(),
        "redis writer emitted errors"
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
    let membership = unique_consumer_group_membership("redis-manual-ack-batch-group");
    let group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let stream_clone = stream.clone();
    let group_clone = group.clone();
    let consumer_clone = consumer_name.clone();
    let (backend, context) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()))
            .add_consumer_group(
                RedisConsumerGroupSpec::new(
                    [stream_clone.clone()],
                    group_clone.clone(),
                    consumer_clone.clone(),
                )
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
    reader_app.insert_resource(ConsumerMembership(Some(membership.clone())));
    reader_app.insert_resource(Received::default());
    reader_app.add_systems(Update, redis_reader_ack_system);

    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(writer_backend));
    let writer_errors: Arc<Mutex<Vec<BusErrorContext>>> = Arc::new(Mutex::new(Vec::new()));
    let writer_callback: BusErrorCallback = {
        let sink: Arc<Mutex<Vec<BusErrorContext>>> = Arc::clone(&writer_errors);
        Arc::new(move |ctx| {
            sink.lock().unwrap().push(ctx);
        })
    };
    writer_app.insert_resource(WriterErrorCallback(writer_callback));
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

    assert!(
        writer_errors.lock().unwrap().is_empty(),
        "redis writer emitted errors"
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
