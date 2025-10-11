#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, EventWrapper, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group_membership, unique_topic, update_until, wait_for_events,
};
use integration_tests::utils::redis_setup;

#[derive(Resource)]
struct BridgeWriterState {
    stream: String,
    primed: bool,
    dispatched: bool,
    standard_payload: TestEvent,
    bus_payload: TestEvent,
}

#[derive(Resource)]
struct WriterDispatch {
    stream: String,
    payload: TestEvent,
    primed: bool,
    dispatched: bool,
}

#[derive(Resource)]
struct ReaderState {
    stream: String,
    consumer_group: String,
}

#[derive(Resource, Default)]
struct CapturedEvents(Vec<EventWrapper<TestEvent>>);

fn dual_writer_bridge_emitter(
    mut standard_writer: EventWriter<TestEvent>,
    mut bus_writer: RedisEventWriter,
    mut state: ResMut<BridgeWriterState>,
) {
    if state.dispatched {
        return;
    }

    if !state.primed {
        state.primed = true;
        return;
    }

    standard_writer.write(state.standard_payload.clone());

    let config = RedisProducerConfig::new(state.stream.clone());
    bus_writer.write(&config, state.bus_payload.clone());

    state.dispatched = true;
}

fn dispatch_single_backend_writer(mut writer: RedisEventWriter, mut state: ResMut<WriterDispatch>) {
    if state.dispatched {
        return;
    }

    if !state.primed {
        state.primed = true;
        return;
    }

    let config = RedisProducerConfig::new(state.stream.clone());
    writer.write(&config, state.payload.clone());
    state.dispatched = true;
}

fn capture_wrapped_events(
    mut reader: RedisEventReader<TestEvent>,
    state: Res<ReaderState>,
    mut captured: ResMut<CapturedEvents>,
) {
    let config = RedisConsumerConfig::new(state.consumer_group.clone(), [state.stream.clone()]);
    for wrapper in reader.read(&config) {
        captured.0.push(wrapper.clone());
    }
}

#[test]
fn external_bus_events_flow_from_both_writers() {
    let stream = unique_topic("dual-writer-bridge");
    let membership = unique_consumer_group_membership("dual-writer-bridge-group");

    let database_index =
        redis_setup::allocate_database().expect("Redis database allocated for bridge test");

    let writer_stream = stream.clone();
    let (writer_backend, _writer_ctx) = redis_setup::with_database(database_index, || {
        redis_setup::prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer_stream.clone()))
                .add_event_single::<TestEvent>(writer_stream.clone());
        })
    })
    .expect("Redis writer backend setup successful");

    let reader_stream = stream.clone();
    let reader_group = membership.group.clone();
    let reader_consumer = membership.member.clone();
    let (reader_backend, _reader_ctx) = redis_setup::with_database(database_index, || {
        redis_setup::prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader_stream.clone()))
                .add_consumer_group(RedisConsumerGroupSpec::new(
                    [reader_stream.clone()],
                    reader_group.clone(),
                    reader_consumer.clone(),
                ))
                .add_event_single::<TestEvent>(reader_stream.clone());
        })
    })
    .expect("Redis reader backend setup successful");

    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(writer_backend));
    writer_app.insert_resource(BridgeWriterState {
        stream: stream.clone(),
        primed: false,
        dispatched: false,
        standard_payload: TestEvent {
            message: "standard-writer".to_string(),
            value: 1,
        },
        bus_payload: TestEvent {
            message: "event-bus-writer".to_string(),
            value: 2,
        },
    });
    writer_app.add_systems(Update, dual_writer_bridge_emitter);

    run_app_updates(&mut writer_app, 4);

    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(reader_backend));
    reader_app.insert_resource(ReaderState {
        stream: stream.clone(),
        consumer_group: membership.group.clone(),
    });
    reader_app.insert_resource(CapturedEvents::default());
    reader_app.add_systems(Update, capture_wrapped_events);

    let received = wait_for_events(&mut reader_app, &stream, 12_000, 2, |app| {
        app.world().resource::<CapturedEvents>().0.clone()
    });

    assert_eq!(
        received.len(),
        2,
        "Exactly two events should arrive – one per writer path"
    );

    let mut messages: Vec<&str> = received
        .iter()
        .map(|wrapper| wrapper.message.as_str())
        .collect();
    messages.sort();
    assert_eq!(messages, vec!["event-bus-writer", "standard-writer"]);

    for wrapper in &received {
        let metadata = wrapper.metadata();
        assert_eq!(metadata.source.as_str(), stream.as_str());
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

/// Test that multiple writer applications with separate backends operate independently
/// (they cannot communicate with each other as they use separate Redis instances)
#[test]
fn external_bus_events_independent_operation() {
    let stream = unique_topic("dual-writer");
    let membership = unique_consumer_group_membership("dual-writer-group");
    let consumer_group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let writer1_db =
        redis_setup::allocate_database().expect("Writer1 Redis backend setup successful");
    let writer2_db =
        redis_setup::allocate_database().expect("Writer2 Redis backend setup successful");
    let reader_db =
        redis_setup::allocate_database().expect("Reader Redis backend setup successful");

    let writer1_stream = stream.clone();
    let (writer1_backend, _context1) = redis_setup::with_database(writer1_db, || {
        redis_setup::prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer1_stream.clone()))
                .add_event_single::<TestEvent>(writer1_stream.clone());
        })
    })
    .expect("Writer1 Redis backend setup successful");

    let writer2_stream = stream.clone();
    let (writer2_backend, _context2) = redis_setup::with_database(writer2_db, || {
        redis_setup::prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer2_stream.clone()))
                .add_event_single::<TestEvent>(writer2_stream.clone());
        })
    })
    .expect("Writer2 Redis backend setup successful");

    let reader_stream = stream.clone();
    let reader_group = consumer_group.clone();
    let reader_consumer = consumer_name.clone();
    let (reader_backend, _context3) = redis_setup::with_database(reader_db, || {
        redis_setup::prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader_stream.clone()))
                .add_consumer_group(RedisConsumerGroupSpec::new(
                    [reader_stream.clone()],
                    reader_group.clone(),
                    reader_consumer.clone(),
                ))
                .add_event_single::<TestEvent>(reader_stream.clone());
        })
    })
    .expect("Reader Redis backend setup successful");

    // Two separate writer apps with independent backends
    let mut writer1 = App::new();
    writer1.add_plugins(EventBusPlugins(writer1_backend));
    writer1.insert_resource(WriterDispatch {
        stream: stream.clone(),
        payload: TestEvent {
            message: "from_writer_1".to_string(),
            value: 100,
        },
        primed: false,
        dispatched: false,
    });
    writer1.add_systems(Update, dispatch_single_backend_writer);

    let mut writer2 = App::new();
    writer2.add_plugins(EventBusPlugins(writer2_backend));
    writer2.insert_resource(WriterDispatch {
        stream: stream.clone(),
        payload: TestEvent {
            message: "from_writer_2".to_string(),
            value: 200,
        },
        primed: false,
        dispatched: false,
    });
    writer2.add_systems(Update, dispatch_single_backend_writer);

    // Reader app with its own backend
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));
    reader.insert_resource(ReaderState {
        stream: stream.clone(),
        consumer_group: consumer_group.clone(),
    });
    reader.insert_resource(CapturedEvents::default());
    reader.add_systems(Update, capture_wrapped_events);

    // Start reader first to ensure consumer group is ready
    run_app_updates(&mut reader, 4);

    // Then dispatch events via both writers
    run_app_updates(&mut writer1, 4);
    run_app_updates(&mut writer2, 4);

    // With separate backends, reader cannot receive events from writers (different Redis instances)
    let (received, _) = update_until(&mut reader, 1_000, |app| {
        let collected = app.world().resource::<CapturedEvents>();
        !collected.0.is_empty()
    });

    assert!(
        !received,
        "Reader should not observe events when writers use independent backends"
    );

    // Verify the expected behavior with separate backends
    let collected = reader.world().resource::<CapturedEvents>();
    assert_eq!(
        collected.0.len(),
        0,
        "Reader should receive 0 events (separate Redis instances)"
    );

    // This test confirms that apps with separate backends operate independently
    // Writers can send events without errors, but readers don't see cross-backend events
}
