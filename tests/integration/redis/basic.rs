#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisBackendConfig, RedisConnectionConfig, RedisConsumerConfig, RedisConsumerGroupSpec,
    RedisProducerConfig, RedisStreamSpec, RedisTopologyBuilder,
};
use bevy_event_bus::{
    EventBusBackend, EventBusPlugins, RedisEventBusBackend, RedisMessageReader, RedisMessageWriter,
    TopologyMode, block_on,
};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_consumer_group_member, unique_topic,
    update_two_apps_until, update_until,
};
use integration_tests::utils::redis_setup;
use std::time::Duration;

#[test]
fn redis_single_direction_writer_reader_flow() {
    let stream = unique_topic("redis-basic-writer-reader");
    let consumer_group = unique_consumer_group("redis_basic_reader");
    let consumer_name = unique_consumer_group_member(&consumer_group);

    let stream_for_reader = stream.clone();
    let group_for_reader = consumer_group.clone();
    let name_for_reader = consumer_name.clone();
    let (backend_reader, _ctx_reader) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_reader.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_reader.clone()],
                group_for_reader.clone(),
                name_for_reader.clone(),
            ))
            .add_event_single::<TestEvent>(stream_for_reader.clone());
    })
    .expect("reader backend setup");

    let stream_for_writer = stream.clone();
    let (backend_writer, _ctx_writer) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_writer.clone()))
            .add_event_single::<TestEvent>(stream_for_writer.clone());
    })
    .expect("writer backend setup");

    // Reader app
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(backend_reader));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader_app.insert_resource(Collected::default());
    #[derive(Resource, Clone)]
    struct Stream(String);
    #[derive(Resource, Clone)]
    struct ConsumerGroup(String);
    reader_app.insert_resource(Stream(stream.clone()));
    reader_app.insert_resource(ConsumerGroup(consumer_group.clone()));

    fn reader_system(
        mut reader: RedisMessageReader<TestEvent>,
        stream: Res<Stream>,
        group: Res<ConsumerGroup>,
        mut collected: ResMut<Collected>,
    ) {
        let config = RedisConsumerConfig::new(group.0.clone(), [stream.0.clone()]);
        for wrapper in reader.read(&config) {
            collected.0.push(wrapper.event().clone());
        }
    }
    reader_app.add_systems(Update, reader_system);

    // Writer app
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(backend_writer));

    #[derive(Resource, Clone)]
    struct Outgoing(TestEvent, String);

    let event_to_send = TestEvent {
        message: "From Redis Writer".into(),
        value: 100,
    };
    writer_app.insert_resource(Outgoing(event_to_send.clone(), stream.clone()));

    fn writer_system(mut writer: RedisMessageWriter, data: Res<Outgoing>, mut sent: Local<bool>) {
        if *sent {
            return;
        }
        let config = RedisProducerConfig::new(data.1.clone());
        writer.write(&config, data.0.clone(), None);
        *sent = true;
    }
    writer_app.add_systems(Update, writer_system);

    // Run a few frames to ensure the message is dispatched before polling the reader.
    run_app_updates(&mut writer_app, 20);

    let expected_event = event_to_send.clone();
    let (received, _) = update_until(&mut reader_app, 5_000, move |app| {
        let collected = app.world().resource::<Collected>();
        collected.0.iter().any(|event| event == &expected_event)
    });

    assert!(received, "Expected reader to observe the writer's event");
}

/// Ensures validation mode fails when the configured Redis stream is absent.
#[test]
fn redis_stream_validation_detects_missing_stream() {
    let stream = unique_topic("redis_validate_missing_stream");
    let (_noop_backend, ctx) = redis_setup::prepare_backend(|_| {}).expect("setup backend");
    drop(_noop_backend);

    let connection_string = ctx.connection_string().to_string();
    let connection = RedisConnectionConfig::new(connection_string.clone());

    let mut builder = RedisTopologyBuilder::default();
    builder.add_stream(RedisStreamSpec::new(stream.clone()).mode(TopologyMode::Validate));

    let config = RedisBackendConfig::new(connection, builder.build(), Duration::from_millis(25));
    let err = RedisEventBusBackend::new(config)
        .expect_err("backend initialization should fail when stream is missing");
    assert_eq!(err.backend(), "redis");
    assert!(
        err.reason().contains("Redis stream") && err.reason().contains("TopologyMode::Validate"),
        "unexpected error message: {}",
        err.reason()
    );
}

/// Ensures validation mode succeeds when the Redis stream already exists.
#[test]
fn redis_stream_validation_allows_existing_stream() {
    let stream = unique_topic("redis_validate_existing_stream");
    let (_noop_backend, ctx) = redis_setup::prepare_backend(|_| {}).expect("setup backend");
    drop(_noop_backend);

    let connection_string = ctx.connection_string().to_string();
    let client = redis::Client::open(connection_string.as_str()).expect("redis client");
    let mut conn = client.get_connection().expect("redis connection");
    redis::cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("seed")
        .arg("value")
        .query::<redis::Value>(&mut conn)
        .expect("seed stream");
    drop(conn);

    let connection = RedisConnectionConfig::new(connection_string.clone());
    let mut builder = RedisTopologyBuilder::default();
    builder.add_stream(RedisStreamSpec::new(stream.clone()).mode(TopologyMode::Validate));

    let config = RedisBackendConfig::new(connection, builder.build(), Duration::from_millis(25));
    let mut backend = RedisEventBusBackend::new(config).expect("backend init");

    let connected = block_on(backend.connect());
    assert!(connected, "Redis backend should connect when stream exists");
    let _ = block_on(backend.disconnect());
}

/// Ensures validation mode fails when the consumer group is missing.
#[test]
fn redis_consumer_group_validation_detects_missing_group() {
    let stream = unique_topic("redis_validate_missing_group_stream");
    let consumer_group = unique_consumer_group("redis_validate_missing_group");
    let consumer_name = unique_consumer_group_member(&consumer_group);

    let (_noop_backend, ctx) = redis_setup::prepare_backend(|_| {}).expect("setup backend");
    drop(_noop_backend);

    let connection_string = ctx.connection_string().to_string();
    let client = redis::Client::open(connection_string.as_str()).expect("redis client");
    let mut conn = client.get_connection().expect("redis connection");
    redis::cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("seed")
        .arg("value")
        .query::<redis::Value>(&mut conn)
        .expect("seed stream");
    drop(conn);

    let connection = RedisConnectionConfig::new(connection_string.clone());
    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()).mode(TopologyMode::Validate))
        .add_consumer_group(
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone(), consumer_name)
                .mode(TopologyMode::Validate),
        );

    let config = RedisBackendConfig::new(connection, builder.build(), Duration::from_millis(25));
    let err = RedisEventBusBackend::new(config)
        .expect_err("backend initialization should fail when consumer group is missing");
    assert_eq!(err.backend(), "redis");
    assert!(
        err.reason().contains("consumer group") && err.reason().contains("TopologyMode::Validate"),
        "unexpected error message: {}",
        err.reason()
    );
}

/// Ensures validation mode succeeds when the consumer group already exists.
#[test]
fn redis_consumer_group_validation_allows_existing_group() {
    let stream = unique_topic("redis_validate_existing_group_stream");
    let consumer_group = unique_consumer_group("redis_validate_existing_group");
    let consumer_name = unique_consumer_group_member(&consumer_group);

    let (_noop_backend, ctx) = redis_setup::prepare_backend(|_| {}).expect("setup backend");
    drop(_noop_backend);

    let connection_string = ctx.connection_string().to_string();
    let client = redis::Client::open(connection_string.as_str()).expect("redis client");
    let mut conn = client.get_connection().expect("redis connection");
    redis::cmd("XADD")
        .arg(&stream)
        .arg("*")
        .arg("seed")
        .arg("value")
        .query::<redis::Value>(&mut conn)
        .expect("seed stream");
    redis::cmd("XGROUP")
        .arg("CREATE")
        .arg(&stream)
        .arg(&consumer_group)
        .arg("$")
        .query::<redis::Value>(&mut conn)
        .expect("create consumer group");
    drop(conn);

    let connection = RedisConnectionConfig::new(connection_string.clone());
    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()).mode(TopologyMode::Validate))
        .add_consumer_group(
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone(), consumer_name)
                .mode(TopologyMode::Validate),
        );

    let config = RedisBackendConfig::new(connection, builder.build(), Duration::from_millis(25));
    let mut backend = RedisEventBusBackend::new(config).expect("backend init");

    let connected = block_on(backend.connect());
    assert!(
        connected,
        "Redis backend should connect when consumer group exists"
    );
    let _ = block_on(backend.disconnect());
}

#[test]
fn redis_bidirectional_apps_exchange_events() {
    let stream = unique_topic("redis-bidirectional");
    let group_a = unique_consumer_group("redis_app_a");
    let group_b = unique_consumer_group("redis_app_b");
    let consumer_a = unique_consumer_group_member(&group_a);
    let consumer_b = unique_consumer_group_member(&group_b);

    let stream_for_app_a = stream.clone();
    let group_for_app_a = group_a.clone();
    let consumer_for_app_a = consumer_a.clone();
    let (backend_a, _ctx_a) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_app_a.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_app_a.clone()],
                group_for_app_a.clone(),
                consumer_for_app_a.clone(),
            ))
            .add_event_single::<TestEvent>(stream_for_app_a.clone());
    })
    .expect("app A backend setup");

    let stream_for_app_b = stream.clone();
    let group_for_app_b = group_b.clone();
    let consumer_for_app_b = consumer_b.clone();
    let (backend_b, _ctx_b) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_app_b.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_app_b.clone()],
                group_for_app_b.clone(),
                consumer_for_app_b.clone(),
            ))
            .add_event_single::<TestEvent>(stream_for_app_b.clone());
    })
    .expect("app B backend setup");

    #[derive(Resource, Default)]
    struct Received(Vec<TestEvent>);

    #[derive(Resource, Clone)]
    struct StreamName(String);

    #[derive(Resource, Clone)]
    struct GroupName(String);

    #[derive(Resource, Clone)]
    struct OutgoingEvents {
        stream: String,
        events: Vec<TestEvent>,
        sent: bool,
    }

    fn reader_system(
        mut reader: RedisMessageReader<TestEvent>,
        stream: Res<StreamName>,
        group: Res<GroupName>,
        mut received: ResMut<Received>,
    ) {
        let config = RedisConsumerConfig::new(group.0.clone(), [stream.0.clone()]);
        for wrapper in reader.read(&config) {
            received.0.push(wrapper.event().clone());
        }
    }

    fn writer_system(mut writer: RedisMessageWriter, mut outgoing: ResMut<OutgoingEvents>) {
        if outgoing.sent {
            return;
        }
        let config = RedisProducerConfig::new(outgoing.stream.clone());
        for event in outgoing.events.clone() {
            writer.write(&config, event, None);
        }
        outgoing.sent = true;
    }

    let mut app_a = App::new();
    app_a.add_plugins(EventBusPlugins(backend_a));
    let event_from_a = TestEvent {
        message: "event-from-app-a".into(),
        value: 1,
    };
    app_a.insert_resource(Received::default());
    app_a.insert_resource(StreamName(stream.clone()));
    app_a.insert_resource(GroupName(group_a));
    let _ = consumer_a;
    app_a.insert_resource(OutgoingEvents {
        stream: stream.clone(),
        events: vec![event_from_a.clone()],
        sent: false,
    });
    app_a.add_systems(Update, (reader_system, writer_system));

    let mut app_b = App::new();
    app_b.add_plugins(EventBusPlugins(backend_b));
    let event_from_b = TestEvent {
        message: "event-from-app-b".into(),
        value: 2,
    };
    app_b.insert_resource(Received::default());
    app_b.insert_resource(StreamName(stream.clone()));
    app_b.insert_resource(GroupName(group_b));
    let _ = consumer_b;
    app_b.insert_resource(OutgoingEvents {
        stream,
        events: vec![event_from_b.clone()],
        sent: false,
    });
    app_b.add_systems(Update, (reader_system, writer_system));

    let expected_events = [event_from_a.clone(), event_from_b.clone()];

    let (success, _) = update_two_apps_until(&mut app_a, &mut app_b, 8_000, |app_a, app_b| {
        let app_a_has_all = {
            let world_a = app_a.world();
            let received_a = world_a.resource::<Received>();
            expected_events
                .iter()
                .all(|event| received_a.0.iter().any(|seen| seen == event))
        };

        let app_b_has_all = {
            let world_b = app_b.world();
            let received_b = world_b.resource::<Received>();
            expected_events
                .iter()
                .all(|event| received_b.0.iter().any(|seen| seen == event))
        };

        app_a_has_all && app_b_has_all
    });

    assert!(success, "Both apps should observe each other's events");
}
