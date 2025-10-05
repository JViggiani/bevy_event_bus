#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{RedisStreamSpec, RedisTopologyBuilder, TrimStrategy};
use bevy_event_bus::{EventBusPlugins, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{self, unique_topic};
use integration_tests::utils::redis_setup;
use std::cell::RefCell;

#[derive(Resource)]
struct TrimRequest {
    stream: String,
    max_len: usize,
    strategy: TrimStrategy,
    executed: bool,
}

fn trim_stream_once(mut writer: RedisEventWriter, mut request: ResMut<TrimRequest>) {
    if request.executed {
        return;
    }

    writer
        .trim_stream(&request.stream, request.max_len, request.strategy)
        .expect("stream trim should succeed");
    request.executed = true;
}

#[test]
fn writer_trim_stream_enforces_max_length() {
    let stream = unique_topic("redis-stream-trim");

    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_event_single::<TestEvent>(stream.clone());

    let (backend, context) = redis_setup::setup(builder).expect("Redis backend setup successful");

    let writer_backend = backend;

    // Seed the stream with multiple entries using a raw Redis connection.
    let connection_string = context.connection_string().to_string();

    let client = redis::Client::open(connection_string.as_str())
        .expect("failed to open redis client for seeding");
    let mut conn = client
        .get_connection()
        .expect("failed to obtain redis connection");
    for value in 0..5 {
        let payload = serde_json::to_string(&TestEvent {
            message: format!("seed-{value}"),
            value,
        })
        .expect("serialize payload");
        redis::cmd("XADD")
            .arg(&stream)
            .arg("*")
            .arg("payload")
            .arg(&payload)
            .query::<()>(&mut conn)
            .expect("seed redis stream");
    }

    // Execute the trim operation through the event writer.
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(writer_backend));
    app.insert_resource(TrimRequest {
        stream: stream.clone(),
        max_len: 1,
        strategy: TrimStrategy::Exact,
        executed: false,
    });
    app.add_systems(Update, trim_stream_once);
    app.update();

    // Wait for the asynchronous trim worker to apply the request.
    let conn = RefCell::new(conn);
    let (success, _) = helpers::update_until(&mut app, 2_000, |_| {
        let mut conn = conn.borrow_mut();
        let len: usize = redis::cmd("XLEN")
            .arg(&stream)
            .query(&mut *conn)
            .expect("query redis stream length");
        len <= 1
    });
    assert!(success, "stream should be trimmed within the timeout");

    let mut conn = conn.into_inner();
    let len: usize = redis::cmd("XLEN")
        .arg(&stream)
        .query(&mut conn)
        .expect("query redis stream length");
    assert_eq!(len, 1, "stream should be trimmed to a single entry");

    drop(context);
}
