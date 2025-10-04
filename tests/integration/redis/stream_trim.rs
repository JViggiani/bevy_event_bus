#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::TrimStrategy;
use bevy_event_bus::{EventBusPlugins, RedisEventWriter};
use integration_tests::common::backend_factory::{setup_backend, BackendVariant, TestBackend};
use integration_tests::common::TestEvent;

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
    let (handle, context) = setup_backend(TestBackend::Redis).expect("redis backend setup");
    let BackendVariant::Redis(writer_backend) = handle.writer_backend else {
        panic!("expected redis writer backend");
    };
    let stream = handle.topic.clone();

    // Seed the stream with multiple entries using a raw Redis connection.
    let connection_string = context
        .as_redis()
        .expect("redis tests require redis context")
        .connection_string()
        .to_string();

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

    // Ensure only a single entry remains.
    let len: usize = redis::cmd("XLEN")
        .arg(&stream)
        .query(&mut conn)
        .expect("query redis stream length");
    assert_eq!(len, 1, "stream should be trimmed to a single entry");

    drop(context);
}
