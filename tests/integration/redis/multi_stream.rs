#![cfg(feature = "redis")]

use std::time::Duration;

use bevy_event_bus::EventBusBackend;
use bevy_event_bus::backends::event_bus_backend::{ReceiveOptions, SendOptions};
use bevy_event_bus::config::redis::{RedisConsumerGroupSpec, RedisStreamSpec};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{
    unique_consumer_group_membership, unique_topic, wait_for_messages_in_group,
};
use integration_tests::utils::redis_setup::{self, SetupOptions};
use serde_json::to_vec;
use tokio::task;

fn fast_timeout_options() -> SetupOptions {
    SetupOptions::new().read_block_timeout(Duration::from_millis(25))
}

#[tokio::test]
async fn test_multi_stream_isolation() {
    let stream1 = unique_topic("redis_multi_stream_1");
    let stream2 = unique_topic("redis_multi_stream_2");
    let membership1 = unique_consumer_group_membership("redis_multi_stream_group1");
    let membership2 = unique_consumer_group_membership("redis_multi_stream_group2");

    let stream1_for_config = stream1.clone();
    let stream2_for_config = stream2.clone();
    let group1_for_config = membership1.group.clone();
    let consumer1_for_config = membership1.member.clone();
    let group2_for_config = membership2.group.clone();
    let consumer2_for_config = membership2.member.clone();

    let request = redis_setup::build_request(fast_timeout_options(), move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream1_for_config.clone()))
            .add_stream(RedisStreamSpec::new(stream2_for_config.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream1_for_config.clone()],
                group1_for_config.clone(),
                consumer1_for_config.clone(),
            ))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream2_for_config.clone()],
                group2_for_config.clone(),
                consumer2_for_config.clone(),
            ));
    });
    let (mut backend, _context) = task::spawn_blocking(move || redis_setup::setup(request))
        .await
        .expect("Redis backend setup task panicked")
        .expect("Redis backend setup successful");

    assert!(backend.connect().await, "Failed to connect Redis backend");

    let event_stream1 = TestEvent {
        message: "stream1_event".to_string(),
        value: 111,
    };
    let event_stream2 = TestEvent {
        message: "stream2_event".to_string(),
        value: 222,
    };

    let serialized1 = to_vec(&event_stream1).expect("Serialization should succeed for stream1");
    let serialized2 = to_vec(&event_stream2).expect("Serialization should succeed for stream2");

    assert!(
        backend.try_send_serialized(&serialized1, &stream1, SendOptions::default()),
        "Failed to send message to stream 1",
    );
    assert!(
        backend.try_send_serialized(&serialized2, &stream2, SendOptions::default()),
        "Failed to send message to stream 2",
    );
    backend
        .flush()
        .await
        .expect("Flush should succeed for multi-stream test");

    let received_stream1 =
        wait_for_messages_in_group(&backend, &stream1, &membership1.group, 1, 10_000).await;
    let received_stream2 =
        wait_for_messages_in_group(&backend, &stream2, &membership2.group, 1, 10_000).await;

    assert!(
        !received_stream1.is_empty(),
        "Consumer group 1 should receive messages from stream 1",
    );
    assert!(
        !received_stream2.is_empty(),
        "Consumer group 2 should receive messages from stream 2",
    );

    let found_stream1_message = received_stream1
        .iter()
        .any(|message| String::from_utf8_lossy(message).contains("stream1_event"));
    let found_stream2_message = received_stream2
        .iter()
        .any(|message| String::from_utf8_lossy(message).contains("stream2_event"));

    assert!(found_stream1_message, "Stream 1 message should be received");
    assert!(found_stream2_message, "Stream 2 message should be received");

    let other_group_received_stream1 = received_stream2
        .iter()
        .any(|message| String::from_utf8_lossy(message).contains("stream1_event"));
    let other_group_received_stream2 = received_stream1
        .iter()
        .any(|message| String::from_utf8_lossy(message).contains("stream2_event"));

    assert!(
        !other_group_received_stream1,
        "Stream 1 events should not appear in group 2",
    );
    assert!(
        !other_group_received_stream2,
        "Stream 2 events should not appear in group 1",
    );

    let received = backend
        .receive_serialized(
            &stream1,
            ReceiveOptions::new().consumer_group(&membership2.group),
        )
        .await;
    assert!(
        received.is_empty(),
        "Consumer group 2 should not see stream 1 messages"
    );

    let received = backend
        .receive_serialized(
            &stream2,
            ReceiveOptions::new().consumer_group(&membership1.group),
        )
        .await;
    assert!(
        received.is_empty(),
        "Consumer group 1 should not see stream 2 messages"
    );

    assert!(
        backend.disconnect().await,
        "Redis backend disconnect should succeed"
    );
}
