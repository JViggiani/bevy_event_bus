#![cfg(feature = "redis")]

use std::time::Duration;

use bevy_event_bus::backends::event_bus_backend::{ReceiveOptions, SendOptions};
use bevy_event_bus::config::redis::{
    RedisConsumerGroupSpec, RedisStreamSpec, RedisTopologyBuilder,
};
use bevy_event_bus::{EventBusBackend, RedisEventBusBackend};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{
    unique_consumer_group_membership, unique_topic, wait_for_messages_in_group,
};
use integration_tests::utils::redis_setup::{self, SetupOptions};
use serde_json::to_vec;
use tokio::task;

async fn init_backend<F>(configure: F) -> RedisEventBusBackend
where
    F: FnOnce(&mut RedisTopologyBuilder) + Send + 'static,
{
    task::spawn_blocking(move || {
        let request = redis_setup::build_request(
            SetupOptions::new().read_block_timeout(Duration::from_millis(25)),
            configure,
        );
        let (backend, _context) =
            redis_setup::setup(request).expect("Redis backend setup successful");
        backend
    })
    .await
    .expect("Redis backend setup task panicked")
}

#[tokio::test]
async fn test_consumer_group_ready_after_connect() {
    let stream = unique_topic("redis_consumer_ready");
    let membership = unique_consumer_group_membership("redis_group_ready");
    let consumer_group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let stream_for_config = stream.clone();
    let group_for_config = consumer_group.clone();
    let consumer_for_config = consumer_name.clone();

    let mut backend = init_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_config.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_config.clone()],
                group_for_config.clone(),
                consumer_for_config.clone(),
            ));
    })
    .await;

    assert!(
        backend.connect().await,
        "Failed to connect Redis backend with configured topology"
    );

    let received = backend
        .receive_serialized(
            &stream,
            ReceiveOptions::new().consumer_group(&consumer_group),
        )
        .await;
    assert!(
        received.is_empty(),
        "Fresh consumer group should not yield messages without producers"
    );

    assert!(
        backend.disconnect().await,
        "Redis backend disconnect should succeed"
    );
}

#[tokio::test]
async fn test_receive_serialized_with_group() {
    let stream = unique_topic("redis_receive_group");
    let membership = unique_consumer_group_membership("redis_receive_group");
    let consumer_group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let stream_for_config = stream.clone();
    let group_for_config = consumer_group.clone();
    let consumer_for_config = consumer_name.clone();

    let mut backend = init_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_config.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_config.clone()],
                group_for_config.clone(),
                consumer_for_config.clone(),
            ));
    })
    .await;

    assert!(backend.connect().await, "Failed to connect Redis backend");

    let test_event = TestEvent {
        message: "Test message for group consumption".to_string(),
        value: 123,
    };

    let serialized = to_vec(&test_event).expect("Serialization should succeed");
    assert!(
        backend.try_send_serialized(&serialized, &stream, SendOptions::default()),
        "Failed to send serialized message",
    );
    backend
        .flush()
        .await
        .expect("Flush should succeed after sending message");

    let received = wait_for_messages_in_group(&backend, &stream, &consumer_group, 1, 5_000).await;
    assert!(
        !received.is_empty(),
        "Consumer group should receive at least one message",
    );

    let found_test_message = received.iter().any(|message| {
        String::from_utf8_lossy(message).contains("Test message for group consumption")
    });
    assert!(
        found_test_message,
        "Consumer group should receive the test message",
    );

    assert!(
        backend.disconnect().await,
        "Redis backend disconnect should succeed"
    );
}

#[tokio::test]
async fn test_multiple_consumer_groups_independence() {
    let stream = unique_topic("redis_multi_groups");
    let membership1 = unique_consumer_group_membership("redis_group_1");
    let membership2 = unique_consumer_group_membership("redis_group_2");

    let stream_for_config = stream.clone();
    let group1_for_config = membership1.group.clone();
    let consumer1_for_config = membership1.member.clone();
    let group2_for_config = membership2.group.clone();
    let consumer2_for_config = membership2.member.clone();

    let mut backend = init_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_config.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_config.clone()],
                group1_for_config.clone(),
                consumer1_for_config.clone(),
            ))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_config.clone()],
                group2_for_config.clone(),
                consumer2_for_config.clone(),
            ));
    })
    .await;

    assert!(backend.connect().await, "Failed to connect Redis backend");

    let test_event = TestEvent {
        message: "Test message for multiple groups".to_string(),
        value: 789,
    };

    let serialized = to_vec(&test_event).expect("Serialization should succeed");
    assert!(
        backend.try_send_serialized(&serialized, &stream, SendOptions::default()),
        "Failed to send test message",
    );
    backend
        .flush()
        .await
        .expect("Flush should succeed for multi-group test");

    let received_group1 =
        wait_for_messages_in_group(&backend, &stream, &membership1.group, 1, 5_000).await;
    let received_group2 =
        wait_for_messages_in_group(&backend, &stream, &membership2.group, 1, 5_000).await;

    assert!(
        !received_group1.is_empty(),
        "Group 1 should receive messages",
    );
    assert!(
        !received_group2.is_empty(),
        "Group 2 should receive messages",
    );

    let found_in_group1 = received_group1.iter().any(|message| {
        String::from_utf8_lossy(message).contains("Test message for multiple groups")
    });
    let found_in_group2 = received_group2.iter().any(|message| {
        String::from_utf8_lossy(message).contains("Test message for multiple groups")
    });

    assert!(found_in_group1, "Group 1 should receive the test message");
    assert!(found_in_group2, "Group 2 should receive the test message");

    assert!(
        backend.disconnect().await,
        "Redis backend disconnect should succeed"
    );
}

#[tokio::test]
async fn test_consumer_group_with_multiple_streams() {
    let stream1 = unique_topic("redis_multi_stream_1");
    let stream2 = unique_topic("redis_multi_stream_2");
    let membership = unique_consumer_group_membership("redis_group_multi_streams");
    let consumer_group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let stream1_for_config = stream1.clone();
    let stream2_for_config = stream2.clone();
    let group_for_config = consumer_group.clone();
    let consumer_for_config = consumer_name.clone();

    let mut backend = init_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream1_for_config.clone()))
            .add_stream(RedisStreamSpec::new(stream2_for_config.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream1_for_config.clone(), stream2_for_config.clone()],
                group_for_config.clone(),
                consumer_for_config.clone(),
            ));
    })
    .await;

    assert!(backend.connect().await, "Failed to connect Redis backend");

    let event_stream1 = TestEvent {
        message: "Message to stream 1".to_string(),
        value: 111,
    };
    let event_stream2 = TestEvent {
        message: "Message to stream 2".to_string(),
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
        wait_for_messages_in_group(&backend, &stream1, &consumer_group, 1, 10_000).await;
    let received_stream2 =
        wait_for_messages_in_group(&backend, &stream2, &consumer_group, 1, 10_000).await;

    assert!(
        !received_stream1.is_empty(),
        "Consumer group should receive messages from stream 1",
    );
    assert!(
        !received_stream2.is_empty(),
        "Consumer group should receive messages from stream 2",
    );

    let found_stream1_message = received_stream1
        .iter()
        .any(|message| String::from_utf8_lossy(message).contains("Message to stream 1"));
    let found_stream2_message = received_stream2
        .iter()
        .any(|message| String::from_utf8_lossy(message).contains("Message to stream 2"));

    assert!(found_stream1_message, "Stream 1 message should be received");
    assert!(found_stream2_message, "Stream 2 message should be received");

    assert!(
        backend.disconnect().await,
        "Redis backend disconnect should succeed"
    );
}
