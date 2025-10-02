use std::time::Duration;

use bevy_event_bus::config::kafka::{
    KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaTopicSpec, KafkaTopologyBuilder,
};
use bevy_event_bus::{
    EventBusBackend, KafkaEventBusBackend,
    backends::event_bus_backend::{ReceiveOptions, SendOptions},
};
use integration_tests::common::events::TestEvent;
use integration_tests::common::helpers::{
    unique_consumer_group, unique_topic, wait_for_consumer_group_ready, wait_for_messages_in_group,
};
use integration_tests::common::setup::setup;

async fn init_backend<F>(offset: &str, configure: F) -> (KafkaEventBusBackend, String)
where
    F: FnOnce(&mut KafkaTopologyBuilder) + Send + 'static,
{
    let offset_owned = offset.to_string();
    tokio::task::spawn_blocking(move || setup(offset_owned.as_str(), configure))
        .await
        .expect("setup panicked")
}
#[tokio::test]
async fn test_create_consumer_group() {
    let topic = unique_topic("test_create_consumer_group");
    let group_id = unique_consumer_group("test_group_create");

    let topic_for_config = topic.clone();
    let group_for_config = group_id.clone();

    let (mut backend, _bootstrap) = init_backend("earliest", move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_for_config.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_consumer_group(
                group_for_config.clone(),
                KafkaConsumerGroupSpec::new([topic_for_config.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            );
    })
    .await;

    assert!(
        backend.connect().await,
        "Failed to connect backend with preconfigured topology"
    );

    let consumer_ready = wait_for_consumer_group_ready(&backend, &topic, &group_id, 10_000).await;
    assert!(
        consumer_ready,
        "Preconfigured consumer group should become ready without dynamic creation"
    );

    let dynamic_result = backend
        .create_consumer_group(&[topic.clone()], &group_id)
        .await;
    assert!(
        dynamic_result.is_err(),
        "Dynamic consumer group creation should be rejected"
    );
}

#[tokio::test]
async fn test_receive_serialized_with_group() {
    let topic = unique_topic("test_receive_with_group");
    let group_id = unique_consumer_group("test_group_receive");

    let topic_for_config = topic.clone();
    let group_for_config = group_id.clone();

    let (mut backend, _bootstrap) = init_backend("earliest", move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_for_config.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_consumer_group(
                group_for_config.clone(),
                KafkaConsumerGroupSpec::new([topic_for_config.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            );
    })
    .await;

    assert!(backend.connect().await, "Failed to connect backend");

    let ready = wait_for_consumer_group_ready(&backend, &topic, &group_id, 10_000).await;
    assert!(ready, "Preconfigured consumer group not ready in time");

    let test_event = TestEvent {
        message: "Test message for group consumption".to_string(),
        value: 123,
    };

    let serialized = serde_json::to_string(&test_event).unwrap();
    assert!(
        backend.try_send_serialized(serialized.as_bytes(), &topic, SendOptions::default()),
        "Failed to send message",
    );

    let received = wait_for_messages_in_group(&backend, &topic, &group_id, 1, 5_000).await;
    assert!(
        !received.is_empty(),
        "Should have received at least one message",
    );

    let found_test_message = received.iter().any(|message| {
        String::from_utf8_lossy(message).contains("Test message for group consumption")
    });
    assert!(
        found_test_message,
        "Should have received the test message we sent",
    );
}

#[tokio::test]
async fn test_enable_manual_commits_and_commit_offset() {
    let topic = unique_topic("test_manual_commits");
    let group_id = unique_consumer_group("test_group_manual");

    let topic_for_config = topic.clone();
    let group_for_config = group_id.clone();

    let (mut backend, _bootstrap) = init_backend("earliest", move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_for_config.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_consumer_group(
                group_for_config.clone(),
                KafkaConsumerGroupSpec::new([topic_for_config.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest)
                    .manual_commits(true),
            );
    })
    .await;

    assert!(backend.connect().await, "Failed to connect backend");

    let result = backend.enable_manual_commits(&group_id).await;
    assert!(
        result.is_ok(),
        "Manual commit enabling should succeed for preconfigured group: {:?}",
        result.err()
    );

    let consumer_ready = wait_for_consumer_group_ready(&backend, &topic, &group_id, 10_000).await;
    assert!(
        consumer_ready,
        "Consumer group not ready after enabling manual commits",
    );

    let test_event = TestEvent {
        message: "Test message for manual commit".to_string(),
        value: 456,
    };

    let serialized = serde_json::to_string(&test_event).unwrap();
    assert!(
        backend.try_send_serialized(serialized.as_bytes(), &topic, SendOptions::default()),
        "Failed to send test message",
    );

    let received = wait_for_messages_in_group(&backend, &topic, &group_id, 1, 5_000).await;
    assert!(
        !received.is_empty(),
        "Should have received at least one message",
    );

    let commit_result = backend.commit_offset(&topic, 0, 1).await;
    assert!(
        commit_result.is_ok(),
        "Failed to commit offset: {:?}",
        commit_result.err()
    );
}

#[tokio::test]
async fn test_get_consumer_lag() {
    let topic = unique_topic("test_consumer_lag");
    let group_id = unique_consumer_group("test_group_lag");

    let topic_for_config = topic.clone();
    let group_for_config = group_id.clone();

    let (mut backend, _bootstrap) = init_backend("earliest", move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_for_config.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_consumer_group(
                group_for_config.clone(),
                KafkaConsumerGroupSpec::new([topic_for_config.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            );
    })
    .await;

    assert!(backend.connect().await, "Failed to connect backend");

    let ready = wait_for_consumer_group_ready(&backend, &topic, &group_id, 10_000).await;
    assert!(ready, "Consumer group not ready within timeout");

    for i in 0..5 {
        let test_event = TestEvent {
            message: format!("Test message {} for lag calculation", i),
            value: i,
        };

        let serialized = serde_json::to_string(&test_event).unwrap();
        assert!(
            backend.try_send_serialized(serialized.as_bytes(), &topic, SendOptions::default()),
            "Failed to send test message",
        );
    }

    let _messages = wait_for_messages_in_group(&backend, &topic, &group_id, 1, 5_000).await;

    let lag_result = backend.get_consumer_lag(&topic, &group_id).await;
    assert!(
        lag_result.is_ok(),
        "Failed to get consumer lag: {:?}",
        lag_result.err()
    );

    let lag = lag_result.unwrap();
    assert!(
        lag >= 0,
        "Consumer lag should be non-negative, got: {}",
        lag
    );

    let _received = backend
        .receive_serialized(&topic, ReceiveOptions::new().consumer_group(&group_id))
        .await;

    let lag_result2 = backend.get_consumer_lag(&topic, &group_id).await;
    assert!(
        lag_result2.is_ok(),
        "Failed to get consumer lag on second check: {:?}",
        lag_result2.err()
    );

    let lag2 = lag_result2.unwrap();
    assert!(
        lag2 >= 0,
        "Consumer lag should still be non-negative, got: {}",
        lag2
    );
}

#[tokio::test]
async fn test_multiple_consumer_groups_independence() {
    let topic = unique_topic("test_multi_groups");
    let group_id1 = unique_consumer_group("test_group_1");
    let group_id2 = unique_consumer_group("test_group_2");

    let topic_for_config = topic.clone();
    let group1_for_config = group_id1.clone();
    let group2_for_config = group_id2.clone();

    let (mut backend, _bootstrap) = init_backend("earliest", move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_for_config.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_consumer_group(
                group1_for_config.clone(),
                KafkaConsumerGroupSpec::new([topic_for_config.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            )
            .add_consumer_group(
                group2_for_config.clone(),
                KafkaConsumerGroupSpec::new([topic_for_config.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            );
    })
    .await;

    assert!(backend.connect().await, "Failed to connect backend");

    let group1_ready = wait_for_consumer_group_ready(&backend, &topic, &group_id1, 10_000).await;
    let group2_ready = wait_for_consumer_group_ready(&backend, &topic, &group_id2, 10_000).await;
    assert!(group1_ready, "Consumer group 1 not ready within timeout");
    assert!(group2_ready, "Consumer group 2 not ready within timeout");

    let test_event = TestEvent {
        message: "Test message for multiple groups".to_string(),
        value: 789,
    };

    let serialized = serde_json::to_string(&test_event).unwrap();
    assert!(
        backend.try_send_serialized(serialized.as_bytes(), &topic, SendOptions::default()),
        "Failed to send test message",
    );

    let received1 = wait_for_messages_in_group(&backend, &topic, &group_id1, 1, 5_000).await;
    let received2 = wait_for_messages_in_group(&backend, &topic, &group_id2, 1, 5_000).await;

    assert!(
        !received1.is_empty(),
        "Group 1 should have received messages",
    );
    assert!(
        !received2.is_empty(),
        "Group 2 should have received messages",
    );

    let found_in_group1 = received1.iter().any(|message| {
        String::from_utf8_lossy(message).contains("Test message for multiple groups")
    });
    let found_in_group2 = received2.iter().any(|message| {
        String::from_utf8_lossy(message).contains("Test message for multiple groups")
    });

    assert!(
        found_in_group1,
        "Group 1 should have received the test message",
    );
    assert!(
        found_in_group2,
        "Group 2 should have received the test message",
    );
}

#[tokio::test]
async fn test_consumer_group_with_multiple_topics() {
    let topic1 = unique_topic("test_multi_topic_1");
    let topic2 = unique_topic("test_multi_topic_2");
    let group_id = unique_consumer_group("test_group_multi_topics");

    let topic1_for_config = topic1.clone();
    let topic2_for_config = topic2.clone();
    let group_for_config = group_id.clone();

    let (mut backend, _bootstrap) = init_backend("earliest", move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic1_for_config.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_topic(
                KafkaTopicSpec::new(topic2_for_config.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_consumer_group(
                group_for_config.clone(),
                KafkaConsumerGroupSpec::new([topic1_for_config.clone(), topic2_for_config.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            );
    })
    .await;

    assert!(backend.connect().await, "Failed to connect backend");

    let topic1_ready = wait_for_consumer_group_ready(&backend, &topic1, &group_id, 10_000).await;
    let topic2_ready = wait_for_consumer_group_ready(&backend, &topic2, &group_id, 10_000).await;
    assert!(
        topic1_ready,
        "Consumer group not ready for topic 1 within timeout"
    );
    assert!(
        topic2_ready,
        "Consumer group not ready for topic 2 within timeout"
    );

    let test_event1 = TestEvent {
        message: "Message to topic 1".to_string(),
        value: 111,
    };
    let test_event2 = TestEvent {
        message: "Message to topic 2".to_string(),
        value: 222,
    };

    let serialized1 = serde_json::to_string(&test_event1).unwrap();
    let serialized2 = serde_json::to_string(&test_event2).unwrap();

    assert!(
        backend.try_send_serialized(serialized1.as_bytes(), &topic1, SendOptions::default(),),
        "Failed to send to topic1",
    );
    assert!(
        backend.try_send_serialized(serialized2.as_bytes(), &topic2, SendOptions::default(),),
        "Failed to send to topic2",
    );
    backend
        .flush(Duration::from_secs(5))
        .expect("Failed to flush after sending multi-topic messages");

    let received_topic1 = wait_for_messages_in_group(&backend, &topic1, &group_id, 1, 10_000).await;
    let received_topic2 = wait_for_messages_in_group(&backend, &topic2, &group_id, 1, 10_000).await;

    assert!(
        !received_topic1.is_empty(),
        "Should have received messages from topic 1",
    );
    assert!(
        !received_topic2.is_empty(),
        "Should have received messages from topic 2",
    );

    let found_topic1_message = received_topic1
        .iter()
        .any(|message| String::from_utf8_lossy(message).contains("Message to topic 1"));
    let found_topic2_message = received_topic2
        .iter()
        .any(|message| String::from_utf8_lossy(message).contains("Message to topic 2"));

    assert!(
        found_topic1_message,
        "Should have received message from topic 1",
    );
    assert!(
        found_topic2_message,
        "Should have received message from topic 2",
    );
}
