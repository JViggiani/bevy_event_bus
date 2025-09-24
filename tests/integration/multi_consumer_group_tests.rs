use std::collections::HashMap;
use tokio::time::Duration;

use crate::common::events::TestEvent;
use crate::common::helpers::unique_topic;
use crate::common::setup::setup;
use bevy_event_bus::backends::kafka_backend::{KafkaConnection, KafkaEventBusBackend};
use bevy_event_bus::EventBusBackend;

/// Helper function to ensure topic exists and create consumer group properly
async fn setup_consumer_group(
    backend: &mut KafkaEventBusBackend,
    topic: &str,
    group_id: &str,
) {
    // Connect the backend to Kafka
    let connect_result = backend.connect().await;
    assert!(connect_result, "Failed to connect to Kafka");
    
    // First, send a dummy message to ensure the topic exists in Kafka
    let dummy_message = b"topic_creation_dummy";
    backend.try_send_serialized(dummy_message, topic);
    tokio::time::sleep(Duration::from_millis(100)).await;  // Short wait for topic creation
    
    // Create consumer group after topic exists
    backend.create_consumer_group(&[topic.to_string()], group_id).await.unwrap();
    
    // Allow time for consumer group to initialize and get partition assignments
    tokio::time::sleep(Duration::from_millis(1000)).await;
}

#[tokio::test]
async fn test_create_consumer_group() {
    let (_backend, bootstrap) = setup();
    let topic = unique_topic("test_create_consumer_group");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_create_group".to_string()),
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    let group_id = unique_topic("test_group_create");
    
    // Test creating a consumer group
    let result = backend.create_consumer_group(&[topic.clone()], &group_id).await;
    assert!(result.is_ok(), "Failed to create consumer group: {:?}", result.err());
    
    // Test creating the same group again should be idempotent
    let result2 = backend.create_consumer_group(&[topic.clone()], &group_id).await;
    assert!(result2.is_ok(), "Second creation should succeed (idempotent): {:?}", result2.err());
}

#[tokio::test]
async fn test_receive_serialized_with_group() {
    let (_backend, bootstrap) = setup();
    let topic = unique_topic("test_receive_with_group");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_receive_group".to_string()),
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    let group_id = format!("test_group_receive_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    
    // Setup consumer group properly
    setup_consumer_group(&mut backend, &topic, &group_id).await;
    
    // Send a test message AFTER consumer group is initialized
    let test_event = TestEvent {
        message: "Test message for group consumption".to_string(),
        value: 123,
    };
    
    let serialized = serde_json::to_string(&test_event).unwrap();
    let send_result = backend.try_send_serialized(serialized.as_bytes(), &topic);
    assert!(send_result, "Failed to send message");
    
    // Allow some time for message propagation
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Try to receive the message with the consumer group
    let received = backend.receive_serialized_with_group(&topic, &group_id).await;
    assert!(!received.is_empty(), "Should have received at least one message");
    
    // Verify we got our test message
    let found_test_message = received.iter().any(|message| {
        String::from_utf8_lossy(message).contains("Test message for group consumption")
    });
    assert!(found_test_message, "Should have received the test message we sent");
}

#[tokio::test]
async fn test_enable_manual_commits_and_commit_offset() {
    let (_backend, bootstrap) = setup();
    let topic = unique_topic("test_manual_commits");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_manual_commits".to_string()),
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    let group_id = unique_topic("test_group_manual");
    
    // Setup consumer group properly first  
    setup_consumer_group(&mut backend, &topic, &group_id).await;
    
    // Enable manual commits for this group (this recreates the consumer group)
    let result = backend.enable_manual_commits(&group_id).await;
    assert!(result.is_ok(), "Failed to enable manual commits: {:?}", result.err());
    
    // Allow additional time for the recreated consumer group to initialize
    tokio::time::sleep(Duration::from_millis(1000)).await;
    
    // Send a test message
    let test_event = TestEvent {
        message: "Test message for manual commit".to_string(),
        value: 456,
    };
    
    let serialized = serde_json::to_string(&test_event).unwrap();
    assert!(backend.try_send_serialized(serialized.as_bytes(), &topic), "Failed to send test message");
    
    // Allow some time for message propagation
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Receive the message
    let received = backend.receive_serialized_with_group(&topic, &group_id).await;
    assert!(!received.is_empty(), "Should have received at least one message");
    
    // Manually commit the offset for partition 0 at offset 1
    let commit_result = backend.commit_offset(&topic, 0, 1).await;
    assert!(commit_result.is_ok(), "Failed to commit offset: {:?}", commit_result.err());
}

#[tokio::test]
async fn test_get_consumer_lag() {
    let (_backend, bootstrap) = setup();
    let topic = unique_topic("test_consumer_lag");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_lag".to_string()),
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    let group_id = unique_topic("test_group_lag");
    
    // Create consumer group
    backend.create_consumer_group(&[topic.clone()], &group_id).await.unwrap();
    
    // Send multiple test messages to create some lag
    for i in 0..5 {
        let test_event = TestEvent {
            message: format!("Test message {} for lag calculation", i),
            value: i,
        };
        
        let serialized = serde_json::to_string(&test_event).unwrap();
        assert!(backend.try_send_serialized(serialized.as_bytes(), &topic), "Failed to send test message");
    }
    
    // Allow some time for message propagation
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Get consumer lag - should be >= 0
    let lag_result = backend.get_consumer_lag(&topic, &group_id).await;
    assert!(lag_result.is_ok(), "Failed to get consumer lag: {:?}", lag_result.err());
    
    let lag = lag_result.unwrap();
    assert!(lag >= 0, "Consumer lag should be non-negative, got: {}", lag);
    
    // Consume some messages to change the lag
    let _received = backend.receive_serialized_with_group(&topic, &group_id).await;
    
    // Check lag again - it might have changed
    let lag_result2 = backend.get_consumer_lag(&topic, &group_id).await;
    assert!(lag_result2.is_ok(), "Failed to get consumer lag on second check: {:?}", lag_result2.err());
    
    let lag2 = lag_result2.unwrap();
    assert!(lag2 >= 0, "Consumer lag should still be non-negative, got: {}", lag2);
}

#[tokio::test]
async fn test_multiple_consumer_groups_independence() {
    let (_backend, bootstrap) = setup();
    let topic = unique_topic("test_multi_groups");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_multi_groups".to_string()),
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    let group_id1 = unique_topic("test_group_1");
    let group_id2 = unique_topic("test_group_2");
    
    // Connect the backend first
    let connect_result = backend.connect().await;
    assert!(connect_result, "Failed to connect to Kafka");
    
    // Create topic by sending dummy message
    let dummy_message = b"topic_creation_dummy";
    backend.try_send_serialized(dummy_message, &topic);
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Create two consumer groups
    backend.create_consumer_group(&[topic.clone()], &group_id1).await.unwrap();
    backend.create_consumer_group(&[topic.clone()], &group_id2).await.unwrap();
    
    // Allow time for consumer groups to initialize
    tokio::time::sleep(Duration::from_millis(1000)).await;
    
    // Send a test message
    let test_event = TestEvent {
        message: "Test message for multiple groups".to_string(),
        value: 789,
    };
    
    let serialized = serde_json::to_string(&test_event).unwrap();
    assert!(backend.try_send_serialized(serialized.as_bytes(), &topic), "Failed to send test message");
    
    // Allow some time for message propagation
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Both groups should be able to receive the message independently
    let received1 = backend.receive_serialized_with_group(&topic, &group_id1).await;
    let received2 = backend.receive_serialized_with_group(&topic, &group_id2).await;
    
    // Both groups should have received the message
    assert!(!received1.is_empty(), "Group 1 should have received messages");
    assert!(!received2.is_empty(), "Group 2 should have received messages");
    
    // Verify both got our test message
    let found_in_group1 = received1.iter().any(|message| {
        String::from_utf8_lossy(message).contains("Test message for multiple groups")
    });
    let found_in_group2 = received2.iter().any(|message| {
        String::from_utf8_lossy(message).contains("Test message for multiple groups")
    });
    
    assert!(found_in_group1, "Group 1 should have received the test message");
    assert!(found_in_group2, "Group 2 should have received the test message");
}

#[tokio::test]
async fn test_consumer_group_with_multiple_topics() {
    let (_backend, bootstrap) = setup();
    let topic1 = unique_topic("test_multi_topic_1");
    let topic2 = unique_topic("test_multi_topic_2");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_multi_topics".to_string()),
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    let group_id = unique_topic("test_group_multi_topics");
    
    // Connect the backend first
    let connect_result = backend.connect().await;
    assert!(connect_result, "Failed to connect to Kafka");
    
    // Create both topics by sending dummy messages
    let dummy_message = b"topic_creation_dummy";
    backend.try_send_serialized(dummy_message, &topic1);
    backend.try_send_serialized(dummy_message, &topic2);
    tokio::time::sleep(Duration::from_millis(100)).await;
    
    // Create consumer group with multiple topics
    backend.create_consumer_group(&[topic1.clone(), topic2.clone()], &group_id).await.unwrap();
    
    // Allow time for consumer group to initialize
    tokio::time::sleep(Duration::from_millis(1000)).await;
    
    // Send messages to both topics
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
    
    assert!(backend.try_send_serialized(serialized1.as_bytes(), &topic1), "Failed to send to topic1");
    assert!(backend.try_send_serialized(serialized2.as_bytes(), &topic2), "Failed to send to topic2");
    
    // Allow some time for message propagation
    tokio::time::sleep(Duration::from_millis(500)).await;
    
    // Consumer group should receive messages from both topics
    // Note: In Kafka, when a consumer group subscribes to multiple topics, it receives messages
    // from all topics. We get all messages in one call.
    let all_received = backend.receive_serialized_with_group(&topic1, &group_id).await;
    
    assert!(!all_received.is_empty(), "Should have received messages from topics");
    
    // Verify we got messages from both topics by checking message content
    let found_topic1_message = all_received.iter().any(|message| {
        String::from_utf8_lossy(message).contains("Message to topic 1")
    });
    let found_topic2_message = all_received.iter().any(|message| {
        String::from_utf8_lossy(message).contains("Message to topic 2")
    });
    
    assert!(found_topic1_message, "Should have received message from topic 1");
    assert!(found_topic2_message, "Should have received message from topic 2");
}