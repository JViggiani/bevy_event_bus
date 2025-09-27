use std::collections::HashMap;

use crate::common::events::TestEvent;
use crate::common::helpers::{unique_topic, unique_consumer_group, wait_for_consumer_group_ready, wait_for_messages};
use crate::common::setup::setup;
use bevy_event_bus::{KafkaConnection, KafkaEventBusBackend, EventBusBackend, KafkaReadConfig, KafkaWriteConfig};
use bevy_event_bus::config::{SecurityProtocol, OffsetReset};

#[tokio::test]
async fn test_create_consumer_group() {
    let (_backend, bootstrap) = setup(None);
    let topic = unique_topic("test_create_consumer_group");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_create_group".to_string()),
        security_protocol: SecurityProtocol::Plaintext,
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    let group_id = unique_consumer_group("test_group_create");
    
    // Test creating a consumer group
    let config = KafkaReadConfig::new(&group_id).topics([&topic]);
    let result = backend.create_consumer_group(&config).await;
    assert!(result.is_ok(), "Failed to create consumer group: {:?}", result.err());
    
    // Test creating the same group again should be idempotent
    let result2 = backend.create_consumer_group(&config).await;
    assert!(result2.is_ok(), "Second creation should succeed (idempotent): {:?}", result2.err());
}

#[tokio::test]
async fn test_poll_messages_with_group() {
    let (_backend, bootstrap) = setup(None);
    let topic = unique_topic("test_receive_with_group");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_receive_group".to_string()),
        security_protocol: SecurityProtocol::Plaintext,
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    backend.connect().await;
    
    let group_id = format!("test_group_receive_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    
    // Create consumer group with Earliest offset to read all messages from beginning
    let consumer_config = KafkaReadConfig::new(&group_id).topics([&topic]).offset_reset(OffsetReset::Earliest);
    backend.create_consumer_group(&consumer_config).await.unwrap();
    
    // Wait for consumer group to be properly initialized instead of arbitrary sleep
    let ready = wait_for_consumer_group_ready::<TestEvent>(&mut backend, &consumer_config, 5000).await;
    assert!(ready, "Consumer group failed to initialize within timeout");
    
    // Send a test message AFTER consumer group is initialized
    let test_event = TestEvent {
        message: "Test message for group consumption".to_string(),
        value: 123,
    };
    
    let serialized = serde_json::to_string(&test_event).unwrap();
    let write_config = KafkaWriteConfig::new(&topic);
    let send_result = backend.try_send_serialized(serialized.as_bytes(), &write_config);
    assert!(send_result, "Failed to send message");
    
    // Wait for messages with condition-based polling instead of sleep + manual loop
    let received = wait_for_messages::<TestEvent>(&mut backend, &consumer_config, 1, 10000).await;
    
    assert!(!received.is_empty(), "Should have received at least one message");
    
    // Verify we got our test message
    let found_test_message = received.iter().any(|event| {
        event.message.contains("Test message for group consumption")
    });
    assert!(found_test_message, "Should have received the test message we sent");
}

#[tokio::test]
async fn test_poll_messages_with_repeated_polling() {
    let (_backend, bootstrap) = setup(None);
    let topic = unique_topic("test_repeated_polling");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_repeated_polling".to_string()),
        security_protocol: SecurityProtocol::Plaintext,
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    backend.connect().await;
    
    let group_id = format!("test_group_repeated_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    
    // Create consumer group with Earliest offset
    let consumer_config = KafkaReadConfig::new(&group_id).topics([&topic]).offset_reset(OffsetReset::Earliest);
    backend.create_consumer_group(&consumer_config).await.unwrap();
    
    // Wait for consumer group to be ready instead of arbitrary sleep
    let ready = wait_for_consumer_group_ready::<TestEvent>(&mut backend, &consumer_config, 5000).await;
    assert!(ready, "Consumer group failed to initialize within timeout");
    
    // Send a test message
    let test_event = TestEvent {
        message: "Test message for repeated polling".to_string(),
        value: 456,
    };
    
    let serialized = serde_json::to_string(&test_event).unwrap();
    let write_config = KafkaWriteConfig::new(&topic);
    let send_result = backend.try_send_serialized(serialized.as_bytes(), &write_config);
    assert!(send_result, "Failed to send message");
    
    // Use condition-based message waiting instead of manual polling with sleeps
    let received_messages = wait_for_messages::<TestEvent>(&mut backend, &consumer_config, 1, 10000).await;
    
    println!("Received {} messages", received_messages.len());
    assert!(!received_messages.is_empty(), "Should have received at least one message after repeated polling");
    
    // Verify we got our test message
    let found_test_message = received_messages.iter().any(|event| {
        event.message.contains("Test message for repeated polling")
    });
    assert!(found_test_message, "Should have received the test message we sent");
}

#[tokio::test]
async fn test_single_vs_repeated_polling_comparison() {
    let (_backend, bootstrap) = setup(None);
    let topic = unique_topic("test_polling_comparison");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_comparison".to_string()),
        security_protocol: SecurityProtocol::Plaintext,
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    backend.connect().await;
    
    let group_id = format!("test_group_comparison_{}", std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos());
    
    // Create consumer group with Earliest offset
    let consumer_config = KafkaReadConfig::new(&group_id).topics([&topic]).offset_reset(OffsetReset::Earliest);
    backend.create_consumer_group(&consumer_config).await.unwrap();
    
    // Wait for consumer group to be ready instead of arbitrary sleep
    let ready = wait_for_consumer_group_ready::<TestEvent>(&mut backend, &consumer_config, 5000).await;
    assert!(ready, "Consumer group failed to initialize within timeout");
    
    // Send a test message
    let test_event = TestEvent {
        message: "Test message for comparison".to_string(),
        value: 789,
    };
    
    let serialized = serde_json::to_string(&test_event).unwrap();
    let write_config = KafkaWriteConfig::new(&topic);
    let send_result = backend.try_send_serialized(serialized.as_bytes(), &write_config);
    assert!(send_result, "Failed to send message");
    
    // Use condition-based waiting instead of manual polling with sleeps
    println!("=== CONDITION-BASED MESSAGE WAITING ===");
    let start_time = std::time::Instant::now();
    let messages = wait_for_messages::<TestEvent>(&mut backend, &consumer_config, 1, 10000).await;
    println!("Message waiting completed in {:?}, found {} messages", start_time.elapsed(), messages.len());
    
    assert!(!messages.is_empty(), "Should have received at least one message");
    
    // Verify message content
    let found_test_message = messages.iter().any(|event| {
        event.message.contains("Test message for comparison")
    });
    assert!(found_test_message, "Should have received the test message we sent");
}

#[tokio::test]
async fn test_manual_commits_and_commit_offset() {
    let (_backend, bootstrap) = setup(None);
    let topic = unique_topic("test_manual_commits");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_manual_commits".to_string()),
        security_protocol: SecurityProtocol::Plaintext,
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    let group_id = unique_consumer_group("test_group_manual");
    
    // Create a read config with manual commits enabled and earliest offset
    let read_config = KafkaReadConfig::new(&group_id)
        .topics(vec![topic.clone()])
        .auto_commit(false) // Enable manual commits
        .offset_reset(OffsetReset::Earliest); // Start from earliest messages
    
    // Setup consumer group with manual commit configuration
    backend.create_consumer_group(&read_config).await.unwrap();
    
    // Wait for consumer group to be properly initialized
    let ready = wait_for_consumer_group_ready::<TestEvent>(&mut backend, &read_config, 5000).await;
    assert!(ready, "Consumer group failed to initialize within timeout");
    
    // Send a test message
    let test_event = TestEvent {
        message: "Test message for manual commit".to_string(),
        value: 456,
    };
    
    let serialized = serde_json::to_string(&test_event).unwrap();
    let write_config = KafkaWriteConfig::new(&topic);
    assert!(backend.try_send_serialized(serialized.as_bytes(), &write_config), "Failed to send test message");
    
    // Wait for messages with condition-based polling instead of manual sleep and polling
    let received = wait_for_messages::<TestEvent>(&mut backend, &read_config, 1, 10000).await;
    
    println!("Manual commit test completed, found {} messages", received.len());
    assert!(!received.is_empty(), "Should have received at least one message");
    
    // Manually commit the offset for partition 0 at offset 1
    let commit_result = backend.commit_offset(&topic, 0, 1).await;
    assert!(commit_result.is_ok(), "Failed to commit offset: {:?}", commit_result.err());
}

#[tokio::test]
async fn test_get_consumer_lag() {
    let (_backend, bootstrap) = setup(None);
    let topic = unique_topic("test_consumer_lag");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_lag".to_string()),
        security_protocol: SecurityProtocol::Plaintext,
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    let group_id = unique_consumer_group("test_group_lag");
    
    // Create consumer group
    let config = KafkaReadConfig::new(&group_id).topics([&topic]);
    backend.create_consumer_group(&config).await.unwrap();
    
    // Send multiple test messages to create some lag
    let write_config = KafkaWriteConfig::new(&topic);
    for i in 0..5 {
        let test_event = TestEvent {
            message: format!("Test message {} for lag calculation", i),
            value: i,
        };
        
        let serialized = serde_json::to_string(&test_event).unwrap();
        assert!(backend.try_send_serialized(serialized.as_bytes(), &write_config), "Failed to send test message");
    }
    
    // Message propagation happens asynchronously, no need for fixed sleep
    // Get consumer lag - should be >= 0
    let lag_result = backend.get_consumer_lag(&topic, &group_id).await;
    assert!(lag_result.is_ok(), "Failed to get consumer lag: {:?}", lag_result.err());
    
    let lag = lag_result.unwrap();
    assert!(lag >= 0, "Consumer lag should be non-negative, got: {}", lag);
    
    // Consume some messages to change the lag
    let consumer_config = KafkaReadConfig::new(&group_id).topics([&topic]);
    let mut _received = Vec::new();
    let _ = backend.receive::<TestEvent>(&consumer_config, &mut _received).await;
    
    // Check lag again - it might have changed
    let lag_result2 = backend.get_consumer_lag(&topic, &group_id).await;
    assert!(lag_result2.is_ok(), "Failed to get consumer lag on second check: {:?}", lag_result2.err());
    
    let lag2 = lag_result2.unwrap();
    assert!(lag2 >= 0, "Consumer lag should still be non-negative, got: {}", lag2);
}

#[tokio::test]
async fn test_multiple_consumer_groups_independence() {
    let (_backend, bootstrap) = setup(None);
    let topic = unique_topic("test_multi_groups");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_multi_groups".to_string()),
        security_protocol: SecurityProtocol::Plaintext,
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    let group_id1 = unique_consumer_group("test_group_1");
    let group_id2 = unique_consumer_group("test_group_2");
    
    // Connect the backend first
    let connect_result = backend.connect().await;
    assert!(connect_result, "Failed to connect to Kafka");
    
    // Create topic by sending dummy message
    let dummy_message = b"topic_creation_dummy";
    let write_config = KafkaWriteConfig::new(&topic);
    backend.try_send_serialized(dummy_message, &write_config);
    // Topic creation happens asynchronously, no need to wait
    
    // Create two consumer groups with earliest offset reset
    let config1 = KafkaReadConfig::new(&group_id1).topics([&topic]).offset_reset(OffsetReset::Earliest);
    let config2 = KafkaReadConfig::new(&group_id2).topics([&topic]).offset_reset(OffsetReset::Earliest);
    backend.create_consumer_group(&config1).await.unwrap();
    backend.create_consumer_group(&config2).await.unwrap();
    
    // Wait for consumer groups to be properly initialized
    let ready1 = wait_for_consumer_group_ready::<TestEvent>(&mut backend, &config1, 5000).await;
    let ready2 = wait_for_consumer_group_ready::<TestEvent>(&mut backend, &config2, 5000).await;
    assert!(ready1, "Consumer group 1 failed to initialize within timeout");
    assert!(ready2, "Consumer group 2 failed to initialize within timeout");
    
    // Send a test message
    let test_event = TestEvent {
        message: "Test message for multiple groups".to_string(),
        value: 789,
    };
    
    let serialized = serde_json::to_string(&test_event).unwrap();
    let write_config = KafkaWriteConfig::new(&topic);
    assert!(backend.try_send_serialized(serialized.as_bytes(), &write_config), "Failed to send test message");
    
    // Both groups should be able to receive the message independently
    // Use condition-based waiting instead of manual polling and sleeps
    
    // Group 1 - wait for messages
    let received1 = wait_for_messages::<TestEvent>(&mut backend, &config1, 1, 10000).await;
    
    // Group 2 - wait for messages  
    let received2 = wait_for_messages::<TestEvent>(&mut backend, &config2, 1, 10000).await;
    
    // Both groups should have received the message
    assert!(!received1.is_empty(), "Group 1 should have received messages");
    assert!(!received2.is_empty(), "Group 2 should have received messages");
    
    // Verify both got our test message
    let found_in_group1 = received1.iter().any(|event| {
        event.message.contains("Test message for multiple groups")
    });
    let found_in_group2 = received2.iter().any(|event| {
        event.message.contains("Test message for multiple groups")
    });
    
    assert!(found_in_group1, "Group 1 should have received the test message");
    assert!(found_in_group2, "Group 2 should have received the test message");
}


#[tokio::test]
async fn test_consumer_group_with_multiple_topics() {
    let (_backend, bootstrap) = setup(None);
    let topic1 = unique_topic("test_multi_topic_1");
    let topic2 = unique_topic("test_multi_topic_2");
    
    let config = KafkaConnection {
        bootstrap_servers: bootstrap,
        client_id: Some("test_client_multi_topics".to_string()),
        security_protocol: SecurityProtocol::Plaintext,
        timeout_ms: 5000,
        additional_config: HashMap::new(),
    };
    
    let mut backend = KafkaEventBusBackend::new(config);
    let group_id = unique_consumer_group("test_group_multi_topics");
    
    // Connect the backend first
    let connect_result = backend.connect().await;
    assert!(connect_result, "Failed to connect to Kafka");
    
    // Create both topics by sending dummy messages
    let dummy_message = b"topic_creation_dummy";
    let write_config1 = KafkaWriteConfig::new(&topic1);
    let write_config2 = KafkaWriteConfig::new(&topic2);
    backend.try_send_serialized(dummy_message, &write_config1);
    backend.try_send_serialized(dummy_message, &write_config2);
    // Topic creation happens asynchronously, no need to wait
    
    // Create consumer group with multiple topics and earliest offset reset
    let config = KafkaReadConfig::new(&group_id).topics([&topic1, &topic2]).offset_reset(OffsetReset::Earliest);
    backend.create_consumer_group(&config).await.unwrap();
    
    // Wait for consumer group to be properly initialized
    let ready = wait_for_consumer_group_ready::<TestEvent>(&mut backend, &config, 5000).await;
    assert!(ready, "Consumer group failed to initialize within timeout");
    
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
    
    assert!(backend.try_send_serialized(serialized1.as_bytes(), &write_config1), "Failed to send to topic1");
    assert!(backend.try_send_serialized(serialized2.as_bytes(), &write_config2), "Failed to send to topic2");
    
    // Consumer group should receive messages from both topics 
    // Use condition-based waiting for 2 messages (one from each topic)
    let all_received = wait_for_messages::<TestEvent>(&mut backend, &config, 2, 10000).await;
    
    assert!(!all_received.is_empty(), "Should have received messages from topics");
    
    // Verify we got messages from both topics by checking message content
    let found_topic1_message = all_received.iter().any(|event| {
        event.message.contains("Message to topic 1")
    });
    let found_topic2_message = all_received.iter().any(|event| {
        event.message.contains("Message to topic 2")
    });
    
    assert!(found_topic1_message, "Should have received message from topic 1");
    assert!(found_topic2_message, "Should have received message from topic 2");
}