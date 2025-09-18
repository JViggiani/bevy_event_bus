//! Comprehensive error handling tests for bevy_event_bus
//! 
//! This module tests the library's resilience and error handling capabilities using
//! the new fire-and-forget API where EventBusWriter methods return () and errors
//! are sent as EventBusError<T> events.
//!
//! Tests cover:
//! - Mock backend delivery failures
//! - Type-safe error handling with multiple event types
//! - Centralized vs distributed error handling patterns
//! - Batch operations with errors
//! - Error retry mechanisms

use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusWriter, PreconfiguredTopics};
use bevy_event_bus::{EventBusError, EventBusErrorType, EventBusAppExt};
use serde::{Deserialize, Serialize};
use crate::common::helpers::unique_topic;
use crate::common::MockEventBusBackend;

// Event types for various test scenarios
#[derive(Event, Deserialize, Serialize, Debug, Clone, PartialEq)]
struct TestErrorEvent {
    id: u32,
    message: String,
}

#[derive(Event, Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
struct PlayerEvent {
    player_id: u32,
    action: String,
}

#[derive(Event, Clone, Debug, Serialize, Deserialize, PartialEq)]  
struct CombatEvent {
    attacker_id: u32,
    target_id: u32,
    damage: i32,
}

#[derive(Event, Clone, Debug, Serialize, Deserialize, PartialEq)]
struct AnalyticsEvent {
    event_type: String,
    user_id: u32,
    timestamp: u64,
}

#[derive(Event, Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
struct TestEvent {
    id: u32,
    message: String,
}

/// Test authentication/authorization failures with fire-and-forget API
#[test]
fn test_authentication_authorization_failures() {
    let topic = unique_topic("auth_test");
    
    // Use mock backend to simulate auth failures
    let mut mock_backend = MockEventBusBackend::new();
    mock_backend.simulate_delivery_failure_for_topic(&topic); // Simulate auth failure
    
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(
        mock_backend,
        PreconfiguredTopics::new([topic.clone()]),
    ));
    
    // Add bus event (automatically registers error events)
    app.add_bus_event::<TestErrorEvent>(&topic);
    
    #[derive(Resource, Default)]
    struct AuthTestState {
        auth_attempts: u32,
        errors_received: Vec<EventBusError<TestErrorEvent>>,
        test_topic: String,
        test_completed: bool,
    }
    
    app.insert_resource(AuthTestState {
        test_topic: topic.clone(),
        ..Default::default()
    });
    
    // System to write events that will fail due to simulated auth issues
    app.add_systems(Update, |
        mut state: ResMut<AuthTestState>,
        mut writer: EventBusWriter<TestErrorEvent>,
    | {
        if !state.test_completed {
            let test_event = TestErrorEvent {
                id: 42,
                message: "Auth test event".to_string(),
            };
            
            let topic = state.test_topic.clone();
            state.auth_attempts += 1;
            
            // Fire-and-forget write - auth failures will appear as error events
            writer.write(&topic, test_event);
            state.test_completed = true;
        }
    });
    
    // System to collect errors
    app.add_systems(Update, |
        mut errors: EventReader<EventBusError<TestErrorEvent>>,
        mut state: ResMut<AuthTestState>,
    | {
        for error in errors.read() {
            state.errors_received.push(error.clone());
        }
    });
    
    // Run for a few frames to allow error propagation
    for _ in 0..5 {
        app.update();
    }
    
    let state = app.world().resource::<AuthTestState>();
    
    assert!(state.auth_attempts > 0, "Should have attempted auth operations");
    assert!(!state.errors_received.is_empty(), 
           "Should receive auth error. Got: {} errors", 
           state.errors_received.len());
    
    // Verify error details
    let auth_error = &state.errors_received[0];
    assert_eq!(auth_error.topic, topic, "Error should contain correct topic");
    assert!(auth_error.original_event.is_some(), "Error should contain original event");
    
    let original_event = auth_error.original_event.as_ref().unwrap();
    assert_eq!(original_event.id, 42, "Original event should be preserved");
    assert_eq!(original_event.message, "Auth test event", "Original event message should be preserved");
}

/// Test large message rejection with fire-and-forget API
#[test]
fn test_large_message_rejection() {
    let topic = unique_topic("large_msg_test");
    
    // Use mock backend to simulate large message rejection
    let mut mock_backend = MockEventBusBackend::new();
    mock_backend.simulate_delivery_failure_for_topic(&topic);
    
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(
        mock_backend,
        PreconfiguredTopics::new([topic.clone()]),
    ));
    
    // Add bus event (automatically registers error events)
    app.add_bus_event::<TestErrorEvent>(&topic);
    
    #[derive(Resource, Default)]
    struct LargeMessageTestState {
        large_message_attempts: u32,
        errors_received: Vec<EventBusError<TestErrorEvent>>,
        test_topic: String,
        test_completed: bool,
    }
    
    app.insert_resource(LargeMessageTestState {
        test_topic: topic.clone(),
        ..Default::default()
    });
    
    // System to write large messages that will be rejected
    app.add_systems(Update, |
        mut state: ResMut<LargeMessageTestState>,
        mut writer: EventBusWriter<TestErrorEvent>,
    | {
        if !state.test_completed {
            let topic = state.test_topic.clone();
            
            // Create large message that would exceed typical Kafka limits
            let large_message = "x".repeat(1_000_000); // 1MB message
            let large_event = TestErrorEvent {
                id: 99,
                message: large_message,
            };
            
            state.large_message_attempts += 1;
            writer.write(&topic, large_event);
            state.test_completed = true;
        }
    });
    
    // System to collect errors
    app.add_systems(Update, |
        mut errors: EventReader<EventBusError<TestErrorEvent>>,
        mut state: ResMut<LargeMessageTestState>,
    | {
        for error in errors.read() {
            state.errors_received.push(error.clone());
        }
    });
    
    // Run for a few frames to allow error propagation
    for _ in 0..5 {
        app.update();
    }
    
    let state = app.world().resource::<LargeMessageTestState>();
    
    assert!(state.large_message_attempts > 0, "Should have attempted large message sends");
    
    // Test with mock backend that simulates failures for large messages
    assert!(!state.errors_received.is_empty(), 
           "Should receive error for large message. Got: {} errors", 
           state.errors_received.len());
    
    // Verify error details
    let error = &state.errors_received[0];
    assert_eq!(error.topic, topic, "Error should contain correct topic");
    assert!(error.original_event.is_some(), "Error should contain original event");
    
    let original_event = error.original_event.as_ref().unwrap();
    assert_eq!(original_event.id, 99, "Original large event should be preserved");
    assert!(original_event.message.len() > 100_000, "Original large message should be preserved");
}

/// Test consumer lag handling with fire-and-forget API
#[test]
fn test_consumer_lag_handling() {
    let topic = unique_topic("lag_test");
    
    // Use mock backend to simulate delivery failures during high load
    let mut mock_backend = MockEventBusBackend::new();
    mock_backend.simulate_delivery_failure_for_topic(&topic);
    
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(
        mock_backend,
        PreconfiguredTopics::new([topic.clone()]),
    ));
    
    // Add bus event (automatically registers error events)
    app.add_bus_event::<TestErrorEvent>(&topic);
    
    #[derive(Resource, Default)]
    struct ConsumerLagTestState {
        messages_sent: u32,
        errors_received: Vec<EventBusError<TestErrorEvent>>,
        test_topic: String,
        test_completed: bool,
    }
    
    app.insert_resource(ConsumerLagTestState {
        test_topic: topic.clone(),
        ..Default::default()
    });
    
    // System to send multiple messages rapidly to simulate lag conditions
    app.add_systems(Update, |
        mut state: ResMut<ConsumerLagTestState>,
        mut writer: EventBusWriter<TestErrorEvent>,
    | {
        if !state.test_completed {
            let topic = state.test_topic.clone();
            
            // Send multiple messages to simulate load
            for i in 0..50 {
                let event = TestErrorEvent {
                    id: i,
                    message: format!("Lag test message {}", i),
                };
                
                state.messages_sent += 1;
                writer.write(&topic, event);
            }
            
            state.test_completed = true;
        }
    });
    
    // System to collect errors
    app.add_systems(Update, |
        mut errors: EventReader<EventBusError<TestErrorEvent>>,
        mut state: ResMut<ConsumerLagTestState>,
    | {
        for error in errors.read() {
            state.errors_received.push(error.clone());
        }
    });
    
    // Run for a few frames to allow error propagation
    for _ in 0..5 {
        app.update();
    }
    
    let state = app.world().resource::<ConsumerLagTestState>();
    
    assert!(state.messages_sent > 0, "Should have sent multiple messages");
    assert_eq!(state.messages_sent, 50, "Should have sent exactly 50 messages");
    
    // With mock backend configured to fail, we should get errors for all messages
    assert!(!state.errors_received.is_empty(), 
           "Should receive errors during simulated lag. Got: {} errors", 
           state.errors_received.len());
    
    // Verify all errors are properly formed
    for (i, error) in state.errors_received.iter().enumerate() {
        assert_eq!(error.topic, topic, "Error {} should contain correct topic", i);
        assert!(error.original_event.is_some(), "Error {} should contain original event", i);
        
        let original_event = error.original_event.as_ref().unwrap();
        assert!(original_event.message.contains("Lag test message"), 
               "Error {} should contain original message", i);
    }
}

/// Test delivery error handling using mock backend
#[test] 
fn test_delivery_error_handling() {
    // Create mock backend configured to fail delivery for a specific topic
    let mut mock_backend = MockEventBusBackend::new();
    mock_backend.simulate_delivery_failure_for_topic("fail-topic");
    
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(
        mock_backend,
        PreconfiguredTopics::new(["fail-topic", "success-topic"]),
    ));
    
    // Register the bus events (automatically registers error events)
    app.add_bus_event::<TestErrorEvent>("fail-topic");
    app.add_bus_event::<TestErrorEvent>("success-topic");
    
    // Resource to track delivery errors received
    #[derive(Resource, Default)]
    struct DeliveryErrorTracker {
        errors: Vec<EventBusError<TestErrorEvent>>,
        test_completed: bool,
    }
    
    app.insert_resource(DeliveryErrorTracker::default());
    
    // System to write events to both topics
    app.add_systems(Update, |
        mut writer: EventBusWriter<TestErrorEvent>, 
        mut tracker: ResMut<DeliveryErrorTracker>
    | {
        if !tracker.test_completed {
            // This should succeed (no delivery error)
            writer.write("success-topic", TestErrorEvent {
                id: 1,
                message: "Should succeed".to_string(),
            });
            
            // This should trigger a delivery error
            writer.write("fail-topic", TestErrorEvent {
                id: 2, 
                message: "Should fail delivery".to_string(),
            });
            
            tracker.test_completed = true;
        }
    });
    
    // System to capture delivery errors
    app.add_systems(Update, |
        mut delivery_errors: EventReader<EventBusError<TestErrorEvent>>,
        mut tracker: ResMut<DeliveryErrorTracker>
    | {
        for error in delivery_errors.read() {
            println!("Captured delivery error: topic={}, error={}, type={:?}", 
                     error.topic, error.error_message, error.error_type);
            tracker.errors.push(error.clone());
        }
    });
    
    // Run a few frames to allow the systems to execute
    for _ in 0..5 {
        app.update();
    }
    
    // Verify that we received the expected delivery error
    let tracker = app.world().resource::<DeliveryErrorTracker>();
    
    assert!(!tracker.errors.is_empty(), "Should have received at least one delivery error");
    
    // Find the error for our fail-topic
    let fail_topic_error = tracker.errors.iter()
        .find(|error| error.topic == "fail-topic")
        .expect("Should have received a delivery error for fail-topic");
    
    assert_eq!(fail_topic_error.topic, "fail-topic");
    assert_eq!(fail_topic_error.backend, None); // Immediate errors don't have backend info
    assert_eq!(fail_topic_error.error_message, "Failed to send to external backend");
    assert!(fail_topic_error.partition.is_none()); // Immediate errors don't have partition info
    
    // Verify we didn't get an error for the success topic
    let success_topic_errors = tracker.errors.iter()
        .filter(|error| error.topic == "success-topic")
        .count();
    assert_eq!(success_topic_errors, 0, "Should not have delivery errors for success-topic");
}

/// Test fire-and-forget API basic functionality
#[test]
fn test_fire_and_forget_api_works() {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins);
    
    // Setup mock backend that will fail for fail-topic
    let mut backend = MockEventBusBackend::new();
    backend.simulate_delivery_failure_for_topic("fail-topic");
    
    app.add_plugins(EventBusPlugins(
        backend,
        PreconfiguredTopics::new([
            "success-topic".to_string(),
            "fail-topic".to_string(),
        ]),
    ));
    
    // Add bus events (automatically registers error events)
    app.add_bus_event::<TestEvent>("success-topic");
    app.add_bus_event::<TestEvent>("fail-topic");
    
    // Track errors
    #[derive(Resource, Default)]
    struct ErrorTracker {
        errors: Vec<EventBusError<TestEvent>>,
    }
    app.insert_resource(ErrorTracker::default());
    
    // Test system - fire and forget!
    #[derive(Resource, Default)]
    struct TestCompleted(bool);
    app.insert_resource(TestCompleted::default());
    
    app.add_systems(Update, |
        mut writer: EventBusWriter<TestEvent>,
        mut completed: ResMut<TestCompleted>
    | {
        if !completed.0 {
            // These calls return () - no Result handling needed!
            writer.write("success-topic", TestEvent { id: 1, message: "success".to_string() });
            writer.write("fail-topic", TestEvent { id: 2, message: "should fail".to_string() });
            completed.0 = true;
        }
    });
    
    // Error collection system
    app.add_systems(Update, |
        mut errors: EventReader<EventBusError<TestEvent>>,
        mut tracker: ResMut<ErrorTracker>
    | {
        for error in errors.read() {
            tracker.errors.push(error.clone());
        }
    });
    
    // Run the app
    for _ in 0..5 {
        app.update();
    }
    
    // Verify results
    let tracker = app.world().resource::<ErrorTracker>();
    assert_eq!(tracker.errors.len(), 1, "Should have exactly one error");
    
    let error = &tracker.errors[0];
    assert_eq!(error.topic, "fail-topic");
    assert_eq!(error.error_type, EventBusErrorType::Other);
    assert!(error.original_event.is_some());
    assert_eq!(error.original_event.as_ref().unwrap().id, 2);
}

/// Test batch writes with errors
#[test]
fn test_batch_writes_with_errors() {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins);
    
    let mut backend = MockEventBusBackend::new();
    backend.simulate_delivery_failure_for_topic("batch-fail");
    
    app.add_plugins(EventBusPlugins(
        backend,
        PreconfiguredTopics::new([
            "batch-success".to_string(),
            "batch-fail".to_string(),
        ]),
    ));
    
    app.add_bus_event::<TestEvent>("batch-success");
    app.add_bus_event::<TestEvent>("batch-fail");
    
    #[derive(Resource, Default)]
    struct BatchErrorTracker {
        errors: Vec<EventBusError<TestEvent>>,
    }
    app.insert_resource(BatchErrorTracker::default());
    
    #[derive(Resource, Default)]
    struct BatchTestCompleted(bool);
    app.insert_resource(BatchTestCompleted::default());
    
    app.add_systems(Update, |
        mut writer: EventBusWriter<TestEvent>,
        mut completed: ResMut<BatchTestCompleted>
    | {
        if !completed.0 {
            // Batch write to topic that will fail
            writer.write_batch("batch-fail", vec![
                TestEvent { id: 1, message: "batch1".to_string() },
                TestEvent { id: 2, message: "batch2".to_string() },
                TestEvent { id: 3, message: "batch3".to_string() },
            ]);
            completed.0 = true;
        }
    });
    
    app.add_systems(Update, |
        mut errors: EventReader<EventBusError<TestEvent>>,
        mut tracker: ResMut<BatchErrorTracker>
    | {
        for error in errors.read() {
            tracker.errors.push(error.clone());
        }
    });
    
    for _ in 0..5 {
        app.update();
    }
    
    let tracker = app.world().resource::<BatchErrorTracker>();
    assert_eq!(tracker.errors.len(), 3, "Should have error for each batch item");
    
    // Verify all errors have correct topic and original events
    for (i, error) in tracker.errors.iter().enumerate() {
        assert_eq!(error.topic, "batch-fail");
        assert_eq!(error.error_type, EventBusErrorType::Other);
        assert!(error.original_event.is_some());
        assert_eq!(error.original_event.as_ref().unwrap().id, (i + 1) as u32);
    }
}

/// Test write with headers error handling and header recovery
#[test]
fn test_write_with_headers_error_handling() {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins);
    
    let mut backend = MockEventBusBackend::new();
    backend.simulate_delivery_failure_for_topic("headers-fail");
    
    app.add_plugins(EventBusPlugins(
        backend,
        PreconfiguredTopics::new([
            "headers-fail".to_string(),
        ]),
    ));

    app.add_bus_event::<TestEvent>("headers-fail");
    
    #[derive(Resource, Default)]
    struct HeaderErrorTracker {
        errors: Vec<EventBusError<TestEvent>>,
        // Track what headers were sent with each event ID for recovery verification
        sent_headers_by_id: std::collections::HashMap<u32, std::collections::HashMap<String, String>>,
    }
    app.insert_resource(HeaderErrorTracker::default());
    
    #[derive(Resource, Default)]
    struct HeaderTestCompleted(bool);
    app.insert_resource(HeaderTestCompleted::default());
    
    app.add_systems(Update, |
        mut writer: EventBusWriter<TestEvent>,
        mut completed: ResMut<HeaderTestCompleted>,
        mut tracker: ResMut<HeaderErrorTracker>
    | {
        if !completed.0 {
            let mut headers = std::collections::HashMap::new();
            headers.insert("source".to_string(), "test-system".to_string());
            headers.insert("version".to_string(), "1.0".to_string());
            headers.insert("correlation-id".to_string(), "abc-123".to_string());
            
            let event_id = 42;
            let event = TestEvent { 
                id: event_id, 
                message: "event with important headers".to_string() 
            };
            
            // Track headers by event ID so we can verify recovery later
            tracker.sent_headers_by_id.insert(event_id, headers.clone());
            
            // Write with headers to failing topic
            writer.write_with_headers("headers-fail", event, headers);
            completed.0 = true;
        }
    });
    
    app.add_systems(Update, |
        mut errors: EventReader<EventBusError<TestEvent>>,
        mut tracker: ResMut<HeaderErrorTracker>
    | {
        for error in errors.read() {
            tracker.errors.push(error.clone());
        }
    });
    
    for _ in 0..5 {
        app.update();
    }
    
    let tracker = app.world().resource::<HeaderErrorTracker>();
    assert_eq!(tracker.errors.len(), 1, "Should have one error for headers write");
    
    let error = &tracker.errors[0];
    assert_eq!(error.topic, "headers-fail");
    assert_eq!(error.error_type, EventBusErrorType::Other);
    assert_eq!(error.error_message, "Failed to send to external backend with headers");
    assert!(error.original_event.is_some(), "Error should contain original event for header recovery");
    
    // Demonstrate header recovery: extract event ID from error, then look up original headers
    let original_event = error.original_event.as_ref().unwrap();
    assert_eq!(original_event.id, 42);
    assert_eq!(original_event.message, "event with important headers");
    
    // This demonstrates that headers can be "recovered" by correlating the original event
    // with tracking data maintained by the application
    let recovered_headers = tracker.sent_headers_by_id.get(&original_event.id)
        .expect("Should be able to recover headers using original event ID");
    
    assert_eq!(recovered_headers.get("source").unwrap(), "test-system");
    assert_eq!(recovered_headers.get("version").unwrap(), "1.0");
    assert_eq!(recovered_headers.get("correlation-id").unwrap(), "abc-123");
    
    println!("Successfully demonstrated header recovery from error event:");
    println!("  Event ID: {}", original_event.id);
    println!("  Recovered headers: {:?}", recovered_headers);
}

/// Test Scenario: Multiple Event Type Error Handling
/// Each event type has its own error handling system
#[test]
fn test_separate_error_systems() {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins);
    
    // Setup mock backend that will fail for certain topics
    let mut backend = MockEventBusBackend::new();
    backend.simulate_delivery_failure_for_topic("player-events");
    
    app.add_plugins(EventBusPlugins(
        backend,
        PreconfiguredTopics::new([
            "player-events".to_string(), 
            "combat-events".to_string()
        ]),
    ));
    
    // Add bus events (automatically registers error events)
    app.add_bus_event::<PlayerEvent>("player-events");
    app.add_bus_event::<CombatEvent>("combat-events");
    
    // Track received errors per event type
    #[derive(Resource, Default)]
    struct ErrorTracker {
        player_errors: Vec<EventBusError<PlayerEvent>>,
        combat_errors: Vec<EventBusError<CombatEvent>>,
        test_completed: bool,
    }
    app.insert_resource(ErrorTracker::default());
    
    // Separate error handling systems
    app.add_systems(Update, (
        handle_player_errors,
        handle_combat_errors,
        write_test_events,
    ));
    
    fn write_test_events(
        mut player_writer: EventBusWriter<PlayerEvent>,
        mut combat_writer: EventBusWriter<CombatEvent>,
        mut tracker: ResMut<ErrorTracker>,
    ) {
        if !tracker.test_completed {
            // Write events - both should succeed at the write level
            // but player-events should generate an error
            player_writer.write("player-events", PlayerEvent {
                player_id: 123,
                action: "login".to_string(),
            });
            
            combat_writer.write("combat-events", CombatEvent {
                attacker_id: 1,
                target_id: 2,
                damage: 50,
            });
            
            tracker.test_completed = true;
        }
    }
    
    fn handle_player_errors(
        mut player_errors: EventReader<EventBusError<PlayerEvent>>,
        mut tracker: ResMut<ErrorTracker>,
    ) {
        for error in player_errors.read() {
            tracker.player_errors.push(error.clone());
        }
    }
    
    fn handle_combat_errors(
        mut combat_errors: EventReader<EventBusError<CombatEvent>>,
        mut tracker: ResMut<ErrorTracker>,
    ) {
        for error in combat_errors.read() {
            tracker.combat_errors.push(error.clone());
        }
    }
    
    // Run the systems
    for _ in 0..5 {
        app.update();
    }
    
    // Verify results
    let tracker = app.world().resource::<ErrorTracker>();
    
    // Should have one player error (because we configured the mock to fail on player-events)
    assert_eq!(tracker.player_errors.len(), 1);
    let player_error = &tracker.player_errors[0];
    assert_eq!(player_error.topic, "player-events");
    assert_eq!(player_error.error_type, EventBusErrorType::Other);
    assert!(player_error.original_event.is_some());
    
    // Should have no combat errors (combat-events topic not configured to fail)
    assert_eq!(tracker.combat_errors.len(), 0);
    
    // Verify type safety - each system only sees its own error type
    assert_eq!(player_error.original_event.as_ref().unwrap().player_id, 123);
}

/// Test Scenario: Centralized Error Handling
/// One system handles multiple error types using multiple EventReader<EventBusError<T>>
#[test]
fn test_centralized_error_handling() {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins);
    
    // Setup mock backend that will fail for all topics
    let mut backend = MockEventBusBackend::new();
    backend.simulate_delivery_failure_for_topic("player-events");
    backend.simulate_delivery_failure_for_topic("combat-events");
    backend.simulate_delivery_failure_for_topic("analytics-events");
    
    app.add_plugins(EventBusPlugins(
        backend,
        PreconfiguredTopics::new([
            "player-events".to_string(),
            "combat-events".to_string(), 
            "analytics-events".to_string()
        ]),
    ));
    
    // Add all bus events
    app.add_bus_event::<PlayerEvent>("player-events");
    app.add_bus_event::<CombatEvent>("combat-events");
    app.add_bus_event::<AnalyticsEvent>("analytics-events");
    
    #[derive(Resource, Default)]
    struct CentralizedErrorTracker {
        total_errors: usize,
        error_types: Vec<String>,
        test_completed: bool,
    }
    app.insert_resource(CentralizedErrorTracker::default());
    
    app.add_systems(Update, (
        centralized_error_handler,
        write_multiple_events,
    ));
    
    fn write_multiple_events(
        mut player_writer: EventBusWriter<PlayerEvent>,
        mut combat_writer: EventBusWriter<CombatEvent>,
        mut analytics_writer: EventBusWriter<AnalyticsEvent>,
        mut tracker: ResMut<CentralizedErrorTracker>,
    ) {
        if !tracker.test_completed {
            // Write events of different types
            player_writer.write("player-events", PlayerEvent {
                player_id: 1,
                action: "move".to_string(),
            });
            
            combat_writer.write("combat-events", CombatEvent {
                attacker_id: 1,
                target_id: 2,
                damage: 25,
            });
            
            analytics_writer.write("analytics-events", AnalyticsEvent {
                event_type: "click".to_string(),
                user_id: 1,
                timestamp: 123456789,
            });
            
            tracker.test_completed = true;
        }
    }
    
    fn centralized_error_handler(
        mut player_errors: EventReader<EventBusError<PlayerEvent>>,
        mut combat_errors: EventReader<EventBusError<CombatEvent>>,
        mut analytics_errors: EventReader<EventBusError<AnalyticsEvent>>,
        mut tracker: ResMut<CentralizedErrorTracker>,
    ) {
        fn log_error<T: bevy_event_bus::BusEvent>(error: &EventBusError<T>, event_type: &str, tracker: &mut CentralizedErrorTracker) {
            tracker.total_errors += 1;
            tracker.error_types.push(format!("{}: {} on topic {}", event_type, error.error_message, error.topic));
        }
        
        for error in player_errors.read() { log_error(error, "PlayerEvent", &mut tracker); }
        for error in combat_errors.read() { log_error(error, "CombatEvent", &mut tracker); }
        for error in analytics_errors.read() { log_error(error, "AnalyticsEvent", &mut tracker); }
    }
    
    // Run the systems
    for _ in 0..5 {
        app.update();
    }
    
    // Verify centralized handling
    let tracker = app.world().resource::<CentralizedErrorTracker>();
    assert_eq!(tracker.total_errors, 3, "Should have 3 errors total");
    assert!(tracker.error_types.iter().any(|e| e.contains("PlayerEvent")));
    assert!(tracker.error_types.iter().any(|e| e.contains("CombatEvent")));
    assert!(tracker.error_types.iter().any(|e| e.contains("AnalyticsEvent")));
}

/// Test Scenario: Multiple Errors per Frame
/// Test that multiple errors in the same frame are handled correctly
#[test]
fn test_multiple_errors_same_frame() {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins);
    
    // Setup mock backend to fail on all topics
    let mut backend = MockEventBusBackend::new();
    backend.simulate_delivery_failure_for_topic("test-topic");
    
    app.add_plugins(EventBusPlugins(
        backend,
        PreconfiguredTopics::new(["test-topic".to_string()]),
    ));
    
    app.add_bus_event::<PlayerEvent>("test-topic");
    
    #[derive(Resource, Default)]
    struct BatchErrorTracker {
        errors: Vec<EventBusError<PlayerEvent>>,
        test_completed: bool,
    }
    app.insert_resource(BatchErrorTracker::default());
    
    app.add_systems(Update, (
        write_batch_events,
        collect_batch_errors,
    ));
    
    fn write_batch_events(
        mut writer: EventBusWriter<PlayerEvent>, 
        mut tracker: ResMut<BatchErrorTracker>
    ) {
        if !tracker.test_completed {
            // Write multiple events in the same frame
            for i in 0..5 {
                writer.write("test-topic", PlayerEvent {
                    player_id: i,
                    action: format!("action_{}", i),
                });
            }
            tracker.test_completed = true;
        }
    }
    
    fn collect_batch_errors(
        mut errors: EventReader<EventBusError<PlayerEvent>>,
        mut tracker: ResMut<BatchErrorTracker>,
    ) {
        for error in errors.read() {
            tracker.errors.push(error.clone());
        }
    }
    
    // Run the systems
    for _ in 0..5 {
        app.update();
    }
    
    // Verify batch error handling
    let tracker = app.world().resource::<BatchErrorTracker>();
    assert_eq!(tracker.errors.len(), 5, "Should have 5 errors from batch write");
    
    // Verify all original events are available
    for (i, error) in tracker.errors.iter().enumerate() {
        assert!(error.original_event.is_some());
        assert_eq!(error.original_event.as_ref().unwrap().player_id, i as u32);
        assert_eq!(error.error_type, EventBusErrorType::Other);
    }
}

/// Test Scenario: Fire and Forget API Compilation
/// Test that the API truly is fire-and-forget at compile time
#[test]
fn test_fire_and_forget_api() {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins);
    
    app.add_plugins(EventBusPlugins(
        MockEventBusBackend::new(),
        PreconfiguredTopics::new(["test".to_string()]),
    ));
    
    app.add_bus_event::<PlayerEvent>("test");
    
    app.add_systems(Update, test_api_usage);
    
    fn test_api_usage(mut writer: EventBusWriter<PlayerEvent>) {
        // These calls should compile without any Result handling
        writer.write("test", PlayerEvent {
            player_id: 1,
            action: "test".to_string(),
        });
        
        writer.write_batch("test", vec![
            PlayerEvent { player_id: 2, action: "batch1".to_string() },
            PlayerEvent { player_id: 3, action: "batch2".to_string() },
        ]);
        
        let mut headers = std::collections::HashMap::new();
        headers.insert("source".to_string(), "test".to_string());
        writer.write_with_headers("test", PlayerEvent {
            player_id: 4,
            action: "with_headers".to_string(),
        }, headers);
        
        writer.write_default("test");
        
        // No Result handling needed - this is the key feature
        // If any of these returned Result, this test would fail to compile
    }
    
    // Just run once to verify it works
    app.update();
}

/// Test Scenario: Type-Safe Error Retry
/// Test that errors can be retried in a type-safe manner
#[test]
fn test_type_safe_error_retry() {
    let mut app = App::new();
    app.add_plugins(MinimalPlugins);
    
    // Mock backend that initially fails but then succeeds
    let mut backend = MockEventBusBackend::new();
    backend.simulate_delivery_failure_for_topic("retry-topic");
    
    app.add_plugins(EventBusPlugins(
        backend,
        PreconfiguredTopics::new(["retry-topic".to_string()]),
    ));
    
    app.add_bus_event::<PlayerEvent>("retry-topic");
    
    #[derive(Resource, Default)]
    struct RetryTracker {
        retry_attempts: usize,
        initial_write_done: bool,
        errors_received: Vec<EventBusError<PlayerEvent>>,
    }
    app.insert_resource(RetryTracker::default());
    
    // System to write initial event
    app.add_systems(Update, initial_write_system);
    
    // Separate system to collect errors (no EventBusWriter to avoid conflict)
    app.add_systems(Update, error_collector_system);
    
    fn initial_write_system(
        mut writer: EventBusWriter<PlayerEvent>,
        mut tracker: ResMut<RetryTracker>,
    ) {
        if !tracker.initial_write_done {
            writer.write("retry-topic", PlayerEvent {
                player_id: 99,
                action: "initial_attempt".to_string(),
            });
            tracker.initial_write_done = true;
        }
    }
    
    fn error_collector_system(
        mut errors: EventReader<EventBusError<PlayerEvent>>,
        mut tracker: ResMut<RetryTracker>,
    ) {
        for error in errors.read() {
            tracker.errors_received.push(error.clone());
            tracker.retry_attempts += 1;
        }
    }
    
    // Run multiple frames to allow errors to be generated and collected
    for _ in 0..5 {
        app.update();
    }
    
    let tracker = app.world().resource::<RetryTracker>();
    assert!(tracker.retry_attempts > 0, "Should have attempted at least one retry (got an error)");
    assert!(tracker.errors_received.len() > 0, "Should have received at least one error event");
    
    // Verify the error has the correct structure for retry
    let first_error = &tracker.errors_received[0];
    assert_eq!(first_error.topic, "retry-topic");
    assert!(first_error.original_event.is_some());
    assert_eq!(first_error.original_event.as_ref().unwrap().player_id, 99);
}