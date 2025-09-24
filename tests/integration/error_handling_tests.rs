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
use bevy_event_bus::{EventBusError, EventBusAppExt, KafkaProducerConfig};
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

/// Test delivery error handling using mock backend
#[test] 
fn test_delivery_error_handling() {
    let topic = unique_topic("error_test");
    let topic_clone = topic.clone();
    
    // Use mock backend that simulates delivery failures
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
    struct ErrorTestState {
        messages_sent: u32,
        errors_received: Vec<EventBusError<TestErrorEvent>>,
        test_completed: bool,
    }
    
    app.insert_resource(ErrorTestState::default());
    
    // System to write events that will fail due to simulated backend issues
    app.add_systems(Update, move |
        mut state: ResMut<ErrorTestState>,
        mut writer: EventBusWriter<TestErrorEvent>,
    | {
        if !state.test_completed {
            for i in 0..3 {
                let test_event = TestErrorEvent {
                    id: i,
                    message: format!("Test message {}", i),
                };
                
                state.messages_sent += 1;
                
                // Fire-and-forget write - delivery failures will appear as error events
                let config = KafkaProducerConfig::new("localhost:9092", [topic_clone.clone()]);
                writer.write(&config, test_event);
            }
            
            state.test_completed = true;
        }
    });
    
    // System to collect errors
    app.add_systems(Update, |
        mut errors: EventReader<EventBusError<TestErrorEvent>>,
        mut state: ResMut<ErrorTestState>,
    | {
        for error in errors.read() {
            state.errors_received.push(error.clone());
        }
    });
    
    // Run for a few frames to allow error propagation
    for _ in 0..5 {
        app.update();
    }
    
    let state = app.world().resource::<ErrorTestState>();
    
    assert!(state.messages_sent > 0, "Should have sent messages");
    assert_eq!(state.messages_sent, 3, "Should have sent exactly 3 messages");
    
    // With mock backend configured to fail, we should get errors for all messages
    assert_eq!(state.errors_received.len(), 3, 
              "Should receive exactly 3 errors. Got: {} errors", 
              state.errors_received.len());
    
    // Verify error details
    for (i, error) in state.errors_received.iter().enumerate() {
        assert_eq!(error.topic, topic, "Error {} should contain correct topic", i);
        assert!(error.original_event.is_some(), "Error {} should contain original event", i);
        
        let original_event = error.original_event.as_ref().unwrap();
        assert_eq!(original_event.id, i as u32, "Error {} should preserve original event id", i);
        assert_eq!(original_event.message, format!("Test message {}", i), 
                  "Error {} should preserve original message", i);
    }
}

/// Test error handling for multiple event types simultaneously
#[test]
fn test_multiple_event_types_error_handling() {
    let player_topic = unique_topic("player_events");
    let combat_topic = unique_topic("combat_events");
    let analytics_topic = unique_topic("analytics_events");
    
    let player_topic_clone = player_topic.clone();
    let combat_topic_clone = combat_topic.clone();
    let analytics_topic_clone = analytics_topic.clone();
    
    // Use mock backend that simulates failures for all topics
    let mut mock_backend = MockEventBusBackend::new();
    mock_backend.simulate_delivery_failure_for_topic(&player_topic);
    mock_backend.simulate_delivery_failure_for_topic(&combat_topic);
    mock_backend.simulate_delivery_failure_for_topic(&analytics_topic);
    
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(
        mock_backend,
        PreconfiguredTopics::new([player_topic.clone(), combat_topic.clone(), analytics_topic.clone()]),
    ));
    
    // Add all event types (automatically registers error events for each)
    app.add_bus_event::<PlayerEvent>(&player_topic);
    app.add_bus_event::<CombatEvent>(&combat_topic);
    app.add_bus_event::<AnalyticsEvent>(&analytics_topic);
    
    #[derive(Resource, Default)]
    struct MultiEventTestState {
        player_errors: Vec<EventBusError<PlayerEvent>>,
        combat_errors: Vec<EventBusError<CombatEvent>>,
        analytics_errors: Vec<EventBusError<AnalyticsEvent>>,
        test_completed: bool,
    }
    
    app.insert_resource(MultiEventTestState::default());
    
    // System to write events to all topics
    app.add_systems(Update, move |
        mut state: ResMut<MultiEventTestState>,
        mut player_writer: EventBusWriter<PlayerEvent>,
        mut combat_writer: EventBusWriter<CombatEvent>,
        mut analytics_writer: EventBusWriter<AnalyticsEvent>,
    | {
        if !state.test_completed {
            // Player event
            let player_event = PlayerEvent {
                player_id: 123,
                action: "login".to_string(),
            };
            player_writer.write(&KafkaProducerConfig::new("localhost:9092", [player_topic_clone.clone()]), player_event);
            
            // Combat event  
            let combat_event = CombatEvent {
                attacker_id: 456,
                target_id: 789,
                damage: 100,
            };
            combat_writer.write(&KafkaProducerConfig::new("localhost:9092", [combat_topic_clone.clone()]), combat_event);
            
            // Analytics event
            let analytics_event = AnalyticsEvent {
                event_type: "user_action".to_string(),
                user_id: 123,
                timestamp: 1234567890,
            };
            analytics_writer.write(&KafkaProducerConfig::new("localhost:9092", [analytics_topic_clone.clone()]), analytics_event);
            
            state.test_completed = true;
        }
    });
    
    // Systems to collect errors for each event type
    app.add_systems(Update, (
        |mut player_errors: EventReader<EventBusError<PlayerEvent>>, mut state: ResMut<MultiEventTestState>| {
            for error in player_errors.read() {
                state.player_errors.push(error.clone());
            }
        },
        |mut combat_errors: EventReader<EventBusError<CombatEvent>>, mut state: ResMut<MultiEventTestState>| {
            for error in combat_errors.read() {
                state.combat_errors.push(error.clone());
            }
        },
        |mut analytics_errors: EventReader<EventBusError<AnalyticsEvent>>, mut state: ResMut<MultiEventTestState>| {
            for error in analytics_errors.read() {
                state.analytics_errors.push(error.clone());
            }
        },
    ));
    
    // Run for a few frames to allow error propagation
    for _ in 0..5 {
        app.update();
    }
    
    let state = app.world().resource::<MultiEventTestState>();
    
    // Verify we got errors for all event types
    assert_eq!(state.player_errors.len(), 1, "Should receive 1 player error");
    assert_eq!(state.combat_errors.len(), 1, "Should receive 1 combat error");
    assert_eq!(state.analytics_errors.len(), 1, "Should receive 1 analytics error");
    
    // Verify error details for each type
    let player_error = &state.player_errors[0];
    assert_eq!(player_error.topic, player_topic);
    assert!(player_error.original_event.is_some());
    assert_eq!(player_error.original_event.as_ref().unwrap().player_id, 123);
    
    let combat_error = &state.combat_errors[0];
    assert_eq!(combat_error.topic, combat_topic);
    assert!(combat_error.original_event.is_some());
    assert_eq!(combat_error.original_event.as_ref().unwrap().damage, 100);
    
    let analytics_error = &state.analytics_errors[0];
    assert_eq!(analytics_error.topic, analytics_topic);
    assert!(analytics_error.original_event.is_some());
    assert_eq!(analytics_error.original_event.as_ref().unwrap().user_id, 123);
}

/// Test centralized error handling pattern with mixed success/failure
#[test]
fn test_centralized_error_handling() {
    let working_topic = unique_topic("working_topic");
    let failing_topic = unique_topic("failing_topic");
    
    let working_topic_clone = working_topic.clone();
    let failing_topic_clone = failing_topic.clone();
    let failing_topic_assert = failing_topic.clone();
    
    // Use mock backend where only one topic fails
    let mut mock_backend = MockEventBusBackend::new();
    mock_backend.simulate_delivery_failure_for_topic(&failing_topic);
    // working_topic is not configured to fail, so it should succeed
    
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(
        mock_backend,
        PreconfiguredTopics::new([working_topic.clone(), failing_topic.clone()]),
    ));
    
    // Add bus events for both topics
    app.add_bus_event::<TestEvent>(&working_topic);
    app.add_bus_event::<TestEvent>(&failing_topic);
    
    #[derive(Resource, Default)]
    struct CentralizedErrorTestState {
        events_sent: u32,
        all_errors: Vec<EventBusError<TestEvent>>,
        error_counts_by_topic: std::collections::HashMap<String, u32>,
        test_completed: bool,
    }
    
    app.insert_resource(CentralizedErrorTestState::default());
    
    // System to send events to both topics
    app.add_systems(Update, move |
        mut state: ResMut<CentralizedErrorTestState>,
        mut writer: EventBusWriter<TestEvent>,
    | {
        if !state.test_completed {
            // Send to working topic
            for i in 0..3 {
                let event = TestEvent {
                    id: i,
                    message: format!("Working event {}", i),
                };
                state.events_sent += 1;
                writer.write(&KafkaProducerConfig::new("localhost:9092", [working_topic_clone.clone()]), event);
            }
            
            // Send to failing topic
            for i in 0..3 {
                let event = TestEvent {
                    id: i + 100,
                    message: format!("Failing event {}", i),
                };
                state.events_sent += 1;
                writer.write(&KafkaProducerConfig::new("localhost:9092", [failing_topic_clone.clone()]), event);
            }
            
            state.test_completed = true;
        }
    });
    
    // Centralized error handling system
    app.add_systems(Update, |
        mut errors: EventReader<EventBusError<TestEvent>>,
        mut state: ResMut<CentralizedErrorTestState>,
    | {
        for error in errors.read() {
            state.all_errors.push(error.clone());
            let count = state.error_counts_by_topic.entry(error.topic.clone()).or_insert(0);
            *count += 1;
        }
    });
    
    // Run for a few frames
    for _ in 0..5 {
        app.update();
    }
    
    let state = app.world().resource::<CentralizedErrorTestState>();
    
    assert_eq!(state.events_sent, 6, "Should have sent 6 events total");
    
    // Only failing topic should generate errors
    assert_eq!(state.all_errors.len(), 3, "Should have 3 errors from failing topic");
    assert_eq!(state.error_counts_by_topic.len(), 1, "Should have errors from only 1 topic");
    assert_eq!(state.error_counts_by_topic.get(&failing_topic_assert), Some(&3), 
              "Should have 3 errors from failing topic");
    
    // All errors should be from failing topic with correct event data
    for error in &state.all_errors {
        assert_eq!(error.topic, failing_topic_assert, "All errors should be from failing topic");
        assert!(error.original_event.is_some(), "All errors should contain original event");
        let original = error.original_event.as_ref().unwrap();
        assert!(original.id >= 100, "Original event should have failing topic ID range");
        assert!(original.message.contains("Failing event"), "Original event should have failing message");
    }
}

/// Test batch operation error handling
#[test]
fn test_batch_operation_error_handling() {
    let topic = unique_topic("batch_test");
    let topic_clone = topic.clone();
    
    // Use mock backend that fails for this topic
    let mut mock_backend = MockEventBusBackend::new();
    mock_backend.simulate_delivery_failure_for_topic(&topic);
    
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(
        mock_backend,
        PreconfiguredTopics::new([topic.clone()]),
    ));
    
    app.add_bus_event::<TestEvent>(&topic);
    
    #[derive(Resource, Default)]
    struct BatchTestState {
        batch_operations: u32,
        events_per_batch: u32,
        total_errors: Vec<EventBusError<TestEvent>>,
        test_completed: bool,
    }
    
    app.insert_resource(BatchTestState::default());
    
    // System to send events in batches
    app.add_systems(Update, move |
        mut state: ResMut<BatchTestState>,
        mut writer: EventBusWriter<TestEvent>,
    | {
        if !state.test_completed {
            let events_per_batch = 5;
            let num_batches = 3;
            
            state.events_per_batch = events_per_batch;
            
            for batch in 0..num_batches {
                state.batch_operations += 1;
                
                // Send a batch of events
                for event_in_batch in 0..events_per_batch {
                    let event = TestEvent {
                        id: batch * events_per_batch + event_in_batch,
                        message: format!("Batch {} Event {}", batch, event_in_batch),
                    };
                    writer.write(&KafkaProducerConfig::new("localhost:9092", [topic_clone.clone()]), event);
                }
            }
            
            state.test_completed = true;
        }
    });
    
    // System to collect all errors
    app.add_systems(Update, |
        mut errors: EventReader<EventBusError<TestEvent>>,
        mut state: ResMut<BatchTestState>,
    | {
        for error in errors.read() {
            state.total_errors.push(error.clone());
        }
    });
    
    // Run for a few frames
    for _ in 0..5 {
        app.update();
    }
    
    let state = app.world().resource::<BatchTestState>();
    
    let expected_total_events = state.batch_operations * state.events_per_batch;
    
    assert_eq!(state.batch_operations, 3, "Should have completed 3 batch operations");
    assert_eq!(state.events_per_batch, 5, "Should have 5 events per batch");
    assert_eq!(expected_total_events, 15, "Should calculate 15 total events");
    
    // All events should fail since the topic is configured to fail
    assert_eq!(state.total_errors.len(), expected_total_events as usize,
              "Should receive error for every event sent. Expected: {}, Got: {}", 
              expected_total_events, state.total_errors.len());
    
    // Verify each error contains the correct batch and event information
    for (i, error) in state.total_errors.iter().enumerate() {
        assert_eq!(error.topic, topic, "Error {} should have correct topic", i);
        assert!(error.original_event.is_some(), "Error {} should contain original event", i);
        
        let original = error.original_event.as_ref().unwrap();
        assert_eq!(original.id, i as u32, "Error {} should preserve event ID", i);
        assert!(original.message.contains("Batch"), "Error {} should preserve batch message", i);
    }
}

/// Test error retry mechanism pattern
#[test]
fn test_error_retry_mechanism() {
    let topic = unique_topic("retry_test");
    let topic_clone1 = topic.clone();
    let topic_clone2 = topic.clone();
    
    // Use mock backend that fails for this topic
    let mut mock_backend = MockEventBusBackend::new();
    mock_backend.simulate_delivery_failure_for_topic(&topic);
    
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(
        mock_backend,
        PreconfiguredTopics::new([topic.clone()]),
    ));
    
    app.add_bus_event::<TestEvent>(&topic);
    
    #[derive(Resource, Default)]
    struct RetryTestState {
        initial_events_sent: u32,
        retry_attempts: u32,
        max_retries: u32,
        errors_received: Vec<EventBusError<TestEvent>>,
        retried_event_ids: std::collections::HashSet<u32>,
        initial_send_complete: bool,
    }
    
    app.insert_resource(RetryTestState {
        max_retries: 3,
        ..Default::default()
    });
    
    // System to send initial events
    app.add_systems(Update, move |
        mut state: ResMut<RetryTestState>,
        mut writer: EventBusWriter<TestEvent>,
    | {
        if !state.initial_send_complete {
            // Send initial events
            for i in 0..3 {
                let event = TestEvent {
                    id: i,
                    message: format!("Initial event {}", i),
                };
                state.initial_events_sent += 1;
                writer.write(&KafkaProducerConfig::new("localhost:9092", [topic_clone1.clone()]), event);
            }
            state.initial_send_complete = true;
        }
    });
    
    // System to handle errors and implement retry logic
    app.add_systems(Update, move |
        mut errors: EventReader<EventBusError<TestEvent>>,
        mut state: ResMut<RetryTestState>,
        mut writer: EventBusWriter<TestEvent>,
    | {
        for error in errors.read() {
            state.errors_received.push(error.clone());
            
            // Extract original event
            if let Some(original_event) = &error.original_event {
                let event_id = original_event.id;
                
                // Check if we should retry (haven't exceeded max retries for this event)
                if state.retry_attempts < state.max_retries && !state.retried_event_ids.contains(&event_id) {
                    // Mark this event as retried
                    state.retried_event_ids.insert(event_id);
                    state.retry_attempts += 1;
                    
                    // Create retry event with modified message
                    let retry_event = TestEvent {
                        id: original_event.id,
                        message: format!("{} (retry {})", original_event.message, state.retry_attempts),
                    };
                    
                    // Retry the event (will still fail in this test since mock backend is configured to fail)
                    writer.write(&KafkaProducerConfig::new("localhost:9092", [topic_clone2.clone()]), retry_event);
                }
            }
        }
    });
    
    // Run for several frames to allow error/retry cycles
    for _ in 0..10 {
        app.update();
    }
    
    let state = app.world().resource::<RetryTestState>();
    
    assert_eq!(state.initial_events_sent, 3, "Should have sent 3 initial events");
    assert_eq!(state.retry_attempts, 3, "Should have made 3 retry attempts");
    assert_eq!(state.retried_event_ids.len(), 3, "Should have retried all 3 event IDs");
    
    // We should have errors for both initial attempts and retries
    // Initial: 3 errors, Retries: 3 errors = 6 total errors
    assert_eq!(state.errors_received.len(), 6, 
              "Should receive 6 total errors (3 initial + 3 retries). Got: {}", 
              state.errors_received.len());
    
    // Verify we have both original and retry attempts
    let initial_errors: Vec<_> = state.errors_received.iter()
        .filter(|e| e.original_event.as_ref().map_or(false, |ev| !ev.message.contains("retry")))
        .collect();
    let retry_errors: Vec<_> = state.errors_received.iter()
        .filter(|e| e.original_event.as_ref().map_or(false, |ev| ev.message.contains("retry")))
        .collect();
    
    assert_eq!(initial_errors.len(), 3, "Should have 3 initial errors");
    assert_eq!(retry_errors.len(), 3, "Should have 3 retry errors");
    
    // Verify retry events have correct format
    for retry_error in retry_errors {
        assert!(retry_error.original_event.as_ref().unwrap().message.contains("retry"),
               "Retry error should contain retry message");
    }
}