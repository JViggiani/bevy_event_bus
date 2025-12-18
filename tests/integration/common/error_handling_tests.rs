//! Comprehensive error handling tests for bevy_event_bus
//!
//! This module tests the library's resilience and error handling capabilities using
//! the new fire-and-forget API where KafkaMessageWriter methods return () and errors
//! are surfaced via error callbacks.
//!
//! Tests cover:
//! - Mock backend delivery failures
//! - Type-safe error handling with multiple event types
//! - Centralized vs distributed error handling patterns
//! - Batch operations with errors
//! - Error retry mechanisms

use bevy::prelude::*;
use bevy_event_bus::config::kafka::{KafkaProducerConfig, KafkaTopologyEventBinding};
use bevy_event_bus::{BusErrorCallback, BusErrorContext, BusErrorKind, EventBusPlugins, KafkaMessageWriter};
use integration_tests::utils::MockEventBusBackend;
use integration_tests::utils::helpers::unique_topic;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
struct ErrorStore {
    callback: BusErrorCallback,
    store: Arc<Mutex<Vec<BusErrorContext>>>,
}

impl ErrorStore {
    fn new() -> Self {
        let store = Arc::new(Mutex::new(Vec::new()));
        let callback: BusErrorCallback = {
            let sink = Arc::clone(&store);
            Arc::new(move |ctx| {
                sink.lock().unwrap().push(ctx);
            })
        };
        Self { callback, store }
    }

    fn collected(&self) -> Vec<BusErrorContext> {
        self.store.lock().unwrap().clone()
    }

    fn drain(&self) -> Vec<BusErrorContext> {
        let mut guard = self.store.lock().unwrap();
        std::mem::take(&mut *guard)
    }
}

fn decode_event<T: DeserializeOwned>(ctx: &BusErrorContext) -> Option<T> {
    ctx.original_bytes
        .as_ref()
        .and_then(|bytes| serde_json::from_slice(bytes).ok())
}

#[derive(Resource, Clone)]
struct ErrorStoreResource(ErrorStore);

// Event types for various test scenarios
#[derive(Message, Deserialize, Serialize, Debug, Clone, PartialEq)]
struct TestErrorEvent {
    id: u32,
    message: String,
}

#[derive(Message, Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
struct PlayerEvent {
    player_id: u32,
    action: String,
}

#[derive(Message, Clone, Debug, Serialize, Deserialize, PartialEq)]
struct CombatEvent {
    attacker_id: u32,
    target_id: u32,
    damage: i32,
}

#[derive(Message, Clone, Debug, Serialize, Deserialize, PartialEq)]
struct AnalyticsEvent {
    event_type: String,
    user_id: u32,
    timestamp: u64,
}

#[derive(Message, Clone, Debug, Serialize, Deserialize, PartialEq, Default)]
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
    app.add_plugins(EventBusPlugins(mock_backend));

    // Register event bindings for the mock backend scenario
    KafkaTopologyEventBinding::new::<TestErrorEvent>(vec![topic.clone()]).apply(&mut app);

    #[derive(Resource, Default)]
    struct ErrorTestState {
        messages_sent: u32,
        test_completed: bool,
    }

    let errors = ErrorStore::new();
    app.insert_resource(ErrorStoreResource(errors.clone()));
    app.insert_resource(ErrorTestState::default());

    // System to write events that will fail due to simulated backend issues
    app.add_systems(
        Update,
        move |mut state: ResMut<ErrorTestState>, mut writer: KafkaMessageWriter, errors: Res<ErrorStoreResource>| {
            if !state.test_completed {
                let config = KafkaProducerConfig::new([topic_clone.clone()]);
                for i in 0..3 {
                    let test_event = TestErrorEvent {
                        id: i,
                        message: format!("Test message {}", i),
                    };

                    state.messages_sent += 1;

                    // Fire-and-forget write - delivery failures will appear as error events
                    writer.write(&config, test_event, Some(errors.0.callback.clone()));
                }

                state.test_completed = true;
            }
        },
    );

    // Run for a few frames to allow error propagation
    for _ in 0..5 {
        app.update();
    }
    let state = app.world().resource::<ErrorTestState>();
    let errors_collected = errors.collected();

    assert!(state.messages_sent > 0, "Should have sent messages");
    assert_eq!(
        state.messages_sent, 3,
        "Should have sent exactly 3 messages"
    );

    // With mock backend configured to fail, we should get errors for all messages
    assert_eq!(errors_collected.len(), 3, "Should receive exactly 3 errors. Got: {} errors", errors_collected.len());

    // Verify error details
    for (i, error) in errors_collected.iter().enumerate() {
        assert_eq!(error.topic, topic, "Error {} should contain correct topic", i);
        assert_eq!(error.kind, BusErrorKind::DeliveryFailure);
        assert!(error.original_bytes.is_some(), "Error {} should include serialized payload", i);
        // best-effort check the payload encodes the id we sent
        let payload_text = String::from_utf8_lossy(error.original_bytes.as_ref().unwrap());
        assert!(payload_text.contains(&format!("{}", i)), "Payload should contain event id {i}");
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
    app.add_plugins(EventBusPlugins(mock_backend));

    // Register all event types through topology bindings (includes error events)
    KafkaTopologyEventBinding::new::<PlayerEvent>(vec![player_topic.clone()]).apply(&mut app);
    KafkaTopologyEventBinding::new::<CombatEvent>(vec![combat_topic.clone()]).apply(&mut app);
    KafkaTopologyEventBinding::new::<AnalyticsEvent>(vec![analytics_topic.clone()]).apply(&mut app);

    #[derive(Resource, Default)]
    struct MultiEventTestState {
        test_completed: bool,
    }

    let errors = ErrorStore::new();
    app.insert_resource(ErrorStoreResource(errors.clone()));
    app.insert_resource(MultiEventTestState::default());

    // System to write events to all topics
    app.add_systems(
        Update,
          move |mut state: ResMut<MultiEventTestState>,
              mut player_writer: KafkaMessageWriter,
              mut combat_writer: KafkaMessageWriter,
              mut analytics_writer: KafkaMessageWriter,
              errors: Res<ErrorStoreResource>| {
            if !state.test_completed {
                let player_config = KafkaProducerConfig::new([player_topic_clone.clone()]);
                let combat_config = KafkaProducerConfig::new([combat_topic_clone.clone()]);
                let analytics_config = KafkaProducerConfig::new([analytics_topic_clone.clone()]);
                // Player event
                let player_event = PlayerEvent {
                    player_id: 123,
                    action: "login".to_string(),
                };
                player_writer.write(
                    &player_config,
                    player_event,
                    Some(errors.0.callback.clone()),
                );

                // Combat event
                let combat_event = CombatEvent {
                    attacker_id: 456,
                    target_id: 789,
                    damage: 100,
                };
                combat_writer.write(
                    &combat_config,
                    combat_event,
                    Some(errors.0.callback.clone()),
                );

                // Analytics event
                let analytics_event = AnalyticsEvent {
                    event_type: "user_action".to_string(),
                    user_id: 123,
                    timestamp: 1234567890,
                };
                analytics_writer.write(
                    &analytics_config,
                    analytics_event,
                    Some(errors.0.callback.clone()),
                );

                state.test_completed = true;
            }
        },
    );

    // Run for a few frames to allow error propagation
    for _ in 0..5 {
        app.update();
    }
    let errors = errors.collected();

    let player_error = errors
        .iter()
        .find(|e| e.topic == player_topic && e.kind == BusErrorKind::DeliveryFailure)
        .expect("Should receive 1 player error");
    let player_original: PlayerEvent =
        decode_event(player_error).expect("player error should decode payload");
    assert_eq!(player_original.player_id, 123);

    let combat_error = errors
        .iter()
        .find(|e| e.topic == combat_topic && e.kind == BusErrorKind::DeliveryFailure)
        .expect("Should receive 1 combat error");
    let combat_original: CombatEvent =
        decode_event(combat_error).expect("combat error should decode payload");
    assert_eq!(combat_original.damage, 100);

    let analytics_error = errors
        .iter()
        .find(|e| e.topic == analytics_topic && e.kind == BusErrorKind::DeliveryFailure)
        .expect("Should receive 1 analytics error");
    let analytics_original: AnalyticsEvent =
        decode_event(analytics_error).expect("analytics error should decode payload");
    assert_eq!(analytics_original.user_id, 123);
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
    app.add_plugins(EventBusPlugins(mock_backend));

    // Register the event bindings for both topics
    KafkaTopologyEventBinding::new::<TestEvent>(vec![working_topic.clone()]).apply(&mut app);
    KafkaTopologyEventBinding::new::<TestEvent>(vec![failing_topic.clone()]).apply(&mut app);

    #[derive(Resource, Default)]
    struct CentralizedErrorTestState {
        events_sent: u32,
        test_completed: bool,
    }

    let errors = ErrorStore::new();
    app.insert_resource(ErrorStoreResource(errors.clone()));
    app.insert_resource(CentralizedErrorTestState::default());

    // System to send events to both topics
    app.add_systems(
        Update,
        move |mut state: ResMut<CentralizedErrorTestState>, mut writer: KafkaMessageWriter, errors: Res<ErrorStoreResource>| {
            if !state.test_completed {
                let working_config = KafkaProducerConfig::new([working_topic_clone.clone()]);
                let failing_config = KafkaProducerConfig::new([failing_topic_clone.clone()]);
                // Send to working topic
                for i in 0..3 {
                    let event = TestEvent {
                        id: i,
                        message: format!("Working event {}", i),
                    };
                    state.events_sent += 1;
                    writer.write(&working_config, event, Some(errors.0.callback.clone()));
                }

                // Send to failing topic
                for i in 0..3 {
                    let event = TestEvent {
                        id: i + 100,
                        message: format!("Failing event {}", i),
                    };
                    state.events_sent += 1;
                    writer.write(&failing_config, event, Some(errors.0.callback.clone()));
                }

                state.test_completed = true;
            }
        },
    );

    // Run for a few frames
    for _ in 0..5 {
        app.update();
    }

    let state = app.world().resource::<CentralizedErrorTestState>();
    let collected = errors.collected();

    assert_eq!(state.events_sent, 6, "Should have sent 6 events total");

    let failing_errors: Vec<_> = collected
        .iter()
        .filter(|e| e.topic == failing_topic_assert && e.kind == BusErrorKind::DeliveryFailure)
        .collect();
    let working_errors: Vec<_> = collected
        .iter()
        .filter(|e| e.topic == working_topic && e.kind == BusErrorKind::DeliveryFailure)
        .collect();

    assert_eq!(failing_errors.len(), 3, "Should have 3 errors from failing topic");
    assert!(working_errors.is_empty(), "Working topic should not produce errors");

    // All errors should be from failing topic with correct event data
    for error in &failing_errors {
        assert_eq!(error.topic, failing_topic_assert, "All errors should be from failing topic");
        let original: TestEvent = decode_event(error).expect("Should decode original event");
        assert!(original.id >= 100, "Original event should have failing topic ID range");
        assert!(
            original.message.contains("Failing event"),
            "Original event should have failing message"
        );
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
    app.add_plugins(EventBusPlugins(mock_backend));

    KafkaTopologyEventBinding::new::<TestEvent>(vec![topic.clone()]).apply(&mut app);

    #[derive(Resource, Default)]
    struct BatchTestState {
        batch_operations: u32,
        events_per_batch: u32,
        test_completed: bool,
    }

    let errors = ErrorStore::new();
    app.insert_resource(ErrorStoreResource(errors.clone()));
    app.insert_resource(BatchTestState::default());

    // System to send events in batches
    app.add_systems(
        Update,
        move |mut state: ResMut<BatchTestState>, mut writer: KafkaMessageWriter, errors: Res<ErrorStoreResource>| {
            if !state.test_completed {
                let events_per_batch = 5;
                let num_batches = 3;

                state.events_per_batch = events_per_batch;
                let config = KafkaProducerConfig::new([topic_clone.clone()]);

                for batch in 0..num_batches {
                    state.batch_operations += 1;

                    // Send a batch of events
                    for event_in_batch in 0..events_per_batch {
                        let event = TestEvent {
                            id: batch * events_per_batch + event_in_batch,
                            message: format!("Batch {} Event {}", batch, event_in_batch),
                        };
                        writer.write(&config, event, Some(errors.0.callback.clone()));
                    }
                }

                state.test_completed = true;
            }
        },
    );

    // Run for a few frames
    for _ in 0..5 {
        app.update();
    }
    let state = app.world().resource::<BatchTestState>();
    let collected = errors.collected();

    let expected_total_events = state.batch_operations * state.events_per_batch;

    assert_eq!(
        state.batch_operations, 3,
        "Should have completed 3 batch operations"
    );
    assert_eq!(state.events_per_batch, 5, "Should have 5 events per batch");
    assert_eq!(
        expected_total_events, 15,
        "Should calculate 15 total events"
    );

    // All events should fail since the topic is configured to fail
    assert_eq!(
        collected.len(),
        expected_total_events as usize,
        "Should receive error for every event sent. Expected: {}, Got: {}",
        expected_total_events,
        collected.len()
    );

    // Verify each error contains the correct batch and event information
    for (i, error) in collected.iter().enumerate() {
        assert_eq!(error.topic, topic, "Error {} should have correct topic", i);
        let original: TestEvent = decode_event(error).expect("Error should decode event");
        assert_eq!(original.id, i as u32, "Error {} should preserve event ID", i);
        assert!(
            original.message.contains("Batch"),
            "Error {} should preserve batch message",
            i
        );
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
    app.add_plugins(EventBusPlugins(mock_backend));

    KafkaTopologyEventBinding::new::<TestEvent>(vec![topic.clone()]).apply(&mut app);

    #[derive(Resource, Default)]
    struct RetryTestState {
        initial_events_sent: u32,
        retry_attempts: u32,
        max_retries: u32,
        errors_received: Vec<BusErrorContext>,
        retried_event_ids: std::collections::HashSet<u32>,
        initial_send_complete: bool,
    }

    let errors = ErrorStore::new();
    app.insert_resource(ErrorStoreResource(errors.clone()));
    app.insert_resource(RetryTestState {
        max_retries: 3,
        ..Default::default()
    });

    // System to send initial events
    app.add_systems(
        Update,
        move |mut state: ResMut<RetryTestState>, mut writer: KafkaMessageWriter, errors: Res<ErrorStoreResource>| {
            if !state.initial_send_complete {
                // Send initial events
                let config = KafkaProducerConfig::new([topic_clone1.clone()]);
                for i in 0..3 {
                    let event = TestEvent {
                        id: i,
                        message: format!("Initial event {}", i),
                    };
                    state.initial_events_sent += 1;
                    writer.write(&config, event, Some(errors.0.callback.clone()));
                }
                state.initial_send_complete = true;
            }
        },
    );

    // System to handle errors and implement retry logic
    app.add_systems(
        Update,
        move |errors: Res<ErrorStoreResource>,
              mut state: ResMut<RetryTestState>,
              mut writer: KafkaMessageWriter| {
            let retry_config = KafkaProducerConfig::new([topic_clone2.clone()]);
            for error in errors.0.drain() {
                state.errors_received.push(error.clone());

                if let Some(original_event) = decode_event::<TestEvent>(&error) {
                    let event_id = original_event.id;
                    if state.retry_attempts < state.max_retries
                        && !state.retried_event_ids.contains(&event_id)
                    {
                        state.retried_event_ids.insert(event_id);
                        state.retry_attempts += 1;

                        let retry_event = TestEvent {
                            id: original_event.id,
                            message: format!(
                                "{} (retry {})",
                                original_event.message, state.retry_attempts
                            ),
                        };

                        writer.write(
                            &retry_config,
                            retry_event,
                            Some(errors.0.callback.clone()),
                        );
                    }
                }
            }
        },
    );

    // Run for several frames to allow error/retry cycles
    for _ in 0..10 {
        app.update();
    }

    let state = app.world().resource::<RetryTestState>();

    assert_eq!(
        state.initial_events_sent, 3,
        "Should have sent 3 initial events"
    );
    assert_eq!(state.retry_attempts, 3, "Should have made 3 retry attempts");
    assert_eq!(
        state.retried_event_ids.len(),
        3,
        "Should have retried all 3 event IDs"
    );

    // We should have errors for both initial attempts and retries
    // Initial: 3 errors, Retries: 3 errors = 6 total errors
    assert_eq!(
        state.errors_received.len(),
        6,
        "Should receive 6 total errors (3 initial + 3 retries). Got: {}",
        state.errors_received.len()
    );

    // Verify we have both original and retry attempts
    let initial_errors: Vec<_> = state
        .errors_received
        .iter()
        .filter(|e| decode_event::<TestEvent>(e).map(|ev| !ev.message.contains("retry")).unwrap_or(false))
        .collect();
    let retry_errors: Vec<_> = state
        .errors_received
        .iter()
        .filter(|e| decode_event::<TestEvent>(e).map(|ev| ev.message.contains("retry")).unwrap_or(false))
        .collect();

    assert_eq!(initial_errors.len(), 3, "Should have 3 initial errors");
    assert_eq!(retry_errors.len(), 3, "Should have 3 retry errors");

    // Verify retry events have correct format
    for retry_error in retry_errors {
        let original: TestEvent = decode_event(retry_error).expect("retry event should decode");
        assert!(original.message.contains("retry"), "Retry error should contain retry message");
    }
}
