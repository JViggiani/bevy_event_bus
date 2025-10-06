//! Performance benchmarks for bevy_event_bus
//!
//! This module contains performance tests for measuring throughput and latency
//! of the event bus system under high load scenarios.
//!
//! Run with: cargo test --test integration_tests performance_tests::test_message_throughput --release -- --ignored --nocapture

use bevy::prelude::*;
use bevy_event_bus::{
    EventBusPlugins, KafkaEventReader, KafkaEventWriter,
    config::kafka::{
        KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
        KafkaTopicSpec,
    },
};
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic};
use integration_tests::utils::kafka_setup;
use integration_tests::utils::performance::record_performance_results;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Event, Deserialize, Serialize, Debug, Clone, PartialEq)]
struct PerformanceTestEvent {
    id: u64,
    timestamp: u64,
    payload: String,
    sequence: u32,
}

impl PerformanceTestEvent {
    fn new(id: u64, sequence: u32, payload_size: usize) -> Self {
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Generate payload of specified size
        let payload = "x".repeat(payload_size);

        Self {
            id,
            timestamp,
            payload,
            sequence,
        }
    }
}

/// Mark this test as ignored by default - run manually to benchmark the system
#[ignore]
#[test]
fn test_message_throughput() {
    let topic = unique_topic("perf_test");
    let consumer_group = unique_consumer_group("perf_test_group");

    let topic_for_config = topic.clone();
    let group_for_config = consumer_group.clone();

    let (backend, _container) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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
            )
            .add_event_single::<PerformanceTestEvent>(topic_for_config.clone());
    }));

    // Configuration for the performance test
    let message_count = 10_000;
    let payload_size = 100; // bytes per message

    run_throughput_test(
        "test_message_throughput",
        backend,
        topic,
        consumer_group,
        message_count,
        payload_size,
    );
}

/// Test with larger payload sizes
#[ignore]
#[test]
fn test_large_message_throughput() {
    let topic = unique_topic("perf_large_test");
    let consumer_group = unique_consumer_group("perf_large_test_group");

    let topic_for_config = topic.clone();
    let group_for_config = consumer_group.clone();

    let (backend, _container) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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
            )
            .add_event_single::<PerformanceTestEvent>(topic_for_config.clone());
    }));

    // Test with larger messages
    let message_count = 1_000;
    let payload_size = 10_000; // 10KB per message

    run_throughput_test(
        "test_large_message_throughput",
        backend,
        topic,
        consumer_group,
        message_count,
        payload_size,
    );
}

/// Test with high volume of small messages
#[ignore]
#[test]
fn test_high_volume_small_messages() {
    let topic = unique_topic("perf_small_test");
    let consumer_group = unique_consumer_group("perf_small_test_group");

    let topic_for_config = topic.clone();
    let group_for_config = consumer_group.clone();

    let (backend, _container) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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
            )
            .add_event_single::<PerformanceTestEvent>(topic_for_config.clone());
    }));

    // Test with many small messages
    let message_count = 50_000;
    let payload_size = 20; // 20 bytes per message

    run_throughput_test(
        "test_high_volume_small_messages",
        backend,
        topic,
        consumer_group,
        message_count,
        payload_size,
    );
}

/// Run a throughput test with the specified parameters
fn run_throughput_test(
    test_name: &str,
    backend: impl bevy_event_bus::backends::EventBusBackend + 'static,
    topic: String,
    consumer_group: String,
    message_count: u64,
    payload_size: usize,
) {
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(backend));

    #[derive(Resource)]
    struct PerformanceTestState {
        messages_to_send: u64,
        messages_sent: u64,
        messages_received: Vec<PerformanceTestEvent>,
        send_start_time: Option<Instant>,
        send_end_time: Option<Instant>,
        receive_start_time: Option<Instant>,
        receive_end_time: Option<Instant>,
        test_topic: String,
        consumer_group: String,
        payload_size: usize,
        producer_flushed: bool,
    }

    let consumer_group_for_state = consumer_group.clone();

    app.insert_resource(PerformanceTestState {
        messages_to_send: message_count,
        messages_sent: 0,
        messages_received: Vec::new(),
        send_start_time: None,
        send_end_time: None,
        receive_start_time: None,
        receive_end_time: None,
        test_topic: topic.clone(),
        consumer_group: consumer_group_for_state,
        payload_size,
        producer_flushed: false,
    });

    // Sending system
    fn sender_system(mut state: ResMut<PerformanceTestState>, mut writer: KafkaEventWriter) {
        if state.messages_sent >= state.messages_to_send {
            if state.send_end_time.is_none() {
                state.send_end_time = Some(Instant::now());
                println!("Finished sending {} messages", state.messages_sent);
            }

            if !state.producer_flushed {
                match writer.flush(Duration::from_secs(30)) {
                    Ok(_) => {
                        state.producer_flushed = true;
                        println!("Producer flush completed.");
                    }
                    Err(err) => {
                        println!("Producer flush failed: {err}; will retry next frame");
                    }
                }
            }
            return;
        }

        if state.send_start_time.is_none() {
            state.send_start_time = Some(Instant::now());
            println!("Starting to send {} messages...", state.messages_to_send);
        }

        // Send messages in batches for better performance
        let batch_size = 100;
        let config = KafkaProducerConfig::new([state.test_topic.clone()]);
        for _ in 0..batch_size {
            if state.messages_sent >= state.messages_to_send {
                break;
            }

            let event = PerformanceTestEvent::new(
                state.messages_sent,
                state.messages_sent as u32,
                state.payload_size,
            );

            // Fire-and-forget write - no Result to handle
            writer.write(&config, event);
            state.messages_sent += 1;
        }
    }

    // Receiving system
    fn receiver_system(
        mut state: ResMut<PerformanceTestState>,
        mut reader: KafkaEventReader<PerformanceTestEvent>,
    ) {
        let config = KafkaConsumerConfig::new(state.consumer_group.clone(), [&state.test_topic]);
        let batch = reader.read(&config);

        if !batch.is_empty() && state.receive_start_time.is_none() {
            state.receive_start_time = Some(Instant::now());
            println!("Started receiving messages...");
        }

        for wrapper in batch {
            state.messages_received.push(wrapper.event().clone());

            // Check if we've received all messages
            if state.messages_received.len() >= state.messages_to_send as usize
                && state.receive_end_time.is_none()
            {
                state.receive_end_time = Some(Instant::now());
                println!(
                    "Finished receiving {} messages",
                    state.messages_received.len()
                );
            }
        }
    }

    app.add_systems(Update, (sender_system, receiver_system).chain());

    // Run the test
    let test_start = Instant::now();

    // Keep running until we've sent and received all messages
    while {
        let state = app.world().resource::<PerformanceTestState>();
        state.send_end_time.is_none() || state.receive_end_time.is_none()
    } {
        app.update();

        // Timeout after 5 minutes
        if test_start.elapsed().as_secs() > 300 {
            panic!("Performance test timed out after 5 minutes");
        }

        // Brief yield to prevent busy waiting
        std::thread::yield_now();
    }

    // Calculate and display results
    let state = app.world().resource::<PerformanceTestState>();

    let send_duration = state.send_end_time.unwrap() - state.send_start_time.unwrap();
    let receive_duration = state.receive_end_time.unwrap() - state.receive_start_time.unwrap();

    let send_rate = state.messages_sent as f64 / send_duration.as_secs_f64();
    let receive_rate = state.messages_received.len() as f64 / receive_duration.as_secs_f64();

    let total_bytes_sent = state.messages_sent * state.payload_size as u64;
    let total_bytes_received = state.messages_received.len() * state.payload_size;

    let send_throughput_mb = (total_bytes_sent as f64 / 1_000_000.0) / send_duration.as_secs_f64();
    let receive_throughput_mb =
        (total_bytes_received as f64 / 1_000_000.0) / receive_duration.as_secs_f64();

    println!("=== Performance Test Results ===");
    println!("Messages sent: {}", state.messages_sent);
    println!("Messages received: {}", state.messages_received.len());
    println!("Send duration: {:.2?}", send_duration);
    println!("Receive duration: {:.2?}", receive_duration);
    println!("Send rate: {:.0} messages/sec", send_rate);
    println!("Receive rate: {:.0} messages/sec", receive_rate);
    println!("Send throughput: {:.2} MB/sec", send_throughput_mb);
    println!("Receive throughput: {:.2} MB/sec", receive_throughput_mb);

    // Record results to CSV file
    record_performance_results(
        "kafka",
        test_name,
        state.messages_sent,
        state.messages_received.len() as u64,
        state.payload_size,
        send_duration,
        receive_duration,
        send_rate,
        receive_rate,
        send_throughput_mb,
        receive_throughput_mb,
    );

    // Verify test completed successfully
    assert_eq!(
        state.messages_sent, message_count,
        "Not all messages were sent"
    );
    assert_eq!(
        state.messages_received.len(),
        message_count as usize,
        "Not all messages were received"
    );

    // Verify message ordering and content
    state
        .messages_received
        .iter()
        .enumerate()
        .for_each(|(i, msg)| {
            assert_eq!(
                msg.sequence, i as u32,
                "Message sequence mismatch at index {}",
                i
            );
            assert_eq!(
                msg.payload.len(),
                payload_size,
                "Payload size mismatch at index {}",
                i
            );
        });

    println!("✅ Performance test completed successfully!");
}
