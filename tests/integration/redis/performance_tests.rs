#![cfg(feature = "redis")]

//! Performance benchmarks for bevy_event_bus
//!
//! This module mirrors the Kafka performance tests to measure throughput and
//! latency under various Redis workloads. Run the individual tests manually
//! because they are flagged with `#[ignore]` to keep the default test suite fast.

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::helpers::{unique_consumer_group_membership, unique_topic};
use integration_tests::utils::performance::record_performance_results;
use integration_tests::utils::redis_setup;
use serde::{Deserialize, Serialize};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

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

        Self {
            id,
            timestamp,
            payload: "x".repeat(payload_size),
            sequence,
        }
    }
}

/// Measure baseline throughput with medium payloads.
#[ignore]
#[test]
fn test_message_throughput() {
    let stream = unique_topic("perf_test");
    let membership = unique_consumer_group_membership("perf_test_group");
    let consumer_group = membership.group.clone();

    let stream_for_topology = stream.clone();
    let group_for_topology = consumer_group.clone();
    let consumer_for_topology = membership.member.clone();
    let (backend, _ctx) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_topology.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_topology.clone()],
                group_for_topology.clone(),
                consumer_for_topology.clone(),
            ))
            .add_event_single::<PerformanceTestEvent>(stream_for_topology.clone());
    })
    .expect("Redis backend setup successful");

    let message_count = 10_000;
    let payload_size = 100;

    run_throughput_test(
        "test_message_throughput",
        backend,
        stream,
        consumer_group,
        message_count,
        payload_size,
    );
}

/// Measure throughput with larger payload sizes.
#[ignore]
#[test]
fn test_large_message_throughput() {
    let stream = unique_topic("perf_large_test");
    let membership = unique_consumer_group_membership("perf_large_test_group");
    let consumer_group = membership.group.clone();

    let stream_for_topology = stream.clone();
    let group_for_topology = consumer_group.clone();
    let consumer_for_topology = membership.member.clone();
    let (backend, _ctx) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_topology.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_topology.clone()],
                group_for_topology.clone(),
                consumer_for_topology.clone(),
            ))
            .add_event_single::<PerformanceTestEvent>(stream_for_topology.clone());
    })
    .expect("Redis backend setup successful");

    let message_count = 1_000;
    let payload_size = 10_000; // 10 KB per message

    run_throughput_test(
        "test_large_message_throughput",
        backend,
        stream,
        consumer_group,
        message_count,
        payload_size,
    );
}

/// Measure throughput with many tiny messages.
#[ignore]
#[test]
fn test_high_volume_small_messages() {
    let stream = unique_topic("perf_small_test");
    let membership = unique_consumer_group_membership("perf_small_test_group");
    let consumer_group = membership.group.clone();

    let stream_for_topology = stream.clone();
    let group_for_topology = consumer_group.clone();
    let consumer_for_topology = membership.member.clone();
    let (backend, _ctx) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_topology.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_topology.clone()],
                group_for_topology.clone(),
                consumer_for_topology.clone(),
            ))
            .add_event_single::<PerformanceTestEvent>(stream_for_topology.clone());
    })
    .expect("Redis backend setup successful");

    let message_count = 50_000;
    let payload_size = 20;

    run_throughput_test(
        "test_high_volume_small_messages",
        backend,
        stream,
        consumer_group,
        message_count,
        payload_size,
    );
}

fn run_throughput_test(
    test_name: &str,
    backend: impl bevy_event_bus::backends::EventBusBackend + 'static,
    stream: String,
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
        stream: String,
        consumer_group: String,
        payload_size: usize,
        producer_flushed: bool,
    }

    app.insert_resource(PerformanceTestState {
        messages_to_send: message_count,
        messages_sent: 0,
        messages_received: Vec::new(),
        send_start_time: None,
        send_end_time: None,
        receive_start_time: None,
        receive_end_time: None,
        stream,
        consumer_group,
        payload_size,
        producer_flushed: false,
    });

    // Sending system
    fn sender_system(mut state: ResMut<PerformanceTestState>, mut writer: RedisEventWriter) {
        if state.messages_sent >= state.messages_to_send {
            if state.send_end_time.is_none() {
                state.send_end_time = Some(Instant::now());
                println!("Finished sending {} messages", state.messages_sent);
            }

            if !state.producer_flushed {
                match writer.flush() {
                    Ok(_) => {
                        state.producer_flushed = true;
                        println!("Writer flush completed.");
                    }
                    Err(err) => {
                        println!("Writer flush failed: {err}; will retry next frame");
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
        let config = RedisProducerConfig::new(state.stream.clone());
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

    fn receiver_system(
        mut state: ResMut<PerformanceTestState>,
        mut reader: RedisEventReader<PerformanceTestEvent>,
    ) {
        let config = RedisConsumerConfig::new(state.consumer_group.clone(), [state.stream.clone()]);
        let batch = reader.read(&config);

        if !batch.is_empty() && state.receive_start_time.is_none() {
            state.receive_start_time = Some(Instant::now());
            println!("Started receiving messages...");
        }

        for wrapper in batch {
            state.messages_received.push(wrapper.event().clone());

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

    let test_start = Instant::now();
    while {
        let state = app.world().resource::<PerformanceTestState>();
        state.send_end_time.is_none() || state.receive_end_time.is_none()
    } {
        app.update();

        if test_start.elapsed().as_secs() > 300 {
            panic!("Performance test timed out after 5 minutes");
        }

        std::thread::yield_now();
    }

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

    record_performance_results(
        "redis",
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
    let mut ordered_messages = state.messages_received.clone();
    ordered_messages.sort_by_key(|msg| msg.sequence);

    ordered_messages
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
