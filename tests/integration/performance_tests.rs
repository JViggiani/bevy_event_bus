//! Performance benchmarks for bevy_event_bus
//!
//! This module contains performance tests for measuring throughput and latency
//! of the event bus system under high load scenarios.
//!
//! Run with: cargo test --test integration_tests performance_tests::test_message_throughput --release -- --ignored --nocapture

use crate::common::helpers::{
    DEFAULT_KAFKA_BOOTSTRAP, kafka_consumer_config, kafka_producer_config, unique_consumer_group,
    unique_topic,
};
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{
    EventBusAppExt, EventBusPlugins, EventBusReader, EventBusWriter, PreconfiguredTopics,
};
use serde::{Deserialize, Serialize};
use std::time::Instant;
use std::{
    fs::OpenOptions,
    io::{Read, Write},
    process::Command,
    time::{SystemTime, UNIX_EPOCH},
};

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
    let (backend, _container) = setup();
    let topic = unique_topic("perf_test");

    // Configuration for the performance test
    let message_count = 10_000;
    let payload_size = 100; // bytes per message

    run_throughput_test(
        "test_message_throughput",
        backend,
        topic,
        message_count,
        payload_size,
    );
}

/// Test with larger payload sizes
#[ignore]
#[test]
fn test_large_message_throughput() {
    let (backend, _container) = setup();
    let topic = unique_topic("perf_large_test");

    // Test with larger messages
    let message_count = 1_000;
    let payload_size = 10_000; // 10KB per message

    run_throughput_test(
        "test_large_message_throughput",
        backend,
        topic,
        message_count,
        payload_size,
    );
}

/// Test with high volume of small messages
#[ignore]
#[test]
fn test_high_volume_small_messages() {
    let (backend, _container) = setup();
    let topic = unique_topic("perf_small_test");

    // Test with many small messages
    let message_count = 50_000;
    let payload_size = 20; // 20 bytes per message

    run_throughput_test(
        "test_high_volume_small_messages",
        backend,
        topic,
        message_count,
        payload_size,
    );
}

/// Run a throughput test with the specified parameters
fn run_throughput_test(
    test_name: &str,
    backend: impl bevy_event_bus::backends::EventBusBackend + 'static,
    topic: String,
    message_count: u64,
    payload_size: usize,
) {
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(
        backend,
        PreconfiguredTopics::new([topic.clone()]),
    ));
    app.add_bus_event::<PerformanceTestEvent>(&topic);

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
    }

    app.insert_resource(PerformanceTestState {
        messages_to_send: message_count,
        messages_sent: 0,
        messages_received: Vec::new(),
        send_start_time: None,
        send_end_time: None,
        receive_start_time: None,
        receive_end_time: None,
        test_topic: topic.clone(),
        consumer_group: unique_consumer_group(&format!("perf_{}", test_name)),
        payload_size,
    });

    // Sending system
    fn sender_system(
        mut state: ResMut<PerformanceTestState>,
        mut writer: EventBusWriter<PerformanceTestEvent>,
    ) {
        if state.messages_sent >= state.messages_to_send {
            if state.send_end_time.is_none() {
                state.send_end_time = Some(Instant::now());
                println!("Finished sending {} messages", state.messages_sent);
            }
            return;
        }

        if state.send_start_time.is_none() {
            state.send_start_time = Some(Instant::now());
            println!("Starting to send {} messages...", state.messages_to_send);
        }

        // Send messages in batches for better performance
        let batch_size = 100;
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
            writer.write(
                &kafka_producer_config(DEFAULT_KAFKA_BOOTSTRAP, [&state.test_topic]),
                event,
            );
            state.messages_sent += 1;
        }
    }

    // Receiving system
    fn receiver_system(
        mut state: ResMut<PerformanceTestState>,
        mut reader: EventBusReader<PerformanceTestEvent>,
    ) {
        if state.receive_start_time.is_none() && !state.messages_received.is_empty() {
            state.receive_start_time = Some(Instant::now());
            println!("Started receiving messages...");
        }

        for wrapper in reader.read(&kafka_consumer_config(
            DEFAULT_KAFKA_BOOTSTRAP,
            state.consumer_group.as_str(),
            [&state.test_topic],
        )) {
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

/// Record performance test results to CSV file for tracking over time
fn record_performance_results(
    test_name: &str,
    messages_sent: u64,
    messages_received: u64,
    payload_size: usize,
    send_duration: std::time::Duration,
    receive_duration: std::time::Duration,
    send_rate: f64,
    receive_rate: f64,
    send_throughput_mb: f64,
    receive_throughput_mb: f64,
) {
    let ts_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis())
        .unwrap_or(0);

    let git_hash = Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()
        .and_then(|o| String::from_utf8(o.stdout).ok())
        .map(|s| s.trim().to_string())
        .unwrap_or_else(|| "unknown".into());

    let run_name = std::env::var("BENCH_NAME").unwrap_or_else(|_| test_name.to_string());

    let csv_path =
        std::env::var("BENCH_CSV_PATH").unwrap_or_else(|_| "event_bus_perf_results.csv".into());

    let header = "timestamp_ms,git_hash,run_name,messages_sent,messages_received,payload_size_bytes,send_duration_ms,receive_duration_ms,send_rate_per_sec,receive_rate_per_sec,send_throughput_mb_per_sec,receive_throughput_mb_per_sec\n";

    let record = format!(
        "{ts},{hash},{run},{ms},{mr},{ps},{sd_ms},{rd_ms},{sr},{rr},{st},{rt}\n",
        ts = ts_ms,
        hash = git_hash,
        run = run_name,
        ms = messages_sent,
        mr = messages_received,
        ps = payload_size,
        sd_ms = (send_duration.as_secs_f64() * 1000.0) as u64,
        rd_ms = (receive_duration.as_secs_f64() * 1000.0) as u64,
        sr = send_rate,
        rr = receive_rate,
        st = send_throughput_mb,
        rt = receive_throughput_mb
    );

    // Create file if missing and write header once; otherwise just append
    let mut need_header = true;
    if let Ok(mut f) = OpenOptions::new().read(true).open(&csv_path) {
        let mut buf = [0u8; 1];
        if f.read(&mut buf).ok().filter(|&n| n > 0).is_some() {
            need_header = false;
        }
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&csv_path)
        .expect("Failed to open performance results CSV file");

    if need_header {
        file.write_all(header.as_bytes())
            .expect("Failed to write CSV header");
    }

    file.write_all(record.as_bytes())
        .expect("Failed to write CSV record");

    println!("📊 Performance results recorded to: {}", csv_path);
}
