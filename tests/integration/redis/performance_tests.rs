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
use integration_tests::utils::performance::{PerformanceMetrics, record_performance_results};
use integration_tests::utils::redis_setup;
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

    let message_count = 25_000;
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

    let message_count = 2_500;
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

    let message_count = 100_000;
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
        warmup_messages: u64,
        warmup_sent: u64,
        warmup_received: u64,
        warmup_flushed: bool,
        measurement_flushed: bool,
        measuring: bool,
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
        warmup_messages: std::cmp::max(message_count / 10, 500u64),
        warmup_sent: 0,
        warmup_received: 0,
        warmup_flushed: false,
        measurement_flushed: false,
        measuring: false,
    });

    fn try_begin_measurement(state: &mut PerformanceTestState) -> bool {
        if state.measuring {
            return false;
        }

        if state.warmup_sent >= state.warmup_messages
            && state.warmup_received >= state.warmup_messages
            && state.warmup_flushed
        {
            state.measuring = true;
            state.messages_sent = 0;
            state.messages_received.clear();
            state.send_start_time = None;
            state.send_end_time = None;
            state.receive_start_time = None;
            state.receive_end_time = None;
            println!(
                "Warmup complete; starting measured run of {} messages",
                state.messages_to_send
            );
            return true;
        }

        false
    }

    // Sending system
    fn sender_system(mut state: ResMut<PerformanceTestState>, mut writer: RedisEventWriter) {
        if state.measuring && state.messages_sent >= state.messages_to_send {
            if state.send_end_time.is_none() {
                state.send_end_time = Some(Instant::now());
                println!("Finished sending {} messages", state.messages_sent);
            }

            if !state.measurement_flushed {
                match writer.flush() {
                    Ok(_) => {
                        state.measurement_flushed = true;
                        println!("Writer flush completed.");
                    }
                    Err(err) => {
                        println!("Writer flush failed: {err}; will retry next frame");
                    }
                }
            }
            return;
        }

        if !state.measuring {
            if state.warmup_sent == 0 {
                println!(
                    "Pre-warming Redis backend with {} warmup messages...",
                    state.warmup_messages
                );
            }

            if state.warmup_sent >= state.warmup_messages {
                if !state.warmup_flushed {
                    match writer.flush() {
                        Ok(_) => {
                            state.warmup_flushed = true;
                            println!("Warmup flush completed.");
                        }
                        Err(err) => {
                            println!("Warmup flush failed: {err}; will retry next frame");
                        }
                    }
                }

                if try_begin_measurement(&mut state) {
                    return;
                }
                return;
            }
        }

        if state.measuring && state.send_start_time.is_none() {
            state.send_start_time = Some(Instant::now());
            println!("Starting to send {} messages...", state.messages_to_send);
        }

        // Send messages in batches for better performance
        let batch_size = 100;
        let config = RedisProducerConfig::new(state.stream.clone());
        for _ in 0..batch_size {
            if state.measuring {
                if state.messages_sent >= state.messages_to_send {
                    break;
                }
            } else if state.warmup_sent >= state.warmup_messages {
                break;
            }

            let sequence = if state.measuring {
                state.messages_sent
            } else {
                state.warmup_sent
            };

            let event = PerformanceTestEvent::new(sequence, sequence as u32, state.payload_size);

            // Fire-and-forget write - no Result to handle
            writer.write(&config, event);

            if state.measuring {
                state.messages_sent += 1;
            } else {
                state.warmup_sent += 1;
            }
        }
    }

    fn receiver_system(
        mut state: ResMut<PerformanceTestState>,
        mut reader: RedisEventReader<PerformanceTestEvent>,
    ) {
        let config = RedisConsumerConfig::new(state.consumer_group.clone(), [state.stream.clone()]);
        let batch = reader.read(&config);

        for wrapper in batch {
            if !state.measuring {
                if state.warmup_received < state.warmup_messages {
                    state.warmup_received += 1;
                }

                if try_begin_measurement(&mut state) {
                    continue;
                }

                continue;
            }

            if state.receive_start_time.is_none() {
                state.receive_start_time = Some(Instant::now());
                println!("Started receiving messages...");
            }

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

        if !state.measuring {
            try_begin_measurement(&mut state);
        }
    }

    app.add_systems(Update, (sender_system, receiver_system).chain());

    const MEASUREMENT_ITERATIONS: u32 = 5;
    const OUTLIER_STD_MULTIPLIER: f64 = 2.0;

    struct MeasurementStats {
        send_duration: Duration,
        receive_duration: Duration,
        messages_sent: u64,
        messages_received: usize,
    }

    fn reset_state(state: &mut PerformanceTestState) {
        state.messages_sent = 0;
        state.messages_received.clear();
        state.send_start_time = None;
        state.send_end_time = None;
        state.receive_start_time = None;
        state.receive_end_time = None;
        state.warmup_sent = 0;
        state.warmup_received = 0;
        state.warmup_flushed = false;
        state.measurement_flushed = false;
        state.measuring = false;
    }

    fn measurement_complete(state: &PerformanceTestState) -> bool {
        state.measuring
            && state.measurement_flushed
            && state.send_end_time.is_some()
            && state.receive_end_time.is_some()
    }

    fn mean(values: &[f64]) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.iter().sum::<f64>() / values.len() as f64
    }

    fn median(mut values: Vec<f64>) -> f64 {
        if values.is_empty() {
            return 0.0;
        }
        values.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        let mid = values.len() / 2;
        if values.len() % 2 == 0 {
            (values[mid - 1] + values[mid]) / 2.0
        } else {
            values[mid]
        }
    }

    fn standard_deviation(values: &[f64]) -> f64 {
        if values.len() <= 1 {
            return 0.0;
        }
        let mean = mean(values);
        let variance = values
            .iter()
            .map(|value| {
                let deviation = value - mean;
                deviation * deviation
            })
            .sum::<f64>()
            / (values.len() as f64 - 1.0);
        variance.sqrt()
    }

    fn filter_outliers(values: &[f64], multiplier: f64) -> Vec<f64> {
        if values.len() <= 1 {
            return values.to_vec();
        }

        let std_dev = standard_deviation(values);
        if std_dev <= f64::EPSILON {
            return values.to_vec();
        }

        let mean = mean(values);
        let max_deviation = std_dev * multiplier;
        let filtered: Vec<f64> = values
            .iter()
            .copied()
            .filter(|value| (value - mean).abs() <= max_deviation)
            .collect();

        if filtered.is_empty() {
            values.to_vec()
        } else {
            filtered
        }
    }

    fn run_measurement(app: &mut App) -> MeasurementStats {
        let measurement_start = Instant::now();

        loop {
            {
                let state = app.world().resource::<PerformanceTestState>();
                if measurement_complete(state) {
                    break;
                }
            }

            app.update();

            if measurement_start.elapsed().as_secs() > 300 {
                panic!("Performance test timed out after 5 minutes");
            }

            std::thread::yield_now();
        }

        let state = app.world().resource::<PerformanceTestState>();

        let send_start = state
            .send_start_time
            .expect("Send start time should be recorded");
        let send_end = state
            .send_end_time
            .expect("Send end time should be recorded");
        let receive_start = state
            .receive_start_time
            .expect("Receive start time should be recorded");
        let receive_end = state
            .receive_end_time
            .expect("Receive end time should be recorded");

        MeasurementStats {
            send_duration: send_end - send_start,
            receive_duration: receive_end - receive_start,
            messages_sent: state.messages_sent,
            messages_received: state.messages_received.len(),
        }
    }

    let mut measurements = Vec::with_capacity(MEASUREMENT_ITERATIONS as usize);

    for iteration in 0..MEASUREMENT_ITERATIONS {
        {
            let mut state = app.world_mut().resource_mut::<PerformanceTestState>();
            reset_state(&mut state);
        }

        let measurement = run_measurement(&mut app);
        println!(
            "Completed measurement {} of {}",
            iteration + 1,
            MEASUREMENT_ITERATIONS
        );
        measurements.push(measurement);
    }

    let state = app.world().resource::<PerformanceTestState>();

    let send_secs: Vec<f64> = measurements
        .iter()
        .map(|measurement| measurement.send_duration.as_secs_f64())
        .collect();
    let receive_secs: Vec<f64> = measurements
        .iter()
        .map(|measurement| measurement.receive_duration.as_secs_f64())
        .collect();

    let raw_avg_send_secs = mean(&send_secs);
    let raw_avg_receive_secs = mean(&receive_secs);
    let raw_median_send_secs = median(send_secs.clone());
    let raw_median_receive_secs = median(receive_secs.clone());
    let send_std_dev = standard_deviation(&send_secs);
    let receive_std_dev = standard_deviation(&receive_secs);

    let filtered_send_secs = filter_outliers(&send_secs, OUTLIER_STD_MULTIPLIER);
    let filtered_receive_secs = filter_outliers(&receive_secs, OUTLIER_STD_MULTIPLIER);

    let filtered_avg_send_secs = mean(&filtered_send_secs);
    let filtered_avg_receive_secs = mean(&filtered_receive_secs);
    let filtered_median_send_secs = median(filtered_send_secs.clone());
    let filtered_median_receive_secs = median(filtered_receive_secs.clone());

    let raw_avg_send_duration = Duration::from_secs_f64(raw_avg_send_secs);
    let raw_avg_receive_duration = Duration::from_secs_f64(raw_avg_receive_secs);
    let raw_median_send_duration = Duration::from_secs_f64(raw_median_send_secs);
    let raw_median_receive_duration = Duration::from_secs_f64(raw_median_receive_secs);
    let filtered_avg_send_duration = Duration::from_secs_f64(filtered_avg_send_secs);
    let filtered_avg_receive_duration = Duration::from_secs_f64(filtered_avg_receive_secs);
    let filtered_median_send_duration = Duration::from_secs_f64(filtered_median_send_secs);
    let filtered_median_receive_duration = Duration::from_secs_f64(filtered_median_receive_secs);

    let messages_sent = measurements
        .last()
        .map(|measurement| measurement.messages_sent)
        .unwrap_or(state.messages_sent);
    let messages_received = measurements
        .last()
        .map(|measurement| measurement.messages_received)
        .unwrap_or(state.messages_received.len());

    let raw_avg_send_rate = message_count as f64 / raw_avg_send_secs;
    let raw_avg_receive_rate = message_count as f64 / raw_avg_receive_secs;
    let raw_median_send_rate = message_count as f64 / raw_median_send_secs;
    let raw_median_receive_rate = message_count as f64 / raw_median_receive_secs;
    let filtered_avg_send_rate = message_count as f64 / filtered_avg_send_secs;
    let filtered_avg_receive_rate = message_count as f64 / filtered_avg_receive_secs;
    let filtered_median_send_rate = message_count as f64 / filtered_median_send_secs;
    let filtered_median_receive_rate = message_count as f64 / filtered_median_receive_secs;

    let total_bytes_sent = message_count * state.payload_size as u64;
    let total_bytes_received = message_count * state.payload_size as u64;

    let raw_avg_send_throughput_mb = (total_bytes_sent as f64 / 1_000_000.0) / raw_avg_send_secs;
    let raw_avg_receive_throughput_mb =
        (total_bytes_received as f64 / 1_000_000.0) / raw_avg_receive_secs;
    let raw_median_send_throughput_mb =
        (total_bytes_sent as f64 / 1_000_000.0) / raw_median_send_secs;
    let raw_median_receive_throughput_mb =
        (total_bytes_received as f64 / 1_000_000.0) / raw_median_receive_secs;
    let filtered_avg_send_throughput_mb =
        (total_bytes_sent as f64 / 1_000_000.0) / filtered_avg_send_secs;
    let filtered_avg_receive_throughput_mb =
        (total_bytes_received as f64 / 1_000_000.0) / filtered_avg_receive_secs;
    let filtered_median_send_throughput_mb =
        (total_bytes_sent as f64 / 1_000_000.0) / filtered_median_send_secs;
    let filtered_median_receive_throughput_mb =
        (total_bytes_received as f64 / 1_000_000.0) / filtered_median_receive_secs;

    println!("=== Per-iteration timing (seconds) ===");
    for (index, measurement) in measurements.iter().enumerate() {
        println!(
            "Iteration {}: send {:.3}s, receive {:.3}s",
            index + 1,
            measurement.send_duration.as_secs_f64(),
            measurement.receive_duration.as_secs_f64()
        );
    }

    println!("=== Aggregated Performance Test Results ===");
    println!("Messages sent: {}", messages_sent);
    println!("Messages received: {}", messages_received);
    println!("Raw average send duration: {:.2?}", raw_avg_send_duration);
    println!("Raw median send duration: {:.2?}", raw_median_send_duration);
    println!(
        "Filtered average send duration (within ±{:.1}σ): {:.2?}",
        OUTLIER_STD_MULTIPLIER, filtered_avg_send_duration
    );
    println!(
        "Filtered median send duration (within ±{:.1}σ): {:.2?}",
        OUTLIER_STD_MULTIPLIER, filtered_median_send_duration
    );
    println!(
        "Raw average receive duration: {:.2?}",
        raw_avg_receive_duration
    );
    println!(
        "Raw median receive duration: {:.2?}",
        raw_median_receive_duration
    );
    println!(
        "Filtered average receive duration (within ±{:.1}σ): {:.2?}",
        OUTLIER_STD_MULTIPLIER, filtered_avg_receive_duration
    );
    println!(
        "Filtered median receive duration (within ±{:.1}σ): {:.2?}",
        OUTLIER_STD_MULTIPLIER, filtered_median_receive_duration
    );
    println!(
        "Raw average send rate: {:.0} messages/sec",
        raw_avg_send_rate
    );
    println!(
        "Raw median send rate: {:.0} messages/sec",
        raw_median_send_rate
    );
    println!(
        "Filtered average send rate (within ±{:.1}σ): {:.0} messages/sec",
        OUTLIER_STD_MULTIPLIER, filtered_avg_send_rate
    );
    println!(
        "Filtered median send rate (within ±{:.1}σ): {:.0} messages/sec",
        OUTLIER_STD_MULTIPLIER, filtered_median_send_rate
    );
    println!(
        "Raw average receive rate: {:.0} messages/sec",
        raw_avg_receive_rate
    );
    println!(
        "Raw median receive rate: {:.0} messages/sec",
        raw_median_receive_rate
    );
    println!(
        "Filtered average receive rate (within ±{:.1}σ): {:.0} messages/sec",
        OUTLIER_STD_MULTIPLIER, filtered_avg_receive_rate
    );
    println!(
        "Filtered median receive rate (within ±{:.1}σ): {:.0} messages/sec",
        OUTLIER_STD_MULTIPLIER, filtered_median_receive_rate
    );
    println!(
        "Raw average send throughput: {:.2} MB/sec",
        raw_avg_send_throughput_mb
    );
    println!(
        "Raw median send throughput: {:.2} MB/sec",
        raw_median_send_throughput_mb
    );
    println!(
        "Filtered average send throughput (within ±{:.1}σ): {:.2} MB/sec",
        OUTLIER_STD_MULTIPLIER, filtered_avg_send_throughput_mb
    );
    println!(
        "Filtered median send throughput (within ±{:.1}σ): {:.2} MB/sec",
        OUTLIER_STD_MULTIPLIER, filtered_median_send_throughput_mb
    );
    println!(
        "Raw average receive throughput: {:.2} MB/sec",
        raw_avg_receive_throughput_mb
    );
    println!(
        "Raw median receive throughput: {:.2} MB/sec",
        raw_median_receive_throughput_mb
    );
    println!(
        "Filtered average receive throughput (within ±{:.1}σ): {:.2} MB/sec",
        OUTLIER_STD_MULTIPLIER, filtered_avg_receive_throughput_mb
    );
    println!(
        "Filtered median receive throughput (within ±{:.1}σ): {:.2} MB/sec",
        OUTLIER_STD_MULTIPLIER, filtered_median_receive_throughput_mb
    );
    println!(
        "Send duration standard deviation: {:.3}s ({} samples)",
        send_std_dev,
        send_secs.len()
    );
    println!(
        "Receive duration standard deviation: {:.3}s ({} samples)",
        receive_std_dev,
        receive_secs.len()
    );
    println!(
        "Filtered send samples retained: {} of {}",
        filtered_send_secs.len(),
        send_secs.len()
    );
    println!(
        "Filtered receive samples retained: {} of {}",
        filtered_receive_secs.len(),
        receive_secs.len()
    );

    record_performance_results(PerformanceMetrics {
        backend: "redis",
        test_name,
        messages_sent: message_count,
        messages_received: message_count,
        payload_size: state.payload_size,
        send_duration: filtered_median_send_duration,
        receive_duration: filtered_median_receive_duration,
        send_rate: filtered_median_send_rate,
        receive_rate: filtered_median_receive_rate,
        send_throughput_mb: filtered_median_send_throughput_mb,
        receive_throughput_mb: filtered_median_receive_throughput_mb,
    });

    assert_eq!(
        state.messages_sent, message_count,
        "Not all messages were sent"
    );
    assert_eq!(
        state.messages_received.len(),
        message_count as usize,
        "Not all messages were received"
    );

    let mut ordered_messages = state.messages_received.clone();
    ordered_messages.sort_by_key(|msg| msg.sequence);

    ordered_messages
        .iter()
        .enumerate()
        .for_each(|(index, msg)| {
            assert_eq!(
                msg.sequence, index as u32,
                "Message sequence mismatch at index {}",
                index
            );
            assert_eq!(
                msg.payload.len(),
                payload_size,
                "Payload size mismatch at index {}",
                index
            );
        });

    println!("✅ Performance test completed successfully!");
}
