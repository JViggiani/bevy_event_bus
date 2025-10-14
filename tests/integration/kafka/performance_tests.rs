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
use integration_tests::utils::performance::{PerformanceMetrics, record_performance_results};
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

    let (backend, _container) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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
    let message_count = 25_000;
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

    let (backend, _container) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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
    let message_count = 2_500;
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

    let (backend, _container) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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
    let message_count = 100_000;
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
        warmup_messages: u64,
        warmup_sent: u64,
        warmup_received: u64,
        warmup_flushed: bool,
        measurement_flushed: bool,
        measuring: bool,
    }

    let consumer_group_for_state = consumer_group.clone();
    let warmup_messages = std::cmp::max(message_count / 10, 500u64);

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
        warmup_messages,
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
    fn sender_system(mut state: ResMut<PerformanceTestState>, mut writer: KafkaEventWriter) {
        if state.measuring && state.messages_sent >= state.messages_to_send {
            if state.send_end_time.is_none() {
                state.send_end_time = Some(Instant::now());
                println!("Finished sending {} messages", state.messages_sent);
            }

            if !state.measurement_flushed {
                match writer.flush(Duration::from_secs(30)) {
                    Ok(_) => {
                        state.measurement_flushed = true;
                        println!("Producer flush completed.");
                    }
                    Err(err) => {
                        println!("Producer flush failed: {err}; will retry next frame");
                    }
                }
            }
            return;
        }

        if !state.measuring {
            if state.warmup_sent == 0 {
                println!(
                    "Pre-warming Kafka backend with {} warmup messages...",
                    state.warmup_messages
                );
            }

            if state.warmup_sent >= state.warmup_messages {
                if !state.warmup_flushed {
                    match writer.flush(Duration::from_secs(30)) {
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
        let config = KafkaProducerConfig::new([state.test_topic.clone()]);
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

    // Receiving system
    fn receiver_system(
        mut state: ResMut<PerformanceTestState>,
        mut reader: KafkaEventReader<PerformanceTestEvent>,
    ) {
        let config = KafkaConsumerConfig::new(state.consumer_group.clone(), [&state.test_topic]);
        let batch = reader.read(&config);

        for wrapper in batch {
            if !state.measuring {
                if state.warmup_received < state.warmup_messages {
                    state.warmup_received += 1;
                }

                if try_begin_measurement(&mut state) {
                    // Drop warmup metrics and treat subsequent messages as measured.
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
        backend: "kafka",
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
