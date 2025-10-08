#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{BackendStatus, EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_consumer_group_member, unique_topic,
    update_until,
};
use integration_tests::utils::performance::record_performance_results;
use integration_tests::utils::redis_setup;
use std::time::{Duration, Instant};

#[derive(Resource, Default)]
struct WriterStats {
    sent: usize,
    send_start: Option<Instant>,
    send_end: Option<Instant>,
}

impl WriterStats {
    fn mark_send_start(&mut self) {
        if self.send_start.is_none() {
            self.send_start = Some(Instant::now());
        }
    }

    fn mark_send_end(&mut self) {
        if self.send_end.is_none() {
            self.send_end = Some(Instant::now());
        }
    }
}

#[derive(Resource, Default)]
struct ReaderStats {
    received: usize,
    receive_start: Option<Instant>,
    receive_end: Option<Instant>,
}

impl ReaderStats {
    fn mark_receive_start(&mut self) {
        if self.receive_start.is_none() {
            self.receive_start = Some(Instant::now());
        }
    }

    fn mark_receive_end(&mut self) {
        if self.receive_end.is_none() {
            self.receive_end = Some(Instant::now());
        }
    }
}

fn duration_between(start: Option<Instant>, end: Option<Instant>, label: &str) -> Duration {
    let start = start.unwrap_or_else(|| panic!("{label} start time not recorded"));
    let end = end.unwrap_or_else(|| panic!("{label} end time not recorded"));
    end.duration_since(start)
}

/// Test message throughput performance
#[test]
#[ignore] // Performance test - run manually
fn test_message_throughput() {
    let stream = unique_topic("perf_throughput");
    let consumer_group = unique_consumer_group("perf_group");
    let consumer_name = unique_consumer_group_member("perf_consumer");

    let stream_clone_for_topology = stream.clone();
    let consumer_group_clone_for_topology = consumer_group.clone();
    let (backend, _context) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone_for_topology.clone()))
            .add_consumer_group(
                consumer_group_clone_for_topology.clone(),
                RedisConsumerGroupSpec::new(
                    [stream_clone_for_topology.clone()],
                    consumer_group_clone_for_topology.clone(),
                ),
            )
            .add_event_single::<TestEvent>(stream_clone_for_topology.clone());
    })
    .expect("Redis backend setup successful");

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend.clone()));
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend));

    writer.insert_resource(WriterStats::default());
    reader.insert_resource(ReaderStats::default());

    const TARGET_MESSAGES: usize = 1000;
    const PAYLOAD_SIZE: usize = 100;
    let payload_for_writer = "x".repeat(PAYLOAD_SIZE);

    // High throughput writer
    let stream_clone = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter,
              mut stats: ResMut<WriterStats>,
              status: Option<Res<BackendStatus>>| {
            if !status.map(|s| s.ready).unwrap_or(false) {
                return;
            }

            if stats.sent >= TARGET_MESSAGES {
                stats.mark_send_end();
                return;
            }

            stats.mark_send_start();

            let config = RedisProducerConfig::new(stream_clone.clone());
            let remaining = TARGET_MESSAGES - stats.sent;
            let batch_size = remaining.min(100);
            let start_index = stats.sent;

            for offset in 0..batch_size {
                let value = (start_index + offset) as i32;
                w.write(
                    &config,
                    TestEvent {
                        message: payload_for_writer.clone(),
                        value,
                    },
                );
            }

            stats.sent += batch_size;
            if stats.sent >= TARGET_MESSAGES {
                stats.mark_send_end();
            }
        },
    );

    // High throughput reader
    let stream_clone = stream.clone();
    let group_clone = consumer_group.clone();
    let consumer_clone = consumer_name.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>,
              mut stats: ResMut<ReaderStats>,
              status: Option<Res<BackendStatus>>| {
            if !status.map(|s| s.ready).unwrap_or(false) {
                return;
            }

            let config = RedisConsumerConfig::new(group_clone.clone(), [stream_clone.clone()])
                .set_consumer_name(consumer_clone.clone());

            let events = r.read(&config);
            if !events.is_empty() {
                println!("Received batch of {} events", events.len());
                stats.mark_receive_start();
                stats.received += events.len();
                if stats.received >= TARGET_MESSAGES {
                    stats.mark_receive_end();
                }
            }
        },
    );

    loop {
        writer.update();
        reader.update();

        if writer.world().resource::<WriterStats>().send_end.is_some() {
            break;
        }
    }

    run_app_updates(&mut writer, 10);

    let (all_received, _) = update_until(&mut reader, 30000, |app| {
        app.world().resource::<ReaderStats>().received >= TARGET_MESSAGES
    });

    if !all_received {
        let reader_stats = reader.world().resource::<ReaderStats>();
        panic!(
            "Should receive all {TARGET_MESSAGES} messages (received {})",
            reader_stats.received
        );
    }

    let writer_stats = writer.world().resource::<WriterStats>();
    let reader_stats = reader.world().resource::<ReaderStats>();

    assert_eq!(writer_stats.sent, TARGET_MESSAGES);
    assert_eq!(reader_stats.received, TARGET_MESSAGES);

    let send_duration = duration_between(writer_stats.send_start, writer_stats.send_end, "send");
    let receive_duration = duration_between(
        reader_stats.receive_start,
        reader_stats.receive_end,
        "receive",
    );

    let messages_sent = TARGET_MESSAGES as u64;
    let messages_received = TARGET_MESSAGES as u64;
    let send_rate = messages_sent as f64 / send_duration.as_secs_f64();
    let receive_rate = messages_received as f64 / receive_duration.as_secs_f64();
    let total_sent_bytes = (TARGET_MESSAGES * PAYLOAD_SIZE) as u64;
    let send_throughput_mb = (total_sent_bytes as f64 / 1_000_000.0) / send_duration.as_secs_f64();
    let receive_throughput_mb =
        (total_sent_bytes as f64 / 1_000_000.0) / receive_duration.as_secs_f64();

    record_performance_results(
        "redis",
        "test_message_throughput",
        messages_sent,
        messages_received,
        PAYLOAD_SIZE,
        send_duration,
        receive_duration,
        send_rate,
        receive_rate,
        send_throughput_mb,
        receive_throughput_mb,
    );

    println!("Throughput test results:");
    println!("  Messages: {}", TARGET_MESSAGES);
    println!(
        "  Send time: {:?} ({:.2} msg/sec)",
        send_duration, send_rate
    );
    println!(
        "  Receive time: {:?} ({:.2} msg/sec)",
        receive_duration, receive_rate
    );
}

/// Test high volume small messages
#[test]
#[ignore] // Performance test - run manually
fn test_high_volume_small_messages() {
    let stream = unique_topic("perf_small");
    let consumer_group = unique_consumer_group("perf_small_group");
    let consumer_name = unique_consumer_group_member("perf_small_consumer");

    let stream_clone_for_topology = stream.clone();
    let consumer_group_clone_for_topology = consumer_group.clone();
    let (backend, _context) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone_for_topology.clone()))
            .add_consumer_group(
                consumer_group_clone_for_topology.clone(),
                RedisConsumerGroupSpec::new(
                    [stream_clone_for_topology.clone()],
                    consumer_group_clone_for_topology.clone(),
                ),
            )
            .add_event_single::<TestEvent>(stream_clone_for_topology.clone());
    })
    .expect("Redis backend setup successful");

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend.clone()));
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend));

    writer.insert_resource(WriterStats::default());
    reader.insert_resource(ReaderStats::default());

    const MESSAGE_COUNT: usize = 5000;
    const PAYLOAD_SIZE: usize = 20;
    let payload_for_writer = "x".repeat(PAYLOAD_SIZE);

    // Send many small messages quickly
    let stream_clone = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter,
              mut stats: ResMut<WriterStats>,
              status: Option<Res<BackendStatus>>| {
            if !status.map(|s| s.ready).unwrap_or(false) {
                return;
            }

            if stats.sent >= MESSAGE_COUNT {
                stats.mark_send_end();
                return;
            }

            stats.mark_send_start();

            let config = RedisProducerConfig::new(stream_clone.clone());
            let remaining = MESSAGE_COUNT - stats.sent;
            let batch_size = remaining.min(200);
            let start_index = stats.sent;

            for offset in 0..batch_size {
                let value = (start_index + offset) as i32;
                w.write(
                    &config,
                    TestEvent {
                        message: payload_for_writer.clone(),
                        value,
                    },
                );
            }

            stats.sent += batch_size;
            if stats.sent >= MESSAGE_COUNT {
                stats.mark_send_end();
            }
        },
    );

    // Process small messages
    let stream_clone = stream.clone();
    let group_clone = consumer_group.clone();
    let consumer_clone = consumer_name.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>,
              mut stats: ResMut<ReaderStats>,
              status: Option<Res<BackendStatus>>| {
            if !status.map(|s| s.ready).unwrap_or(false) {
                return;
            }

            let config = RedisConsumerConfig::new(group_clone.clone(), [stream_clone.clone()])
                .set_consumer_name(consumer_clone.clone());

            let events = r.read(&config);
            if !events.is_empty() {
                stats.mark_receive_start();
                stats.received += events.len();
                if stats.received >= MESSAGE_COUNT {
                    stats.mark_receive_end();
                }
            }
        },
    );

    while writer.world().resource::<WriterStats>().send_end.is_none() {
        writer.update();
    }

    run_app_updates(&mut writer, 25);

    let (received_all, _) = update_until(&mut reader, 30000, |app| {
        app.world().resource::<ReaderStats>().received >= MESSAGE_COUNT
    });

    if !received_all {
        let reader_stats = reader.world().resource::<ReaderStats>();
        panic!(
            "Should process all {MESSAGE_COUNT} messages (received {})",
            reader_stats.received
        );
    }

    let writer_stats = writer.world().resource::<WriterStats>();
    let reader_stats = reader.world().resource::<ReaderStats>();

    assert_eq!(writer_stats.sent, MESSAGE_COUNT);
    assert_eq!(reader_stats.received, MESSAGE_COUNT);

    let send_duration = duration_between(writer_stats.send_start, writer_stats.send_end, "send");
    let receive_duration = duration_between(
        reader_stats.receive_start,
        reader_stats.receive_end,
        "receive",
    );

    let messages_sent = MESSAGE_COUNT as u64;
    let messages_received = MESSAGE_COUNT as u64;
    let send_rate = messages_sent as f64 / send_duration.as_secs_f64();
    let receive_rate = messages_received as f64 / receive_duration.as_secs_f64();
    let total_bytes = (MESSAGE_COUNT * PAYLOAD_SIZE) as u64;
    let send_throughput_mb = (total_bytes as f64 / 1_000_000.0) / send_duration.as_secs_f64();
    let receive_throughput_mb = (total_bytes as f64 / 1_000_000.0) / receive_duration.as_secs_f64();

    record_performance_results(
        "redis",
        "test_high_volume_small_messages",
        messages_sent,
        messages_received,
        PAYLOAD_SIZE,
        send_duration,
        receive_duration,
        send_rate,
        receive_rate,
        send_throughput_mb,
        receive_throughput_mb,
    );

    println!(
        "High volume small messages test:\n  Processed {} messages in {:?} ({:.2} msg/sec)",
        MESSAGE_COUNT, receive_duration, receive_rate
    );
}

/// Test large message throughput  
#[test]
#[ignore] // Performance test - run manually
fn test_large_message_throughput() {
    let stream = unique_topic("perf_large");
    let consumer_group = unique_consumer_group("perf_large_group");
    let consumer_name = unique_consumer_group_member("perf_large_consumer");

    #[derive(Event, Clone, Debug, serde::Serialize, serde::Deserialize, PartialEq)]
    struct LargeEvent {
        id: u32,
        data: Vec<u8>, // Large payload
        metadata: String,
    }

    let stream_clone_for_topology = stream.clone();
    let consumer_group_clone_for_topology = consumer_group.clone();
    let (backend, _context) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone_for_topology.clone()))
            .add_consumer_group(
                consumer_group_clone_for_topology.clone(),
                RedisConsumerGroupSpec::new(
                    [stream_clone_for_topology.clone()],
                    consumer_group_clone_for_topology.clone(),
                ),
            )
            .add_event_single::<LargeEvent>(stream_clone_for_topology.clone());
    })
    .expect("Redis backend setup successful");

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend.clone()));
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend));

    writer.insert_resource(WriterStats::default());
    reader.insert_resource(ReaderStats::default());

    const LARGE_MESSAGE_COUNT: usize = 100;
    const PAYLOAD_SIZE: usize = 10 * 1024; // 10KB per message

    // Send large messages
    let stream_clone = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter,
              mut stats: ResMut<WriterStats>,
              status: Option<Res<BackendStatus>>| {
            if !status.map(|s| s.ready).unwrap_or(false) {
                return;
            }

            if stats.sent >= LARGE_MESSAGE_COUNT {
                stats.mark_send_end();
                return;
            }

            stats.mark_send_start();

            let config = RedisProducerConfig::new(stream_clone.clone());
            let large_event = LargeEvent {
                id: stats.sent as u32,
                data: vec![0u8; PAYLOAD_SIZE],
                metadata: format!("large_message_{}", stats.sent),
            };
            w.write(&config, large_event);
            stats.sent += 1;

            if stats.sent >= LARGE_MESSAGE_COUNT {
                stats.mark_send_end();
            }
        },
    );

    // Process large messages
    let stream_clone = stream.clone();
    let group_clone = consumer_group.clone();
    let consumer_clone = consumer_name.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<LargeEvent>,
              mut stats: ResMut<ReaderStats>,
              status: Option<Res<BackendStatus>>| {
            if !status.map(|s| s.ready).unwrap_or(false) {
                return;
            }

            let config = RedisConsumerConfig::new(group_clone.clone(), [stream_clone.clone()])
                .set_consumer_name(consumer_clone.clone());

            let events = r.read(&config);
            if !events.is_empty() {
                stats.mark_receive_start();
                for wrapper in events {
                    assert_eq!(wrapper.event().data.len(), PAYLOAD_SIZE);
                    stats.received += 1;
                }
                if stats.received >= LARGE_MESSAGE_COUNT {
                    stats.mark_receive_end();
                }
            }
        },
    );

    while writer.world().resource::<WriterStats>().send_end.is_none() {
        writer.update();
    }

    run_app_updates(&mut writer, 25);

    let (received_all, _) = update_until(&mut reader, 30000, |app| {
        app.world().resource::<ReaderStats>().received >= LARGE_MESSAGE_COUNT
    });

    if !received_all {
        let reader_stats = reader.world().resource::<ReaderStats>();
        panic!(
            "Should process all {LARGE_MESSAGE_COUNT} messages (received {})",
            reader_stats.received
        );
    }

    let writer_stats = writer.world().resource::<WriterStats>();
    let reader_stats = reader.world().resource::<ReaderStats>();

    assert_eq!(writer_stats.sent, LARGE_MESSAGE_COUNT);
    assert_eq!(reader_stats.received, LARGE_MESSAGE_COUNT);

    let send_duration = duration_between(writer_stats.send_start, writer_stats.send_end, "send");
    let receive_duration = duration_between(
        reader_stats.receive_start,
        reader_stats.receive_end,
        "receive",
    );

    let messages_sent = LARGE_MESSAGE_COUNT as u64;
    let messages_received = LARGE_MESSAGE_COUNT as u64;
    let send_rate = messages_sent as f64 / send_duration.as_secs_f64();
    let receive_rate = messages_received as f64 / receive_duration.as_secs_f64();
    let total_bytes = (LARGE_MESSAGE_COUNT * PAYLOAD_SIZE) as u64;
    let send_throughput_mb = (total_bytes as f64 / 1_000_000.0) / send_duration.as_secs_f64();
    let receive_throughput_mb = (total_bytes as f64 / 1_000_000.0) / receive_duration.as_secs_f64();

    record_performance_results(
        "redis",
        "test_large_message_throughput",
        messages_sent,
        messages_received,
        PAYLOAD_SIZE,
        send_duration,
        receive_duration,
        send_rate,
        receive_rate,
        send_throughput_mb,
        receive_throughput_mb,
    );

    println!("Large message throughput test:");
    let total_megabytes = total_bytes as f64 / (1024.0 * 1024.0);
    println!("Large message throughput test:");
    println!(
        "  Processed {} messages ({:.2} MB)",
        LARGE_MESSAGE_COUNT, total_megabytes
    );
    println!("  Send: {:?} ({:.2} msg/sec)", send_duration, send_rate);
    println!(
        "  Receive: {:?} ({:.2} msg/sec)",
        receive_duration, receive_rate
    );
}
