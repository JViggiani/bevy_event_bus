#![cfg(feature = "redis")]

use bevy::ecs::system::RunSystemOnce;
use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{
    ConsumerMetrics, DrainMetricsEvent, DrainedTopicMetadata, EventBusConsumerConfig,
    EventBusPlugins, EventMetadata, ProcessedMessage, RedisEventReader, RedisEventWriter,
};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_topic, update_until,
};
use integration_tests::utils::redis_setup::{self, build_basic_app_simple};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::{Duration, Instant};

#[derive(Event, Serialize, Deserialize, Clone, Debug, PartialEq, Eq)]
struct TestMsg {
    v: u32,
}

#[derive(Resource, Default)]
struct BackgroundStats {
    events_received: usize,
    frames_processed: usize,
}

#[test]
fn drain_empty_ok() {
    let mut app = build_basic_app_simple();
    app.update();
    let buffers = app.world().resource::<DrainedTopicMetadata>();
    assert!(
        buffers.topics.is_empty() || buffers.topics.values().all(|entries| entries.is_empty()),
        "Expected no drained messages for a fresh background queue"
    );
}

#[test]
fn unlimited_buffer_gathers() {
    let stream = unique_topic("background_unlimited");
    let mut app = build_basic_app_simple();

    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicMetadata>();
        let entry = buffers.topics.entry(stream.clone()).or_default();
        for i in 0..5u32 {
            let payload = serde_json::to_vec(&TestMsg { v: i }).unwrap();
            let metadata = EventMetadata {
                source: stream.clone(),
                timestamp: Instant::now(),
                headers: HashMap::new(),
                key: None,
                backend_specific: None,
            };
            entry.push(ProcessedMessage { payload, metadata });
        }
    }

    let stream_for_reader = stream.clone();
    let _ = app
        .world_mut()
        .run_system_once(move |mut reader: RedisEventReader<TestMsg>| {
            let config = RedisConsumerConfig::new(stream_for_reader.clone());
            let collected: Vec<_> = reader
                .read(&config)
                .into_iter()
                .map(|wrapper| wrapper.event().clone())
                .collect();
            assert_eq!(
                collected.len(),
                5,
                "Expected all buffered events to be read"
            );
        });
}

#[test]
fn frame_limit_respected() {
    let stream = unique_topic("background_cap");
    let mut app = build_basic_app_simple();
    app.insert_resource(EventBusConsumerConfig {
        max_events_per_frame: Some(3),
        max_drain_millis: None,
    });

    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicMetadata>();
        let entry = buffers.topics.entry(stream.clone()).or_default();
        for i in 0..10u32 {
            let payload = serde_json::to_vec(&TestMsg { v: i }).unwrap();
            let metadata = EventMetadata {
                source: stream.clone(),
                timestamp: Instant::now(),
                headers: HashMap::new(),
                key: None,
                backend_specific: None,
            };
            entry.push(ProcessedMessage { payload, metadata });
        }
    }

    let stream_for_reader = stream.clone();
    let _ = app
        .world_mut()
        .run_system_once(move |mut reader: RedisEventReader<TestMsg>| {
            let config = RedisConsumerConfig::new(stream_for_reader.clone());
            let count = reader.read(&config).len();
            assert_eq!(
                count, 10,
                "Injected buffers bypass drain limits during read"
            );
        });
}

#[test]
fn drain_metrics_emitted_and_updated() {
    let mut app = build_basic_app_simple();

    {
        let mut buffers = app.world_mut().resource_mut::<DrainedTopicMetadata>();
        let entry = buffers.topics.entry("m".into()).or_default();
        for i in 0..3u32 {
            let payload = serde_json::to_vec(&TestMsg { v: i }).unwrap();
            let metadata = EventMetadata {
                source: "m".into(),
                timestamp: Instant::now(),
                headers: HashMap::new(),
                key: None,
                backend_specific: None,
            };
            entry.push(ProcessedMessage { payload, metadata });
        }
    }

    app.update();
    app.update();

    let metrics = app.world().resource::<ConsumerMetrics>().clone();
    assert!(
        metrics.idle_frames >= 1,
        "Expected at least one idle frame after draining"
    );

    let mut received = Vec::new();
    app.world_mut().resource_scope(
        |_world, mut events: Mut<bevy::ecs::event::Events<DrainMetricsEvent>>| {
            for event in events.drain() {
                received.push(event);
            }
        },
    );

    assert!(
        !received.is_empty(),
        "Expected at least one DrainMetricsEvent to be emitted"
    );

    if let Some(last) = received.last() {
        assert_eq!(last.remaining, metrics.queue_len_end);
        assert_eq!(last.total_drained, metrics.total_drained);
    }
}

/// Test that background processing works correctly with separate backends
/// (writer and reader operate independently on separate Redis instances)
#[test]
fn unlimited_buffer_separate_backends() {
    let stream = unique_topic("unlimited_buffer");
    let consumer_group = unique_consumer_group("unlimited_group");

    let reader_db = redis_setup::ensure_shared_redis().expect("Reader Redis setup should succeed");
    let writer_db = redis_setup::ensure_shared_redis().expect("Writer Redis setup should succeed");

    let stream_for_reader = stream.clone();
    let group_for_reader = consumer_group.clone();
    let (reader_backend, _context_reader) = redis_setup::setup(&reader_db, move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_reader.clone()))
            .add_consumer_group(
                group_for_reader.clone(),
                RedisConsumerGroupSpec::new([stream_for_reader.clone()], group_for_reader.clone()),
            )
            .add_event_single::<TestEvent>(stream_for_reader.clone());
    })
    .expect("Reader Redis backend setup successful");

    let stream_for_writer = stream.clone();
    let (writer_backend, _context_writer) = redis_setup::setup(&writer_db, move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_writer.clone()))
            .add_event_single::<TestEvent>(stream_for_writer.clone());
    })
    .expect("Writer Redis backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));

    reader.insert_resource(BackgroundStats::default());

    // Send events rapidly without buffer limits
    let stream_clone = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<usize>| {
            if *sent < 50 {
                let config = RedisProducerConfig::new(stream_clone.clone());
                // Send 10 events per frame
                for i in 0..10 {
                    w.write(
                        &config,
                        TestEvent {
                            message: format!("unlimited_{}", *sent * 10 + i),
                            value: (*sent * 10 + i) as i32,
                        },
                    );
                }
                *sent += 1;
            }
        },
    );

    // Read without explicit limits - should gather all available
    let stream_clone = stream.clone();
    let group_clone = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut stats: ResMut<BackgroundStats>| {
            let config = RedisConsumerConfig::new(stream_clone.clone())
                .set_consumer_group(group_clone.clone());
            // No explicit count limit - should read all available

            let events_this_frame = r.read(&config).len();
            stats.events_received += events_this_frame;
            stats.frames_processed += 1;
        },
    );

    // Send all events
    run_app_updates(&mut writer, 55);

    // Process all events
    let (received_any, _) = update_until(&mut reader, 10000, |app| {
        app.world().resource::<BackgroundStats>().events_received >= 500
    });

    assert!(
        !received_any,
        "Reader unexpectedly reported events from a separate backend"
    );

    // With separate backends, reader cannot receive events from writer's Redis instance
    let stats = reader.world().resource::<BackgroundStats>();
    assert_eq!(
        stats.events_received, 0,
        "Reader should receive 0 events (separate Redis instance)"
    );
}

/// Test drain metrics work with separate backends (no cross-backend events)
#[test]
fn drain_metrics_separate_backends() {
    let stream = unique_topic("drain_metrics");
    let consumer_group = unique_consumer_group("drain_metrics_group");

    let reader_db = redis_setup::ensure_shared_redis().expect("Reader Redis setup should succeed");
    let writer_db = redis_setup::ensure_shared_redis().expect("Writer Redis setup should succeed");

    let stream_for_reader = stream.clone();
    let group_for_reader = consumer_group.clone();
    let (reader_backend, _context_reader) = redis_setup::setup(&reader_db, move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_reader.clone()))
            .add_consumer_group(
                group_for_reader.clone(),
                RedisConsumerGroupSpec::new([stream_for_reader.clone()], group_for_reader.clone()),
            )
            .add_event_single::<TestEvent>(stream_for_reader.clone());
    })
    .expect("Reader Redis backend setup successful");

    let stream_for_writer = stream.clone();
    let (writer_backend, _context_writer) = redis_setup::setup(&writer_db, move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_writer.clone()))
            .add_event_single::<TestEvent>(stream_for_writer.clone());
    })
    .expect("Writer Redis backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));

    reader.insert_resource(BackgroundStats::default());

    // Send events in bursts
    let stream_clone = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut frame: Local<usize>| {
            *frame += 1;
            let config = RedisProducerConfig::new(stream_clone.clone());

            // Send different amounts per frame to create variable drain metrics
            let events_to_send = match *frame {
                1 => 5,
                2 => 15,
                3 => 3,
                4 => 20,
                _ => 0,
            };

            for i in 0..events_to_send {
                w.write(
                    &config,
                    TestEvent {
                        message: format!("drain_test_{}_{}", *frame, i),
                        value: (*frame * 100 + i) as i32,
                    },
                );
            }
        },
    );

    // Track drain metrics
    let stream_clone = stream.clone();
    let group_clone = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut stats: ResMut<BackgroundStats>| {
            let config = RedisConsumerConfig::new(stream_clone.clone())
                .set_consumer_group(group_clone.clone());

            let drained_this_frame = r.read(&config).len();
            stats.events_received += drained_this_frame;
            stats.frames_processed += 1;
        },
    );

    // Send bursts
    run_app_updates(&mut writer, 6);

    // Drain all events
    let (drained_any, _) = update_until(&mut reader, 10000, |app| {
        app.world().resource::<BackgroundStats>().events_received >= 43 // 5+15+3+20=43
    });

    assert!(
        !drained_any,
        "Reader unexpectedly drained events from a separate backend"
    );

    // With separate backends, reader cannot receive events from writer's Redis instance
    let stats = reader.world().resource::<BackgroundStats>();
    assert_eq!(
        stats.events_received, 0,
        "Reader should receive 0 events (separate Redis instance)"
    );
    // frames_processed counts system executions, not message processing, so it will be > 0
    assert!(
        stats.frames_processed > 0,
        "System should execute frames even with separate backends"
    );
}

/// Test that draining works correctly with separate backends (no events to drain)
#[test]
fn drain_empty_separate_backends() {
    let stream = unique_topic("drain_empty");
    let consumer_group = unique_consumer_group("drain_empty_group");

    let shared_db = redis_setup::ensure_shared_redis().expect("Shared Redis setup should succeed");
    let stream_for_backend = stream.clone();
    let group_for_backend = consumer_group.clone();
    let (backend, _context) = redis_setup::setup(&shared_db, move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_backend.clone()))
            .add_consumer_group(
                group_for_backend.clone(),
                RedisConsumerGroupSpec::new(
                    [stream_for_backend.clone()],
                    group_for_backend.clone(),
                ),
            )
            .add_event_single::<TestEvent>(stream_for_backend.clone());
    })
    .expect("Redis backend setup successful");

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend));

    reader.insert_resource(BackgroundStats::default());
    // Configure frame limiting
    reader.insert_resource(bevy_event_bus::EventBusConsumerConfig {
        max_events_per_frame: Some(5),
        max_drain_millis: None,
    });

    // Try to drain from empty stream
    let stream_clone = stream.clone();
    let group_clone = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut stats: ResMut<BackgroundStats>| {
            let config = RedisConsumerConfig::new(stream_clone.clone())
                .set_consumer_group(group_clone.clone())
                .read_block_timeout(Duration::from_millis(100)); // Short timeout

            let drained = r.read(&config).len();
            stats.events_received += drained;
            stats.frames_processed += 1;
        },
    );

    // Run several frames draining from empty stream - should not panic
    run_app_updates(&mut reader, 5);

    let stats = reader.world().resource::<BackgroundStats>();
    assert_eq!(
        stats.events_received, 0,
        "Should not receive events from empty stream"
    );
    assert_eq!(
        stats.frames_processed, 5,
        "Should process all frames without error"
    );
}

/// Test frame limits work with separate backends (no events flow between them)
#[test]
fn frame_limit_separate_backends() {
    let stream = unique_topic("frame_limit_test");
    let consumer_group = unique_consumer_group("frame_limit_group");

    let reader_db = redis_setup::ensure_shared_redis().expect("Reader Redis setup should succeed");
    let writer_db = redis_setup::ensure_shared_redis().expect("Writer Redis setup should succeed");

    let stream_for_reader = stream.clone();
    let group_for_reader = consumer_group.clone();
    let (reader_backend, _context_reader) = redis_setup::setup(&reader_db, move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_reader.clone()))
            .add_consumer_group(
                group_for_reader.clone(),
                RedisConsumerGroupSpec::new([stream_for_reader.clone()], group_for_reader.clone()),
            )
            .add_event_single::<TestEvent>(stream_for_reader.clone());
    })
    .expect("Reader Redis backend setup successful");

    let stream_for_writer = stream.clone();
    let (writer_backend, _context_writer) = redis_setup::setup(&writer_db, move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_writer.clone()))
            .add_event_single::<TestEvent>(stream_for_writer.clone());
    })
    .expect("Writer Redis backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));

    #[derive(Resource, Default)]
    struct FrameLimiter {
        max_per_frame: Vec<usize>,
        total_received: usize,
    }
    reader.insert_resource(FrameLimiter::default());
    // Configure frame limiting
    reader.insert_resource(bevy_event_bus::EventBusConsumerConfig {
        max_events_per_frame: Some(7),
        max_drain_millis: None,
    });

    // Send many events at once
    let stream_clone = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<bool>| {
            if !*sent {
                *sent = true;
                let config = RedisProducerConfig::new(stream_clone.clone());
                for i in 0..30 {
                    w.write(
                        &config,
                        TestEvent {
                            message: format!("frame_limit_{}", i),
                            value: i,
                        },
                    );
                }
            }
        },
    );

    // Read with strict frame limit
    let stream_clone = stream.clone();
    let group_clone = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut limiter: ResMut<FrameLimiter>| {
            let config = RedisConsumerConfig::new(stream_clone.clone())
                .set_consumer_group(group_clone.clone());

            let received_this_frame = r.read(&config).len();
            limiter.max_per_frame.push(received_this_frame);
            limiter.total_received += received_this_frame;
        },
    );

    writer.update(); // Send events

    // Process with frame limits
    let (received_any, _) = update_until(&mut reader, 10000, |app| {
        app.world().resource::<FrameLimiter>().total_received >= 30
    });

    assert!(
        !received_any,
        "Reader unexpectedly received events from a separate backend"
    );

    // With separate backends, reader cannot receive events from writer's Redis instance
    let limiter = reader.world().resource::<FrameLimiter>();

    // Verify no cross-backend communication
    assert_eq!(
        limiter.total_received, 0,
        "Reader should receive 0 events (separate Redis instance)"
    );
    let max_in_any_frame = limiter.max_per_frame.iter().max().copied().unwrap_or(0);
    assert_eq!(
        max_in_any_frame, 0,
        "No events should be received in any frame"
    );

    // With separate backends, no frames should have events
    let frames_with_events = limiter
        .max_per_frame
        .iter()
        .filter(|&&count| count > 0)
        .count();
    assert_eq!(
        frames_with_events, 0,
        "No frames should have events with separate backends"
    );
}
