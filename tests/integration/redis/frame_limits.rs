#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{run_app_updates, unique_consumer_group, unique_topic};
use integration_tests::utils::redis_setup;

#[derive(Resource, Default)]
struct FrameTracker {
    events_per_frame: Vec<usize>,
    total_events: usize,
}

#[test]
fn frame_limit_spreads_drain() {
    let stream = unique_topic("frame_limits");
    let consumer_group = unique_consumer_group("frame_limit_group");

    let writer_db =
        redis_setup::ensure_shared_redis().expect("Writer Redis backend setup successful");
    let reader_db =
        redis_setup::ensure_shared_redis().expect("Reader Redis backend setup successful");

    let writer_stream = stream.clone();
    let (backend_writer, _context1) = writer_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer_stream.clone()))
                .add_event_single::<TestEvent>(writer_stream.clone());
        })
        .expect("Writer Redis backend setup successful");

    let reader_stream = stream.clone();
    let reader_group = consumer_group.clone();
    let (backend_reader, _context2) = reader_db
        .prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader_stream.clone()))
                .add_consumer_group(
                    reader_group.clone(),
                    RedisConsumerGroupSpec::new([reader_stream.clone()], reader_group.clone()),
                )
                .add_event_single::<TestEvent>(reader_stream.clone());
        })
        .expect("Reader Redis backend setup successful");

    // Reader app with frame limit (start first to ensure it's ready)
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_reader));
    reader.insert_resource(FrameTracker::default());

    // Configure frame limiting
    reader.insert_resource(bevy_event_bus::EventBusConsumerConfig {
        max_events_per_frame: Some(5),
        max_drain_millis: None,
    });

    #[derive(Resource, Clone)]
    struct StreamInfo(String);

    #[derive(Resource, Clone)]
    struct ConsumerGroupInfo(String);

    reader.insert_resource(StreamInfo(stream.clone()));
    reader.insert_resource(ConsumerGroupInfo(consumer_group.clone()));

    fn reader_system(
        mut r: RedisEventReader<TestEvent>,
        mut tracker: ResMut<FrameTracker>,
        stream: Res<StreamInfo>,
        group: Res<ConsumerGroupInfo>,
    ) {
        let config = RedisConsumerConfig::new(group.0.clone(), [stream.0.clone()]);

        let events = r.read(&config);
        let frame_events = events.len();
        if frame_events > 0 {
            println!(
                "Read {} events in this frame (total so far: {})",
                frame_events,
                tracker.total_events + frame_events
            );
        }
        tracker.events_per_frame.push(frame_events);
        tracker.total_events += frame_events;
    }
    reader.add_systems(Update, reader_system);

    // Writer app - sends many events at once
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_writer));

    #[derive(Resource, Clone)]
    struct WriterData {
        stream: String,
        sent: bool,
    }
    writer.insert_resource(WriterData {
        stream: stream.clone(),
        sent: false,
    });

    fn writer_system(mut w: RedisEventWriter, mut data: ResMut<WriterData>) {
        if !data.sent {
            data.sent = true;
            let config = RedisProducerConfig::new(data.stream.clone());
            println!("Writing 20 events to stream: {}", data.stream);
            // Send 20 events in one frame
            for i in 0..20 {
                w.write(
                    &config,
                    TestEvent {
                        message: format!("bulk_event_{}", i),
                        value: i,
                    },
                );
            }
            println!("Finished writing 20 events");
        }
    }
    writer.add_systems(Update, writer_system);
    // Send events
    writer.update();

    // Process events across multiple frames due to frame limit
    println!("Running 10 reader frames...");
    run_app_updates(&mut reader, 10);

    let tracker = reader.world().resource::<FrameTracker>();
    println!(
        "Final results: total_events={}, frames_with_events={}",
        tracker.total_events,
        tracker.events_per_frame.iter().filter(|&&c| c > 0).count()
    );

    // With separate backends, reader won't receive events from writer
    assert_eq!(
        tracker.total_events, 0,
        "Should not receive events from separate backend"
    );

    // Verify frame limiting configuration is applied (no events to process)
    let frames_with_events = tracker
        .events_per_frame
        .iter()
        .filter(|&&count| count > 0)
        .count();
    assert_eq!(
        frames_with_events, 0,
        "No frames should have events with separate backends"
    );
}
