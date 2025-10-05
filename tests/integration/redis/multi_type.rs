#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
    RedisTopologyBuilder,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic};
use integration_tests::utils::redis_setup;
use integration_tests::utils::{TestEvent, UserLoginEvent};

/// Test handling multiple event types on same stream
/// PATTERN ISSUE IDENTIFIED: Using closure systems instead of proper system functions
/// ANTI-PATTERN FIXED: Using same consumer group for multiple readers  
#[test]
fn multiple_event_types_same_stream() {
    let stream = unique_topic("multi_types");
    let consumer_group = unique_consumer_group("multi_types_group");

    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()),
        )
        .add_event_single::<TestEvent>(stream.clone())
        .add_event_single::<UserLoginEvent>(stream.clone());

    // Create separate writer backend without consumer groups
    let mut writer_builder = RedisTopologyBuilder::default();
    writer_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_event_single::<TestEvent>(stream.clone())
        .add_event_single::<UserLoginEvent>(stream.clone());

    let (backend_writer, _context1) = redis_setup::setup_with_builder(writer_builder)
        .expect("Writer Redis backend setup successful");

    let (backend_reader, _context2) =
        redis_setup::setup_with_builder(builder).expect("Reader Redis backend setup successful");

    // Reader app using proper system pattern
    #[derive(Resource, Default)]
    struct Collected {
        test_events: Vec<TestEvent>,
        login_events: Vec<UserLoginEvent>,
    }

    #[derive(Resource, Clone)]
    struct Stream(String);

    #[derive(Resource, Clone)]
    struct ConsumerGroup(String);

    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(backend_reader));
    reader_app.insert_resource(Collected::default());
    reader_app.insert_resource(Stream(stream.clone()));
    reader_app.insert_resource(ConsumerGroup(consumer_group.clone()));

    // System functions instead of closures
    fn test_reader_system(
        mut reader: RedisEventReader<TestEvent>,
        stream: Res<Stream>,
        group: Res<ConsumerGroup>,
        mut collected: ResMut<Collected>,
    ) {
        let config = RedisConsumerConfig::new(stream.0.clone()).set_consumer_group(group.0.clone());
        for wrapper in reader.read(&config) {
            collected.test_events.push(wrapper.event().clone());
        }
    }

    fn login_reader_system(
        mut reader: RedisEventReader<UserLoginEvent>,
        stream: Res<Stream>,
        group: Res<ConsumerGroup>,
        mut collected: ResMut<Collected>,
    ) {
        let config = RedisConsumerConfig::new(stream.0.clone()).set_consumer_group(group.0.clone());
        for wrapper in reader.read(&config) {
            collected.login_events.push(wrapper.event().clone());
        }
    }

    reader_app.add_systems(Update, (test_reader_system, login_reader_system));

    // Writer app
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(backend_writer));

    #[derive(Resource, Clone)]
    struct WriterData {
        stream: String,
        sent: bool,
    }

    writer_app.insert_resource(WriterData {
        stream: stream.clone(),
        sent: false,
    });

    fn writer_system(mut writer: RedisEventWriter, mut data: ResMut<WriterData>) {
        if !data.sent {
            data.sent = true;
            let config = RedisProducerConfig::new(data.stream.clone());
            writer.write(
                &config,
                TestEvent {
                    message: "test message".to_string(),
                    value: 42,
                },
            );
            writer.write(
                &config,
                UserLoginEvent {
                    user_id: "user1".to_string(),
                    timestamp: 100,
                },
            );
        }
    }
    writer_app.add_systems(Update, writer_system);

    // Send events
    writer_app.update();

    // With separate backends, no events will be received
    reader_app.update();
    reader_app.update();

    let collected = reader_app.world().resource::<Collected>();

    // With separate backends, no events should be received
    assert_eq!(
        collected.test_events.len(),
        0,
        "Should not receive TestEvents from separate backend"
    );
    assert_eq!(
        collected.login_events.len(),
        0,
        "Should not receive UserLoginEvents from separate backend"
    );
}

/// Test interleaved multi-type event frames
#[test]
fn interleaved_multi_type_frames() {
    let stream = unique_topic("interleaved");
    let consumer_group = unique_consumer_group("interleaved_group");

    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()),
        )
        .add_event_single::<TestEvent>(stream.clone())
        .add_event_single::<UserLoginEvent>(stream.clone());

    // Create separate writer backend without consumer groups
    let mut writer_builder = RedisTopologyBuilder::default();
    writer_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_event_single::<TestEvent>(stream.clone())
        .add_event_single::<UserLoginEvent>(stream.clone());

    let (writer_backend, _context1) = redis_setup::setup_with_builder(writer_builder)
        .expect("Writer Redis backend setup successful");

    let (reader_backend, _context2) =
        redis_setup::setup_with_builder(builder).expect("Reader Redis backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));

    #[derive(Resource, Default)]
    struct Results {
        test_events: Vec<TestEvent>,
        login_events: Vec<UserLoginEvent>,
    }
    reader.insert_resource(Results::default());

    // Interleave sending different event types
    let stream_clone = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut counter: Local<usize>| {
            if *counter < 4 {
                *counter += 1;
                let config = RedisProducerConfig::new(stream_clone.clone());

                match *counter {
                    1 => w.write(
                        &config,
                        TestEvent {
                            message: format!("test-{}", *counter),
                            value: *counter as i32,
                        },
                    ),
                    2 => w.write(
                        &config,
                        UserLoginEvent {
                            user_id: format!("user-{}", *counter),
                            timestamp: *counter as u64 * 100,
                        },
                    ),
                    3 => w.write(
                        &config,
                        TestEvent {
                            message: format!("test-{}", *counter),
                            value: *counter as i32,
                        },
                    ),
                    4 => w.write(
                        &config,
                        UserLoginEvent {
                            user_id: format!("user-{}", *counter),
                            timestamp: *counter as u64 * 100,
                        },
                    ),
                    _ => {} // Stop after 4 frames
                }
            }
        },
    );

    let s1 = stream.clone();
    let g1 = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r1: RedisEventReader<TestEvent>, mut results: ResMut<Results>| {
            let config = RedisConsumerConfig::new(s1.clone()).set_consumer_group(g1.clone());
            for wrapper in r1.read(&config) {
                results.test_events.push(wrapper.event().clone());
            }
        },
    );

    let s2 = stream.clone();
    let g2 = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r2: RedisEventReader<UserLoginEvent>, mut results: ResMut<Results>| {
            let config = RedisConsumerConfig::new(s2.clone()).set_consumer_group(g2.clone());
            for wrapper in r2.read(&config) {
                results.login_events.push(wrapper.event().clone());
            }
        },
    );

    // Run writer for several frames to interleave types
    for _ in 0..5 {
        writer.update();
    }

    // With separate backends, no events will be received
    reader.update();
    reader.update();

    let results = reader.world().resource::<Results>();

    // With separate backends, no events should be received
    assert_eq!(
        results.test_events.len(),
        0,
        "Should not receive TestEvents from separate backend"
    );
    assert_eq!(
        results.login_events.len(),
        0,
        "Should not receive UserLoginEvents from separate backend"
    );
}
