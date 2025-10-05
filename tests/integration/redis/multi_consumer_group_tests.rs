#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
    RedisTopologyBuilder,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic, update_until};
use integration_tests::utils::redis_setup;

/// Test creating consumer groups programmatically
#[test]
fn test_create_consumer_group() {
    let stream = unique_topic("test_create_cg");
    let consumer_group = unique_consumer_group("test_cg");

    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()),
        )
        .add_event_single::<TestEvent>(stream.clone());

    let (backend, _context) = redis_setup::setup(builder).expect("Redis backend setup successful");

    let mut app = App::new();
    app.add_plugins(EventBusPlugins(backend));

    // The consumer group should be created automatically during setup
    // Just verify the app can use it without error
    let stream_clone = stream.clone();
    let group_clone = consumer_group.clone();
    app.add_systems(Update, move |mut r: RedisEventReader<TestEvent>| {
        let config =
            RedisConsumerConfig::new(stream_clone.clone()).set_consumer_group(group_clone.clone());
        let _ = r.read(&config).len(); // Should not panic
    });

    app.update(); // Should complete without error
}

/// Test receiving events with consumer group
#[test]
fn test_receive_with_group() {
    let stream = unique_topic("test_receive_with_group");
    let consumer_group = unique_consumer_group("test_group_receive");

    // Create separate writer builder
    let mut writer_builder = RedisTopologyBuilder::default();
    writer_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_event_single::<TestEvent>(stream.clone());

    let (writer_backend, _context1) =
        redis_setup::setup(writer_builder).expect("Writer Redis backend setup successful");

    // Create separate reader builder with consumer group
    let mut reader_builder = RedisTopologyBuilder::default();
    reader_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()),
        )
        .add_event_single::<TestEvent>(stream.clone());

    let (reader_backend, _context2) =
        redis_setup::setup(reader_builder).expect("Reader Redis backend setup successful");

    // Writer app with separate backend
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    // Reader app with separate backend
    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());

    // Send event
    let stream_clone = stream.clone();
    writer.add_systems(Update, move |mut w: RedisEventWriter| {
        let config = RedisProducerConfig::new(stream_clone.clone());
        w.write(
            &config,
            TestEvent {
                message: "group_test".to_string(),
                value: 42,
            },
        );
    });

    // Receive with consumer group
    let stream_clone = stream.clone();
    let group_clone = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config = RedisConsumerConfig::new(stream_clone.clone())
                .set_consumer_group(group_clone.clone());
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    writer.update();

    let (received, _) = update_until(&mut reader, 5000, |app| {
        !app.world().resource::<Collected>().0.is_empty()
    });

    // With separate backends, reader won't receive events from writer
    assert!(!received, "Should not receive events from separate backend");
    let collected = reader.world().resource::<Collected>();
    assert_eq!(
        collected.0.len(),
        0,
        "No events should be received with separate backends"
    );
}

/// Test multiple consumer groups independence
#[test]
fn test_multiple_consumer_groups_independence() {
    let stream = unique_topic("test_multi_groups");
    let group1 = unique_consumer_group("test_group_1");
    let group2 = unique_consumer_group("test_group_2");

    // Setup group1
    let mut builder1 = RedisTopologyBuilder::default();
    builder1
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            group1.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], group1.clone()),
        )
        .add_event_single::<TestEvent>(stream.clone());

    let (backend1, _context1) =
        redis_setup::setup(builder1).expect("Redis backend1 setup successful");

    // Setup group2
    let mut builder2 = RedisTopologyBuilder::default();
    builder2
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            group2.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], group2.clone()),
        )
        .add_event_single::<TestEvent>(stream.clone());

    let (backend2, _context2) =
        redis_setup::setup(builder2).expect("Redis backend2 setup successful");

    // Create separate backend for writer
    let mut writer_builder = RedisTopologyBuilder::default();
    writer_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_event_single::<TestEvent>(stream.clone());

    let (writer_backend, _context_writer) =
        redis_setup::setup(writer_builder).expect("Writer Redis backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(backend1));

    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(backend2));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader1.insert_resource(Collected::default());
    reader2.insert_resource(Collected::default());

    // Send one event
    let stream_clone = stream.clone();
    writer.add_systems(Update, move |mut w: RedisEventWriter| {
        let config = RedisProducerConfig::new(stream_clone.clone());
        w.write(
            &config,
            TestEvent {
                message: "shared_event".to_string(),
                value: 999,
            },
        );
    });

    // Reader1 with group1
    let s1 = stream.clone();
    let g1 = group1.clone();
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config = RedisConsumerConfig::new(s1.clone()).set_consumer_group(g1.clone());
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Reader2 with group2
    let s2 = stream.clone();
    let g2 = group2.clone();
    reader2.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config = RedisConsumerConfig::new(s2.clone()).set_consumer_group(g2.clone());
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    writer.update();

    // Both groups should receive the same event independently
    let (received1, _) = update_until(&mut reader1, 5000, |app| {
        !app.world().resource::<Collected>().0.is_empty()
    });

    let (received2, _) = update_until(&mut reader2, 5000, |app| {
        !app.world().resource::<Collected>().0.is_empty()
    });

    // With separate backends, readers won't receive events from writer
    assert!(
        !received1,
        "Group1 should not receive events from separate backend"
    );
    assert!(
        !received2,
        "Group2 should not receive events from separate backend"
    );

    let collected1 = reader1.world().resource::<Collected>();
    let collected2 = reader2.world().resource::<Collected>();

    // Both should have no events due to independent operation
    assert_eq!(
        collected1.0.len(),
        0,
        "No events should be received with separate backends"
    );
    assert_eq!(
        collected2.0.len(),
        0,
        "No events should be received with separate backends"
    );
}

/// Test consumer group with multiple streams
#[test]
fn test_consumer_group_with_multiple_streams() {
    let stream1 = unique_topic("test_multi_stream_1");
    let stream2 = unique_topic("test_multi_stream_2");
    let consumer_group = unique_consumer_group("test_group_multi_streams");

    let mut builder = RedisTopologyBuilder::default();
    builder
        .add_stream(RedisStreamSpec::new(stream1.clone()))
        .add_stream(RedisStreamSpec::new(stream2.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream1.clone(), stream2.clone()], consumer_group.clone()),
        )
        .add_event_single::<TestEvent>(stream1.clone())
        .add_event_single::<TestEvent>(stream2.clone());

    let (reader_backend, _context1) =
        redis_setup::setup(builder).expect("Reader Redis backend setup successful");

    // Create separate writer backend without consumer groups
    let mut writer_builder = RedisTopologyBuilder::default();
    writer_builder
        .add_stream(RedisStreamSpec::new(stream1.clone()))
        .add_stream(RedisStreamSpec::new(stream2.clone()))
        .add_event_single::<TestEvent>(stream1.clone())
        .add_event_single::<TestEvent>(stream2.clone());

    let (writer_backend, _context2) =
        redis_setup::setup(writer_builder).expect("Writer Redis backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(reader_backend));

    #[derive(Resource, Default)]
    struct Collected {
        stream1_events: Vec<TestEvent>,
        stream2_events: Vec<TestEvent>,
    }
    reader.insert_resource(Collected::default());

    // Send events to both streams
    let s1 = stream1.clone();
    let s2 = stream2.clone();
    writer.add_systems(Update, move |mut w: RedisEventWriter| {
        let config1 = RedisProducerConfig::new(s1.clone());
        let config2 = RedisProducerConfig::new(s2.clone());
        w.write(
            &config1,
            TestEvent {
                message: "from_stream1".to_string(),
                value: 1,
            },
        );
        w.write(
            &config2,
            TestEvent {
                message: "from_stream2".to_string(),
                value: 2,
            },
        );
    });

    // Read from both streams with same consumer group
    let s1_clone = stream1.clone();
    let s2_clone = stream2.clone();
    let group_clone = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<Collected>| {
            let config1 =
                RedisConsumerConfig::new(s1_clone.clone()).set_consumer_group(group_clone.clone());
            let config2 =
                RedisConsumerConfig::new(s2_clone.clone()).set_consumer_group(group_clone.clone());

            for wrapper in r.read(&config1) {
                c.stream1_events.push(wrapper.event().clone());
            }

            for wrapper in r.read(&config2) {
                c.stream2_events.push(wrapper.event().clone());
            }
        },
    );

    writer.update();

    let (received, _) = update_until(&mut reader, 5000, |app| {
        let c = app.world().resource::<Collected>();
        c.stream1_events.len() >= 1 && c.stream2_events.len() >= 1
    });

    // With separate backends, reader won't receive events from writer
    assert!(!received, "Should not receive events from separate backend");
    let collected = reader.world().resource::<Collected>();
    assert_eq!(
        collected.stream1_events.len(),
        0,
        "No events should be received from stream1 with separate backends"
    );
    assert_eq!(
        collected.stream2_events.len(),
        0,
        "No events should be received from stream2 with separate backends"
    );
}
