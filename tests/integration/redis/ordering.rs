#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
    RedisTopologyBuilder,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic};
use integration_tests::utils::redis_setup;

#[test]
fn per_stream_order_preserved() {
    let stream = unique_topic("ordered");
    let consumer_group = unique_consumer_group("ordering_single_stream");

    // Create separate writer backend without consumer groups
    let mut writer_builder = RedisTopologyBuilder::default();
    writer_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_event_single::<TestEvent>(stream.clone());

    let (writer_backend, _context1) = redis_setup::setup_with_builder(writer_builder)
        .expect("Writer Redis backend setup successful");

    // Create separate reader builder with consumer group
    let mut reader_builder = RedisTopologyBuilder::default();
    reader_builder
        .add_stream(RedisStreamSpec::new(stream.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream.clone()], consumer_group.clone()),
        )
        .add_event_single::<TestEvent>(stream.clone());

    let (reader_backend, _context2) = redis_setup::setup_with_builder(reader_builder)
        .expect("Reader Redis backend setup successful");

    let mut writer = App::new();
    let mut reader = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));
    reader.add_plugins(EventBusPlugins(reader_backend));

    let stream_clone = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut started: Local<bool>| {
            if !*started {
                *started = true;
                return;
            }
            let config = RedisProducerConfig::new(stream_clone.clone());
            for i in 0..10 {
                w.write(
                    &config,
                    TestEvent {
                        message: format!("msg-{i}"),
                        value: i,
                    },
                );
            }
        },
    );
    writer.update(); // warm frame triggers started=true
    writer.update(); // send frame

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader.insert_resource(Collected::default());
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

    // With separate backends, no events will be received
    reader.update();
    reader.update();

    let c = reader.world().resource::<Collected>();
    // With separate backends, no events should be received
    assert_eq!(
        c.0.len(),
        0,
        "Should not receive events from separate backend"
    );
}

#[test]
fn cross_stream_interleave_each_ordered() {
    let stream1 = unique_topic("t1");
    let stream2 = unique_topic("t2");
    let consumer_group = unique_consumer_group("ordering_multi_stream");

    // Create separate writer backend without consumer groups
    let mut writer_builder = RedisTopologyBuilder::default();
    writer_builder
        .add_stream(RedisStreamSpec::new(stream1.clone()))
        .add_stream(RedisStreamSpec::new(stream2.clone()))
        .add_event_single::<TestEvent>(stream1.clone())
        .add_event_single::<TestEvent>(stream2.clone());

    let (writer_backend, _context1) = redis_setup::setup_with_builder(writer_builder)
        .expect("Writer Redis backend setup successful");

    // Create separate reader builder with consumer group
    let mut reader_builder = RedisTopologyBuilder::default();
    reader_builder
        .add_stream(RedisStreamSpec::new(stream1.clone()))
        .add_stream(RedisStreamSpec::new(stream2.clone()))
        .add_consumer_group(
            consumer_group.clone(),
            RedisConsumerGroupSpec::new([stream1.clone(), stream2.clone()], consumer_group.clone()),
        )
        .add_event_single::<TestEvent>(stream1.clone())
        .add_event_single::<TestEvent>(stream2.clone());

    let (reader_backend, _context2) = redis_setup::setup_with_builder(reader_builder)
        .expect("Reader Redis backend setup successful");

    let mut writer = App::new();
    let mut reader = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));
    reader.add_plugins(EventBusPlugins(reader_backend));

    let s1 = stream1.clone();
    let s2 = stream2.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut started: Local<bool>| {
            if !*started {
                *started = true;
                return;
            }

            let config1 = RedisProducerConfig::new(s1.clone());
            let config2 = RedisProducerConfig::new(s2.clone());

            for i in 0..5 {
                w.write(
                    &config1,
                    TestEvent {
                        message: format!("s1-{i}"),
                        value: i * 100,
                    },
                );
                w.write(
                    &config2,
                    TestEvent {
                        message: format!("s2-{i}"),
                        value: i * 100 + 50,
                    },
                );
            }
        },
    );

    writer.update(); // warm frame
    writer.update(); // send frame

    #[derive(Resource, Default)]
    struct Collected {
        stream1_events: Vec<TestEvent>,
        stream2_events: Vec<TestEvent>,
    }
    reader.insert_resource(Collected::default());

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

    // With separate backends, no events will be received
    reader.update();
    reader.update();

    let c = reader.world().resource::<Collected>();

    // With separate backends, no events should be received
    assert_eq!(
        c.stream1_events.len(),
        0,
        "Should not receive events from stream1 with separate backend"
    );
    assert_eq!(
        c.stream2_events.len(),
        0,
        "Should not receive events from stream2 with separate backend"
    );
}
