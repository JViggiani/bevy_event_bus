#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    unique_consumer_group, unique_consumer_group_member, unique_consumer_group_membership,
    unique_topic,
};
use integration_tests::utils::redis_setup;

#[test]
fn per_stream_order_preserved() {
    let stream = unique_topic("ordered");
    let membership = unique_consumer_group_membership("ordering_single_stream");
    let consumer_group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let writer_db =
        redis_setup::allocate_database().expect("Writer Redis backend setup successful");
    let reader_db =
        redis_setup::allocate_database().expect("Reader Redis backend setup successful");

    let writer_stream = stream.clone();
    let (writer_backend, _context1) = redis_setup::with_database(writer_db, || {
        redis_setup::prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer_stream.clone()))
                .add_event_single::<TestEvent>(writer_stream.clone());
        })
    })
    .expect("Writer Redis backend setup successful");

    let reader_stream = stream.clone();
    let reader_group = consumer_group.clone();
    let reader_consumer = consumer_name.clone();
    let (reader_backend, _context2) = redis_setup::with_database(reader_db, || {
        redis_setup::prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader_stream.clone()))
                .add_consumer_group(RedisConsumerGroupSpec::new(
                    [reader_stream.clone()],
                    reader_group.clone(),
                    reader_consumer.clone(),
                ))
                .add_event_single::<TestEvent>(reader_stream.clone());
        })
    })
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
            let config = RedisConsumerConfig::new(group_clone.clone(), [stream_clone.clone()]);
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
    let consumer_name = unique_consumer_group_member(&consumer_group);

    let writer_db =
        redis_setup::allocate_database().expect("Writer Redis backend setup successful");
    let reader_db =
        redis_setup::allocate_database().expect("Reader Redis backend setup successful");

    let writer_stream1 = stream1.clone();
    let writer_stream2 = stream2.clone();
    let (writer_backend, _context1) = redis_setup::with_database(writer_db, || {
        redis_setup::prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer_stream1.clone()))
                .add_stream(RedisStreamSpec::new(writer_stream2.clone()))
                .add_event_single::<TestEvent>(writer_stream1.clone())
                .add_event_single::<TestEvent>(writer_stream2.clone());
        })
    })
    .expect("Writer Redis backend setup successful");

    let reader_stream1 = stream1.clone();
    let reader_stream2 = stream2.clone();
    let reader_group = consumer_group.clone();
    let reader_consumer = consumer_name.clone();
    let (reader_backend, _context2) = redis_setup::with_database(reader_db, || {
        redis_setup::prepare_backend(move |builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader_stream1.clone()))
                .add_stream(RedisStreamSpec::new(reader_stream2.clone()))
                .add_consumer_group(RedisConsumerGroupSpec::new(
                    [reader_stream1.clone(), reader_stream2.clone()],
                    reader_group.clone(),
                    reader_consumer.clone(),
                ))
                .add_event_single::<TestEvent>(reader_stream1.clone())
                .add_event_single::<TestEvent>(reader_stream2.clone());
        })
    })
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
            let config1 = RedisConsumerConfig::new(group_clone.clone(), [s1_clone.clone()]);
            let config2 = RedisConsumerConfig::new(group_clone.clone(), [s2_clone.clone()]);

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
