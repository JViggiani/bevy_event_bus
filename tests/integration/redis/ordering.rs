#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    unique_consumer_group, unique_consumer_group_member, unique_consumer_group_membership,
    unique_topic, wait_for_events,
};
use integration_tests::utils::redis_setup;

#[test]
fn per_stream_order_preserved() {
    let stream = unique_topic("ordered");
    let membership = unique_consumer_group_membership("ordering_single_stream");
    let consumer_group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let stream_for_writer = stream.clone();
    let (backend_writer, _ctx_writer) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_writer.clone()))
            .add_event::<TestEvent>([stream_for_writer.clone()]);
    })
    .expect("Redis writer backend setup successful");

    let stream_for_reader = stream.clone();
    let group_for_reader = consumer_group.clone();
    let consumer_for_reader = consumer_name.clone();
    let (backend_reader, _ctx_reader) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_reader.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream_for_reader.clone()],
                group_for_reader.clone(),
                consumer_for_reader.clone(),
            ))
            .add_event::<TestEvent>([stream_for_reader.clone()]);
    })
    .expect("Redis reader backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_writer));

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_reader));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);

    reader.insert_resource(Collected::default());

    let stream_for_reader = stream.clone();
    let group_for_reader = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut reader: RedisEventReader<TestEvent>, mut collected: ResMut<Collected>| {
            let config =
                RedisConsumerConfig::new(group_for_reader.clone(), [stream_for_reader.clone()]);
            for wrapper in reader.read(&config) {
                collected.0.push(wrapper.event().clone());
            }
        },
    );

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut writer: RedisEventWriter, mut started: Local<bool>, mut counter: Local<u32>| {
            if !*started {
                *started = true;
                return;
            }

            if *counter >= 10 {
                return;
            }

            let config = RedisProducerConfig::new(stream_for_writer.clone());
            writer.write(
                &config,
                TestEvent {
                    message: format!("msg-{}", *counter),
                    value: *counter as i32,
                },
            );
            *counter += 1;
        },
    );

    for _ in 0..12 {
        writer.update();
    }

    let collected_events = wait_for_events(&mut reader, &stream, 12_000, 10, |app| {
        let collected = app.world().resource::<Collected>();
        collected.0.clone()
    });
    assert_eq!(collected_events.len(), 10);
    let mut last = -1;
    for event in &collected_events {
        assert!(event.value > last);
        last = event.value;
    }

    let collected = reader.world().resource::<Collected>();
    assert_eq!(collected.0.len(), 10);
}

#[test]
fn cross_stream_interleave_each_ordered() {
    let stream1 = unique_topic("t1");
    let stream2 = unique_topic("t2");
    let consumer_group = unique_consumer_group("ordering_multi_stream");
    let consumer_name = unique_consumer_group_member(&consumer_group);

    let stream1_for_writer = stream1.clone();
    let stream2_for_writer = stream2.clone();
    let (backend_writer, _ctx_writer) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream1_for_writer.clone()))
            .add_stream(RedisStreamSpec::new(stream2_for_writer.clone()))
            .add_event::<TestEvent>([stream1_for_writer.clone(), stream2_for_writer.clone()]);
    })
    .expect("Redis writer backend setup successful");

    let stream1_for_reader = stream1.clone();
    let stream2_for_reader = stream2.clone();
    let group_for_reader = consumer_group.clone();
    let consumer_for_reader = consumer_name.clone();
    let (backend_reader, _ctx_reader) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream1_for_reader.clone()))
            .add_stream(RedisStreamSpec::new(stream2_for_reader.clone()))
            .add_consumer_group(RedisConsumerGroupSpec::new(
                [stream1_for_reader.clone(), stream2_for_reader.clone()],
                group_for_reader.clone(),
                consumer_for_reader.clone(),
            ))
            .add_event::<TestEvent>([stream1_for_reader.clone()])
            .add_event::<TestEvent>([stream2_for_reader.clone()]);
    })
    .expect("Redis reader backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_writer));

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_reader));

    #[derive(Resource, Default)]
    struct Collected {
        stream1_events: Vec<TestEvent>,
        stream2_events: Vec<TestEvent>,
    }

    reader.insert_resource(Collected::default());

    let stream1_for_reader = stream1.clone();
    let stream2_for_reader = stream2.clone();
    let group_for_reader = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut reader: RedisEventReader<TestEvent>, mut collected: ResMut<Collected>| {
            let config1 =
                RedisConsumerConfig::new(group_for_reader.clone(), [stream1_for_reader.clone()]);
            for wrapper in reader.read(&config1) {
                collected.stream1_events.push(wrapper.event().clone());
            }

            let config2 =
                RedisConsumerConfig::new(group_for_reader.clone(), [stream2_for_reader.clone()]);
            for wrapper in reader.read(&config2) {
                collected.stream2_events.push(wrapper.event().clone());
            }
        },
    );

    let stream1_for_writer = stream1.clone();
    let stream2_for_writer = stream2.clone();
    writer.add_systems(
        Update,
        move |mut writer: RedisEventWriter, mut started: Local<bool>, mut counter: Local<u32>| {
            if !*started {
                *started = true;
                return;
            }

            if *counter >= 5 {
                return;
            }

            let config1 = RedisProducerConfig::new(stream1_for_writer.clone());
            let config2 = RedisProducerConfig::new(stream2_for_writer.clone());

            writer.write(
                &config1,
                TestEvent {
                    message: format!("s1-{}", *counter),
                    value: *counter as i32,
                },
            );
            writer.write(
                &config2,
                TestEvent {
                    message: format!("s2-{}", *counter),
                    value: *counter as i32,
                },
            );
            *counter += 1;
        },
    );

    for _ in 0..12 {
        writer.update();
    }

    let _ = wait_for_events(&mut reader, &stream1, 15_000, 5, |app| {
        let collected = app.world().resource::<Collected>();
        collected.stream1_events.clone()
    });
    reader.update();
    let _ = wait_for_events(&mut reader, &stream2, 15_000, 5, |app| {
        let collected = app.world().resource::<Collected>();
        collected.stream2_events.clone()
    });

    let collected = reader.world().resource::<Collected>();

    assert_eq!(
        collected.stream1_events.len(),
        5,
        "Stream {} expected 5 events, got {}",
        stream1,
        collected.stream1_events.len()
    );
    assert_eq!(
        collected.stream2_events.len(),
        5,
        "Stream {} expected 5 events, got {}",
        stream2,
        collected.stream2_events.len()
    );

    for (i, event) in collected.stream1_events.iter().enumerate() {
        assert_eq!(event.value, i as i32);
        assert!(event.message.starts_with("s1-"));
    }

    for (i, event) in collected.stream2_events.iter().enumerate() {
        assert_eq!(event.value, i as i32);
        assert!(event.message.starts_with("s2-"));
    }
}
