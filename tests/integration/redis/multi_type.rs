#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::helpers::{
    unique_consumer_group_membership, unique_topic, update_until,
};
use integration_tests::utils::redis_setup;
use integration_tests::utils::{TestEvent, UserLoginEvent};

#[test]
fn single_topic_multiple_types_same_frame() {
    let stream = unique_topic("multi_type_same_frame");
    let membership = unique_consumer_group_membership("multi_type_same_frame");
    let consumer_group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let stream_for_writer = stream.clone();
    let (backend_writer, _ctx_writer) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_writer.clone()))
            .add_event_single::<TestEvent>(stream_for_writer.clone())
            .add_event_single::<UserLoginEvent>(stream_for_writer.clone());
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
            .add_event_single::<TestEvent>(stream_for_reader.clone())
            .add_event_single::<UserLoginEvent>(stream_for_reader.clone());
    })
    .expect("Redis reader backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_writer));

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_reader));

    #[derive(Resource, Default)]
    struct CollectedTests(Vec<TestEvent>);

    #[derive(Resource, Default)]
    struct CollectedLogins(Vec<UserLoginEvent>);

    reader.insert_resource(CollectedTests::default());
    reader.insert_resource(CollectedLogins::default());

    let stream_for_tests = stream.clone();
    let group_for_tests = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut reader: RedisEventReader<TestEvent>, mut collected: ResMut<CollectedTests>| {
            let config =
                RedisConsumerConfig::new(group_for_tests.clone(), [stream_for_tests.clone()]);
            for wrapper in reader.read(&config) {
                collected.0.push(wrapper.event().clone());
            }
        },
    );

    let stream_for_logins = stream.clone();
    let group_for_logins = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut reader: RedisEventReader<UserLoginEvent>,
              mut collected: ResMut<CollectedLogins>| {
            let config =
                RedisConsumerConfig::new(group_for_logins.clone(), [stream_for_logins.clone()]);
            for wrapper in reader.read(&config) {
                collected.0.push(wrapper.event().clone());
            }
        },
    );

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut writer: RedisEventWriter, mut started: Local<bool>| {
            if !*started {
                *started = true;
                return;
            }

            let config = RedisProducerConfig::new(stream_for_writer.clone());
            writer.write(
                &config,
                TestEvent {
                    message: "hello".to_string(),
                    value: 42,
                },
            );
            writer.write(
                &config,
                UserLoginEvent {
                    user_id: "u1".to_string(),
                    timestamp: 1,
                },
            );
        },
    );

    writer.update();
    writer.update();

    let (ok, _frames) = update_until(&mut reader, 5_000, |app| {
        let tests = app.world().resource::<CollectedTests>();
        let logins = app.world().resource::<CollectedLogins>();
        !tests.0.is_empty() && !logins.0.is_empty()
    });

    assert!(ok, "Timed out waiting for both event types within timeout");

    let tests = reader.world().resource::<CollectedTests>();
    let logins = reader.world().resource::<CollectedLogins>();
    assert_eq!(
        tests.0.len(),
        1,
        "Expected 1 TestEvent, got {}",
        tests.0.len()
    );
    assert_eq!(
        logins.0.len(),
        1,
        "Expected 1 UserLoginEvent, got {}",
        logins.0.len()
    );
}

#[test]
fn single_topic_multiple_types_interleaved_frames() {
    let stream = unique_topic("multi_type_interleaved");
    let membership = unique_consumer_group_membership("multi_type_interleaved");
    let consumer_group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let stream_for_writer = stream.clone();
    let (backend_writer, _ctx_writer) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_for_writer.clone()))
            .add_event_single::<TestEvent>(stream_for_writer.clone())
            .add_event_single::<UserLoginEvent>(stream_for_writer.clone());
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
            .add_event_single::<TestEvent>(stream_for_reader.clone())
            .add_event_single::<UserLoginEvent>(stream_for_reader.clone());
    })
    .expect("Redis reader backend setup successful");

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend_writer));

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins(backend_reader));

    #[derive(Resource, Default)]
    struct CollectedTests(Vec<TestEvent>);

    #[derive(Resource, Default)]
    struct CollectedLogins(Vec<UserLoginEvent>);

    reader.insert_resource(CollectedTests::default());
    reader.insert_resource(CollectedLogins::default());

    let stream_for_tests = stream.clone();
    let group_for_tests = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut reader: RedisEventReader<TestEvent>, mut collected: ResMut<CollectedTests>| {
            let config =
                RedisConsumerConfig::new(group_for_tests.clone(), [stream_for_tests.clone()]);
            for wrapper in reader.read(&config) {
                collected.0.push(wrapper.event().clone());
            }
        },
    );

    let stream_for_logins = stream.clone();
    let group_for_logins = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut reader: RedisEventReader<UserLoginEvent>,
              mut collected: ResMut<CollectedLogins>| {
            let config =
                RedisConsumerConfig::new(group_for_logins.clone(), [stream_for_logins.clone()]);
            for wrapper in reader.read(&config) {
                collected.0.push(wrapper.event().clone());
            }
        },
    );

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut writer: RedisEventWriter, mut counter: Local<u32>| {
            let config = RedisProducerConfig::new(stream_for_writer.clone());
            match *counter % 2 {
                0 => {
                    let _ = writer.write(
                        &config,
                        TestEvent {
                            message: format!("m{}", *counter),
                            value: *counter as i32,
                        },
                    );
                }
                _ => {
                    let _ = writer.write(
                        &config,
                        UserLoginEvent {
                            user_id: format!("u{}", *counter),
                            timestamp: *counter as u64,
                        },
                    );
                }
            }
            *counter += 1;
        },
    );

    for _ in 0..6 {
        writer.update();
    }

    let (ok, _frames) = update_until(&mut reader, 12_000, |app| {
        let tests = app.world().resource::<CollectedTests>();
        let logins = app.world().resource::<CollectedLogins>();
        tests.0.len() >= 3 && logins.0.len() >= 3
    });

    assert!(
        ok,
        "Timed out waiting for interleaved events within timeout"
    );

    let tests = reader.world().resource::<CollectedTests>();
    let logins = reader.world().resource::<CollectedLogins>();
    assert!(tests.0.len() >= 3);
    assert!(logins.0.len() >= 3);
}
