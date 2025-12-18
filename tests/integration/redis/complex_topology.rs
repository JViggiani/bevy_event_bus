#![cfg(feature = "redis")]

use std::collections::{HashMap, HashSet};

use bevy::ecs::system::Local;
use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisMessageReader, RedisMessageWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group_membership, unique_topic, update_two_apps_until,
};
use integration_tests::utils::redis_setup;

#[derive(Resource, Default)]
struct Collected {
    per_group: HashMap<String, HashMap<String, Vec<String>>>,
}

impl Collected {
    fn record(&mut self, group: &str, stream: &str, message: String) {
        self.per_group
            .entry(group.to_string())
            .or_default()
            .entry(stream.to_string())
            .or_default()
            .push(message);
    }
}

#[derive(Default)]
struct WriterDispatchState {
    primed: bool,
    dispatched: bool,
}

#[test]
fn complex_topology_no_cross_talk() {
    let stream_a = unique_topic("complex_a");
    let stream_b = unique_topic("complex_b");
    let stream_c = unique_topic("complex_c");

    let membership_alpha = unique_consumer_group_membership("complex_alpha");
    let membership_beta = unique_consumer_group_membership("complex_beta");
    let membership_gamma = unique_consumer_group_membership("complex_gamma");

    let database_index = redis_setup::allocate_database().expect("Redis database allocated");

    // Reader backend pre-provisions all streams and groups within a single topology.
    let reader_streams = (stream_a.clone(), stream_b.clone(), stream_c.clone());
    let reader_groups = (
        membership_alpha.clone(),
        membership_beta.clone(),
        membership_gamma.clone(),
    );
    let (reader_backend, _reader_ctx) = redis_setup::with_database(database_index, || {
        redis_setup::prepare_backend(|builder| {
            builder
                .add_stream(RedisStreamSpec::new(reader_streams.0.clone()))
                .add_stream(RedisStreamSpec::new(reader_streams.1.clone()))
                .add_stream(RedisStreamSpec::new(reader_streams.2.clone()))
                .add_consumer_group(RedisConsumerGroupSpec::new(
                    [reader_streams.0.clone(), reader_streams.1.clone()],
                    reader_groups.0.group.clone(),
                    reader_groups.0.member.clone(),
                ))
                .add_consumer_group(RedisConsumerGroupSpec::new(
                    [reader_streams.1.clone(), reader_streams.2.clone()],
                    reader_groups.1.group.clone(),
                    reader_groups.1.member.clone(),
                ))
                .add_consumer_group(RedisConsumerGroupSpec::new(
                    [reader_streams.0.clone(), reader_streams.2.clone()],
                    reader_groups.2.group.clone(),
                    reader_groups.2.member.clone(),
                ))
                .add_event_single::<TestEvent>(reader_streams.0.clone())
                .add_event_single::<TestEvent>(reader_streams.1.clone())
                .add_event_single::<TestEvent>(reader_streams.2.clone());
        })
    })
    .expect("Redis reader backend setup successful");

    // Writer backend shares the same database but only needs stream bindings.
    let writer_streams = (stream_a.clone(), stream_b.clone(), stream_c.clone());
    let (writer_backend, _writer_ctx) = redis_setup::with_database(database_index, || {
        redis_setup::prepare_backend(|builder| {
            builder
                .add_stream(RedisStreamSpec::new(writer_streams.0.clone()))
                .add_stream(RedisStreamSpec::new(writer_streams.1.clone()))
                .add_stream(RedisStreamSpec::new(writer_streams.2.clone()))
                .add_event_single::<TestEvent>(writer_streams.0.clone())
                .add_event_single::<TestEvent>(writer_streams.1.clone())
                .add_event_single::<TestEvent>(writer_streams.2.clone());
        })
    })
    .expect("Redis writer backend setup successful");

    let message_catalog: HashMap<String, TestEvent> = [
        (
            stream_a.clone(),
            TestEvent {
                message: "payload-a".to_string(),
                value: 101,
            },
        ),
        (
            stream_b.clone(),
            TestEvent {
                message: "payload-b".to_string(),
                value: 202,
            },
        ),
        (
            stream_c.clone(),
            TestEvent {
                message: "payload-c".to_string(),
                value: 303,
            },
        ),
    ]
    .into_iter()
    .collect();

    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(writer_backend));

    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(reader_backend));
    reader_app.insert_resource(Collected::default());

    let writer_messages = message_catalog.clone();
    writer_app.add_systems(
        Update,
        move |mut writer: RedisMessageWriter, mut state: Local<WriterDispatchState>| {
            if state.dispatched {
                return;
            }

            if !state.primed {
                state.primed = true;
                return;
            }

            for (stream, event) in &writer_messages {
                let config = RedisProducerConfig::new(stream.clone());
                writer.write(&config, event.clone(), None);
            }

            state.dispatched = true;
        },
    );

    let group_specs: Vec<(String, Vec<String>)> = vec![
        (
            membership_alpha.group.clone(),
            vec![stream_a.clone(), stream_b.clone()],
        ),
        (
            membership_beta.group.clone(),
            vec![stream_b.clone(), stream_c.clone()],
        ),
        (
            membership_gamma.group.clone(),
            vec![stream_a.clone(), stream_c.clone()],
        ),
    ];

    for (group, streams) in group_specs.clone() {
        let group_id = group.clone();
        let streams = streams.clone();
        reader_app.add_systems(
            Update,
            move |mut reader: RedisMessageReader<TestEvent>, mut collected: ResMut<Collected>| {
                let config = RedisConsumerConfig::new(group_id.clone(), streams.clone());
                for wrapper in reader.read(&config) {
                    let metadata = wrapper
                        .metadata()
                        .redis_metadata()
                        .expect("Redis metadata present");
                    if let Some(observed_group) = metadata.consumer_group.as_deref() {
                        if observed_group == group_id {
                            collected.record(
                                observed_group,
                                &metadata.stream,
                                wrapper.event().message.clone(),
                            );
                        }
                    }
                }
            },
        );
    }

    run_app_updates(&mut reader_app, 10);

    let expected_per_group: HashMap<String, HashSet<String>> = [
        (
            membership_alpha.group.clone(),
            vec![stream_a.clone(), stream_b.clone()]
                .into_iter()
                .collect(),
        ),
        (
            membership_beta.group.clone(),
            vec![stream_b.clone(), stream_c.clone()]
                .into_iter()
                .collect(),
        ),
        (
            membership_gamma.group.clone(),
            vec![stream_a.clone(), stream_c.clone()]
                .into_iter()
                .collect(),
        ),
    ]
    .into_iter()
    .collect();

    let (satisfied, _) =
        update_two_apps_until(&mut writer_app, &mut reader_app, 6_000, |_, reader_app| {
            let collected = reader_app.world().resource::<Collected>();
            expected_per_group.iter().all(|(group, expected_streams)| {
                if let Some(stream_map) = collected.per_group.get(group) {
                    expected_streams.iter().all(|stream| {
                        stream_map
                            .get(stream)
                            .map(|entries| !entries.is_empty())
                            .unwrap_or(false)
                    })
                } else {
                    false
                }
            })
        });

    if !satisfied {
        let collected = reader_app.world().resource::<Collected>();
        panic!(
            "Timed out waiting for Redis consumer groups to receive events; observed state: {:?}",
            collected.per_group
        );
    }

    let collected = reader_app.world().resource::<Collected>();
    for (group, expected_streams) in &expected_per_group {
        let stream_map = collected.per_group.get(group).expect("group data recorded");
        assert_eq!(
            stream_map.keys().collect::<HashSet<_>>(),
            expected_streams.iter().collect::<HashSet<_>>(),
            "Group {group} observed unexpected streams",
        );

        for stream in expected_streams {
            let entries = stream_map
                .get(stream)
                .expect("expected stream events present");
            let expected_message = &message_catalog
                .get(stream)
                .expect("message catalog contains stream")
                .message;
            assert!(
                !entries.is_empty(),
                "Group {group} should receive at least one event from stream {stream}",
            );
            assert!(
                entries.iter().all(|entry| entry == expected_message),
                "Group {group} received incorrect payload(s) from stream {stream}",
            );
        }
    }
}
