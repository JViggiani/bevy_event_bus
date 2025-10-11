#![cfg(feature = "kafka")]

use std::collections::{HashMap, HashSet};

use bevy::ecs::system::Local;
use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{EventBusPlugins, KafkaEventReader, KafkaEventWriter};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_topic, update_two_apps_until,
};
use integration_tests::utils::kafka_setup;

#[derive(Resource, Default)]
struct Collected {
    per_group: HashMap<String, HashMap<String, Vec<String>>>,
}

impl Collected {
    fn record(&mut self, group: &str, topic: &str, message: String) {
        self.per_group
            .entry(group.to_string())
            .or_default()
            .entry(topic.to_string())
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
fn complex_topology_routing() {
    let topic_alpha = unique_topic("complex_alpha");
    let topic_beta = unique_topic("complex_beta");
    let topic_gamma = unique_topic("complex_gamma");

    let group_alpha = unique_consumer_group("complex_group_alpha");
    let group_beta = unique_consumer_group("complex_group_beta");
    let group_gamma = unique_consumer_group("complex_group_gamma");

    let reader_builder = kafka_setup::build_topology(|builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_alpha.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_topic(
                KafkaTopicSpec::new(topic_beta.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_topic(
                KafkaTopicSpec::new(topic_gamma.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_consumer_group(
                group_alpha.clone(),
                KafkaConsumerGroupSpec::new([topic_alpha.clone(), topic_beta.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            )
            .add_consumer_group(
                group_beta.clone(),
                KafkaConsumerGroupSpec::new([topic_beta.clone(), topic_gamma.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            )
            .add_consumer_group(
                group_gamma.clone(),
                KafkaConsumerGroupSpec::new([topic_alpha.clone(), topic_gamma.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            )
            .add_event_single::<TestEvent>(topic_alpha.clone())
            .add_event_single::<TestEvent>(topic_beta.clone())
            .add_event_single::<TestEvent>(topic_gamma.clone());
    });
    let (reader_backend, _reader_bootstrap) =
        kafka_setup::prepare_backend((reader_builder, kafka_setup::SetupOptions::earliest()));

    let writer_builder = kafka_setup::build_topology(|builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_alpha.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_topic(
                KafkaTopicSpec::new(topic_beta.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_topic(
                KafkaTopicSpec::new(topic_gamma.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_event_single::<TestEvent>(topic_alpha.clone())
            .add_event_single::<TestEvent>(topic_beta.clone())
            .add_event_single::<TestEvent>(topic_gamma.clone());
    });
    let (writer_backend, _writer_bootstrap) =
        kafka_setup::prepare_backend((writer_builder, kafka_setup::SetupOptions::earliest()));

    let message_catalog: HashMap<String, TestEvent> = [
        (
            topic_alpha.clone(),
            TestEvent {
                message: "payload-alpha".to_string(),
                value: 11,
            },
        ),
        (
            topic_beta.clone(),
            TestEvent {
                message: "payload-beta".to_string(),
                value: 22,
            },
        ),
        (
            topic_gamma.clone(),
            TestEvent {
                message: "payload-gamma".to_string(),
                value: 33,
            },
        ),
    ]
    .into_iter()
    .collect();

    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(writer_backend));

    let writer_messages = message_catalog.clone();
    writer_app.add_systems(
        Update,
        move |mut writer: KafkaEventWriter, mut state: Local<WriterDispatchState>| {
            if state.dispatched {
                return;
            }

            if !state.primed {
                state.primed = true;
                return;
            }

            for (topic, event) in &writer_messages {
                let config = KafkaProducerConfig::new([topic.clone()]);
                writer.write(&config, event.clone());
            }

            state.dispatched = true;
        },
    );

    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(reader_backend));
    reader_app.insert_resource(Collected::default());

    let group_topics: Vec<(String, Vec<String>)> = vec![
        (
            group_alpha.clone(),
            vec![topic_alpha.clone(), topic_beta.clone()],
        ),
        (
            group_beta.clone(),
            vec![topic_beta.clone(), topic_gamma.clone()],
        ),
        (
            group_gamma.clone(),
            vec![topic_alpha.clone(), topic_gamma.clone()],
        ),
    ];

    let expected_groups: HashSet<String> = group_topics
        .iter()
        .map(|(group, _)| group.clone())
        .collect();

    reader_app.add_systems(Update, {
        let group_topics = group_topics.clone();
        let expected_groups = expected_groups.clone();
        move |mut reader: KafkaEventReader<TestEvent>, mut collected: ResMut<Collected>| {
            for (group, topics) in &group_topics {
                let config = KafkaConsumerConfig::new(group.clone(), topics.clone())
                    .auto_offset_reset("earliest");
                for wrapper in reader.read(&config) {
                    let metadata = wrapper
                        .metadata()
                        .kafka_metadata()
                        .expect("Kafka metadata present");
                    if let Some(group_id) = metadata.consumer_group.as_deref() {
                        if expected_groups.contains(group_id) {
                            collected.record(
                                group_id,
                                &metadata.topic,
                                wrapper.event().message.clone(),
                            );
                        }
                    }
                }
            }
        }
    });

    run_app_updates(&mut reader_app, 10);

    let expected_per_group: HashMap<String, HashSet<String>> = [
        (
            group_alpha.clone(),
            vec![topic_alpha.clone(), topic_beta.clone()]
                .into_iter()
                .collect(),
        ),
        (
            group_beta.clone(),
            vec![topic_beta.clone(), topic_gamma.clone()]
                .into_iter()
                .collect(),
        ),
        (
            group_gamma.clone(),
            vec![topic_alpha.clone(), topic_gamma.clone()]
                .into_iter()
                .collect(),
        ),
    ]
    .into_iter()
    .collect();

    let (satisfied, _) =
        update_two_apps_until(&mut writer_app, &mut reader_app, 12_000, |_, reader_app| {
            let collected = reader_app.world().resource::<Collected>();
            expected_per_group.iter().all(|(group, expected_topics)| {
                collected
                    .per_group
                    .get(group)
                    .map(|topic_map| {
                        expected_topics.iter().all(|topic| {
                            topic_map
                                .get(topic)
                                .map(|entries| entries.len() >= 1)
                                .unwrap_or(false)
                        }) && topic_map
                            .keys()
                            .all(|observed| expected_topics.contains(observed))
                    })
                    .unwrap_or(false)
            })
        });

    if !satisfied {
        let collected = reader_app.world().resource::<Collected>();
        panic!(
            "Timed out waiting for Kafka consumer groups to receive events; observed state: {:?}",
            collected.per_group
        );
    }

    let collected = reader_app.world().resource::<Collected>();
    assert_eq!(
        collected.per_group.keys().collect::<HashSet<_>>(),
        expected_per_group.keys().collect::<HashSet<_>>(),
        "Unexpected Kafka consumer groups observed"
    );

    for (group, expected_topics) in &expected_per_group {
        let topic_map = collected.per_group.get(group).expect("group data recorded");

        assert_eq!(
            topic_map.keys().collect::<HashSet<_>>(),
            expected_topics.iter().collect::<HashSet<_>>(),
            "Group {group} observed unexpected topics"
        );

        for topic in expected_topics {
            let entries = topic_map.get(topic).expect("expected topic events present");
            let expected_message = &message_catalog
                .get(topic)
                .expect("message catalog contains topic")
                .message;
            assert!(
                entries.len() >= 1,
                "Group {group} should receive at least one event from topic {topic}"
            );
            assert!(
                entries.iter().all(|entry| entry == expected_message),
                "Group {group} received incorrect payload(s) from topic {topic}"
            );
        }
    }
}
