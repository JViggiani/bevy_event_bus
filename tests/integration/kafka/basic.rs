use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{EventBusPlugins, KafkaMessageReader, KafkaMessageWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_topic, update_two_apps_until, update_until,
};
use integration_tests::utils::kafka_setup;

#[test]
fn kafka_single_direction_writer_reader_flow() {
    let topic = unique_topic("kafka-basic-writer-reader");
    let consumer_group = unique_consumer_group("kafka_basic_reader");

    let topic_for_writer = topic.clone();
    let (backend_writer, _bootstrap_writer) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
            builder
                .add_topic(
                    KafkaTopicSpec::new(topic_for_writer.clone())
                        .partitions(1)
                        .replication(1),
                )
                .add_event_single::<TestEvent>(topic_for_writer.clone());
        }));

    let topic_for_reader = topic.clone();
    let group_for_reader = consumer_group.clone();
    let (backend_reader, _bootstrap_reader) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
            builder
                .add_topic(
                    KafkaTopicSpec::new(topic_for_reader.clone())
                        .partitions(1)
                        .replication(1),
                )
                .add_consumer_group(
                    group_for_reader.clone(),
                    KafkaConsumerGroupSpec::new([topic_for_reader.clone()])
                        .initial_offset(KafkaInitialOffset::Earliest),
                )
                .add_event_single::<TestEvent>(topic_for_reader.clone());
        }));

    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(backend_reader));

    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    reader_app.insert_resource(Collected::default());
    #[derive(Resource, Clone)]
    struct Topic(String);
    #[derive(Resource, Clone)]
    struct ConsumerGroup(String);
    reader_app.insert_resource(Topic(topic.clone()));
    reader_app.insert_resource(ConsumerGroup(consumer_group.clone()));

    fn reader_system(
        mut reader: KafkaMessageReader<TestEvent>,
        topic: Res<Topic>,
        group: Res<ConsumerGroup>,
        mut collected: ResMut<Collected>,
    ) {
        let config = KafkaConsumerConfig::new(group.0.clone(), [topic.0.clone()]);
        for wrapper in reader.read(&config) {
            collected.0.push(wrapper.event().clone());
        }
    }
    reader_app.add_systems(Update, reader_system);

    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(backend_writer));

    #[derive(Resource, Clone)]
    struct Outgoing(TestEvent, String);

    let event_to_send = TestEvent {
        message: "From Kafka Writer".into(),
        value: 100,
    };
    writer_app.insert_resource(Outgoing(event_to_send.clone(), topic.clone()));

    fn writer_system(mut writer: KafkaMessageWriter, data: Res<Outgoing>, mut sent: Local<bool>) {
        if *sent {
            return;
        }
        let config = KafkaProducerConfig::new([data.1.clone()]);
        writer.write(&config, data.0.clone(), None);
        *sent = true;
    }
    writer_app.add_systems(Update, writer_system);

    run_app_updates(&mut writer_app, 20);

    let expected_event = event_to_send.clone();
    let (received, _) = update_until(&mut reader_app, 8_000, move |app| {
        let collected = app.world().resource::<Collected>();
        collected.0.iter().any(|event| event == &expected_event)
    });

    assert!(received, "Expected reader to observe the writer's event");
}

#[test]
fn kafka_bidirectional_apps_exchange_events() {
    let topic = unique_topic("kafka-bidirectional");
    let group_a = unique_consumer_group("kafka_app_a");
    let group_b = unique_consumer_group("kafka_app_b");

    let topic_for_app_a = topic.clone();
    let group_for_app_a = group_a.clone();
    let (backend_a, _bootstrap_a) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
            builder
                .add_topic(
                    KafkaTopicSpec::new(topic_for_app_a.clone())
                        .partitions(1)
                        .replication(1),
                )
                .add_consumer_group(
                    group_for_app_a.clone(),
                    KafkaConsumerGroupSpec::new([topic_for_app_a.clone()])
                        .initial_offset(KafkaInitialOffset::Earliest),
                )
                .add_event_single::<TestEvent>(topic_for_app_a.clone());
        }));

    let topic_for_app_b = topic.clone();
    let group_for_app_b = group_b.clone();
    let (backend_b, _bootstrap_b) =
        kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
            builder
                .add_topic(
                    KafkaTopicSpec::new(topic_for_app_b.clone())
                        .partitions(1)
                        .replication(1),
                )
                .add_consumer_group(
                    group_for_app_b.clone(),
                    KafkaConsumerGroupSpec::new([topic_for_app_b.clone()])
                        .initial_offset(KafkaInitialOffset::Earliest),
                )
                .add_event_single::<TestEvent>(topic_for_app_b.clone());
        }));

    #[derive(Resource, Default)]
    struct Received(Vec<TestEvent>);

    #[derive(Resource, Clone)]
    struct TopicName(String);

    #[derive(Resource, Clone)]
    struct GroupName(String);

    #[derive(Resource, Clone)]
    struct OutgoingEvents {
        topic: String,
        events: Vec<TestEvent>,
        sent: bool,
    }

    fn reader_system(
        mut reader: KafkaMessageReader<TestEvent>,
        topic: Res<TopicName>,
        group: Res<GroupName>,
        mut received: ResMut<Received>,
    ) {
        let config = KafkaConsumerConfig::new(group.0.clone(), [topic.0.clone()]);
        for wrapper in reader.read(&config) {
            received.0.push(wrapper.event().clone());
        }
    }

    fn writer_system(mut writer: KafkaMessageWriter, mut outgoing: ResMut<OutgoingEvents>) {
        if outgoing.sent {
            return;
        }
        let config = KafkaProducerConfig::new([outgoing.topic.clone()]);
        for event in outgoing.events.clone() {
            writer.write(&config, event, None);
        }
        outgoing.sent = true;
    }

    let mut app_a = App::new();
    app_a.add_plugins(EventBusPlugins(backend_a));
    let event_from_a = TestEvent {
        message: "event-from-kafka-app-a".into(),
        value: 1,
    };
    app_a.insert_resource(Received::default());
    app_a.insert_resource(TopicName(topic.clone()));
    app_a.insert_resource(GroupName(group_a));
    app_a.insert_resource(OutgoingEvents {
        topic: topic.clone(),
        events: vec![event_from_a.clone()],
        sent: false,
    });
    app_a.add_systems(Update, (reader_system, writer_system));

    let mut app_b = App::new();
    app_b.add_plugins(EventBusPlugins(backend_b));
    let event_from_b = TestEvent {
        message: "event-from-kafka-app-b".into(),
        value: 2,
    };
    app_b.insert_resource(Received::default());
    app_b.insert_resource(TopicName(topic.clone()));
    app_b.insert_resource(GroupName(group_b));
    app_b.insert_resource(OutgoingEvents {
        topic,
        events: vec![event_from_b.clone()],
        sent: false,
    });
    app_b.add_systems(Update, (reader_system, writer_system));

    let expected_events = [event_from_a.clone(), event_from_b.clone()];

    let (success, _) = update_two_apps_until(&mut app_a, &mut app_b, 10_000, |app_a, app_b| {
        let app_a_has_all = {
            let world_a = app_a.world();
            let received_a = world_a.resource::<Received>();
            expected_events
                .iter()
                .all(|event| received_a.0.iter().any(|seen| seen == event))
        };

        let app_b_has_all = {
            let world_b = app_b.world();
            let received_b = world_b.resource::<Received>();
            expected_events
                .iter()
                .all(|event| received_b.0.iter().any(|seen| seen == event))
        };

        app_a_has_all && app_b_has_all
    });

    assert!(
        success,
        "Both Kafka apps should observe each other's events"
    );
}
