use bevy::prelude::*;
use bevy_event_bus::config::kafka::{
    KafkaConsumerConfig, KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaProducerConfig,
    KafkaTopicSpec,
};
use bevy_event_bus::{EventBusPlugins, KafkaMessageReader, KafkaMessageWriter};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{unique_consumer_group, unique_topic, wait_for_events};
use integration_tests::utils::kafka_setup;

#[test]
fn per_topic_order_preserved() {
    let topic = unique_topic("ordered");
    let consumer_group = unique_consumer_group("ordering_single_topic");

    let topic_for_writer = topic.clone();
    let (backend_w, _b1) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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
    let (backend_r, _b2) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
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

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins { backend: backend_w });

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins { backend: backend_r });

    #[derive(Resource, Default)]
    struct Collected {
        events: Vec<TestEvent>,
    }

    reader.insert_resource(Collected::default());

    let read_topic = topic.clone();
    let read_group = consumer_group.clone();
    reader.add_systems(
        Update,
        move |mut r: KafkaMessageReader<TestEvent>, mut c: ResMut<Collected>| {
            let config = KafkaConsumerConfig::new(read_group.clone(), [read_topic.clone()]);
            for wrapper in r.read(&config) {
                c.events.push(wrapper.event().clone());
            }
        },
    );

    let write_topic = topic.clone();
    writer.add_systems(
        Update,
        move |mut w: KafkaMessageWriter, mut counter: Local<u32>| {
            if *counter >= 10 {
                return;
            }

            let config = KafkaProducerConfig::new([write_topic.clone()]);
            w.write(
                &config,
                TestEvent {
                    message: format!("msg-{}", *counter),
                    value: *counter as i32,
                },
                None,
            );
            *counter += 1;
        },
    );

    for _ in 0..12 {
        writer.update();
    }

    // Eventual consistency loop: allow background producer task to deliver.
    let _ = wait_for_events(&mut reader, &topic, 12_000, 10, |app| {
        let c = app.world().resource::<Collected>();
        c.events.clone()
    });

    let c = reader.world().resource::<Collected>();
    let mut last = -1;
    for ev in &c.events {
        assert!(ev.value > last);
        last = ev.value;
    }
    assert_eq!(c.events.len(), 10);
}

#[test]
fn cross_topic_interleave_each_ordered() {
    let t1 = unique_topic("t1");
    let t2 = unique_topic("t2");
    let consumer_group = unique_consumer_group("ordering_dual_topic");

    let t1_writer = t1.clone();
    let t2_writer = t2.clone();
    let (backend_w, _b1) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(t1_writer.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_topic(
            KafkaTopicSpec::new(t2_writer.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_event::<TestEvent>([t1_writer.clone(), t2_writer.clone()]);
    }));

    let t1_reader = t1.clone();
    let t2_reader = t2.clone();
    let group_for_reader = consumer_group.clone();
    let (backend_r, _b2) = kafka_setup::prepare_backend(kafka_setup::earliest(move |builder| {
        builder.add_topic(
            KafkaTopicSpec::new(t1_reader.clone())
                .partitions(1)
                .replication(1),
        );
        builder.add_topic(
            KafkaTopicSpec::new(t2_reader.clone())
                .partitions(1)
                .replication(1),
        );
        builder
            .add_consumer_group(
                group_for_reader.clone(),
                KafkaConsumerGroupSpec::new([t1_reader.clone(), t2_reader.clone()])
                    .initial_offset(KafkaInitialOffset::Earliest),
            )
            .add_event_single::<TestEvent>(t1_reader.clone())
            .add_event_single::<TestEvent>(t2_reader.clone());
    }));

    bevy_event_bus::runtime();

    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins { backend: backend_w });

    let mut reader = App::new();
    reader.add_plugins(EventBusPlugins { backend: backend_r });

    let t1_runtime = t1.clone();
    let t2_runtime = t2.clone();
    writer.add_systems(
        Update,
        move |mut w: KafkaMessageWriter, mut started: Local<bool>, mut counter: Local<u32>| {
            if !*started {
                *started = true;
                return;
            }

            if *counter >= 5 {
                return;
            }

            let config_t1 = KafkaProducerConfig::new([t1_runtime.clone()]);
            let config_t2 = KafkaProducerConfig::new([t2_runtime.clone()]);
            w.write(
                &config_t1,
                TestEvent {
                    message: format!("A{}", *counter),
                    value: *counter as i32,
                },
                None,
            );
            w.write(
                &config_t2,
                TestEvent {
                    message: format!("B{}", *counter),
                    value: *counter as i32,
                },
                None,
            );
            *counter += 1;
        },
    );

    for _ in 0..12 {
        writer.update();
    }

    #[derive(Resource, Default)]
    struct CollectedT1 {
        events: Vec<TestEvent>,
    }

    #[derive(Resource, Default)]
    struct CollectedT2 {
        events: Vec<TestEvent>,
    }

    reader.insert_resource(CollectedT1::default());
    reader.insert_resource(CollectedT2::default());

    let read_group = consumer_group.clone();
    let read_t1 = t1.clone();
    let read_t2 = t2.clone();
    reader.add_systems(
        Update,
        move |mut r: KafkaMessageReader<TestEvent>,
              mut c1: ResMut<CollectedT1>,
              mut c2: ResMut<CollectedT2>| {
            let config_a = KafkaConsumerConfig::new(read_group.clone(), [read_t1.clone()]);
            for wrapper in r.read(&config_a) {
                c1.events.push(wrapper.event().clone());
            }

            let config_b = KafkaConsumerConfig::new(read_group.clone(), [read_t2.clone()]);
            for wrapper in r.read(&config_b) {
                c2.events.push(wrapper.event().clone());
            }
        },
    );

    let a_events = wait_for_events(&mut reader, &t1, 15_000, 5, |app| {
        let c1 = app.world().resource::<CollectedT1>();
        c1.events.clone()
    });

    // Give one extra update tick after first topic completes to reduce chance of asymmetric arrival.
    reader.update();

    let b_events = wait_for_events(&mut reader, &t2, 15_000, 5, |app| {
        let c2 = app.world().resource::<CollectedT2>();
        c2.events.clone()
    });

    assert_eq!(
        a_events.len(),
        5,
        "Topic {} expected 5 events got {}",
        t1,
        a_events.len()
    );
    assert_eq!(
        b_events.len(),
        5,
        "Topic {} expected 5 events got {}",
        t2,
        b_events.len()
    );

    for (i, ev) in a_events.iter().enumerate() {
        assert_eq!(ev.value, i as i32);
        assert!(ev.message.starts_with('A'));
    }
    for (i, ev) in b_events.iter().enumerate() {
        assert_eq!(ev.value, i as i32);
        assert!(ev.message.starts_with('B'));
    }
}
