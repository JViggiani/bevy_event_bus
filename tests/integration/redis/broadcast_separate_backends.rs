#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group_membership, unique_topic, update_until,
};
use integration_tests::utils::redis_setup;

#[derive(Resource, Default)]
struct EventCollector(Vec<TestEvent>);

/// Test that readers in DIFFERENT consumer groups each receive ALL messages (broadcast behavior)
/// Uses separate backends per consumer group to ensure proper isolation
#[test]
fn test_broadcast_with_separate_backends() {
    let stream = unique_topic("broadcast-separate");
    let membership1 = unique_consumer_group_membership("group1");
    let membership2 = unique_consumer_group_membership("group2");

    let reader1_stream = stream.clone();
    let reader1_group = membership1.group.clone();
    let reader1_consumer = membership1.member.clone();
    let (backend1, _context1) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(reader1_stream.clone()))
            .add_consumer_group(
                reader1_group.clone(),
                RedisConsumerGroupSpec::new(
                    [reader1_stream.clone()],
                    reader1_group.clone(),
                    reader1_consumer.clone(),
                ),
            )
            .add_event_single::<TestEvent>(reader1_stream.clone());
    })
    .expect("Redis backend1 setup successful");

    let reader2_stream = stream.clone();
    let reader2_group = membership2.group.clone();
    let reader2_consumer = membership2.member.clone();
    let (backend2, _context2) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(reader2_stream.clone()))
            .add_consumer_group(
                reader2_group.clone(),
                RedisConsumerGroupSpec::new(
                    [reader2_stream.clone()],
                    reader2_group.clone(),
                    reader2_consumer.clone(),
                ),
            )
            .add_event_single::<TestEvent>(reader2_stream.clone());
    })
    .expect("Redis backend2 setup successful");

    let writer_stream = stream.clone();
    let (writer_backend, _writer_context) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(writer_stream.clone()))
            .add_event_single::<TestEvent>(writer_stream.clone());
    })
    .expect("Redis writer backend setup successful");

    // Setup reader1 app with backend1
    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(backend1));
    reader1.insert_resource(EventCollector::default());

    let s1 = stream.clone();
    let g1 = membership1.group.clone();
    let c1 = membership1.member.clone();
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g1.clone(), c1.clone(), [s1.clone()]);
            let initial_count = c.0.len();
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
            if c.0.len() > initial_count {
                println!("Reader1 (group1) total events: {}", c.0.len());
            }
        },
    );

    // Setup reader2 app with backend2
    let mut reader2 = App::new();
    reader2.add_plugins(EventBusPlugins(backend2));
    reader2.insert_resource(EventCollector::default());

    let s2 = stream.clone();
    let g2 = membership2.group.clone();
    let c2 = membership2.member.clone();
    reader2.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config = RedisConsumerConfig::new(g2.clone(), c2.clone(), [s2.clone()]);
            let initial_count = c.0.len();
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
            if c.0.len() > initial_count {
                println!("Reader2 (group2) total events: {}", c.0.len());
            }
        },
    );

    // Setup writer app with writer backend
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(writer_backend));

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<usize>| {
            if *sent < 4 {
                let config = RedisProducerConfig::new(stream_for_writer.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: format!("broadcast_{}", *sent),
                        value: *sent as i32,
                    },
                );
                println!("Writer sent event {}: broadcast_{}", *sent, *sent);
                *sent += 1;
            }
        },
    );

    // Start readers first to establish consumer groups
    println!("Starting reader1 (group1)...");
    run_app_updates(&mut reader1, 3);
    println!("Starting reader2 (group2)...");
    run_app_updates(&mut reader2, 3);

    // Wait for consumer groups to be established
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Send events
    println!("Starting writer to send 4 events...");
    run_app_updates(&mut writer, 5);

    // Wait for message distribution
    let (_received1, _) = update_until(&mut reader1, 5000, |app| {
        let collected = app.world().resource::<EventCollector>();
        collected.0.len() >= 4 // Expect all messages
    });

    let (_received2, _) = update_until(&mut reader2, 5000, |app| {
        let collected = app.world().resource::<EventCollector>();
        collected.0.len() >= 4 // Expect all messages
    });

    let collected1 = reader1.world().resource::<EventCollector>();
    let collected2 = reader2.world().resource::<EventCollector>();

    println!(
        "Test result: Reader1 (group1) got {} events, Reader2 (group2) got {} events",
        collected1.0.len(),
        collected2.0.len()
    );

    // Separate consumer groups on the shared backend should each observe the complete stream.
    assert_eq!(
        collected1.0.len(),
        4,
        "Reader1 should receive all broadcast events"
    );
    assert_eq!(
        collected2.0.len(),
        4,
        "Reader2 should receive all broadcast events"
    );
}
