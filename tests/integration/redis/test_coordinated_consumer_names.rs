#![cfg(feature = "redis")]

use bevy::prelude::*;
use bevy_event_bus::config::redis::{
    RedisConsumerConfig, RedisConsumerGroupSpec, RedisProducerConfig, RedisStreamSpec,
};
use bevy_event_bus::{EventBusPlugins, RedisEventReader, RedisEventWriter};
use integration_tests::utils::TestEvent;
use integration_tests::utils::helpers::{
    run_app_updates, unique_consumer_group, unique_consumer_group_member, unique_topic,
    update_until,
};
use integration_tests::utils::redis_setup;

#[derive(Resource, Default)]
struct EventCollector(Vec<TestEvent>);

/// Test consumer name coordination by setting the same name in both topology spec AND reader config
#[test]
fn test_coordinated_consumer_names() {
    let stream = unique_topic("coordinated-names");
    let consumer_group = unique_consumer_group("coordinated-group");
    let consumer_name1 = unique_consumer_group_member("coord-consumer-1");

    let stream_clone = stream.clone();
    let consumer_group_clone = consumer_group.clone();
    let consumer_name_clone = consumer_name1.clone();
    let (backend, _context) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()))
            .add_consumer_group(
                consumer_group_clone.clone(),
                RedisConsumerGroupSpec::new([stream_clone.clone()], consumer_group_clone.clone())
                    .consumer_name(consumer_name_clone.clone()),
            )
            .add_event_single::<TestEvent>(stream_clone.clone());
    })
    .expect("Redis backend setup successful");

    // Setup reader1 app with MATCHING consumer name
    let mut reader1 = App::new();
    reader1.add_plugins(EventBusPlugins(backend.clone()));
    reader1.insert_resource(EventCollector::default());

    let s1 = stream.clone();
    let g1 = consumer_group.clone();
    let c1 = consumer_name1.clone();
    reader1.add_systems(
        Update,
        move |mut r: RedisEventReader<TestEvent>, mut c: ResMut<EventCollector>| {
            let config =
                RedisConsumerConfig::new(g1.clone(), [s1.clone()]).set_consumer_name(c1.clone()); // Use SAME name as topology
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        },
    );

    // Setup writer app
    let mut writer = App::new();
    writer.add_plugins(EventBusPlugins(backend));

    let stream_for_writer = stream.clone();
    writer.add_systems(
        Update,
        move |mut w: RedisEventWriter, mut sent: Local<usize>| {
            if *sent < 3 {
                let config = RedisProducerConfig::new(stream_for_writer.clone());
                w.write(
                    &config,
                    TestEvent {
                        message: format!("coordinated_{}", *sent),
                        value: *sent as i32,
                    },
                );
                *sent += 1;
            }
        },
    );

    // Start reader first to establish consumer group
    run_app_updates(&mut reader1, 3);

    // Wait for consumer group to be established
    std::thread::sleep(std::time::Duration::from_millis(100));

    // Send events
    run_app_updates(&mut writer, 4);

    // Wait for message processing
    let (received1, _) = update_until(&mut reader1, 5000, |app| {
        let collected = app.world().resource::<EventCollector>();
        collected.0.len() >= 3
    });

    let collected1 = reader1.world().resource::<EventCollector>();

    assert!(
        received1,
        "Reader should receive events with coordinated consumer names"
    );
    assert_eq!(collected1.0.len(), 3, "Reader should receive all 3 events");
}
