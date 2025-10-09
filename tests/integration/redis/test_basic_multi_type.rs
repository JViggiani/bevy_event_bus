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

/// Test basic multi-type events using the proven working pattern
#[test]
fn test_basic_multi_type_redis() {
    let stream = unique_topic("basic-multi-type");
    let membership = unique_consumer_group_membership("basic_multi_group");
    let consumer_group = membership.group.clone();
    let consumer_name = membership.member.clone();

    let stream_clone = stream.clone();
    let consumer_group_clone = consumer_group.clone();
    let consumer_name_clone = consumer_name.clone();
    let (backend, _context) = redis_setup::prepare_backend(move |builder| {
        builder
            .add_stream(RedisStreamSpec::new(stream_clone.clone()))
            .add_consumer_group(
                consumer_group_clone.clone(),
                RedisConsumerGroupSpec::new(
                    [stream_clone.clone()],
                    consumer_group_clone.clone(),
                    consumer_name_clone.clone(),
                ),
            )
            .add_event_single::<TestEvent>(stream_clone.clone())
            .add_event_single::<UserLoginEvent>(stream_clone.clone());
    })
    .expect("Redis backend setup successful");

    let backend_reader = backend.clone();
    let backend_writer = backend;

    // Reader app
    let mut reader_app = App::new();
    reader_app.add_plugins(EventBusPlugins(backend_reader));

    #[derive(Resource, Default)]
    struct Collected {
        test_events: Vec<TestEvent>,
        login_events: Vec<UserLoginEvent>,
    }
    reader_app.insert_resource(Collected::default());

    #[derive(Resource, Clone)]
    struct Stream(String);
    #[derive(Resource, Clone)]
    struct ConsumerMembership {
        group: String,
        consumer: String,
    }
    reader_app.insert_resource(Stream(stream.clone()));
    reader_app.insert_resource(ConsumerMembership {
        group: consumer_group,
        consumer: consumer_name,
    });

    fn test_reader_system(
        mut reader: RedisEventReader<TestEvent>,
        stream: Res<Stream>,
        membership: Res<ConsumerMembership>,
        mut collected: ResMut<Collected>,
    ) {
        let config = RedisConsumerConfig::new(
            membership.group.clone(),
            membership.consumer.clone(),
            [stream.0.clone()],
        );
        for wrapper in reader.read(&config) {
            collected.test_events.push(wrapper.event().clone());
        }
    }

    fn login_reader_system(
        mut reader: RedisEventReader<UserLoginEvent>,
        stream: Res<Stream>,
        membership: Res<ConsumerMembership>,
        mut collected: ResMut<Collected>,
    ) {
        let config = RedisConsumerConfig::new(
            membership.group.clone(),
            membership.consumer.clone(),
            [stream.0.clone()],
        );
        for wrapper in reader.read(&config) {
            collected.login_events.push(wrapper.event().clone());
        }
    }

    reader_app.add_systems(Update, (test_reader_system, login_reader_system));

    // Writer app
    let mut writer_app = App::new();
    writer_app.add_plugins(EventBusPlugins(backend_writer));

    #[derive(Resource)]
    struct EventsToSend {
        test_event: TestEvent,
        login_event: UserLoginEvent,
        stream: String,
    }

    let events = EventsToSend {
        test_event: TestEvent {
            message: "Multi type test".into(),
            value: 42,
        },
        login_event: UserLoginEvent {
            user_id: "test_user".into(),
            timestamp: 12345,
        },
        stream: stream.clone(),
    };
    writer_app.insert_resource(events);

    fn writer_system(mut writer: RedisEventWriter, data: Res<EventsToSend>) {
        let config = RedisProducerConfig::new(data.stream.clone());
        writer.write(&config, data.test_event.clone());
        writer.write(&config, data.login_event.clone());
    }
    writer_app.add_systems(Update, writer_system);
    writer_app.update();

    // Poll until messages received or timeout
    let (received, _) = update_until(&mut reader_app, 8000, |app| {
        let collected = app.world().resource::<Collected>();
        !collected.test_events.is_empty() && !collected.login_events.is_empty()
    });

    assert!(
        received,
        "Expected to receive both event types within timeout"
    );

    let collected = reader_app.world().resource::<Collected>();
    assert_eq!(
        collected.test_events.len(),
        1,
        "Should have received 1 TestEvent"
    );
    assert_eq!(
        collected.login_events.len(),
        1,
        "Should have received 1 UserLoginEvent"
    );

    assert_eq!(collected.test_events[0].message, "Multi type test");
    assert_eq!(collected.test_events[0].value, 42);
    assert_eq!(collected.login_events[0].user_id, "test_user");
    assert_eq!(collected.login_events[0].timestamp, 12345);
}
