use bevy_event_bus::backends::event_bus_backend::{ReceiveOptions, SendOptions};
use bevy_event_bus::config::kafka::{KafkaConsumerGroupSpec, KafkaInitialOffset, KafkaTopicSpec};
use bevy_event_bus::{EventBusBackend, KafkaEventBusBackend};
use integration_tests::utils::events::TestEvent;
use integration_tests::utils::helpers::{
    unique_consumer_group, unique_topic, wait_for_consumer_group_ready, wait_for_messages_in_group,
};
use integration_tests::utils::kafka_setup::{self, SetupOptions};
use serde_json::to_string;
use tokio::task;

#[tokio::test]
async fn test_multi_topic_isolation() {
    let topic_a = unique_topic("kafka_multi_topic_a");
    let topic_b = unique_topic("kafka_multi_topic_b");
    let consumer_group = unique_consumer_group("kafka_multi_topic_group");

    let topic_a_for_config = topic_a.clone();
    let topic_b_for_config = topic_b.clone();
    let group_for_config = consumer_group.clone();

    let request = kafka_setup::build_request(SetupOptions::earliest(), move |builder| {
        builder
            .add_topic(
                KafkaTopicSpec::new(topic_a_for_config.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_topic(
                KafkaTopicSpec::new(topic_b_for_config.clone())
                    .partitions(1)
                    .replication(1),
            )
            .add_consumer_group(
                group_for_config.clone(),
                KafkaConsumerGroupSpec::new([
                    topic_a_for_config.clone(),
                    topic_b_for_config.clone(),
                ])
                .initial_offset(KafkaInitialOffset::Earliest),
            );
    });
    let (mut backend, _bootstrap) =
        task::spawn_blocking(move || kafka_setup::prepare_backend(request))
            .await
            .expect("Kafka backend setup task panicked");

    assert!(backend.connect().await, "Failed to connect Kafka backend");

    let ready_a = wait_for_consumer_group_ready(&backend, &topic_a, &consumer_group, 10_000).await;
    let ready_b = wait_for_consumer_group_ready(&backend, &topic_b, &consumer_group, 10_000).await;
    assert!(ready_a, "Consumer group not ready for topic A");
    assert!(ready_b, "Consumer group not ready for topic B");

    let event_a = TestEvent {
        message: "A1".into(),
        value: 1,
    };
    let event_b = TestEvent {
        message: "B1".into(),
        value: 2,
    };

    let serialized_a = to_string(&event_a).expect("Serialization should succeed for topic A");
    let serialized_b = to_string(&event_b).expect("Serialization should succeed for topic B");

    assert!(
        backend.try_send_serialized(serialized_a.as_bytes(), &topic_a, SendOptions::default()),
        "Failed to send to topic A",
    );
    assert!(
        backend.try_send_serialized(serialized_b.as_bytes(), &topic_b, SendOptions::default()),
        "Failed to send to topic B",
    );

    <KafkaEventBusBackend as EventBusBackend>::flush(&backend)
        .await
        .expect("Flush should succeed for multi-topic test");

    let received_topic_a =
        wait_for_messages_in_group(&backend, &topic_a, &consumer_group, 1, 10_000).await;
    let received_topic_b =
        wait_for_messages_in_group(&backend, &topic_b, &consumer_group, 1, 10_000).await;

    assert!(
        !received_topic_a.is_empty(),
        "Should have received messages from topic A",
    );
    assert!(
        !received_topic_b.is_empty(),
        "Should have received messages from topic B",
    );

    let found_topic_a_message = received_topic_a
        .iter()
        .any(|message| String::from_utf8_lossy(message).contains("A1"));
    let found_topic_b_message = received_topic_b
        .iter()
        .any(|message| String::from_utf8_lossy(message).contains("B1"));

    assert!(
        found_topic_a_message,
        "Should have received message from topic A"
    );
    assert!(
        found_topic_b_message,
        "Should have received message from topic B"
    );

    let cross_topic_messages_a = backend
        .receive_serialized(
            &topic_a,
            ReceiveOptions::new().consumer_group(&consumer_group),
        )
        .await;
    assert!(
        cross_topic_messages_a
            .iter()
            .all(|message| String::from_utf8_lossy(message).contains("A")),
        "Consumer group should only observe topic A payloads when reading topic A",
    );

    let cross_topic_messages_b = backend
        .receive_serialized(
            &topic_b,
            ReceiveOptions::new().consumer_group(&consumer_group),
        )
        .await;
    assert!(
        cross_topic_messages_b
            .iter()
            .all(|message| String::from_utf8_lossy(message).contains("B")),
        "Consumer group should only observe topic B payloads when reading topic B",
    );

    assert!(
        backend.disconnect().await,
        "Kafka backend disconnect should succeed"
    );
}
