use crate::common::events::TestEvent;
use crate::common::helpers::{unique_topic, unique_consumer_group, update_until};
use crate::common::setup::{setup, build_app};
use bevy::prelude::*;
use bevy_event_bus::{
    EventBusReader, EventBusWriter, EventBusAppExt,
    EventBusConsumerConfig, KafkaReadConfig, KafkaWriteConfig
};

#[test]
fn frame_limit_spreads_drain() {
    let (backend_w, _b1) = setup(None);
    let topic = unique_topic("limit");
    let mut writer = crate::common::setup::build_app(backend_w, None, |app| {
        app.add_bus_event::<TestEvent>(&topic);
    });
    
    // Create reader app with pre-created consumer group
    let consumer_group = unique_consumer_group("test_group");
    let config = KafkaReadConfig::new(&consumer_group).topics([&topic]);
    
    #[derive(Resource, Default)]
    struct Collected(Vec<TestEvent>);
    
    let (backend_reader, _bootstrap_reader) = setup(None);
    let mut reader = build_app(backend_reader, Some(&[config.clone()]), |app| {
        app.add_bus_event::<TestEvent>(&topic);
        
        app.insert_resource(EventBusConsumerConfig {
            max_events_per_frame: Some(5),
            max_drain_millis: None,
        });
        app.insert_resource(Collected::default());
        
        // Use the same config that was passed to setup
        app.add_systems(Update, move |mut r: EventBusReader<TestEvent>, mut c: ResMut<Collected>| {
            for wrapper in r.read(&config) {
                c.0.push(wrapper.event().clone());
            }
        });
    });
    
    let tclone = topic.clone();
    writer.add_systems(Update, move |mut w: EventBusWriter<TestEvent>| {
        for i in 0..15 {
            let _ = w.write(
                &KafkaWriteConfig::new(&tclone),
                TestEvent {
                    message: format!("v{i}"),
                    value: i,
                },
            );
        }
    });
    writer.update();

    // Spin until all 15 collected or timeout
    let (ok, _frames) = update_until(&mut reader, 5000, |app| {
        let c = app.world().resource::<Collected>();
        c.0.len() >= 15
    });
    assert!(ok, "Timed out waiting for all events under frame limit");
    let collected = reader.world().resource::<Collected>();
    assert_eq!(collected.0.len(), 15);
}
