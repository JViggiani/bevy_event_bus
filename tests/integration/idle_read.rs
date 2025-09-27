use crate::common::events::TestEvent;
use crate::common::helpers::{unique_topic, unique_consumer_group};
use crate::common::setup::build_app;
use bevy::prelude::*;
use bevy_event_bus::{EventBusReader, KafkaReadConfig};

// Test that repeatedly reading an empty topic does not hang or block frames.
#[test]
fn idle_empty_topic_poll_does_not_block() {
    let topic = unique_topic("idle"); // preconfigure but never send
    
    // Create consumer configuration
    let consumer_group = unique_consumer_group("test_group");
    let config = KafkaReadConfig::new(&consumer_group).topics([&topic]);
    
    // Build app with consumer group created during setup
    let (backend, _bootstrap) = crate::common::setup::setup(None);
    let mut app = build_app(backend, Some(&[config.clone()]), |app| {
        app.add_event::<TestEvent>(); // ensure event type registered though we won't send
    });
    
    #[derive(Resource, Default)]
    struct Ticks(u32);
    app.insert_resource(Ticks::default());
    
    // Simple system that just reads - no consumer group creation needed
    app.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut ticks: ResMut<Ticks>| {
            // Try reading every frame; should be instant (reader fallback/drained path fast)
            for _ in r.read(&config) { /* none expected */ }
            ticks.0 += 1;
        },
    );
    // Run many frames; total wall time should stay small if non-blocking
    let start = std::time::Instant::now();
    for _ in 0..200 {
        app.update();
    }
    let elapsed_ms = start.elapsed().as_millis();
    let ticks = app.world().resource::<Ticks>().0;
    assert_eq!(ticks, 200, "All frames should execute (ticks={ticks})");
    // Heuristic: 200 empty frames should complete quickly (<1500ms even on CI)
    assert!(
        elapsed_ms < 1500,
        "Idle polling took too long: {elapsed_ms}ms"
    );
}
