use crate::common::events::TestEvent;
use crate::common::helpers::unique_topic;
use crate::common::setup::setup;
use bevy::prelude::*;
use bevy_event_bus::{EventBusPlugins, EventBusReader};

// Test that repeatedly reading an empty topic does not hang or block frames.
#[test]
fn idle_empty_topic_poll_does_not_block() {
    let (backend, _) = setup();
    let topic = unique_topic("idle"); // preconfigure but never send
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(
        backend,
        bevy_event_bus::PreconfiguredTopics::new([topic.clone()]),
    ));
    #[derive(Resource, Default)]
    struct Ticks(u32);
    app.insert_resource(Ticks::default());
    app.add_event::<TestEvent>(); // ensure event type registered though we won't send
    let topic_read = topic.clone();
    app.add_systems(
        Update,
        move |mut r: EventBusReader<TestEvent>, mut ticks: ResMut<Ticks>| {
            // Try reading every frame; should be instant (reader fallback/drained path fast)
            for _ in r.read(&topic_read) { /* none expected */ }
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
