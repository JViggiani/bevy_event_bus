use bevy::prelude::Message;

#[derive(Message, Debug, Clone)]
pub struct BackendReadyMessage {
    pub backend: String,
    pub topics: Vec<String>,
}

#[derive(Message, Debug, Clone)]
pub struct BackendDownMessage {
    pub backend: String,
    pub reason: String,
}

/// Emitted after each drain with snapshot metrics (optional for user systems).
#[derive(Message, Debug, Clone)]
pub struct DrainMetricsMessage {
    pub drained: usize,
    pub remaining: usize,
    pub total_drained: usize,
    pub dropped: usize,
    pub drain_duration_us: u128,
}
