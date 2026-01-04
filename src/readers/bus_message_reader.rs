use bevy::prelude::Message;
use bevy_event_bus::{config::EventBusConfig, resources::MessageWrapper, BusEvent};

/// Common capabilities shared by all bus message readers.
pub trait BusMessageReader<T: BusEvent + Message> {
    /// Drain the buffered messages for the supplied configuration.
    fn read<C: EventBusConfig>(&mut self, config: &C) -> Vec<MessageWrapper<T>> {
        self.read_bounded(config, usize::MAX)
    }

    /// Drain up to `max_messages`, leaving any remaining buffered messages for later reads.
    fn read_bounded<C: EventBusConfig>(
        &mut self,
        config: &C,
        max_messages: usize,
    ) -> Vec<MessageWrapper<T>>;
}
