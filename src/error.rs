use thiserror::Error;

/// Errors that can occur in the event bus
#[derive(Error, Debug)]
pub enum EventBusError {
    #[error("Serialization error: {0}")]
    Serialization(String),
    
    #[error("Connection error: {0}")]
    Connection(String),
    
    #[error("Backend not configured: {0}")]
    NotConfigured(String),
    
    #[error("Topic error: {0}")]
    Topic(String),
    
    #[error("Timeout error")]
    Timeout,
    
    #[error("Other error: {0}")]
    Other(String),
}

impl From<serde_json::Error> for EventBusError {
    fn from(error: serde_json::Error) -> Self {
        EventBusError::Serialization(error.to_string())
    }
}
