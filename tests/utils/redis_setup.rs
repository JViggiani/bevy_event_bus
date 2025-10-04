#![cfg(feature = "redis")]

use std::thread::sleep;
use std::time::Duration;

use anyhow::{anyhow, Context, Result};
use bevy_event_bus::backends::RedisEventBusBackend;
use bevy_event_bus::config::redis::{
    RedisBackendConfig, RedisConnectionConfig, RedisTopologyBuilder,
};
use testcontainers::core::{IntoContainerPort, WaitFor};
use testcontainers::runners::SyncRunner;
use testcontainers::{Container, GenericImage};

const REDIS_PORT: u16 = 6379;
const REDIS_IMAGE_TAG: &str = "5.0";

/// Test context that keeps a Redis container (if any) alive for the duration of a test.
pub struct RedisTestContext {
    connection_string: String,
    container: Option<Container<GenericImage>>,
}

impl RedisTestContext {
    pub fn new_external(connection_string: String) -> Self {
        Self {
            connection_string,
            container: None,
        }
    }

    fn new_container(connection_string: String, container: Container<GenericImage>) -> Self {
        Self {
            connection_string,
            container: Some(container),
        }
    }

    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }
}

impl Drop for RedisTestContext {
    fn drop(&mut self) {
        // Drop container first to allow it to stop cleanly.
        if let Some(container) = self.container.take() {
            drop(container);
        }
    }
}

/// Create a Redis backend using the provided topology builder.
///
/// The connection is either provided via the `BEB_REDIS_URL` environment variable or backed by
/// a disposable Docker container started through `testcontainers`.
pub fn setup_with_builder(
    builder: RedisTopologyBuilder,
) -> Result<(RedisEventBusBackend, RedisTestContext)> {
    bevy_event_bus::runtime();

    let (connection, ctx) = ensure_connection()?;
    let topology = builder.build();
    let config = RedisBackendConfig::new(connection, topology, Duration::from_millis(250));

    let backend = RedisEventBusBackend::new(config);
    Ok((backend, ctx))
}

fn ensure_connection() -> Result<(RedisConnectionConfig, RedisTestContext)> {
    if let Ok(url) = std::env::var("BEB_REDIS_URL") {
        let connection = RedisConnectionConfig::new(url.clone());
        let context = RedisTestContext::new_external(url);
        return Ok((connection, context));
    }

    if !docker_available() {
        return Err(anyhow!("Redis tests require Docker or BEB_REDIS_URL to be set"));
    }

    let image = GenericImage::new("redis", REDIS_IMAGE_TAG)
        .with_exposed_port(REDIS_PORT.tcp())
        .with_wait_for(WaitFor::message_on_stdout(
            "Ready to accept connections",
        ));

    let container = image
        .start()
        .context("failed to start Redis test container")?;
    let port = container
        .get_host_port_ipv4(REDIS_PORT)
        .context("Unable to obtain mapped Redis port")?;
    let endpoint = format!("redis://127.0.0.1:{port}");

    wait_for_redis(&endpoint)?;

    let connection = RedisConnectionConfig::new(endpoint.clone());
    let context = RedisTestContext::new_container(endpoint, container);

    Ok((connection, context))
}

fn docker_available() -> bool {
    std::process::Command::new("docker")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn wait_for_redis(endpoint: &str) -> Result<()> {
    let max_attempts = 50;
    for attempt in 0..max_attempts {
        match redis::Client::open(endpoint) {
            Ok(client) => match client.get_connection() {
                Ok(_) => return Ok(()),
                Err(err) => {
                    tracing::debug!(?err, "Redis connection attempt failed");
                }
            },
            Err(err) => {
                tracing::debug!(?err, "Redis client creation failed");
            }
        }
        sleep(Duration::from_millis(100 * (attempt + 1) as u64));
    }
    Err(anyhow!("Timed out waiting for Redis at {endpoint}"))
}