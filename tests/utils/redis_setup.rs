#![cfg(feature = "redis")]

use anyhow::{Context, Result, anyhow};
use bevy::prelude::App;
use bevy_event_bus::EventBusPlugins;
use bevy_event_bus::backends::RedisEventBusBackend;
use bevy_event_bus::config::redis::{
    RedisBackendConfig, RedisConnectionConfig, RedisTopologyBuilder, RedisTopologyConfig,
};
use once_cell::sync::Lazy;
use redis::RedisError;
use std::collections::HashMap;
use std::process::Command;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::thread;
use std::time::Duration;
use tracing::{debug, info, info_span};

const REDIS_IMAGE: &str = "redis:5.0";
const CONTAINER_NAME: &str = "bevy_event_bus_test_redis";
const REDIS_PORT: u16 = 6379;
const DEFAULT_DB_INDEX: usize = 0;
const WAIT_RETRIES: usize = 50;
const WAIT_BASE_DELAY_MS: u64 = 25;
const WAIT_MAX_DELAY_MS: u64 = 400;
const MAX_SHARED_DATABASES: usize = 512;

#[derive(Clone, Debug)]
struct ContainerInfo {
    id: Option<String>,
    endpoint: String,
    owned: bool,
    keep_container: bool,
}

#[derive(Default, Debug, Clone)]
struct ContainerState {
    info: Option<ContainerInfo>,
}

static CONTAINER_STATE: Lazy<Mutex<ContainerState>> =
    Lazy::new(|| Mutex::new(ContainerState::default()));
static CONTAINER_SHUTDOWN: Lazy<AtomicBool> = Lazy::new(|| AtomicBool::new(false));
static NEXT_SHARED_DB_INDEX: Lazy<AtomicUsize> =
    Lazy::new(|| AtomicUsize::new(DEFAULT_DB_INDEX + 1));

/// Runtime context returned alongside a configured Redis backend.
#[derive(Clone, Debug)]
pub struct RedisTestContext {
    connection_string: String,
}

impl RedisTestContext {
    fn new(connection_string: impl Into<String>) -> Self {
        Self {
            connection_string: connection_string.into(),
        }
    }

    /// Return the Redis connection string associated with this test backend.
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }
}

/// Additional configuration controls for Redis test backend construction.
#[derive(Clone, Debug)]
pub struct SetupOptions {
    read_block_timeout: Duration,
    connection_overrides: HashMap<String, String>,
    pool_size: Option<usize>,
}

impl SetupOptions {
    /// Create a new options struct with sensible defaults.
    /// By default the blocking read timeout is 250 milliseconds and no extra
    /// connection overrides are applied.
    pub fn new() -> Self {
        Self {
            read_block_timeout: Duration::from_millis(250),
            connection_overrides: HashMap::new(),
            pool_size: None,
        }
    }

    /// Override the blocking read timeout used by the backend.
    pub fn read_block_timeout(mut self, timeout: Duration) -> Self {
        self.read_block_timeout = timeout;
        self
    }

    /// Override the Redis connection pool size.
    pub fn pool_size(mut self, pool_size: usize) -> Self {
        self.pool_size = Some(pool_size);
        self
    }

    /// Append an additional key/value pair to the Redis connection configuration.
    pub fn insert_connection_config(
        mut self,
        key: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        self.connection_overrides.insert(key.into(), value.into());
        self
    }
}

impl Default for SetupOptions {
    fn default() -> Self {
        Self::new()
    }
}

pub struct SetupRequest {
    builder: RedisTopologyBuilder,
    options: SetupOptions,
    connection_override: Option<String>,
}

impl From<RedisTopologyBuilder> for SetupRequest {
    fn from(builder: RedisTopologyBuilder) -> Self {
        Self {
            builder,
            options: SetupOptions::default(),
            connection_override: None,
        }
    }
}

impl From<(RedisTopologyBuilder, SetupOptions)> for SetupRequest {
    fn from((builder, options): (RedisTopologyBuilder, SetupOptions)) -> Self {
        Self {
            builder,
            options,
            connection_override: None,
        }
    }
}

impl SetupRequest {
    pub fn with_connection(mut self, connection_string: impl Into<String>) -> Self {
        self.connection_override = Some(connection_string.into());
        self
    }

    fn into_parts(self) -> (RedisTopologyBuilder, SetupOptions, Option<String>) {
        (self.builder, self.options, self.connection_override)
    }
}

/// Build a baseline Bevy `App` wired to a Redis backend created via [`setup`].
/// Tests can provide a customization closure to insert additional resources or systems.
pub fn build_basic_app<F>(customize: F) -> App
where
    F: FnOnce(&mut App),
{
    let (backend, _) = prepare_backend(|_| {}).expect("Redis backend setup");
    let mut app = App::new();
    app.add_plugins(EventBusPlugins(backend));
    customize(&mut app);
    app
}

/// Convenience helper when no additional customization is required.
pub fn build_basic_app_simple() -> App {
    build_basic_app(|_| {})
}

/// Helper to obtain a [`RedisTopologyBuilder`] by applying a closure, allowing tests to keep
/// concise configuration blocks while still passing a fully-constructed builder into [`setup`].
pub fn build_topology<F>(configure: F) -> RedisTopologyBuilder
where
    F: FnOnce(&mut RedisTopologyBuilder),
{
    let mut builder = RedisTopologyBuilder::default();
    configure(&mut builder);
    builder
}

/// Create a [`SetupRequest`] from a configuration closure and [`SetupOptions`].
pub fn build_request<F>(options: SetupOptions, configure: F) -> SetupRequest
where
    F: FnOnce(&mut RedisTopologyBuilder),
{
    SetupRequest::from((build_topology(configure), options))
}

/// Construct a Redis backend for integration tests using a prepared topology builder.
/// Tests may provide either a builder directly or pair it with [`SetupOptions`] for
/// additional connection-level tweaks.
pub fn setup<S>(input: S) -> Result<(RedisEventBusBackend, RedisTestContext)>
where
    S: Into<SetupRequest>,
{
    let (builder, options, connection_override) = input.into().into_parts();

    bevy_event_bus::runtime();

    let connection_string = match connection_override {
        Some(connection) => connection,
        None => default_connection_string()?,
    };
    build_backend_from_parts(connection_string, builder, options)
}

fn build_backend_from_parts(
    connection_string: String,
    builder: RedisTopologyBuilder,
    options: SetupOptions,
) -> Result<(RedisEventBusBackend, RedisTestContext)> {
    let SetupOptions {
        read_block_timeout,
        connection_overrides,
        pool_size,
    } = options;

    let mut connection = RedisConnectionConfig::new(connection_string.clone());
    if let Some(pool_size) = pool_size {
        connection = connection.set_pool_size(pool_size);
    }
    for (key, value) in connection_overrides {
        connection = connection.insert_additional_config(key, value);
    }

    let topology = builder.build();
    ensure_topology_provisioned(&connection_string, &topology)?;

    let config = RedisBackendConfig::new(connection, topology, read_block_timeout);
    let backend = RedisEventBusBackend::new(config);
    let context = RedisTestContext::new(connection_string);
    Ok((backend, context))
}

/// Convenience helper to construct a Redis backend directly from a topology
/// configuration closure without needing to manually call [`build_topology`].
pub fn prepare_backend<F>(configure: F) -> Result<(RedisEventBusBackend, RedisTestContext)>
where
    F: FnOnce(&mut RedisTopologyBuilder),
{
    setup(build_topology(configure))
}

fn default_connection_string() -> Result<String> {
    default_connection_string_for_index(DEFAULT_DB_INDEX)
}

fn default_connection_string_for_index(index: usize) -> Result<String> {
    if let Ok(url) = std::env::var("BEB_REDIS_URL") {
        return Ok(url);
    }

    let endpoint = ensure_container_endpoint()?;
    Ok(format!("{endpoint}/{index}"))
}

fn ensure_container_endpoint() -> Result<String> {
    let mut state = CONTAINER_STATE.lock().unwrap();
    if let Some(info) = &state.info {
        return Ok(info.endpoint.clone());
    }

    if !docker_available() {
        return Err(anyhow!(
            "Redis integration tests require docker or the BEB_REDIS_URL environment variable"
        ));
    }

    let span = info_span!("redis_container.ensure");
    let _g = span.enter();

    let info = start_or_reuse_container()?;
    wait_for_redis(&info.endpoint)?;
    state.info = Some(info.clone());
    Ok(info.endpoint)
}

fn start_or_reuse_container() -> Result<ContainerInfo> {
    let keep_container = std::env::var("BEVY_EVENT_BUS_KEEP_REDIS")
        .ok()
        .map(|flag| flag == "1" || flag.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if let Some(existing) = find_existing_container()? {
        return Ok(ContainerInfo {
            id: Some(existing),
            endpoint: format!("redis://127.0.0.1:{}", REDIS_PORT),
            owned: false,
            keep_container,
        });
    }

    pull_image();
    let id = start_container().context("failed to start Redis test container")?;

    Ok(ContainerInfo {
        id: Some(id),
        endpoint: format!("redis://127.0.0.1:{}", REDIS_PORT),
        owned: true,
        keep_container,
    })
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn find_existing_container() -> Result<Option<String>> {
    let output = Command::new("docker")
        .args([
            "ps",
            "--filter",
            &format!("name={}", CONTAINER_NAME),
            "--format",
            "{{.ID}}",
        ])
        .output()
        .context("failed to query docker for existing Redis containers")?;

    if !output.status.success() {
        return Ok(None);
    }

    let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if id.is_empty() {
        Ok(None)
    } else {
        Ok(Some(id))
    }
}

fn pull_image() {
    let _ = Command::new("docker").args(["pull", REDIS_IMAGE]).status();
}

fn start_container() -> Result<String> {
    let output = Command::new("docker")
        .args([
            "run",
            "-d",
            "--rm",
            "--name",
            CONTAINER_NAME,
            "-p",
            &format!("{}:{}", REDIS_PORT, REDIS_PORT),
            REDIS_IMAGE,
            "redis-server",
            "--save",
            "",
            "--appendonly",
            "no",
            "--databases",
            &MAX_SHARED_DATABASES.to_string(),
        ])
        .output()
        .context("failed to launch Redis docker container")?;

    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("docker run redis container failed: {stderr}"));
    }

    let id = String::from_utf8_lossy(&output.stdout).trim().to_string();
    if id.is_empty() {
        Err(anyhow!("docker run returned empty container id"))
    } else {
        Ok(id)
    }
}

fn wait_for_redis(endpoint: &str) -> Result<()> {
    let mut delay = WAIT_BASE_DELAY_MS;
    for attempt in 0..WAIT_RETRIES {
        match redis::Client::open(endpoint) {
            Ok(client) => match client.get_connection() {
                Ok(mut connection) => match redis::cmd("PING").query::<String>(&mut connection) {
                    Ok(_) => return Ok(()),
                    Err(err) => {
                        debug!(?err, "Redis PING failed while waiting for readiness");
                    }
                },
                Err(err) => {
                    debug!(?err, "Redis connection acquisition failed during wait");
                }
            },
            Err(err) => {
                debug!(?err, "Redis client creation failed");
            }
        }

        delay = (delay * 2).min(WAIT_MAX_DELAY_MS);
        thread::sleep(Duration::from_millis(delay + attempt as u64));
    }

    Err(anyhow!("Timed out waiting for Redis at {endpoint}"))
}

fn ensure_topology_provisioned(
    connection_string: &str,
    topology: &RedisTopologyConfig,
) -> Result<()> {
    if topology.consumer_groups().is_empty() {
        return Ok(());
    }

    let client = redis::Client::open(connection_string)
        .with_context(|| format!("failed to create Redis client for {connection_string}"))?;
    let mut connection = client
        .get_connection()
        .context("failed to acquire Redis connection while provisioning topology")?;

    for spec in topology.consumer_groups().values() {
        for stream in &spec.streams {
            let mut cmd = redis::cmd("XGROUP");
            cmd.arg("CREATE")
                .arg(stream)
                .arg(&spec.consumer_group)
                .arg(&spec.start_id)
                .arg("MKSTREAM");

            match cmd.query::<redis::Value>(&mut connection) {
                Ok(_) => {}
                Err(err) if is_busy_group(&err) => {}
                Err(err) => {
                    return Err(anyhow!(
                        "failed to create consumer group '{}' for stream '{}': {}",
                        spec.consumer_group,
                        stream,
                        err
                    ));
                }
            }
        }
    }

    Ok(())
}

fn is_busy_group(err: &RedisError) -> bool {
    err.code() == Some("BUSYGROUP")
}

#[ctor::dtor]
fn teardown_redis_container() {
    if CONTAINER_SHUTDOWN.swap(true, Ordering::SeqCst) {
        return;
    }

    if std::env::var("BEB_REDIS_URL").is_ok() {
        return;
    }

    if !docker_available() {
        return;
    }

    let info = {
        let mut state = CONTAINER_STATE.lock().unwrap();
        state.info.clone()
    };

    if let Some(info) = info {
        if !info.owned || info.keep_container {
            return;
        }

        if let Some(id) = info.id {
            let span = info_span!("redis_container.teardown", container_id = %id);
            let _g = span.enter();
            let _ = Command::new("docker").args(["stop", &id]).status();
            info!("Redis test container stopped");
        }
    }
}

#[derive(Clone, Debug)]
pub struct SharedRedisDatabase {
    connection_string: String,
}

impl SharedRedisDatabase {
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }
}

pub fn ensure_shared_redis() -> Result<SharedRedisDatabase> {
    let index = NEXT_SHARED_DB_INDEX.fetch_add(1, Ordering::SeqCst);
    if index >= MAX_SHARED_DATABASES {
        return Err(anyhow!(
            "Exceeded maximum number of Redis databases reserved for tests ({MAX_SHARED_DATABASES})"
        ));
    }

    let connection_string = default_connection_string_for_index(index)?;
    Ok(SharedRedisDatabase { connection_string })
}

impl SharedRedisDatabase {
    pub fn prepare_backend<F>(
        &self,
        configure_topology: F,
    ) -> Result<(RedisEventBusBackend, RedisTestContext)>
    where
        F: FnOnce(&mut RedisTopologyBuilder),
    {
        let request = SetupRequest::from(build_topology(configure_topology))
            .with_connection(self.connection_string.clone());
        setup(request)
    }
}
