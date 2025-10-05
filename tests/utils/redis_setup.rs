#![cfg(feature = "redis")]

use anyhow::{Context, Result, anyhow};
use bevy_event_bus::backends::RedisEventBusBackend;
use bevy_event_bus::config::redis::{
    RedisBackendConfig, RedisConnectionConfig, RedisTopologyBuilder, RedisTopologyConfig,
};
use once_cell::sync::OnceCell;
use std::collections::VecDeque;
use std::iter::FromIterator;
use std::process::Command;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

const REDIS_IMAGE: &str = "redis:5.0";
const CONTAINER_NAME: &str = "bevy_event_bus_test_redis";
const REDIS_PORT: u16 = 6379;
const SHARED_DATABASES: usize = 64;
const WAIT_RETRIES: usize = 50;
const WAIT_BASE_DELAY_MS: u64 = 25;
const WAIT_MAX_DELAY_MS: u64 = 400;
const DATABASE_VALIDATION_ATTEMPTS: usize = 5;
const MAX_CONTAINER_SETUP_ATTEMPTS: usize = 3;

#[derive(Clone, Debug)]
struct ContainerInfo {
    id: Option<String>,
    base_endpoint: String,
    owned: bool,
    keep_container: bool,
}

/// Holds lifecycle information for a Redis backend created during testing.
pub struct RedisTestContext {
    connection_string: String,
    _shared: Option<SharedRedisDatabase>,
}

impl RedisTestContext {
    fn new_external(connection_string: String) -> Self {
        Self {
            connection_string,
            _shared: None,
        }
    }

    fn new_shared(database: SharedRedisDatabase) -> Self {
        let connection_string = database.connection_string().to_string();
        Self {
            connection_string,
            _shared: Some(database),
        }
    }

    /// Return the Redis connection string (including database index) associated with this lease.
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }
}

/// Lease-like handle to a Redis database index within the shared container.
#[derive(Clone)]
pub struct SharedRedisDatabase {
    inner: Arc<SharedRedisDatabaseInner>,
}

struct SharedRedisDatabaseInner {
    connection_string: String,
    lease: SharedLease,
}

enum SharedLease {
    External,
    Managed {
        redis: Arc<SharedRedisInner>,
        db_index: usize,
    },
}

impl SharedRedisDatabase {
    fn managed(redis: Arc<SharedRedisInner>, db_index: usize, connection_string: String) -> Self {
        Self {
            inner: Arc::new(SharedRedisDatabaseInner {
                connection_string,
                lease: SharedLease::Managed { redis, db_index },
            }),
        }
    }

    fn external(connection_string: String) -> Self {
        Self {
            inner: Arc::new(SharedRedisDatabaseInner {
                connection_string,
                lease: SharedLease::External,
            }),
        }
    }

    pub fn connection_string(&self) -> &str {
        &self.inner.connection_string
    }
}

impl Drop for SharedRedisDatabaseInner {
    fn drop(&mut self) {
        if let SharedLease::Managed { redis, db_index } = &self.lease {
            redis.release_db(*db_index);
        }
    }
}

static SHARED_REDIS: OnceCell<Arc<SharedRedisInner>> = OnceCell::new();

fn shared_redis() -> Result<Arc<SharedRedisInner>> {
    SHARED_REDIS
        .get_or_try_init(|| SharedRedisInner::new().map(Arc::new))
        .map(Arc::clone)
}

struct SharedRedisInner {
    container: ContainerInfo,
    available_dbs: Mutex<VecDeque<usize>>,
    available_dbs_cv: Condvar,
    shutdown_called: AtomicBool,
}

impl SharedRedisInner {
    fn new() -> Result<Self> {
        if std::env::var("BEB_REDIS_URL").is_ok() {
            return Err(anyhow!(
                "BEB_REDIS_URL is set; use external connection helpers instead of shared Redis"
            ));
        }

        if !docker_available() {
            return Err(anyhow!(
                "Redis integration tests require docker or the BEB_REDIS_URL environment variable"
            ));
        }

        for attempt in 1..=MAX_CONTAINER_SETUP_ATTEMPTS {
            let container = ensure_container()?;
            wait_for_redis(&container.base_endpoint)?;

            match verify_database_support(&container.base_endpoint) {
                Ok(()) => {
                    return Ok(Self {
                        available_dbs: Mutex::new(VecDeque::from_iter(0..SHARED_DATABASES)),
                        available_dbs_cv: Condvar::new(),
                        container,
                        shutdown_called: AtomicBool::new(false),
                    });
                }
                Err(err) => {
                    if let Some(id) = &container.id {
                        let _ = Command::new("docker").args(["stop", id]).status();
                    }

                    if attempt == MAX_CONTAINER_SETUP_ATTEMPTS {
                        return Err(err);
                    }
                }
            }
        }

        Err(anyhow!(
            "failed to provision Redis container with required database support"
        ))
    }

    fn base_endpoint(&self) -> &str {
        &self.container.base_endpoint
    }

    fn acquire_db(self: &Arc<Self>) -> Result<SharedRedisDatabase> {
        let mut guard = self.available_dbs.lock().unwrap();
        loop {
            if let Some(db_index) = guard.pop_front() {
                drop(guard);
                self.flush_db(db_index)?;
                let connection_string = format!("{}/{}", self.base_endpoint(), db_index);
                return Ok(SharedRedisDatabase::managed(
                    Arc::clone(self),
                    db_index,
                    connection_string,
                ));
            }
            guard = self.available_dbs_cv.wait(guard).unwrap();
        }
    }

    fn release_db(&self, db_index: usize) {
        let _ = self.flush_db(db_index);
        let mut guard = self.available_dbs.lock().unwrap();
        guard.push_back(db_index);
        self.available_dbs_cv.notify_one();
    }

    fn flush_db(&self, db_index: usize) -> Result<()> {
        let connection_string = format!("{}/{}", self.base_endpoint(), db_index);
        let client = redis::Client::open(connection_string)?;
        let mut connection = client.get_connection()?;
        let _: () = redis::cmd("FLUSHDB").query(&mut connection)?;
        Ok(())
    }

    fn shutdown(&self) {
        if self.shutdown_called.swap(true, Ordering::SeqCst) {
            return;
        }

        if !self.container.owned || self.container.keep_container {
            return;
        }

        if let Some(id) = &self.container.id {
            let _ = Command::new("docker").args(["stop", id]).status();
        }
    }
}

fn docker_available() -> bool {
    Command::new("docker")
        .arg("version")
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false)
}

fn ensure_container() -> Result<ContainerInfo> {
    let keep_container = std::env::var("BEVY_EVENT_BUS_KEEP_REDIS")
        .ok()
        .map(|flag| flag == "1" || flag.eq_ignore_ascii_case("true"))
        .unwrap_or(false);

    if let Some(existing) = find_existing_container()? {
        return Ok(ContainerInfo {
            id: Some(existing),
            base_endpoint: format!("redis://127.0.0.1:{}", REDIS_PORT),
            owned: false,
            keep_container,
        });
    }

    pull_image();
    let id = start_container().context("failed to start Redis test container")?;

    Ok(ContainerInfo {
        id: Some(id),
        base_endpoint: format!("redis://127.0.0.1:{}", REDIS_PORT),
        owned: true,
        keep_container,
    })
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
            &SHARED_DATABASES.to_string(),
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
                        tracing::debug!(?err, "Redis PING failed while waiting for readiness");
                    }
                },
                Err(err) => {
                    tracing::debug!(?err, "Redis connection acquisition failed during wait");
                }
            },
            Err(err) => {
                tracing::debug!(?err, "Redis client creation failed");
            }
        }

        delay = (delay * 2).min(WAIT_MAX_DELAY_MS);
        thread::sleep(Duration::from_millis(delay + attempt as u64));
    }

    Err(anyhow!("Timed out waiting for Redis at {endpoint}"))
}

fn verify_database_support(endpoint: &str) -> Result<()> {
    let mut last_error: Option<anyhow::Error> = None;
    let mut delay = WAIT_BASE_DELAY_MS;

    for attempt in 1..=DATABASE_VALIDATION_ATTEMPTS {
        match verify_database_support_once(endpoint) {
            Ok(()) => return Ok(()),
            Err(err) => {
                last_error = Some(err);
                if attempt == DATABASE_VALIDATION_ATTEMPTS {
                    break;
                }

                thread::sleep(Duration::from_millis(delay));
                delay = (delay * 2).min(WAIT_MAX_DELAY_MS);
            }
        }
    }

    Err(last_error.unwrap_or_else(|| {
        anyhow!(
            "failed to verify Redis database support after {DATABASE_VALIDATION_ATTEMPTS} attempts"
        )
    }))
}

fn verify_database_support_once(endpoint: &str) -> Result<()> {
    let client = redis::Client::open(endpoint)
        .with_context(|| format!("failed to create Redis client for {endpoint}"))?;
    let mut connection = client
        .get_connection()
        .context("failed to acquire Redis connection while validating database support")?;

    let values: Vec<String> = redis::cmd("CONFIG")
        .arg("GET")
        .arg("databases")
        .query(&mut connection)
        .context("failed to query Redis for configured database count")?;

    let configured = values
        .get(1)
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(16);

    if configured < SHARED_DATABASES {
        return Err(anyhow!(
            "Redis instance exposes {configured} databases but {SHARED_DATABASES} are required"
        ));
    }

    let last_index = (SHARED_DATABASES - 1) as isize;
    let _: () = redis::cmd("SELECT")
        .arg(last_index)
        .query(&mut connection)
        .context("failed to select highest Redis database for validation")?;
    let _: () = redis::cmd("SELECT")
        .arg(0)
        .query(&mut connection)
        .context("failed to reset Redis database after validation")?;

    Ok(())
}

fn acquire_shared_database() -> Result<SharedRedisDatabase> {
    let shared = shared_redis()?;
    shared.acquire_db()
}

/// Ensure that a Redis instance is available and return a database handle that can be shared
/// across multiple backends in a single test.
pub fn ensure_shared_redis() -> Result<SharedRedisDatabase> {
    if let Ok(url) = std::env::var("BEB_REDIS_URL") {
        return Ok(SharedRedisDatabase::external(url));
    }

    acquire_shared_database()
}

/// Construct a Redis backend and accompanying context from the supplied topology builder.
pub fn setup(builder: RedisTopologyBuilder) -> Result<(RedisEventBusBackend, RedisTestContext)> {
    bevy_event_bus::runtime();

    let (connection, context) = ensure_connection()?;
    let topology = builder.build();
    ensure_topology_provisioned(connection.connection_string(), &topology)?;
    let config = RedisBackendConfig::new(connection, topology, Duration::from_millis(250));

    Ok((RedisEventBusBackend::new(config), context))
}

fn ensure_connection() -> Result<(RedisConnectionConfig, RedisTestContext)> {
    if let Ok(url) = std::env::var("BEB_REDIS_URL") {
        let connection = RedisConnectionConfig::new(url.clone());
        let context = RedisTestContext::new_external(url);
        return Ok((connection, context));
    }

    let database = acquire_shared_database()?;
    let connection = RedisConnectionConfig::new(database.connection_string().to_string());
    let context = RedisTestContext::new_shared(database);
    Ok((connection, context))
}

/// Construct a Redis backend bound to the specified shared database.
pub fn setup_backend_with_shared_redis<F>(
    shared_database: &SharedRedisDatabase,
    configure_topology: F,
) -> Result<(RedisEventBusBackend, RedisTestContext)>
where
    F: FnOnce(&mut RedisTopologyBuilder),
{
    bevy_event_bus::runtime();

    let mut builder = RedisTopologyBuilder::default();
    configure_topology(&mut builder);

    let connection_string = shared_database.connection_string().to_string();
    let connection = RedisConnectionConfig::new(connection_string.clone());
    let topology = builder.build();
    ensure_topology_provisioned(&connection_string, &topology)?;
    let config = RedisBackendConfig::new(connection, topology, Duration::from_millis(250));
    let context = RedisTestContext::new_shared(shared_database.clone());

    Ok((RedisEventBusBackend::new(config), context))
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
                Err(err) if err.code() == Some("BUSYGROUP") => {}
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

#[ctor::dtor]
fn teardown_redis_container() {
    if let Some(shared) = SHARED_REDIS.get() {
        shared.shutdown();
    }
}
