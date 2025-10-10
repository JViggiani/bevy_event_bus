#![cfg(feature = "redis")]

use async_trait::async_trait;
use crossbeam_channel::{Receiver, SendTimeoutError, Sender, TryRecvError, TrySendError, bounded};
use redis::aio::ConnectionManager;
use redis::streams::{StreamId, StreamKey, StreamReadReply};
use redis::{ErrorKind as RedisErrorKind, RedisError, Value as RedisValue, from_redis_value};
use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use crate::config::TopologyMode;
use bevy::prelude::*;
use bevy_event_bus::{
    backends::event_bus_backend::{
        BackendConfigError, BackendPluginSetup, EventBusBackend, ManualCommitDescriptor,
        ManualCommitHandle, ManualCommitStyle, ReceiveOptions, SendOptions, StreamTrimStrategy,
    },
    config::redis::{
        RedisBackendConfig, RedisConnectionConfig, RedisConsumerGroupSpec, RedisRuntimeTuning,
        RedisTopologyConfig, TrimStrategy,
    },
    resources::{
        ConsumerMetrics, IncomingMessage, RedisAckQueue, RedisAckRequest,
        backend_metadata::RedisMetadata,
    },
    runtime,
};

#[derive(Debug, Clone, Default)]
struct AckCounters {
    acknowledged: Arc<AtomicUsize>,
    failed: Arc<AtomicUsize>,
}

impl AckCounters {
    fn record_success(&self, count: usize) {
        if count > 0 {
            self.acknowledged.fetch_add(count, Ordering::Relaxed);
        }
    }

    fn record_failure(&self, count: usize) {
        if count > 0 {
            self.failed.fetch_add(count, Ordering::Relaxed);
        }
    }
}

#[derive(Resource, Debug, Clone, Default)]
pub struct RedisAckWorkerStats {
    counters: AckCounters,
}

impl RedisAckWorkerStats {
    pub fn acknowledged(&self) -> usize {
        self.counters.acknowledged.load(Ordering::Relaxed)
    }

    pub fn failed(&self) -> usize {
        self.counters.failed.load(Ordering::Relaxed)
    }
}

struct RedisManualAckHandle {
    sender: Sender<RedisAckRequest>,
    counters: AckCounters,
}

impl RedisManualAckHandle {
    fn new(sender: Sender<RedisAckRequest>, counters: AckCounters) -> Self {
        Self { sender, counters }
    }
}

impl ManualCommitHandle for RedisManualAckHandle {
    fn register_resources(&self, world: &mut World) {
        world.insert_resource(RedisAckQueue(self.sender.clone()));
        world.insert_resource(RedisAckWorkerStats {
            counters: self.counters.clone(),
        });
    }

    fn descriptor(&self) -> ManualCommitDescriptor {
        ManualCommitDescriptor {
            backend: "redis",
            style: ManualCommitStyle::StreamAck,
        }
    }
}

#[derive(Clone)]
struct StreamWriteConfig {
    maxlen: Option<usize>,
    trim_strategy: TrimStrategy,
}

struct RedisTrimRequest {
    stream: String,
    maxlen: usize,
    strategy: TrimStrategy,
}

/// Shared runtime state for the Redis backend so cloned backends operate over the same queues and tasks.
struct RedisBackendState {
    config: RedisBackendConfig,
    runtime: RedisRuntimeTuning,
    client: redis::Client,
    writer: Mutex<Option<Arc<tokio::sync::Mutex<ConnectionManager>>>>,
    incoming_tx: Sender<IncomingMessage>,
    incoming_rx: Mutex<Option<Receiver<IncomingMessage>>>,
    pending_messages: Mutex<VecDeque<IncomingMessage>>,
    ack_tx: Option<Sender<RedisAckRequest>>,
    ack_rx: Mutex<Option<Receiver<RedisAckRequest>>>,
    trim_tx: Sender<RedisTrimRequest>,
    trim_rx: Mutex<Option<Receiver<RedisTrimRequest>>>,
    ack_counters: AckCounters,
    handles: Mutex<Vec<JoinHandle<()>>>,
    running: Arc<AtomicBool>,
    dropped: Arc<AtomicUsize>,
    stream_writes: HashMap<String, StreamWriteConfig>,
}

impl RedisBackendState {
    fn take_receiver(&self) -> Option<Receiver<IncomingMessage>> {
        self.incoming_rx.lock().unwrap().take()
    }

    fn ack_sender(&self) -> Option<Sender<RedisAckRequest>> {
        self.ack_tx.clone()
    }

    fn take_ack_receiver(&self) -> Option<Receiver<RedisAckRequest>> {
        self.ack_rx.lock().unwrap().take()
    }

    fn trim_sender(&self) -> Sender<RedisTrimRequest> {
        self.trim_tx.clone()
    }

    fn take_trim_receiver(&self) -> Option<Receiver<RedisTrimRequest>> {
        self.trim_rx.lock().unwrap().take()
    }

    fn writer(&self) -> Option<Arc<tokio::sync::Mutex<ConnectionManager>>> {
        self.writer.lock().unwrap().clone()
    }

    fn set_writer(&self, manager: ConnectionManager) {
        self.writer
            .lock()
            .unwrap()
            .replace(Arc::new(tokio::sync::Mutex::new(manager)));
    }

    fn clear_writer(&self) {
        self.writer.lock().unwrap().take();
    }

    fn push_handle(&self, handle: JoinHandle<()>) {
        self.handles.lock().unwrap().push(handle);
    }

    fn abort_tasks(&self) {
        let mut handles = self.handles.lock().unwrap();
        for handle in handles.drain(..) {
            handle.abort();
        }
    }

    fn dropped_count(&self) -> usize {
        self.dropped.load(Ordering::Relaxed)
    }
}

#[derive(Clone)]
pub struct RedisEventBusBackend {
    state: Arc<RedisBackendState>,
}

impl Debug for RedisEventBusBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RedisEventBusBackend")
            .field(
                "endpoint",
                &self.state.config.connection.connection_string(),
            )
            .field(
                "streams",
                &self
                    .state
                    .config
                    .topology
                    .stream_names()
                    .into_iter()
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

fn build_client(connection: &RedisConnectionConfig) -> Result<redis::Client, RedisError> {
    redis::Client::open(connection.connection_string())
}

fn build_stream_write_map(topology: &RedisTopologyConfig) -> HashMap<String, StreamWriteConfig> {
    let mut map = HashMap::new();
    for spec in topology.streams() {
        map.insert(
            spec.name.clone(),
            StreamWriteConfig {
                maxlen: spec.maxlen,
                trim_strategy: TrimStrategy::Approximate,
            },
        );
    }
    map
}

fn redis_topology_error(message: impl Into<String>) -> RedisError {
    RedisError::from((
        RedisErrorKind::ExtensionError,
        "bevy_event_bus::redis_topology",
        message.into(),
    ))
}

async fn stream_exists(manager: &mut ConnectionManager, stream: &str) -> Result<bool, RedisError> {
    let exists: i64 = redis::cmd("EXISTS")
        .arg(stream)
        .query_async(manager)
        .await?;
    Ok(exists > 0)
}

async fn ensure_consumer_group_present(
    manager: &mut ConnectionManager,
    stream: &str,
    spec: &RedisConsumerGroupSpec,
) -> Result<(), RedisError> {
    let response = redis::cmd("XINFO")
        .arg("GROUPS")
        .arg(stream)
        .query_async::<_, RedisValue>(manager)
        .await?;

    let groups = match response {
        RedisValue::Bulk(groups) => groups,
        _ => {
            return Err(redis_topology_error(format!(
                "Unexpected response while inspecting groups for Redis stream '{}'",
                stream
            )));
        }
    };

    for group in groups {
        if let RedisValue::Bulk(entries) = group {
            let mut iter = entries.iter();
            while let Some(key) = iter.next() {
                if redis_value_to_string(key).as_deref() == Some("name") {
                    if let Some(value) = iter.next() {
                        if redis_value_to_string(value).as_deref()
                            == Some(spec.consumer_group.as_str())
                        {
                            return Ok(());
                        }
                    }
                }
            }
        }
    }

    Err(redis_topology_error(format!(
        "Redis consumer group '{}' is missing on stream '{}' while configured with TopologyMode::Validate",
        spec.consumer_group, stream
    )))
}

async fn ensure_redis_topology(
    manager: &mut ConnectionManager,
    topology: &RedisTopologyConfig,
) -> Result<(), RedisError> {
    let mut known_streams = HashSet::new();

    for spec in topology.streams() {
        known_streams.insert(spec.name.clone());

        if matches!(spec.mode, TopologyMode::Validate)
            && !stream_exists(manager, &spec.name).await?
        {
            return Err(redis_topology_error(format!(
                "Redis stream '{}' is missing while configured with TopologyMode::Validate",
                spec.name
            )));
        }
    }

    for spec in topology.consumer_groups().values() {
        for stream in &spec.streams {
            if !known_streams.contains(stream) {
                match spec.mode {
                    TopologyMode::Provision => {
                        known_streams.insert(stream.clone());
                    }
                    TopologyMode::Validate => {
                        if !stream_exists(manager, stream).await? {
                            return Err(redis_topology_error(format!(
                                "Redis stream '{}' is missing while validating consumer group '{}'",
                                stream, spec.consumer_group
                            )));
                        }
                        known_streams.insert(stream.clone());
                    }
                }
            }

            match spec.mode {
                TopologyMode::Provision => {
                    provision_consumer_group(manager, stream, spec).await?;
                }
                TopologyMode::Validate => {
                    ensure_consumer_group_present(manager, stream, spec).await?;
                }
            }
        }
    }

    Ok(())
}

async fn provision_consumer_group(
    manager: &mut ConnectionManager,
    stream: &str,
    spec: &RedisConsumerGroupSpec,
) -> Result<(), RedisError> {
    let mut cmd = redis::cmd("XGROUP");
    cmd.arg("CREATE")
        .arg(stream)
        .arg(&spec.consumer_group)
        .arg(&spec.start_id)
        .arg("MKSTREAM");

    match cmd.query_async::<_, RedisValue>(manager).await {
        Ok(_) => {
            debug!(stream = %stream, group = %spec.consumer_group, "Created Redis consumer group");
            Ok(())
        }
        Err(err) if err.code() == Some("BUSYGROUP") => {
            debug!(
                stream = %stream,
                group = %spec.consumer_group,
                "Redis consumer group already exists"
            );
            Ok(())
        }
        Err(err) => Err(err),
    }
}

fn redis_value_to_bytes(value: &RedisValue) -> Option<Vec<u8>> {
    match value {
        RedisValue::Data(bytes) => Some(bytes.clone()),
        RedisValue::Bulk(items) => {
            let mut buffer = Vec::new();
            for item in items {
                buffer.extend(redis_value_to_bytes(item)?);
            }
            Some(buffer)
        }
        RedisValue::Int(num) => Some(num.to_string().into_bytes()),
        RedisValue::Nil => None,
        _ => from_redis_value::<Vec<u8>>(value).ok(),
    }
}

fn redis_value_to_string(value: &RedisValue) -> Option<String> {
    match value {
        RedisValue::Data(bytes) => String::from_utf8(bytes.clone()).ok(),
        RedisValue::Int(num) => Some(num.to_string()),
        RedisValue::Nil => None,
        _ => from_redis_value::<String>(value).ok(),
    }
}

fn convert_stream_entries(
    reply: StreamReadReply,
    consumer_group: Option<&str>,
    manual_ack: bool,
) -> Vec<IncomingMessage> {
    let mut messages = Vec::new();

    for StreamKey { key, ids } in reply.keys {
        for StreamId { id, map } in ids {
            if let Some(payload_value) = map.get("payload") {
                if let Some(payload) = redis_value_to_bytes(payload_value) {
                    let mut key_field = None;

                    for (field, value) in &map {
                        if field == "payload" {
                            continue;
                        } else if field == "event_key" {
                            key_field = redis_value_to_string(value);
                        }
                    }

                    let metadata = RedisMetadata {
                        stream: key.clone(),
                        entry_id: id.clone(),
                        consumer_group: consumer_group.map(|g| g.to_string()),
                        manual_ack,
                    };

                    messages.push(IncomingMessage::plain(
                        key.clone(),
                        payload,
                        key_field,
                        Some(Box::new(metadata)),
                    ));
                }
            }
        }
    }

    messages
}

impl RedisEventBusBackend {
    pub fn new(config: RedisBackendConfig) -> Result<Self, BackendConfigError> {
        let client = build_client(&config.connection)
            .map_err(|err| BackendConfigError::new("redis", err.to_string()))?;
        runtime::block_on(async {
            let mut manager = ConnectionManager::new(client.clone())
                .await
                .map_err(|err| BackendConfigError::new("redis", err.to_string()))?;
            ensure_redis_topology(&mut manager, &config.topology)
                .await
                .map_err(|err| BackendConfigError::new("redis", err.to_string()))
        })?;
        let manual_ack = config
            .topology
            .consumer_groups()
            .values()
            .any(|spec| spec.manual_ack);
        let stream_writes = build_stream_write_map(&config.topology);

        let runtime = config.runtime.clone();

        let (incoming_tx, incoming_rx) = bounded(runtime.message_channel_capacity);
        let (ack_tx, ack_rx) = if manual_ack {
            let (tx, rx) = bounded(runtime.ack_channel_capacity);
            (Some(tx), Some(rx))
        } else {
            (None, None)
        };
        let (trim_tx, trim_rx) = bounded(runtime.trim_channel_capacity);

        let state = RedisBackendState {
            config,
            runtime,
            client,
            writer: Mutex::new(None),
            incoming_tx,
            incoming_rx: Mutex::new(Some(incoming_rx)),
            pending_messages: Mutex::new(VecDeque::new()),
            ack_tx,
            ack_rx: Mutex::new(ack_rx),
            trim_tx,
            trim_rx: Mutex::new(Some(trim_rx)),
            ack_counters: AckCounters::default(),
            handles: Mutex::new(Vec::new()),
            running: Arc::new(AtomicBool::new(false)),
            dropped: Arc::new(AtomicUsize::new(0)),
            stream_writes,
        };

        Ok(Self {
            state: Arc::new(state),
        })
    }

    fn spawn_runtime_tasks(&self) {
        self.spawn_group_readers();
        self.spawn_plain_stream_readers();
        self.spawn_ack_worker();
        self.spawn_trim_worker();
    }

    fn spawn_group_readers(&self) {
        let topology = self.state.config.topology.clone();
        let read_block = self.state.config.read_block_timeout;
        let runtime = self.state.runtime.clone();
        for spec in topology.consumer_groups().values().cloned() {
            self.spawn_group_reader(spec, read_block, &runtime);
        }
    }

    fn spawn_group_reader(
        &self,
        spec: RedisConsumerGroupSpec,
        read_block: Duration,
        runtime: &RedisRuntimeTuning,
    ) {
        let client = self.state.client.clone();
        let incoming = self.state.incoming_tx.clone();
        let dropped = self.state.dropped.clone();
        let running = self.state.running.clone();
        let manual_ack = spec.manual_ack;
        let consumer_group = spec.consumer_group.clone();
        let consumer_name = spec.consumer_name.clone();
        let batch_size = runtime.read_batch_size;
        let empty_backoff = runtime.empty_read_backoff;

        let handle = runtime::runtime().spawn(async move {
            let mut manager = match ConnectionManager::new(client).await {
                Ok(m) => m,
                Err(err) => {
                    error!(error = %err, group = %consumer_group, "Failed to build Redis connection manager");
                    return;
                }
            };

            let block_ms = read_block.as_millis() as usize;

            while running.load(Ordering::SeqCst) {
                let mut cmd = redis::cmd("XREADGROUP");
                cmd.arg("GROUP")
                    .arg(&consumer_group)
                    .arg(&consumer_name)
                    .arg("COUNT")
                    .arg(batch_size)
                    .arg("BLOCK")
                    .arg(block_ms);
                if !manual_ack {
                    cmd.arg("NOACK");
                }
                cmd.arg("STREAMS");
                for stream in &spec.streams {
                    cmd.arg(stream);
                }
                for _ in &spec.streams {
                    cmd.arg(">");
                }

                match cmd.query_async::<_, RedisValue>(&mut manager).await {
                    Ok(RedisValue::Nil) => {
                        tokio::time::sleep(empty_backoff).await;
                    }
                    Ok(value) => match from_redis_value::<StreamReadReply>(&value) {
                        Ok(reply) => {
                            let messages =
                                convert_stream_entries(reply, Some(&consumer_group), manual_ack);
                            if messages.is_empty() {
                                continue;
                            }

                            for message in messages {
                                RedisEventBusBackend::dispatch_message(
                                    &incoming,
                                    message,
                                    &running,
                                    &dropped,
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                error = %err,
                                group = %consumer_group,
                                "Failed to decode Redis stream reply"
                            );
                        }
                    },
                    Err(err) => {
                        warn!(
                            error = %err,
                            group = %consumer_group,
                            "Redis XREADGROUP failed"
                        );
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        self.state.push_handle(handle);
    }

    fn spawn_plain_stream_readers(&self) {
        let topology = self.state.config.topology.clone();
        let read_block = self.state.config.read_block_timeout;
        let runtime = self.state.runtime.clone();

        let grouped: HashSet<String> = topology
            .consumer_groups()
            .values()
            .flat_map(|spec| spec.streams.iter().cloned())
            .collect();

        for stream in topology.stream_names() {
            if !grouped.contains(&stream) {
                self.spawn_plain_stream_reader(stream, read_block, &runtime);
            }
        }
    }

    fn spawn_plain_stream_reader(
        &self,
        stream: String,
        read_block: Duration,
        runtime: &RedisRuntimeTuning,
    ) {
        let client = self.state.client.clone();
        let incoming = self.state.incoming_tx.clone();
        let dropped = self.state.dropped.clone();
        let running = self.state.running.clone();
        let batch_size = runtime.read_batch_size;
        let empty_backoff = runtime.empty_read_backoff;

        let handle = runtime::runtime().spawn(async move {
            let mut manager = match ConnectionManager::new(client).await {
                Ok(m) => m,
                Err(err) => {
                    error!(error = %err, stream = %stream, "Failed to build Redis reader connection");
                    return;
                }
            };

            let block_ms = read_block.as_millis() as usize;
            let mut last_id = "$".to_string();

            while running.load(Ordering::SeqCst) {
                let mut cmd = redis::cmd("XREAD");
                cmd.arg("COUNT")
                    .arg(batch_size)
                    .arg("BLOCK")
                    .arg(block_ms)
                    .arg("STREAMS")
                    .arg(&stream)
                    .arg(&last_id);

                match cmd.query_async::<_, RedisValue>(&mut manager).await {
                    Ok(RedisValue::Nil) => {
                        tokio::time::sleep(empty_backoff).await;
                    }
                    Ok(value) => match from_redis_value::<StreamReadReply>(&value) {
                        Ok(reply) => {
                            let mut messages =
                                convert_stream_entries(reply, None, false);

                            if let Some(last) = messages.last() {
                                if let Some(meta) = last
                                    .backend_metadata
                                    .as_ref()
                                    .and_then(|meta| meta.as_any().downcast_ref::<RedisMetadata>())
                                {
                                    last_id = meta.entry_id.clone();
                                }
                            }

                            for message in messages.drain(..) {
                                RedisEventBusBackend::dispatch_message(
                                    &incoming,
                                    message,
                                    &running,
                                    &dropped,
                                );
                            }
                        }
                        Err(err) => {
                            warn!(
                                error = %err,
                                stream = %stream,
                                "Failed to decode Redis stream reply"
                            );
                        }
                    },
                    Err(err) => {
                        warn!(error = %err, stream = %stream, "Redis XREAD failed");
                        tokio::time::sleep(Duration::from_millis(50)).await;
                    }
                }
            }
        });

        self.state.push_handle(handle);
    }

    fn spawn_ack_worker(&self) {
        let Some(receiver) = self.state.take_ack_receiver() else {
            return;
        };

        let client = self.state.client.clone();
        let running = self.state.running.clone();
        let counters = self.state.ack_counters.clone();

        let runtime = self.state.runtime.clone();
        let empty_backoff = runtime.empty_read_backoff;
        let ack_batch_size = runtime.ack_batch_size.max(1);
        let ack_flush_interval = runtime.ack_flush_interval;

        let handle = runtime::runtime().spawn(async move {
            let mut manager = match ConnectionManager::new(client).await {
                Ok(m) => m,
                Err(err) => {
                    error!(error = %err, "Failed to create Redis ack connection");
                    return;
                }
            };

            let mut pending = Vec::with_capacity(ack_batch_size);
            let mut last_enqueue = Instant::now();

            loop {
                let mut saw_disconnect = false;

                loop {
                    match receiver.try_recv() {
                        Ok(request) => {
                            pending.push(request);
                            last_enqueue = Instant::now();

                            if pending.len() >= ack_batch_size {
                                RedisEventBusBackend::flush_ack_requests(
                                    &mut manager,
                                    &counters,
                                    &mut pending,
                                )
                                .await;
                                last_enqueue = Instant::now();
                            }
                        }
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            saw_disconnect = true;
                            break;
                        }
                    }
                }

                let running_flag = running.load(Ordering::SeqCst);
                let flush_due = !pending.is_empty()
                    && (saw_disconnect
                        || !running_flag
                        || last_enqueue.elapsed() >= ack_flush_interval);

                if flush_due {
                    RedisEventBusBackend::flush_ack_requests(&mut manager, &counters, &mut pending)
                        .await;
                    last_enqueue = Instant::now();
                }

                if saw_disconnect && pending.is_empty() {
                    break;
                }

                if !running_flag && receiver.is_empty() && pending.is_empty() {
                    break;
                }

                if pending.is_empty() {
                    tokio::time::sleep(empty_backoff).await;
                } else {
                    tokio::task::yield_now().await;
                }
            }
        });

        self.state.push_handle(handle);
    }

    fn spawn_trim_worker(&self) {
        let Some(receiver) = self.state.take_trim_receiver() else {
            return;
        };

        let client = self.state.client.clone();
        let running = self.state.running.clone();
        let runtime = self.state.runtime.clone();
        let idle_backoff = runtime.empty_read_backoff;

        let handle = runtime::runtime().spawn(async move {
            let mut manager = match ConnectionManager::new(client).await {
                Ok(m) => m,
                Err(err) => {
                    error!(error = %err, "Failed to create Redis trim connection");
                    return;
                }
            };

            loop {
                let mut drained = Vec::new();
                let mut saw_disconnect = false;

                loop {
                    match receiver.try_recv() {
                        Ok(request) => drained.push(request),
                        Err(TryRecvError::Empty) => break,
                        Err(TryRecvError::Disconnected) => {
                            saw_disconnect = true;
                            break;
                        }
                    }
                }

                for request in drained.drain(..) {
                    let mut cmd = redis::cmd("XTRIM");
                    cmd.arg(&request.stream).arg("MAXLEN");
                    if matches!(request.strategy, TrimStrategy::Approximate) {
                        cmd.arg("~");
                    }
                    cmd.arg(request.maxlen as usize);

                    if let Err(err) = cmd.query_async::<_, RedisValue>(&mut manager).await {
                        warn!(
                            stream = %request.stream,
                            maxlen = request.maxlen,
                            error = %err,
                            "Redis XTRIM request failed"
                        );
                    } else {
                        debug!(
                            stream = %request.stream,
                            maxlen = request.maxlen,
                            "Redis XTRIM request applied"
                        );
                    }
                }

                let running_flag = running.load(Ordering::SeqCst);
                if saw_disconnect && receiver.is_empty() {
                    break;
                }

                if !running_flag && receiver.is_empty() {
                    break;
                }

                if receiver.is_empty() {
                    tokio::time::sleep(idle_backoff).await;
                } else {
                    tokio::task::yield_now().await;
                }
            }
        });

        self.state.push_handle(handle);
    }

    async fn flush_ack_requests(
        manager: &mut ConnectionManager,
        counters: &AckCounters,
        pending: &mut Vec<RedisAckRequest>,
    ) {
        if pending.is_empty() {
            return;
        }

        let mut grouped: HashMap<(String, String), Vec<String>> = HashMap::new();
        for request in pending.drain(..) {
            grouped
                .entry((request.stream, request.consumer_group))
                .or_insert_with(Vec::new)
                .push(request.entry_id);
        }

        for ((stream, group), entry_ids) in grouped {
            let mut cmd = redis::cmd("XACK");
            cmd.arg(&stream).arg(&group);
            for entry_id in &entry_ids {
                cmd.arg(entry_id);
            }

            match cmd.query_async::<_, i64>(manager).await {
                Ok(acked) => {
                    let acked_count = acked.max(0) as usize;
                    if acked_count >= entry_ids.len() {
                        counters.record_success(entry_ids.len());
                    } else {
                        counters.record_success(acked_count);
                        counters.record_failure(entry_ids.len() - acked_count);
                        warn!(
                            stream = %stream,
                            group = %group,
                            expected = entry_ids.len(),
                            acknowledged = acked_count,
                            "Redis XACK acknowledged fewer messages than requested"
                        );
                    }
                }
                Err(err) => {
                    counters.record_failure(entry_ids.len());
                    warn!(
                        stream = %stream,
                        group = %group,
                        count = entry_ids.len(),
                        error = %err,
                        "Redis XACK batch failed"
                    );
                }
            }
        }
    }

    /// Enqueue a trim request for the supplied stream. Execution occurs in the trim worker
    /// to keep writers free from direct network calls.
    pub fn trim_stream(
        &self,
        stream: &str,
        maxlen: usize,
        strategy: TrimStrategy,
    ) -> Result<(), String> {
        if self.state.writer().is_none() {
            return Err("Redis writer connection not available".to_string());
        }

        let sender = self.state.trim_sender();
        let request = RedisTrimRequest {
            stream: stream.to_string(),
            maxlen,
            strategy,
        };

        match sender.try_send(request) {
            Ok(_) => Ok(()),
            Err(TrySendError::Full(request)) => {
                match sender.send_timeout(request, Duration::from_millis(50)) {
                    Ok(()) => Ok(()),
                    Err(SendTimeoutError::Timeout(_)) => {
                        Err("Redis trim queue is full; try again later".to_string())
                    }
                    Err(SendTimeoutError::Disconnected(_)) => {
                        Err("Redis trim queue is unavailable".to_string())
                    }
                }
            }
            Err(TrySendError::Disconnected(_)) => {
                Err("Redis trim queue is unavailable".to_string())
            }
        }
    }

    fn drain_matching_messages(&self, stream: &str, group: Option<&str>) -> Vec<Vec<u8>> {
        let mut matches = Vec::new();
        let mut deferred = VecDeque::new();

        {
            let mut pending = self.state.pending_messages.lock().unwrap();
            while let Some(msg) = pending.pop_front() {
                if Self::message_matches(&msg, stream, group) {
                    matches.push(msg.payload.clone());
                } else {
                    deferred.push_back(msg);
                }
            }
        }

        let receiver_opt = {
            let mut guard = self.state.incoming_rx.lock().unwrap();
            guard.take()
        };

        if let Some(receiver) = receiver_opt {
            loop {
                match receiver.try_recv() {
                    Ok(msg) => {
                        if Self::message_matches(&msg, stream, group) {
                            matches.push(msg.payload.clone());
                        } else {
                            deferred.push_back(msg);
                        }
                    }
                    Err(TryRecvError::Empty) => break,
                    Err(TryRecvError::Disconnected) => break,
                }
            }

            let mut guard = self.state.incoming_rx.lock().unwrap();
            *guard = Some(receiver);
        }

        if !deferred.is_empty() {
            let mut pending = self.state.pending_messages.lock().unwrap();
            while let Some(msg) = deferred.pop_front() {
                pending.push_back(msg);
            }
        }

        matches
    }

    fn message_matches(message: &IncomingMessage, stream: &str, group: Option<&str>) -> bool {
        if message.source != stream {
            return false;
        }

        match group {
            Some(group_id) => message
                .backend_metadata
                .as_ref()
                .and_then(|meta| meta.as_any().downcast_ref::<RedisMetadata>())
                .and_then(|redis_meta| redis_meta.consumer_group.as_deref())
                .map(|g| g == group_id)
                .unwrap_or(false),
            None => true,
        }
    }

    fn stream_write_cfg(&self, stream: &str) -> Option<StreamWriteConfig> {
        self.state.stream_writes.get(stream).cloned()
    }

    fn dispatch_message(
        tx: &Sender<IncomingMessage>,
        mut message: IncomingMessage,
        running: &Arc<AtomicBool>,
        dropped: &Arc<AtomicUsize>,
    ) {
        loop {
            match tx.try_send(message) {
                Ok(_) => break,
                Err(TrySendError::Full(returned)) => {
                    if !running.load(Ordering::Relaxed) {
                        dropped.fetch_add(1, Ordering::Relaxed);
                        break;
                    }

                    match tokio::task::block_in_place(|| {
                        tx.send_timeout(returned, Duration::from_millis(50))
                    }) {
                        Ok(()) => break,
                        Err(SendTimeoutError::Timeout(returned_again)) => {
                            if !running.load(Ordering::Relaxed) {
                                dropped.fetch_add(1, Ordering::Relaxed);
                                break;
                            }
                            message = returned_again;
                        }
                        Err(SendTimeoutError::Disconnected(_)) => {
                            dropped.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
                Err(TrySendError::Disconnected(_)) => {
                    dropped.fetch_add(1, Ordering::Relaxed);
                    break;
                }
            }
        }
    }
}

#[async_trait]
impl EventBusBackend for RedisEventBusBackend {
    fn clone_box(&self) -> Box<dyn EventBusBackend> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn backend_name(&self) -> &'static str {
        "redis"
    }

    fn configure_plugin(&self, _app: &mut App) {}

    fn setup_plugin(&self, _world: &mut World) -> BackendPluginSetup {
        let mut setup = BackendPluginSetup::default();
        setup.ready_topics = self
            .state
            .config
            .topology
            .stream_names()
            .into_iter()
            .collect();
        setup.message_stream = self.state.take_receiver();
        if let Some(sender) = self.state.ack_sender() {
            setup.manual_commit = Some(Box::new(RedisManualAckHandle::new(
                sender,
                self.state.ack_counters.clone(),
            )));
        }
        setup.redis_topology = Some(self.state.config.topology.clone());
        setup
    }

    fn augment_metrics(&self, metrics: &mut ConsumerMetrics) {
        metrics.dropped_messages = self.state.dropped_count();
    }

    fn apply_event_bindings(&self, app: &mut App) {
        for binding in self.state.config.topology.event_bindings() {
            binding.apply(app);
        }
    }

    async fn connect(&mut self) -> bool {
        if self
            .state
            .running
            .compare_exchange(false, true, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return true;
        }

        let mut manager = match ConnectionManager::new(self.state.client.clone()).await {
            Ok(m) => m,
            Err(err) => {
                error!(error = %err, "Failed to build Redis connection manager");
                self.state.running.store(false, Ordering::SeqCst);
                return false;
            }
        };

        if let Err(err) = ensure_redis_topology(&mut manager, &self.state.config.topology).await {
            error!(error = %err, "Redis topology preparation failed");
            self.state.running.store(false, Ordering::SeqCst);
            return false;
        }

        self.state.set_writer(manager);
        self.spawn_runtime_tasks();
        true
    }

    async fn disconnect(&mut self) -> bool {
        if !self.state.running.swap(false, Ordering::SeqCst) {
            return true;
        }

        self.state.abort_tasks();
        self.state.clear_writer();
        true
    }

    fn try_send_serialized(
        &self,
        event_json: &[u8],
        stream: &str,
        options: SendOptions<'_>,
    ) -> bool {
        let manager = match self.state.writer() {
            Some(manager) => manager,
            None => return false,
        };

        let payload = event_json.to_vec();
        let stream_name = stream.to_string();
        let key = options.partition_key.map(|k| k.to_string());
        let override_cfg = options
            .stream_trim
            .map(|(maxlen, strategy)| StreamWriteConfig {
                maxlen: Some(maxlen),
                trim_strategy: match strategy {
                    StreamTrimStrategy::Exact => TrimStrategy::Exact,
                    StreamTrimStrategy::Approximate => TrimStrategy::Approximate,
                },
            });
        let write_cfg = override_cfg.or_else(|| self.stream_write_cfg(stream));

        runtime::runtime().spawn(async move {
            let mut guard = manager.lock().await;
            let mut cmd = redis::cmd("XADD");
            cmd.arg(&stream_name);
            if let Some(cfg) = write_cfg {
                if let Some(maxlen) = cfg.maxlen {
                    cmd.arg("MAXLEN");
                    if cfg.trim_strategy == TrimStrategy::Approximate {
                        cmd.arg("~");
                    }
                    cmd.arg(maxlen as usize);
                }
            }
            cmd.arg("*");
            cmd.arg("payload").arg(&payload);
            if let Some(k) = key.as_deref() {
                cmd.arg("event_key").arg(k);
            }

            if let Err(err) = cmd.query_async::<_, RedisValue>(&mut *guard).await {
                warn!(stream = %stream_name, error = %err, "Failed to enqueue Redis message");
            }
        });

        true
    }

    async fn receive_serialized(&self, stream: &str, options: ReceiveOptions<'_>) -> Vec<Vec<u8>> {
        // Validate that the consumer group is defined in the topology if specified
        if let Some(consumer_group) = options.consumer_group {
            let topology_groups = self.state.config.topology.consumer_groups();
            let is_topology_defined = topology_groups
                .values()
                .any(|spec| spec.consumer_group == consumer_group);

            if !is_topology_defined {
                error!(
                    consumer_group = %consumer_group,
                    stream = %stream,
                    "BLOCKED: Attempted to read from consumer group not defined in topology - this is not allowed"
                );
                // Return empty result for non-topology groups - they should not receive any messages
                return Vec::new();
            }
        }

        self.drain_matching_messages(stream, options.consumer_group)
    }

    async fn flush(&self) -> Result<(), String> {
        if let Some(writer) = self.state.writer() {
            let _guard = writer.lock().await;
        }
        Ok(())
    }
}
