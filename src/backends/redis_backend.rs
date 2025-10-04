#![cfg(feature = "redis")]

use async_trait::async_trait;
use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded};
use redis::aio::ConnectionManager;
use redis::streams::{StreamId, StreamKey, StreamReadReply};
use redis::{RedisError, Value as RedisValue, from_redis_value};
use std::any::Any;
use std::collections::{HashMap, HashSet, VecDeque};
use std::fmt::Debug;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::task::JoinHandle;
use tracing::{debug, error, warn};

use bevy::prelude::*;
use bevy_event_bus::{
    backends::event_bus_backend::{
        BackendPluginSetup, EventBusBackend, ManualCommitDescriptor, ManualCommitHandle,
        ManualCommitStyle, ReceiveOptions, SendOptions, StreamTrimStrategy,
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

static CONSUMER_COUNTER: AtomicUsize = AtomicUsize::new(1);

#[derive(Debug, Clone, Default)]
struct AckCounters {
    acknowledged: Arc<AtomicUsize>,
    failed: Arc<AtomicUsize>,
}

impl AckCounters {
    fn mark_success(&self) {
        self.acknowledged.fetch_add(1, Ordering::Relaxed);
    }

    fn mark_failure(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
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

fn build_client(connection: &RedisConnectionConfig) -> redis::Client {
    redis::Client::open(connection.connection_string())
        .expect("Failed to create Redis client")
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

async fn ensure_streams(
    manager: &mut ConnectionManager,
    topology: &RedisTopologyConfig,
) -> Result<(), RedisError> {
    for spec in topology.consumer_groups().values() {
        for stream in &spec.streams {
            let mut cmd = redis::cmd("XGROUP");
            cmd.arg("CREATE")
                .arg(stream)
                .arg(&spec.consumer_group)
                .arg(&spec.start_id)
                .arg("MKSTREAM");
            match cmd.query_async::<_, RedisValue>(manager).await {
                Ok(_) => {
                    debug!(stream = %stream, group = %spec.consumer_group, "Created Redis consumer group")
                }
                Err(err) if err.code() == Some("BUSYGROUP") => {
                    debug!(
                        stream = %stream,
                        group = %spec.consumer_group,
                        "Redis consumer group already exists"
                    );
                }
                Err(err) => return Err(err),
            }
        }
    }
    Ok(())
}

fn next_consumer_name(prefix: &str) -> String {
    let id = CONSUMER_COUNTER.fetch_add(1, Ordering::Relaxed);
    format!("{prefix}-{id}")
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
                    let mut headers = HashMap::new();
                    let mut key_field = None;

                    for (field, value) in &map {
                        if field == "payload" {
                            continue;
                        } else if field == "event_key" {
                            key_field = redis_value_to_string(value);
                        } else if let Some(stripped) = field.strip_prefix("header:") {
                            if let Some(val) = redis_value_to_string(value) {
                                headers.insert(stripped.to_string(), val);
                            }
                        }
                    }

                    let metadata = RedisMetadata {
                        stream: key.clone(),
                        entry_id: id.clone(),
                        consumer_group: consumer_group.map(|g| g.to_string()),
                        manual_ack,
                    };

                    messages.push(IncomingMessage {
                        source: key.clone(),
                        payload,
                        headers,
                        key: key_field,
                        timestamp: Instant::now(),
                        backend_metadata: Some(Box::new(metadata)),
                    });
                }
            }
        }
    }

    messages
}

impl RedisEventBusBackend {
    pub fn new(config: RedisBackendConfig) -> Self {
        let client = build_client(&config.connection);
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
            ack_counters: AckCounters::default(),
            handles: Mutex::new(Vec::new()),
            running: Arc::new(AtomicBool::new(false)),
            dropped: Arc::new(AtomicUsize::new(0)),
            stream_writes,
        };

        Self {
            state: Arc::new(state),
        }
    }

    fn spawn_runtime_tasks(&self) {
        self.spawn_group_readers();
        self.spawn_plain_stream_readers();
        self.spawn_ack_worker();
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
        let consumer_name = spec
            .consumer_name
            .clone()
            .unwrap_or_else(|| next_consumer_name("redis-consumer"));
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
                    .arg(block_ms)
                    .arg("STREAMS");
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
                                let entry_id = message
                                    .backend_metadata
                                    .as_ref()
                                    .and_then(|meta| meta.as_any().downcast_ref::<RedisMetadata>())
                                    .map(|meta| meta.entry_id.clone());
                                let stream_name = message.source.clone();

                                match incoming.try_send(message) {
                                    Ok(_) => {
                                        if !manual_ack {
                                            if let Some(entry_id) = entry_id {
                                                let mut ack_cmd = redis::cmd("XACK");
                                                ack_cmd
                                                    .arg(&stream_name)
                                                    .arg(&consumer_group)
                                                    .arg(&entry_id);
                                                if let Err(err) =
                                                    ack_cmd.query_async::<_, i64>(&mut manager).await
                                                {
                                                    warn!(
                                                        stream = %stream_name,
                                                        group = %consumer_group,
                                                        entry = %entry_id,
                                                        error = %err,
                                                        "Failed to auto-ack Redis entry"
                                                    );
                                                }
                                            }
                                        }
                                    }
                                    Err(TrySendError::Full(_)) => {
                                        dropped.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(TrySendError::Disconnected(_)) => {
                                        warn!(
                                            "Message channel disconnected while sending Redis message"
                                        );
                                        return;
                                    }
                                }
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
                                match incoming.try_send(message) {
                                    Ok(_) => {}
                                    Err(TrySendError::Full(_)) => {
                                        dropped.fetch_add(1, Ordering::Relaxed);
                                    }
                                    Err(TrySendError::Disconnected(_)) => {
                                        warn!(
                                            stream = %stream,
                                            "Message channel disconnected while sending Redis message"
                                        );
                                        return;
                                    }
                                }
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

        let empty_backoff = self.state.runtime.empty_read_backoff;

        let handle = runtime::runtime().spawn(async move {
            let mut manager = match ConnectionManager::new(client).await {
                Ok(m) => m,
                Err(err) => {
                    error!(error = %err, "Failed to create Redis ack connection");
                    println!("Redis ack connection failed: {err}");
                    return;
                }
            };

            loop {
                match receiver.try_recv() {
                    Ok(request) => {
                        let mut cmd = redis::cmd("XACK");
                        cmd.arg(&request.stream)
                            .arg(&request.consumer_group)
                            .arg(&request.entry_id);
                        match cmd.query_async::<_, i64>(&mut manager).await {
                            Ok(_) => counters.mark_success(),
                            Err(err) => {
                                counters.mark_failure();
                                warn!(
                                    stream = %request.stream,
                                    group = %request.consumer_group,
                                    entry = %request.entry_id,
                                    error = %err,
                                    "Redis XACK failed"
                                );
                            }
                        }
                    }
                    Err(TryRecvError::Empty) => {
                        if !running.load(Ordering::SeqCst) && receiver.is_empty() {
                            break;
                        }
                        tokio::time::sleep(empty_backoff).await;
                    }
                    Err(TryRecvError::Disconnected) => break,
                }
            }
        });

        self.state.push_handle(handle);
    }

    pub fn trim_stream(
        &self,
        stream: &str,
        maxlen: usize,
        strategy: TrimStrategy,
    ) -> Result<(), String> {
        let manager = self
            .state
            .writer()
            .ok_or_else(|| "Redis writer connection not available".to_string())?;

        runtime::block_on(async move {
            let mut guard = manager.lock().await;
            let mut cmd = redis::cmd("XTRIM");
            cmd.arg(stream).arg("MAXLEN");
            if matches!(strategy, TrimStrategy::Approximate) {
                cmd.arg("~");
            }
            cmd.arg(maxlen as usize);

            cmd.query_async::<_, RedisValue>(&mut *guard)
                .await
                .map(|_| ())
                .map_err(|err| err.to_string())
        })
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
                error!(error = %err, "Redis connection failed");
                println!("Redis connection failed: {err}");
                self.state.running.store(false, Ordering::SeqCst);
                return false;
            }
        };

        if let Err(err) = ensure_streams(&mut manager, &self.state.config.topology).await {
            error!(error = %err, "Ensuring Redis streams failed");
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
        let headers: Vec<(String, String)> = options
            .headers
            .map(|h| h.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
            .unwrap_or_default();
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
            for (header_key, header_value) in headers {
                cmd.arg(format!("header:{header_key}")).arg(header_value);
            }

            if let Err(err) = cmd.query_async::<_, RedisValue>(&mut *guard).await {
                warn!(stream = %stream_name, error = %err, "Failed to enqueue Redis message");
            }
        });

        true
    }

    async fn receive_serialized(&self, stream: &str, options: ReceiveOptions<'_>) -> Vec<Vec<u8>> {
        self.drain_matching_messages(stream, options.consumer_group)
    }

    async fn flush(&self) -> Result<(), String> {
        if let Some(writer) = self.state.writer() {
            let _guard = writer.lock().await;
        }
        Ok(())
    }
}
