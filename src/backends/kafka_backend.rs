use async_trait::async_trait;
use crossbeam_channel::{
    Receiver, RecvTimeoutError, SendTimeoutError, Sender, TryRecvError, TrySendError, bounded,
};
use rdkafka::{
    ClientContext, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{BaseConsumer, CommitMode, Consumer},
    error::KafkaError,
    message::{Header, Headers, Message, OwnedHeaders},
    producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext},
};
use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    thread,
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use bevy::prelude::*;
use bevy_event_bus::{
    TopologyMode,
    backends::event_bus_backend::{
        BackendConfigError, BackendPluginSetup, EventBusBackend, LagReportingBackend,
        LagReportingDescriptor, LagReportingHandle, ManualCommitController, ManualCommitDescriptor,
        ManualCommitHandle, ManualCommitStyle, ReceiveOptions, SendOptions,
    },
    config::kafka::{
        KafkaBackendConfig, KafkaConnectionConfig, KafkaConsumerGroupSpec, KafkaInitialOffset,
        KafkaTopologyConfig,
    },
    resources::{
        ConsumerMetrics, IncomingMessage, KafkaCommitQueue, KafkaCommitResultChannel,
        KafkaLagCacheResource, backend_metadata::KafkaMetadata,
    },
    runtime,
};

#[derive(Debug, Clone)]
pub struct KafkaCommitRequest {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub consumer_group: String,
}

#[derive(Debug, Clone)]
pub struct KafkaCommitResult {
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub consumer_group: String,
    pub error: Option<String>,
}

#[derive(Debug, Clone)]
pub struct KafkaLagMeasurement {
    pub lag: i64,
    pub last_updated: Instant,
}

#[derive(Clone, Default)]
pub struct KafkaLagCache {
    inner: Arc<RwLock<HashMap<(String, String), KafkaLagMeasurement>>>,
}

impl KafkaLagCache {
    pub fn update(&self, consumer_group: &str, topic: &str, measurement: KafkaLagMeasurement) {
        let mut guard = self.inner.write().unwrap();
        guard.insert((consumer_group.to_string(), topic.to_string()), measurement);
    }

    pub fn snapshot_for_group(&self, consumer_group: &str) -> HashMap<String, KafkaLagMeasurement> {
        let guard = self.inner.read().unwrap();
        guard
            .iter()
            .filter(|((group, _), _)| group == consumer_group)
            .map(|((_, topic), measurement)| (topic.clone(), measurement.clone()))
            .collect()
    }

    pub fn get(&self, consumer_group: &str, topic: &str) -> Option<KafkaLagMeasurement> {
        let guard = self.inner.read().unwrap();
        guard
            .get(&(consumer_group.to_string(), topic.to_string()))
            .cloned()
    }
}

struct KafkaManualCommitHandle {
    sender: Sender<KafkaCommitRequest>,
    results: Mutex<Option<Receiver<KafkaCommitResult>>>,
}

impl KafkaManualCommitHandle {
    fn new(
        sender: Sender<KafkaCommitRequest>,
        results: Option<Receiver<KafkaCommitResult>>,
    ) -> Self {
        Self {
            sender,
            results: Mutex::new(results),
        }
    }
}

impl ManualCommitHandle for KafkaManualCommitHandle {
    fn register_resources(&self, world: &mut World) {
        world.insert_resource(KafkaCommitQueue(self.sender.clone()));
        if let Some(receiver) = self.results.lock().unwrap().take() {
            world.insert_resource(KafkaCommitResultChannel { receiver });
        }
    }

    fn descriptor(&self) -> ManualCommitDescriptor {
        ManualCommitDescriptor {
            backend: "kafka",
            style: ManualCommitStyle::OffsetQueue,
        }
    }
}

struct KafkaLagHandle {
    cache: KafkaLagCache,
}

impl KafkaLagHandle {
    fn new(cache: KafkaLagCache) -> Self {
        Self { cache }
    }
}

impl LagReportingHandle for KafkaLagHandle {
    fn register_resources(&self, world: &mut World) {
        world.insert_resource(KafkaLagCacheResource(self.cache.clone()));
    }

    fn descriptor(&self) -> LagReportingDescriptor {
        LagReportingDescriptor {
            backend: "kafka",
            detail: "consumer_lag",
        }
    }
}

fn kafka_commit_result_dispatch_system(
    mut commands: Commands,
    maybe_channel: Option<Res<KafkaCommitResultChannel>>,
    mut events: EventWriter<KafkaCommitResultEvent>,
) {
    if let Some(channel) = maybe_channel {
        loop {
            match channel.receiver.try_recv() {
                Ok(result) => {
                    events.write(KafkaCommitResultEvent {
                        backend: "kafka".into(),
                        consumer_group: result.consumer_group,
                        topic: result.topic,
                        partition: result.partition,
                        offset: result.offset,
                        error: result.error,
                    });
                }
                Err(TryRecvError::Empty) => break,
                Err(TryRecvError::Disconnected) => {
                    commands.remove_resource::<KafkaCommitResultChannel>();
                    break;
                }
            }
        }
    }
}

fn kafka_commit_result_stats_system(
    mut stats: ResMut<KafkaCommitResultStats>,
    mut events: EventReader<KafkaCommitResultEvent>,
) {
    for event in events.read() {
        let key = (
            event.backend.clone(),
            event.consumer_group.clone(),
            event.topic.clone(),
        );
        let entry = stats.totals.entry(key).or_default();
        entry.partition = event.partition;
        entry.last_offset = event.offset;
        match &event.error {
            Some(err) => {
                entry.failures += 1;
                entry.last_error = Some(err.clone());
            }
            None => {
                entry.successes += 1;
                entry.last_error = None;
            }
        }
    }
}

/// Emitted when an asynchronous Kafka manual commit completes.
#[derive(Event, Debug, Clone)]
pub struct KafkaCommitResultEvent {
    pub backend: String,
    pub consumer_group: String,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub error: Option<String>,
}

/// Tracks aggregate statistics for Kafka commit outcomes so tests or diagnostics can inspect behaviour.
#[derive(Resource, Debug, Clone, Default)]
pub struct KafkaCommitResultStats {
    pub totals: HashMap<(String, String, String), KafkaCommitOutcome>,
}

#[derive(Debug, Clone, Default)]
pub struct KafkaCommitOutcome {
    pub partition: i32,
    pub last_offset: i64,
    pub successes: usize,
    pub failures: usize,
    pub last_error: Option<String>,
}

#[derive(Clone)]
struct ConsumerRuntime {
    consumer: Arc<BaseConsumer>,
    manual_commit: bool,
    group_id: String,
    topics: Vec<String>,
}

/// Shared runtime state for the Kafka backend, allowing clones to coordinate producer, consumer and worker tasks.
struct KafkaBackendState {
    config: KafkaBackendConfig,
    producer: Arc<BaseProducer<EventBusProducerContext>>,
    consumers: Arc<Mutex<HashMap<String, ConsumerRuntime>>>,
    incoming_tx: Sender<IncomingMessage>,
    incoming_rx: Mutex<Option<Receiver<IncomingMessage>>>,
    pending_messages: Mutex<VecDeque<IncomingMessage>>,
    commit_tx: Sender<KafkaCommitRequest>,
    commit_rx: Mutex<Option<Receiver<KafkaCommitRequest>>>,
    commit_outcome_tx: Sender<KafkaCommitResult>,
    commit_outcome_rx: Mutex<Option<Receiver<KafkaCommitResult>>>,
    lag_cache: KafkaLagCache,
    running: Arc<AtomicBool>,
    consumer_handles: Mutex<Vec<JoinHandle<()>>>,
    commit_handle: Mutex<Option<JoinHandle<()>>>,
    lag_handle: Mutex<Option<JoinHandle<()>>>,
    dropped: Arc<AtomicUsize>,
}

#[derive(Clone)]
pub struct KafkaEventBusBackend {
    state: Arc<KafkaBackendState>,
}

impl Debug for KafkaEventBusBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let consumers = self.state.consumers.lock().unwrap();
        f.debug_struct("KafkaEventBusBackend")
            .field(
                "bootstrap",
                &self.state.config.connection.bootstrap_servers(),
            )
            .field(
                "consumer_groups",
                &consumers.keys().cloned().collect::<Vec<_>>(),
            )
            .finish()
    }
}

/// Custom producer context for Kafka delivery reporting
#[derive(Clone)]
struct EventBusProducerContext;

impl ClientContext for EventBusProducerContext {}

impl ProducerContext for EventBusProducerContext {
    type DeliveryOpaque = ();

    fn delivery(&self, delivery_result: &DeliveryResult, _delivery_opaque: Self::DeliveryOpaque) {
        match delivery_result {
            Err((kafka_error, owned_message)) => {
                warn!(
                    topic = %owned_message.topic(),
                    partition = owned_message.partition(),
                    error = %kafka_error,
                    "Kafka message delivery failed"
                );
                // TODO: Fire EventBusError<T> event when we have access to the event type
            }
            Ok(delivery) => {
                debug!(
                    topic = %delivery.topic(),
                    partition = delivery.partition(),
                    offset = delivery.offset(),
                    "Kafka message delivered successfully"
                );
            }
        }
    }
}

impl KafkaEventBusBackend {
    pub fn new(config: KafkaBackendConfig) -> Result<Self, BackendConfigError> {
        prepare_kafka_topology(&config.connection, &config.topology)?;

        let producer = Arc::new(
            build_producer(&config.connection)
                .map_err(|err| BackendConfigError::new("kafka", err.to_string()))?,
        );

        let consumers = Arc::new(Mutex::new(build_consumers(
            &config.connection,
            &config.topology,
        )));

        let capacities = config.channel_capacities.clone();

        let (incoming_tx, incoming_rx) = bounded(capacities.message);
        let (commit_tx, commit_rx) = bounded(capacities.commit);
        let (result_tx, result_rx) = bounded(capacities.result);

        let state = KafkaBackendState {
            config,
            producer,
            consumers: consumers.clone(),
            incoming_tx,
            incoming_rx: Mutex::new(Some(incoming_rx)),
            pending_messages: Mutex::new(VecDeque::new()),
            commit_tx,
            commit_rx: Mutex::new(Some(commit_rx)),
            commit_outcome_tx: result_tx,
            commit_outcome_rx: Mutex::new(Some(result_rx)),
            lag_cache: KafkaLagCache::default(),
            running: Arc::new(AtomicBool::new(false)),
            consumer_handles: Mutex::new(Vec::new()),
            commit_handle: Mutex::new(None),
            lag_handle: Mutex::new(None),
            dropped: Arc::new(AtomicUsize::new(0)),
        };
        Ok(Self {
            state: Arc::new(state),
        })
    }

    /// Returns true if the backend topology provisions the supplied consumer group identifier.
    pub fn bootstrap_servers(&self) -> &str {
        self.state.config.connection.bootstrap_servers()
    }

    pub fn configured_topics(&self) -> Vec<String> {
        self.state
            .config
            .topology
            .topics()
            .iter()
            .map(|spec| spec.name.clone())
            .collect()
    }

    pub fn take_receiver(&self) -> Option<Receiver<IncomingMessage>> {
        self.state.incoming_rx.lock().unwrap().take()
    }

    pub fn commit_sender(&self) -> Sender<KafkaCommitRequest> {
        self.state.commit_tx.clone()
    }

    pub fn take_commit_results(&self) -> Option<Receiver<KafkaCommitResult>> {
        self.state.commit_outcome_rx.lock().unwrap().take()
    }

    pub fn lag_cache(&self) -> KafkaLagCache {
        self.state.lag_cache.clone()
    }

    pub fn dropped_count(&self) -> usize {
        self.state.dropped.load(Ordering::Relaxed)
    }

    pub fn poll_producer(&self) {
        self.state.producer.poll(Duration::from_millis(0));
    }

    pub fn flush(&self, timeout: Duration) -> Result<(), String> {
        self.state
            .producer
            .flush(timeout)
            .map_err(|err| err.to_string())
    }

    fn drain_matching_messages(&self, topic: &str, group: Option<&str>) -> Vec<Vec<u8>> {
        let mut matches = Vec::new();
        let mut deferred = VecDeque::new();

        {
            let mut pending = self.state.pending_messages.lock().unwrap();
            while let Some(msg) = pending.pop_front() {
                if Self::message_matches(&msg, topic, group) {
                    let payload = msg.payload;
                    matches.push(payload);
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
                        let matches_topic = Self::message_matches(&msg, topic, group);
                        if matches_topic {
                            let payload = msg.payload;
                            matches.push(payload);
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

    fn message_matches(message: &IncomingMessage, topic: &str, group: Option<&str>) -> bool {
        if message.source != topic {
            return false;
        }

        match group {
            Some(group_id) => message
                .backend_metadata
                .as_ref()
                .and_then(|meta| meta.as_any().downcast_ref::<KafkaMetadata>())
                .and_then(|kafka| kafka.consumer_group.as_deref())
                .map(|candidate| candidate == group_id)
                .unwrap_or(false),
            None => true,
        }
    }

    fn spawn_runtime_tasks(&self) {
        self.spawn_consumer_tasks();
        self.spawn_commit_worker();
        self.spawn_lag_worker();
    }

    fn spawn_consumer_tasks(&self) {
        let consumers = self.state.consumers.lock().unwrap().clone();
        let running = self.state.running.clone();
        let producer = self.state.producer.clone();
        let tx = self.state.incoming_tx.clone();
        let dropped = self.state.dropped.clone();

        let mut handles = self.state.consumer_handles.lock().unwrap();
        for runtime in consumers.into_values() {
            let tx_clone = tx.clone();
            let running_clone = running.clone();
            let producer_clone = producer.clone();
            let dropped_clone = dropped.clone();
            let handle = runtime::runtime().spawn(async move {
                consumer_loop(
                    runtime,
                    tx_clone,
                    running_clone,
                    producer_clone,
                    dropped_clone,
                )
                .await;
            });
            handles.push(handle);
        }
    }

    fn spawn_commit_worker(&self) {
        let mut guard = self.state.commit_handle.lock().unwrap();
        if guard.is_some() {
            return;
        }

        let running = self.state.running.clone();
        let consumers = self.state.consumers.clone();
        let rx = self
            .state
            .commit_rx
            .lock()
            .unwrap()
            .take()
            .expect("commit receiver already taken");
        let outcome_tx = self.state.commit_outcome_tx.clone();

        let handle = runtime::runtime().spawn(async move {
            while running.load(Ordering::Relaxed) {
                match tokio::task::block_in_place(|| rx.recv_timeout(Duration::from_millis(50))) {
                    Ok(req) => {
                        let consumer_opt =
                            consumers.lock().unwrap().get(&req.consumer_group).cloned();
                        let result = if let Some(runtime) = consumer_opt {
                            commit_offset_sync(&runtime, &req)
                        } else {
                            Err(format!(
                                "Consumer group '{}' not found for commit",
                                req.consumer_group
                            ))
                        };

                        let _ = outcome_tx.try_send(KafkaCommitResult {
                            topic: req.topic,
                            partition: req.partition,
                            offset: req.offset,
                            consumer_group: req.consumer_group,
                            error: result.err(),
                        });
                    }
                    Err(RecvTimeoutError::Timeout) => {}
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        });

        *guard = Some(handle);
    }

    fn spawn_lag_worker(&self) {
        let mut guard = self.state.lag_handle.lock().unwrap();
        if guard.is_some() {
            return;
        }

        let running = self.state.running.clone();
        let consumers = self.state.consumers.clone();
        let lag_cache = self.state.lag_cache.clone();
        let poll_interval = self.state.config.consumer_lag_poll_interval;

        let handle = runtime::runtime().spawn(async move {
            let mut ticker = tokio::time::interval(poll_interval);
            while running.load(Ordering::Relaxed) {
                ticker.tick().await;
                let snapshot = consumers.lock().unwrap().clone();
                for runtime in snapshot.values() {
                    for topic in &runtime.topics {
                        match compute_consumer_lag(runtime, topic) {
                            Ok(lag) => {
                                lag_cache.update(
                                    &runtime.group_id,
                                    topic,
                                    KafkaLagMeasurement {
                                        lag,
                                        last_updated: Instant::now(),
                                    },
                                );
                            }
                            Err(err) => {
                                warn!(
                                    consumer_group = %runtime.group_id,
                                    topic = %topic,
                                    error = %err,
                                    "Failed to refresh consumer lag"
                                );
                            }
                        }
                    }
                }
            }
        });

        *guard = Some(handle);
    }

    fn stop_tasks(&self) {
        if let Ok(mut handles) = self.state.consumer_handles.lock() {
            for handle in handles.drain(..) {
                handle.abort();
            }
        }

        if let Ok(mut guard) = self.state.commit_handle.lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }

        if let Ok(mut guard) = self.state.lag_handle.lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }
}

impl Drop for KafkaEventBusBackend {
    fn drop(&mut self) {
        if self.state.running.swap(false, Ordering::SeqCst) {
            self.stop_tasks();
        }

        for _ in 0..10 {
            self.state.producer.poll(Duration::from_millis(10));
        }
        let _ = self.state.producer.flush(Duration::from_millis(250));
    }
}

#[async_trait]
impl EventBusBackend for KafkaEventBusBackend {
    fn clone_box(&self) -> Box<dyn EventBusBackend> {
        Box::new(self.clone())
    }

    fn as_any(&self) -> &dyn std::any::Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }

    fn backend_name(&self) -> &'static str {
        "kafka"
    }

    fn configure_plugin(&self, app: &mut App) {
        app.add_event::<KafkaCommitResultEvent>();
        app.init_resource::<KafkaCommitResultStats>();
        app.add_systems(
            PreUpdate,
            (
                kafka_commit_result_dispatch_system,
                kafka_commit_result_stats_system,
            )
                .chain(),
        );
    }

    fn setup_plugin(&self, _world: &mut World) -> BackendPluginSetup {
        let mut setup = BackendPluginSetup::default();
        setup.ready_topics = self.configured_topics();
        setup.message_stream = self.take_receiver();
        setup.manual_commit = Some(Box::new(KafkaManualCommitHandle::new(
            self.commit_sender(),
            self.take_commit_results(),
        )));
        setup.lag_reporting = Some(Box::new(KafkaLagHandle::new(self.lag_cache())));
        setup.kafka_topology = Some(self.state.config.topology.clone());

        setup
    }

    fn augment_metrics(&self, metrics: &mut ConsumerMetrics) {
        metrics.dropped_messages = self.dropped_count();
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

        self.spawn_runtime_tasks();
        true
    }

    async fn disconnect(&mut self) -> bool {
        if !self.state.running.swap(false, Ordering::SeqCst) {
            return true;
        }

        self.stop_tasks();
        true
    }

    fn try_send_serialized(
        &self,
        event_json: &[u8],
        topic: &str,
        options: SendOptions<'_>,
    ) -> bool {
        let mut record = BaseRecord::to(topic).payload(event_json);

        if let Some(key) = options.partition_key {
            record = record.key(key.as_bytes());
        }

        if let Some(backend_data) = options.backend.as_any() {
            if let Some(headers) = backend_data.downcast_ref::<HashMap<String, String>>() {
                if !headers.is_empty() {
                    let mut kafka_headers = OwnedHeaders::new();
                    for (key, value) in headers {
                        kafka_headers = kafka_headers.insert(Header {
                            key,
                            value: Some(value.as_bytes()),
                        });
                    }
                    record = record.headers(kafka_headers);
                }
            }
        }

        match self.state.producer.send(record) {
            Ok(_) => true,
            Err((err, _)) => {
                warn!(target = %topic, error = %err, "Failed to enqueue Kafka message");
                false
            }
        }
    }

    async fn receive_serialized(&self, topic: &str, options: ReceiveOptions<'_>) -> Vec<Vec<u8>> {
        self.drain_matching_messages(topic, options.consumer_group)
    }

    async fn flush(&self) -> Result<(), String> {
        match self.state.producer.flush(Duration::from_secs(30)) {
            Ok(_) => Ok(()),
            Err(err) => Err(format!("Flush failed: {err}")),
        }
    }
}

#[async_trait]
impl ManualCommitController for KafkaEventBusBackend {
    async fn enable_manual_commits(&mut self, group_id: &str) -> Result<(), String> {
        let consumers = self.state.consumers.lock().unwrap();
        if let Some(runtime) = consumers.get(group_id) {
            if runtime.manual_commit {
                Ok(())
            } else {
                Err(format!(
                    "Consumer group '{}' is configured for auto commits",
                    group_id
                ))
            }
        } else {
            Err(format!("Consumer group '{}' not found", group_id))
        }
    }

    async fn commit_offset(&self, topic: &str, partition: i32, offset: i64) -> Result<(), String> {
        let consumers = self.state.consumers.lock().unwrap();
        let runtime = consumers
            .values()
            .find(|cg| cg.manual_commit && cg.topics.contains(&topic.to_string()))
            .cloned()
            .ok_or_else(|| format!("No manual commit consumer group for topic '{}'", topic))?;
        drop(consumers);

        let request = KafkaCommitRequest {
            topic: topic.to_string(),
            partition,
            offset,
            consumer_group: runtime.group_id.clone(),
        };

        match self.state.commit_tx.try_send(request.clone()) {
            Ok(_) => Ok(()),
            Err(crossbeam_channel::TrySendError::Full(_)) => commit_offset_sync(&runtime, &request),
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                Err("Kafka commit queue is not available".to_string())
            }
        }
    }
}

#[async_trait]
impl LagReportingBackend for KafkaEventBusBackend {
    async fn get_consumer_lag(&self, topic: &str, group_id: &str) -> Result<i64, String> {
        let consumers = self.state.consumers.lock().unwrap();
        if let Some(runtime) = consumers.get(group_id) {
            compute_consumer_lag(runtime, topic)
        } else {
            Err(format!("Consumer group '{}' not found", group_id))
        }
    }
}

fn apply_connection_settings(cfg: &mut ClientConfig, connection: &KafkaConnectionConfig) {
    cfg.set("bootstrap.servers", connection.bootstrap_servers());

    if let Some(client_id) = connection.client_id() {
        cfg.set("client.id", client_id);
    }

    for (key, value) in connection.additional_config() {
        cfg.set(key, value);
    }
}

fn build_producer(
    connection: &KafkaConnectionConfig,
) -> Result<BaseProducer<EventBusProducerContext>, KafkaError> {
    let mut cfg = ClientConfig::new();
    apply_connection_settings(&mut cfg, connection);
    cfg.set("message.timeout.ms", connection.timeout_ms().to_string());

    cfg.create_with_context(EventBusProducerContext)
}

fn prepare_kafka_topology(
    connection: &KafkaConnectionConfig,
    topology: &KafkaTopologyConfig,
) -> Result<(), BackendConfigError> {
    prepare_topics(connection, topology)
        .map_err(|reason| BackendConfigError::new("kafka", reason))?;
    prepare_consumer_groups(connection, topology)
        .map_err(|reason| BackendConfigError::new("kafka", reason))?;
    Ok(())
}

fn prepare_topics(
    connection: &KafkaConnectionConfig,
    topology: &KafkaTopologyConfig,
) -> Result<(), String> {
    if topology.topics().is_empty() {
        return Ok(());
    }

    let mut cfg = ClientConfig::new();
    apply_connection_settings(&mut cfg, connection);
    cfg.set("allow.auto.create.topics", "false");

    let admin = cfg
        .create::<AdminClient<DefaultClientContext>>()
        .map_err(|err| format!("Failed to create Kafka admin client: {err}"))?;

    let mut provision_specs = Vec::new();
    let mut validate_specs = Vec::new();
    for spec in topology.topics() {
        match spec.mode {
            TopologyMode::Provision => provision_specs.push(spec),
            TopologyMode::Validate => validate_specs.push(spec),
        }
    }

    if !provision_specs.is_empty() {
        let options = AdminOptions::new();
        let topics: Vec<NewTopic> = provision_specs
            .iter()
            .map(|spec| {
                let partitions = spec.partitions.unwrap_or(1);
                let replication = spec
                    .replication
                    .map(|r| TopicReplication::Fixed(r as i32))
                    .unwrap_or(TopicReplication::Fixed(1));
                NewTopic::new(&spec.name, partitions, replication)
            })
            .collect();

        if !topics.is_empty() {
            if let Err(err) = runtime::block_on(admin.create_topics(topics.iter(), &options)) {
                let msg = err.to_string();
                if !msg.contains("TopicAlreadyExists") {
                    return Err(format!("Topic creation batch failed: {msg}"));
                }
            }
        }
    }

    for spec in validate_specs {
        let metadata = admin
            .inner()
            .fetch_metadata(Some(&spec.name), Duration::from_secs(5))
            .map_err(|err| {
                format!(
                    "Failed to fetch metadata for Kafka topic '{}': {err}",
                    spec.name
                )
            })?;

        let topic_metadata = metadata
            .topics()
            .iter()
            .find(|topic| topic.name() == spec.name)
            .ok_or_else(|| {
                format!(
                    "Kafka topic '{}' metadata missing during validation",
                    spec.name
                )
            })?;

        if let Some(error) = topic_metadata.error() {
            return Err(format!(
                "Kafka topic '{}' reported error during validation: {:?}",
                spec.name, error
            ));
        }
    }

    Ok(())
}

fn prepare_consumer_groups(
    connection: &KafkaConnectionConfig,
    topology: &KafkaTopologyConfig,
) -> Result<(), String> {
    if topology.consumer_groups().is_empty() {
        return Ok(());
    }

    for (group_id, spec) in topology.consumer_groups() {
        if spec.mode == TopologyMode::Provision {
            provision_consumer_group(connection, group_id, spec)?;
        }
    }

    let mut validator = build_validation_consumer(connection)?;

    for (group_id, spec) in topology.consumer_groups() {
        validate_consumer_group(&mut validator, group_id).map_err(|err| match spec.mode {
            TopologyMode::Provision => format!(
                "Kafka consumer group '{}' could not be provisioned: {err}",
                group_id
            ),
            TopologyMode::Validate => format!(
                "Kafka consumer group '{}' is missing while configured with TopologyMode::Validate: {err}",
                group_id
            ),
        })?;
    }

    Ok(())
}

fn provision_consumer_group(
    connection: &KafkaConnectionConfig,
    group_id: &str,
    spec: &KafkaConsumerGroupSpec,
) -> Result<(), String> {
    let consumer = build_consumer(connection, group_id, spec).map_err(|err| {
        format!(
            "Failed to build Kafka consumer while provisioning group '{}': {err}",
            group_id
        )
    })?;

    if !spec.topics.is_empty() {
        let topic_refs: Vec<&str> = spec.topics.iter().map(String::as_str).collect();
        consumer.subscribe(&topic_refs).map_err(|err| {
            format!(
                "Failed to subscribe while provisioning Kafka consumer group '{}': {err}",
                group_id
            )
        })?;
    }

    const REGISTRATION_ATTEMPTS: usize = 10;
    for attempt in 0..REGISTRATION_ATTEMPTS {
        if let Some(Err(err)) = consumer.poll(Duration::from_millis(100)) {
            warn!(
                group = %group_id,
                error = %err,
                "Kafka consumer poll error while provisioning group"
            );
        }

        match consumer
            .client()
            .fetch_group_list(Some(group_id), Duration::from_secs(2))
        {
            Ok(metadata) => {
                let found = metadata
                    .groups()
                    .iter()
                    .any(|group| group.name() == group_id);
                if found {
                    return Ok(());
                }
            }
            Err(err) => {
                warn!(
                    group = %group_id,
                    error = %err,
                    "Kafka consumer group metadata fetch failed during provisioning"
                );
            }
        }

        if attempt + 1 < REGISTRATION_ATTEMPTS {
            thread::sleep(Duration::from_millis(100));
        }
    }

    Err(format!(
        "Kafka consumer group '{}' not reported by broker after provisioning",
        group_id
    ))
}

fn build_validation_consumer(connection: &KafkaConnectionConfig) -> Result<BaseConsumer, String> {
    let mut cfg = ClientConfig::new();
    apply_connection_settings(&mut cfg, connection);
    cfg.set("group.id", "__bevy_event_bus.topology_validator");
    cfg.set("enable.auto.commit", "false");
    cfg.set("allow.auto.create.topics", "false");
    cfg.set("socket.timeout.ms", "5000");

    cfg.create()
        .map_err(|err| format!("Failed to create Kafka validation consumer: {err}"))
}

fn validate_consumer_group(consumer: &mut BaseConsumer, group_id: &str) -> Result<(), String> {
    const VALIDATION_RETRIES: usize = 10;

    for attempt in 0..VALIDATION_RETRIES {
        let metadata = consumer
            .client()
            .fetch_group_list(Some(group_id), Duration::from_secs(5))
            .map_err(|err| {
                format!(
                    "Failed to fetch metadata for Kafka consumer group '{}': {err}",
                    group_id
                )
            })?;

        let found = metadata
            .groups()
            .iter()
            .any(|group| group.name() == group_id);
        if found {
            return Ok(());
        }

        if attempt + 1 < VALIDATION_RETRIES {
            thread::sleep(Duration::from_millis(100));
        }
    }

    Err("group not reported by broker".to_string())
}

fn build_consumers(
    connection: &KafkaConnectionConfig,
    topology: &KafkaTopologyConfig,
) -> HashMap<String, ConsumerRuntime> {
    let mut map = HashMap::new();
    for (group_id, spec) in topology.consumer_groups() {
        let consumer = build_consumer(connection, group_id, spec)
            .unwrap_or_else(|e| panic!("Failed to build consumer for group {}: {e}", group_id));

        if !spec.topics.is_empty() {
            let topic_refs: Vec<&str> = spec.topics.iter().map(String::as_str).collect();
            consumer
                .subscribe(&topic_refs)
                .unwrap_or_else(|e| panic!("Failed to subscribe group {}: {e}", group_id));
        }

        map.insert(
            group_id.clone(),
            ConsumerRuntime {
                consumer: Arc::new(consumer),
                manual_commit: spec.manual_commits,
                group_id: group_id.clone(),
                topics: spec.topics.clone(),
            },
        );
    }

    map
}

fn build_consumer(
    connection: &KafkaConnectionConfig,
    group_id: &str,
    spec: &KafkaConsumerGroupSpec,
) -> Result<BaseConsumer, KafkaError> {
    let mut cfg = ClientConfig::new();
    apply_connection_settings(&mut cfg, connection);
    cfg.set("group.id", group_id);
    cfg.set(
        "enable.auto.commit",
        if spec.manual_commits { "false" } else { "true" },
    );
    cfg.set("session.timeout.ms", "6000");
    cfg.set(
        "auto.offset.reset",
        match spec.initial_offset {
            KafkaInitialOffset::Earliest => "earliest",
            KafkaInitialOffset::Latest => "latest",
            KafkaInitialOffset::None => "none",
        },
    );
    if let Some(client_id) = connection.client_id() {
        cfg.set("client.id", &format!("{}_{}", client_id, group_id));
    }

    cfg.create()
}

async fn consumer_loop(
    runtime: ConsumerRuntime,
    tx: Sender<IncomingMessage>,
    running: Arc<AtomicBool>,
    producer: Arc<BaseProducer<EventBusProducerContext>>,
    dropped: Arc<AtomicUsize>,
) {
    while running.load(Ordering::Relaxed) {
        producer.poll(Duration::from_millis(0));
        match runtime.consumer.poll(Duration::from_millis(50)) {
            Some(Ok(message)) => {
                let payload = match message.payload() {
                    Some(payload) => payload.to_vec(),
                    None => Vec::new(),
                };

                let key = message
                    .key()
                    .map(|k| String::from_utf8_lossy(k).into_owned());
                let mut headers_map = HashMap::new();
                if let Some(headers) = message.headers() {
                    for header in headers.iter() {
                        if let Some(value) = header.value {
                            if let Ok(str_value) = String::from_utf8(value.to_vec()) {
                                headers_map.insert(header.key.to_string(), str_value);
                            }
                        }
                    }
                }

                let kafka_metadata = KafkaMetadata {
                    topic: message.topic().to_string(),
                    partition: message.partition(),
                    offset: message.offset(),
                    consumer_group: Some(runtime.group_id.clone()),
                    manual_commit: runtime.manual_commit,
                    headers: headers_map,
                };

                let msg = IncomingMessage::plain(
                    message.topic().to_string(),
                    payload,
                    key,
                    Some(Box::new(kafka_metadata)),
                );

                let mut pending = msg;

                loop {
                    match tx.try_send(pending) {
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
                                    pending = returned_again;
                                }
                                Err(SendTimeoutError::Disconnected(_returned)) => {
                                    dropped.fetch_add(1, Ordering::Relaxed);
                                    break;
                                }
                            }
                        }
                        Err(TrySendError::Disconnected(returned)) => {
                            let _ = returned;
                            dropped.fetch_add(1, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            }
            Some(Err(err)) => {
                warn!("Kafka consumer poll error: {err}");
            }
            None => {}
        }
    }
}

fn commit_offset_sync(runtime: &ConsumerRuntime, req: &KafkaCommitRequest) -> Result<(), String> {
    if !runtime.manual_commit {
        return Err(format!(
            "Consumer group '{}' is not configured for manual commits",
            runtime.group_id
        ));
    }

    let mut tpl = TopicPartitionList::new();
    tpl.add_partition_offset(&req.topic, req.partition, Offset::Offset(req.offset + 1))
        .map_err(|err| err.to_string())?;

    runtime
        .consumer
        .commit(&tpl, CommitMode::Sync)
        .map_err(|err| err.to_string())
}

fn compute_consumer_lag(runtime: &ConsumerRuntime, topic: &str) -> Result<i64, String> {
    let consumer = &runtime.consumer;

    let metadata = consumer
        .fetch_metadata(Some(topic), Duration::from_secs(5))
        .map_err(|err| err.to_string())?;

    let topic_metadata = metadata
        .topics()
        .iter()
        .find(|t| t.name() == topic)
        .ok_or_else(|| format!("Topic '{topic}' not found in metadata"))?;

    let mut total_lag = 0i64;

    for partition in topic_metadata.partitions() {
        let partition_id = partition.id();

        let (low, high) = consumer
            .fetch_watermarks(topic, partition_id, Duration::from_secs(5))
            .map_err(|err| err.to_string())?;

        let mut tpl = TopicPartitionList::new();
        tpl.add_partition_offset(topic, partition_id, Offset::Invalid)
            .map_err(|err| err.to_string())?;

        let committed = consumer
            .committed_offsets(tpl, Duration::from_secs(5))
            .map_err(|err| err.to_string())?;

        if let Some(elem) = committed.elements().first() {
            match elem.offset() {
                Offset::Offset(committed_offset) => {
                    let lag = high - committed_offset;
                    total_lag += lag.max(0);
                }
                _ => {
                    let lag = high - low;
                    total_lag += lag.max(0);
                }
            }
        }
    }

    Ok(total_lag)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_producer_context_creation() {
        let context = EventBusProducerContext;
        assert_eq!(std::mem::size_of_val(&context), 0);
    }
}
