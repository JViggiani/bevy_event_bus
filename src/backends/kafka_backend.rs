use async_trait::async_trait;
use crossbeam_channel::{Receiver, Sender, TryRecvError, TrySendError, bounded};
use rdkafka::{
    ClientContext, Offset, TopicPartitionList,
    admin::{AdminClient, AdminOptions, NewTopic, TopicReplication},
    client::DefaultClientContext,
    config::ClientConfig,
    consumer::{BaseConsumer, CommitMode, Consumer},
    error::KafkaError,
    message::{Headers, Message},
    producer::{BaseProducer, BaseRecord, DeliveryResult, Producer, ProducerContext},
};
use std::{
    collections::{HashMap, VecDeque},
    fmt::Debug,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::task::JoinHandle;
use tracing::{debug, warn};

use bevy::prelude::App;
use bevy_event_bus::{
    EventBusBackend,
    config::kafka::{
        KafkaBackendConfig, KafkaConnectionConfig, KafkaConsumerGroupSpec, KafkaInitialOffset,
        KafkaTopologyConfig,
    },
    resources::IncomingMessage,
    runtime,
};

const MESSAGE_CHANNEL_CAPACITY: usize = 10_000;
const COMMIT_CHANNEL_CAPACITY: usize = 2_048;
const RESULT_CHANNEL_CAPACITY: usize = 1_024;

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

#[derive(Clone)]
struct ConsumerRuntime {
    consumer: Arc<BaseConsumer>,
    manual_commit: bool,
    group_id: String,
    topics: Vec<String>,
}

struct KafkaBackendInner {
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
    inner: Arc<KafkaBackendInner>,
}

impl Debug for KafkaEventBusBackend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let consumers = self.inner.consumers.lock().unwrap();
        f.debug_struct("KafkaEventBusBackend")
            .field(
                "bootstrap",
                &self.inner.config.connection.bootstrap_servers(),
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
    pub fn new(config: KafkaBackendConfig) -> Self {
        let producer = Arc::new(build_producer(&config.connection));
        ensure_topics(&config.connection, &config.topology);

        let consumers = Arc::new(Mutex::new(build_consumers(
            &config.connection,
            &config.topology,
        )));

        let (incoming_tx, incoming_rx) = bounded(MESSAGE_CHANNEL_CAPACITY);
        let (commit_tx, commit_rx) = bounded(COMMIT_CHANNEL_CAPACITY);
        let (result_tx, result_rx) = bounded(RESULT_CHANNEL_CAPACITY);

        let inner = KafkaBackendInner {
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

        Self {
            inner: Arc::new(inner),
        }
    }

    pub fn bootstrap_servers(&self) -> &str {
        self.inner.config.connection.bootstrap_servers()
    }

    pub fn configured_topics(&self) -> Vec<String> {
        self.inner
            .config
            .topology
            .topics()
            .iter()
            .map(|spec| spec.name.clone())
            .collect()
    }

    pub fn take_receiver(&self) -> Option<Receiver<IncomingMessage>> {
        self.inner.incoming_rx.lock().unwrap().take()
    }

    pub fn commit_sender(&self) -> Sender<KafkaCommitRequest> {
        self.inner.commit_tx.clone()
    }

    pub fn take_commit_results(&self) -> Option<Receiver<KafkaCommitResult>> {
        self.inner.commit_outcome_rx.lock().unwrap().take()
    }

    pub fn lag_cache(&self) -> KafkaLagCache {
        self.inner.lag_cache.clone()
    }

    pub fn dropped_count(&self) -> usize {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    pub fn poll_producer(&self) {
        self.inner.producer.poll(Duration::from_millis(0));
    }

    pub fn flush_with_timeout(&self, timeout: Duration) -> Result<(), String> {
        self.inner
            .producer
            .flush(timeout)
            .map_err(|err| err.to_string())
    }

    fn drain_matching_messages(&self, topic: &str, group: Option<&str>) -> Vec<Vec<u8>> {
        let mut matches = Vec::new();
        let mut deferred = VecDeque::new();

        {
            let mut pending = self.inner.pending_messages.lock().unwrap();
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
            let mut guard = self.inner.incoming_rx.lock().unwrap();
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

            let mut guard = self.inner.incoming_rx.lock().unwrap();
            *guard = Some(receiver);
        }

        if !deferred.is_empty() {
            let mut pending = self.inner.pending_messages.lock().unwrap();
            while let Some(msg) = deferred.pop_front() {
                pending.push_back(msg);
            }
        }

        matches
    }

    fn message_matches(message: &IncomingMessage, topic: &str, group: Option<&str>) -> bool {
        if message.topic != topic {
            return false;
        }

        match group {
            Some(group_id) => message
                .consumer_group
                .as_deref()
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
        let consumers = self.inner.consumers.lock().unwrap().clone();
        let running = self.inner.running.clone();
        let producer = self.inner.producer.clone();
        let tx = self.inner.incoming_tx.clone();
        let dropped = self.inner.dropped.clone();

        let mut handles = self.inner.consumer_handles.lock().unwrap();
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
        let mut guard = self.inner.commit_handle.lock().unwrap();
        if guard.is_some() {
            return;
        }

        let running = self.inner.running.clone();
        let consumers = self.inner.consumers.clone();
        let rx = self
            .inner
            .commit_rx
            .lock()
            .unwrap()
            .take()
            .expect("commit receiver already taken");
        let outcome_tx = self.inner.commit_outcome_tx.clone();

        let handle = runtime::runtime().spawn(async move {
            while running.load(Ordering::Relaxed) {
                match rx.try_recv() {
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
                    Err(crossbeam_channel::TryRecvError::Empty) => {
                        tokio::time::sleep(Duration::from_millis(25)).await;
                    }
                    Err(crossbeam_channel::TryRecvError::Disconnected) => break,
                }
            }
        });

        *guard = Some(handle);
    }

    fn spawn_lag_worker(&self) {
        let mut guard = self.inner.lag_handle.lock().unwrap();
        if guard.is_some() {
            return;
        }

        let running = self.inner.running.clone();
        let consumers = self.inner.consumers.clone();
        let cache = self.inner.lag_cache.clone();
        let interval = self.inner.config.consumer_lag_poll_interval;

        let handle = runtime::runtime().spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            while running.load(Ordering::Relaxed) {
                ticker.tick().await;
                let snapshot = consumers.lock().unwrap().clone();
                for runtime in snapshot.values() {
                    for topic in &runtime.topics {
                        match compute_consumer_lag(runtime, topic) {
                            Ok(lag) => {
                                cache.update(
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
        if let Ok(mut handles) = self.inner.consumer_handles.lock() {
            for handle in handles.drain(..) {
                handle.abort();
            }
        }

        if let Ok(mut guard) = self.inner.commit_handle.lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }

        if let Ok(mut guard) = self.inner.lag_handle.lock() {
            if let Some(handle) = guard.take() {
                handle.abort();
            }
        }
    }
}

impl Drop for KafkaEventBusBackend {
    fn drop(&mut self) {
        if self.inner.running.swap(false, Ordering::SeqCst) {
            self.stop_tasks();
        }

        for _ in 0..10 {
            self.inner.producer.poll(Duration::from_millis(10));
        }
        let _ = self.inner.producer.flush(Duration::from_millis(250));
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

    fn apply_event_bindings(&self, app: &mut App) {
        for binding in self.inner.config.topology.event_bindings() {
            binding.apply(app);
        }
    }

    async fn connect(&mut self) -> bool {
        if self
            .inner
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
        if !self.inner.running.swap(false, Ordering::SeqCst) {
            return true;
        }

        self.stop_tasks();
        true
    }

    fn try_send_serialized(&self, event_json: &[u8], topic: &str) -> bool {
        let record = BaseRecord::<(), _>::to(topic).payload(event_json);
        match self.inner.producer.send(record) {
            Ok(_) => true,
            Err((err, _)) => {
                warn!(target = %topic, error = %err, "Failed to enqueue Kafka message");
                false
            }
        }
    }

    fn try_send_serialized_with_headers(
        &self,
        event_json: &[u8],
        topic: &str,
        headers: &HashMap<String, String>,
    ) -> bool {
        let mut record = BaseRecord::<(), _>::to(topic).payload(event_json);
        if !headers.is_empty() {
            let mut kafka_headers = rdkafka::message::OwnedHeaders::new();
            for (key, value) in headers {
                kafka_headers = kafka_headers.insert(rdkafka::message::Header {
                    key,
                    value: Some(value.as_bytes()),
                });
            }
            record = record.headers(kafka_headers);
        }

        match self.inner.producer.send(record) {
            Ok(_) => true,
            Err((err, _)) => {
                warn!(target = %topic, error = %err, "Failed to enqueue Kafka message with headers");
                false
            }
        }
    }

    async fn receive_serialized(&self, topic: &str) -> Vec<Vec<u8>> {
        self.drain_matching_messages(topic, None)
    }

    async fn subscribe(&mut self, _topic: &str) -> bool {
        true
    }

    async fn unsubscribe(&mut self, _topic: &str) -> bool {
        true
    }

    async fn create_consumer_group(
        &mut self,
        _topics: &[String],
        _group_id: &str,
    ) -> Result<(), String> {
        Err("Dynamic consumer group creation is not supported at runtime".into())
    }

    async fn receive_serialized_with_group(&self, topic: &str, group_id: &str) -> Vec<Vec<u8>> {
        self.drain_matching_messages(topic, Some(group_id))
    }

    async fn enable_manual_commits(&mut self, group_id: &str) -> Result<(), String> {
        let consumers = self.inner.consumers.lock().unwrap();
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
        let consumers = self.inner.consumers.lock().unwrap();
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

        match self.inner.commit_tx.try_send(request.clone()) {
            Ok(_) => Ok(()),
            Err(crossbeam_channel::TrySendError::Full(_)) => commit_offset_sync(&runtime, &request),
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                Err("Kafka commit queue is not available".to_string())
            }
        }
    }

    async fn get_consumer_lag(&self, topic: &str, group_id: &str) -> Result<i64, String> {
        let consumers = self.inner.consumers.lock().unwrap();
        if let Some(runtime) = consumers.get(group_id) {
            compute_consumer_lag(runtime, topic)
        } else {
            Err(format!("Consumer group '{}' not found", group_id))
        }
    }

    async fn flush(&self) -> Result<(), String> {
        match self.inner.producer.flush(Duration::from_secs(30)) {
            Ok(_) => Ok(()),
            Err(err) => Err(format!("Flush failed: {err}")),
        }
    }
}

fn build_producer(connection: &KafkaConnectionConfig) -> BaseProducer<EventBusProducerContext> {
    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", connection.bootstrap_servers());
    cfg.set("message.timeout.ms", connection.timeout_ms().to_string());

    if let Some(client_id) = connection.client_id() {
        cfg.set("client.id", client_id);
    }

    for (key, value) in connection.additional_config() {
        cfg.set(key, value);
    }

    cfg.create_with_context(EventBusProducerContext)
        .expect("Failed to create Kafka producer")
}

fn ensure_topics(connection: &KafkaConnectionConfig, topology: &KafkaTopologyConfig) {
    if topology.topics().is_empty() {
        return;
    }

    let mut cfg = ClientConfig::new();
    cfg.set("bootstrap.servers", connection.bootstrap_servers());
    if let Some(client_id) = connection.client_id() {
        cfg.set("client.id", client_id);
    }

    match cfg.create::<AdminClient<DefaultClientContext>>() {
        Ok(admin) => {
            let options = AdminOptions::new();
            let topics: Vec<NewTopic> = topology
                .topics()
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
                        warn!(error = %msg, "Topic creation batch failed");
                    }
                }
            }
        }
        Err(err) => warn!(error = %err, "Failed to create Kafka admin client"),
    }
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
    cfg.set("bootstrap.servers", connection.bootstrap_servers());
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
    for (key, value) in connection.additional_config() {
        cfg.set(key, value);
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

                let key = message.key().map(|k| k.to_vec());
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

                let msg = IncomingMessage {
                    topic: message.topic().to_string(),
                    partition: message.partition(),
                    offset: message.offset(),
                    key,
                    payload,
                    timestamp: Instant::now(),
                    headers: headers_map,
                    consumer_group: Some(runtime.group_id.clone()),
                    manual_commit: runtime.manual_commit,
                };

                let mut pending = msg;

                loop {
                    match tx.try_send(pending) {
                        Ok(_) => break,
                        Err(TrySendError::Full(returned)) => {
                            if !running.load(Ordering::Relaxed) {
                                dropped.fetch_add(1, Ordering::Relaxed);
                                break;
                            }

                            tokio::time::sleep(Duration::from_millis(2)).await;
                            pending = returned;
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
