use std::{convert::From, fmt, process, thread};

use std::fmt::{Debug, Display};

use serde::{Deserialize, Serialize};
use std::{
    collections::{HashMap, HashSet},
    error::Error,
    marker::Copy,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use tokio::time::{delay_for};

use rdkafka::{
    client::{ClientContext, DefaultClientContext},
    config::ClientConfig,
    consumer::{
        stream_consumer::{MessageStream, StreamConsumer},
        Consumer, ConsumerContext, DefaultConsumerContext,
    },
    error::{KafkaError, KafkaResult},
    message::{
        BorrowedHeaders, BorrowedMessage, Headers, Message, OwnedHeaders, OwnedMessage, ToBytes,
    },
    producer::{FutureProducer, FutureRecord},
    statistics::Statistics,
    util::Timeout,
    TopicPartitionList, topic_partition_list::TopicPartitionListElem, groups::GroupList, metadata::Metadata,
};

use futures::{future, stream::StreamExt};

use std::{
    env::{self, VarError},
    sync::{
        atomic::{AtomicUsize, Ordering, AtomicBool},
        Arc, Mutex,
    },
};

use byte_unit::Byte;
use std::{ops, iter::Iterator};


use super::metrics;
use metrics::PipelineMetrics;
use ops::{Add, AddAssign};
use std::convert::TryFrom;
// pub struct ProducerContext {
//     pub some_data: i64, // Add some data so that valgrind can check proper allocation
// }

// impl ClientContext for ProducerContext {
//     // fn stats(&self, _: Statistics) {} // Don't print stats
// }

const NANOS_PER_MICRO: u128 = 1_000;
const DEFAULT_MEDATADA_FETCH_TIMEOUT: u64 = 5;
const DEFAULT_SHOW_PROGRESS_INTERVAL_SECS: u64 = 60;
const DEFAULT_CONFIG_MESSAGE_TIMEOUT: &str = "5000";
const DEFAULT_FETCH_INTERVAL_SECS: u64 = 60;
const LOOP_ITERATION_SLEEP: u64 = 1;
const DEFAULT_UPDATE_METRICS_INTERVAL_SECS: u64 = 60;
const DEFAULT_CONSUMER_POLL_INTERVAL: u64 = 100;

pub struct ConsumerTestContext {
    pub _n: i64, // Add data for memory access validation
    pub wakeups: Arc<AtomicUsize>,
}

impl ClientContext for ConsumerTestContext {
    // Access stats
    fn stats(&self, stats: Statistics) {
        let stats_str = format!("{:?}", stats);
        println!("Stats received: {} bytes", stats_str.len());
    }
}

impl ConsumerContext for ConsumerTestContext {
    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        println!("Committing offsets: {:?}", result);
    }

    fn message_queue_nonempty_callback(&self) {
        println!("message_queue_nonempty_callback");
        self.wakeups.fetch_add(1, Ordering::SeqCst);
    }
}

// struct ConsumerBenchmarkStats {
//     messages: Messages,
//     bytes: Bytes,
//     time: Seconds,
// }

// impl ConsumerBenchmarkStats {
//     fn new(messages: u64, bytes: u64, time: Duration) -> ConsumerBenchmarkStats {
//         ConsumerBenchmarkStats {
//             messages: Messages::from(messages),
//             bytes: Bytes::from(bytes),
//             time: Seconds(time),
//         }
//     }

//     fn print(&self) {
//         println!("Received: {}, {}", self.messages, self.bytes);
//         println!("Elapsed:  {} ({}, {})", self.time, self.messages / self.time, self.bytes / self.time)
//     }
// }

// fn get_topic_partitions_count<X: ConsumerContext, C: Consumer<X>>(consumer: &C, topic_name: &str) -> Option<usize> {
//     let metadata = consumer.fetch_metadata(Some(topic_name), Duration::from_secs(30))
//         .expect("Failed to fetch metadata");

//     if metadata.topics().is_empty() {
//         None
//     } else {
//         let partitions = metadata.topics()[0].partitions();
//         if partitions.is_empty() {
//             None  // Topic was auto-created
//         } else {
//             Some(partitions.len())
//         }
//     }
// }

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Config {
    pub clusters: Vec<Cluster>,
    pub clients: Vec<Client>,
    pub routes: Vec<Route>,
    pub observers: Vec<ObserverConfig>,
    pub prometheus: Option<PrometheusConfig>
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Client {
    #[serde(rename(deserialize = "client"))]
    pub name: String,

    #[serde(rename(deserialize = "cluster"))]
    pub cluster: String,

    #[serde(rename(deserialize = "config"))]
    #[serde(default)]
    pub config: HashMap<String, String>,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Cluster {
    pub name: String,
    pub hosts: Vec<String>, // config: Option<String>
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum RepartitioningStrategy {
    #[serde(rename(deserialize = "strict_p2p"))]
    StrictP2P,
    #[serde(rename(deserialize = "random"))]
    Random,
}

impl fmt::Display for RepartitioningStrategy {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "{:}",
            if let RepartitioningStrategy::StrictP2P = &self {
                "strict_p2p"
            } else {
                "random"
            }
        )
    }
}

#[derive(Debug, Serialize, Deserialize, Copy, Clone)]
enum BeginOffset {
    #[serde(rename(deserialize = "earliest"))]
    Earliest,
    #[serde(rename(deserialize = "latest"))]
    Latest,
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Route {
    name: Option<String>,
    upstream_group_id: Option<String>,

    upstream_client: String,
    downstream_client: String,

    upstream_topics: Vec<String>,
    // downstream_topics: Vec<String>,
    downstream_topic: String,

    repartitioning_strategy: RepartitioningStrategy,

    default_begin_offset: Option<BeginOffset>,
    show_progress_interval_secs: Option<u64>,

    update_metrics_interval_secs: Option<u64>,

    upstream_poll_interval_ms: Option<u64>
}

#[derive(Debug, Copy, Clone)]
pub struct DeltaStorage<T>{
    pub current: T,
    pub delta: T
}


impl<T: Display> fmt::Display for DeltaStorage<T> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "DeltaStorage({:}, {:})", self.current, self.delta)
    }
}


impl DeltaStorage<u64> {
    pub fn new(current: u64) -> Self {
        Self::new_with_delta(current, 0)
    }

    pub fn new_with_delta(current: u64, delta: u64) -> Self {
        Self {current, delta}

    }

    pub fn get_delta_and_reset(&mut self) -> u64 {
        let delta = self.delta;
        self.delta = 0;
        delta
    }
}

// TODO: try to use <T: Into<u64>>
impl From<u64> for DeltaStorage<u64> {
    fn from(item: u64) -> Self {
        Self::new(item)
    }
}

impl From<DeltaStorage<u64>> for u64 {
    fn from(item: DeltaStorage<u64>) -> u64 {
        item.current
    }

}


impl Add<u64> for DeltaStorage<u64> {
    type Output = DeltaStorage<u64>;

    fn add(mut self, other: u64) -> Self::Output {
        self.current += other;
        self.delta += other;
        self
    }
}

impl Add<DeltaStorage<u64>> for DeltaStorage<u64> {
    type Output = DeltaStorage<u64>;

    fn add(mut self, other: DeltaStorage<u64>) -> Self::Output {
        DeltaStorage::new_with_delta(self.current + other.current,
                                  self.delta + other.delta)
    }
}


impl AddAssign<u64> for DeltaStorage<u64> {
    fn add_assign(&mut self, other: u64) {
        self.current += other;
        self.delta += other
    }
}



// TODO: realize default trait
pub struct PipelineStats {
    start_ts: SystemTime,
    received: DeltaStorage<u64>,
    transfered: DeltaStorage<u64>,
    cumulative_duration: u64,
    // number_of_bytes: u64,
    keys_bytes: DeltaStorage<u64>,
    payload_bytes: DeltaStorage<u64>,
    headers_bytes: DeltaStorage<u64>,
    last_receive_ts: Option<SystemTime>
}

impl fmt::Display for PipelineStats {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "Stats( messages={:} dur={:?}/{:.3?} volume={:} rate={:}/sec )",
            self.transfered.current,
            self.get_avg_duration(),
            Duration::from_nanos(self.cumulative_duration),
            Byte::from_bytes(self.get_bytes_size() as u128).get_appropriate_unit(true),
            Byte::from_bytes(self.get_data_rate() as u128).get_appropriate_unit(true)
        )
    }
}


pub struct Pipeline {
    id: u64,
    name: String,
    downstream_topic: String,
    upstream_topics: Vec<String>,
    // upstream_client: StreamConsumer,
    upstream_client: ClientConfig,
    downstream_client: FutureProducer<DefaultClientContext>,
    upstream_client_name: String,
    downstream_client_name: String,
    show_progress_interval_secs: Option<u64>,

    last_metrics_update_time: Instant,
    update_metrics_interval_secs: Option<u64>,

    repartitioning_strategy: RepartitioningStrategy,
    stats: PipelineStats,
    metrics: Arc<Mutex<PipelineMetrics>>,
    is_running: Arc<AtomicBool>,
    last_status_update_time: Instant,
    upstream_poll_interval: Option<u64>
    // message_stream: Option<MessageStream<'a, DefaultConsumerContext>>
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct PrometheusConfig {
    pub labels: Option<HashMap<String, String>>,
    pub namespace: Option<String>

}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ObserverConfig {
    client: String,
    group_id: Option<String>,
    topics: Vec<String>,
    show_progress_interval_secs: Option<u64>,
    fetch_timeout_secs: Option<u64>,
    fetch_interval_secs: Option<u64>,
    name: Option<String>,
}

pub struct Observer {
    name: String,
    client: StreamConsumer,
    topics: Vec<String>,
    topics_set: HashSet<String>,
    group_id: Option<String>,
    show_progress_interval_secs: Option<u64>,

    fetch_data_interval_secs: Option<u64>,
    fetch_timeout_secs: Option<u64>,

    last_status_time: Instant,
    last_update_time: Instant,
    last_results: HashMap<String, (i64, i64)>,

    metrics: Arc<Mutex<metrics::ObserverMetrics>>,
}

impl fmt::Debug for Observer {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Observer {{ topics: {:} }}", &self.topics.join(", "))
    }
}

impl PipelineStats {
    pub fn new() -> Self {
        PipelineStats {
            received: DeltaStorage::new(0),
            transfered: DeltaStorage::new(0),
            cumulative_duration: 0,
            start_ts: SystemTime::now(),
            last_receive_ts: None,
            keys_bytes:  DeltaStorage::new(0),
            payload_bytes: DeltaStorage::new(0),
            headers_bytes: DeltaStorage::new(0),
        }
    }

    pub fn update_cumulative(&mut self, value: Duration) {
        self.cumulative_duration += value.as_nanos() as u64;
    }

    pub fn get_avg_duration(&self) -> Duration {
        let duration = if self.transfered.current > 0 {
            // TODO: impl From for self.replicated.current
            self.cumulative_duration / self.transfered.current as u64
        } else {
            0
        };

        return Duration::from_nanos(duration);
    }

    pub fn get_bytes_size(&self) -> u64 {
        (self.keys_bytes + self.payload_bytes + self.headers_bytes).into()
    }

    pub fn get_data_rate(&self) -> u64 {
        if Duration::from_nanos(self.cumulative_duration).as_secs() > 0 {
            self.get_bytes_size() / Duration::from_nanos(self.cumulative_duration).as_secs()
        } else {
            0
        }
    }
}

impl Observer {

    pub fn start(&mut self, is_running: Arc<AtomicBool>) {

        if self.topics.len() > 0 {
            info!(
                "Starting {:} for topics \"{:}\"",
                self.name,
                self.topics.join(", ")
            );
        } else {
            info!("Starting {:} all broker topics", self.name);
        };



        let loop_sleep = Duration::from_secs(LOOP_ITERATION_SLEEP);

        loop {
            if !is_running.load( Ordering::SeqCst){
                info!("{:} received SIGINT, exiting...", self.name);
                break
            }
            self.update_data_if_need();
            self.print_status_if_need();
            thread::sleep(loop_sleep);
        }
    }

    pub fn update_data_if_need(&mut self) {

        let difference = Instant::now().duration_since(self.last_update_time);

        if self.get_fetch_interval().as_secs() > 0 && difference > self.get_fetch_interval() {
            match self.update_current_status() {
                Ok(results) => {
                    self.last_results = results;
                    self.last_update_time = Instant::now();
                },
                Err(text) => {
                    error!("{}", text);
                }
            }

        }
    }

    pub fn print_status_if_need(&mut self) {

        let difference = Instant::now().duration_since(self.last_status_time);

        if self.get_show_progress_interval().as_secs() > 0 && difference > self.get_show_progress_interval() {

            for (name, (count, remaining)) in self.last_results.iter() {
                if let Some(group_id) = self.group_id.clone(){
                    info!("\"{:}\": topic=\"{:}\" group=\"{:}\" messages={:} remaining={:}", self.name, name, group_id, count, remaining);

                }else {
                    info!("\"{:}\": topic \"{:}\" messages={:} remaining={:}", self.name, name, count, remaining);
                }
            }
            self.last_status_time = Instant::now();
        }
    }

    pub fn get_fetch_timeout(&self) -> Duration {
        Duration::from_secs(
            self.fetch_timeout_secs
                .unwrap_or(DEFAULT_MEDATADA_FETCH_TIMEOUT)
        )
    }

    pub fn get_show_progress_interval(&self) -> Duration {
        Duration::from_secs(
            self.show_progress_interval_secs
                .unwrap_or(DEFAULT_SHOW_PROGRESS_INTERVAL_SECS)
        )
    }

    pub fn get_fetch_interval(&self) -> Duration {
        Duration::from_secs(
            self.fetch_data_interval_secs
                .unwrap_or(DEFAULT_FETCH_INTERVAL_SECS)
        )
    }

    pub fn update_current_status(&self) -> Result<HashMap<String, (i64, i64)>, String> {

        let mut results: HashMap<String, (i64, i64)> = HashMap::new();

        let topic: Option<&str> =
            if self.topics.len() == 1 {
                Some(&self.topics[0])
            } else {
                None
        };

        let metadata: Metadata = match self
            .client
            .fetch_metadata(topic, self.get_fetch_timeout()){
                Ok(metadata) => metadata,
                Err(_) => {
                    return Result::Err(String::from("Failed to fetch metadata"))
                }
            };

        for topic in metadata.topics().iter() {
            if self.topics_set.contains(topic.name()) || self.topics_set.len() == 0 {


                let tp_map: HashMap<(String, i32), rdkafka::Offset> = topic.partitions()
                    .iter()
                    .map(|partition_md|{
                        ((topic.name().to_string(), partition_md.id()), rdkafka::Offset::Stored)
                    }).collect();

                let tpl = TopicPartitionList::from_topic_map(&tp_map);
                let commited_offsets = self.client.committed_offsets(tpl, self.get_fetch_timeout()).unwrap_or(TopicPartitionList::new());


                let mut topic_message_count = 0;
                let mut topic_remaining_count = 0;
                for partition in topic.partitions() {

                    let tpl_item: Option<TopicPartitionListElem> = commited_offsets.find_partition(topic.name(), partition.id());

                    let mut current_partition_offset = 0;
                    if let Some(value) = tpl_item {
                        match value.offset(){
                            rdkafka::Offset::Offset(offset) => {
                                current_partition_offset = offset;
                            },
                            _ => {}
                        }
                    }

                    let (low, high) = self
                        .client
                        .fetch_watermarks(
                            topic.name(),
                            partition.id(),
                            Duration::from_secs(1))
                        .unwrap_or((-1, -1));

                    let partition_message_count = high - low;
                    topic_message_count += partition_message_count;

                    let partition_remaining_count = high - current_partition_offset;
                    topic_remaining_count += partition_remaining_count;

                    let labels = [topic.name(), &partition.id().to_string()];

                    match self.metrics.lock() {
                        Ok(guard) => {

                            guard.partition_start_offset.with_label_values(&labels).set(low);

                            guard.partition_end_offset.with_label_values(&labels).set(high);

                            guard.number_of_records_for_partition.with_label_values(&labels).set(partition_message_count);

                            if let Some(group_id) = self.group_id.clone() {
                                let labels = [topic.name(), &partition.id().to_string(), &group_id];
                                guard.commited_offset.with_label_values(&labels).set(current_partition_offset);
                                guard.remaining_by_partition.with_label_values(&labels).set(partition_remaining_count);

                            }
                        },
                        Err(_poisoned) => {
                            error!("Can't acquire metrics lock for topic={:} and partition={:}", topic.name(), &partition.id());
                        },


                    };

                }

                results.insert(topic.name().to_string(), (topic_message_count, topic_remaining_count));

                match self.metrics.lock() {
                    Ok(guard) => {
                        let since_the_epoch = SystemTime::now()
                            .duration_since(UNIX_EPOCH)
                            .expect("Time went backwards");
                        guard.number_of_records_total.with_label_values(&[topic.name()]).set(topic_message_count);
                        guard.last_fetch_ts.with_label_values(&[topic.name()]).set(since_the_epoch.as_secs_f64());

                        if let Some(group_id) = self.group_id.clone() {
                            guard.remaining_for_topic.with_label_values(&[topic.name(), &group_id]).set(topic_remaining_count);
                        }

                    },
                    Err(_) => {
                         error!("Can't acquire metrics lock for topic={:}", topic.name())
                    }
                }
            }
        }
        Ok(results)
    }
}

impl Pipeline {

    // Subscribe upstream client to topics
    pub fn get_upstream_stream(&self) -> StreamConsumer {
        let topics: Vec<&str> = self.upstream_topics.iter().map(|x| &**x).collect();

        let client: StreamConsumer = self.upstream_client.create().expect("Can't create consumer");

        info!("Subscribe upstream topics {:} [ {:} ] -> {:} [ {:} ]",
              self.upstream_client_name,
              self.upstream_topics.join(","),
              self.downstream_client_name,
              self.downstream_topic);
        client.subscribe(&topics)
            .expect("Can't subscribe to specified topics");

        client

    }

    pub async fn start(&mut self) {
        info!(
            "Starting replication {:} {:} [ {:} ] -> {:} [ {:} ] strategy={:}",
            self.name,
            self.upstream_client_name,
            self.upstream_topics.join(","),
            self.downstream_client_name,
            self.downstream_topic,
            self.repartitioning_strategy
        );

        match self.metrics.lock() {
            Ok(guard) =>{
                // TODO: add some codes?
                // TODO: move labels values to method
                 let start_ts = self.stats.start_ts
                    .duration_since(UNIX_EPOCH)
                    .expect("Time went backwards").as_secs_f64();
                guard.start_ts.with_label_values(&self.get_metrics_labels()).set(start_ts)
            },
            Err(_poisoned) => {
                error!("Can't acquire metrics for {:} {:} [ {:} ] -> {:} [ {:} ]",
                       self.name,
                       self.upstream_client_name,
                       self.upstream_topics.join(","),
                       self.downstream_client_name,
                       self.downstream_topic);
            }
        }

        let upstream_client = self.get_upstream_stream();
        let mut stream = upstream_client.start_with(
            Duration::from_millis(self.upstream_poll_interval.unwrap_or(DEFAULT_CONSUMER_POLL_INTERVAL)), true);

        while self.is_running.load(Ordering::SeqCst) {

            if let Some(message) = stream.next().await {
                match message {
                    Err(KafkaError::NoMessageReceived) => {
                        // delay_for(Duration::from_secs(1)).await;
                        // error!("Received error {:?}", KafkaError::NoMessageReceived);
                    },
                    Err(error) => {
                        error!("Received error {:?}", error);

                        match self.metrics.lock() {
                            Ok(guard) =>{
                                // TODO: add some codes?
                                // TODO: move labels values to method
                                guard.errors_total.with_label_values(&self.get_metrics_labels()).inc_by(1)
                            },
                            Err(_poisoned) => {
                                error!("Can't acquire metrics for {:} {:} [ {:} ] -> {:} [ {:} ]",
                                       self.name,
                                       self.upstream_client_name,
                                       self.upstream_topics.join(","),
                                       self.downstream_client_name,
                                       self.downstream_topic);
                            }
                        }
                    }
                    Ok(message_content) => {
                        self.pump_message(message_content);
                    }
                };

            };
            self.update_metrics_if_need();
            self.print_status_if_need();
        }

        info!("Unsubscribe upstream topics and flush producer queue {:} {:} [ {:} ] -> {:} [ {:} ]",
              self.name,
              self.upstream_client_name,
              self.upstream_topics.join(","),
              self.downstream_client_name,
              self.downstream_topic);

        upstream_client.unsubscribe();

        self.downstream_client.flush(Duration::from_secs(10));

        self.update_metrics();

        info!(
            "Finishing replication {:} {:} [ {:} ] -> {:} [ {:} ]",
            self.name,
            self.upstream_client_name,
            self.upstream_topics.join(","),
            self.downstream_client_name,
            self.downstream_topic
        );
    }

    fn update_metrics_if_need(&mut self){
        let update_metrics_interval_secs = self.update_metrics_interval_secs.unwrap_or(DEFAULT_UPDATE_METRICS_INTERVAL_SECS);

        let difference = Instant::now().duration_since(self.last_metrics_update_time);

        if update_metrics_interval_secs > 0 && difference.as_secs() > update_metrics_interval_secs {

            self.update_metrics();

        }
    }

    pub fn update_metrics(&mut self){
        self.last_metrics_update_time = Instant::now();

        let labels: [&str; 3] = [&self.name, &self.upstream_client_name, &self.downstream_client_name];

        match self.metrics.lock() {
            Ok(guard) => {

                guard.received_total.with_label_values(&labels).inc_by(i64::try_from(self.stats.received.get_delta_and_reset()).unwrap_or(0));
                guard.transfered_total.with_label_values(&labels).inc_by(i64::try_from(self.stats.transfered.get_delta_and_reset()).unwrap_or(0));

                let headers_bytes = self.stats.headers_bytes.get_delta_and_reset();
                let keys_bytes = self.stats.keys_bytes.get_delta_and_reset();
                let payload_bytes = self.stats.payload_bytes.get_delta_and_reset();
                let total_bytes = headers_bytes + keys_bytes + payload_bytes;

                guard.keys_bytes.with_label_values(&labels).inc_by(i64::try_from(keys_bytes).unwrap_or(0));
                guard.headers_bytes.with_label_values(&labels).inc_by(i64::try_from(headers_bytes).unwrap_or(0));

                guard.payload_bytes.with_label_values(&labels).inc_by(i64::try_from(payload_bytes).unwrap_or(0));
                guard.total_bytes.with_label_values(&labels).inc_by(i64::try_from(total_bytes).unwrap_or(0));
                guard.avg_transfer_duration.with_label_values(&labels).set(self.stats.get_avg_duration().as_secs_f64());

                if let Some(value) = self.stats.last_receive_ts {

                    let last_receive_ts = value
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards").as_secs_f64();
                    guard.last_receive_ts.with_label_values(&labels).set(last_receive_ts);

                }

            },
            Err(_poisoned) => {
                error!("Can't acquire metrics for {:} {:} [ {:} ] -> {:} [ {:} ]",
                       self.name,
                       self.upstream_client_name,
                       self.upstream_topics.join(","),
                       self.downstream_client_name,
                       self.downstream_topic);
            }
        }
    }

    pub fn get_metrics_labels(&self) -> [&str; 3]{
        [&self.name, &self.upstream_client_name, &self.downstream_client_name]
    }

    pub fn print_status(&mut self) {
        self.last_status_update_time = Instant::now();

        debug!(
            "{:} status {:} [{:}] -> {:} [{:}]: {:}",
            self.name,
            self.upstream_client_name,
            self.upstream_topics.join(","),
            self.downstream_client_name,
            self.downstream_topic,
            self.stats,
            // &received,
            // &replicated,
            // Duration::from_nanos(avg_duration as u64)
        )

    }

    pub fn print_status_if_need(&mut self) {
        let show_progress_interval_secs = self.show_progress_interval_secs.unwrap_or(0);

        let difference = Instant::now().duration_since(self.last_status_update_time);

        if show_progress_interval_secs > 0 && difference.as_secs() > show_progress_interval_secs {

            self.print_status();
        };
    }

    pub fn pump_message(&mut self, message_content: BorrowedMessage) {
        let received_time = Instant::now();
        self.stats.last_receive_ts = Some(SystemTime::now());
        self.stats.received += 1;

        let unwraped_message = &message_content;

        let input_payload = &unwraped_message.payload();
        let input_key = &unwraped_message.key();
        let input_headers = &unwraped_message.headers();

        let mut record: FutureRecord<'_, [u8], [u8]> =
            FutureRecord::to(&self.downstream_topic);

        if (&input_payload).is_some() == true {
            record = record.payload(&input_payload.unwrap());
        };

        if (&input_key).is_some() == true {
            record = record.key(&input_key.unwrap());
        };

        let mut headers_len = 0;

        // TODO: maybe try convert BorrowedHeaders ?
        if input_headers.is_some() == true {
            let mut new_headers = OwnedHeaders::new();

            for i in 0..input_headers.unwrap().count() {
                let header = input_headers.unwrap().get(i);
                if let Some((key, value)) = header {
                    new_headers = new_headers.add(key, value);
                    headers_len += key.len() + value.len();
                }
            }
            record = record.headers(new_headers);
        };

        if let RepartitioningStrategy::StrictP2P = &self.repartitioning_strategy {
            record = record.partition(unwraped_message.partition());
        }

        // &self.downstream_client.send(record, Duration::from_secs(0));

        self.downstream_client.send(record, 0);

        // Update statistics
        self.stats.transfered += 1;
        self.stats.keys_bytes += unwraped_message.key_len() as u64;
        self.stats.payload_bytes += unwraped_message.payload_len() as u64;
        self.stats.headers_bytes += headers_len as u64;

        let duration = Instant::now().duration_since(received_time);

        self.stats.update_cumulative(duration);

    }
}

impl Config {
    pub fn get_cluster(&self, name: &str) -> Result<&Cluster, String> {
        // debug!("Get cluster config by name: {:}", &name);

        for cluster in self.clusters.iter() {
            if cluster.name == name {
                return Ok(&cluster);
            };
            // self.clusters_map.insert(&cluster.name, &cluster);
        }
        Result::Err(format!("Not found cluster for given name: {:}", name))
    }

    pub fn get_client(&self, name: &str) -> Result<&Client, String> {
        // debug!("Get client config by name: {:}", &name);

        for client in self.clients.iter() {
            if client.name == name {
                return Ok(&client);
            };
        }

        Result::Err(format!("Not found client for given name: {:}", name))
    }

    pub fn create_client_config(&self, name: &str, group_id: Option<&str>) -> ClientConfig {
        let client = self.get_client(&name).unwrap();
        let cluster = self.get_cluster(&client.cluster).unwrap();

        let mut config = ClientConfig::new();

        info!(
            "Configure client \"{:}\" bootstrap servers: {:}",
            &name,
            &cluster.hosts.join(",")
        );
        config
            .set("bootstrap.servers", &cluster.hosts.join(","))
            .set("message.timeout.ms", DEFAULT_CONFIG_MESSAGE_TIMEOUT);

        if let Some(v) = group_id {
            info!("Configure client \"{:}\" group: {:}", &name, v);
            config.set("group.id", v);
        };

        for (key, value) in &client.config {
            info!(
                "Configure client \"{:}\" option: {:}={:}",
                &name, &key, &value
            );
            config.set(key, value);
        }

        config
    }

    pub fn check_partitions(
        &self,
        upstream_config: &ClientConfig,
        downstream_config: &ClientConfig,
        upstream_topics: HashSet<String>,
        downstream_topic: String,
    ) -> Result<(), String> {
        let upstream_consumer: StreamConsumer =
            upstream_config.create().expect("Can't create consumer.");

        let downstream_consumer: StreamConsumer =
            downstream_config.create().expect("Can't create consumer.");

        let downstream_metadata = downstream_consumer
            .fetch_metadata(Some(&downstream_topic), Duration::from_secs(10))
            .expect("Failed to fetch metadata");
        let upstream_metadata = upstream_consumer
            .fetch_metadata(None, Duration::from_secs(10))
            .expect("Failed to fetch metadata");

        let md_downstream_topic_size = downstream_metadata
            .topics()
            .iter()
            .filter(|topic| topic.name() == downstream_topic)
            .map(|topic| {
                // (topic.name(), topic.partitions().len())
                topic.partitions().len()
            })
            .next()
            .expect(&format!("Not found topic: {:}", &downstream_topic));

        let md_upstream_topics = upstream_metadata
            .topics()
            .iter()
            .filter(|topic| upstream_topics.contains(topic.name()))
            .map(|topic| (topic.name(), topic.partitions().len()));

        for (name, size) in md_upstream_topics {
            if size != md_downstream_topic_size {
                return Result::Err(format!("Upstream ({:}) and downstream ({:}) topics have different number of partitions.", name, downstream_topic));
                //error!("Upstream and downstream topics have different number of partitions.");
                //process::abort();
            }
        }

        Ok(())
    }

    pub fn get_pipeline(&self, index: usize, metrics: Arc<Mutex<PipelineMetrics>>, is_running: Arc<AtomicBool>) -> Pipeline {
        let route = &self.routes[index];

        let group_id: Option<&str> = (&route.upstream_group_id).as_deref();

        let upstream_config: ClientConfig =
            self.create_client_config(&route.upstream_client, group_id);
        let downstream_config: ClientConfig =
            self.create_client_config(&route.downstream_client, Option::None);

        match route.repartitioning_strategy {
            RepartitioningStrategy::StrictP2P => {
                self.check_partitions(
                    &upstream_config,
                    &downstream_config,
                    route.upstream_topics.clone().iter().cloned().collect(),
                    route.downstream_topic.clone(),
                )
                .expect("Upstream and downstream topics have different number of partitions");
            }
            _ => {}
        };

        let mut upstream_client = upstream_config;
        let downstream_client = downstream_config;

        match &route.default_begin_offset {
            Some(BeginOffset::Earliest) => {
                &upstream_client.set("auto.offset.reset", "earliest");
            }
            Some(BeginOffset::Latest) => {
                &upstream_client.set("auto.offset.reset", "latest");
            }
            _ => {}
        };

        Pipeline {
            id: index as u64,
            name: route.name.clone().unwrap_or(format!("Pipeline #{:}", index)),
            repartitioning_strategy: route.repartitioning_strategy,
            show_progress_interval_secs: route.show_progress_interval_secs.clone(),
            upstream_client_name: route.upstream_client.clone(),
            downstream_client_name: route.downstream_client.clone(),
            downstream_topic: route.downstream_topic.clone(),
            upstream_topics: route.upstream_topics.clone(),
            // upstream_client: upstream_client.create().expect("Can't create consumer"),
            upstream_client: upstream_client,
            downstream_client: downstream_client
                .create()
                .expect("Failed to create producer"), // repartitioning_strategy: route.repartitioning_strategy.copy()

            last_metrics_update_time: Instant::now(),
            last_status_update_time: Instant::now(),
            update_metrics_interval_secs: route.show_progress_interval_secs.clone(),
            stats: PipelineStats::new(),
            metrics,
            is_running,
            upstream_poll_interval: route.upstream_poll_interval_ms.clone(),
            // message_stream: None
        }
    }

    pub fn get_pipelines(&self, metrics: Arc<Mutex<PipelineMetrics>>, is_running: Arc<AtomicBool>) -> Vec<Pipeline> {
        let rules: Vec<_> = self
            .routes
            .iter()
            .enumerate()
            .map(|(pos, _)| self.get_pipeline(pos, metrics.clone(), is_running.clone()))
            .collect();
        rules
    }

    pub fn get_observers(&self, metrics: Arc<Mutex<metrics::ObserverMetrics>>) -> Vec<Observer> {
        let observers: Vec<_> = self
            .observers
            .iter()
            .enumerate()
            .map(|(i, x)| {

                let client: ClientConfig = self.create_client_config(&x.client, (x.group_id).as_deref());

                Observer {
                    client: client.create().expect("Can't create consumer"),
                    group_id: x.group_id.clone(),
                    topics: x.topics.clone(),
                    topics_set: x.topics.iter().cloned().collect(),
                    show_progress_interval_secs: x.show_progress_interval_secs.clone(),
                    fetch_data_interval_secs: x.fetch_interval_secs.clone(),
                    fetch_timeout_secs: x.fetch_timeout_secs.clone(),
                    name: if let Some(v) = &x.name {
                        v.clone()
                    } else {
                        format!("Observer #{:}", i)
                    },

                    metrics: metrics.clone(),
                    last_results: HashMap::new(),

                    last_status_time: Instant::now(),
                    last_update_time: Instant::now()

                }
            })
            .collect();

        observers
    }
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn it_works() {
//         assert_eq!(2 + 2, 4);
//     }
// }
