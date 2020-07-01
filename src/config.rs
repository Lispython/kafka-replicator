use std::convert::From;
use std::fmt;
use std::thread;
use std::process;


use std::fmt::{ Debug, Display};

use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    collections::HashSet,
    error::Error,
    marker::Copy,
    time::{Duration, SystemTime},
};

use rdkafka::{
    client::{ClientContext, DefaultClientContext},
    config::ClientConfig,
    consumer::{
        stream_consumer::{MessageStream, StreamConsumer},
        Consumer, ConsumerContext, DefaultConsumerContext,
    },
    error::KafkaResult,
    message::{
        BorrowedHeaders, BorrowedMessage, Headers, Message, OwnedHeaders, OwnedMessage, ToBytes,
    },
    producer::{FutureProducer, FutureRecord},
    statistics::Statistics,
    util::Timeout,
    TopicPartitionList,
};

use futures::{future, stream::StreamExt};

use std::{
    env::{self, VarError},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use std::iter::Iterator;

// pub struct ProducerContext {
//     pub some_data: i64, // Add some data so that valgrind can check proper allocation
// }

// impl ClientContext for ProducerContext {
//     // fn stats(&self, _: Statistics) {} // Don't print stats
// }

const NANOS_PER_MICRO: u128 = 1_000;
const DEFAULT_MEDATADA_FETCH_TIMEOUT: u64 = 5;
const DEFAULT_SHOW_PROGRESS_EVERY_SECS: u64 = 60;
const DEFAULT_CONFIG_MESSAGE_TIMEOUT: &str = "5000";

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
    pub watchers: Vec<WatcherConfig>,
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
        write!(f, "{:}", if let RepartitioningStrategy::StrictP2P = &self {
            "strict_p2p"
        } else {
            "random"
        })
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
    upstream_group_id: Option<String>,

    upstream_client: String,
    downstream_client: String,

    upstream_topics: Vec<String>,
    // downstream_topics: Vec<String>,
    downstream_topic: String,

    repartitioning_strategy: RepartitioningStrategy,

    default_begin_offset: Option<BeginOffset>,
    progress_every_secs: Option<u64>,
}

pub struct ReplicationRule {
    downstream_topic: String,
    upstream_topics: Vec<String>,
    upstream_client: StreamConsumer,
    downstream_client: FutureProducer<DefaultClientContext>,
    upstream_client_name: String,
    downstream_client_name: String,
    show_progress_every_secs: Option<u64>,
    repartitioning_strategy: RepartitioningStrategy
}

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct WatcherConfig {
    client: String,
    topics: Vec<String>,
    show_progress_every_secs: Option<u64>,
    fetch_timeout_secs: Option<u64>,
    name: Option<String>
}



pub struct Watcher {
    client: StreamConsumer,
    topics: Vec<String>,
    topics_set: HashSet<String>,
    show_progress_every_secs: Option<u64>,
    fetch_timeout_secs: Option<u64>,
    name: String
}

impl fmt::Debug for Watcher {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Watcher {{ topics: {:} }}", &self.topics.join(", "))
    }
}


impl Watcher {

    pub fn start(&self) {
        if self.topics.len() > 0 {
            info!("Starting {:} for topics \"{:}\"", self.name, self.topics.join(", "));
        }
        else {
            info!("Starting {:} all broker topics", self.name);
        };

        let timeout: Duration =  Duration::from_secs(self.fetch_timeout_secs.unwrap_or(DEFAULT_MEDATADA_FETCH_TIMEOUT));

        let topic: Option<&str> =  if self.topics.len() == 1 {
            Some(&self.topics[0])
        } else {
            None
        };

        let progress_timeout = Duration::from_secs(self.show_progress_every_secs.unwrap_or(DEFAULT_SHOW_PROGRESS_EVERY_SECS));

        loop {
            self.print_current_status(topic.clone(), timeout.clone());

            thread::sleep(progress_timeout);
        }

    }
    pub fn print_current_status(&self, topic: Option<&str>, timeout: Duration){
        let metadata = self.client
            .fetch_metadata(topic, timeout)
            .expect("Failed to fetch metadata");

        for topic in metadata.topics().iter(){
            if self.topics_set.contains(topic.name()) || self.topics_set.len() == 0{

                let mut message_count = 0;
                for partition in topic.partitions(){

                    let (low, high) = self.client
                        .fetch_watermarks(topic.name(), partition.id(), Duration::from_secs(1))
                        .unwrap_or((-1, -1));

                    message_count += high - low;

                }

                info!("\"{:}\" messages status for topic \"{:}\": {:}", self.name, topic.name(), message_count);
            }
        };
    }
}

impl ReplicationRule {
    pub async fn start(&self) {

        let repartitioning_strategy = if let RepartitioningStrategy::StrictP2P = &self.repartitioning_strategy {
            true
        } else {
            false
        };

        info!(
            "Starting replication {:} [ {:} ] -> {:} [ {:} ] strategy={:}",
            &self.upstream_client_name,
            &self.upstream_topics.join(","),
            &self.downstream_client_name,
            &self.downstream_topic,
            &self.repartitioning_strategy
        );

        let topics: &Vec<&str> = &self.upstream_topics.iter().map(|x| &**x).collect();

        &self.upstream_client
            .subscribe(topics)
            .expect("Can't subscribe to specified topics");

        let mut stream = (&self.upstream_client).start_with(Duration::from_millis(100), true);

        let mut received: u64 = 0;
        let mut replicated: u64 = 0;
        let mut cumulative_duration: u64 = 0;
        let mut avg_duration: u64 = 0;
        let mut last_info_time = SystemTime::now();

        let progress_every_secs = self.show_progress_every_secs.unwrap_or(0);

        while let Some(message) = stream.next().await {
            let difference = SystemTime::now()
                .duration_since(last_info_time)
                .expect("Clock may have gone backwards");

            if progress_every_secs > 0 && difference.as_secs() > progress_every_secs {
                if replicated > 0 {
                    avg_duration = cumulative_duration / replicated;
                }

                last_info_time = SystemTime::now();

                debug!("Current progress for {:} [{:}] -> {:} [{:}]: received={:} replicated={:} avg_duration={:?}",
                       &self.upstream_client_name,
                       &self.upstream_topics.join(","),
                       &self.downstream_client_name,
                       &self.downstream_topic,
                       &received,
                       &replicated,
                       Duration::from_nanos(avg_duration as u64))
            };

            match message {
                Err(_e) => {
                    // println!(">>>>> {:}", e);
                }
                Ok(message_content) => {
                    let received_time = SystemTime::now();

                    received += 1;
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
                        record = record.key(&input_key.unwrap())
                    };

                    // TODO: maybe try convert BorrowedHeaders ?
                    if input_headers.is_some() == true {
                        let mut new_headers = OwnedHeaders::new();

                        for i in 0..input_headers.unwrap().count() {
                            let header = input_headers.unwrap().get(i);
                            if let Some((key, value)) = header {
                                new_headers = new_headers.add(key, value);
                            }
                        }
                        record = record.headers(new_headers);
                    };

                    if repartitioning_strategy == true {
                        record = record.partition(unwraped_message.partition());
                    }

                    &self.downstream_client.send(record, 0);

                    replicated += 1;

                    let duration = SystemTime::now()
                        .duration_since(received_time)
                        .expect("Clock may have gone backwards");

                    cumulative_duration += duration.as_nanos() as u64;
                }
            }

            // upstream_client.commit_message(&m, CommitMode::Async).unwrap();
            // downstream_producer.flush(Timeout::Never);
        }

        info!(
            "Finishing replication {:} [ {:} ] -> {:} [ {:} ]",
            &self.upstream_client_name,
            &self.upstream_topics.join(","),
            &self.downstream_client_name,
            &self.downstream_topic
        );
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

    pub fn check_partitions(&self, upstream_config: &ClientConfig, downstream_config: &ClientConfig,
                            upstream_topics: HashSet<String>, downstream_topic: String) -> Result<(), String>{

        let upstream_consumer: StreamConsumer = upstream_config.create().expect("Can't create consumer.");

        let downstream_consumer: StreamConsumer = downstream_config.create().expect("Can't create consumer.");


        let downstream_metadata = downstream_consumer.fetch_metadata(Some(&downstream_topic), Duration::from_secs(10)).expect("Failed to fetch metadata");
        let upstream_metadata = upstream_consumer.fetch_metadata(None, Duration::from_secs(10)).expect("Failed to fetch metadata");

        let md_downstream_topic_size = downstream_metadata.topics().iter()
            .filter(|topic| {topic.name() == downstream_topic})
            .map(|topic| {
                // (topic.name(), topic.partitions().len())
                topic.partitions().len()
            }).next().expect(&format!("Not found topic: {:}", &downstream_topic));


        let md_upstream_topics = upstream_metadata.topics()
            .iter()
            .filter(|topic| {upstream_topics.contains(topic.name())})
            .map(|topic| {
                (topic.name(), topic.partitions().len())});

        for (name, size) in md_upstream_topics {
            if size != md_downstream_topic_size {
                return Result::Err(format!("Upstream ({:}) and downstream ({:}) topics have different number of partitions.", name, downstream_topic));
                //error!("Upstream and downstream topics have different number of partitions.");
                //process::abort();
            }
        }


        Ok(())
    }

    pub fn get_route_clients(&self, index: usize) -> ReplicationRule {
        let route = &self.routes[index];

        let group_id: Option<&str> = (&route.upstream_group_id).as_deref();

        let upstream_config: ClientConfig =
            self.create_client_config(&route.upstream_client, group_id);
        let downstream_config: ClientConfig =
            self.create_client_config(&route.downstream_client, Option::None);

        match route.repartitioning_strategy {
            RepartitioningStrategy::StrictP2P => {
                self.check_partitions(&upstream_config, &downstream_config,
                                      route.upstream_topics.clone().iter().cloned().collect(),
                                      route.downstream_topic.clone()).expect("Upstream and downstream topics have different number of partitions");
            },
            _=> {}
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

        ReplicationRule {
            repartitioning_strategy: route.repartitioning_strategy,
            show_progress_every_secs: route.progress_every_secs.clone(),
            upstream_client_name: route.upstream_client.clone(),
            downstream_client_name: route.downstream_client.clone(),
            downstream_topic: route.downstream_topic.clone(),
            upstream_topics: route.upstream_topics.clone(),
            upstream_client: upstream_client.create().expect("Can't create consumer"),
            downstream_client: downstream_client
                .create()
                .expect("Failed to create producer"), // repartitioning_strategy: route.repartitioning_strategy.copy()
        }
    }

    pub fn get_routes(&self) -> Vec<ReplicationRule> {
        let rules: Vec<_> = self
            .routes
            .iter()
            .enumerate()
            .map(|(pos, _)| self.get_route_clients(pos))
            .collect();
        rules
    }

    pub fn get_watchers(&self) -> Vec<Watcher> {

        let watchers: Vec<_> = self.watchers.iter().enumerate().map(|(i, x)| {
            let client: ClientConfig = self.create_client_config(&x.client, None);

            Watcher {
                client: client.create().expect("Can't create consumer"),
                topics: x.topics.clone(),
                topics_set: x.topics.iter().cloned().collect(),
                show_progress_every_secs: x.show_progress_every_secs.clone(),
                fetch_timeout_secs: x.fetch_timeout_secs.clone(),
                name: if let Some(v) = &x.name {
                    v.clone()
                }
                else {
                    format!("Observer #{:}", i)
                }
            }
        }).collect();

        watchers
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
