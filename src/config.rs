use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
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
    show_progress_every_secs: Option<u64>, // repartitioning_strategy: RepartitioningStrategy
}

impl ReplicationRule {
    pub async fn start(&self) {
        info!(
            "Starting replication {:} [ {:} ] -> {:} [ {:} ]",
            &self.upstream_client_name,
            &self.upstream_topics.join(","),
            &self.downstream_client_name,
            &self.downstream_topic
        );

        let topics: &Vec<&str> = &self.upstream_topics.iter().map(|x| &**x).collect();

        &self
            .upstream_client
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

                    // if &self.repartitioning_strategy == RepartitioningStrategy.StrictP2P {
                    //     record = record.partition(&unwraped_message.partition())
                    // }

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
        debug!("Create common kafka client");

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
            .set("message.timeout.ms", "5000");

        if let Some(v) = group_id {
            info!("Configure client \"{:}\" group: {:}", &name, v);
            config.set("group.id", v);
        };

        for (key, value) in &client.config {
            info!(
                "Configure client \" {:}\" option: {:}={:}",
                &name, &key, &value
            );
            config.set(key, value);
        }

        config
    }

    pub fn get_route_clients(&self, index: usize) -> ReplicationRule {
        let route = &self.routes[index];

        let group_id: Option<&str> = (&route.upstream_group_id).as_deref();

        let upstream_config: ClientConfig =
            self.create_client_config(&route.upstream_client, group_id);
        let downstream_config: ClientConfig =
            self.create_client_config(&route.downstream_client, Option::None);

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
}

// #[cfg(test)]
// mod tests {
//     use super::*;

//     #[test]
//     fn it_works() {
//         assert_eq!(2 + 2, 4);
//     }
// }
