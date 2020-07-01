use serde_json::{self, Result as JsonResult, Value as JsonValue};

use replicator::*;

use serde::{Deserialize, Serialize};

use serde_yaml::{Result as YamlResult, Value as YamlValue};

use tokio::prelude::*;

use std::{thread, time};

use rdkafka::{
    client::{ClientContext, DefaultClientContext},
    config::ClientConfig,
    consumer::{
        stream_consumer::{MessageStream, StreamConsumer},
        Consumer, ConsumerContext, DefaultConsumerContext,
    },
    error::{KafkaError, KafkaResult, RDKafkaError},
    message::{BorrowedMessage, Headers, Message, OwnedHeaders, OwnedMessage, ToBytes},
    producer::{FutureProducer, FutureRecord},
    statistics::Statistics,
    util::Timeout,
    TopicPartitionList,
};

use futures::{future, stream::StreamExt};

use std::iter::Iterator;

#[macro_use]
extern crate log;

use std::{
    env::{self, VarError},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

// $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list=127.0.0.1:9092 --topic=test_topic
// $KAFKA_HOME/bin/kafka-console-consumer.sh --zookeeper=$KAFKA_ZOOKEEPER_CONNECT --from-beginning --topic=topic2

// replicator-zookeeper-1.test:2181
//   replicator-kafka-1.test:9092
//    replicator-kafka-2.test:9092

// $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list=replicator-kafka-1.test:9092 --topic=topic1

// $KAFKA_HOME/bin/kafka-console-producer.sh --broker-list=replicator-kafka-2.test:9092 --topic=test_topic

// kafkacat -b mybroker -t syslog

// $ echo "hello there" | kafkacat -b replicator-kafka-1.test -H "header1=header value" -H "nullheader" -H "emptyheader=" -H "header1=duplicateIsOk"

static CONFIG_CONTENT: &'static str = r#"
# kafka_clusters:
#   cluster_1:
#     - 'replicator-kafka-1:9092'
#     - 'replicator-kafka-1:9092'
#   cluster_2:
#     - 'replicator-kafka-2:9092'

clusters:
  - name: cluster_1
    hosts:
      - kafka_replicator_replicator_kafka_1_1:9092
      # - 'replicator-kafka-1:9092'
      # - 'replicator-kafka-1:9092'
  - name: cluster_2
    hosts:
      # - 'replicator-kafka-2:9092'
      - kafka_replicator_replicator_kafka_2_1:9092

clients:
  - client: cl_1_client_1
    cluster: cluster_1
    config: # optional
       message.timeout.ms: 5000
       # auto.offset.reset: earliest
  - client: cl_2_client_1
    cluster: cluster_2

routes:
  - upstream_client: cl_1_client_1
    # downstream_client: cl_1_client_1
    downstream_client: cl_2_client_1
    upstream_topics:
      - 'topic1'
    downstream_topic: 'topic2'
    downstream_topics:
      - 'topic2'
    repartitioning_strategy: random # strict_p2p | random
    upstream_cg_id: group_1
    upstream_group_id: group_2

  - upstream_client: cl_2_client_1
    downstream_client: cl_1_client_1
    upstream_topics:
      - 'topic2'
    downstream_topic: 'topic3'
    downstream_topics:
      - 'topic2'
    repartitioning_strategy: strict_p2p # strict_p2p | random
    default_begin_offset: earliest # optional
    upstream_group_id: group_2

watchers:
  - client: cl_1_client_1

    topics:
      - 'topic1'
      - 'topic2'
    fetch_timeout_secs: 20

  - client: cl_2_client_1
    topic: 'topic3'
    topics:
      - 'topic2'

  - client: cl_1_client_1
    topic: 'topic1'
    topics: []

"#;

#[test]
fn test_config() -> YamlResult<()> {
    // println!("{:}", CONFIG_CONTENT);

    let repl_config: config::Config = serde_yaml::from_str(CONFIG_CONTENT)?;
    // dbg!(&repl_config);

    assert_eq!(repl_config.clusters.len(), 2);
    assert_eq!(repl_config.clients.len(), 2);
    assert_eq!(repl_config.routes.len(), 2);

    let cluster_1: &config::Cluster = repl_config.get_cluster("cluster_1").unwrap();
    assert_eq!(cluster_1.name, "cluster_1".to_string());

    let cluster_not_found: Result<&config::Cluster, String> = repl_config.get_cluster("invalid");
    assert_eq!(cluster_not_found.is_err(), true);

    let client_1: &config::Client = repl_config.get_client("cl_1_client_1").unwrap();
    assert_eq!(client_1.name, "cl_1_client_1".to_string());

    let client_not_found: Result<&config::Client, String> = repl_config.get_client("invalid");
    assert_eq!(client_not_found.is_err(), true);

    Ok(())
}

async fn main() {
    env_logger::init();

    let repl_config: config::Config = serde_yaml::from_str(CONFIG_CONTENT).unwrap();

    let config: ClientConfig = repl_config.create_client_config("cl_1_client_1", None);

    dbg!(&repl_config);

    let replication_rule = repl_config.get_route_clients(0);
    replication_rule.start().await;
}


// #[test]
// #[tokio::test]
// async fn test_upstream_client_create() {
//     dbg!(main().await);
//     // main().await;
// }


// #[test]
#[test]
fn test_watchers() {
    env_logger::init();

    let repl_config: config::Config = serde_yaml::from_str(CONFIG_CONTENT).unwrap();

    // dbg!(&repl_config);

    for watcher in repl_config.get_watchers() {
        watcher.start();
    }
}
