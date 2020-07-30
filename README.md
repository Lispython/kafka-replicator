# Kafka replicator

**Kafka Replicator** is an easy to use tool for copying data between two Apache Kafka clusters with configurable re-partitionning strategy.

Data will be read from topics in the origin cluster and written to a topic/topics in the destination cluster according config rules.


# Features

Lets start with an overview of features that exist in kafka-replicator:

  * [x] **Data replication:** Real-time event streaming between Kafka clusters and data centers;
  * [ ] **Schema replication:** Copy schema from source cluster to destination;
  * [x] **Flexible topic selection:** Select topics with configurable config;
  * [ ] **Auto-create topics:** Destination topics are automatically created for strict_p2p strategy;
  * [x] **Stats:** The tool shows replication status;
  * [x] **Monitoring:** Kafka replicator exports stats via prometheus.
  * [ ] **Cycle detection**



# Use cases

  * Replicate data between Kafka clusters;
  * Aggregate record from several topics and put them into one;
  * Extend bandwidth for exist topic via repartitioning strategy.


# Installation


### System dependencies

``` shell
libsasl2-dev
libssl-dev
```


## Install from crates.io


If you have the Rust toolchain already installed on your local system.

``` shell
rustup update stable
cargo install kafka-replicator
```


## Compile and run it from sources

Clone the repository and change it to your working directory.

```shell
git clone https://github.com/lispython/kafka-replicator.git
cd kafka-replicator

rustup override set stable
rustup update stable
cargo install
```


# Usage

``` shell
RUST_LOG=info kafka-replicator /path/to/config.yml
```

## Run it using Docker


``` shell
sudo docker run -it -v /replication/:/replication/ -e RUST_LOG=info lispython/kafka_replicator:latest kafka-replicator /replication/config.yml
```

### Example config


``` yaml
clusters:
  - name: cluster_1
    hosts:
      - replicator-kafka-1:9092
      - replicator-kafka-1:9092
  - name: cluster_2
    hosts:
      - replicator-kafka-2:9092

clients:
  - client: cl_1_client_1
    cluster: cluster_1
    config: # optional
       message.timeout.ms: 5000
       auto.offset.reset: earliest
  - client: cl_2_client_1
    cluster: cluster_2

routes:
  - upstream_client: cl_1_client_1
    downstream_client: cl_1_client_1
    upstream_topics:
      - 'topic1'
    downstream_topic: 'topic2'
    repartitioning_strategy: random # strict_p2p | random
    upstream_group_id: group_22
    show_progress_interval_secs: 10
    limits:
      messages_per_sec: 10000
      number_of_messages:

  - upstream_client: cl_1_client_1
    downstream_client: cl_2_client_1
    upstream_topics:
      - 'topic2'
    downstream_topic: 'topic2'
    repartitioning_strategy: strict_p2p
    upstream_group_id: group_22
    show_progress_interval_secs: 10

  - upstream_client: cl_2_client_1
    downstream_client: cl_1_client_1
    upstream_topics:
      - 'topic2'
    downstream_topic: 'topic3'
    repartitioning_strategy: strict_p2p # strict_p2p | random
    default_begin_offset: earliest # optional
    upstream_group_id: group_2
    show_progress_interval_secs: 10


observers:
  - client: cl_1_client_1
    name: "my name"
    group_id: group_name # used for remaining metrics
    topics: # filter by topics
      - 'topic1'
      - 'topic2'
    fetch_timeout_secs: 5 # default: 5
    fetch_interval_secs: 5 # default: 60
    show_progress_interval_secs: 10 # default: 60

  - client: cl_2_client_1
    topic: 'topic3'
    topics:
      - 'topic2'
    show_progress_interval_secs: 5


  - client: cl_1_client_1
    topic: 'topic1'
    topics: [] # fetch all topics
```


### Options describing

Root config options:
 - _clusters_ - are a list of Kafka Clusters
 - _clients_ - are a list of configurations for consumers
 - _routes_ - are a list of replication rules
 - _observers_ - are a list of observers


### Contributing
Any suggestion, feedback or contributing is highly appreciated. Thank you for your support!
