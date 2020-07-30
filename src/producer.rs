use tokio;

#[macro_use]
extern crate log;

use rdkafka::{
    client::{ClientContext, DefaultClientContext},
    message::{BorrowedHeaders, Headers, Message, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};

use std::{
    path::PathBuf,
    time::{Duration, Instant, SystemTime},
};
use structopt::StructOpt;

use replicator::*;

#[derive(StructOpt, Debug)]
#[structopt(version = cli::get_crate_version())]
#[structopt(name = "producer")]
/// Commands help
pub struct ProducerCommandLine {
    #[structopt(parse(from_os_str), name = "CONFIG")]
    pub input: PathBuf,

    #[structopt(long)]
    pub validate: bool,

    #[structopt(long)]
    pub watch: bool,

    #[structopt(long = "topic", default_value = "test_topic")]
    pub topic: String,

    #[structopt(long = "client")]
    pub client: String,

    #[structopt(long = "num", default_value = "5")]
    pub num: u64,

    #[structopt(long = "status-every", default_value = "10000")]
    pub status_every_n_records: u64,
}

pub fn parse_args() -> ProducerCommandLine {
    ProducerCommandLine::from_args()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    info!("Kafka debug topics producer");

    let opt: ProducerCommandLine = parse_args();

    let config = match utils::get_config(&opt.input) {
        Ok(value) => value,
        _ => panic!("Invalid config file: {:?}", &opt.input),
    };

    let producer: FutureProducer<DefaultClientContext> = config
        .create_client_config(&opt.client, None)
        .create()
        .expect("Can't create producer.");

    let start_time = Instant::now();
    for x in 0..opt.num {
        let payload = utils::rand_string("payload_", Some(10));
        let key = utils::rand_string("key_", None);

        let mut record: FutureRecord<'_, _, _> =
            FutureRecord::to(&opt.topic).payload(&payload).key(&key);

        record = record.headers(OwnedHeaders::new().add(
            &utils::rand_string("header_key_", Some(10)),
            &utils::rand_string("header_value_", None),
        ));

        // record = record.headers(OwnedHeaders::new().add("header_key",
        //                                                 "header_value"));

        producer.send(record, 0);

        if x % opt.status_every_n_records == 0 {
            info!(
                "Produced {:} messages to topic {:} within {:?}",
                x,
                opt.topic,
                Instant::now().duration_since(start_time)
            );
        };

        // let mut rt = tokio::runtime::Runtime::new().unwrap();
    }
    producer.flush(Timeout::Never);

    info!(
        "Produced {:} messages to topic {:} within {:?}",
        opt.num,
        opt.topic,
        Instant::now().duration_since(start_time)
    );
    Ok(())
}
