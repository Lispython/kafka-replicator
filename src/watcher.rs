use tokio;

#[macro_use]
extern crate log;
use std::thread;

use rdkafka::{
    client::{ClientContext, DefaultClientContext},
    message::{BorrowedHeaders, Headers, Message, OwnedHeaders},
    producer::{FutureProducer, FutureRecord},
    util::Timeout,
};

use std::{
    collections::HashMap,
    collections::HashSet,
    error::Error,
    marker::Copy,
    time::{Duration, SystemTime},
};

use std::path::PathBuf;
use structopt::StructOpt;

use replicator::*;

#[derive(StructOpt, Debug)]
#[structopt(version = "get_crate_version()")]
#[structopt(name = "producer")]
/// Commands help
pub struct WatcherCommandLine {
    #[structopt(parse(from_os_str), name = "CONFIG")]
    pub input: PathBuf,

    #[structopt(long)]
    pub validate: bool,

    #[structopt(long = "num", default_value = "5")]
    pub num: u64,
}

pub fn parse_args() -> WatcherCommandLine {
    WatcherCommandLine::from_args()
}


fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    info!("Kafka topic watcher");

    let opt: WatcherCommandLine = parse_args();

    let config = match utils::get_config(&opt.input) {
        Ok(value) => value,
        _ => panic!("Invalid config file: {:?}", &opt.input),
    };

    let mut watchers = vec![];

    for watcher in config.get_watchers() {

        watchers.push(thread::spawn(move || {
            watcher.start()
        }));

    };

    for thread in watchers {
        thread.join();
    }

    Ok(())
}
