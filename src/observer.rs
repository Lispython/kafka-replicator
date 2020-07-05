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
    collections::{HashMap, HashSet},
    error::Error,
    marker::Copy,
    time::{Duration, SystemTime},
};

use std::path::PathBuf;
use structopt::StructOpt;

use replicator::*;

#[derive(StructOpt, Debug)]
#[structopt(version = get_crate_version())]
#[structopt(name = "Kafka topics observer")]
/// Commands help
pub struct ObserverCommandLine {
    #[structopt(parse(from_os_str), name = "CONFIG")]
    pub input: PathBuf,

    #[structopt(long)]
    pub validate: bool,

    #[structopt(long = "num", default_value = "5")]
    pub num: u64,
}

pub fn parse_args() -> ObserverCommandLine {
    ObserverCommandLine::from_args()
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    info!("Kafka topic observer");

    let opt: ObserverCommandLine = parse_args();

    let config = match utils::get_config(&opt.input) {
        Ok(value) => value,
        _ => panic!("Invalid config file: {:?}", &opt.input),
    };

    let mut observers = vec![];

    for observer in config.get_observers() {
        observers.push(thread::spawn(move || observer.start()));
    }

    for thread in observers {
        thread.join();
    }

    Ok(())
}
