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
    error::Error,
    marker::Copy,
    time::{Duration, SystemTime},
};

use actix_web::{web, App, HttpResponse, HttpServer};

use std::{collections::HashMap, path::PathBuf, sync::{atomic::{Ordering, AtomicBool}, Arc, Mutex}};
use structopt::StructOpt;


use replicator::*;



use prometheus::{self, IntGauge, IntCounter, TextEncoder, Encoder, Registry, labels};
use metrics::{run_prometheus_server, ObserverMetrics};


#[derive(StructOpt, Debug)]
#[structopt(version = cli::get_crate_version())]
#[structopt(name = "Kafka topics observer")]
/// Commands help
pub struct ObserverCommandLine {
    #[structopt(parse(from_os_str), name = "CONFIG")]
    pub input: PathBuf,

    #[structopt(long)]
    pub validate: bool,

    /// Port for listening
    #[structopt(short = "p", long = "port", default_value = "9444")]
    pub port: u32,

    /// Host or ip address for listening
    #[structopt(short = "h", long = "host", default_value = "127.0.0.1")]
    pub host: String,
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

    let running = Arc::new(AtomicBool::new(true));

    let namespace = config.prometheus.clone().map_or(
        None,
        |config|config.namespace);

    let labels = config.prometheus.clone().map_or(
        None,
        |config| config.labels);

    let observer_metrics: Arc<Mutex<ObserverMetrics>> = Arc::new(Mutex::new(ObserverMetrics::new(namespace, labels)));

    let mut observers = vec![];

    for mut observer in config.get_observers(observer_metrics.clone()) {
        let is_running = Arc::clone(&running);
        observers.push(thread::spawn(move || observer.start(is_running)));
    }

    let running_switcher = running.clone();

    ctrlc::set_handler(move || {
        running_switcher.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    run_prometheus_server::<ObserverMetrics>(&opt.host, opt.port, observer_metrics.clone())?;

    for thread in observers {
        thread.join().unwrap();
    }

    Ok(())
}
