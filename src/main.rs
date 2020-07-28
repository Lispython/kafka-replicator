use tokio;

#[macro_use]
extern crate log;

use replicator::*;
use std::{rc::Rc, sync::{Arc, atomic::{Ordering, AtomicBool}, Mutex}};
use metrics::{run_prometheus_server, PipelineMetrics};

async fn run_replicator(config: config::Config, is_running: Arc<AtomicBool>, metrics: Arc<Mutex<PipelineMetrics>>) -> Result<(), Box<dyn std::error::Error>> {
    for mut pipeline in config.get_pipelines(metrics.clone(), is_running.clone()) {
        tokio::spawn(async move {
            pipeline.start().await

        });
    }

    Ok(())
}


// #[tokio::main]
fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    info!("Kafka topics replicator");

    let opt = cli::parse_args();

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


    let replicator_metrics = Arc::new(Mutex::new(PipelineMetrics::new(namespace, labels)));

    let mut rt = tokio::runtime::Runtime::new().unwrap();

    if opt.validate == true {
        debug!("Validate config");
        return Ok(());
    }

    let running_switcher = running.clone();

    ctrlc::set_handler(move || {
        running_switcher.store(false, Ordering::SeqCst);
    }).expect("Error setting Ctrl-C handler");

    rt.block_on(run_replicator(config, Arc::clone(&running), replicator_metrics.clone()));


    run_prometheus_server::<PipelineMetrics>(&opt.host, opt.port, replicator_metrics.clone())?;


    Ok(())
}
