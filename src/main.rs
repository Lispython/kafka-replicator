use tokio;

#[macro_use]
extern crate log;

// use tokio::signal::unix::{signal, SignalKind};

use replicator::*;

async fn run_replicator(config: config::Config) -> Result<(), Box<dyn std::error::Error>> {
    for route_rule in config.get_routes() {
        tokio::spawn(async move { route_rule.start().await });
    }
    tokio::signal::ctrl_c().await?;

    // let mut stream = signal(SignalKind::hangup())?;

    // // Print whenever a HUP signal is received
    // loop {
    //     stream.recv().await;
    //     println!("got signal HUP");
    // }

    // // println!("ctrl-c received!");

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

    let mut rt = tokio::runtime::Runtime::new().unwrap();

    if opt.validate == true {
        debug!("Validate config");
        return Ok(());
    }

    rt.block_on(run_replicator(config))

    // rt.block_on(async {

    //     for route_rule in config.get_routes() {
    //         tokio::spawn(async move {
    //             route_rule.start().await
    //         });
    //     }
    //     // tokio::signal::ctrl_c().await?;

    //     let mut stream = signal(SignalKind::hangup())?;

    //     // Print whenever a HUP signal is received
    //     // loop {
    //     //     stream.recv().await;
    //     //     println!("got signal HUP");
    //     // }

    //     // println!("ctrl-c received!");

    //     Ok(())
    // })
}
