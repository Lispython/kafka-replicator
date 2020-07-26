use prometheus::{self, Encoder, IntCounter, IntGauge, IntGaugeVec, TextEncoder, Registry, Opts, GaugeVec};

use std::{env, io, collections::HashMap, sync::{Arc, Mutex}};

use actix_rt;
use actix_web::{get, web, App, HttpServer, Responder, middleware};
use web::Data;
use super::cli;

const DEFAULT_OBSERVER_NAMESPACE: &str = "service:kafka_observer:";

#[derive(Debug)]
pub struct ObserverMetrics {

    pub registry: Registry,
    pub namespace: String,
    pub const_labels: HashMap<String, String>,

    pub number_of_records_total: IntGaugeVec,
    pub partition_start_offset: IntGaugeVec,
    pub partition_end_offset: IntGaugeVec,
    pub number_of_records_for_partition: IntGaugeVec,
    pub last_fetch_ts: GaugeVec
}

impl ObserverMetrics {
    pub fn new(namespace: Option<String>, labels: Option<HashMap<String, String>>) -> Self {

        let registry = Registry::new();

        let labels = labels.unwrap_or(HashMap::new());
        let namespace = namespace.unwrap_or(DEFAULT_OBSERVER_NAMESPACE.to_string());
        let opts = Opts::new(
            vec!(namespace.clone(), "number_of_records".to_string()).join(""),
            "Number of records".to_string())
            .const_labels(labels.clone());

        let number_of_records_total: IntGaugeVec = IntGaugeVec::new(opts, &["topic"]).unwrap();


        let label_names = ["topic", "partition"];

        let opts= Opts::new(
            vec!(namespace.clone(), "start_offset_for_partition".to_string()).join(""),
            "topic partition start offset".to_string()).const_labels(labels.clone());

        let partition_start_offset: IntGaugeVec = IntGaugeVec::new(opts, &label_names).unwrap();


        let opts= Opts::new(
            vec!(namespace.clone(), "end_offset_for_partition".to_string()).join(""),
            "topic partition end offset".to_string()).const_labels(labels.clone());

        let partition_end_offset: IntGaugeVec = IntGaugeVec::new(opts, &label_names).unwrap();


        let opts= Opts::new(
            vec!(namespace.clone(), "number_of_records_for_partition".to_string()).join(""),
            "number of records for partition".to_string()).const_labels(labels.clone());

        let number_of_records_for_partition: IntGaugeVec = IntGaugeVec::new(opts, &label_names).unwrap();

        let metrics = [
            &number_of_records_total,
            &partition_start_offset,
            &partition_end_offset,
            &number_of_records_for_partition];

        for item in metrics.iter() {
            registry.register(Box::new((*item).clone())).expect("Can't register metric");
        }

        let opts= Opts::new(
            vec!(namespace.clone(), "last_fetch_ts".to_string()).join(""),
            "last fetch timestamp".to_string()).const_labels(labels.clone());

        let last_fetch_ts: GaugeVec = GaugeVec::new(opts, &["topic"]).unwrap();
        registry.register(Box::new(last_fetch_ts.clone())).expect("Can't register metric");


        Self { registry: registry,
               namespace: namespace,
               const_labels: labels,
               number_of_records_total: number_of_records_total,
               partition_start_offset: partition_start_offset,
               partition_end_offset: partition_end_offset,
               number_of_records_for_partition: number_of_records_for_partition,
               last_fetch_ts: last_fetch_ts
        }

    }

    pub fn get_metrics(&self) -> String {
        let mut buffer = vec![];
        TextEncoder::new()
            .encode(&self.registry.gather(), &mut buffer)
            .unwrap();
        String::from_utf8(buffer).unwrap()
    }


}

#[get("/prometheus/metrics")]
async fn metrics_handler(metrics: Data<Arc<Mutex<ObserverMetrics>>>,
) -> impl Responder {

    metrics.lock().unwrap().get_metrics()
}

pub fn run_prometheus_server(host: &str, port: u32, metrics: Arc<Mutex<ObserverMetrics>>) -> io::Result<()> {
    let sys = actix_rt::System::new("kafka_replicator");

    let data = web::Data::new(metrics);

    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .service(metrics_handler)
            .wrap(middleware::DefaultHeaders::new().header("X-Version", cli::get_crate_version()))

    })
        .bind(format!("{:}:{:}", host, port))?
        .workers(5).run();

    sys.run()
}
