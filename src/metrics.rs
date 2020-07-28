use prometheus::{self, Encoder, IntCounter, IntGauge, IntGaugeVec, TextEncoder, Registry, Opts, GaugeVec, IntCounterVec};

use std::{env, io, collections::HashMap, sync::{Arc, Mutex}, marker::Send};

use actix_rt;
use actix_web::{get, web, App, HttpServer, Responder, middleware};
use web::Data;
use super::cli;

const DEFAULT_OBSERVER_NAMESPACE: &str = "service:kafka_observer:";
const DEFAULT_REPLICATOR_NAMESPACE: &str = "service:kafka_replicator:";


pub trait Metrics: Sync + Send + 'static {
    // fn get_metrics(&self) -> String {
    //     String::from("vfvasfadsfsa")
    // }
    fn get_registry(&self) -> &Registry;
    fn get_metrics(&self) -> String {
        let mut buffer = vec![];
        TextEncoder::new()
            .encode(&self.get_registry().gather(), &mut buffer)
            .unwrap();
        String::from_utf8(buffer).unwrap()

    }
}

#[derive(Debug)]
pub struct PipelineMetrics {
    pub registry: Registry,
    pub namespace: String,
    pub const_labels: HashMap<String, String>,
    pub last_receive_ts: GaugeVec,
    pub start_ts: GaugeVec,
    pub received_total: IntCounterVec,
    pub transfered_total: IntCounterVec,
    pub keys_bytes: IntCounterVec,
    pub payload_bytes: IntCounterVec,
    pub headers_bytes: IntCounterVec,
    pub total_bytes: IntCounterVec,
    pub errors_total: IntCounterVec,
    pub avg_transfer_duration: GaugeVec
}


impl PipelineMetrics {
    pub fn new(namespace: Option<String>, labels: Option<HashMap<String, String>>) -> Self {

        let registry = Registry::new();

        let labels = labels.unwrap_or(HashMap::new());
        let namespace = namespace.unwrap_or(DEFAULT_REPLICATOR_NAMESPACE.to_string());
        let opts = Opts::new(
            vec!(namespace.clone(), "received_total_count".to_string()).join(""),
            "Count of received records".to_string())
            .const_labels(labels.clone());

        let label_names = ["name", "upstream_name", "downstream_name"];

        let received_total: IntCounterVec = IntCounterVec::new(opts, &label_names).unwrap();

        let opts = Opts::new(
            vec!(namespace.clone(), "transfered_total_count".to_string()).join(""),
            "Count of produced records".to_string())
            .const_labels(labels.clone());

        let transfered_total: IntCounterVec = IntCounterVec::new(opts, &label_names).unwrap();


        let opts = Opts::new(
            vec!(namespace.clone(), "keys_bytes".to_string()).join(""),
            "number of transfered keys bytes".to_string())
            .const_labels(labels.clone());

        let keys_bytes: IntCounterVec = IntCounterVec::new(opts, &label_names).unwrap();


        let opts = Opts::new(
            vec!(namespace.clone(), "payload_bytes".to_string()).join(""),
            "number of transfered payload bytes".to_string())
            .const_labels(labels.clone());

        let payload_bytes: IntCounterVec = IntCounterVec::new(opts, &label_names).unwrap();


        let opts = Opts::new(
            vec!(namespace.clone(), "headers_bytes".to_string()).join(""),
            "number of transfered headers bytes".to_string())
            .const_labels(labels.clone());

        let headers_bytes: IntCounterVec = IntCounterVec::new(opts, &label_names).unwrap();


        let opts = Opts::new(
            vec!(namespace.clone(), "total_bytes".to_string()).join(""),
            "number of total transfered  bytes".to_string())
            .const_labels(labels.clone());

        let total_bytes: IntCounterVec = IntCounterVec::new(opts, &label_names).unwrap();

        let opts = Opts::new(
            vec!(namespace.clone(), "errors_total".to_string()).join(""),
            "number of total received errors".to_string())
            .const_labels(labels.clone());

        let errors_total: IntCounterVec = IntCounterVec::new(opts, &label_names).unwrap();


        for item in [
            &received_total,
            &transfered_total,
            &keys_bytes,
            &payload_bytes,
            &headers_bytes,
            &total_bytes,
            &errors_total].iter() {
            registry.register(Box::new((*item).clone())).expect("Can't register metric");
        }

        let opts= Opts::new(
            vec!(namespace.clone(), "last_receive_ts".to_string()).join(""),
            "last receive timestamp".to_string()).const_labels(labels.clone());

        let last_receive_ts: GaugeVec = GaugeVec::new(opts, &label_names).unwrap();

        let opts= Opts::new(
            vec!(namespace.clone(), "start_ts".to_string()).join(""),
            "start timestamp".to_string()).const_labels(labels.clone());

        let start_ts: GaugeVec = GaugeVec::new(opts, &label_names).unwrap();

        let opts= Opts::new(
            vec!(namespace.clone(), "average_transfer_duration_ms".to_string()).join(""),
            "start timestamp".to_string()).const_labels(labels.clone());

        let avg_transfer_duration: GaugeVec = GaugeVec::new(opts, &label_names).unwrap();


        for item in [&last_receive_ts, &start_ts, &avg_transfer_duration].iter() {
            registry.register(Box::new((*item).clone())).expect("Can't register metric");
        }

        Self { registry,
               namespace,
               const_labels: labels,
               received_total,
               last_receive_ts,
               start_ts,
               transfered_total,
               keys_bytes,
               payload_bytes,
               headers_bytes,
               total_bytes,
               errors_total,
               avg_transfer_duration

        }
    }
}

impl Metrics for PipelineMetrics {
    fn get_registry(&self) -> &Registry {
        &(self.registry)
    }
}


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


        Self { registry,
               namespace,
               const_labels: labels,
               number_of_records_total,
               partition_start_offset,
               partition_end_offset,
               number_of_records_for_partition,
               last_fetch_ts
        }

    }

    // pub fn get_metrics(&self) -> String {
    //     let mut buffer = vec![];
    //     TextEncoder::new()
    //         .encode(&self.registry.gather(), &mut buffer)
    //         .unwrap();
    //     String::from_utf8(buffer).unwrap()
    // }
}

impl Metrics for ObserverMetrics {
    fn get_registry(&self) -> &Registry {
        &(self.registry)
    }
}

// get macros not work for generic functions
// #[get("/prometheus/metrics")]
async fn metrics_handler<T: Metrics + Send + 'static>(metrics: Data<Arc<Mutex<T>>>,
) -> impl Responder  {

    metrics.lock().unwrap().get_metrics()
}

pub fn run_prometheus_server<T: Metrics + Send + 'static>(host: &str, port: u32, metrics: Arc<Mutex<T>>) -> io::Result<()> {
    let sys = actix_rt::System::new("kafka_replicator");

    let data: Data<Arc<Mutex<T>>> = web::Data::new(metrics);

    HttpServer::new(move || {
        App::new()
            .app_data(data.clone())
            .service(web::resource("/prometheus/metrics")
                     .route(web::get().to(metrics_handler::<T>)))
            .wrap(middleware::DefaultHeaders::new().header("X-Version", cli::get_crate_version()))

    })
        .bind(format!("{:}:{:}", host, port))?
        .workers(5).run();

    sys.run()
}
