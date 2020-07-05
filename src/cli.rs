use std::path::PathBuf;
use structopt::StructOpt;
use toml::Value;

use std::fs;
use toml;

pub fn get_crate_version() -> &'static str {
    env!("CARGO_PKG_VERSION")
}

#[derive(StructOpt, Debug)]
#[structopt(version = get_crate_version())]
#[structopt(name = "Kafka topics replicator")]
/// Commands help
pub struct CommandLine {
    // /// Files to process
    // #[structopt(name = "FILE", parse(from_os_str))]
    // files: Vec<PathBuf>,
    /// Port for listening
    #[structopt(short = "p", long = "port", default_value = "9444")]
    pub port: u32,

    /// Host or ip address for listening
    #[structopt(short = "h", long = "host", default_value = "127.0.0.1")]
    pub host: String,

    #[structopt(parse(from_os_str), name = "CONFIG")]
    pub input: PathBuf,

    #[structopt(long)]
    pub validate: bool,
}

pub fn parse_args() -> CommandLine {
    CommandLine::from_args()
}

pub fn parse_config(file: PathBuf) -> std::io::Result<Value> {
    let contents = fs::read_to_string(fs::canonicalize(file)?)?;

    Ok(contents.parse::<Value>().unwrap())
}
