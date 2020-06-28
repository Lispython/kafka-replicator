use std::{fs, path::PathBuf};

use std::ffi::OsStr;

use rand::{distributions::Alphanumeric, thread_rng, Rng};
use std::iter;

use std::{
    collections::HashMap,
    env::{self, VarError},
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};

use serde_json;
use serde_yaml;
use toml;

use super::{config, errors};

pub enum Value {
    // serde_json::Value<T>,
// serde_yaml::Value<T>
}

pub fn rand_string(prefix: &str, size: Option<u64>) -> String {
    let size = match size {
        Some(v) => v,
        _ => 5,
    };

    let mut rng = thread_rng();
    let chars: String = iter::repeat(())
        .map(|()| rng.sample(Alphanumeric))
        .take(size as usize)
        .collect();

    format!("{:}{:}", prefix, chars)
}

pub fn get_config(file: &PathBuf) -> Result<config::Config, &str> {
    match &file.extension().and_then(OsStr::to_str) {
        Some("yml") | Some("yaml") => "yml",
        Some("toml") => "toml",
        Some("json") => "json",
        Some(v) => panic!("Invalid config file extension: {:}", v),
        _ => panic!("Invalid config file extension"),
    };

    let path = match fs::canonicalize(&file) {
        Ok(value) => value,
        Err(_error) => panic!("Invalid config file path: {:?}", &file),
    };
    let contents = match fs::read_to_string(&path) {
        Ok(value) => value,
        Err(_error) => panic!("Can't read config file: {:?}", &path),
    };

    // let config = match serde_yaml::from_str(&contents){
    //     Some(v) => v,
    //     Err(e) => return Err("Can't parse config file")

    // };

    let config: config::Config = match serde_yaml::from_str(&contents) {
        Ok(value) => value,
        Err(_error) => panic!("Invalid config contents."),
    };

    Ok(config)
}
