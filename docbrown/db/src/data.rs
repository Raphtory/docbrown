use crate::{graphdb::GraphDB, loaders::csv::CsvLoader};
use docbrown_core::utils;
use docbrown_core::Prop;
use fetch_data::{fetch, FetchDataError};
use serde::Deserialize;
use std::path::PathBuf;
use std::env;

pub mod lotr_graph;
pub mod twitter_graph;

pub fn fetch_file(name: &str, url: &str, hash: &str) -> Result<PathBuf, FetchDataError> {
    let tmp_dir = env::temp_dir();
    let file = tmp_dir.join(name);
    fetch(url, hash, &file)?;
    Ok(file)
}