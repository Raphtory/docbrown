use crate::{csv_loader::csv::CsvLoader, graph::Graph};
use docbrown_core::utils;
use docbrown_core::Prop;
use fetch_data::{fetch, FetchDataError};
use serde::Deserialize;
use std::env;
use std::path::PathBuf;
// In order to add new files to this module, obtain the hash using bin/hash_for_url.rs

#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    src_id: String,
    dst_id: String,
    time: i64,
}

#[derive(Deserialize, std::fmt::Debug)]
pub struct Twitter {
    src_id: String,
    dst_id: String,
}

pub fn lotr_file() -> Result<PathBuf, FetchDataError> {
    fetch_file(
        "lotr.csv",
        "https://raw.githubusercontent.com/Raphtory/Data/main/lotr.csv",
        "c37083a5018827d06de3b884bea8275301d5ef6137dfae6256b793bffb05d033",
    )
}

pub fn twitter_file() -> Result<PathBuf, FetchDataError> {
    fetch_file(
        "twitter.csv",
        "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv",
        "b9cbdf68086c0c6b1501efa2e5ac6b1b0d9e069ad9215cebeba244e6e623c1bb",
    )
}

fn fetch_file(name: &str, url: &str, hash: &str) -> Result<PathBuf, FetchDataError> {
    let tmp_dir = env::temp_dir();
    let file = tmp_dir.join(name);
    fetch(url, hash, &file)?;
    Ok(file)
}

pub fn lotr_graph(shards: usize) -> Graph {
    let graph = {
        let g = Graph::new(shards);

        CsvLoader::new(&lotr_file().unwrap())
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                let src_id = utils::calculate_hash(&lotr.src_id);
                let dst_id = utils::calculate_hash(&lotr.dst_id);
                let time = lotr.time;

                g.add_vertex(
                    time,
                    src_id,
                    &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                );
                g.add_vertex(
                    time,
                    src_id,
                    &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                );
                g.add_edge(
                    time,
                    src_id,
                    dst_id,
                    &vec![(
                        "name".to_string(),
                        Prop::Str("Character Co-occurrence".to_string()),
                    )],
                );
            })
            .expect("Failed to load graph from CSV data files");
        g
    };
    graph
}

pub fn twitter_graph(shards: usize) -> Graph {
    let graph = {
        let g = Graph::new(shards);

        CsvLoader::new(&twitter_file().unwrap())
            .set_delimiter(" ")
            .load_into_graph(&g, |twitter: Twitter, g: &Graph| {
                let src_id = utils::calculate_hash(&twitter.src_id);
                let dst_id = utils::calculate_hash(&twitter.dst_id);
                let time = 1;

                g.add_vertex(
                    time,
                    src_id,
                    &vec![("name".to_string(), Prop::Str("User".to_string()))],
                );
                g.add_vertex(
                    time,
                    src_id,
                    &vec![("name".to_string(), Prop::Str("User".to_string()))],
                );
                g.add_edge(
                    time,
                    src_id,
                    dst_id,
                    &vec![("name".to_string(), Prop::Str("Tweet".to_string()))],
                );
            })
            .expect("Failed to load graph from CSV data files");
        g
    };

    graph
}
