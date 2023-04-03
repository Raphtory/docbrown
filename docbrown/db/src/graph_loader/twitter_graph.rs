use crate::{
    graph::Graph,
    graph_loader::{fetch_file, CsvLoader},
};
use docbrown_core::{utils, Prop};
use serde::Deserialize;
use std::path::PathBuf;

#[derive(Deserialize, std::fmt::Debug)]
pub struct Twitter {
    src_id: String,
    dst_id: String,
}

pub fn twitter_file() -> Result<PathBuf, Box<dyn std::error::Error>> {
    fetch_file(
        "twitter.csv",
        "https://raw.githubusercontent.com/Raphtory/Data/main/snap-twitter.csv",
        600,
    )
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
                )
                .map_err(|err| println!("{:?}", err))
                .ok();
                g.add_vertex(
                    time,
                    src_id,
                    &vec![("name".to_string(), Prop::Str("User".to_string()))],
                )
                .map_err(|err| println!("{:?}", err))
                .ok();
                g.add_edge(
                    time,
                    src_id,
                    dst_id,
                    &vec![("name".to_string(), Prop::Str("Tweet".to_string()))],
                    None,
                );
            })
            .expect("Failed to load graph from CSV data files");
        g
    };

    graph
}
