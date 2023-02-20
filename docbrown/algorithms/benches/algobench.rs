use criterion::{criterion_group, criterion_main, Criterion};
use docbrown_algorithms::connectedcomponents::connected_components;
use docbrown_core::graphview::WindowedView;
use docbrown_core::utils::calculate_hash;
use docbrown_core::Prop;
use docbrown_db::graphdb::GraphDB;
use docbrown_db::loaders::csv::CsvLoader;
use regex::Regex;
use serde::Deserialize;
use std::path::{Path, PathBuf};

#[derive(Deserialize, std::fmt::Debug)]
pub struct Lotr {
    src_id: String,
    dst_id: String,
    time: i64,
}

pub fn connected_components_lotr_analysis(c: &mut Criterion) {
    let mut benchmark_group = c.benchmark_group("connected_components");

    benchmark_group.bench_function("connected-components-lotr", |b| {
        b.iter(|| {
            let g = GraphDB::new(1);
            let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resources"]
                .iter()
                .collect();
            //insert file name inside +() e.g. +(lotr.csv)
            let r = Regex::new(r".+(lotr.csv)").unwrap();
            let csv_loader = CsvLoader::new(Path::new(&csv_path));
            csv_loader
                .set_header(false)
                .set_delimiter(",")
                .with_filter(r)
                .load_into_graph(&g, |lotr: Lotr, graph: &GraphDB| {
                    let src_id = calculate_hash(&lotr.src_id);
                    let dst_id = calculate_hash(&lotr.dst_id);
                    let time = lotr.time;

                    graph.add_vertex(
                        src_id,
                        time,
                        &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                    );
                    graph.add_vertex(
                        dst_id,
                        time,
                        &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                    );
                    graph.add_edge(
                        src_id,
                        dst_id,
                        time,
                        &vec![(
                            "name".to_string(),
                            Prop::Str("Character Co-occurrence".to_string()),
                        )],
                    );
                })
                .expect("Csv did not parse.");
            let gv = WindowedView::new(&g, 0..32674);
            connected_components(&gv).unwrap();
        })
    });

    benchmark_group.finish();
}

criterion_group!(benches, connected_components_lotr_analysis);
criterion_main!(benches);
