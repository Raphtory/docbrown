use crate::common::bootstrap_graph;
use common::{run_analysis_benchmarks, run_ingestion_benchmarks};
use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use csv::StringRecord;
use csv_sniffer::Type;
use docbrown_db::data;
use docbrown_db::graphdb::GraphDB;
use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::fs::File;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::path::Path;

mod common;

fn hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn load_csv(graph: &mut GraphDB, path: &Path, source: usize, target: usize, time: Option<usize>) {
    let mut times: Range<i64> = (0..i64::MAX);
    let mut metadata = csv_sniffer::Sniffer::new().sniff_path(path).unwrap();
    metadata.dialect.header = csv_sniffer::metadata::Header {
        has_header_row: false,
        num_preamble_rows: 0,
    };
    let ids_are_numbers =
        metadata.types[source] == Type::Unsigned && metadata.types[target] == Type::Unsigned;
    let mut reader = metadata
        .dialect
        .open_reader(File::open(path).unwrap())
        .unwrap();

    let mut parse_record = |rec: &StringRecord| {
        let source_str = rec.get(source).ok_or("No source id")?;
        let target_str = rec.get(target).ok_or("No target id")?;
        let (source_value, target_value): (u64, u64) = if ids_are_numbers {
            (source_str.parse::<u64>()?, target_str.parse::<u64>()?)
        } else {
            (hash(&source_str), hash(&target_str))
        };
        let time_value: i64 = match time {
            Some(time) => rec.get(time).ok_or("No time value")?.parse()?,
            None => times.next().ok_or("Max time reached")?,
        };
        Ok::<(u64, u64, i64), Box<dyn Error>>((source_value, target_value, time_value))
    };

    for record in reader.records() {
        let record_ok = record.unwrap();
        let (source_id, target_id, time) =
            parse_record(&record_ok).expect(&format!("Unable to parse record: {:?}", record_ok));
        graph.add_vertex(source_id, time, &vec![]);
        graph.add_vertex(target_id, time, &vec![]);
        graph.add_edge(source_id, target_id, time, &vec![]);
    }
}

pub fn base(c: &mut Criterion) {
    let mut ingestion_group = c.benchmark_group("ingestion");
    ingestion_group.throughput(Throughput::Elements(1));
    run_ingestion_benchmarks(&mut ingestion_group, || bootstrap_graph(4, 10_000), None);
    ingestion_group.finish();

    let mut analysis_group = c.benchmark_group("analysis");
    run_analysis_benchmarks(&mut analysis_group, || data::lotr_graph(4), None);
    analysis_group.finish();
}

criterion_group!(benches, base);
criterion_main!(benches);
