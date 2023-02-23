use criterion::{criterion_group, criterion_main, BatchSize, Bencher, Criterion, Throughput};
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
use std::time::Duration;
use rand::distributions::Uniform;
use rand::Rng;

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

pub fn ingestion(c: &mut Criterion) {
    fn make_index_gen() -> Box<dyn Iterator<Item = u64>> {
        let mut rng = rand::thread_rng();
        let range = Uniform::new(u64::MIN, u64::MAX);
        Box::new(rng.sample_iter(range))
    }
    fn make_time_gen() -> Box<dyn Iterator<Item = i64>> {
        let mut rng = rand::thread_rng();
        let range = Uniform::new(i64::MIN, i64::MAX);
        Box::new(rng.sample_iter(range))
    }

    let mut indexes = make_index_gen();
    let mut times = make_time_gen();
    let mut index_sample = || indexes.next().unwrap();
    let mut time_sample = || times.next().unwrap();

    let mut group = c.benchmark_group("ingestion");
    group.throughput(Throughput::Elements(1));

    // Creates a graph with 50k random edges -> 100k random vertices
    fn bootstrap_graph() -> GraphDB {
        let mut graph = GraphDB::new(4);
        let mut indexes = make_index_gen();
        let mut times = make_time_gen();
        for _ in 0..50_000 {
            let source = indexes.next().unwrap();
            let target = indexes.next().unwrap();
            let time = times.next().unwrap();
            graph.add_edge(source, target, time, &vec![]);
        }
        graph
    };

    group.bench_function("existing vertex varying time", |b: &mut Bencher| {
        b.iter_batched_ref(
            || (bootstrap_graph(), time_sample()),
            |(g, t): &mut (GraphDB, i64)| g.add_vertex(0, *t, &vec![]),
            BatchSize::SmallInput,
        )
    });
    group.bench_function("new vertex constant time", |b: &mut Bencher| {
        b.iter_batched_ref(
            || (bootstrap_graph(), index_sample()),
            |(g, v): &mut (GraphDB, u64)| g.add_vertex(*v, 0, &vec![]),
            BatchSize::SmallInput,
        )
    });
    group.bench_function("existing edge varying time", |b: &mut Bencher| {
        b.iter_batched_ref(
            || (bootstrap_graph(), time_sample()),
            |(g, t)| g.add_edge(0, 0, *t, &vec![]),
            BatchSize::SmallInput,
        )
    });
    group.bench_function("new edge constant time", |b: &mut Bencher| {
        b.iter_batched_ref(
            || (bootstrap_graph(), index_sample(), index_sample()),
            |(g, s, d)| g.add_edge(*s, *d, 0, &vec![]),
            BatchSize::SmallInput,
        )
    });

    group.finish();
}

pub fn analysis(c: &mut Criterion) {
    let mut group = c.benchmark_group("analysis");
    let lotr = data::lotr().unwrap();
    let mut graph = GraphDB::new(4);
    load_csv(&mut graph, &lotr, 0, 1, Some(2));
    group.bench_function("edges_len", |b| b.iter(|| graph.edges_len()));
    group.finish();
}

criterion_group!(benches, ingestion, analysis);
criterion_main!(benches);
