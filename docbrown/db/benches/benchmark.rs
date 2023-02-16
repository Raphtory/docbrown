use std::collections::hash_map::DefaultHasher;
use std::error::Error;
use std::hash::{Hash, Hasher};
use std::ops::Range;
use std::path::Path;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Bencher, BatchSize};
use csv::StringRecord;
use docbrown_core::Direction;
use docbrown_db::graphdb::GraphDB;

use docbrown_it::data;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn read_csv(path: &Path, source: usize, target: usize, time: Option<usize>) -> GraphDB {
    let mut times: Range<i64> = (0..i64::MAX);
    let mut parse_record = |rec: &StringRecord| {
        let source_value: String = rec.get(source).ok_or("No source id")?.parse()?;
        let target_value: String = rec.get(target).ok_or("No target id")?.parse()?;
        let time_value: i64 = match time {
            Some(time) => rec.get(time).ok_or("No time value")?.parse()?,
            None => times.next().ok_or("Max time reached")?
        };
        Ok::<(String, String, i64), Box<dyn Error>>((source_value, target_value, time_value))
    };

    let mut reader = csv::Reader::from_path(path).unwrap();
    let graph = GraphDB::new(4);

    for record in reader.records() {
        let record_ok = record.unwrap();
        let (source, target, time) = parse_record(&record_ok).expect(&format!("Unable to parse record: {:?}", record_ok));
        let src_id = calculate_hash(&source);
        let dst_id = calculate_hash(&target);
        graph.add_vertex(src_id, time, &vec![]);
        graph.add_vertex(dst_id, time, &vec![]);
        graph.add_edge(src_id, dst_id, time, &vec![]);
    }

    graph
}

// TODO: use different number of partitions using a criterion group

pub fn element_additions(c: &mut Criterion) {
    let mut graph = GraphDB::new(4);
    graph.add_vertex(0, 0, &vec![]);

    c.bench_function("existing vertex addition constant time", |b| b.iter(|| graph.add_vertex(1, 0, &vec![])));

    let mut times: Range<i64> = (0..i64::MAX);
    c.bench_function("existing vertex addition varying time", |b: &mut Bencher| b.iter_batched(|| times.next().unwrap(), |t| graph.add_vertex(0, t, &vec![]), BatchSize::SmallInput));

    let mut indexes: Range<u64> = (0..u64::MAX);
    c.bench_function("new vertex addition constant time", |b: &mut Bencher| b.iter_batched(|| indexes.next().unwrap(), |vid| graph.add_vertex(vid, 0, &vec![]), BatchSize::SmallInput));

    c.bench_function("existing edge addition constant time", |b| b.iter(|| graph.add_edge(0, 1, 0, &vec![])));
}

pub fn edges_len(c: &mut Criterion) {
    let path = data::lotr().unwrap();
    let graph = read_csv(&path, 0, 1, Some(2));

    c.bench_function("edges len", |b| b.iter(|| graph.edges_len()));
}

pub fn degree(c: &mut Criterion) {
    let path = data::lotr().unwrap();
    let graph = read_csv(&path, 0, 1, Some(2));
    let vertex = calculate_hash(&"Frodo");

    c.bench_function("degree", |b| b.iter(|| graph.degree(vertex, Direction::OUT)));
}

pub fn ingestion(c: &mut Criterion) {
    let lotr = data::lotr().unwrap();
    c.bench_function("load lotr.csv", |b: &mut Bencher| b.iter_with_large_drop(|| read_csv(&lotr, 0, 1, Some(2))));

    let twitter = data::twitter().unwrap();
    c.bench_function("load twitter.csv", |b: &mut Bencher| b.iter_with_large_drop(|| read_csv(&twitter, 0, 1, None)));
}

criterion_group!(benches, element_additions, edges_len, degree, ingestion);
criterion_main!(benches);
