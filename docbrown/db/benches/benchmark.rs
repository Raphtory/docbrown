use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::path::Path;
use criterion::{black_box, criterion_group, criterion_main, Criterion, Bencher};
use csv::StringRecord;
use docbrown_core::Direction;
use docbrown_db::GraphDB;

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}

fn read_csv(filepath: &str, source: usize, target: usize, time: usize) -> GraphDB {
    let parse_record = |rec: &StringRecord| {
        let source_value: String = rec.get(source).unwrap().parse().unwrap();
        let target_value: String = rec.get(target).unwrap().parse().unwrap();
        let time_value: i64 = rec.get(time).unwrap().parse().unwrap();
        (source_value, target_value, time_value)
    };

    let path = Path::new(filepath);
    let mut reader = csv::Reader::from_path(path).unwrap();
    let graph = GraphDB::new(4);

    for record in reader.records() {
        let (source, target, time) = parse_record(&record.unwrap());
        let src_id = calculate_hash(&source);
        let dst_id = calculate_hash(&target);
        graph.add_vertex(src_id, time, vec![]);
        graph.add_vertex(dst_id, time, vec![]);
        graph.add_edge(src_id, dst_id, time, &vec![]);
    }

    graph
}

pub fn edges_len(c: &mut Criterion) {
    let filepath = "resources/test/lotr.csv";
    let graph = read_csv(filepath, 0, 1, 2);

    c.bench_function("edges len", |b| b.iter(|| graph.edges_len()));
}

pub fn neighbour_benchmark(c: &mut Criterion) {
    let filepath = "resources/test/lotr.csv";
    let graph = read_csv(filepath, 0, 1, 2);
    let vertex = calculate_hash(&"Frodo");

    c.bench_function("neighbour window", |b| b.iter(|| graph.neighbours_window(1000, 2000, vertex, Direction::OUT)));
}

pub fn ingestion_benchmark(c: &mut Criterion) {
    let filepath = "resources/test/lotr.csv";
    c.bench_function("read csv", |b: &mut Bencher| b.iter_with_large_drop(|| read_csv(filepath, 0, 1, 2)));
}

criterion_group!(benches, neighbour_benchmark, edges_len, ingestion_benchmark);
criterion_main!(benches);
