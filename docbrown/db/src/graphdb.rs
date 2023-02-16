use std::ops::Range;
use std::path::{Path, PathBuf};

use docbrown_core::{
    graphview::{GraphView, GraphViewInternals, Properties},
    tpartition::{TEdge, TemporalGraphPart},
    Direction, Prop,
};

use docbrown_core::graph::TemporalGraph;
use docbrown_core::graphview::{EdgeIterator, NeighboursIterator, PropertyHistory, VertexIterator};
use docbrown_core::vertexview::{VertexPointer, VertexView};
use polars;
use rayon::prelude::{IntoParallelRefIterator, ParallelIterator};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphDB {
    nr_shards: usize,
    shards: Vec<TemporalGraphPart>,
}

impl GraphDB {
    pub fn new(nr_shards: usize) -> Self {
        GraphDB {
            nr_shards,
            shards: (0..nr_shards)
                .map(|_| TemporalGraphPart::default())
                .collect(),
        }
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<bincode::ErrorKind>> {
        // use BufReader for better performance

        println!("loading from {:?}", path.as_ref());
        let mut p = PathBuf::from(path.as_ref());
        p.push("graphdb_nr_shards");

        let f = std::fs::File::open(p).unwrap();
        let mut reader = std::io::BufReader::new(f);
        let nr_shards = bincode::deserialize_from(&mut reader)?;

        let mut shard_paths = vec![];
        for i in 0..nr_shards {
            let mut p = PathBuf::from(path.as_ref());
            p.push(format!("shard_{}", i));
            shard_paths.push((i, p));
        }
        let mut shards = shard_paths
            .par_iter()
            .map(|(i, path)| {
                let shard = TemporalGraphPart::load_from_file(path)?;
                Ok((*i, shard))
            })
            .collect::<Result<Vec<_>, Box<bincode::ErrorKind>>>()?;

        shards.sort_by_cached_key(|(i, _)| *i);

        let shards = shards.into_iter().map(|(_, shard)| shard).collect();

        Ok(GraphDB { nr_shards, shards })
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<bincode::ErrorKind>> {
        // write each shard to a different file

        // crate directory path if it doesn't exist
        std::fs::create_dir_all(path.as_ref())?;

        let mut shard_paths = vec![];
        for i in 0..self.nr_shards {
            let mut p = PathBuf::from(path.as_ref());
            p.push(format!("shard_{}", i));
            println!("saving shard {} to {:?}", i, p);
            shard_paths.push((i, p));
        }
        shard_paths
            .par_iter()
            .try_for_each(|(i, path)| self.shards[*i].save_to_file(path))?;

        let mut p = PathBuf::from(path.as_ref());
        p.push("graphdb_nr_shards");

        let f = std::fs::File::create(p)?;
        let writer = std::io::BufWriter::new(f);
        bincode::serialize_into(writer, &self.nr_shards)?;
        Ok(())
    }

    // TODO: Probably add vector reference here like add
    pub fn add_vertex(&self, v: u64, t: i64, props: &Vec<(String, Prop)>) {
        let shard_id = self.get_shard_id_from_global_vid(v);
        self.shards[shard_id].add_vertex(v, t, &props);
    }

    pub fn add_edge(&self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        let src_shard_id = self.get_shard_id_from_global_vid(src);
        let dst_shard_id = self.get_shard_id_from_global_vid(dst);

        if src_shard_id == dst_shard_id {
            self.shards[src_shard_id].add_edge(src, dst, t, props)
        } else {
            // FIXME these are sort of connected, we need to hold both locks for
            // the src partition and dst partition to add a remote edge between both
            self.shards[src_shard_id].add_edge_remote_out(src, dst, t, props);
            self.shards[dst_shard_id].add_edge_remote_into(src, dst, t, props);
        }
    }

    pub fn neighbours_window_t(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = TEdge>> {
        let shard_id = self.get_shard_id_from_global_vid(v);

        let iter = self.shards[shard_id].neighbours_window_t(v, t_start, t_end, d);

        Box::new(iter)
    }

    #[inline(always)]
    fn get_shard_id_from_global_vid(&self, v_gid: u64) -> usize {
        let a: usize = v_gid.try_into().unwrap();
        a % self.nr_shards
    }
}

impl GraphViewInternals for GraphDB {
    fn local_n_vertices(&self) -> usize {
        self.shards.iter().map(|g| g.local_n_vertices()).sum()
    }

    fn local_n_edges(&self, direction: Direction) -> usize {
        self.shards.iter().map(|g| g.local_n_edges(direction)).sum()
    }

    fn local_n_vertices_window(&self, w: Range<i64>) -> usize {
        self.shards
            .iter()
            .map(|g| g.local_n_vertices_window(w.clone()))
            .sum()
    }

    fn local_n_edges_window(&self, w: Range<i64>, direction: Direction) -> usize {
        self.shards
            .iter()
            .map(|g| g.local_n_edges_window(w.clone(), direction))
            .sum()
    }

    fn local_vertex(&self, gid: u64) -> Option<VertexView<Self>> {
        let sid = self.get_shard_id_from_global_vid(gid);
        self.shards[sid]
            .local_vertex(gid)
            .map(|v| v.as_view_of(self))
    }

    fn local_vertex_window(&self, gid: u64, w: Range<i64>) -> Option<VertexView<Self>> {
        let sid = self.get_shard_id_from_global_vid(gid);
        self.shards[sid]
            .local_vertex_window(gid, w)
            .map(|v| v.as_view_of(self))
    }

    fn local_contains_vertex(&self, gid: u64) -> bool {
        let sid = self.get_shard_id_from_global_vid(gid);
        self.shards[sid].local_contains_vertex(gid)
    }

    fn local_contains_vertex_window(&self, gid: u64, w: Range<i64>) -> bool {
        let sid = self.get_shard_id_from_global_vid(gid);
        self.shards[sid].local_contains_vertex_window(gid, w)
    }

    fn iter_local_vertices(&self) -> VertexIterator<Self> {
        Box::new(
            self.shards
                .iter()
                .flat_map(|g| g.iter_local_vertices().map(|v| v.as_view_of(self))),
        )
    }

    fn iter_local_vertices_window(&self, window: Range<i64>) -> VertexIterator<Self> {
        Box::new(self.shards.iter().flat_map(move |g| {
            g.iter_local_vertices_window(window.clone())
                .map(|v| v.as_view_of(self))
        }))
    }

    fn degree(&self, vertex: VertexPointer, direction: Direction) -> usize {
        let sid = self.get_shard_id_from_global_vid(vertex.gid);
        self.shards[sid].degree(vertex, direction)
    }

    fn neighbours<'a>(
        &'a self,
        vertex: VertexPointer,
        direction: Direction,
    ) -> NeighboursIterator<'a, Self> {
        let sid = self.get_shard_id_from_global_vid(vertex.gid);
        Box::new(
            self.shards[sid]
                .neighbours(vertex, direction)
                .map(|v| v.as_view_of(self)),
        )
    }

    fn edges<'a>(&'a self, vertex: VertexPointer, direction: Direction) -> EdgeIterator<'a, Self> {
        let sid = self.get_shard_id_from_global_vid(vertex.gid);
        Box::new(
            self.shards[sid]
                .edges(vertex, direction)
                .map(|v| v.as_view_of(self)),
        )
    }

    fn property_history<'a>(
        &'a self,
        vertex: VertexPointer,
        name: &'a str,
    ) -> Option<PropertyHistory<'a>> {
        let sid = self.get_shard_id_from_global_vid(vertex.gid);
        self.shards[sid].property_history(vertex, name)
    }
}

impl GraphView for GraphDB {
    fn n_nodes(&self) -> usize {
        self.local_n_vertices()
    }

    fn n_edges(&self) -> usize {
        self.local_n_edges(Direction::OUT)
    }
}

#[cfg(test)]
mod db_tests {
    use csv::StringRecord;
    use itertools::Itertools;

    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        path::PathBuf,
        sync::Arc,
    };

    use super::*;

    #[test]
    fn cloning_vec() {
        let mut vs = vec![];
        for i in 0..10 {
            vs.push(Arc::new(i))
        }
        let should_be_10: usize = vs.iter().map(|arc| Arc::strong_count(arc)).sum();
        assert_eq!(should_be_10, 10);

        let vs2 = vs.clone();

        let should_be_10: usize = vs2.iter().map(|arc| Arc::strong_count(arc)).sum();
        assert_eq!(should_be_10, 20)
    }

    #[test]
    fn basic_additions_to_graph_len() {
        let graph = GraphDB::new(4);
        assert_eq!(graph.n_nodes(), 0);
        assert_eq!(graph.local_n_edges(Direction::BOTH), 0);
        graph.add_edge(1, 2, 0, &Vec::new());
        graph.add_edge(1, 3, 0, &Vec::new());
        assert_eq!(graph.n_nodes(), 3);
        assert_eq!(graph.n_edges(), 2);
    }

    #[quickcheck]
    fn add_vertex_to_graph_len_grows(vs: Vec<(u8, u8)>) {
        let g = GraphDB::new(2);

        let expected_len = vs.iter().map(|(v, _)| v).sorted().dedup().count();
        for (v, t) in vs {
            g.add_vertex(v.into(), t.into(), &vec![]);
        }

        assert_eq!(g.n_nodes(), expected_len)
    }

    #[test]
    fn db_lotr() {
        let g = GraphDB::new(4);

        fn calculate_hash<T: Hash>(t: &T) -> u64 {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish()
        }

        fn parse_record(rec: &StringRecord) -> Option<(String, String, i64)> {
            let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
            let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
            let t = rec.get(2).and_then(|s| s.parse::<i64>().ok())?;
            Some((src, dst, t))
        }

        let lotr_csv: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources/test/lotr.csv"]
            .iter()
            .collect();

        let empty: Vec<(String, Prop)> = vec![]; // FIXME: add actual properties here

        if let Ok(mut reader) = csv::Reader::from_path(lotr_csv) {
            for rec_res in reader.records() {
                if let Ok(rec) = rec_res {
                    if let Some((src, dst, t)) = parse_record(&rec) {
                        let src_id = calculate_hash(&src);
                        let dst_id = calculate_hash(&dst);

                        g.add_vertex(src_id, t, &vec![]);
                        g.add_vertex(dst_id, t, &vec![]);
                        g.add_edge(src_id, dst_id, t, &empty);
                    }
                }
            }
        }
    }
}
