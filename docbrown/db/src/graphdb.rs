use std::ops::Range;
use std::path::{Path, PathBuf};

use docbrown_core::{
    graphview::{GraphView, GraphViewInternals, Properties},
    tpartition::{TEdge, TemporalGraphPart},
    utils, Direction, Prop,
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
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].add_vertex(v, t, &props);
    }

    pub fn add_edge(&self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        let src_shard_id = utils::get_shard_id_from_global_vid(src, self.nr_shards);
        let dst_shard_id = utils::get_shard_id_from_global_vid(dst, self.nr_shards);

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
        utils::get_shard_id_from_global_vid(v_gid, self.nr_shards)
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
    fn n_vertices(&self) -> usize {
        self.local_n_vertices()
    }

    fn n_edges(&self) -> usize {
        self.local_n_edges(Direction::OUT)
    }
}

#[cfg(test)]
mod db_tests {
    use csv::StringRecord;
    use docbrown_core::graphview::WindowedView;
    use docbrown_core::utils;
    use docbrown_core::vertexview::VertexViewMethods;
    use itertools::Itertools;
    use quickcheck::{quickcheck, TestResult};
    use rand::Rng;
    use std::fs;
    use std::{path::PathBuf, sync::Arc};
    use uuid::Uuid;
    use docbrown_it::data;

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

    #[quickcheck]
    fn add_vertex_grows_graph_len(vs: Vec<(u8, u8)>) {
        let g = GraphDB::new(2);

        let expected_len = vs.iter().map(|(v, _)| v).sorted().dedup().count();
        for (v, t) in vs {
            g.add_vertex(v.into(), t.into(), &vec![]);
        }

        assert_eq!(g.local_n_vertices(), expected_len)
    }

    #[quickcheck]
    fn add_edge_grows_graph_edge_len(edges: Vec<(u64, u64, i64)>) {
        let nr_shards: usize = 2;

        let g = GraphDB::new(nr_shards);

        let unique_vertices_count = edges
            .iter()
            .map(|(src, dst, _)| vec![src, dst])
            .flat_map(|v| v)
            .sorted()
            .dedup()
            .count();

        let unique_edge_count = edges
            .iter()
            .map(|(src, dst, _)| (src, dst))
            .unique()
            .count();

        for (src, dst, t) in edges {
            g.add_edge(src, dst, t, &vec![]);
        }

        assert_eq!(g.n_vertices(), unique_vertices_count);
        assert_eq!(g.n_edges(), unique_edge_count);
    }

    #[test]
    fn graph_save_to_load_from_file() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = GraphDB::new(2);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let rand_dir = Uuid::new_v4();
        let tmp_docbrown_path = "/tmp/docbrown";
        let shards_path = format!("{}/{}", tmp_docbrown_path, rand_dir);

        // Save to files
        let mut expected = vec![
            format!("{}/shard_1", shards_path),
            format!("{}/shard_0", shards_path),
            format!("{}/graphdb_nr_shards", shards_path),
        ];

        expected.sort();

        match g.save_to_file(&shards_path) {
            Ok(()) => {
                let mut actual = fs::read_dir(&shards_path)
                    .unwrap()
                    .map(|f| f.unwrap().path().display().to_string())
                    .collect::<Vec<_>>();

                actual.sort();

                assert_eq!(actual, expected);
            }
            Err(e) => panic!("{e}"),
        }

        // Load from files
        match GraphDB::load_from_file(Path::new(&shards_path)) {
            Ok(g) => {
                assert!(g.contains_vertex(1));
                assert_eq!(g.nr_shards, 2);
            }
            Err(e) => panic!("{e}"),
        }

        // Delete all files
        fs::remove_dir_all(tmp_docbrown_path).unwrap();
    }

    #[quickcheck]
    fn graph_contains_vertex(vs: Vec<(u64, i64)>) -> TestResult {
        if vs.is_empty() {
            return TestResult::discard();
        }

        let g = GraphDB::new(2);

        let rand_index = rand::thread_rng().gen_range(0..vs.len());
        let rand_vertex = vs.get(rand_index).unwrap().0;

        for (v, t) in vs {
            g.add_vertex(v.into(), t.into(), &vec![]);
        }

        TestResult::from_bool(g.contains_vertex(rand_vertex))
    }

    #[quickcheck]
    fn graph_contains_vertex_window(mut vs: Vec<(i64, u64)>) -> TestResult {
        if vs.is_empty() {
            return TestResult::discard();
        }

        let g = GraphDB::new(2);

        for (t, v) in &vs {
            g.add_vertex(*v, *t, &vec![]);
        }

        vs.sort(); // Sorted by time
        vs.dedup();

        let rand_start_index = rand::thread_rng().gen_range(0..vs.len());
        let rand_end_index = rand::thread_rng().gen_range(0..vs.len());

        if rand_end_index < rand_start_index {
            return TestResult::discard();
        }

        let g = GraphDB::new(2);

        for (t, v) in &vs {
            g.add_vertex(*v, *t, &vec![]);
        }

        let start = vs.get(rand_start_index).unwrap().0;
        let end = vs.get(rand_end_index).unwrap().0;

        if start == end {
            let v = vs.get(rand_start_index).unwrap().1;
            return TestResult::from_bool(!g.contains_vertex_window(v, start..end));
        }

        if rand_start_index == rand_end_index {
            let v = vs.get(rand_start_index).unwrap().1;
            return TestResult::from_bool(!g.contains_vertex_window(v, start..end));
        }

        let rand_index_within_rand_start_end: usize =
            rand::thread_rng().gen_range(rand_start_index..rand_end_index);

        let (i, v) = vs.get(rand_index_within_rand_start_end).unwrap();

        if *i == end {
            return TestResult::from_bool(!g.contains_vertex_window(*v, start..end));
        } else {
            return TestResult::from_bool(g.contains_vertex_window(*v, start..end));
        }
    }

    #[test]
    fn graph_degree() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = GraphDB::new(2);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = vec![(2, 3, 3), (2, 1, 2), (1, 1, 2)];
        let actual = (1..=3)
            .map(|i| {
                let v = g.vertex(i).unwrap();
                (
                    v.clone().in_degree(),
                    v.clone().out_degree(),
                    v.clone().degree(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = GraphDB::new(1);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = (1..=3)
            .map(|i| {
                let v = g.vertex(i).unwrap();
                (
                    v.clone().in_degree(),
                    v.clone().out_degree(),
                    v.clone().degree(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn graph_degree_window() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = GraphDB::new(1);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = vec![(2, 3, 1), (1, 0, 0), (1, 0, 0)];
        let actual = (1..=3)
            .map(|i| {
                let v = g.vertex(i).unwrap();
                (
                    v.clone().with_window(-1..7).in_degree(),
                    v.clone().with_window(1..7).out_degree(),
                    v.clone().with_window(0..1).degree(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = GraphDB::new(3);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = (1..=3)
            .map(|i| {
                let v = g.vertex(i).unwrap();
                (
                    v.clone().with_window(-1..7).in_degree(),
                    v.clone().with_window(1..7).out_degree(),
                    v.clone().with_window(0..1).degree(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn graph_vertices() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = GraphDB::new(1);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let actual = g.vertices().id().collect::<Vec<_>>();
        assert_eq!(actual, vec![1, 2, 3]);

        // Check results from multiple graphs with different number of shards
        let g = GraphDB::new(10);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = g.vertices().id().collect::<Vec<_>>();
        assert_eq!(actual, expected);
    }

    #[test]
    fn graph_neighbours() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = GraphDB::new(12);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = vec![(2, 3, 5), (2, 1, 3), (1, 1, 2)];
        let actual = (1..=3)
            .map(|i| {
                let v = g.vertex(i).unwrap();
                (
                    v.clone().in_edges().count(),
                    v.clone().out_edges().count(),
                    v.clone().edges().count(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = GraphDB::new(1);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = (1..=3)
            .map(|i| {
                let v = g.vertex(i).unwrap();
                (
                    v.clone().in_edges().count(),
                    v.clone().out_edges().count(),
                    v.clone().edges().count(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn graph_neighbours_window() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = GraphDB::new(1);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = vec![(2, 3, 2), (1, 0, 0), (1, 0, 0)];
        let actual = (1..=3)
            .map(|i| {
                let v = g.vertex(i).unwrap();
                (
                    v.clone().with_window(-1..7).in_edges().count(),
                    v.clone().with_window(1..7).out_edges().count(),
                    v.clone().with_window(0..1).edges().count(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = GraphDB::new(10);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = (1..=3)
            .map(|i| {
                let v = g.vertex(i).unwrap();
                (
                    v.clone().with_window(-1..7).in_edges().count(),
                    v.clone().with_window(1..7).out_edges().count(),
                    v.clone().with_window(0..1).edges().count(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn graph_neighbours_window_t() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = GraphDB::new(1);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let in_actual = (1..=3)
            .map(|i| {
                g.neighbours_window_t(i, -1, 7, Direction::IN)
                    .map(|e| e.t.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![-1, 0, 1], vec![1], vec![2]], in_actual);

        let out_actual = (1..=3)
            .map(|i| {
                g.neighbours_window_t(i, 1, 7, Direction::OUT)
                    .map(|e| e.t.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![1, 1, 2], vec![], vec![]], out_actual);

        let both_actual = (1..=3)
            .map(|i| {
                g.neighbours_window_t(i, 0, 1, Direction::BOTH)
                    .map(|e| e.t.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![0, 0], vec![], vec![]], both_actual);

        // Check results from multiple graphs with different number of shards
        let g = GraphDB::new(4);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let in_expected = (1..=3)
            .map(|i| {
                let mut e = g
                    .neighbours_window_t(i, -1, 7, Direction::IN)
                    .map(|e| e.t.unwrap())
                    .collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect::<Vec<_>>();
        assert_eq!(in_expected, in_actual);

        let out_expected = (1..=3)
            .map(|i| {
                let mut e = g
                    .neighbours_window_t(i, 1, 7, Direction::OUT)
                    .map(|e| e.t.unwrap())
                    .collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect::<Vec<_>>();
        assert_eq!(out_expected, out_actual);

        let both_expected = (1..=3)
            .map(|i| {
                let mut e = g
                    .neighbours_window_t(i, 0, 1, Direction::BOTH)
                    .map(|e| e.t.unwrap())
                    .collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect::<Vec<_>>();
        assert_eq!(both_expected, both_actual);
    }

    #[test]
    fn vertices_window() {
        let vs = vec![(1, 2, 1), (3, 4, 3), (5, 6, 5), (7, 1, 7)];

        let args = vec![(i64::MIN..8), (i64::MIN..2), (i64::MIN..4), (3..6)];

        let expected = vec![
            vec![1, 2, 3, 4, 5, 6, 7],
            vec![1, 2],
            vec![1, 2, 3, 4],
            vec![3, 4, 5, 6],
        ];

        let g = GraphDB::new(1);

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let res: Vec<_> = (0..=3)
            .map(|i| {
                let view = WindowedView::new(&g, args[i].clone());
                let mut e = view.vertices().id().collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();

        assert_eq!(res, expected);

        let g = GraphDB::new(3);
        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }
        let res: Vec<_> = (0..=3)
            .map(|i| {
                let view = WindowedView::new(&g, args[i].clone());
                let mut e = view.vertices().id().collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();
        assert_eq!(res, expected);
    }

    #[test]
    fn db_lotr() {
        let g = GraphDB::new(4);

        let data_dir = data::lotr().expect("Failed to get lotr.csv file");

        fn parse_record(rec: &StringRecord) -> Option<(String, String, i64)> {
            let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
            let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
            let t = rec.get(2).and_then(|s| s.parse::<i64>().ok())?;
            Some((src, dst, t))
        }

        if let Ok(mut reader) = csv::Reader::from_path(data_dir) {
            for rec_res in reader.records() {
                if let Ok(rec) = rec_res {
                    if let Some((src, dst, t)) = parse_record(&rec) {
                        let src_id = utils::calculate_hash(&src);
                        let dst_id = utils::calculate_hash(&dst);

                        g.add_vertex(
                            src_id,
                            t,
                            &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                        );
                        g.add_vertex(
                            dst_id,
                            t,
                            &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                        );
                        g.add_edge(
                            src_id,
                            dst_id,
                            t,
                            &vec![(
                                "name".to_string(),
                                Prop::Str("Character Co-occurrence".to_string()),
                            )],
                        );
                    }
                }
            }
        }

        let gandalf = utils::calculate_hash(&"Gandalf");
        assert!(g.contains_vertex(gandalf));
    }
}
