use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use genawaiter::rc::gen;
use genawaiter::yield_;

use crate::graph::{EdgeView, TemporalGraph};
use crate::graphview::{EdgeIterator, GraphViewInternals, PropertyHistory, VertexIterator};
use crate::vertexview::VertexView;
use crate::{Direction, Prop};
use itertools::*;

#[derive(Debug)]
pub struct TEdge {
    src: u64,
    dst: u64,
    // edge_meta_id: AdjEdge,
    t: Option<i64>,
    is_remote: bool,
}

impl<'a> From<EdgeView<'a, TemporalGraph>> for TEdge {
    fn from(e: EdgeView<'a, TemporalGraph>) -> Self {
        Self {
            src: e.global_src(),
            dst: e.global_dst(),
            t: e.time(),
            is_remote: e.is_remote(),
        }
    }
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[repr(transparent)]
pub struct TemporalGraphPart(pub Arc<RwLock<TemporalGraph>>);

impl TemporalGraphPart {
    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<bincode::ErrorKind>> {
        // use BufReader for better performance
        let f = std::fs::File::open(path).unwrap();
        let mut reader = std::io::BufReader::new(f);
        bincode::deserialize_from(&mut reader)
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<bincode::ErrorKind>> {
        // use BufWriter for better performance
        let f = std::fs::File::create(path).unwrap();
        let mut writer = std::io::BufWriter::new(f);
        bincode::serialize_into(&mut writer, self)
    }

    #[inline(always)]
    fn write_shard<A, F>(&self, f: F) -> A
    where
        F: Fn(&mut TemporalGraph) -> A,
    {
        let mut shard = self.0.write();
        f(&mut shard)
    }

    #[inline(always)]
    fn read_shard<A, F>(&self, f: F) -> A
    where
        F: Fn(&TemporalGraph) -> A,
    {
        let shard = self.0.read();
        f(&shard)
    }

    pub fn add_vertex(&self, v: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_vertex(v, t))
    }

    pub fn add_edge(&self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_with_props(src, dst, t, props))
    }

    pub fn add_edge_remote_out(&self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_remote_out(src, dst, t, props))
    }

    pub fn add_edge_remote_into(&self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_remote_into(src, dst, t, props))
    }
}

impl GraphViewInternals for TemporalGraphPart {
    fn local_n_vertices(&self) -> usize {
        self.read_shard(|tg| tg.local_n_vertices())
    }

    fn contains_vertex(&self, gid: u64) -> bool {
        self.read_shard(|tg| tg.contains_vertex(gid))
    }

    fn contains_vertex_window(&self, gid: u64, w: Range<i64>) -> bool {
        self.read_shard(|tg| tg.contains_vertex_window(gid, w))
    }

    fn degree(&self, v: u64, d: Direction) -> usize {
        self.read_shard(|tg: &TemporalGraph| tg.degree(v, d))
    }

    // TODO: check if there is any value in returning Vec<usize> vs just usize, what is the cost of the generator
    pub fn vertices_window(
        &self,
        t_start: i64,
        t_end: i64,
        chunk_size: usize,
    ) -> impl Iterator<Item = Vec<usize>> {
        let tg = self.clone();
        let vertices_iter = gen!({
            let g = tg.0.read();
            let chunks = (*g).vertices_window(t_start..t_end).chunks(chunk_size);
            let iter = chunks.into_iter().map(|chunk| chunk.collect::<Vec<_>>());
            for v_id in iter {
                yield_!(v_id)
            }
        });

        vertices_iter.into_iter()
    }

    pub fn neighbours(&self, v: u64, d: Direction) -> impl Iterator<Item = TEdge> {
        let tg = self.clone();
        let vertices_iter = gen!({
            let g = tg.0.read();
            let chunks = (*g).neighbours(v, d).map(|e| e.into());
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        vertices_iter.into_iter()
    }

    pub fn neighbours_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> impl Iterator<Item = TEdge> {
        let tg = self.clone();
        let vertices_iter = gen!({
            let g = tg.0.read();
            let chunks = (*g)
                .neighbours_window(v, &(t_start..t_end), d)
                .map(|e| e.into());
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        vertices_iter.into_iter()
    }

    pub fn neighbours_window_t(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> impl Iterator<Item = TEdge> {
        let tg = self.clone();
        let vertices_iter = gen!({
            let g = tg.0.read();
            let chunks = (*g)
                .neighbours_window_t(v, &(t_start..t_end), d)
                .map(|e| e.into());
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        vertices_iter.into_iter()
    }
}

impl GraphViewInternals for TemporalGraphPart {
    fn local_n_vertices(&self) -> usize {
        todo!()
    }

    fn local_n_edges(&self, direction: Direction) -> usize {
        todo!()
    }

    fn local_n_vertices_window(&self, w: Range<i64>) -> usize {
        todo!()
    }

    fn local_n_edges_window(&self, w: Range<i64>, direction: Direction) -> usize {
        todo!()
    }

    fn vertex(&self, gid: u64) -> Option<VertexView<Self>> {
        todo!()
    }

    fn vertex_window(&self, gid: u64, w: Range<i64>) -> Option<VertexView<Self>> {
        todo!()
    }

    fn iter_vertices(&self) -> VertexIterator<Self> {
        Box::new(
            self.read_shard(|g| g.iter_vertices())
                .map(move |vertex| self.move_vertex_up(vertex)),
        )
    }

    fn iter_vertices_window(&self, window: Range<i64>) -> VertexIterator<Self> {
        Box::new(
            self.read_shard(|g| g.iter_vertices_window(window))
                .map(move |vertex| self.move_vertex_up(vertex)),
        )
    }

    fn degree(&self, vertex: &VertexView<Self>, direction: Direction) -> usize {
        self.read_shard(|g| g.degree(vertex.as_view_of(g), direction))
    }

    fn neighbours(&self, vertex: &VertexView<Self>, direction: Direction) -> VertexIterator<Self> {
        todo!()
    }

    fn edges(&self, vertex: &VertexView<Self>, direction: Direction) -> EdgeIterator<Self> {
        todo!()
    }

    fn property_history<'a>(
        &'a self,
        vertex: &VertexView<Self>,
        name: &str,
    ) -> Option<PropertyHistory<'a>> {
        todo!()
    }
}

#[cfg(test)]
mod temporal_graph_partition_test {
    use crate::Direction;

    use super::TemporalGraphPart;
    use itertools::Itertools;
    use quickcheck::Arbitrary;

    // non overlaping time intervals
    #[derive(Clone, Debug)]
    struct Intervals(Vec<(i64, i64)>);

    impl Arbitrary for Intervals {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let mut some_nums = Vec::<i64>::arbitrary(g);
            some_nums.sort();
            let intervals = some_nums
                .into_iter()
                .tuple_windows()
                .filter(|(a, b)| a != b)
                .collect_vec();
            Intervals(intervals)
        }
    }

    #[quickcheck]
    fn add_vertex_to_graph_len_grows(vs: Vec<(u8, u8)>) {
        let g = TemporalGraphPart::default();

        let expected_len = vs.iter().map(|(v, _)| v).sorted().dedup().count();
        for (v, t) in vs {
            g.add_vertex(v.into(), t.into(), &vec![]);
        }

        assert_eq!(g.len(), expected_len)
    }

    // add one single vertex per interval
    // then go through each window
    // and select the vertices
    // should recover each inserted vertex exactly once
    #[quickcheck]
    fn iterate_vertex_windows(intervals: Intervals) {
        let g = TemporalGraphPart::default();

        for (v, (t_start, _)) in intervals.0.iter().enumerate() {
            g.add_vertex(v.try_into().unwrap(), *t_start, &vec![])
        }

        for (v, (t_start, t_end)) in intervals.0.iter().enumerate() {
            let vertex_window = g.vertices_window(*t_start, *t_end, 1);
            let iter = &mut vertex_window.into_iter().flatten();
            let v_actual = iter.next();
            assert_eq!(Some(v), v_actual);
            assert_eq!(None, iter.next()); // one vertex per interval
        }
    }

    #[test]
    fn get_in_degree_window() {
        let g = TemporalGraphPart::default();

        g.add_vertex(100, 1, &vec![]);
        g.add_vertex(101, 2, &vec![]);
        g.add_vertex(102, 3, &vec![]);
        g.add_vertex(103, 4, &vec![]);
        g.add_vertex(104, 5, &vec![]);
        g.add_vertex(105, 5, &vec![]);

        g.add_edge(100, 101, 6, &vec![]);
        g.add_edge(100, 102, 7, &vec![]);
        g.add_edge(101, 103, 8, &vec![]);
        g.add_edge(102, 104, 9, &vec![]);
        g.add_edge(110, 104, 9, &vec![]);

        assert_eq!(g.degree_window(101, 0, i64::MAX, Direction::IN), 1);
        assert_eq!(g.degree_window(100, 0, i64::MAX, Direction::IN), 0);
        assert_eq!(g.degree_window(101, 0, 1, Direction::IN), 0);
        assert_eq!(g.degree_window(101, 10, 20, Direction::IN), 0);
        assert_eq!(g.degree_window(105, 0, i64::MAX, Direction::IN), 0);
        assert_eq!(g.degree_window(104, 0, i64::MAX, Direction::IN), 2)
    }

    #[test]
    fn get_out_degree_window() {
        let g = TemporalGraphPart::default();

        g.add_vertex(100, 1, &vec![]);
        g.add_vertex(101, 2, &vec![]);
        g.add_vertex(102, 3, &vec![]);
        g.add_vertex(103, 4, &vec![]);
        g.add_vertex(104, 5, &vec![]);
        g.add_vertex(105, 5, &vec![]);

        g.add_edge(100, 101, 6, &vec![]);
        g.add_edge(100, 102, 7, &vec![]);
        g.add_edge(101, 103, 8, &vec![]);
        g.add_edge(102, 104, 9, &vec![]);
        g.add_edge(110, 104, 9, &vec![]);

        assert_eq!(g.degree_window(101, 0, i64::MAX, Direction::OUT), 1);
        assert_eq!(g.degree_window(103, 0, i64::MAX, Direction::OUT), 0);
        assert_eq!(g.degree_window(105, 0, i64::MAX, Direction::OUT), 0);
        assert_eq!(g.degree_window(101, 0, 1, Direction::OUT), 0);
        assert_eq!(g.degree_window(101, 10, 20, Direction::OUT), 0);
        assert_eq!(g.degree_window(100, 0, i64::MAX, Direction::OUT), 2)
    }

    #[test]
    fn get_degree_window() {
        let g = TemporalGraphPart::default();

        g.add_vertex(100, 1, &vec![]);
        g.add_vertex(101, 2, &vec![]);
        g.add_vertex(102, 3, &vec![]);
        g.add_vertex(103, 4, &vec![]);
        g.add_vertex(104, 5, &vec![]);
        g.add_vertex(105, 5, &vec![]);

        g.add_edge(100, 101, 6, &vec![]);
        g.add_edge(100, 102, 7, &vec![]);
        g.add_edge(100, 102, 8, &vec![]);
        g.add_edge(101, 103, 8, &vec![]);
        g.add_edge(102, 104, 9, &vec![]);
        g.add_edge(110, 104, 9, &vec![]);

        assert_eq!(g.degree_window(101, 0, i64::MAX, Direction::BOTH), 2);
        assert_eq!(g.degree_window(100, 0, i64::MAX, Direction::BOTH), 2);
        assert_eq!(g.degree_window(100, 0, 1, Direction::BOTH), 0);
        assert_eq!(g.degree_window(100, 10, 20, Direction::BOTH), 0);
        assert_eq!(g.degree_window(105, 0, i64::MAX, Direction::BOTH), 0)
    }
}
