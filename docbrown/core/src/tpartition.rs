use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use genawaiter::rc::gen;
use genawaiter::yield_;

use crate::graph::{EdgeView, TemporalGraph};
use crate::graphview::{
    EdgeIterator, GraphViewInternals, NeighboursIterator, PropertyHistory, VertexIterator,
};
use crate::vertexview::{VertexPointer, VertexView, VertexViewMethods};
use crate::{Direction, Prop};
use itertools::*;

#[derive(Debug)]
pub struct TEdge {
    src: u64,
    dst: u64,
    // edge_meta_id: AdjEdge,
    pub t: Option<i64>,
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

// FIXME: This implementation is currently asking for deadlocks when using the iterators as we acquire read locks while still holding a read lock.
// Probably, the best option is to create read and write locked views of the graph which hold on to the lock guard and then allow working with the iterators without problems.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[repr(transparent)]
pub struct TemporalGraphPart(Arc<RwLock<TemporalGraph>>);

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

    /// FIXME: Trait for this
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
        self.read_shard(|tg| tg.local_n_vertices())
    }

    fn local_vertex(&self, gid: u64) -> Option<VertexView<Self>> {
        self.read_shard(|g| g.local_vertex(gid).map(move |v| v.as_view_of(self)))
    }

    fn local_vertex_window(&self, gid: u64, w: Range<i64>) -> Option<VertexView<Self>> {
        self.read_shard(|g| {
            g.local_vertex_window(gid, w.clone())
                .map(|v| v.as_view_of(self))
        })
    }

    fn local_contains_vertex(&self, gid: u64) -> bool {
        self.read_shard(|tg| tg.local_contains_vertex(gid))
    }

    fn local_contains_vertex_window(&self, gid: u64, w: Range<i64>) -> bool {
        self.read_shard(|tg| tg.local_contains_vertex_window(gid, w.clone()))
    }

    fn iter_local_vertices(&self) -> VertexIterator<Self> {
        let vertex_iter = gen!({
            let g = self.0.read();
            let iter = (*g).iter_local_vertices().map(move |v| v.as_view_of(self));
            for v in iter {
                yield_!(v)
            }
        });
        Box::new(vertex_iter.into_iter())
    }

    // TODO: check if there is any value in returning Vec<usize> vs just usize, what is the cost of the generator
    fn iter_local_vertices_window(&self, window: Range<i64>) -> VertexIterator<Self> {
        let vertex_iter = gen!({
            let g = self.0.read();
            let iter = (*g)
                .iter_local_vertices_window(window)
                .map(|v| v.as_view_of(self));
            for v in iter {
                yield_!(v)
            }
        });
        Box::new(vertex_iter.into_iter())
    }

    fn degree(&self, vertex: VertexPointer, direction: Direction) -> usize {
        self.read_shard(|g| g.degree(vertex.clone(), direction))
    }

    fn neighbours(&self, vertex: VertexPointer, direction: Direction) -> NeighboursIterator<Self> {
        let vertex_iter = gen!({
            let g = self.0.read();
            let iter = (*g)
                .neighbours(vertex, direction)
                .map(|v| v.as_view_of(self));
            for v in iter {
                yield_!(v)
            }
        });
        Box::new(vertex_iter.into_iter())
    }

    fn edges(&self, vertex: VertexPointer, direction: Direction) -> EdgeIterator<Self> {
        let edge_iter = gen!({
            let g = self.0.read();
            let iter = (*g).edges(vertex, direction).map(|v| v.as_view_of(self));
            for v in iter {
                yield_!(v)
            }
        });
        Box::new(edge_iter.into_iter())
    }

    fn property_history<'a>(
        &'a self,
        vertex: VertexPointer,
        name: &'a str,
    ) -> Option<PropertyHistory<'a>> {
        self.read_shard(|g| g.property_history(vertex.clone(), name))
    }
}

#[cfg(test)]
mod temporal_graph_partition_test {
    use crate::Direction;

    use super::TemporalGraphPart;
    use crate::graphview::GraphViewInternals;
    use crate::vertexview::VertexViewMethods;
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
    fn shard_contains_vertex(vs: Vec<(u64, i64)>) -> TestResult {
        if vs.is_empty() {
            return TestResult::discard();
        }

        let g = TemporalGraphPart::default();

        let rand_index = rand::thread_rng().gen_range(0..vs.len());
        let rand_vertex = vs.get(rand_index).unwrap().0;

        for (v, t) in vs {
            g.add_vertex(v.into(), t.into(), &vec![]);
        }

        TestResult::from_bool(g.contains(rand_vertex))
    }

    #[test]
    fn shard_contains_vertex_window() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        assert!(g.contains_window(1, -1, 7));
        assert!(!g.contains_window(2, 0, 1));
        assert!(g.contains_window(3, 0, 8));
    }

    #[quickcheck]
    fn add_vertex_to_shard_len_grows(vs: Vec<(u8, u8)>) {
        let g = TemporalGraphPart::default();

        let expected_len = vs.iter().map(|(v, _)| v).sorted().dedup().count();
        for (v, t) in vs {
            g.add_vertex(v.into(), t.into(), &vec![]);
        }

        assert_eq!(g.local_n_vertices(), expected_len)
    }

    #[test]
    fn shard_vertices() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let actual = g.vertices().collect::<Vec<_>>();
        assert_eq!(actual, vec![1, 2, 3]);
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
            let mut vertex_window = g.iter_local_vertices_window(*t_start..*t_end);
            let v_actual = vertex_window.next().map(|v| v.gid);
            assert_eq!(Some(v.try_into().unwrap()), v_actual);
            assert!(vertex_window.next().is_none()); // one vertex per interval
        }
    }

    #[test]
    fn get_shard_degree() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = vec![(2, 3, 3), (2, 1, 2), (1, 1, 2)];
        let actual = (1..=3)
            .map(|i| {
                (
                    g.degree(i, Direction::IN),
                    g.degree(i, Direction::OUT),
                    g.degree(i, Direction::BOTH),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);

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

        let v100 = g.local_vertex(100).unwrap();
        let v101 = g.local_vertex(101).unwrap();
        let v104 = g.local_vertex(104).unwrap();
        let v105 = g.local_vertex(105).unwrap();

        assert_eq!(
            g.degree(v101.as_pointer().with_window(0..i64::MAX), Direction::IN),
            1
        );
        assert_eq!(
            g.degree(v100.as_pointer().with_window(0..i64::MAX), Direction::IN),
            0
        );
        assert_eq!(
            g.degree(v101.as_pointer().with_window(0..1), Direction::IN),
            0
        );
        assert_eq!(
            g.degree(v101.as_pointer().with_window(10..20), Direction::IN),
            0
        );
        assert_eq!(
            g.degree(v105.as_pointer().with_window(0..i64::MAX), Direction::IN),
            0
        );
        assert_eq!(
            g.degree(v104.as_pointer().with_window(0..i64::MAX), Direction::IN),
            2
        )
    }

    #[test]
    fn get_shard_degree_window() {
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

        let v100 = g.local_vertex(100).unwrap();
        let v101 = g.local_vertex(101).unwrap();
        let v103 = g.local_vertex(103).unwrap();
        let v105 = g.local_vertex(105).unwrap();

        assert_eq!(g.degree_window(101, 0, i64::MAX, Direction::IN), 1);
        assert_eq!(g.degree_window(100, 0, i64::MAX, Direction::IN), 0);
        assert_eq!(g.degree_window(101, 0, 1, Direction::IN), 0);
        assert_eq!(g.degree_window(101, 10, 20, Direction::IN), 0);
        assert_eq!(g.degree_window(105, 0, i64::MAX, Direction::IN), 0);
        assert_eq!(g.degree_window(104, 0, i64::MAX, Direction::IN), 2);

        assert_eq!(g.degree_window(101, 0, i64::MAX, Direction::OUT), 1);
        assert_eq!(g.degree_window(103, 0, i64::MAX, Direction::OUT), 0);
        assert_eq!(g.degree_window(105, 0, i64::MAX, Direction::OUT), 0);
        assert_eq!(g.degree_window(101, 0, 1, Direction::OUT), 0);
        assert_eq!(g.degree_window(101, 10, 20, Direction::OUT), 0);
        assert_eq!(g.degree_window(100, 0, i64::MAX, Direction::OUT), 2);

        assert_eq!(g.degree_window(101, 0, i64::MAX, Direction::BOTH), 2);
        assert_eq!(g.degree_window(100, 0, i64::MAX, Direction::BOTH), 2);
        assert_eq!(g.degree_window(100, 0, 1, Direction::BOTH), 0);
        assert_eq!(g.degree_window(100, 10, 20, Direction::BOTH), 0);
        assert_eq!(g.degree_window(105, 0, i64::MAX, Direction::BOTH), 0);

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

        let v100 = g.local_vertex(100).unwrap();
        let v101 = g.local_vertex(101).unwrap();
        let v105 = g.local_vertex(105).unwrap();
        assert_eq!(g.degree(v101.as_pointer(), Direction::BOTH), 2);
        assert_eq!(g.degree(v100.as_pointer(), Direction::BOTH), 2);
        assert_eq!(
            g.degree(v100.as_pointer().with_window(0..1), Direction::BOTH),
            0
        );
        assert_eq!(
            g.degree(v100.as_pointer().with_window(10..20), Direction::BOTH),
            0
        );
        assert_eq!(g.degree(v105.as_pointer(), Direction::BOTH), 0)
    }

    #[test]
    fn get_shard_neighbours() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = vec![(2, 3, 5), (2, 1, 3), (1, 1, 2)];
        let actual = (1..=3)
            .map(|i| {
                (
                    g.neighbours(i, Direction::IN).collect::<Vec<_>>().len(),
                    g.neighbours(i, Direction::OUT).collect::<Vec<_>>().len(),
                    g.neighbours(i, Direction::BOTH).collect::<Vec<_>>().len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn get_shard_neighbours_window() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }

        let expected = vec![(2, 3, 2), (1, 0, 0), (1, 0, 0)];
        let actual = (1..=3)
            .map(|i| {
                (
                    g.neighbours_window(i, -1, 7, Direction::IN)
                        .collect::<Vec<_>>()
                        .len(),
                    g.neighbours_window(i, 1, 7, Direction::OUT)
                        .collect::<Vec<_>>()
                        .len(),
                    g.neighbours_window(i, 0, 1, Direction::BOTH)
                        .collect::<Vec<_>>()
                        .len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn get_shard_neighbours_window_t() {
        let vs = vec![
            (1, 2, 1),
            (1, 3, 2),
            (2, 1, -1),
            (1, 1, 0),
            (3, 2, 7),
            (1, 1, 1),
        ];

        let g = TemporalGraphPart::default();

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
    }
}
