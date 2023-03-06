use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;

use genawaiter::sync::{gen, GenBoxed};
use genawaiter::yield_;

use crate::tgraph::{EdgeView, TemporalGraph, VertexView};
use crate::{Direction, Prop};

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
#[repr(transparent)]
pub struct TGraphShard {
    #[serde(with = "arc_rwlock_serde")]
    rc: Arc<tokio::sync::RwLock<TemporalGraph>>,
}

mod arc_rwlock_serde {
    use serde::de::Deserializer;
    use serde::ser::Serializer;
    use serde::{Deserialize, Serialize};
    use std::sync::Arc;

    pub fn serialize<S, T>(val: &Arc<tokio::sync::RwLock<T>>, s: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
        T: Serialize,
    {
        T::serialize(&*val.blocking_read(), s)
    }

    pub fn deserialize<'de, D, T>(d: D) -> Result<Arc<tokio::sync::RwLock<T>>, D::Error>
    where
        D: Deserializer<'de>,
        T: Deserialize<'de>,
    {
        Ok(Arc::new(tokio::sync::RwLock::new(T::deserialize(d)?)))
    }
}

impl TGraphShard {
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
        let mut shard = self.rc.blocking_write();
        f(&mut shard)
    }

    #[inline(always)]
    fn read_shard<A, F>(&self, f: F) -> A
    where
        F: Fn(&TemporalGraph) -> A,
    {
        let shard = self.rc.blocking_read();
        f(&shard)
    }

    pub fn len(&self) -> usize {
        self.read_shard(|tg| tg.len())
    }

    pub fn out_edges_len(&self) -> usize {
        self.read_shard(|tg| tg.out_edges_len())
    }

    pub fn has_edge(&self, v1: u64, v2: u64) -> bool {
        self.read_shard(|tg| tg.has_edge(v1, v2))
    }

    pub fn has_vertex(&self, v: u64) -> bool {
        self.read_shard(|tg| tg.has_vertex(v))
    }

    pub fn has_vertex_window(&self, v: u64, w: Range<i64>) -> bool {
        self.read_shard(|tg| tg.has_vertex_window(&w, v))
    }

    pub fn add_vertex(&self, t: i64, v: u64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_vertex_with_props(t, v, props))
    }

    pub fn add_edge(&self, t: i64, src: u64, dst: u64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_with_props(t, src, dst, props))
    }

    pub fn add_edge_remote_out(&self, t: i64, src: u64, dst: u64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_remote_out(t, src, dst, props))
    }

    pub fn add_edge_remote_into(&self, t: i64, src: u64, dst: u64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_remote_into(t, src, dst, props))
    }

    pub fn degree(&self, v: u64, d: Direction) -> usize {
        self.read_shard(|tg: &TemporalGraph| tg.degree(v, d))
    }

    pub fn degree_window(&self, v: u64, w: Range<i64>, d: Direction) -> usize {
        self.read_shard(|tg: &TemporalGraph| tg.degree_window(v, &w, d))
    }

    pub fn vertex(&self, v: u64) -> Option<VertexView> {
        self.read_shard(|tg| tg.vertex(v))
    }

    pub fn vertex_window(&self, v: u64, w: Range<i64>) -> Option<VertexView> {
        self.read_shard(|tg| tg.vertex_window(v, &w))
    }

    pub fn vertex_ids(&self) -> impl Iterator<Item = u64> {
        let tgshard = self.rc.clone();
        let iter: GenBoxed<u64> = GenBoxed::new_boxed(|co| async move {
            let g = tgshard.blocking_read();
            let iter = (*g).vertex_ids();
            for v_id in iter {
                co.yield_(v_id).await;
            }
        });

        iter.into_iter()
    }

    pub fn vertex_ids_window(&self, w: Range<i64>) -> impl Iterator<Item = u64> {
        let tgshard = self.rc.clone();
        let iter: GenBoxed<u64> = GenBoxed::new_boxed(|co| async move {
            let g = tgshard.blocking_read();
            let iter = (*g).vertex_ids_window(w).map(|v| v.into());
            for v_id in iter {
                co.yield_(v_id).await;
            }
        });

        iter.into_iter()
    }

    pub fn vertices(&self) -> impl Iterator<Item = VertexView> {
        let tgshard = self.rc.clone();
        let iter: GenBoxed<VertexView> = GenBoxed::new_boxed(|co| async move {
            let g = tgshard.blocking_read();
            let iter = (*g).vertices();
            for vv in iter {
                co.yield_(vv).await;
            }
        });

        iter.into_iter()
    }

    pub fn vertices_window(&self, w: Range<i64>) -> impl Iterator<Item = VertexView> {
        let tgshard = self.rc.clone();
        let iter: GenBoxed<VertexView> = GenBoxed::new_boxed(|co| async move {
            let g = tgshard.blocking_read();
            let iter = (*g).vertices_window(w);
            for vv in iter {
                co.yield_(vv).await;
            }
        });

        iter.into_iter()
    }

    pub fn edge(&self, v1: u64, v2: u64) -> Option<EdgeView> {
        self.read_shard(|tg| tg.edge(v1, v2))
    }

    pub fn edge_window(&self, v1: u64, v2: u64, w: Range<i64>) -> Option<EdgeView> {
        self.read_shard(|tg| tg.edge_window(v1, v2, &w))
    }

    pub fn vertex_edges(&self, v: u64, d: Direction) -> impl Iterator<Item = EdgeView> {
        let tgshard = self.rc.clone();
        let iter: GenBoxed<EdgeView> = GenBoxed::new_boxed(|co| async move {
            let g = tgshard.blocking_read();
            let iter = (*g).vertex_edges(v, d);
            for ev in iter {
                co.yield_(ev).await;
            }
        });

        iter.into_iter()
    }

    pub fn vertex_edges_window(
        &self,
        v: u64,
        w: Range<i64>,
        d: Direction,
    ) -> impl Iterator<Item = EdgeView> {
        let tgshard = self.clone();
        let iter = gen!({
            let g = tgshard.rc.blocking_read();
            let chunks = (*g).vertex_edges_window(v, &w, d).map(|e| e.into());
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        iter.into_iter()
    }

    pub fn vertex_edges_window_t(
        &self,
        v: u64,
        w: Range<i64>,
        d: Direction,
    ) -> impl Iterator<Item = EdgeView> {
        let tgshard = self.clone();
        let iter = gen!({
            let g = tgshard.rc.blocking_read();
            let chunks = (*g).vertex_edges_window_t(v, &w, d).map(|e| e.into());
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        iter.into_iter()
    }

    pub fn neighbours(&self, v: u64, d: Direction) -> impl Iterator<Item = VertexView> {
        let tgshard = self.clone();
        let iter = gen!({
            let g = tgshard.rc.blocking_read();
            let chunks = (*g).neighbours(v, d);
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        iter.into_iter()
    }

    pub fn neighbours_window(
        &self,
        v: u64,
        w: Range<i64>,
        d: Direction,
    ) -> impl Iterator<Item = VertexView> {
        let tgshard = self.clone();
        let iter = gen!({
            let g = tgshard.rc.blocking_read();
            let chunks = (*g).neighbours_window(v, &w, d);
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        iter.into_iter()
    }

    pub fn neighbours_ids(&self, v: u64, d: Direction) -> impl Iterator<Item = u64>
    where
        Self: Sized,
    {
        let tgshard = self.clone();
        let iter = gen!({
            let g = tgshard.rc.blocking_read();
            let chunks = (*g).neighbours_ids(v, d);
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        iter.into_iter()
    }

    pub fn neighbours_ids_window(
        &self,
        v: u64,
        w: Range<i64>,
        d: Direction,
    ) -> impl Iterator<Item = u64>
    where
        Self: Sized,
    {
        let tgshard = self.clone();
        let iter = gen!({
            let g = tgshard.rc.blocking_read();
            let chunks = (*g).neighbours_ids_window(v, &w, d);
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        iter.into_iter()
    }

    pub fn vertex_prop_vec(&self, v: u64, name: String) -> Vec<(i64, Prop)> {
        self.read_shard(|tg| tg.vertex_prop_vec(v, &name).unwrap_or_else(|| vec![]))
    }

    pub fn vertex_prop_vec_window(&self, v: u64, name: String, w: Range<i64>) -> Vec<(i64, Prop)> {
        self.read_shard(|tg| {
            tg.vertex_prop_vec_window(v, &name, &w)
                .unwrap_or_else(|| vec![])
        })
    }

    pub fn vertex_props(&self, v: u64) -> HashMap<String, Vec<(i64, Prop)>> {
        self.read_shard(|tg| {
            tg.vertex_props(v)
                .unwrap_or_else(|| HashMap::<String, Vec<(i64, Prop)>>::new())
        })
    }

    pub fn vertex_props_window(&self, v: u64, w: Range<i64>) -> HashMap<String, Vec<(i64, Prop)>> {
        self.read_shard(|tg| {
            tg.vertex_props_window(v, &w)
                .unwrap_or_else(|| HashMap::<String, Vec<(i64, Prop)>>::new())
        })
    }

    pub fn edge_prop_vec(&self, e: usize, name: String) -> Vec<(i64, Prop)> {
        self.read_shard(|tg| tg.edge_prop_vec(e, &name).unwrap_or_else(|| vec![]))
    }

    pub fn edge_props_vec_window(&self, e: usize, name: String, w: Range<i64>) -> Vec<(i64, Prop)> {
        self.read_shard(|tg| {
            tg.edge_prop_vec_window(e, &name, w.clone())
                .unwrap_or_else(|| vec![])
        })
    }
}

#[cfg(test)]
mod temporal_graph_partition_test {
    use super::TGraphShard;
    use crate::Direction;
    use itertools::Itertools;
    use quickcheck::{Arbitrary, TestResult};
    use rand::Rng;

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

        let g = TGraphShard::default();

        let rand_index = rand::thread_rng().gen_range(0..vs.len());
        let rand_vertex = vs.get(rand_index).unwrap().0;

        for (v, t) in vs {
            g.add_vertex(t.into(), v.into(), &vec![]);
        }

        TestResult::from_bool(g.has_vertex(rand_vertex))
    }

    #[test]
    fn shard_contains_vertex_window() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::default();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        assert!(g.has_vertex_window(1, -1..7));
        assert!(!g.has_vertex_window(2, 0..1));
        assert!(g.has_vertex_window(3, 0..8));
    }

    #[quickcheck]
    fn add_vertex_to_shard_len_grows(vs: Vec<(u8, u8)>) {
        let g = TGraphShard::default();

        let expected_len = vs.iter().map(|(_, v)| v).sorted().dedup().count();
        for (t, v) in vs {
            g.add_vertex(t.into(), v.into(), &vec![]);
        }

        assert_eq!(g.len(), expected_len)
    }

    #[test]
    fn shard_vertices() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::default();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let actual = g.vertex_ids().collect::<Vec<_>>();
        assert_eq!(actual, vec![1, 2, 3]);
    }

    // add one single vertex per interval
    // then go through each window
    // and select the vertices
    // should recover each inserted vertex exactly once
    #[quickcheck]
    fn iterate_vertex_windows(intervals: Intervals) {
        let g = TGraphShard::default();

        for (v, (t_start, _)) in intervals.0.iter().enumerate() {
            g.add_vertex(*t_start, v.try_into().unwrap(), &vec![])
        }

        for (v, (t_start, t_end)) in intervals.0.iter().enumerate() {
            let vertex_window = g
                .vertices_window(*t_start..*t_end)
                .map(move |v| v.g_id)
                .collect::<Vec<_>>();
            let iter = &mut vertex_window.iter();
            let v_actual = iter.next();
            assert_eq!(Some(v as u64), Some(*v_actual.unwrap()));
            assert_eq!(None, iter.next()); // one vertex per interval
        }
    }

    #[test]
    fn get_shard_degree() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::default();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
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
    }

    #[test]
    fn get_shard_degree_window() {
        let g = TGraphShard::default();

        g.add_vertex(1, 100, &vec![]);
        g.add_vertex(2, 101, &vec![]);
        g.add_vertex(3, 102, &vec![]);
        g.add_vertex(4, 103, &vec![]);
        g.add_vertex(5, 104, &vec![]);
        g.add_vertex(5, 105, &vec![]);

        g.add_edge(6, 100, 101, &vec![]);
        g.add_edge(7, 100, 102, &vec![]);
        g.add_edge(8, 101, 103, &vec![]);
        g.add_edge(9, 102, 104, &vec![]);
        g.add_edge(9, 110, 104, &vec![]);

        assert_eq!(g.degree_window(101, 0i64..i64::MAX, Direction::IN), 1);
        assert_eq!(g.degree_window(100, 0..i64::MAX, Direction::IN), 0);
        assert_eq!(g.degree_window(101, 0..1, Direction::IN), 0);
        assert_eq!(g.degree_window(101, 10..20, Direction::IN), 0);
        assert_eq!(g.degree_window(105, 0..i64::MAX, Direction::IN), 0);
        assert_eq!(g.degree_window(104, 0..i64::MAX, Direction::IN), 2);
        assert_eq!(g.degree_window(101, 0..i64::MAX, Direction::OUT), 1);
        assert_eq!(g.degree_window(103, 0..i64::MAX, Direction::OUT), 0);
        assert_eq!(g.degree_window(105, 0..i64::MAX, Direction::OUT), 0);
        assert_eq!(g.degree_window(101, 0..1, Direction::OUT), 0);
        assert_eq!(g.degree_window(101, 10..20, Direction::OUT), 0);
        assert_eq!(g.degree_window(100, 0..i64::MAX, Direction::OUT), 2);
        assert_eq!(g.degree_window(101, 0..i64::MAX, Direction::BOTH), 2);
        assert_eq!(g.degree_window(100, 0..i64::MAX, Direction::BOTH), 2);
        assert_eq!(g.degree_window(100, 0..1, Direction::BOTH), 0);
        assert_eq!(g.degree_window(100, 10..20, Direction::BOTH), 0);
        assert_eq!(g.degree_window(105, 0..i64::MAX, Direction::BOTH), 0);
    }

    #[test]
    fn get_shard_neighbours() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::default();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let expected = vec![(2, 3, 5), (2, 1, 3), (1, 1, 2)];
        let actual = (1..=3)
            .map(|i| {
                (
                    g.vertex_edges(i, Direction::IN).collect::<Vec<_>>().len(),
                    g.vertex_edges(i, Direction::OUT).collect::<Vec<_>>().len(),
                    g.vertex_edges(i, Direction::BOTH).collect::<Vec<_>>().len(),
                )
            })
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn get_shard_neighbours_window() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::default();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let expected = vec![(2, 3, 2), (1, 0, 0), (1, 0, 0)];
        let actual = (1..=3)
            .map(|i| {
                (
                    g.vertex_edges_window(i, -1..7, Direction::IN)
                        .collect::<Vec<_>>()
                        .len(),
                    g.vertex_edges_window(i, 1..7, Direction::OUT)
                        .collect::<Vec<_>>()
                        .len(),
                    g.vertex_edges_window(i, 0..1, Direction::BOTH)
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
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = TGraphShard::default();

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let in_actual = (1..=3)
            .map(|i| {
                g.vertex_edges_window_t(i, -1..7, Direction::IN)
                    .map(|e| e.time.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![-1, 0, 1], vec![1], vec![2]], in_actual);

        let out_actual = (1..=3)
            .map(|i| {
                g.vertex_edges_window_t(i, 1..7, Direction::OUT)
                    .map(|e| e.time.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![1, 1, 2], vec![], vec![]], out_actual);

        let both_actual = (1..=3)
            .map(|i| {
                g.vertex_edges_window_t(i, 0..1, Direction::BOTH)
                    .map(|e| e.time.unwrap())
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        assert_eq!(vec![vec![0, 0], vec![], vec![]], both_actual);
    }
}
