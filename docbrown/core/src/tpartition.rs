use parking_lot::RwLock;
use std::sync::Arc;

use genawaiter::rc::gen;
use genawaiter::yield_;

use crate::graph::{EdgeView, TemporalGraph};
use crate::{Direction, Prop};
use itertools::*;

#[derive(Clone, Debug, Default)]
#[repr(transparent)]
pub struct TemporalGraphPart(Arc<RwLock<TemporalGraph>>);

pub struct TEdge {
    src: u64,
    dst: u64,
    // edge_meta_id: AdjEdge,
    t: Option<u64>,
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

impl TemporalGraphPart {
    pub fn add_vertex(&self, t: u64, v: u64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_vertex(v, t))
    }

    pub fn add_edge(&self, t: u64, src: u64, dst: u64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_props(src, dst, t, props))
    }

    pub fn add_edge_remote_out(&self, t: u64, src: u64, dst: u64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_remote_out(src, dst, t, props))
    }

    pub fn add_edge_remote_into(&self, t: u64, src: u64, dst: u64, props: &Vec<(String, Prop)>) {
        self.write_shard(|tg| tg.add_edge_remote_into(src, dst, t, props))
    }

    // TODO: check if there is any value in returning Vec<usize> vs just usize, what is the cost of the generator
    pub fn vertices_window(
        &self,
        t_start: u64,
        t_end: u64,
        chunk_size: usize,
    ) -> impl Iterator<Item = Vec<usize>> {
        let tg = self.clone();
        let vertices_iter = gen!({
            let g = tg.0.read();
            let chunks = (*g).vertices_window_iter(t_start..t_end).chunks(chunk_size);
            let iter = chunks.into_iter().map(|chunk| chunk.collect::<Vec<_>>());
            for v_id in iter {
                yield_!(v_id)
            }
        });

        vertices_iter.into_iter()
    }

    pub fn neighbours_window(
        &self,
        t_start: u64,
        t_end: u64,
        v: u64,
        d: Direction,
    ) -> impl Iterator<Item = TEdge> {
        let tg = self.clone();
        let vertices_iter = gen!({
            let g = tg.0.read();
            let chunks = (*g)
                .neighbours_window((t_start..t_end), v, d)
                .map(|e| e.into());
            let iter = chunks.into_iter();
            for v_id in iter {
                yield_!(v_id)
            }
        });

        vertices_iter.into_iter()
    }

    pub fn len(&self) -> usize {
        self.read_shard(|tg| tg.len())
    }

    pub fn contains(&self, v: u64) -> bool {
        self.read_shard(|tg| tg.contains(v))
    }

    pub fn contains_t(&self, t_start: u64, t_end: u64, v: u64) -> bool {
        self.read_shard(|tg| tg.contains_vertex_w(t_start..t_end, v))
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
}

#[cfg(test)]
mod temporal_graph_partition_test {
    use super::TemporalGraphPart;
    use itertools::Itertools;
    use quickcheck::Arbitrary;

    // non overlaping time intervals
    #[derive(Clone, Debug)]
    struct Intervals(Vec<(u64, u64)>);

    impl Arbitrary for Intervals {
        fn arbitrary(g: &mut quickcheck::Gen) -> Self {
            let mut some_nums = Vec::<u64>::arbitrary(g);
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
            g.add_vertex(t.into(), v.into(), &vec![]);
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
            g.add_vertex(*t_start, v.try_into().unwrap(), &vec![])
        }

        for (v, (t_start, t_end)) in intervals.0.iter().enumerate() {
            let vertex_window = g.vertices_window(*t_start, *t_end, 1);
            let iter = &mut vertex_window.into_iter().flatten();
            let v_actual = iter.next();
            assert_eq!(Some(v), v_actual);
            assert_eq!(None, iter.next()); // one vertex per interval
        }
    }
}
