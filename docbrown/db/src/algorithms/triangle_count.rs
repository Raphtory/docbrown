use crate::{
    graph::Graph,
    program::{GlobalEvalState, LocalState, Program},
    view_api::*,
};
use docbrown_core::{state, tgraph_shard::errors::GraphError};
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::FxHashSet;
use std::ops::Range;

pub fn local_triangle_count<G: GraphViewOps>(graph: &G, v: u64) -> Result<usize, GraphError> {
    let vertex = graph.vertex(v)?.unwrap();

    let count = if vertex.degree()? >= 2 {
        let r: Result<Vec<_>, _> = vertex
            .neighbours()
            .id()
            .into_iter()
            .combinations(2)
            .filter_map(|nb| match graph.has_edge(nb[0], nb[1]) {
                Ok(true) => Some(Ok(nb)),
                Ok(false) => match graph.has_edge(nb[1], nb[0]) {
                    Ok(true) => Some(Ok(nb)),
                    Ok(false) => None,
                    Err(e) => Some(Err(e)),
                },
                Err(e) => Some(Err(e)),
            })
            .collect();

        r.map(|t| t.len())?
    } else {
        0
    };

    Ok(count)
}

pub fn global_triangle_count<G: GraphViewOps>(graph: &G) -> Result<usize, GraphError> {
    let r: Result<Vec<_>, _> = graph
        .vertices()
        .into_iter()
        .par_bridge()
        .map(|v| {
            let r: Result<Vec<_>, _> = v
                .neighbours()
                .id()
                .into_iter()
                .combinations(2)
                .filter_map(|nb| match graph.has_edge(nb[0], nb[1]) {
                    Ok(true) => Some(Ok(nb)),
                    Ok(false) => match graph.has_edge(nb[1], nb[0]) {
                        Ok(true) => Some(Ok(nb)),
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    },
                    Err(e) => Some(Err(e)),
                })
                .collect();
            r.map(|t| t.len())
        })
        .collect();

    let count: usize = r?.into_iter().sum();
    Ok(count / 3)
}

pub struct TriangleCountS1 {}

impl Program for TriangleCountS1 {
    fn local_eval(&self, c: &LocalState) {
        let neighbors_set = c.agg(state::def::hash_set(0));

        c.step(|s| {
            for t in s.neighbours() {
                if s.global_id() > t.global_id() {
                    t.update(&neighbors_set, s.global_id());
                }
            }
        });
    }

    fn post_eval(&self, c: &mut GlobalEvalState) {
        let _ = c.agg(state::def::hash_set::<u64>(0));
        c.step(|_| false)
    }

    type Out = ();

    fn produce_output(&self, g: &Graph, window: Range<i64>, gs: &GlobalEvalState) -> Self::Out
    where
        Self: Sync,
    {
    }
}

pub fn triangle_counting_fast(g: &Graph, window: Range<i64>) -> Option<usize> {
    let mut gs = GlobalEvalState::new(g.clone(), window.clone(), false);
    let tc = TriangleCountS1 {};

    tc.run_step(g, &mut gs);

    let tc = TriangleCountS2 {};

    tc.run_step(g, &mut gs);

    tc.produce_output(g, window, &gs)
}

pub struct TriangleCountS2 {}

impl Program for TriangleCountS2 {
    type Out = Option<usize>;
    fn local_eval(&self, c: &LocalState) {
        let neighbors_set = c.agg(state::def::hash_set::<u64>(0));
        let count = c.global_agg(state::def::sum::<usize>(1));

        c.step(|s| {
            for t in s.neighbours() {
                if s.global_id() > t.global_id() {
                    let intersection_count = {
                        // when using entry() we need to make sure the reference is released before we can update the state, otherwise we break the Rc<RefCell<_>> invariant
                        // where there can either be one mutable or many immutable references

                        match (
                            s.entry(&neighbors_set)
                                .read_ref()
                                .unwrap_or(&FxHashSet::default()),
                            t.entry(&neighbors_set)
                                .read_ref()
                                .unwrap_or(&FxHashSet::default()),
                        ) {
                            (s_set, t_set) => {
                                let intersection = s_set.intersection(t_set);
                                intersection.count()
                            }
                        }
                    };

                    s.global_update(&count, intersection_count);
                }
            }
        });
    }

    fn post_eval(&self, c: &mut GlobalEvalState) {
        let _ = c.global_agg(state::def::sum::<usize>(1));
        c.step(|_| false)
    }

    fn produce_output(&self, g: &Graph, window: Range<i64>, gs: &GlobalEvalState) -> Self::Out
    where
        Self: Sync,
    {
        gs.read_global_state(&state::def::sum::<usize>(1))
    }
}

pub struct TriangleCountSlowS2 {}

impl Program for TriangleCountSlowS2 {
    fn local_eval(&self, c: &LocalState) {
        let count = c.global_agg(state::def::sum::<usize>(0));

        c.step(|v| {
            let my_neighbours_less_myself = v
                .neighbours()
                .map(|n| n.global_id())
                .filter(|n| *n != v.global_id())
                .collect::<FxHashSet<_>>();

            let c1 = my_neighbours_less_myself.len();

            for n in v.neighbours() {
                if v.global_id() > n.global_id() {
                    let nn_less_itself = n
                        .neighbours()
                        .map(|n| n.global_id())
                        .filter(|v| *v != n.global_id())
                        .collect::<FxHashSet<_>>();

                    let c2 = my_neighbours_less_myself
                        .difference(&nn_less_itself)
                        .count();
                    v.global_update(&count, c1 - c2);
                }
            }
        })
    }

    fn post_eval(&self, c: &mut GlobalEvalState) {
        let _ = c.global_agg(state::def::sum::<usize>(0));
        c.step(|_| false)
    }

    type Out = usize;

    fn produce_output(&self, g: &Graph, window: Range<i64>, gs: &GlobalEvalState) -> Self::Out
    where
        Self: Sync,
    {
        todo!()
    }
}

#[cfg(test)]
mod triangle_count_tests {
    use super::*;
    use crate::graph::Graph;

    #[test]
    fn counts_triangles_local() {
        let g = Graph::new(1);
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]).unwrap();
        }

        let windowed_graph = g.window(0, 5);
        let expected = vec![(1), (1), (1)];

        let actual = (1..=3)
            .map(|v| local_triangle_count(&windowed_graph, v).unwrap())
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }

    #[test]
    fn counts_triangles_global() {
        let g = Graph::new(1);
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]).unwrap();
        }

        let windowed_graph = g.window(0, 5);
        let expected = 1;

        let actual = global_triangle_count(&windowed_graph).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn counts_triangles_global_again() {
        let g = Graph::new(1);

        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];

        for (src, dst, t) in &edges {
            g.add_edge(*t, *src, *dst, &vec![]).unwrap();
        }

        let windowed_graph = g.window(0, 95);
        let expected = 8;

        let actual = global_triangle_count(&windowed_graph).unwrap();

        assert_eq!(actual, expected);
    }

    #[test]
    fn triangle_count_1() {
        let graph = Graph::new(2);

        let edges = vec![
            // triangle 1
            (1, 2, 1),
            (2, 3, 1),
            (3, 1, 1),
            //triangle 2
            (4, 5, 1),
            (5, 6, 1),
            (6, 4, 1),
            // triangle 4 and 5
            (7, 8, 2),
            (8, 9, 3),
            (9, 7, 4),
            (8, 10, 5),
            (10, 9, 6),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]);
        }

        let actual_tri_count = triangle_counting_fast(&graph, 0..96);

        assert_eq!(actual_tri_count, Some(4))
    }

    #[test]
    fn triangle_count_1_slow() {
        let graph = Graph::new(2);

        let edges = vec![
            // triangle 1
            (1, 2, 1),
            (2, 3, 1),
            (3, 1, 1),
            //triangle 2
            (4, 5, 1),
            (5, 6, 1),
            (6, 4, 1),
            // triangle 4 and 5
            (7, 8, 2),
            (8, 9, 3),
            (9, 7, 4),
            (8, 10, 5),
            (10, 9, 6),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]).unwrap();
        }

        let program_s1 = TriangleCountSlowS2 {};
        let agg = state::def::sum::<usize>(0);

        let mut gs = GlobalEvalState::new(graph.clone(), 0..95, false);

        program_s1.run_step(&graph, &mut gs);

        let actual_tri_count = gs.read_global_state(&agg).map(|v| v / 3);

        assert_eq!(actual_tri_count, Some(4))
    }

    #[test]
    fn triangle_count_2() {
        let graph = Graph::new(4);

        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]).unwrap();
        }

        let program_s1 = TriangleCountSlowS2 {};
        let agg = state::def::sum::<usize>(0);

        let mut gs = GlobalEvalState::new(graph.clone(), 0..64, false);

        program_s1.run_step(&graph, &mut gs);

        let actual_tri_count = gs.read_global_state(&agg).map(|v| v / 3);

        assert_eq!(actual_tri_count, Some(8));
    }

    #[test]
    fn triangle_count_3() {
        let graph = Graph::new(2);

        let edges = vec![
            (1, 2, 1),
            (1, 3, 2),
            (1, 4, 3),
            (3, 1, 4),
            (3, 4, 5),
            (3, 5, 6),
            (4, 5, 7),
            (5, 6, 8),
            (5, 8, 9),
            (7, 5, 10),
            (8, 5, 11),
            (1, 9, 12),
            (9, 1, 13),
            (6, 3, 14),
            (4, 8, 15),
            (8, 3, 16),
            (5, 10, 17),
            (10, 5, 18),
            (10, 8, 19),
            (1, 11, 20),
            (11, 1, 21),
            (9, 11, 22),
            (11, 9, 23),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]).unwrap();
        }

        let actual_tri_count = triangle_counting_fast(&graph, 0..27);

        assert_eq!(actual_tri_count, Some(8))
    }
}
