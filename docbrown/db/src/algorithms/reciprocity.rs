use crate::program::{GlobalEvalState, LocalState, Program};
use crate::view_api::GraphViewOps;
use docbrown_core::state;
use std::collections::HashSet;

pub struct GlobalReciprocity {}

impl Program for GlobalReciprocity {
    type Out = f64;

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let total_out_neighbours = c.agg(state::def::sum::<usize>(0));
        let total_out_inter_in = c.agg(state::def::sum::<usize>(1));

        c.step(|v| {
            let out_neighbours: HashSet<u64> = v
                .neighbours_out()
                .map(|n| n.global_id())
                .filter(|x| *x != v.global_id())
                .collect();
            v.global_update(&total_out_neighbours, out_neighbours.len());

            let out_inter_in = out_neighbours
                .intersection(
                    &v.neighbours_in()
                        .map(|n| n.global_id())
                        .filter(|x| *x != v.global_id())
                        .collect(),
                )
                .count();
            v.global_update(&total_out_inter_in, out_inter_in);
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.global_agg(state::def::sum::<usize>(0));
        let _ = c.global_agg(state::def::sum::<usize>(1));
        c.step(|_| true);
    }

    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
        let a = gs
            .read_global_state(&state::def::sum::<usize>(1))
            .unwrap_or(0);
        let b = gs
            .read_global_state(&state::def::sum::<usize>(0))
            .unwrap_or(0);
        a as f64 / b as f64
    }
}

pub struct AllLocalReciprocity {}

impl Program for AllLocalReciprocity {
    type Out = f64;

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let local_recip = c.agg(state::def::hash_set(0));
        todo!()
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        todo!()
    }

    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
        todo!()
    }
}

#[cfg(test)]
mod program_test {
    use crate::graph::Graph;
    use crate::program::algo::global_reciprocity;
    use docbrown_core::state;
    use itertools::chain;
    use pretty_assertions::assert_eq;
    use rustc_hash::FxHashMap;
    use std::{cmp::Reverse, iter::once};

    #[test]
    fn test_global_recip() {
        let graph = Graph::new(2);

        let vs = vec![
            (1, 2),
            (1, 4),
            (2, 3),
            (3, 2),
            (3, 1),
            (4, 3),
            (4, 1),
            (1, 5),
        ];

        for (src, dst) in &vs {
            graph.add_edge(0, *src, *dst, &vec![]);
        }

        let actual = global_reciprocity(&graph);
        assert_eq!(actual, 0.5);
    }
}
