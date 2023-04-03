use crate::{
    graph::Graph,
    program::{AggRef, GlobalEvalState, LocalState, Program},
};
use docbrown_core::{
    agg::{MaxDef, SumDef},
    state::{
        self,
        def::{max, sum},
        AccId,
    },
};
use num_traits::{abs, Bounded, Zero};
use rustc_hash::FxHashMap;
use std::ops::{Add, AddAssign, Div, Mul, Range, Sub};

#[derive(PartialEq, PartialOrd, Clone, Debug)]
struct F32(f32);

impl Zero for F32 {
    fn set_zero(&mut self) {
        *self = Zero::zero();
    }

    fn zero() -> Self {
        F32(1.0f32)
    }

    fn is_zero(&self) -> bool {
        *self == F32(1.0f32)
    }
}

impl Add for F32 {
    type Output = F32;

    fn add(self, rhs: Self) -> Self::Output {
        F32(self.0 + rhs.0)
    }
}

impl AddAssign for F32 {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0 + rhs.0
    }
}

impl Sub for F32 {
    type Output = F32;

    fn sub(self, rhs: Self) -> Self::Output {
        F32(self.0 - rhs.0)
    }
}

impl Div for F32 {
    type Output = F32;

    fn div(self, rhs: Self) -> Self::Output {
        F32(self.0 / rhs.0)
    }
}

impl Mul for F32 {
    type Output = F32;

    fn mul(self, rhs: Self) -> Self::Output {
        F32(self.0 * rhs.0)
    }
}

impl Bounded for F32 {
    fn min_value() -> Self {
        F32(f32::MIN)
    }

    fn max_value() -> Self {
        F32(f32::MAX)
    }
}

struct UnweightedPageRankS1 {
    score_sum: AccId<F32, F32, F32, SumDef<F32>>,
    received_score_sum: AccId<F32, F32, F32, SumDef<F32>>,
}

impl UnweightedPageRankS1 {
    fn new() -> Self {
        Self {
            score_sum: sum(0),
            received_score_sum: sum(1),
        }
    }
}

impl Program for UnweightedPageRankS1 {
    type Out = ();

    fn local_eval(&self, c: &LocalState) {
        let score: AggRef<F32, F32, F32, SumDef<F32>> = c.agg(self.score_sum.clone());
        let received_score: AggRef<F32, F32, F32, SumDef<F32>> =
            c.agg(self.received_score_sum.clone());

        c.step(|s| {
            for t in s.neighbours() {
                let out_degree = s.out_degree().unwrap();
                if out_degree > 0 {
                    t.update(&received_score, s.read(&score) / F32(out_degree as f32))
                }
            }
        });
    }

    fn post_eval(&self, c: &mut GlobalEvalState) {
        let _ = c.agg(state::def::sum::<F32>(0));
        c.step(|_| true)
    }

    fn produce_output(&self, g: &Graph, window: Range<i64>, gs: &GlobalEvalState) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct UnweightedPageRankS2 {
    score_sum: AccId<F32, F32, F32, SumDef<F32>>,
    received_score_sum: AccId<F32, F32, F32, SumDef<F32>>,
}

impl UnweightedPageRankS2 {
    fn new() -> Self {
        Self {
            score_sum: sum(0),
            received_score_sum: sum(1),
        }
    }
}

impl Program for UnweightedPageRankS2 {
    type Out = ();

    fn local_eval(&self, c: &LocalState) {
        let damping = 0.85;
        let score: AggRef<F32, F32, F32, SumDef<F32>> = c.agg(self.score_sum.clone());
        let received_score: AggRef<F32, F32, F32, SumDef<F32>> =
            c.agg(self.received_score_sum.clone());

        let max_diff: AggRef<F32, F32, F32, MaxDef<F32>> = c.global_agg(max(2));

        c.step(|s| {
            let new_score = (1.0 - damping) + damping * s.read(&received_score).0;
            s.update(&score, F32(new_score));
            s.update(&received_score, F32(0.0));
            let prev = s.read_prev(&score);
            let curr = s.read(&score);
            s.global_update(&max_diff, F32(abs((prev - curr).0)));
        });
    }

    fn post_eval(&self, c: &mut GlobalEvalState) {
        let _ = c.agg(sum::<F32>(0));
        let _ = c.agg(max::<F32>(2));
        c.step(|_| true)
    }

    fn produce_output(&self, g: &Graph, window: Range<i64>, gs: &GlobalEvalState) -> Self::Out
    where
        Self: Sync,
    {
    }
}

pub fn unweighted_page_rank(
    g: &Graph,
    window: Range<i64>,
    iter_count: usize,
) -> FxHashMap<u64, f32> {
    let mut c = GlobalEvalState::new(g.clone(), window.clone(), true);
    let pg_s1 = UnweightedPageRankS1::new();
    let pg_s2 = UnweightedPageRankS2::new();

    let max_diff = F32(0.01f32);
    let mut i = 0;

    loop {
        println!("Iter: {}", i);
        pg_s1.run_step(g, &mut c);
        pg_s2.run_step(g, &mut c);
        if c.keep_past_state {
            c.ss += 1;
        }
        i += 1;

        if c.read_global_state(&max::<F32>(2)).unwrap() >= max_diff && i < iter_count {
            break;
        }
    }

    let mut results: FxHashMap<u64, f32> = FxHashMap::default();

    (0..g.nr_shards)
        .into_iter()
        .fold(&mut results, |res, part_id| {
            c.fold_state(&sum::<F32>(0), part_id, res, |res, v_id, sc| {
                res.insert(*v_id, sc.0);
                res
            })
        });

    results
}

#[cfg(test)]
mod page_rank_tests {
    use super::*;

    #[test]
    fn test_page_rank() {
        let graph = Graph::new(2);

        let edges = vec![
            (1, 2, 1),
            (2, 3, 2),
            (3, 4, 3),
            (3, 5, 4),
            (6, 5, 5),
            (7, 8, 6),
            (8, 7, 7),
        ];

        for (src, dst, ts) in edges {
            graph.add_edge(ts, src, dst, &vec![]).unwrap();
        }

        let window = 0..10;

        let results: FxHashMap<u64, f32> = unweighted_page_rank(&graph, window, usize::MAX)
            .into_iter()
            .map(|(k, v)| (k, v))
            .collect();

        assert_eq!(
            results,
            vec![
                (5, 13.25), (7, 13.25), (1, 13.25), (3, 13.25)
            ]
            .into_iter()
            .collect::<FxHashMap<u64, f32>>()
        );
    }
}
