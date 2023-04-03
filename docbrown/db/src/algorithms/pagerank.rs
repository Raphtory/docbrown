use crate::{
    graph::Graph,
    program::{AggRef, GlobalEvalState, LocalState, Program},
};
use docbrown_core::{
    agg::{MaxDef, SumDef, ValDef},
    state::{
        self,
        def::{max, sum, val},
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
    score: AccId<F32, F32, F32, ValDef<F32>>,
    recv_score: AccId<F32, F32, F32, SumDef<F32>>,
}

impl UnweightedPageRankS1 {
    fn new() -> Self {
        Self {
            score: val(0),
            recv_score: sum(1),
        }
    }
}

impl Program for UnweightedPageRankS1 {
    type Out = ();

    fn local_eval(&self, c: &LocalState) {
        let score: AggRef<F32, F32, F32, ValDef<F32>> = c.agg(self.score.clone());
        let recv_score: AggRef<F32, F32, F32, SumDef<F32>> = c.agg(self.recv_score.clone());

        c.step(|s| {
            let out_degree = s.out_degree().unwrap();
            let new_score = s.read(&score).0;
            let w = F32(new_score / out_degree as f32);
            if out_degree > 0 {
                for t in s.neighbours() {
                    t.update(&recv_score, w.clone())
                }
            }
        });
    }

    fn post_eval(&self, c: &mut GlobalEvalState) {
        let _ = c.agg(val::<F32>(0));
        let _ = c.agg(sum::<F32>(1));
        c.step(|_| true)
    }

    fn produce_output(&self, g: &Graph, window: Range<i64>, gs: &GlobalEvalState) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct UnweightedPageRankS2 {
    score: AccId<F32, F32, F32, ValDef<F32>>,
    recv_score: AccId<F32, F32, F32, SumDef<F32>>,
    max_diff: AccId<F32, F32, F32, MaxDef<F32>>,
}

impl UnweightedPageRankS2 {
    fn new() -> Self {
        Self {
            score: val(0),
            recv_score: sum(1),
            max_diff: max(2),
        }
    }
}

impl Program for UnweightedPageRankS2 {
    type Out = ();

    fn local_eval(&self, c: &LocalState) {
        let score: AggRef<F32, F32, F32, ValDef<F32>> = c.agg(self.score.clone());
        let recv_score: AggRef<F32, F32, F32, SumDef<F32>> = c.agg(self.recv_score.clone());
        let max_diff: AggRef<F32, F32, F32, MaxDef<F32>> = c.global_agg(self.max_diff.clone());

        c.step(|s| {
            s.update(&score, s.read(&recv_score));
            let prev = s.read_prev(&score);
            let curr = s.read(&score);
            s.global_update(&max_diff, F32(abs((prev - curr).0)));
        });
    }

    fn post_eval(&self, c: &mut GlobalEvalState) {
        let _ = c.agg(val::<F32>(0));
        let _ = c.agg(sum::<F32>(1));
        let _ = c.global_agg(max::<F32>(2));
        c.step(|_| true)
    }

    fn produce_output(&self, g: &Graph, window: Range<i64>, gs: &GlobalEvalState) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct UnweightedPageRankS3 {
    recv_score: AccId<F32, F32, F32, SumDef<F32>>,
    max_diff: AccId<F32, F32, F32, MaxDef<F32>>,
}

impl UnweightedPageRankS3 {
    fn new() -> Self {
        Self {
            recv_score: sum(1),
            max_diff: max(2),
        }
    }
}

impl Program for UnweightedPageRankS3 {
    type Out = ();

    fn local_eval(&self, c: &LocalState) {
        let recv_score: AggRef<F32, F32, F32, SumDef<F32>> = c.agg(self.recv_score.clone());
        let max_diff: AggRef<F32, F32, F32, MaxDef<F32>> = c.global_agg(self.max_diff.clone());

        c.step(|s| {
            s.reset(&recv_score);
            s.reset(&max_diff);
        });
    }

    fn post_eval(&self, c: &mut GlobalEvalState) {
        let _ = c.agg(sum::<F32>(1));
        let _ = c.global_agg(max::<F32>(2));
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
    let pg_s3 = UnweightedPageRankS3::new();

    let max_diff = F32(0.01f32);
    let mut i = 0;

    loop {
        pg_s1.run_step(g, &mut c);
        pg_s2.run_step(g, &mut c);
        if c.keep_past_state {
            c.ss += 1;
        }
        i += 1;

        if c.read_global_state(&max::<F32>(2)).unwrap() >= max_diff && i < iter_count {
            // if c.ss >= 3 && i < iter_count {
            break;
        }

        pg_s3.run_step(g, &mut c);
    }

    let mut results: FxHashMap<u64, f32> = FxHashMap::default();

    (0..g.nr_shards)
        .into_iter()
        .fold(&mut results, |res, part_id| {
            c.fold_state(&val::<F32>(0), part_id, res, |res, v_id, sc| {
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
            (2, 3),
            (3, 2),
            (4, 1),
            (4, 2),
            (5, 2),
            (5, 4),
            (5, 6),
            (6, 2),
            (6, 5),
            (7, 2),
            (7, 5),
            (8, 2),
            (8, 5),
            (9, 2),
            (9, 5),
            (10, 5),
            (11, 5),
        ];

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, &vec![]).unwrap();
        }

        let window = 0..10;

        let results: FxHashMap<u64, f32> = unweighted_page_rank(&graph, window, usize::MAX)
            .into_iter()
            .map(|(k, v)| (k, v))
            .collect();

        assert_eq!(
            results,
            vec![
                (8, 7.1225),
                (5, 6.76125),
                (2, 9.18375),
                (7, 7.1225),
                (4, 5.06125),
                (1, 7.4837503),
                (3, 7.4837503)
            ]
            // {8: 1.0, 5: 0.575, 2: 0.5, 7: 1.0, 4: 0.5, 1: 1.0, 3: 1.0}
            // vec![(8, 20.7725), (5, 29.76125), (2, 25.38375), (7, 20.7725), (4, 16.161251), (1, 21.133749), (3, 21.133749)]
            .into_iter()
            .collect::<FxHashMap<u64, f32>>()
        );
    }
}
