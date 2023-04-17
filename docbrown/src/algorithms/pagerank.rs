use crate::core::{
    agg::{MaxDef, SumDef, ValDef},
    state::{
        def::{max, sum, val},
        AccId,
    },
};
use crate::db::{
    graph::Graph,
    program::{AggRef, GlobalEvalState, LocalState, Program},
    view_api::GraphViewOps,
};
use num_traits::{abs, Bounded, Zero};
use rustc_hash::FxHashMap;
use std::ops::{Add, AddAssign, Div, Mul, Range, Sub};

#[derive(PartialEq, PartialOrd, Copy, Clone, Debug)]
struct MulF32(f32);

const MUL_F32_ZERO: MulF32 = MulF32(1.0f32);

impl Zero for MulF32 {
    fn zero() -> Self {
        MUL_F32_ZERO
    }

    fn set_zero(&mut self) {
        *self = Zero::zero();
    }

    fn is_zero(&self) -> bool {
        *self == MUL_F32_ZERO
    }
}

impl Add for MulF32 {
    type Output = MulF32;

    fn add(self, rhs: Self) -> Self::Output {
        MulF32(self.0 + rhs.0)
    }
}

impl AddAssign for MulF32 {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0 + rhs.0
    }
}

impl Sub for MulF32 {
    type Output = MulF32;

    fn sub(self, rhs: Self) -> Self::Output {
        MulF32(self.0 - rhs.0)
    }
}

impl Div for MulF32 {
    type Output = MulF32;

    fn div(self, rhs: Self) -> Self::Output {
        MulF32(self.0 / rhs.0)
    }
}

impl Mul for MulF32 {
    type Output = MulF32;

    fn mul(self, rhs: Self) -> Self::Output {
        MulF32(self.0 * rhs.0)
    }
}

impl Bounded for MulF32 {
    fn min_value() -> Self {
        MulF32(f32::MIN)
    }

    fn max_value() -> Self {
        MulF32(f32::MAX)
    }
}

#[derive(PartialEq, PartialOrd, Copy, Clone, Debug)]
struct SumF32(f32);

impl Zero for SumF32 {
    fn zero() -> Self {
        SumF32(0.0f32)
    }

    fn set_zero(&mut self) {
        *self = Zero::zero();
    }

    fn is_zero(&self) -> bool {
        *self == SumF32(1.0f32)
    }
}

impl Add for SumF32 {
    type Output = SumF32;

    fn add(self, rhs: Self) -> Self::Output {
        SumF32(self.0 + rhs.0)
    }
}

impl AddAssign for SumF32 {
    fn add_assign(&mut self, rhs: Self) {
        self.0 = self.0 + rhs.0
    }
}

impl Sub for SumF32 {
    type Output = SumF32;

    fn sub(self, rhs: Self) -> Self::Output {
        SumF32(self.0 - rhs.0)
    }
}

impl Div for SumF32 {
    type Output = SumF32;

    fn div(self, rhs: Self) -> Self::Output {
        SumF32(self.0 / rhs.0)
    }
}

impl Mul for SumF32 {
    type Output = SumF32;

    fn mul(self, rhs: Self) -> Self::Output {
        SumF32(self.0 * rhs.0)
    }
}

impl Bounded for SumF32 {
    fn min_value() -> Self {
        SumF32(f32::MIN)
    }

    fn max_value() -> Self {
        SumF32(f32::MAX)
    }
}

struct UnweightedPageRankS0 {
    total_vertices: usize,
    score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
}

impl UnweightedPageRankS0 {
    fn new(total_vertices: usize) -> Self {
        Self {
            total_vertices,
            score: val(0),
        }
    }
}

impl Program for UnweightedPageRankS0 {
    type Out = ();

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let score: AggRef<MulF32, MulF32, MulF32, ValDef<MulF32>> = c.agg(self.score);

        c.step(|s| s.update(&score, MulF32(1f32 / self.total_vertices as f32)));
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.agg(self.score);
        c.step(|_| true)
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct UnweightedPageRankS1 {
    score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    recv_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
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

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let score: AggRef<MulF32, MulF32, MulF32, ValDef<MulF32>> = c.agg(self.score.clone());
        let recv_score: AggRef<SumF32, SumF32, SumF32, SumDef<SumF32>> =
            c.agg(self.recv_score.clone());

        c.step(|s| {
            let out_degree = s.out_degree();
            if out_degree > 0 {
                let new_score = s.read(&score).0 / out_degree as f32;
                for t in s.neighbours_out() {
                    t.update(&recv_score, SumF32(new_score))
                }
            }
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.agg(self.recv_score);
        c.step(|_| true)
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct UnweightedPageRankS2 {
    score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    recv_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    max_diff: AccId<f32, f32, f32, MaxDef<f32>>,
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

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let damping_factor = 0.85;
        let score: AggRef<MulF32, MulF32, MulF32, ValDef<MulF32>> = c.agg(self.score);
        let recv_score: AggRef<SumF32, SumF32, SumF32, SumDef<SumF32>> =
            c.agg(self.recv_score.clone());
        let max_diff: AggRef<f32, f32, f32, MaxDef<f32>> = c.global_agg(self.max_diff);

        c.step(|s| {
            s.update(
                &score,
                MulF32((1f32 - damping_factor) + (damping_factor * s.read(&recv_score).0)),
            );
            let prev = s.read_prev(&score);
            let curr = s.read(&score);
            let md = abs((prev.clone() - curr.clone()).0);
            println!(
                "prev = {:?}, curr = {:?}, id = {}, max_diff = {:?}",
                prev,
                curr,
                s.global_id(),
                md
            );
            s.global_update(&max_diff, md);
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.global_agg(self.max_diff);
        c.step(|s| {

            let curr = s.read(&AggRef::new(self.score));
            let prev = s.read_prev(&AggRef::new(self.score));

            println!(
                "POST EVAL prev = {:?}, curr = {:?}, id = {}",
                prev,
                curr,
                s.global_id(),
            );
            true

        })
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct UnweightedPageRankS3 {
    recv_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    max_diff: AccId<f32, f32, f32, MaxDef<f32>>,
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

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let recv_score: AggRef<SumF32, SumF32, SumF32, SumDef<SumF32>> =
            c.agg(self.recv_score.clone());
        let max_diff: AggRef<f32, f32, f32, MaxDef<f32>> = c.global_agg(self.max_diff.clone());

        c.step(|s| {
            s.reset(&recv_score);
            s.global_reset(&max_diff);
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.global_agg(max::<f32>(2));
        let _ = c.agg(sum::<SumF32>(1));
        c.step(|_| true)
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

#[allow(unused_variables)]
pub fn unweighted_page_rank(
    g: &Graph,
    window: Range<i64>,
    iter_count: usize,
) -> FxHashMap<u64, f32> {
    let mut c = GlobalEvalState::new(g.clone(), true);
    let pg_s0 = UnweightedPageRankS0::new(g.num_vertices());
    let pg_s1 = UnweightedPageRankS1::new();
    let pg_s2 = UnweightedPageRankS2::new();
    let pg_s3 = UnweightedPageRankS3::new();

    let max_diff = 0.01f32;
    let mut i = 0;

    pg_s0.run_step(g, &mut c);

    loop {
        pg_s1.run_step(g, &mut c);
        println!("vec parts: {:?}", c.read_vec_partitions(&val::<MulF32>(0)));

        pg_s2.run_step(g, &mut c);

        let r = c.read_global_state(&max::<f32>(2)).unwrap();
        println!("max_diff = {:?}", r);

        if r <= max_diff || i > iter_count {
            break;
        }

        pg_s3.run_step(g, &mut c);

        if c.keep_past_state {
            c.ss += 1;
        }
        i += 1;
    }

    let mut results: FxHashMap<u64, f32> = FxHashMap::default();

    (0..g.nr_shards)
        .into_iter()
        .fold(&mut results, |res, part_id| {
            c.fold_state(&val::<MulF32>(0), part_id, res, |res, v_id, sc| {
                res.insert(*v_id, sc.0);
                res
            })
        });

    results
}

#[cfg(test)]
mod page_rank_tests {
    use pretty_assertions::assert_eq;

    use crate::core::{agg::Accumulator, state::StateType};

    use super::*;

    fn load_graph(n_shards: usize) -> Graph {
        let graph = Graph::new(n_shards);

        let edges = vec![(1, 2), (1, 4), (2, 3), (3, 1), (4, 1)];

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, &vec![], None).unwrap();
        }
        graph
    }

    #[test]
    fn test_page_rank() {
        let graph = load_graph(1);

        let window = 0..10;

        let results: FxHashMap<u64, f32> = unweighted_page_rank(&graph, window, 20)
            .into_iter()
            .collect();

        assert_eq!(
            results,
            vec![
                (2, 0.78044075),
                (4, 0.78044075),
                (1, 1.4930439),
                (3, 0.8092761)
            ]
            // {8: 1.0, 5: 0.575, 2: 0.5, 7: 1.0, 4: 0.5, 1: 1.0, 3: 1.0}
            // vec![(8, 20.7725), (5, 29.76125), (2, 25.38375), (7, 20.7725), (4, 16.161251), (1, 21.133749), (3, 21.133749)]
            .into_iter()
            .collect::<FxHashMap<u64, f32>>()
        );
    }

    #[test]
    fn test_page_rank_step1() {
        let graph_1 = load_graph(1);
        let graph_2 = load_graph(2);

        let mut c_g1 = GlobalEvalState::new(graph_1.clone(), true);
        let pg_s0_g1 = UnweightedPageRankS0::new(graph_1.num_vertices());

        let mut c_g2 = GlobalEvalState::new(graph_2.clone(), true);
        let pg_s0_g2 = UnweightedPageRankS0::new(graph_2.num_vertices());

        // run step1 for graph1
        pg_s0_g1.run_step(&graph_1, &mut c_g1);
        // run step1 for graph2
        pg_s0_g2.run_step(&graph_2, &mut c_g2);

        let (actual_g1_part0, actual_g2) = lift_state(pg_s0_g1.score, &c_g1,&c_g2);
        assert_partitions_data_equal_post_step(actual_g1_part0, actual_g2);
    }


    #[test]
    fn test_page_rank_step2() {
        let graph_1 = load_graph(1);
        let graph_2 = load_graph(2);

        let mut c_g1 = GlobalEvalState::new(graph_1.clone(), true);
        let pg_s0_g1 = UnweightedPageRankS0::new(graph_1.num_vertices());
        let pg_s1_g1 = UnweightedPageRankS1::new();

        let mut c_g2 = GlobalEvalState::new(graph_2.clone(), true);
        let pg_s0_g2 = UnweightedPageRankS0::new(graph_2.num_vertices());
        let pg_s1_g2 = UnweightedPageRankS1::new();

        // run step1 for graph1
        pg_s0_g1.run_step(&graph_1, &mut c_g1);
        // run step1 for graph2
        pg_s0_g2.run_step(&graph_2, &mut c_g2);

        let (actual_g1_part0, actual_g2) = lift_state(pg_s0_g1.score, &c_g1,&c_g2);

        assert_partitions_data_equal_post_step(actual_g1_part0, actual_g2);

        // run step2 for graph1
        pg_s1_g1.run_step(&graph_1, &mut c_g1);
        // run step2 for graph2
        pg_s1_g2.run_step(&graph_2, &mut c_g2);

        let (actual_g1_part0, actual_g2) = lift_state(pg_s1_g1.score, &c_g1,&c_g2);
        assert_partitions_data_equal_post_step(actual_g1_part0, actual_g2);

        let (actual_g1_part0, actual_g2) = lift_state(pg_s1_g1.recv_score, &c_g1,&c_g2);
        assert_partitions_data_equal_post_step(actual_g1_part0, actual_g2);

    }

    #[test]
    fn test_page_rank_step3() {
        let graph_1 = load_graph(1);
        let graph_2 = load_graph(2);

        let mut c_g1 = GlobalEvalState::new(graph_1.clone(), true);
        let pg_s0_g1 = UnweightedPageRankS0::new(graph_1.num_vertices());
        let pg_s1_g1 = UnweightedPageRankS1::new();
        let pg_s2_g1 = UnweightedPageRankS2::new();

        let mut c_g2 = GlobalEvalState::new(graph_2.clone(), true);
        let pg_s0_g2 = UnweightedPageRankS0::new(graph_2.num_vertices());
        let pg_s1_g2 = UnweightedPageRankS1::new();
        let pg_s2_g2 = UnweightedPageRankS2::new();

        // run step1 for graph1
        pg_s0_g1.run_step(&graph_1, &mut c_g1);
        // run step1 for graph2
        pg_s0_g2.run_step(&graph_2, &mut c_g2);

        let (actual_g1_part0, actual_g2) = lift_state(pg_s0_g1.score, &c_g1,&c_g2);

        assert_partitions_data_equal_post_step(actual_g1_part0, actual_g2);

        // run step2 for graph1
        pg_s1_g1.run_step(&graph_1, &mut c_g1);
        // run step2 for graph2
        pg_s1_g2.run_step(&graph_2, &mut c_g2);

        let (actual_g1_part0, actual_g2) = lift_state(pg_s1_g1.score, &c_g1,&c_g2);
        assert_partitions_data_equal_post_step(actual_g1_part0, actual_g2);

        let (actual_g1_part0, actual_g2) = lift_state(pg_s1_g1.recv_score, &c_g1,&c_g2);
        assert_partitions_data_equal_post_step(actual_g1_part0, actual_g2);

        // run step3 for graph1
        pg_s2_g1.run_step(&graph_1, &mut c_g1);
        // run step3 for graph2
        pg_s2_g2.run_step(&graph_2, &mut c_g2);

        // println!("SHARDS! c_g2: {:?}", c_g2);

        let (actual_g1_part0, actual_g2) = lift_state(pg_s2_g1.score, &c_g1,&c_g2);
        assert_partitions_data_equal_post_step(actual_g1_part0, actual_g2);

        let (actual_g1_part0, actual_g2) = lift_state(pg_s2_g1.recv_score, &c_g1, &c_g2);
        assert_partitions_data_equal_post_step(actual_g1_part0, actual_g2);

    }

    fn lift_state<A: 'static, IN, OUT: StateType, ACC: Accumulator<A, IN, OUT>>(
        acc_id: AccId<A, IN, OUT, ACC>,
        c_g1: &GlobalEvalState<Graph>,
        c_g2: &GlobalEvalState<Graph>,
    ) -> (Vec<OUT>, Vec<Vec<Vec<OUT>>>) {
        let actual_g1 = c_g1.read_vec_partitions(&acc_id);
        assert!(actual_g1.len() == 1);
        let actual_g1_part0 = &actual_g1[0][0];

        let actual_g2 = c_g2.read_vec_partitions(&acc_id);

        (actual_g1_part0.clone(), actual_g2)
    }

    fn assert_partitions_data_equal_post_step<A: PartialEq + std::fmt::Debug>(
        actual_g1: Vec<A>,
        actual_g2: Vec<Vec<Vec<A>>>,
    ) {
        println!("actual_g1 = {:?}", actual_g1);
        println!("actual_g2 = {:?}", actual_g2);

        let actual_g2_part1_view = &actual_g2[0];
        let actual_g2_part2_view = &actual_g2[1];

        assert_eq!(actual_g2_part1_view, actual_g2_part2_view);

        let actual_g2_local_part_0 = &actual_g2_part1_view[0];
        let actual_g2_local_part_1 = &actual_g2_part1_view[1];

        assert_eq!(actual_g2_local_part_0, &actual_g1[0..2]);
        assert_eq!(actual_g2_local_part_1, &actual_g1[2..]);
    }
}
