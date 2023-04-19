use crate::algorithms::*;
use crate::core::agg::*;
use crate::core::state::def::*;
use crate::core::state::*;
use crate::db::graph::Graph;
use crate::db::program::*;
use crate::db::view_api::GraphViewOps;
use rustc_hash::FxHashMap;

struct HitsS0 {
    hub_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    auth_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
}

impl HitsS0 {
    fn new() -> Self {
        Self {
            hub_score: val(0),
            auth_score: val(1),
        }
    }
}

impl Program for HitsS0 {
    type Out = ();

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let hub_score = c.agg(self.hub_score);
        let auth_score = c.agg(self.auth_score);
        c.step(|s| {
            s.update(&hub_score, MulF32::zero());
            s.update(&auth_score, MulF32::zero())
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        c.step(|_| true)
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct HitsS1 {
    hub_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    auth_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    recv_hub_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    recv_auth_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    total_hub_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    total_auth_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
}

impl HitsS1 {
    fn new() -> Self {
        Self {
            hub_score: val(0),
            auth_score: val(1),
            recv_hub_score: sum(2),
            recv_auth_score: sum(3),
            total_hub_score: sum(4),
            total_auth_score: sum(5),
        }
    }
}

impl Program for HitsS1 {
    type Out = ();

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let hub_score = c.agg(self.hub_score);
        let auth_score = c.agg(self.auth_score);
        let recv_hub_score = c.agg(self.recv_hub_score);
        let recv_auth_score = c.agg(self.recv_auth_score);
        let total_hub_score = c.global_agg(self.total_hub_score);
        let total_auth_score = c.global_agg(self.total_auth_score);

        c.step(|s| {
            for t in s.neighbours_out() {
                t.update(&recv_hub_score, SumF32(s.read(&hub_score).0))
            }
            for t in s.neighbours_in() {
                t.update(&recv_auth_score, SumF32(s.read(&auth_score).0))
            }
            s.global_update(&total_hub_score, SumF32(s.read(&hub_score).0));
            s.global_update(&total_auth_score, SumF32(s.read(&auth_score).0));
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.agg(self.recv_hub_score);
        let _ = c.agg(self.recv_auth_score);
        let _ = c.global_agg(self.total_hub_score);
        let _ = c.global_agg(self.total_auth_score);
        c.step(|_| true)
    }

    #[allow(unused_variables)]
    fn produce_output<G: GraphViewOps>(&self, g: &G, gs: &GlobalEvalState<G>) -> Self::Out
    where
        Self: Sync,
    {
    }
}

struct HitsS2 {
    hub_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    auth_score: AccId<MulF32, MulF32, MulF32, ValDef<MulF32>>,
    recv_hub_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    recv_auth_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    total_hub_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    total_auth_score: AccId<SumF32, SumF32, SumF32, SumDef<SumF32>>,
    max_diff_hub_score: AccId<f32, f32, f32, MaxDef<f32>>,
    max_diff_auth_score: AccId<f32, f32, f32, MaxDef<f32>>,
}

impl HitsS2 {
    fn new() -> Self {
        Self {
            hub_score: val(0),
            auth_score: val(1),
            recv_hub_score: sum(2),
            recv_auth_score: sum(3),
            total_hub_score: sum(4),
            total_auth_score: sum(5),
            max_diff_hub_score: max(6),
            max_diff_auth_score: max(7),
        }
    }
}

impl Program for HitsS2 {
    type Out = ();

    fn local_eval<G: GraphViewOps>(&self, c: &LocalState<G>) {
        let hub_score = c.agg(self.hub_score);
        let auth_score = c.agg(self.auth_score);
        let recv_hub_score = c.agg(self.recv_hub_score);
        let recv_auth_score = c.agg(self.recv_auth_score);
        let total_hub_score = c.global_agg(self.total_hub_score);
        let total_auth_score = c.global_agg(self.total_auth_score);
        let max_diff_hub_score = c.global_agg(self.max_diff_hub_score);
        let max_diff_auth_score = c.global_agg(self.max_diff_auth_score);

        c.step(|s| {
            println!("total_hub_score = {}", s.read_global(&total_hub_score).0);
            s.update(
                &hub_score,
                MulF32(s.read(&recv_auth_score).0 / s.read_global(&total_hub_score).0),
            );
            println!("total_auth_score = {}", s.read_global(&total_auth_score).0);
            s.update(
                &auth_score,
                MulF32(s.read(&recv_hub_score).0 / s.read_global(&total_auth_score).0),
            );

            let prev_hub_score = s.read_prev(&hub_score);
            let curr_hub_score = s.read(&hub_score);
            let md_hub_score = abs((prev_hub_score - curr_hub_score).0);
            s.global_update(&max_diff_hub_score, md_hub_score);

            let prev_auth_score = s.read_prev(&auth_score);
            let curr_auth_score = s.read(&auth_score);
            let md_auth_score = abs((prev_auth_score.clone() - curr_auth_score.clone()).0);
            s.global_update(&max_diff_auth_score, md_auth_score);
        });
    }

    fn post_eval<G: GraphViewOps>(&self, c: &mut GlobalEvalState<G>) {
        let _ = c.agg_reset(self.recv_hub_score);
        let _ = c.agg_reset(self.recv_auth_score);
        let _ = c.global_agg_reset(self.total_hub_score);
        let _ = c.global_agg_reset(self.total_auth_score);
        let _ = c.global_agg_reset(self.max_diff_hub_score);
        let _ = c.global_agg_reset(self.max_diff_auth_score);
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
pub fn hits(g: &Graph, window: Range<i64>, iter_count: usize) -> FxHashMap<u64, (f32, f32)> {
    let mut c = GlobalEvalState::new(g.clone(), true);
    let hits_s0 = HitsS0::new();
    let hits_s1 = HitsS1::new();
    let hits_s2 = HitsS2::new();

    hits_s0.run_step(g, &mut c);

    let max_diff_hub_score = 0.01f32;
    let max_diff_auth_score = max_diff_hub_score;
    let mut i = 0;

    loop {
        hits_s1.run_step(g, &mut c);
        println!("vec parts0: {:?}", c.read_vec_partitions(&val::<MulF32>(0)));
        println!("vec parts1: {:?}", c.read_vec_partitions(&val::<MulF32>(1)));

        hits_s2.run_step(g, &mut c);

        let r1 = c.read_global_state(&max::<f32>(6)).unwrap();
        println!("max_diff_hub = {:?}", r1);
        let r2 = c.read_global_state(&max::<f32>(7)).unwrap();
        println!("max_diff_auth = {:?}", r2);

        if (r1 <= max_diff_hub_score && r2 <= max_diff_auth_score) || i > iter_count {
            break;
        }

        if c.keep_past_state {
            c.ss += 1;
        }
        i += 1;
    }

    println!("i = {}", i);

    let mut results: FxHashMap<u64, (f32, f32)> = FxHashMap::default();

    (0..g.nr_shards)
        .into_iter()
        .fold(&mut results, |res, part_id| {
            let r = c.fold_state(&val::<MulF32>(0), part_id, res, |res, v_id, sc| {
                res.insert(*v_id, (sc.0, 0.0));
                res
            });
            c.fold_state(&val::<MulF32>(1), part_id, r, |res, v_id, sc| {
                let (a, _) = res.get(v_id).unwrap();
                res.insert(*v_id, (*a, sc.0));
                res
            })
        });

    results
}

#[cfg(test)]
mod hits_tests {
    use super::*;

    fn load_graph(n_shards: usize) -> Graph {
        let graph = Graph::new(n_shards);

        let edges = vec![(1, 2), (1, 4), (2, 3), (3, 1), (4, 1)];

        for (src, dst) in edges {
            graph.add_edge(0, src, dst, &vec![], None).unwrap();
        }
        graph
    }

    fn test_hits(n_shards: usize) {
        let graph = load_graph(n_shards);

        let window = 0..10;

        let results: FxHashMap<u64, (f32, f32)> = hits(&graph, window, 100).into_iter().collect();

        // ({1: 3.0327917367337087, 2: 1.9760675728086853e-16, 3: -1.0163958683668544, 4: -1.0163958683668544}, {1: -0.5040656372650013, 2: 0.7520328186325006, 3: 9.799998124421708e-17, 4: 0.7520328186325006})

        assert_eq!(
            results,
            vec![
                (2, (0.040000036, 0.31999972)),
                (4, (0.3200003, 0.31999972)),
                (1, (0.6400006, 0.63999945)),
                (3, (0.3200003, 0.039999966))
            ]
            // {8: 1.0, 5: 0.575, 2: 0.5, 7: 1.0, 4: 0.5, 1: 1.0, 3: 1.0}
            // vec![(8, 20.7725), (5, 29.76125), (2, 25.38375), (7, 20.7725), (4, 16.161251), (1, 21.133749), (3, 21.133749)]
            .into_iter()
            .collect::<FxHashMap<u64, (f32, f32)>>()
        );
    }

    #[test]
    fn test_hits_1() {
        test_hits(1);
    }
}
