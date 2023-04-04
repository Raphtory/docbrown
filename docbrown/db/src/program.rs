use std::{
    cell::{Ref, RefCell},
    fmt::Debug,
    ops::Range,
    rc::Rc,
    sync::Arc,
};

use docbrown_core::{
    agg::Accumulator,
    state::{self, AccId, ShuffleComputeState},
    state::{ComputeStateMap, StateType},
    tgraph_shard::errors::GraphError,
};
use itertools::Itertools;
use rayon::prelude::*;
use rustc_hash::FxHashSet;

use crate::vertex::VertexView;
use crate::view_api::GraphViewOps;
use crate::{
    graph::Graph,
    graph_window::{WindowedGraph, WindowedVertex},
    view_api::{internal::GraphViewInternalOps, VertexViewOps},
};

type CS = ComputeStateMap;

#[derive(Debug, Clone)]
pub struct AggRef<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(state::AccId<A, IN, OUT, ACC>)
where
    A: StateType;

pub struct LocalState {
    ss: usize,
    shard: usize,
    graph: Graph,
    window: Range<i64>,
    shard_local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
    next_vertex_set: Option<Arc<FxHashSet<u64>>>,
}

impl LocalState {
    pub fn new(
        ss: usize,
        shard: usize,
        graph: Graph,
        window: Range<i64>,
        shard_local_state: Rc<RefCell<ShuffleComputeState<CS>>>,
        next_vertex_set: Option<Arc<FxHashSet<u64>>>,
    ) -> Self {
        Self {
            ss,
            shard,
            graph,
            window,
            shard_local_state,
            next_vertex_set,
        }
    }

    pub(crate) fn agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_ref: state::AccId<A, IN, OUT, ACC>,
    ) -> AggRef<A, IN, OUT, ACC>
    where
        A: StateType,
    {
        AggRef(agg_ref)
    }

    pub(crate) fn global_agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_ref: state::AccId<A, IN, OUT, ACC>,
    ) -> AggRef<A, IN, OUT, ACC>
    where
        A: StateType,
    {
        AggRef(agg_ref)
    }

    pub(crate) fn step<F>(&self, f: F)
    where
        F: Fn(EvalVertexView),
    {
        let window_graph = Arc::new(WindowedGraph::new(
            self.graph.clone(),
            self.window.start,
            self.window.end,
        ));
        let graph = Arc::new(self.graph.clone());

        let iter = match self.next_vertex_set {
            None => window_graph.vertices_shard(self.shard),
            Some(ref next_vertex_set) => Box::new(
                next_vertex_set
                    .iter()
                    .flat_map(|&v| {
                        graph
                            .vertex_ref(v as u64)
                            .expect("vertex ID in set not available in graph")
                    })
                    .map(|vref| VertexView::new(window_graph.clone(), vref)),
            ),
        };

        let mut c = 0;
        println!("LOCAL STEP KICK-OFF");
        iter.for_each(|v| {
            f(EvalVertexView::new(
                self.ss,
                v,
                self.shard_local_state.clone(),
            ));
            c += 1;
            if c % 100000 == 0 {
                let t_id = std::thread::current().id();
                println!("LOCAL STEP {} vertices on {t_id:?}", c);
            }
        });
    }

    fn consume(self) -> ShuffleComputeState<CS> {
        Rc::try_unwrap(self.shard_local_state).unwrap().into_inner()
    }
}

#[derive(Debug)]
pub struct GlobalEvalState {
    pub ss: usize,
    g: Graph,
    window: Range<i64>,
    pub keep_past_state: bool,
    // running state
    pub next_vertex_set: Option<Vec<Arc<FxHashSet<u64>>>>,
    states: Vec<Arc<parking_lot::RwLock<Option<ShuffleComputeState<CS>>>>>,
    post_agg_state: Arc<parking_lot::RwLock<Option<ShuffleComputeState<CS>>>>, // FIXME this is a pointer to one of the states in states, beware of deadlocks
}

impl GlobalEvalState {
    pub fn read_vec_partitions<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg: &AccId<A, IN, OUT, ACC>,
    ) -> Vec<Vec<Vec<OUT>>>
    where
        OUT: StateType,
        A: 'static,
    {
        // println!("read_vec_partitions: {:#?}", self.states);
        self.states
            .iter()
            .map(|state| {
                let state = state.read();
                let state = state.as_ref().unwrap();
                state.read_vec_partition::<A, IN, OUT, ACC>(self.ss, agg)
            })
            .collect()
    }

    pub fn fold_state<A, IN, OUT, ACC: Accumulator<A, IN, OUT>, B, F>(
        &self,
        agg: &AccId<A, IN, OUT, ACC>,
        part_id: usize,
        b: B,
        f: F,
    ) -> B
    where
        OUT: StateType,
        A: StateType,
        B: Debug,
        F: Fn(B, &u64, OUT) -> B + std::marker::Copy,
    {
        let part_state = self.states[part_id].read();
        let part_state = part_state.as_ref().unwrap();

        part_state.fold_state::<A, IN, OUT, ACC, B, F>(self.ss, b, agg, f)
    }

    pub fn read_global_state<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg: &AccId<A, IN, OUT, ACC>,
    ) -> Option<OUT>
    where
        OUT: StateType,
        A: StateType,
    {
        let state = self.post_agg_state.read();
        let state = state.as_ref().unwrap();
        state.read_global(self.ss, agg)
    }

    pub fn do_loop(&self) -> bool {
        if self.next_vertex_set.is_none() {
            return true;
        }
        self.next_vertex_set.as_ref().map(|next_vertex_set_shard| {
            next_vertex_set_shard
                .iter()
                .any(|next_vertex_set| !next_vertex_set.is_empty())
        }) == Some(true)
    }

    // make new Context with n_parts as input
    pub fn new(g: Graph, window: Range<i64>, keep_past_state: bool) -> Self {
        let n_parts = g.nr_shards;
        let mut states = Vec::with_capacity(n_parts);
        for _ in 0..n_parts {
            states.push(Arc::new(parking_lot::RwLock::new(Some(
                ShuffleComputeState::new(n_parts),
            ))));
        }
        Self {
            ss: 0,
            g,
            keep_past_state,
            window,
            next_vertex_set: None,
            states,
            post_agg_state: Arc::new(parking_lot::RwLock::new(None)),
        }
    }

    pub(crate) fn global_agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        agg: state::AccId<A, IN, OUT, ACC>,
    ) -> AggRef<A, IN, OUT, ACC>
    where
        A: StateType,
    {
        self.agg(agg)
    }

    pub(crate) fn agg<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &mut self,
        agg: state::AccId<A, IN, OUT, ACC>,
    ) -> AggRef<A, IN, OUT, ACC>
    where
        A: StateType,
    {
        let states = self.states.clone();

        // remove the accumulated state represendet by agg_ref from the states
        // then merge it accross all states (in parallel)
        // update the post_agg_state
        let new_global_state = states
            .into_par_iter()
            .reduce_with(|left, right| {
                let t_id = std::thread::current().id();
                println!("MERGING aggregator states! {t_id:?}");
                // peel left
                let left_placeholder = &mut left.write();
                let mut state1 = left_placeholder.take().unwrap();
                // peel right
                let right_placeholder = &mut right.write();
                let state2 = right_placeholder.take().unwrap();

                state1.merge_mut(&state2, &agg, self.ss);
                state1.merge_mut_global(&state2, &agg, self.ss);

                **left_placeholder = Some(state1);
                **right_placeholder = Some(state2);

                println!("DONE MERGING aggregator states! {t_id:?}");
                left.clone()
            })
            .unwrap();

        if !Arc::ptr_eq(&self.post_agg_state, &new_global_state)
            && (&self.post_agg_state.read()).is_some()
        {
            let left_placeholder = &mut self.post_agg_state.write();
            let mut state1 = left_placeholder.take().unwrap();

            let right_placeholder = &mut new_global_state.write();
            let state2 = right_placeholder.take().unwrap();
            state1.merge_mut(&state2, &agg, self.ss);
        } else {
            self.post_agg_state = new_global_state;
        }

        // if the new state is not the same as the old one then we merge them too
        println!("DONE FULL MERGE!");
        AggRef(agg)
    }

    fn broadcast_state(&mut self) {
        let broadcast_state = self.post_agg_state.read();

        for state in self.states.iter() {
            // this avoids a deadlock since we may already hold the read lock
            if Arc::ptr_eq(&state, &self.post_agg_state) {
                continue;
            }

            let mut state = state.write();

            let prev = state.take();
            drop(prev); // not sure if this is needed but I really want the old state to be dropped
            let new_shard_state = broadcast_state.clone();
            *state = new_shard_state;
        }
    }

    pub(crate) fn step<F>(&mut self, f: F)
    where
        F: Fn(EvalVertexView) -> bool + Sync,
    {
        println!("START BROADCAST STATE");
        self.broadcast_state();
        println!("DONE BROADCAST STATE");

        let ss = self.ss;
        let graph = Arc::new(self.g.clone());
        let window_graph = Arc::new(WindowedGraph::new(
            self.g.clone(),
            self.window.start,
            self.window.end,
        ));
        let next_vertex_set = (0..self.g.shards.len())
            .collect_vec()
            .par_iter()
            .map(|shard| {
                println!("STARTED POST_EVAL SHARD {:#?}", shard);
                let i = *shard;
                let local_state = self.states[i].clone();
                // take control of the actual state
                let local_state = &mut local_state.write();
                let own_state = (local_state).take().unwrap();

                let mut next_vertex_set = own_state.changed_keys(i, ss).collect::<FxHashSet<_>>(); // FxHashSet::default();
                let prev_vertex_set = self
                    .next_vertex_set
                    .as_ref()
                    .map(|vs| vs[i].clone())
                    .unwrap_or_else(|| Arc::new(own_state.keys(i).collect()));

                let rc_state = Rc::new(RefCell::new(own_state));

                for vv in prev_vertex_set
                    .iter()
                    .flat_map(|v_id| {
                        graph
                            .vertex_ref(*v_id)
                            .expect("Vertex ID in set not available in graph")
                    })
                    .map(|v| WindowedVertex::new(window_graph.clone(), v))
                {
                    let evv = EvalVertexView::new(self.ss, vv, rc_state.clone());
                    let g_id = evv.global_id();
                    // we need to account for the vertices that will be included in the next step
                    if f(evv) {
                        next_vertex_set.insert(g_id);
                    }
                }

                // put back the modified keys
                let mut own_state: ShuffleComputeState<CS> =
                    Rc::try_unwrap(rc_state).unwrap().into_inner();
                if self.keep_past_state {
                    own_state.copy_over_next_ss(self.ss);
                }
                // put back the local state
                **local_state = Some(own_state);
                println!("DONE POST_EVAL SHARD {:#?}", shard);
                Arc::new(next_vertex_set)
            })
            .collect::<Vec<_>>();

        println!("DONE POST_EVAL SHARD ALL");
        self.next_vertex_set = Some(next_vertex_set);
    }
}

pub struct Entry<'a, A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>> {
    state: Ref<'a, ShuffleComputeState<CS>>,
    acc_id: AccId<A, IN, OUT, ACC>,
    i: usize,
    ss: usize,
}

// Entry implementation has read_ref function to access Option<&A>
impl<'a, A: StateType, IN, OUT, ACC: Accumulator<A, IN, OUT>> Entry<'a, A, IN, OUT, ACC> {
    pub fn new(
        state: Ref<'a, ShuffleComputeState<CS>>,
        acc_id: AccId<A, IN, OUT, ACC>,
        i: usize,
        ss: usize,
    ) -> Entry<'a, A, IN, OUT, ACC> {
        Entry {
            state,
            acc_id,
            i,
            ss,
        }
    }

    pub fn read_ref(&self) -> Option<&A> {
        self.state.read_ref(self.ss, self.i, &self.acc_id)
    }
}

pub struct EvalVertexView {
    ss: usize,
    vv: WindowedVertex,
    state: Rc<RefCell<ShuffleComputeState<CS>>>,
}

impl EvalVertexView {
    pub fn reset<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) where
        A: StateType,
    {
        let AggRef(agg) = agg_r;
        self.state
            .borrow_mut()
            .reset(self.ss, self.vv.id() as usize, &agg)
    }

    pub fn update<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
        a: IN,
    ) where
        A: StateType,
    {
        let AggRef(agg) = agg_r;
        self.state
            .borrow_mut()
            .accumulate_into(self.ss, self.vv.id() as usize, a, &agg)
    }

    pub fn global_update<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
        a: IN,
    ) where
        A: StateType,
    {
        let AggRef(agg) = agg_r;
        self.state.borrow_mut().accumulate_global(self.ss, a, &agg)
    }

    pub fn try_read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) -> Result<OUT, OUT>
    where
        A: StateType,
        OUT: Debug
    {
        self.state
            .borrow()
            .read(self.ss, self.vv.id() as usize, &agg_r.0)
            .ok_or(ACC::finish(&ACC::zero()))
    }

    pub fn read<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: Debug
    {
        self.state
            .borrow()
            .read(self.ss, self.vv.id() as usize, &agg_r.0)
            .unwrap_or(ACC::finish(&ACC::zero()))
    }

    pub fn entry<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) -> Entry<'_, A, IN, OUT, ACC>
    where
        A: StateType,
    {
        let ref_state = self.state.borrow();
        Entry::new(ref_state, agg_r.0.clone(), self.vv.id() as usize, self.ss)
    }

    pub fn try_read_prev<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) -> Result<OUT, OUT>
    where
        A: StateType,
        OUT: Debug
    {
        self.state
            .borrow()
            .read(self.ss + 1, self.vv.id() as usize, &agg_r.0)
            .ok_or(ACC::finish(&ACC::zero()))
    }

    pub fn read_prev<A, IN, OUT, ACC: Accumulator<A, IN, OUT>>(
        &self,
        agg_r: &AggRef<A, IN, OUT, ACC>,
    ) -> OUT
    where
        A: StateType,
        OUT: std::fmt::Debug,
    {
        self.try_read_prev::<A, IN, OUT, ACC>(agg_r)
            .or_else(|v| Ok::<OUT, OUT>(v))
            .unwrap()
    }

    pub fn new(ss: usize, vv: WindowedVertex, state: Rc<RefCell<ShuffleComputeState<CS>>>) -> Self {
        Self { ss, vv, state }
    }

    pub fn global_id(&self) -> u64 {
        self.vv.id()
    }

    pub fn out_degree(&self) -> Result<usize, GraphError> {
        self.vv.out_degree()
    }

    pub fn neighbours_out(&self) -> impl Iterator<Item = EvalVertexView> + '_ {
        self.vv
            .out_neighbours()
            .map(move |vv| EvalVertexView::new(self.ss, vv, self.state.clone()))
    }

    pub fn neighbours_in(&self) -> impl Iterator<Item = EvalVertexView> + '_ {
        self.vv
            .in_neighbours()
            .map(move |vv| EvalVertexView::new(self.ss, vv, self.state.clone()))
    }

    pub fn neighbours(&self) -> impl Iterator<Item = EvalVertexView> + '_ {
        self.vv
            .neighbours()
            .map(move |vv| EvalVertexView::new(self.ss, vv, self.state.clone()))
    }
}

pub trait Program {
    type Out;

    fn local_eval(&self, c: &LocalState);

    fn post_eval(&self, c: &mut GlobalEvalState);

    fn run_step(&self, g: &Graph, c: &mut GlobalEvalState)
    where
        Self: Sync,
    {
        println!("RUN STEP {:#?}", c.ss);

        let next_vertex_set = c.next_vertex_set.clone();
        let window = c.window.clone();
        let graph = g.clone();

        (0..g.nr_shards).collect_vec().par_iter().for_each(|shard| {
            let i = *shard;
            let local_state = c.states[i].clone();
            // take control of the actual state
            let local_state = &mut local_state
                .try_write()
                .expect("STATE LOCK SHOULD NOT BE CONTENDED");
            let own_state = (local_state).take().unwrap();

            let rc_state = LocalState::new(
                c.ss,
                i,
                graph.clone(),
                window.clone(),
                Rc::new(RefCell::new(own_state)),
                next_vertex_set.as_ref().map(|v| v[i].clone()),
            );

            self.local_eval(&rc_state);

            let t_id = std::thread::current().id();
            println!(
                "DONE LOCAL STEP ss: {}, shard: {}, thread: {t_id:?}",
                c.ss, i
            );
            // put back the state
            **local_state = Some(rc_state.consume());
        });

        // here we merge all the accumulators
        self.post_eval(c);
        println!("DONE POST STEP ss: {}", c.ss)
    }

    fn run(
        &self,
        g: &Graph,
        window: Range<i64>,
        keep_past_state: bool,
        iter_count: usize,
    ) -> GlobalEvalState
    where
        Self: Sync,
    {
        let mut c = GlobalEvalState::new(g.clone(), window.clone(), keep_past_state);

        let mut i = 0;
        while c.do_loop() && i < iter_count {
            self.run_step(&g, &mut c);
            if c.keep_past_state {
                c.ss += 1;
            }
            i += 1;
        }
        c
    }

    fn produce_output(&self, g: &Graph, window: Range<i64>, gs: &GlobalEvalState) -> Self::Out
    where
        Self: Sync;
}
