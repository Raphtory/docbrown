use std::{
    cell::RefCell,
    collections::{hash_map::Entry, HashMap},
    hash::Hash,
    ops::{AddAssign, Range},
    option::Option,
    rc::Rc,
};

use rustc_hash::FxHashSet;

use crate::{
    tgraph::{TemporalGraph, VertexView},
    Direction,
};

pub trait Eval {
    fn eval<'a, FMAP, PRED>(
        &'a self,
        c: Option<Context>,
        window: Range<i64>,
        f: FMAP,
        having: PRED,
    ) -> Context
    where
        FMAP: Fn(&mut EvalVertexView<'a, Self>, &mut Context) -> Vec<VertexRef>,
        PRED: Fn(&EvalVertexView<'a, Self>, &mut Context) -> bool,
        Self: Sized + 'a;
}

pub struct VertexRef(usize);

impl VertexRef {
    fn pid(&self) -> usize {
        self.0
    }
}

/// In abstract algebra, a branch of mathematics, a monoid is
/// a set equipped with an associative binary operation and an identity element.
/// For example, the nonnegative integers with addition form a monoid, the identity element being 0.
/// Associativity
///    For all a, b and c in S, the equation (a • b) • c = a • (b • c) holds.
/// Identity element
///    There exists an element e in S such that for every element a in S, the equalities e • a = a and a • e = a hold.
#[derive(Clone, Debug)]
pub struct Monoid<K, A, F> {
    pub(crate) id: A,
    pub(crate) bin_op: F,
    pub(crate) state: Rc<RefCell<StateStore<K, A>>>,
}

impl<K, A, F> Monoid<K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    A: Clone,
    K: std::cmp::Eq + Hash + Clone,
{
    pub fn new(id: A, bin_op: F) -> Self {
        Self {
            id,
            bin_op,
            state: Rc::new(RefCell::new(StateStore::new())),
        }
    }

    pub fn consume(self) -> StateStore<K, A> {
        Rc::try_unwrap(self.state)
            .ok()
            .expect("Monoid is still in use")
            .into_inner()
    }
}

pub struct Context {
    ss: u64, // the superstep decides which state do we use from the accuumulators, we start at 0 and we flip flop between odd and even
}

impl Context {
    fn new() -> Self {
        Self { ss: 0 }
    }

    fn inc(&mut self) {
        self.ss += 1;
    }

    pub fn acc<'a, 'b, A, F>(&'a mut self, monoid: &'b Monoid<u64, A, F>) -> Accumulator<u64, A, F>
    where
        A: Clone,
        F: Fn(&mut A, A) + Clone,
    {
        Accumulator::new(monoid, self.ss)
    }

    #[cfg(test)]
    fn as_hash_map<A: Clone, F>(&self, m: &Monoid<u64, A, F>) -> Option<HashMap<u64, A>> {
        m.state
            .try_borrow_mut()
            .ok()
            .map(|mut s| s.copy_hash_map(self.ss))
    }
}

#[derive(Clone, PartialEq, Debug)]
pub(crate) struct PairAcc<A> {
    even: Option<A>,
    odd: Option<A>,
}

impl<A> PairAcc<A> {
    fn new(current: A, ss: u64) -> Self {
        let (even, odd) = if ss % 2 == 0 {
            (Some(current), None)
        } else {
            (None, Some(current))
        };
        Self { even, odd }
    }
}

impl<A> PairAcc<A> {
    fn as_mut(&mut self, ss: u64) -> &mut Option<A> {
        if ss % 2 == 0 {
            &mut self.even
        } else {
            &mut self.odd
        }
    }

    fn current(&self, ss: u64) -> Option<&A> {
        if ss % 2 == 0 {
            self.even.as_ref()
        } else {
            self.odd.as_ref()
        }
    }

    fn prev(&self, ss: u64) -> Option<&A> {
        if ss % 2 == 0 {
            self.odd.as_ref()
        } else {
            self.even.as_ref()
        }
    }
}

impl<A: Clone> PairAcc<A> {
    fn copy_from_prev(&mut self, ss: u64) {
        if ss % 2 == 0 {
            self.even = self.odd.clone();
        } else {
            self.odd = self.even.clone();
        }
    }
}

#[derive(Debug)]
pub struct StateStore<K, A> {
    state: HashMap<K, PairAcc<A>>,
    pub(crate) ss: Option<u64>,
}

impl<K: std::cmp::Eq + Hash + Clone, A> StateStore<K, A> {
    fn new() -> Self {
        Self {
            state: HashMap::new(),
            ss: None,
        }
    }
}

impl<K: std::cmp::Eq + Hash + Clone, A: Clone> StateStore<K, A> {
    fn update_state_from_ss(&mut self, ss: u64) {
        // if our current ss is different than the argument we copy all the values from prev to current

        if self.ss.is_none() {
            self.ss = Some(ss);
        } else if self.ss < Some(ss) {
            // TODO: this could possibly be optimised but in our case we just flip all the entries in the state store
            // perhaps we could pass the active set here and only flip those?
            for (_, pair) in self.state.iter_mut() {
                pair.copy_from_prev(ss);
            }

            self.ss = Some(ss);
        }
    }

    fn entry(&mut self, ss: u64, k: K) -> Entry<K, PairAcc<A>> {
        // if our current ss is different than the argument we copy all the values from prev to current
        self.update_state_from_ss(ss);
        self.state.entry(k)
    }

    fn get(&mut self, ss: u64, k: &K) -> Option<&PairAcc<A>> {
        self.update_state_from_ss(ss);
        self.state.get(k)
    }
}

impl<K: Clone + std::cmp::Eq + std::hash::Hash, A: Clone> StateStore<K, A> {
    #[cfg(test)]
    fn copy_hash_map(&mut self, ss: u64) -> HashMap<K, A> {
        self.update_state_from_ss(ss);
        self.state
            .iter()
            .flat_map(|(k, v)| v.current(ss).map(|v| (k.clone(), v.clone())))
            .collect()
    }
}

#[derive(Clone)]
pub struct Accumulator<K: std::cmp::Eq + std::hash::Hash, A: Clone, F> {
    state: Rc<RefCell<StateStore<K, A>>>,
    acc: Monoid<K, A, F>,
    ss: u64,
}

impl<K, A, F> Accumulator<K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
{
    fn new(acc: &Monoid<K, A, F>, ss: u64) -> Self {
        Self {
            state: acc.state.clone(),
            acc: acc.clone(),
            ss,
        }
    }

    fn accumulate(&self, id: K, value: A) {
        let mut state = self.state.borrow_mut();

        state
            .entry(self.ss, id)
            .and_modify(|e| {
                let acc = e.as_mut(self.ss);

                if let Some(acc) = acc {
                    (self.acc.bin_op)(acc, value.clone());
                } else {
                    // clone the previous step into current
                    let acc2 = e.as_mut(self.ss).get_or_insert(self.acc.id.clone());
                    (self.acc.bin_op)(acc2, value.clone());
                }
            })
            .or_insert_with(|| {
                let mut v = self.acc.id.clone();
                (self.acc.bin_op)(&mut v, value.clone());
                PairAcc::new(v, self.ss)
            });
    }

    fn read_prev(&self, k: &K) -> Option<A> {
        self.state
            .borrow_mut()
            .get(self.ss, k)
            .and_then(|e| e.prev(self.ss).cloned())
    }

    fn read(&self, k: &K) -> Option<A> {
        self.state
            .borrow_mut()
            .get(self.ss, k)
            .and_then(|e| e.current(self.ss).cloned())
    }
}

pub struct AccEntry<'a, K: std::cmp::Eq + std::hash::Hash, A: std::clone::Clone, F> {
    parent: &'a Accumulator<K, A, F>,
    k: K,
}

impl<'a, K, A, F> AccEntry<'a, K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
{
    pub fn read(&self) -> Option<A> {
        self.parent.read(&self.k)
    }

    pub fn read_prev(&self) -> Option<A> {
        self.parent.read_prev(&self.k)
    }
}

impl<'a, K, A, F> AccEntry<'a, K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
{
    fn new(parent: &'a Accumulator<K, A, F>, k: K) -> AccEntry<'a, K, A, F> {
        AccEntry { parent, k }
    }
}

impl<'a, K, A, F> AddAssign<A> for AccEntry<'a, K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
{
    fn add_assign(&mut self, rhs: A) {
        self.parent.accumulate(self.k.clone(), rhs);
    }
}

impl<'a, K, A, F> AddAssign<Option<A>> for AccEntry<'a, K, A, F>
where
    F: Fn(&mut A, A),
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
    Self: AddAssign<A>,
{
    fn add_assign(&mut self, rhs: Option<A>) {
        match rhs {
            Some(rhs0) => {
                self.add_assign(rhs0);
            }
            None => {}
        }
    }
}

impl<'a, K, A, F> AddAssign<AccEntry<'a, K, A, F>> for AccEntry<'a, K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
    Self: AddAssign<Option<A>>,
{
    fn add_assign(&mut self, rhs: AccEntry<K, A, F>) {
        self.add_assign(rhs.read());
    }
}

impl Eval for TemporalGraph {
    fn eval<'a, FMAP, PRED>(
        &'a self,
        c: Option<Context>,
        window: Range<i64>,
        f: FMAP,
        having: PRED,
    ) -> Context
    where
        FMAP: Fn(&mut EvalVertexView<'a, Self>, &mut Context) -> Vec<VertexRef>,
        PRED: Fn(&EvalVertexView<'a, Self>, &mut Context) -> bool,
        Self: Sized + 'a,
    {
        // we start with all the vertices considered inside the working set
        let mut cur_active_set: WorkingSet<usize> = WorkingSet::All;
        let mut next_active_set = FxHashSet::default();
        let mut ctx = c.unwrap_or_else(|| Context::new());

        while !cur_active_set.is_empty() {
            // define iterator over the active vertices
            let iter = if !cur_active_set.is_all() {
                let active_vertices_iter = cur_active_set.iter().map(|pid| {
                    let g_id = self.adj_lists[*pid].logical();
                    VertexView::new(self, *g_id, *pid, Some(window.clone()))
                });
                Box::new(active_vertices_iter)
            } else {
                self.vertices_window(window.clone())
            };

            // iterate over the active vertices
            for v_view in iter {
                let mut eval_v_view = EvalVertexView { vv: v_view };
                let next_vertices = f(&mut eval_v_view, &mut ctx);
                for next_vertex in next_vertices {
                    next_active_set.insert(next_vertex.pid());
                }
            }

            // from the next_active_set we apply the PRED
            next_active_set.retain(|pid| {
                let g_id = self.adj_lists[*pid].logical();
                let v_view = VertexView::new(self, *g_id, *pid, Some(window.clone()));
                having(&EvalVertexView { vv: v_view }, &mut ctx)
            });

            cur_active_set = WorkingSet::Set(next_active_set);
            next_active_set = FxHashSet::default();
            ctx.inc();
        }
        ctx
    }
}

// view over the vertex
// this includes the state during the evaluation
pub struct EvalVertexView<'a, G> {
    vv: VertexView<'a, G>,
}

// here we implement the Fn trait for the EvalVertexView to return Option<AccumulatorEntry>

impl<'a> EvalVertexView<'a, TemporalGraph> {
    pub fn get<'b, A: Clone, F>(&self, acc: &'b Accumulator<u64, A, F>) -> AccEntry<'b, u64, A, F>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        AccEntry::new(acc, id)
    }

    pub fn get_prev<A: Clone, F>(&self, acc: &'a Accumulator<u64, A, F>) -> Option<A>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        acc.read_prev(&id)
    }

    pub fn as_vertex_ref(&self) -> VertexRef {
        VertexRef(self.vv.pid)
    }
}

impl<'a> EvalVertexView<'a, TemporalGraph> {
    pub fn neighbours(
        &'a self,
        d: Direction,
    ) -> impl Iterator<Item = EvalVertexView<'a, TemporalGraph>> {
        self.vv.neighbours(d).map(move |vv| EvalVertexView { vv })
    }
}

enum WorkingSet<A> {
    All,
    Set(FxHashSet<A>),
}

impl<A> WorkingSet<A>
where
    A: Eq + std::hash::Hash,
{
    fn is_empty(&self) -> bool {
        match self {
            WorkingSet::All => false,
            WorkingSet::Set(s) => s.is_empty(),
        }
    }

    fn is_all(&self) -> bool {
        match self {
            WorkingSet::All => true,
            _ => false,
        }
    }

    fn iter(&self) -> impl Iterator<Item = &A> {
        match self {
            WorkingSet::All => panic!("cannot iterate over all"),
            WorkingSet::Set(s) => s.iter(),
        }
    }
}

#[cfg(test)]
mod eval_test {
    use std::{
        collections::{hash_map, HashMap},
        marker::PhantomData,
    };

    use crate::{
        eval::{Eval, Monoid, PairAcc},
        tgraph::TemporalGraph,
        utils::get_shard_id_from_global_vid,
    };

    use super::{AccEntry, Context};

    use arrow2::{
        array::{MutableArray, MutablePrimitiveArray, PrimitiveArray},
        types::NativeType,
    };
    use pretty_assertions::assert_eq;

    #[test]
    fn test_pair_acc() {
        let mut acc = PairAcc::new(0, 0);
        acc.as_mut(0).as_mut().map(|v| *v += 1);
        acc.as_mut(0).as_mut().map(|v| *v += 2);
        acc.as_mut(0).as_mut().map(|v| *v += 3);
    }

    #[test]
    fn storage_on_different_super_steps() {
        let sum = Monoid::new(0, |a: &mut u64, b: u64| *a += b);

        let mut ctx = Context::new();

        let acc = ctx.acc(&sum);
        let mut entry = AccEntry::new(&acc, 0);

        entry += 1;
        entry += 2;

        assert_eq!(acc.read(&0), Some(3));

        ctx.inc();

        let acc = ctx.acc(&sum);
        let entry = AccEntry::new(&acc, 0);

        // read current value must be copied from last time on first read
        assert_eq!(entry.read(), Some(3));
        assert_eq!(entry.read_prev(), Some(3));

        assert_eq!(acc.read(&0), Some(3));
        assert_eq!(acc.read_prev(&0), Some(3));
    }

    // test that monoids are correctly applied
    #[test]
    fn test_simple_sum_2_steps() {
        let sum = Monoid::new(0, |a: &mut u64, b: u64| *a += b);

        let mut ctx = Context::new();

        let acc = ctx.acc(&sum);
        let mut entry = AccEntry::new(&acc, 0);

        entry += 1;
        entry += 2;

        ctx.inc();

        assert_eq!(acc.read(&0), Some(3));
        assert_eq!(acc.read_prev(&0), None);

        let actual = ctx.as_hash_map(&sum);

        assert_eq!(actual, Some(vec![(0, 3)].into_iter().collect()));

        let acc = ctx.acc(&sum);
        let mut entry = AccEntry::new(&acc, 0);

        entry += 1;
        entry += 2;

        assert_eq!(acc.read(&0), Some(6));
        assert_eq!(acc.read_prev(&0), Some(3));

        let actual = ctx.as_hash_map(&sum);

        assert_eq!(actual, Some(vec![(0, 6)].into_iter().collect()));
    }

    #[test]
    fn eval_2_simple_connected_components() {
        let mut g = TemporalGraph::default();

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
            g.add_edge(ts, src, dst);
        }

        let min = Monoid::new(u64::MAX, |a: &mut u64, b: u64| *a = u64::min(*a, b));

        // initial step where we init every vertex to it's own ID
        let state = g.eval(
            None,
            0..9,
            |vertex, ctx| {
                let min_cc_id = ctx.acc(&min); // get the accumulator for the min_cc_id

                let mut min_acc = vertex.get(&min_cc_id); // get the entry for this vertex in the min_cc_id accumulator

                min_acc += vertex.vv.global_id(); // set the value to the global id of the vertex

                vec![] // nothing to propagate at this step
            },
            // insert exchange step before having
            |_, _| false,
        );

        assert_eq!(state.ss, 1);
        let actual = state.as_hash_map(&min).unwrap();

        assert_eq!(
            actual,
            HashMap::from_iter(vec![
                (1, 1),
                (2, 2),
                (3, 3),
                (4, 4),
                (5, 5),
                (6, 6),
                (7, 7),
                (8, 8),
            ])
        );

        // second step where we check the state of our neighbours and set ourselves to the min
        // we stop when the state of the vertex does not change
        let state = g.eval(
            Some(state),
            0..9,
            |vertex, ctx| {
                let min_cc_id = ctx.acc(&min); // get the accumulator for the min_cc_id

                let mut out = vec![];
                for neighbour in vertex.neighbours(crate::Direction::BOTH) {
                    let mut n_min_acc = neighbour.get(&min_cc_id);

                    n_min_acc += vertex.get(&min_cc_id);

                    out.push(neighbour.as_vertex_ref());
                }

                out
            },
            |v, ctx| {
                let min_cc_id = ctx.acc(&min); // get the accumulator for the min_cc_id

                let min_acc = v.get(&min_cc_id).read();
                let prev_min_acc = v.get_prev(&min_cc_id);

                min_acc != prev_min_acc
            },
        );

        assert_eq!(state.ss, 3);
        let state = state.as_hash_map(&min).unwrap();

        assert_eq!(
            state,
            HashMap::from_iter(vec![
                (1, 1),
                (2, 1),
                (3, 1),
                (4, 1),
                (5, 1),
                (6, 1),
                (7, 7),
                (8, 7),
            ])
        );
    }

    // take two monoids in 2 vectors, zip them with 2 graphs
    // merge the monoids in parallel

    use rand::Rng;
    use rayon::prelude::*;
    use rustc_hash::FxHashMap;

    #[derive(Clone)]
    pub(crate) struct AggDef<K, A, OUT, ACC, FIN>
    where
        K: Eq + std::hash::Hash + Clone + 'static,
        OUT: Send + Clone + 'static,
        ACC: Fn(&mut A, A) + Send + Clone + 'static,
        FIN: Fn(&A) -> OUT + Send + Clone + 'static,
    {
        id: u32,
        zero: OUT,
        acc: ACC,
        fin: FIN,
        _a: PhantomData<K>,
        _b: PhantomData<A>,
    }

    impl<K, A, OUT, ACC, FIN> AggDef<K, A, OUT, ACC, FIN>
    where
        K: Eq + std::hash::Hash + Clone + 'static,
        A: Send + Clone + 'static,
        OUT: Send + Clone + 'static,
        ACC: Fn(&mut A, A) + Send + Clone + 'static,
        FIN: Fn(&A) -> OUT + Send + Clone + 'static,
    {
        fn new(id: u32, zero: OUT, acc: ACC, fin: FIN) -> Self {
            Self {
                id,
                zero,
                acc,
                fin,
                _a: PhantomData,
                _b: PhantomData,
            }
        }
    }

    mod agg_def {
        use num_traits::Bounded;

        use super::AggDef;

        fn num<K, A, F>(id: u32, zero: A, acc: F) -> AggDef<K, A, A, F, fn(&A) -> A>
        where
            K: Eq + std::hash::Hash + Clone + 'static,
            A: Send + Clone + 'static,
            F: Fn(&mut A, A) + Send + Clone + 'static,
        {
            AggDef::new(id, zero, acc, |a| a.clone())
        }

        pub(crate) fn min<K, A>(id: u32) -> AggDef<K, A, A, fn(&mut A, A) -> (), fn(&A) -> A>
        where
            K: Eq + std::hash::Hash + Clone + 'static,
            A: Send + Clone + 'static,
            A: Bounded + PartialOrd + PartialEq + Ord + PartialOrd + Copy,
        {
            AggDef::new(id, A::max_value(), |a, b| *a = A::min(*a, b), |a| a.clone())
        }

        pub(crate) fn sum<K, A>(id: u32) -> AggDef<K, A, A, fn(&mut A, A) -> (), fn(&A) -> A>
        where
            K: Eq + std::hash::Hash + Clone + 'static,
            A: Send + Clone + 'static + num_traits::Zero + std::ops::AddAssign,
        {
            AggDef::new(id, A::zero(), |a, b| *a += b, |a| a.clone())
        }

        fn max<K, A>(id: u32) -> AggDef<K, A, A, fn(&mut A, A) -> (), fn(&A) -> A>
        where
            K: Eq + std::hash::Hash + Clone + 'static,
            A: Send + Clone + 'static,
            A: Bounded + PartialOrd + PartialEq + Ord + PartialOrd + Copy,
        {
            AggDef::new(id, A::min_value(), |a, b| *a = A::max(*a, b), |a| a.clone())
        }

        // fn avg<K, A>() -> AggDef<K, (A, usize), A, fn(&mut A, A) -> (), fn(&(A, usize)) -> A>
        // where
        //     K: Eq + std::hash::Hash + Clone + 'static,
        //     A: Send + Clone + 'static,
        //     A: Bounded + PartialOrd + PartialEq + Ord + PartialOrd + Copy,
        // {
        //     AggDef::new(A::min_value(), |a, b| *a = A::max(*a, b), |a| a.clone())
        // }
    }

    // state local to the shard
    // this is the state state
    struct ComputeState {
        left: Box<dyn MutableArray + Send + Sync + 'static>,
        right: Box<dyn MutableArray + Send + Sync + 'static>,
    }

    struct PartComputeState {
        parts: FxHashMap<usize, ComputeState>,
    }
    impl PartComputeState {
        fn new() -> Self {
            Self {
                parts: FxHashMap::default(),
            }
        }

        fn into_iter(self) -> hash_map::IntoIter<usize, ComputeState> {
            self.parts.into_iter()
        }

        fn append_null_if_empty(&mut self, ss: usize, i: usize, part_id: usize) {
            let comp_state = self
                .parts
                .entry(part_id)
                .or_insert_with(|| ComputeState::new_mutable_primitive::<i32>());
            comp_state.append_null_if_empty(ss, i);
        }

        fn set<A: NativeType>(&mut self, part_id: usize, ss: usize, i: usize, val: A) {
            let comp_state = self
                .parts
                .entry(part_id)
                .or_insert_with(|| ComputeState::new_mutable_primitive::<A>());
            comp_state.set(ss, i, Some(val));
        }

        fn agg<A: NativeType, ACC>(
            &mut self,
            part_id: usize,
            ss: usize,
            a: A,
            ki: usize,
            zero: A,
            acc: ACC,
        ) where
            ACC: Fn(&mut A, A),
        {
            let cs = self.parts
                .entry(part_id)
                .or_insert_with(||{
                    let mut cs = ComputeState::new_mutable_primitive::<A>();
                    cs.set(ss, ki, Some(zero));
                    cs
                });
            cs.agg(ss,a , ki, zero, acc);
        }
    }

    impl ComputeState {
        fn new_mutable_primitive<T>() -> Self
        where
            T: NativeType + Send + Sync + 'static,
        {
            Self {
                left: Box::new(MutablePrimitiveArray::<T>::new()),
                right: Box::new(MutablePrimitiveArray::<T>::new()),
            }
        }

        fn current_mut(&mut self, ss: usize) -> &mut dyn MutableArray {
            if ss % 2 == 0 {
                self.left.as_mut()
            } else {
                self.right.as_mut()
            }
        }

        fn current(&self, ss: usize) -> &dyn MutableArray {
            if ss % 2 == 0 {
                self.left.as_ref()
            } else {
                self.right.as_ref()
            }
        }

        fn finalize<F, A, OUT>(&self, ss: usize, fin: F) -> PrimitiveArray<OUT>
        where
            F: Fn(&A) -> OUT,
            A: NativeType,
            OUT: NativeType,
        {
            let arr = self
                .current(ss)
                .as_any()
                .downcast_ref::<MutablePrimitiveArray<A>>()
                .unwrap();

            let arr_out: PrimitiveArray<OUT> = arr.iter().map(|v| v.map(|v| fin(v))).collect();
            arr_out
        }

        fn into_inner(self, ss: usize) -> Box<dyn MutableArray + Send> {
            if ss % 2 == 0 {
                self.left
            } else {
                self.right
            }
        }

        fn set<A: NativeType>(&mut self, ss: usize, i: usize, a: Option<A>) {
            let arr = self
                .current_mut(ss)
                .as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<A>>()
                .unwrap();
            // if i is smaller than len, we are updating
            // else we're appending
            if i < arr.len() {
                arr.set(i, a)
            } else {
                arr.push(a)
            }
        }

        fn append_null_if_empty(&mut self, ss: usize, i: usize) {
            if !self.current(ss).is_valid(i) && i < self.current(ss).len() {
                self.current_mut(ss).push_null()
            }
        }

        fn agg<A: NativeType, ACC>(&mut self, ss: usize, a: A, ki: usize, zero: A, acc: ACC)
        where
            ACC: Fn(&mut A, A),
        {
            let mut_arr = self
                .current_mut(ss)
                .as_mut_any()
                .downcast_mut::<MutablePrimitiveArray<A>>()
                .unwrap();

            println!("pushing val:{a} index:{ki} super_step:{ss} {:?}", mut_arr);

            if ki == mut_arr.len() {
                mut_arr.push(Some(zero))
            }

            let mut_arr_slice = mut_arr.values_mut_slice();

            acc(&mut mut_arr_slice[ki], a)
        }
    }

    type AccId = u32;
    type PartId = usize;

    struct ShardComputeState<K> {
        n_parts: usize,                             // number of partitions
        index: Vec<usize>,                          // sorted index for all the vertices
        ks: Vec<K>,                                 // the keys for the accumulators
        states: FxHashMap<AccId, PartComputeState>, // any accumulators we need to compute
    }

    impl<K: std::hash::Hash> ShardComputeState<K> {
        fn new(n_parts: usize) -> Self {
            Self {
                n_parts,
                index: vec![],
                states: FxHashMap::default(),
                ks: vec![],
            }
        }

        fn finalize<A, OUT, ACC, FIN>(
            &mut self,
            ss: usize,
            agg_def: &AggDef<K, A, OUT, ACC, FIN>,
        ) -> Option<HashMap<usize, PrimitiveArray<OUT>>>
        where
            K: Eq + std::hash::Hash + Clone + Ord + 'static,
            A: Send + Clone + NativeType + 'static,
            ACC: Fn(&mut A, A) + Send + Clone + 'static,
            FIN: Fn(&A) -> OUT + Send + Clone + 'static,
            OUT: Send + Clone + NativeType,
        {
            // finalize the accumulator
            let state = self.states.remove(&agg_def.id)?;
            // run the finalizer for
            Some(
                state
                    .into_iter()
                    .map(|(k, v)| {
                        let arr = v.finalize(ss, agg_def.fin.clone());
                        (k, arr)
                    })
                    .collect(),
            )
        }

        // fn finalize_direct<A, ACC, FIN>(
        //     &mut self,
        //     ss: usize,
        //     agg_def: &AggDef<K, A, A, ACC, FIN>,
        // ) -> Option<PrimitiveArray<A>>
        // where
        //     K: Eq + std::hash::Hash + Clone + Ord + 'static,
        //     A: Send + Clone + NativeType + 'static,
        //     ACC: Fn(&mut A, A) + Send + Clone + 'static,
        //     FIN: Fn(&A) -> A + Send + Clone + 'static,
        // {
        //     // finalize the accumulator
        //     let state = self.states.remove(&agg_def.id)?;
        //     Some(state.finalize(ss, agg_def.fin.clone()))
        // }

        fn accumulate_into<A, ACC, FIN>(
            &mut self,
            ss: usize,
            into: &K,
            a: A,
            agg_def: &AggDef<K, A, A, ACC, FIN>,
        ) where
            K: Eq + std::hash::Hash + Clone + Ord + 'static,
            A: Send + Clone + NativeType + 'static,
            ACC: Fn(&mut A, A) + Send + Clone + 'static,
            FIN: Fn(&A) -> A + Send + Clone + 'static,
        {
            // find if K is already in the vec

            // FIXME: this has issues if we add several aggregates after we added keys
            // so we need to add all aggregates per vertex then add keys
            match self.index.binary_search_by(|i| (&self.ks[*i]).cmp(into)) {
                Err(i) => {
                    // insert into the vec
                    let key_id = self.ks.len();
                    self.index.insert(i, key_id);
                    self.ks.push(into.clone());

                    // figure out the partition id
                    let part_id = get_shard_id_from_global_vid(key_id, self.n_parts);

                    // go through all the other states an append null on the aggregator values if they are not already ocupied (it shouldn't be)
                    for (id, cs) in self.states.iter_mut() {
                        if *id != agg_def.id {
                            cs.append_null_if_empty(ss, i, part_id);
                        }
                    }
                    // get the aggregator and apply the accum function onto the current value o zero
                    let agg_state = self.states.entry(agg_def.id).or_insert_with(|| {
                        let mut part = PartComputeState::new();
                        part.set(part_id, ss, key_id, agg_def.zero.clone());
                        part
                    });
                    // accumulate the new value into agg_state
                    agg_state.agg(part_id, ss, a, key_id, agg_def.zero, agg_def.acc.clone())
                }
                Ok(i) => {
                    let key_id = self.index[i];
                    // figure out the partition id
                    let part_id = get_shard_id_from_global_vid(key_id, self.n_parts);
                    let agg_state = self.states.get_mut(&agg_def.id).unwrap();
                    agg_state.agg(part_id, ss, a, key_id, agg_def.zero, agg_def.acc.clone())
                }
            }
        }
    }

    #[test]
    fn min_aggregates_for_3_keys_one_part() {
        let min = agg_def::min(0);

        let mut state: ShardComputeState<u32> = ShardComputeState::new(1);

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec = vec![];
        let mut actual_min = i32::MAX;
        for _ in 0..100 {
            let i = rng.gen_range(0..100);
            actual_min = actual_min.min(i);
            vec.push(i);
        }

        for a in vec {
            state.accumulate_into(0, &1, a, &min);
            state.accumulate_into(0, &2, a, &min);
            state.accumulate_into(0, &3, a, &min);
        }

        let actual = state.finalize(0, &min).unwrap().remove(&0);

        assert_eq!(
            actual,
            Some(
                vec![Some(actual_min), Some(actual_min), Some(actual_min)]
                    .into_iter()
                    .collect()
            )
        );
    }

    #[test]
    fn sum_aggregates_for_3_keys_one_part() {
        let sum = agg_def::sum(0);

        let mut state: ShardComputeState<u32> = ShardComputeState::new(1);

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec = vec![];
        let mut actual_sum = 0;
        for _ in 0..100 {
            let i = rng.gen_range(0..100);
            actual_sum += i;
            vec.push(i);
        }

        for a in vec {
            state.accumulate_into(0, &1, a, &sum);
            state.accumulate_into(0, &2, a, &sum);
            state.accumulate_into(0, &3, a, &sum);
        }

        let actual = state.finalize(0, &sum).unwrap().remove(&0);

        assert_eq!(
            actual,
            Some(
                vec![Some(actual_sum), Some(actual_sum), Some(actual_sum)]
                    .into_iter()
                    .collect()
            )
        );
    }

    #[test]
    fn sum_aggregates_for_3_keys_2_part() {
        let sum = agg_def::sum(0);

        let mut state: ShardComputeState<u32> = ShardComputeState::new(2);

        // create random vec of numbers
        let mut rng = rand::thread_rng();
        let mut vec = vec![];
        let mut actual_sum = 0;
        for _ in 0..100 {
            let i = rng.gen_range(0..100);
            actual_sum += i;
            vec.push(i);
        }

        for a in vec {
            state.accumulate_into(0, &1, a, &sum);
            state.accumulate_into(0, &2, a, &sum);
            state.accumulate_into(0, &3, a, &sum);
        }

        let actual = state.finalize(0, &sum).unwrap().remove(&0);

        assert_eq!(
            actual,
            Some(
                vec![Some(actual_sum), Some(actual_sum), Some(actual_sum)]
                    .into_iter()
                    .collect()
            )
        );
    }

}
