use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    ops::{AddAssign, Range},
    option::Option,
    rc::Rc,
};

use crate::{
    graph::{TemporalGraph, VertexView},
    Direction,
};

pub(crate) trait Eval {
    fn eval<'a, FMAP, PRED>(
        &'a self,
        c: Option<Context>,
        window: Range<i64>,
        f: FMAP,
        having: PRED,
    ) -> Context
    where
        FMAP: Fn(&mut EvalVertexView<'a, Self>, &Context) -> Vec<VertexRef>,
        PRED: Fn(&EvalVertexView<'a, Self>, &Context) -> bool,
        Self: Sized + 'a;
}

pub(crate) struct VertexRef(usize);

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
#[derive(Clone)]
pub(crate) struct Monoid<K, A, F> {
    pub(crate) id: A,
    pub(crate) bin_op: F,
    pub(crate) state: Rc<RefCell<HashMap<K, PairAcc<A>>>>,
}

impl<K, A, F> Monoid<K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    A: Clone,
{
    pub(crate) fn new(name: String, id: A, bin_op: F) -> Self {
        Self { id, bin_op, state: Rc::new(RefCell::new(HashMap::new())) }
    }
}

pub(crate) struct Context {
 ss: u64 // the superstep decides which state do we use from the accuumulators, we start at 0 and we flip flop between odd and even
}

impl Context {
    fn new() -> Self {
        Self {
            ss: 0
        }
    }

    fn inc(&mut self) {
        self.ss += 1;
    }

    fn acc<'a, 'b, A, F>(
        &'a self,
        monoid: &'b Monoid<u64, A, F>,
    ) -> Accumulator<u64, A, F>
    where
        A: Clone,
        F: Fn(&mut A, A) + Clone,
    {
        Accumulator::new(monoid, self.ss)
    }

    fn as_hash_map<A: Clone, F>(&self, m: &Monoid<u64, A, F>) -> Option<HashMap<u64, PairAcc<A>>>{
        m.state.try_borrow().ok().map(|s| s.clone())
    }
}

// this always updates the current value so get_mut returns mutable reference to current
// when calling update_prev it will copy the current value to prev
// prev is NEVER updated only copied from current
// TODO we should be to copy current into prev then reset
// A has to be Clone
#[derive(Clone, PartialEq, Debug)]
pub(crate) struct PairAcc<A> {
    even: Option<A>,
    odd: Option<A>,
}

impl<A> PairAcc<A> {
    fn new(current: A, ss: u64) -> Self {
        let (odd, even) = if ss % 2 == 0 {
            (Some(current), None)
        } else {
            (None, Some(current))
        };
        Self {
            even, odd
        }
    }

    fn new_with_prev(current: A, prev: A, ss: u64) -> Self {
        let (odd, even) = if ss % 2 == 0 {
            (Some(current), Some(prev))
        } else {
            (Some(prev), Some(current))
        };
        Self {
            even, odd
        }
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

impl <A:Clone> PairAcc<A> {

    fn copy_from_prev(&mut self, ss: u64) {
        if ss % 2 == 0 {
            self.even = self.odd.clone();
        } else {
            self.odd = self.even.clone();
        }
    }
}

#[derive(Clone)]
struct Accumulator<K: std::cmp::Eq + std::hash::Hash, A, F> {
    state: Rc<RefCell<HashMap<K, PairAcc<A>>>>,
    acc: Monoid<K, A, F>,
    ss: u64
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
            ss
        }
    }

    fn accumulate(&self, id: K, value: A) {
        let mut state = self.state.borrow_mut();
        state
            .entry(id)
            .and_modify(|e| {
                let acc = e.as_mut(self.ss);

                if let Some(acc) = acc {
                    (self.acc.bin_op)(acc, value.clone());
                } else {
                    // clone the previous step into current
                    e.copy_from_prev(self.ss);
                    let acc2 = e.as_mut(self.ss).get_or_insert(self.acc.id.clone());
                    (self.acc.bin_op)(acc2, value.clone());

                }
            })
            .or_insert_with(|| PairAcc::new(value, self.ss));
    }

    fn read_prev(&self, k: &K) -> Option<A> {
        self.state.borrow().get(k).and_then(|e| e.prev(self.ss).cloned())
    }

    fn read(&self, k: &K) -> Option<A> {
        self.state.borrow().get(k).and_then(|e| e.current(self.ss).cloned())
    }
}

struct AccEntry<'a, K: std::cmp::Eq + std::hash::Hash, A, F> {
    parent: &'a Accumulator<K, A, F>,
    k: K,
}

// impl of AccEntry contains 2 functions
// read to extract a clone of the value A
// read_ref to extract a reference to the value A

impl<'a, K, A, F> AccEntry<'a, K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
{
    fn read(&self) -> Option<A> {
        self.parent.read(&self.k)
    }
}

// new method for AccEntry
// this will create a new AccEntry

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

trait Filp {
    fn flip(&self);
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
        FMAP: Fn(&mut EvalVertexView<'a, Self>, &Context) -> Vec<VertexRef>,
        PRED: Fn(&EvalVertexView<'a, Self>, &Context) -> bool,
        Self: Sized + 'a,
    {

        // we start with all the vertices considered inside the working set
        let mut cur_active_set: WorkingSet<usize> = WorkingSet::All;
        let mut next_active_set = HashSet::new();
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
                let next_vertices = f(&mut eval_v_view, &ctx);
                for next_vertex in next_vertices {
                    next_active_set.insert(next_vertex.pid());
                }
            }

            // from the next_active_set we apply the PRED
            next_active_set.retain(|pid| {
                let g_id = self.adj_lists[*pid].logical();
                let v_view = VertexView::new(self, *g_id, *pid, Some(window.clone()));
                having(&EvalVertexView { vv: v_view }, &ctx)
            });

            println!("next_active_set: {:?}", next_active_set);

            cur_active_set = WorkingSet::Set(next_active_set);
            next_active_set = HashSet::new();
            ctx.inc();
        }
        ctx
    }
}

// view over the vertex
// this includes the state during the evaluation
pub(crate) struct EvalVertexView<'a, G> {
    vv: VertexView<'a, G>,
}

// here we implement the Fn trait for the EvalVertexView to return Option<AccumulatorEntry>

struct PrevAcc<'a, A: Clone, F> {
    id: u64,
    acc: &'a Accumulator<u64, A, F>,
    value: Option<A>,
}

impl<'a, A: Clone, F> PrevAcc<'a, A, F> {
    fn value(&self) -> Option<A> {
        self.value.clone()
    }
}


impl<'a> EvalVertexView<'a, TemporalGraph> {
    fn get<'b, A: Clone, F>(&self, acc: &'b Accumulator<u64, A, F>) -> AccEntry<'b, u64, A, F>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        AccEntry::new(acc, id)
    }

    fn get_prev<A: Clone, F>(&self, acc: &'a Accumulator<u64, A, F>) -> Option<A>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        acc.read_prev(&id)
    }

    fn get_prev_acc<A: Clone, F>(&self, acc: &'a Accumulator<u64, A, F>) -> PrevAcc<'a, A, F>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        PrevAcc {
            id,
            acc,
            value: acc.read_prev(&id),
        }
    }

    fn as_vertex_ref(&self) -> VertexRef {
        VertexRef(self.vv.pid)
    }
}

impl<'a> EvalVertexView<'a, TemporalGraph> {
    fn neighbours(
        &'a self,
        d: Direction,
    ) -> impl Iterator<Item = EvalVertexView<'a, TemporalGraph>> {
        self.vv.neighbours(d).map(move |vv| EvalVertexView { vv })
    }
}

enum WorkingSet<A> {
    All,
    Set(HashSet<A>),
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

// there are 4 possible states for
// an accumulated value out of the
// mapping function
// 0 no value and remove vertex from the next working set
// 1 no value and keep vertex in the next working set
// 2 value and remove vertex from the next working set
// 3 value and keep vertex in the next working set
pub(crate) enum Acc<K, A> {
    Done(K),         // 0
    Keep(K),         // 1
    Value(K, A),     // 2
    ValueKeep(K, A), // 3
}

#[cfg(test)]
mod eval_test {
    use std::collections::HashMap;

    use crate::{
        eval::{Eval, Monoid, PairAcc},
        graph::TemporalGraph,
    };

    #[test]
    fn eval_2_connected_components_same_time() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 1);
        g.add_vertex(2, 1);
        g.add_vertex(3, 1);
        g.add_vertex(4, 1);

        g.add_edge(1, 2, 1);
        g.add_edge(3, 4, 1);

        let min = Monoid::new("min_cc".to_string(), u64::MAX, |a: &mut u64, b: u64| {
            *a = u64::min(*a, b)
        });

        // initial step where we init every vertex to it's own ID
        let state = g.eval(
            None,
            0..3,
            |vertex, ctx| {
                let min_cc_id = ctx.acc(&min); // get the accumulator for the min_cc_id

                let gid = vertex.vv.global_id();

                let mut min_acc = vertex.get(&min_cc_id); // get the entry for this vertex in the min_cc_id accumulator
                min_acc += gid; // set the value to the global id of the vertex

                vec![] // nothing to propagate at this step
            },
            // insert exchange step
            |_, _| false,
        );

        let actual = state.as_hash_map(&min).unwrap();

        assert_eq!(
            actual,
            HashMap::from_iter(vec![
                (1, PairAcc::new(1, 0)),
                (2, PairAcc::new(2, 0)),
                (3, PairAcc::new(3, 0)),
                (4, PairAcc::new(4, 0))
            ])
        );

        // second step where we check the state of our neighbours and set ourselves to the min
        // we stop when the state of the vertex does not change
        let state = g.eval(
            Some(state),
            0..3,
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

        let state = state.as_hash_map(&min).unwrap();

        assert_eq!(
            state,
            HashMap::from_iter(vec![
                (1, PairAcc::new_with_prev(1, 1, 2)),
                (2, PairAcc::new_with_prev(1, 1, 2)),
                (3, PairAcc::new_with_prev(3, 3, 2)),
                (4, PairAcc::new_with_prev(3, 3, 2))
            ])
        );
    }
}
