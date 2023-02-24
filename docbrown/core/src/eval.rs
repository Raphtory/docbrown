use std::{
    cell::RefCell,
    collections::{HashMap, HashSet},
    ops::{AddAssign, Range},
    option::Option,
    rc::Rc, any::Any,
};

use crate::{
    graph::{TemporalGraph, VertexView},
    Direction,
};

pub(crate) trait Eval {
    fn eval<'a, FMAP, PRED>(&'a self, c: Option<Context>, window: Range<i64>, f: FMAP, having: PRED) -> Option<Context>
    where
        FMAP: Fn(&mut EvalVertexView<'a, Self>, &mut Context) -> Vec<VertexRef>,
        PRED: Fn(&EvalVertexView<'a, Self>, &mut Context) -> bool,
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
pub(crate) struct Monoid<A, F> {
    pub(crate) id: A,
    pub(crate) bin_op: F,
    pub(crate) name: String,
}

impl<A, F> Monoid<A, F>
where
    F: Fn(&mut A, A) + Clone,
    A: Clone,
{
    pub(crate) fn new(name: String, id: A, bin_op: F) -> Self {
        Self { id, bin_op, name}
    }
}

struct Accum<K, A, F> {
    storage: Rc<RefCell<HashMap<K, PairAcc<A>>>>,
    monoid: Monoid<A, F>,
}

struct Context {
    state: HashMap<String, Box<dyn Any>>
}

impl Context {
    fn new() -> Self {
        Self {state: HashMap::new()}
    }

    fn acc<K, A: Clone, F: Clone>(&mut self, monoid: &Monoid<A, F>) -> Accum<K, A, F> {

        let accum_mut = self.state.entry(monoid.name.clone()).or_insert_with(|| {
            let storage = Rc::new(RefCell::new(HashMap::<K, PairAcc<A>>::new()));
            let monoid = monoid.clone();
            Box::new(Accum { storage, monoid })
        }).downcast_mut::<Accum<K, A, F>>().unwrap();

        Accum {
            storage: accum_mut.storage.clone(),
            monoid: monoid.clone(),
        }
    }
}

struct AccEntry<K: Eq + std::hash::Hash, A, F> {
    parent: Rc<Accumulator<K, A, F>>,
    k: K,
}

// this always updates the current value so get_mut returns mutable reference to current
// when calling update_prev it will copy the current value to prev
// prev is NEVER updated only copied from current
// TODO we should be to copy current into prev then reset
// A has to be Clone
#[derive(Clone, PartialEq, Debug)]
struct PairAcc<A> {
    current: A,
    prev: Option<A>,
}

impl<A> PairAcc<A> {
    fn new(current: A) -> Self {
        Self {
            current,
            prev: None,
        }
    }

    fn new_with_prev(current: A, prev: A) -> Self {
        Self {
            current,
            prev: Some(prev),
        }
    }
}

impl<A> PairAcc<A>
where
    A: Clone,
{
    fn update_prev(&mut self) {
        self.prev = Some(self.current.clone());
    }

    fn clone_prev(&self) -> Option<A> {
        self.prev.clone()
    }
}

impl<A> PairAcc<A> {
    fn as_mut(&mut self) -> &mut A {
        &mut self.current
    }
}

struct Accumulator<K: std::cmp::Eq + std::hash::Hash, A, F> {
    state: dashmap::DashMap<K, PairAcc<A>>,
    acc: F,
}

impl<K: std::cmp::Eq + std::hash::Hash, A: std::clone::Clone, F> Accumulator<K, A, F> {
    fn flip(&self, id: K) {
        self.state
            .get_mut(&id)
            .map(|mut e| e.value_mut().update_prev());
    }
}

impl<K, A, F> Accumulator<K, A, F>
where
    F: Fn(&mut A, A),
    K: std::hash::Hash + std::cmp::Eq,
    A: Clone,
{
    fn new(acc: F) -> Rc<Self> {
        Rc::new(Self {
            state: dashmap::DashMap::new(),
            acc,
        })
    }

    fn accumulate(&self, id: K, value: A) {
        self.state
            .entry(id)
            .and_modify(|e| {
                (self.acc)(e.as_mut(), value.clone());
            })
            .or_insert_with(|| PairAcc::new(value));
    }

    fn prev_value(&self, k: &K) -> Option<A> {
        self.state.get(&k).and_then(|e| e.value().clone_prev())
    }

    fn read(&self, k: &K) -> Option<A> {
        self.state.get(&k).map(|e| e.value().current.clone())
    }
}

impl<K, A, F> Accumulator<K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
{
    fn as_hash_map(&self) -> HashMap<K, PairAcc<A>> {
        self.state
            .iter()
            .map(|e| (e.key().clone(), e.value().clone()))
            .collect()
    }
}

// impl of AccEntry contains 2 functions
// read to extract a clone of the value A
// read_ref to extract a reference to the value A

impl<K, A, F> AccEntry<K, A, F>
where
    F: Fn(&mut A, A),
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
{
    fn read(&self) -> Option<A> {
        self.parent.read(&self.k)
    }
}

// new method for AccEntry
// this will create a new AccEntry

impl<K, A, F> AccEntry<K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
{
    fn new(parent: Rc<Accumulator<K, A, F>>, k: K) -> Self {
        Self { parent, k }
    }
}

impl<K, A, F> AddAssign<A> for AccEntry<K, A, F>
where
    F: Fn(&mut A, A),
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
{
    fn add_assign(&mut self, rhs: A) {
        self.parent.accumulate(self.k.clone(), rhs);
    }
}

impl<K, A, F> AddAssign<Option<A>> for AccEntry<K, A, F>
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

impl<K, A, F> AddAssign<AccEntry<K, A, F>> for AccEntry<K, A, F>
where
    F: Fn(&mut A, A),
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
    Self: AddAssign<Option<A>>,
{
    fn add_assign(&mut self, rhs: AccEntry<K, A, F>) {
        self.add_assign(rhs.read());
    }
}

impl Eval for TemporalGraph {
    fn eval<'a, MAP, PRED>(&'a self, c: Option<Context>, window: Range<i64>, f: MAP, having: PRED) -> Option<Context>
    where
        MAP: Fn(&mut EvalVertexView<'a, Self>, &Context) -> Vec<VertexRef>,
        PRED: Fn(&EvalVertexView<'a, Self>, &Context) -> bool,
        Self: Sized + 'a,
    {
        // we start with all the vertices considered inside the working set
        let mut cur_active_set: WorkingSet<usize> = WorkingSet::All;
        let mut next_active_set = HashSet::new();
        let ctx = c.unwrap_or_else(|| Context::new());

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
        }
        Some(ctx)
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

// when dropped the PrevAcc will trigger a flip (meaning: the current value will become the previous value)
impl<'a, A: Clone, F> Drop for PrevAcc<'a, A, F> {
    fn drop(&mut self) {
        self.acc.flip(self.id);
    }
}

impl<'a> EvalVertexView<'a, TemporalGraph> {
    fn get_mut<A: Clone, F>(&mut self, acc: Rc<Accumulator<u64, A, F>>) -> AccEntry<u64, A, F>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        AccEntry::new(acc, id)
    }

    fn get<A: Clone, F>(&self, acc: &'a Accumulator<u64, A, F>) -> Option<A>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        acc.read(&id)
    }

    fn get_prev<A: Clone, F>(&self, acc: &'a Accumulator<u64, A, F>) -> Option<A>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        acc.prev_value(&id)
    }

    fn get_prev_acc<A: Clone, F>(&self, acc: &'a Accumulator<u64, A, F>) -> PrevAcc<'a, A, F>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        PrevAcc {
            id,
            acc,
            value: acc.prev_value(&id),
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

    fn remove(&mut self, a: A) {
        match self {
            WorkingSet::All => {}
            WorkingSet::Set(s) => {
                s.remove(&a);
            }
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

    fn insert(&mut self, a: A) {
        match self {
            WorkingSet::All => {
                *self = WorkingSet::Set(HashSet::new());
                self.insert(a)
            }
            WorkingSet::Set(s) => {
                s.insert(a);
            }
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
        eval::{Eval, PairAcc, Monoid},
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

        let min_cc_id = Monoid::new("min_cc".to_string() ,u64::MAX, |a: &mut u64, b: u64| {*a = u64::min(*a, b)});

        // initial step where we init every vertex to it's own ID
        let state = g.eval(
            None,
            0..3,
            |vertex, ctx| {
                let acc = ctx.acc(&min_cc_id);

                let gid = vertex.vv.global_id();

                let mut min_acc = vertex.get_mut(&acc);
                min_acc += gid;
                vec![] // nothing to propagate at this step
            },
            // insert exchange step
            |_, _| false,
        );

        let actual = state.as_hash_map(&min_cc_id);

        assert_eq!(
            actual,
            &HashMap::from_iter(vec![
                (1, PairAcc::new(1)),
                (2, PairAcc::new(2)),
                (3, PairAcc::new(3)),
                (4, PairAcc::new(4))
            ])
        );

        // // second step where we check the state of our neighbours and set ourselves to the min
        // // we stop when the state of the vertex does not change
        // g.eval(
        //     0..3,
        //     |vertex| {
        //         let mut out = vec![];
        //         for mut neighbour in vertex.neighbours(crate::Direction::BOTH) {
        //             let mut n_min_acc = neighbour.get_mut(min_cc_id.clone());

        //             n_min_acc += vertex.get(&min_cc_id);

        //             out.push(neighbour.as_vertex_ref());
        //         }

        //         out
        //     },
        //     |v| {
        //         let min_acc = v.get(&min_cc_id); // this line needs to only get an immutable reference otherwise it will block if it's callled before the one above
        //         let prev_min_acc = v.get_prev_acc(&min_cc_id).value();

        //         min_acc != prev_min_acc
        //     },
        // );

        // let state = &min_cc_id.as_hash_map();

        // assert_eq!(
        //     state,
        //     &HashMap::from_iter(vec![
        //         (1, PairAcc::new_with_prev(1, 1)),
        //         (2, PairAcc::new_with_prev(1, 1)),
        //         (3, PairAcc::new_with_prev(3, 3)),
        //         (4, PairAcc::new_with_prev(3, 3))
        //     ])
        // );
    }
}
