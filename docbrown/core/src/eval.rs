use std::{
    collections::{HashMap, HashSet},
    hash::Hash,
    ops::{AddAssign, Deref, Index, IndexMut, Range},
    option::Option,
    sync::Arc,
};

use dashmap::mapref::{
    entry::Entry,
    one::{Ref, RefMut},
};
use replace_with::replace_with_or_abort;

use crate::{
    graph::{TemporalGraph, VertexView},
    Direction,
};

pub(crate) trait Eval {
    fn eval<'a, FMAP, PRED>(&'a self, window: Range<i64>, f: FMAP, having: PRED)
    where
        FMAP: Fn(EvalVertexView<'a, Self>) -> Vec<VertexRef>,
        PRED: Fn(&EvalVertexView<'a, Self>) -> bool,
        Self: Sized + 'a;
}
pub(crate) struct VertexRef(usize);

impl VertexRef {
    fn pid(&self) -> usize {
        self.0
    }
}

enum AccEntry<'a, K, A, F> {
    MutAcc(RefMut<'a, K, PairAcc<A>>, F),
    EntryAcc(Entry<'a, K, PairAcc<A>>, F),
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

#[derive(Clone)]
struct Accumulator<K: std::cmp::Eq + std::hash::Hash, A, F> {
    state: dashmap::DashMap<K, PairAcc<A>>,
    acc: F,
}

impl<K, A, F> Accumulator<K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    K: std::hash::Hash + std::cmp::Eq,
    A: Clone,
{
    fn new(acc: F) -> Self {
        Self {
            state: dashmap::DashMap::new(),
            acc,
        }
    }

    fn entry<'a>(&'a self, k: K) -> AccEntry<'a, K, A, F> {
        AccEntry::EntryAcc(self.state.entry(k), self.acc.clone())
    }

    fn prev_value(&self, k: K) -> Option<A> {
        self.state.get(&k).and_then(|e| e.value().clone_prev())
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

impl<'a, K, A, F> AccEntry<'a, K, A, F>
where
    F: Fn(&mut A, A),
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
{
    fn read(&self) -> Option<A> {
        match self {
            AccEntry::MutAcc(ref entry, _) => Some(entry.value().current.clone()),
            AccEntry::EntryAcc(Entry::Occupied(e), _) => Some(e.get().current.clone()),
            _ => None,
        }
    }

    fn read_ref(&self) -> Option<&A> {
        match self {
            AccEntry::MutAcc(ref entry, _) => Some(&entry.value().current),
            AccEntry::EntryAcc(Entry::Occupied(e), _) => Some(&e.get().current),
            _ => None,
        }
    }
}

impl<'a, K, A, F> AddAssign<A> for AccEntry<'a, K, A, F>
where
    F: Fn(&mut A, A),
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
{
    fn add_assign(&mut self, rhs: A) {
        match self {
            AccEntry::MutAcc(ref mut entry, acc) => {
                acc(entry.as_mut(), rhs);
            }
            entry => replace_with_or_abort(entry, |_self| match _self {
                AccEntry::EntryAcc(e @ Entry::Occupied(_), acc) => {
                    let same_entry = e.and_modify(|prev| acc(prev.as_mut(), rhs.clone()));
                    AccEntry::EntryAcc(same_entry, acc)
                }
                AccEntry::EntryAcc(e @ Entry::Vacant(_), acc) => {
                    let same_entry = e.or_insert(PairAcc {
                        current: rhs,
                        prev: None,
                    });
                    AccEntry::MutAcc(same_entry, acc)
                }
                _ => unreachable!(),
            }),
        }
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
    F: Fn(&mut A, A),
    K: std::hash::Hash + std::cmp::Eq + Clone,
    A: Clone,
    Self: AddAssign<Option<A>>,
{
    fn add_assign(&mut self, rhs: AccEntry<'a, K, A, F>) {
        self.add_assign(rhs.read());
    }
}

impl Eval for TemporalGraph {
    fn eval<'a, MAP, PRED>(&'a self, window: Range<i64>, f: MAP, having: PRED)
    where
        MAP: Fn(EvalVertexView<'a, Self>) -> Vec<VertexRef>,
        PRED: Fn(&EvalVertexView<'a, Self>) -> bool,
        Self: Sized + 'a,
    {
        // we start with all the vertices considered inside the working set
        let mut cur_active_set: WorkingSet<usize> = WorkingSet::All;
        let mut next_active_set = HashSet::new();

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
                let eval_v_view = EvalVertexView { vv: v_view };
                let next_vertices = f(eval_v_view);
                for next_vertex in next_vertices {
                    next_active_set.insert(next_vertex.pid());
                }
            }

            // from the next_active_set we apply the PRED
            next_active_set.retain(|pid| {
                let g_id = self.adj_lists[*pid].logical();
                let v_view = VertexView::new(self, *g_id, *pid, Some(window.clone()));
                having(&EvalVertexView { vv: v_view })
            });

            cur_active_set = WorkingSet::Set(next_active_set);
            next_active_set = HashSet::new();
        }
    }
}

// view over the vertex
// this includes the state during the evaluation
pub(crate) struct EvalVertexView<'a, G> {
    vv: VertexView<'a, G>,
}

// here we implement the Fn trait for the EvalVertexView to return Option<AccumulatorEntry>

impl<'a> EvalVertexView<'a, TemporalGraph> {
    fn get<A: Clone, F>(&self, acc: &'a Accumulator<u64, A, F>) -> AccEntry<'a, u64, A, F>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        acc.entry(id)
    }

    fn get_prev<A: Clone, F>(&self, acc: &'a Accumulator<u64, A, F>) -> Option<A>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        acc.prev_value(id)
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
        eval::{Acc, Accumulator, Eval, PairAcc},
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

        let min_cc_id = Accumulator::new(|c1, c2| *c1 = u64::min(*c1, c2));
        // initial step where we init every vertex to it's own ID
        g.eval(
            0..3,
            |vertex| {
                let gid = vertex.vv.global_id();

                let mut min_acc = vertex.get(&min_cc_id);
                min_acc += gid;
                vec![] // nothing to propagate at this step
            },
            |_| false,
        );

        let state = &min_cc_id.as_hash_map();

        assert_eq!(
            state,
            &HashMap::from_iter(vec![
                (1, PairAcc::new(1)),
                (2, PairAcc::new(2)),
                (3, PairAcc::new(3)),
                (4, PairAcc::new(4))
            ])
        );

        // second step where we check the state of our neighbours and set ourselves to the min
        // we stop when the state of the vertex does not change
        g.eval(
            0..3,
            |vertex| {
                let mut out = vec![];
                for neighbour in vertex.neighbours(crate::Direction::OUT) {
                    let mut n_min_acc = neighbour.get(&min_cc_id);
                    let v_min_acc = vertex.get(&min_cc_id);
                    n_min_acc += v_min_acc;
                    out.push(neighbour.as_vertex_ref());
                }

                out
            },
            |v| {
                let min_acc = v.get(&min_cc_id);
                let prev_min_acc = v.get_prev(&min_cc_id);

                let value = min_acc.read();
                // value != prev_min_acc
                false
            },
        )
    }
}
