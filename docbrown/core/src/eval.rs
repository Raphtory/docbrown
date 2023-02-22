use std::{
    collections::{hash_map::Entry, HashMap, HashSet},
    hash::Hash,
    ops::{AddAssign, Deref, Index, IndexMut, Range},
    option::Option,
};

use dashmap::mapref::one::{Ref, RefMut};

use crate::{
    graph::{TemporalGraph, VertexView},
    Direction,
};

pub(crate) trait Eval {
    fn eval<A, MAP, ACCF>(
        &self,
        window: Range<i64>,
        // prev_state: &mut HashMap<u64, A>,
        f: MAP,
        accf: ACCF,
    ) where
        MAP: Fn(EvalVertexView<'_, Self>) -> Vec<Acc<u64, A>>,
        ACCF: Fn(&mut A, A),
        A: Send + Default;
    // K: Eq + std::hash::Hash;
}

struct Accumulator<K, A, F> {
    state: dashmap::DashMap<K, A>,
    acc: F,
}

impl<K, A, F> Accumulator<K, A, F>
where
    F: Fn(&mut A, A) + Clone,
    K: std::hash::Hash + std::cmp::Eq,
{
    fn new(acc: F) -> Self {
        Self {
            state: dashmap::DashMap::new(),
            acc,
        }
    }

    fn read<'a>(&'a self, k: &K) -> Option<AccumulatorEntry<'a, K, A>> {
        self.state.get(k).map(|r| AccumulatorEntry(r))
    }

    fn write<'a>(&'a self, k: &K) -> Option<MutAccumulatorEntry<'a, K, A, F>> {
        self.state
            .get_mut(&k)
            .map(|r| MutAccumulatorEntry(r, self.acc.clone()))
    }
}

struct AccumulatorEntry<'a, K, A>(Ref<'a, K, A>);
struct MutAccumulatorEntry<'a, K, A, F>(RefMut<'a, K, A>, F);

impl<'a, K, A, F> AddAssign<A> for MutAccumulatorEntry<'a, K, A, F>
where
    F: Fn(&mut A, A),
    K: std::hash::Hash + std::cmp::Eq,
{
    fn add_assign(&mut self, rhs: A) {
        self.1(&mut self.0, rhs);
    }
}

impl Eval for TemporalGraph {
    fn eval<A, MAP, ACCF>(
        &self,
        window: Range<i64>,
        // prev_state: &mut HashMap<u64, A>,
        f: MAP,
        accf: ACCF,
    ) where
        MAP: Fn(EvalVertexView<'_, Self>) -> Vec<Acc<u64, A>>,
        ACCF: Fn(&mut A, A),
        A: Send + Default,
        // K: Eq + std::hash::Hash,
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
            // accumulate the results
            // remove the vertex id from active_set if it is not needed anymore
            for v_view in iter {
                let pid = v_view.pid;
                let eval_v_view = EvalVertexView { vv: v_view };
                let accs = f(eval_v_view);
                // for acc in accs {
                //     match acc {
                //         Acc::Done(_) => {
                //             // done with this vertex
                //             if !next_active_set.contains(&pid) {
                //                 next_active_set.remove(&pid);
                //             }
                //         }
                //         Acc::Value(k, v) => {
                //             // update the accumulator
                //             prev_state
                //                 .entry(k)
                //                 .and_modify(|prev| accf(prev, &v))
                //                 .or_insert_with(|| v);

                //             // done with this vertex
                //             if !next_active_set.contains(&pid) {
                //                 next_active_set.remove(&pid);
                //             }
                //         }
                //         Acc::Keep(_) => {
                //             next_active_set.insert(pid);
                //         }
                //         Acc::ValueKeep(k, v) => {
                //             // update the accumulator
                //             prev_state
                //                 .entry(k)
                //                 .and_modify(|prev| accf(prev, &v))
                //                 .or_insert_with(|| v);

                //             next_active_set.insert(pid);
                //         }
                //     }
                // }
            }

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
    fn get<A, F>(&self, acc: &'a Accumulator<u64, A, F>) -> Option<AccumulatorEntry<'a, u64, A>>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        acc.read(&id)
    }

    fn get_mut<A, F>(
        &self,
        acc: &'a Accumulator<u64, A, F>,
    ) -> Option<MutAccumulatorEntry<'a, u64, A, F>>
    where
        F: Fn(&mut A, A) + Clone,
    {
        let id = self.vv.global_id();
        acc.write(&id)
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
        eval::{Acc, Accumulator, Eval},
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
            // &mut state,
            move |vertex| {
                let gid = vertex.vv.global_id();

                let mut bla = vertex.get_mut(&min_cc_id).unwrap();
                bla += gid;
                vec![Acc::Value(vertex.vv.global_id(), vertex.vv.global_id())]
            },
            |c1, c2| {
                *c1 = u64::min(*c1, c2); // not really needed but for completeness
            },
        );

        // assert_eq!(
        //     state,
        //     HashMap::from_iter(vec![(1, 1), (2, 2), (3, 3), (4, 4)])
        // );

        // second step where we check the state of our neighbours and set ourselves to the min
    }
}
