use std::{
    collections::{HashMap, HashSet},
    ops::Range,
};

use crate::graph::{TemporalGraph, VertexView};

pub(crate) trait Eval {
    fn eval<A, K, MAP, ACCF>(
        &self,
        window: Range<i64>,
        prev_state: &mut HashMap<K, A>,
        f: MAP,
        accf: ACCF,
    ) where
        MAP: Fn(VertexView<'_, Self>) -> Vec<Acc<K, A>>,
        ACCF: Fn(&mut A, &A),
        A: Send + Default,
        K: Eq + std::hash::Hash;
}

impl Eval for TemporalGraph {
    fn eval<A, K, MAP, ACCF>(
        &self,
        window: Range<i64>,
        prev_state: &mut HashMap<K, A>,
        f: MAP,
        accf: ACCF,
    ) where
        MAP: Fn(VertexView<'_, Self>) -> Vec<Acc<K, A>>,
        ACCF: Fn(&mut A, &A),
        A: Send + Default,
        K: Eq + std::hash::Hash,
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
                let accs = f(v_view);
                for acc in accs {
                    match acc {
                        Acc::Done(_) => {
                            // done with this vertex
                            if !next_active_set.contains(&pid) {
                                next_active_set.remove(&pid);
                            }
                        }
                        Acc::Value(k, v) => {
                            // update the accumulator
                            prev_state
                                .entry(k)
                                .and_modify(|prev| accf(prev, &v))
                                .or_insert_with(|| v);

                            // done with this vertex
                            if !next_active_set.contains(&pid) {
                                next_active_set.remove(&pid);
                            }
                        }
                        Acc::Keep(_) => {
                            next_active_set.insert(pid);
                        }
                        Acc::ValueKeep(k, v) => {
                            // update the accumulator
                            prev_state
                                .entry(k)
                                .and_modify(|prev| accf(prev, &v))
                                .or_insert_with(|| v);

                            next_active_set.insert(pid);
                        }
                    }
                }
            }

            cur_active_set = WorkingSet::Set(next_active_set);
            next_active_set = HashSet::new();
        }
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
mod eval_test{
    use std::collections::HashMap;

    use crate::{graph::TemporalGraph, eval::{Eval, Acc}};


    #[test]
    fn eval_2_connected_components_same_time() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 1);
        g.add_vertex(2, 1);
        g.add_vertex(3, 1);
        g.add_vertex(4, 1);

        g.add_edge(1, 2, 1);
        g.add_edge(3, 4, 1);

        let mut state = HashMap::new();

        // initial step where we init every vertex to it's own ID
        g.eval(
            0..3,
            &mut state,
            |vertex| vec![Acc::Value(vertex.global_id(), vertex.global_id())],
            |c1, c2| {
                *c1 = u64::min(*c1, *c2); // not really needed but for completeness
            },
        );

        assert_eq!(
            state,
            HashMap::from_iter(vec![(1, 1), (2, 2), (3, 3), (4, 4)])
        );

        // second step where we check the state of our neighbours and set ourselves to the min
    }
}