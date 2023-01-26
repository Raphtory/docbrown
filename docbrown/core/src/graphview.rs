use std::ops::{Range};
use crate::graph::{TemporalGraph};
use crate::graph::VertexView;
use polars::prelude::*;
use crate::Direction;
use crate::tadjset::AdjEdge;

type State = DataFrame;

pub struct SingletonIterator<T: Copy>{value: T, exhausted: bool}

impl<T: Copy> SingletonIterator<T> {
    fn new(value: T) -> SingletonIterator<T> {
        SingletonIterator{value, exhausted: false}
    }
}

impl<T: Copy> Iterator for SingletonIterator<T> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        if self.exhausted {
            None
        } else {
            self.exhausted = true;
            Some(self.value)
        }
    }
}

pub struct Vertices<'a> {
    graph_view: &'a GraphView<'a>
}

impl<'a> Vertices<'a> {
    fn new(graph_view: &'a GraphView) -> Vertices<'a> {
        Vertices { graph_view}
    }

    pub fn iter(&'a self) -> VertexIterator<'a> {
        self.graph_view.iter_vertices()
    }
}


impl<'a> IntoIterator for Vertices<'a> {
    type Item = LocalVertexView<'a>;
    type IntoIter = VertexIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.graph_view.iter_vertices()
    }
}


impl<'a> IntoIterator for &'a Vertices<'a> {
    type Item = LocalVertexView<'a>;
    type IntoIter = VertexIterator<'a>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub struct VertexIterator<'a> {
    graph_view: &'a GraphView<'a>,
    inner: Box<dyn Iterator<Item=VertexView<'a, TemporalGraph>> +'a>
}


impl<'a> Iterator for VertexIterator<'a> {
    type Item = LocalVertexView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(
            |v| LocalVertexView::new(self.graph_view, &v)
        )
    }
}


type IteratorWithLifetime<'a, I> = dyn Iterator<Item = I> + 'a;

pub trait VertexViewMethods<'a, I>
where I: VertexViewMethods<'a, I>
{
    type IDsType;
    fn out_neighbours(self) ->  Box<IteratorWithLifetime<'a, I>>;
    fn ids(self) -> Self::IDsType;
}

impl<'a> VertexViewMethods<'a, LocalVertexView<'a>> for LocalVertexView<'a> {
    type IDsType = u64;
    fn out_neighbours(self) -> Box<IteratorWithLifetime<'a, LocalVertexView<'a>>>  {
        Box::new(self.into_out_neighbours())
    }

    fn ids(self) -> Self::IDsType {
        self.global_id()
    }
}


impl<'a, T, R> VertexViewMethods<'a, Box<IteratorWithLifetime<'a, R>>> for T
where
    T: IntoIterator<Item = R> + 'a,
    R: VertexViewMethods<'a, R> + 'a
{
    type IDsType = Box<dyn Iterator<Item = R::IDsType> + 'a>;

    fn out_neighbours(self) -> Box<IteratorWithLifetime<'a, Box<IteratorWithLifetime<'a, R>>>> {
        let inner = self.into_iter();
            Box::new(inner.map(|v| v.out_neighbours()))
    }

    fn ids(self) -> Self::IDsType {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.ids()))
    }
}

pub trait NeighboursIteratorInterface {}

pub struct NeighboursIterator<'a> {
    vertex: &'a LocalVertexView<'a>,
    inner: Box<dyn Iterator<Item=(usize, AdjEdge)> + 'a>
}

pub struct OwnedNeighboursIterator<'a> {
    vertex: LocalVertexView<'a>,
    inner: Box<dyn Iterator<Item=(usize, AdjEdge)> + 'a>
}

impl<'a> Iterator for NeighboursIterator<'a> {
    type Item = LocalVertexView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(
            |(neighbour, AdjEdge(id)) | {
                assert!(id >= 0, "tried to construct remote neighbour but we are assuming everything is local");
                self.vertex.new_neighbour(neighbour)
            }
        )
    }
}


impl<'a> Iterator for OwnedNeighboursIterator<'a> {
    type Item = LocalVertexView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(
            |(neighbour, AdjEdge(id)) | {
                assert!(id >= 0, "tried to construct remote neighbour but we are assuming everything is local");
                self.vertex.new_neighbour(neighbour)
            }
        )
    }
}



pub struct GraphView<'a> {
    graph: &'a TemporalGraph,
    window: &'a Range<i64>,
    state: State
}


pub struct LocalVertexView<'a> {
    graph_view: &'a GraphView<'a>,
    g_id: u64,
    pid: usize
}

impl<'a> LocalVertexView<'a> {
    fn new(graph_view: &'a GraphView, vertex: &VertexView<TemporalGraph>)-> LocalVertexView<'a> {
        LocalVertexView {graph_view, g_id: vertex.global_id(), pid: vertex.pid}
    }
    fn new_neighbour(&self, pid: usize) -> LocalVertexView<'a> {
        LocalVertexView {
            graph_view: self.graph_view,
            g_id: self.graph_view.graph.index[pid].logical().clone(),
            pid
        }
    }

    pub fn global_id(&self) -> u64 {
        self.g_id
    }

    pub fn get_state(&self, name: &str) -> AnyValue {
        self.graph_view.get_state(name).get(self.pid).unwrap()
    }

    pub fn out_neighbours(&'a self) -> NeighboursIterator<'a> {
        let inner = self.graph_view.graph.neighbours_iter_window(self.pid, Direction::OUT, self.graph_view.window );
        NeighboursIterator {inner, vertex: self}
    }
    fn into_out_neighbours(self) -> OwnedNeighboursIterator<'a> {
        let inner = self.graph_view.graph.neighbours_iter_window(self.pid, Direction::OUT, self.graph_view.window );
        OwnedNeighboursIterator {inner, vertex: self}
    }

    pub fn in_neighbours(&'a self) -> NeighboursIterator<'a> {
        let inner = self.graph_view.graph.neighbours_iter_window(self.pid, Direction::IN, self.graph_view.window);
        NeighboursIterator {inner, vertex: self}
    }
}


impl<'a> GraphView<'a> {
    pub fn new(graph: &'a TemporalGraph, window: &'a Range<i64>) -> GraphView<'a> {
        GraphView {graph, window, state: State::default()}
    }

    pub fn n_nodes(&self) -> usize {
        self.iter_vertices().map(|_| 1).sum()
    }

    pub fn vertices(&'a self) -> Vertices<'a> {
        Vertices::new(self)
    }

    fn iter_vertices(&'a self) -> VertexIterator<'a> {
        let inner = self.graph.iter_vs_window(self.window.clone());
        VertexIterator {graph_view: self, inner}
    }

    pub fn with_state<S>(&self, name: &str, value: S) -> GraphView<'a>
    where S: IntoSeries
    {
        let s = Series::new(name, value);

        let new_state = self.state.clone().with_column(s).unwrap().to_owned();
        GraphView {graph: self.graph, window: self.window, state: new_state}
    }

    pub fn get_state(&self, name: &str) -> &Series {
        self.state.column(name).unwrap()
    }

    pub fn ids(&self) -> Series {
        self.iter_vertices().map(|v| v.global_id()).collect()
    }

    fn local_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(self.iter_vertices().map(|v| v.pid))
    }
}


#[cfg(test)]
mod graph_view_tests {
    use itertools::Itertools;
    use super::*;
    use crate::graph::TemporalGraph;

    fn make_mini_graph() -> TemporalGraph {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 0);
        g.add_vertex(2, 0);
        g.add_vertex(3, 1);
        g.add_edge(1, 2, 0);
        g.add_edge(2, 1, 0);
        g.add_edge(2, 3, 0);
        g
    }

    #[test]
    fn test_vertex_window() {
        let g = make_mini_graph();

        let window = 0..1;
        let view = GraphView::new(&g, &window);
        let vertices = view.iter_vertices().map(|v| v.global_id()).collect_vec();
        assert_eq!(vertices, vec![1,2])
    }

    #[test]
    fn test_we_have_state() {
        let g = make_mini_graph();

        let view = GraphView::new(&g, &(0..2));
        let view = view.with_state("ids", view.ids());
        for v in view.iter_vertices() {
            let state = v.get_state("ids");
            let id: u64 = state.extract().unwrap();
            assert_eq!(v.global_id(), id)
        }
    }

    #[test]
    fn test_the_vertices() {
        let g = make_mini_graph();
        let view = GraphView::new(&g, &(0..2));
        let vertex_out_out_neighbours = view.vertices().out_neighbours().out_neighbours().ids().flatten();
        for (id, out_out_neighbours) in view.vertices().ids().zip( vertex_out_out_neighbours) {
            let oo: Vec<u64> = out_out_neighbours.collect();
            println!("vertex: {}, out_out_neighbours: {:?}", id, oo)
        }
        let m = view.vertices().ids().max();
        println!("vertex with maximum id is {}", m.unwrap())
    }
}