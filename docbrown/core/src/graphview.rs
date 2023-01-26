use std::ops::{Range};
use crate::graph::{TemporalGraph};
use crate::graph::VertexView;
use polars::prelude::*;
use crate::Direction;
use crate::tadjset::AdjEdge;

type State = DataFrame;


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

pub trait VertexViewIteratorMethods<'a, I>
where I: VertexViewIteratorMethods<'a, I>
{
    type ItemType<T: 'a>;
    fn out_neighbours(self) ->  Box<IteratorWithLifetime<'a, I>>;
    fn in_neighbours(self) -> Box<IteratorWithLifetime<'a, I>>;
    fn neighbours(self) -> Box<IteratorWithLifetime<'a, I>>;
    fn id(self) -> Self::ItemType<u64>;
    fn out_degree(self) -> Self::ItemType<usize>;
    fn in_degree(self) -> Self::ItemType<usize>;
    fn degree(self) -> Self::ItemType<usize>;
}

impl<'a> VertexViewIteratorMethods<'a, LocalVertexView<'a>> for LocalVertexView<'a> {
    type ItemType<T: 'a> = T;
    fn out_neighbours(self) -> Box<IteratorWithLifetime<'a, LocalVertexView<'a>>>  {
        Box::new(self.into_out_neighbours())
    }

    fn in_neighbours(self) -> Box<IteratorWithLifetime<'a, LocalVertexView<'a>>> {
        Box::new(self.into_in_neighbours())
    }

    fn neighbours(self) -> Box<IteratorWithLifetime<'a, LocalVertexView<'a>>> {
        Box::new(self.into_neighbours())
    }

    fn id(self) -> Self::ItemType<u64> {
        // need to take ownership for chaining iterators
        LocalVertexView::id(&self)
    }

    fn out_degree(self) -> Self::ItemType<usize> {
        // need to take ownership for chaining iterators
        LocalVertexView::out_degree(&self)
    }

    fn in_degree(self) -> Self::ItemType<usize> {
        // need to take ownership for chaining iterators
        LocalVertexView::in_degree(&self)
    }

    fn degree(self) -> Self::ItemType<usize> {
        // need to take ownership for chaining iterators
        LocalVertexView::degree(&self)
    }
}


impl<'a, T, R> VertexViewIteratorMethods<'a, Box<IteratorWithLifetime<'a, R>>> for T
where
    T: IntoIterator<Item = R> + 'a,
    R: VertexViewIteratorMethods<'a, R> + 'a
{
    type ItemType<U: 'a> = Box<dyn Iterator<Item = R::ItemType<U>> + 'a>;

    fn out_neighbours(self) -> Box<IteratorWithLifetime<'a, Box<IteratorWithLifetime<'a, R>>>> {
        let inner = self.into_iter();
            Box::new(inner.map(|v| v.out_neighbours()))
    }

    fn in_neighbours(self) -> Box<IteratorWithLifetime<'a, Box<IteratorWithLifetime<'a, R>>>> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.in_neighbours()))
    }

    fn neighbours(self) -> Box<IteratorWithLifetime<'a, Box<IteratorWithLifetime<'a, R>>>> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.neighbours()))
    }

    fn id(self) -> Self::ItemType<u64> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.id()))
    }

    fn out_degree(self) -> Self::ItemType<usize> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.out_degree()))
    }

    fn in_degree(self) -> Self::ItemType<usize> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.in_degree()))
    }

    fn degree(self) -> Self::ItemType<usize> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.degree()))
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
    fn new(graph_view: &'a GraphView, vertex: &VertexView<TemporalGraph>) -> LocalVertexView<'a> {
        LocalVertexView { graph_view, g_id: vertex.global_id(), pid: vertex.pid }
    }
    fn new_neighbour(&self, pid: usize) -> LocalVertexView<'a> {
        LocalVertexView {
            graph_view: self.graph_view,
            g_id: self.graph_view.graph.index[pid].logical().clone(),
            pid
        }
    }

    pub fn id(&self) -> u64 {
        self.g_id
    }

    pub fn get_state(&self, name: &str) -> AnyValue {
        self.graph_view.get_state(name).get(self.pid).unwrap()
    }

    pub fn out_neighbours(&'a self) -> NeighboursIterator<'a> {
        let inner = self.graph_view.graph.neighbours_iter_window(self.pid, Direction::OUT, self.graph_view.window);
        NeighboursIterator { inner, vertex: self }
    }
    fn into_out_neighbours(self) -> OwnedNeighboursIterator<'a> {
        let inner = self.graph_view.graph.neighbours_iter_window(self.pid, Direction::OUT, self.graph_view.window);
        OwnedNeighboursIterator { inner, vertex: self }
    }

    pub fn in_neighbours(&'a self) -> NeighboursIterator<'a> {
        let inner = self.graph_view.graph.neighbours_iter_window(self.pid, Direction::IN, self.graph_view.window);
        NeighboursIterator { inner, vertex: self }
    }

    fn into_in_neighbours(self) -> OwnedNeighboursIterator<'a> {
        let inner = self.graph_view.graph.neighbours_iter_window(self.pid, Direction::IN, self.graph_view.window);
        OwnedNeighboursIterator { inner, vertex: self }
    }

    pub fn neighbours(&'a self) -> NeighboursIterator<'a> {
        let inner = self.graph_view.graph.neighbours_iter_window(self.pid, Direction::BOTH, self.graph_view.window);
        NeighboursIterator { inner, vertex: self }
    }

    fn into_neighbours(self) -> OwnedNeighboursIterator<'a> {
        let inner = self.graph_view.graph.neighbours_iter_window(self.pid, Direction::BOTH, self.graph_view.window);
        OwnedNeighboursIterator { inner, vertex: self }
    }

    fn out_degree(&self) -> usize {
        self.graph_view.graph._degree_window(self.pid, Direction::OUT, self.graph_view.window)
    }

    fn in_degree(&self) -> usize {
        self.graph_view.graph._degree_window(self.pid, Direction::IN, self.graph_view.window)
    }

    fn degree(&self) -> usize {
        self.graph_view.graph._degree_window(self.pid, Direction::BOTH, self.graph_view.window)
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
        self.iter_vertices().map(|v| v.id()).collect()
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
        g.add_edge(2, 3, 1);
        g
    }

    #[test]
    fn test_vertex_window() {
        let g = make_mini_graph();

        let window = 0..1;
        let view = GraphView::new(&g, &window);
        let vertices = view.iter_vertices().map(|v| v.id()).collect_vec();
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
            assert_eq!(v.id(), id)
        }
    }

    #[test]
    fn test_the_vertices() {
        let g = make_mini_graph();
        let view = GraphView::new(&g, &(0..2));
        let vertex_out_out_neighbours = view.vertices().out_neighbours().out_neighbours().id().flatten();
        for (id, out_out_neighbours) in view.vertices().id().zip( vertex_out_out_neighbours) {
            let oo: Vec<u64> = out_out_neighbours.collect();
            println!("vertex: {}, out_out_neighbours: {:?}", id, oo)
        }
        let m = view.vertices().id().max();
        println!("vertex with maximum id is {}", m.unwrap())
    }
}