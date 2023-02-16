use crate::error::{GraphError, GraphResult};
use crate::graph::{EdgeView, TemporalGraph};
use crate::state::{State, StateVec};
use crate::tadjset::AdjEdge;
use crate::vertexview::{VertexPointer, VertexView, VertexViewMethods};
use crate::{Direction, Prop};
use polars;
use polars::prelude::NamedFrom;
use polars::series::{IntoSeries, Series};
use polars_lazy;
use std::borrow::Borrow;
use std::error::Error;
use std::fmt::Formatter;
use std::ops::Range;
use std::{error, fmt};

pub type IteratorWithLifetime<'a, I> = dyn Iterator<Item = I> + 'a;
pub type VertexIterator<'a, G> = Box<IteratorWithLifetime<'a, VertexView<'a, G>>>;
pub type NeighboursIterator<'a, G> = VertexIterator<'a, G>;
pub type EdgeIterator<'a, G> = Box<IteratorWithLifetime<'a, EdgeView<'a, G>>>;
pub type PropertyHistory<'a> = Vec<(i64, Prop)>;

// type State = DataFrame;

pub struct Vertices<'a, G>
where
    G: GraphView,
{
    graph_view: &'a G,
}

impl<'a, G> Vertices<'a, G>
where
    G: GraphView,
{
    fn new(graph_view: &'a G) -> Vertices<'a, G> {
        Vertices { graph_view }
    }

    pub fn iter(&'a self) -> VertexIterator<'a, G> {
        self.graph_view.iter_local_vertices()
    }
}

impl<'a, G> IntoIterator for Vertices<'a, G>
where
    G: GraphView,
{
    type Item = VertexView<'a, G>;
    type IntoIter = VertexIterator<'a, G>;

    fn into_iter(self) -> Self::IntoIter {
        self.graph_view.iter_local_vertices()
    }
}

impl<'a, G: GraphView> IntoIterator for &'a Vertices<'a, G> {
    type Item = VertexView<'a, G>;
    type IntoIter = VertexIterator<'a, G>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub trait NeighboursIteratorInterface {}

pub type Properties = polars::frame::DataFrame;

pub trait MutableGraph {
    fn add_vertex(&mut self, v: u64, t: i64) {
        self.add_vertex_with_props(v, t, &vec![])
    }

    fn add_vertex_with_props(&mut self, v: u64, t: i64, props: &Vec<(String, Prop)>);

    fn add_edge(&mut self, src: u64, dst: u64, t: i64) {
        self.add_edge_with_props(src, dst, t, &vec![])
    }

    fn add_edge_with_props(&mut self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>);
}

pub trait GraphViewInternals: Sized {
    /// Get number of vertices in the partition of the view
    fn local_n_vertices(&self) -> usize {
        self.iter_local_vertices().count()
    }

    /// Get the number of edges in the partition of the view
    fn local_n_edges(&self, direction: Direction) -> usize {
        match direction {
            Direction::IN => self.iter_local_vertices().in_degree().sum(),
            Direction::OUT => self.iter_local_vertices().out_degree().sum(),
            Direction::BOTH => {
                self.local_n_edges(Direction::IN) + self.local_n_edges(Direction::OUT)
            }
        }
    }

    /// Get number of vertices in the current view with time window
    fn local_n_vertices_window(&self, w: Range<i64>) -> usize {
        self.iter_local_vertices_window(w).count()
    }

    /// Get the number of edges in the current view with time window
    fn local_n_edges_window(&self, w: Range<i64>, direction: Direction) -> usize {
        match direction {
            Direction::IN => self.iter_local_vertices_window(w).in_degree().sum(),
            Direction::OUT => self.iter_local_vertices_window(w).out_degree().sum(),
            Direction::BOTH => {
                self.local_n_edges_window(w.clone(), Direction::IN)
                    + self.local_n_edges_window(w, Direction::OUT)
            }
        }
    }

    /// Get a single vertex by global id
    fn local_vertex(&self, gid: u64) -> Option<VertexView<Self>>;

    fn local_vertex_window(&self, gid: u64, w: Range<i64>) -> Option<VertexView<Self>>;

    fn local_contains_vertex(&self, gid: u64) -> bool {
        self.local_vertex(gid).is_some()
    }

    fn local_contains_vertex_window(&self, gid: u64, w: Range<i64>) -> bool {
        self.local_vertex_window(gid, w).is_some()
    }

    /// Iterate over all vertices in the current view
    fn iter_local_vertices(&self) -> VertexIterator<Self>;

    /// Filter vertices by time window
    fn iter_local_vertices_window(&self, window: Range<i64>) -> VertexIterator<Self>;

    /// Get degree for vertex (Vertex view has a window which should be respected by this function)
    fn degree(&self, vertex: VertexPointer, direction: Direction) -> usize;

    /// Get neighbours for vertex (Vertex view has a window which should be respected by this function)
    fn neighbours<'a>(
        &'a self,
        vertex: VertexPointer,
        direction: Direction,
    ) -> NeighboursIterator<'a, Self>;

    /// Get edges incident at a vertex (Vertex view has a window which should be respected by this function)
    fn edges<'a>(&'a self, vertex: VertexPointer, direction: Direction) -> EdgeIterator<'a, Self>;

    /// Get the property history of a vertex (Vertex view has a window which should be respected by this function)
    fn property_history<'a>(
        &'a self,
        vertex: VertexPointer,
        name: &'a str,
    ) -> Option<PropertyHistory<'a>>;
}

/// Implement this trait to mark a graph as a global view
pub trait GraphView: GraphViewInternals {
    /// Global number of nodes (should be the sum over all partitions)
    fn n_vertices(&self) -> usize {
        self.local_n_vertices()
    }

    /// Global number of edges (should be the sum over all partitions)
    fn n_edges(&self) -> usize {
        self.local_n_edges(Direction::OUT)
    }

    fn vertices(&self) -> Vertices<'_, Self> {
        Vertices::new(self)
    }

    /// Get number of vertices in the current view with time window
    fn n_vertices_window(&self, w: Range<i64>) -> usize {
        self.local_n_vertices_window(w)
    }

    /// Get the number of edges in the current view with time window
    fn n_edges_window(&self, w: Range<i64>) -> usize {
        self.local_n_edges_window(w, Direction::OUT)
    }

    /// Get a single vertex by global id
    fn vertex(&self, gid: u64) -> Option<VertexView<Self>> {
        self.local_vertex(gid)
    }

    fn vertex_window(&self, gid: u64, w: Range<i64>) -> Option<VertexView<Self>> {
        self.local_vertex_window(gid, w)
    }

    fn contains_vertex(&self, gid: u64) -> bool {
        self.local_contains_vertex(gid)
    }

    fn contains_vertex_window(&self, gid: u64, w: Range<i64>) -> bool {
        self.local_contains_vertex_window(gid, w)
    }
}

pub trait StateView: GraphViewInternals {
    fn with_state(&self, name: &str, value: Series) -> GraphResult<Self>;

    fn state(&self) -> &Properties;

    fn get_state(&self, name: &str) -> GraphResult<&Series> {
        Ok(self.state().column(name)?)
    }

    fn new_empty_state<T: Clone>(&self) -> StateVec<Option<T>> {
        StateVec::empty(self.local_n_vertices())
    }

    fn new_full_state<T: Clone>(&self, value: T) -> StateVec<T> {
        StateVec::full(value, self.local_n_vertices())
    }

    fn new_state_from<T, I: IntoIterator<Item = T>>(&self, iter: I) -> GraphResult<StateVec<T>> {
        let state = StateVec::from_iter(iter);
        if state.len() == self.local_n_vertices() {
            Ok(state)
        } else {
            Err(GraphError::StateSizeError)
        }
    }
}

pub struct WindowedView<'a, G: GraphViewInternals> {
    graph: &'a G,
    window: Range<i64>,
    state: Properties,
}

impl<'a, G: GraphViewInternals> WindowedView<'a, G> {
    pub fn new(graph: &'a G, window: Range<i64>) -> Self {
        Self {
            graph,
            window,
            state: Properties::default(),
        }
    }

    fn actual_window(&self, w: Option<Range<i64>>) -> Range<i64> {
        match w {
            Some(w) => {
                std::cmp::max(w.start, self.window.start)..std::cmp::min(w.end, self.window.end)
            }
            None => self.window.clone(),
        }
    }
}

impl<'a, G: StateView> WindowedView<'a, G> {
    pub fn new_from_view(graph: &'a G, window: Range<i64>) -> Self {
        Self {
            graph,
            window,
            state: graph.state().clone(),
        }
    }
}

impl<'a, G> GraphViewInternals for WindowedView<'a, G>
where
    G: GraphViewInternals,
{
    fn local_n_vertices(&self) -> usize {
        self.graph.local_n_vertices_window(self.window.clone())
    }

    fn local_n_edges(&self, direction: Direction) -> usize {
        self.graph
            .local_n_edges_window(self.window.clone(), direction)
    }

    fn local_n_vertices_window(&self, w: Range<i64>) -> usize {
        let actual_window = self.actual_window(Some(w));
        self.graph.local_n_vertices_window(actual_window)
    }

    fn local_n_edges_window(&self, w: Range<i64>, direction: Direction) -> usize {
        let actual_window = self.actual_window(Some(w));
        self.graph.local_n_edges_window(actual_window, direction)
    }

    fn local_vertex(&self, gid: u64) -> Option<VertexView<Self>> {
        self.graph
            .local_vertex_window(gid, self.window.clone())
            .map(|v| v.as_view_of(self))
    }

    fn local_vertex_window(&self, gid: u64, w: Range<i64>) -> Option<VertexView<Self>> {
        let actual_window = self.actual_window(Some(w));
        self.graph
            .local_vertex_window(gid, actual_window)
            .map(|v| v.as_view_of(self))
    }

    fn iter_local_vertices(&self) -> VertexIterator<Self> {
        Box::new(
            self.graph
                .iter_local_vertices_window(self.window.clone())
                .map(|v| v.as_view_of(self)),
        )
    }

    fn iter_local_vertices_window(&self, w: Range<i64>) -> VertexIterator<Self> {
        let actual_window = self.actual_window(Some(w));
        Box::new(
            self.graph
                .iter_local_vertices_window(actual_window)
                .map(|v| v.as_view_of(self)),
        )
    }

    fn degree(&self, vertex: VertexPointer, direction: Direction) -> usize {
        let actual_window = self.actual_window(vertex.w.clone());
        self.graph
            .degree(vertex.with_window(actual_window), direction)
    }

    fn neighbours(&self, vertex: VertexPointer, direction: Direction) -> NeighboursIterator<Self> {
        let actual_window = self.actual_window(vertex.w.clone());
        Box::new(
            self.graph
                .neighbours(vertex.with_window(actual_window), direction)
                .map(|v| v.as_view_of(self)),
        )
    }

    fn edges<'b>(&'b self, vertex: VertexPointer, direction: Direction) -> EdgeIterator<'b, Self> {
        let actual_window = self.actual_window(vertex.w.clone());
        Box::new(
            self.graph
                .edges(vertex.with_window(actual_window), direction)
                .map(|e| e.as_view_of(self)),
        )
    }

    fn property_history<'b>(
        &'b self,
        vertex: VertexPointer,
        name: &'b str,
    ) -> Option<PropertyHistory<'b>> {
        let actual_window = self.actual_window(vertex.w.clone());
        self.graph
            .property_history(vertex.with_window(actual_window), name)
    }
}

impl<'a, G> GraphView for WindowedView<'a, G>
where
    G: GraphView,
{
    fn n_vertices(&self) -> usize {
        self.graph.n_vertices()
    }

    fn n_edges(&self) -> usize {
        self.graph.n_edges()
    }
}

impl<'a, G> StateView for WindowedView<'a, G>
where
    G: GraphViewInternals,
{
    fn with_state(&self, name: &str, value: Series) -> GraphResult<Self> {
        let named_value = Series::new(name, value);
        let mut state = self.state.clone();
        state.with_column(named_value)?;
        Ok(Self {
            graph: self.graph,
            window: self.window.clone(),
            state,
        })
    }

    fn state(&self) -> &Properties {
        &self.state
    }
}

#[cfg(test)]
mod graph_view_tests {
    use super::*;
    use crate::singlepartitiongraph::SinglePartitionGraph;
    use crate::vertexview::VertexViewMethods;
    use itertools::Itertools;

    fn make_mini_graph() -> SinglePartitionGraph {
        let mut g = SinglePartitionGraph::default();

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
        let view = WindowedView::new(&g, window);
        let vertices = view.iter_local_vertices().map(|v| v.id()).collect_vec();
        assert_eq!(vertices, vec![1, 2])
    }

    #[test]
    fn test_we_have_state() {
        let g = make_mini_graph();

        let view = WindowedView::new(&g, 0..2);
        let view = view.with_state("ids", view.vertices().id().collect());
        // for v in view.vertices().iter() {
        //     let state = (&v).get_state("ids");
        //     let id: u64 = state.extract().unwrap();
        //     assert_eq!(v.id(), id)
        // }
    }

    #[test]
    fn test_the_vertices() {
        let g = make_mini_graph();
        let view = WindowedView::new(&g, 0..2);
        let vertex_out_out_neighbours = view
            .vertices()
            .out_neighbours()
            .out_neighbours()
            .id()
            .flatten();
        for (id, out_out_neighbours) in view.vertices().id().zip(vertex_out_out_neighbours) {
            let oo: Vec<u64> = out_out_neighbours.collect();
            println!("vertex: {}, out_out_neighbours: {:?}", id, oo)
        }
        let m = view.vertices().id().max();
        println!("vertex with maximum id is {}", m.unwrap())
    }

    // #[test]
    // fn test_the_state() {
    //     let g = make_mini_graph();
    //     let view = GraphView::new(&g, &(0..2));
    //
    //     let view = view.with_state("ids", view.ids());
    //     let state = view.new_state_from(view.vertices().id()).unwrap();
    //
    //     let out_out_ids = view
    //         .vertices()
    //         .out_neighbours()
    //         .out_neighbours()
    //         .with_state(&state)
    //         .map(|it| it.map(|it| it.collect::<Vec<_>>()).collect::<Vec<_>>())
    //         .collect::<Vec<_>>();
    //
    //     println!("result: {:?}", out_out_ids)
    // }
}
