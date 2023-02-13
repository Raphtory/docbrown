use crate::error::{GraphError, GraphResult};
use crate::graph::{EdgeView, TemporalGraph};
use crate::state::{State, StateVec};
use crate::tadjset::AdjEdge;
use crate::vertexview::VertexView;
use crate::{Direction, Prop};
use polars::prelude::*;
use polars_lazy::prelude::*;
use std::error::Error;
use std::fmt::Formatter;
use std::ops::Range;
use std::{error, fmt};

pub type IteratorWithLifetime<'a, I> = dyn Iterator<Item = I> + 'a;
pub type VertexIterator<'a, G> = Box<IteratorWithLifetime<'a, VertexView<'a, G>>>;
pub type NeighboursIterator<'a, G> = VertexIterator<'a, G>;
pub type EdgeIterator<'a, G> = Box<IteratorWithLifetime<'a, EdgeView<'a, G>>>;
pub type PropertyHistory<'a> = Box<IteratorWithLifetime<'a, (&'a i64, Prop)>>;


// type State = DataFrame;

pub struct Vertices<'a, G>
where
    G: GraphView<'a>,
{
    graph_view: &'a G,
}

impl<'a, G> Vertices<'a, G>
where
    G: GraphView<'a>,
{
    fn new(graph_view: &'a G) -> Vertices<'a, G> {
        Vertices { graph_view }
    }

    pub fn iter(&'a self) -> VertexIterator<'a, G> {
        self.graph_view.iter_vertices()
    }
}

impl<'a, G> IntoIterator for Vertices<'a, G>
where
    G: GraphView<'a>,
{
    type Item = VertexView<'a, G>;
    type IntoIter = VertexIterator<'a, G>;

    fn into_iter(self) -> Self::IntoIter {
        self.graph_view.iter_vertices()
    }
}

impl<'a, G: GraphView<'a>> IntoIterator for &'a Vertices<'a, G> {
    type Item = VertexView<'a, G>;
    type IntoIter = VertexIterator<'a, G>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}

pub trait NeighboursIteratorInterface {}

pub type Properties = DataFrame;

pub trait GraphViewInternals: Sized {
    /// Get number of vertices in the partition of the view
    fn local_n_vertices(&self) -> usize;

    /// Get the number of edges in the partition of the view
    fn local_n_edges(&self) -> usize;

    /// Get number of vertices in the current view with time window
    fn local_n_vertices_window(&self) -> usize;

    /// Get the number of edges in the current view with time window
    fn local_n_edges_window(&self) -> usize;

    /// Get a single vertex by global id
    fn vertex(&self, gid: u64) -> Option<VertexView<Self>>;

    /// Iterate over all vertices in the current view
    fn iter_vertices(&self) -> VertexIterator<Self>;

    /// Filter vertices by time window
    fn iter_vertices_window(&self, window: &Range<i64>) -> VertexIterator<Self>;

    /// Get degree for vertex (Vertex view has a window which should be respected by this function)
    fn degree(&self, vertex: &VertexView<Self>, direction: Direction) -> usize;

    /// Get neighbours for vertex (Vertex view has a window which should be respected by this function)
    fn neighbours(
        &self,
        vertex: &VertexView<Self>,
        direction: Direction,
    ) -> NeighboursIterator<Self>;

    /// Get edges incident at a vertex (Vertex view has a window which should be respected by this function)
    fn edges(&self, vertex: &VertexView<Self>, direction: Direction) -> EdgeIterator<Self>;
    
    /// Get the property history of a vertex (Vertex view has a window which should be respected by this function)
    fn property_history(
        &self, 
        vertex: &VertexView<Self>,
        name: &str,
    ) -> PropertyHistory;
}

pub trait GraphView: GraphViewInternals {
    /// Global number of nodes (should be the sum over all partitions)
    fn n_nodes(&self) -> usize;

    fn vertices(&self) -> Vertices<'_, Self> {
        Vertices::new(self)
    }

    fn with_state(&self, name: &str, value: Series) -> Self;

    fn state(&self) -> Properties;

    fn get_state(&self, name: &str) -> GraphResult<&Series> {
        Ok(self.state().column(name)?)
    }

    fn new_empty_state<T: Clone>(&self) -> StateVec<Option<T>> {
        StateVec::empty(self.n_nodes())
    }

    fn new_full_state<T: Clone>(&self, value: T) -> StateVec<T> {
        StateVec::full(value, self.n_nodes())
    }

    fn new_state_from<T, I: IntoIterator<Item = T>>(&self, iter: I) -> GraphResult<StateVec<T>> {
        let state = StateVec::from_iter(iter);
        if state.len() == self.n_nodes() {
            Ok(state)
        } else {
            Err(GraphError::StateSizeError)
        }
    }
}

#[cfg(test)]
mod graph_view_tests {
    use super::*;
    use crate::graph::TemporalGraph;
    use crate::vertexview::VertexViewMethods;
    use itertools::Itertools;

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
        assert_eq!(vertices, vec![1, 2])
    }

    #[test]
    fn test_we_have_state() {
        let g = make_mini_graph();

        let view = GraphView::new(&g, &(0..2));
        let view = view.with_state("ids", view.ids());
        for v in view.vertices().iter() {
            let state = (&v).get_state("ids");
            let id: u64 = state.extract().unwrap();
            assert_eq!(v.id(), id)
        }
    }

    #[test]
    fn test_the_vertices() {
        let g = make_mini_graph();
        let view = GraphView::new(&g, &(0..2));
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
