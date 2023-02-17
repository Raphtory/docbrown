use crate::error::{GraphError, GraphResult};
use crate::graph::TemporalGraph;
use crate::graphview::{
    EdgeIterator, GraphView, GraphViewInternals, MutableGraph, NeighboursIterator, Properties,
    PropertyHistory, StateView, VertexIterator,
};
use crate::state::StateVec;
use crate::vertexview::{VertexPointer, VertexView};
use crate::{Direction, Prop};
use polars::prelude::{NamedFrom, Series};
use serde::{Deserialize, Serialize};
use std::ops::Range;

#[derive(Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct SinglePartitionGraph {
    graph: TemporalGraph,
    state: Properties,
}

impl GraphViewInternals for SinglePartitionGraph {
    fn local_n_vertices(&self) -> usize {
        self.graph.local_n_vertices()
    }

    fn local_n_edges(&self, direction: Direction) -> usize {
        self.graph.local_n_edges(direction)
    }

    fn local_n_vertices_window(&self, w: Range<i64>) -> usize {
        self.graph.local_n_vertices_window(w)
    }

    fn local_n_edges_window(&self, w: Range<i64>, direction: Direction) -> usize {
        self.graph.local_n_edges_window(w, direction)
    }

    fn local_vertex(&self, gid: u64) -> Option<VertexView<Self>> {
        self.graph.local_vertex(gid).map(|v| v.as_view_of(self))
    }

    fn local_vertex_window(&self, gid: u64, w: Range<i64>) -> Option<VertexView<Self>> {
        self.graph
            .local_vertex_window(gid, w)
            .map(|v| v.as_view_of(self))
    }

    fn local_contains_vertex(&self, gid: u64) -> bool {
        self.graph.local_contains_vertex(gid)
    }

    fn local_contains_vertex_window(&self, gid: u64, w: Range<i64>) -> bool {
        self.graph.local_contains_vertex_window(gid, w)
    }

    fn iter_local_vertices(&self) -> VertexIterator<Self> {
        Box::new(self.graph.iter_local_vertices().map(|v| v.as_view_of(self)))
    }

    fn iter_local_vertices_window(&self, window: Range<i64>) -> VertexIterator<Self> {
        Box::new(
            self.graph
                .iter_local_vertices_window(window)
                .map(|v| v.as_view_of(self)),
        )
    }

    fn degree(&self, vertex: VertexPointer, direction: Direction) -> usize {
        self.graph.degree(vertex, direction)
    }

    fn neighbours(&self, vertex: VertexPointer, direction: Direction) -> NeighboursIterator<Self> {
        Box::new(
            self.graph
                .neighbours(vertex, direction)
                .map(|v| v.as_view_of(self)),
        )
    }

    fn edges(&self, vertex: VertexPointer, direction: Direction) -> EdgeIterator<Self> {
        Box::new(
            self.graph
                .edges(vertex, direction)
                .map(|e| e.as_view_of(self)),
        )
    }

    fn property_history<'a>(
        &'a self,
        vertex: VertexPointer,
        name: &'a str,
    ) -> Option<PropertyHistory<'a>> {
        self.graph.property_history(vertex, name)
    }
}

impl GraphView for SinglePartitionGraph {}

impl MutableGraph for SinglePartitionGraph {
    fn add_vertex_with_props(&mut self, v: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.graph.add_vertex_with_props(v, t, props)
    }

    fn add_edge_with_props(&mut self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.graph.add_edge_with_props(src, dst, t, props)
    }
}

impl StateView for SinglePartitionGraph {
    type StateType<T: Clone> = StateVec<T>;

    fn with_state(self, name: &str, value: Series) -> GraphResult<Self> {
        let named_value = Series::new(name, value);
        let mut state = self.state.clone();
        state.with_column(named_value)?;
        Ok(Self {
            graph: self.graph,
            state,
        })
    }

    fn state(&self) -> &Properties {
        &self.state
    }

    fn get_state(&self, name: &str) -> GraphResult<&Series> {
        Ok(self.state().column(name)?)
    }

    fn new_empty_state<T: Clone>(&self) -> Self::StateType<Option<T>> {
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
