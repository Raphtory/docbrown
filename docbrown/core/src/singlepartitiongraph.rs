use crate::graph::TemporalGraph;
use crate::graphview::{
    EdgeIterator, GraphView, GraphViewInternals, MutableGraph, NeighboursIterator, PropertyHistory,
    VertexIterator,
};
use crate::vertexview::{VertexPointer, VertexView, VertexViewMethods};
use crate::{Direction, Prop};
use serde::{Deserialize, Serialize};
use std::ops::Range;

#[derive(Default, Debug, Serialize, Deserialize, PartialEq)]
pub struct SinglePartitionGraph(TemporalGraph);

impl GraphViewInternals for SinglePartitionGraph {
    fn local_n_vertices(&self) -> usize {
        self.0.local_n_vertices()
    }

    fn local_n_edges(&self, direction: Direction) -> usize {
        self.0.local_n_edges(direction)
    }

    fn local_n_vertices_window(&self, w: Range<i64>) -> usize {
        self.0.local_n_vertices_window(w)
    }

    fn local_n_edges_window(&self, w: Range<i64>, direction: Direction) -> usize {
        self.0.local_n_edges_window(w, direction)
    }

    fn local_vertex(&self, gid: u64) -> Option<VertexView<Self>> {
        self.0.local_vertex(gid).map(|v| v.as_view_of(self))
    }

    fn local_vertex_window(&self, gid: u64, w: Range<i64>) -> Option<VertexView<Self>> {
        self.0
            .local_vertex_window(gid, w)
            .map(|v| v.as_view_of(self))
    }

    fn local_contains_vertex(&self, gid: u64) -> bool {
        self.0.local_contains_vertex(gid)
    }

    fn local_contains_vertex_window(&self, gid: u64, w: Range<i64>) -> bool {
        self.0.local_contains_vertex_window(gid, w)
    }

    fn iter_local_vertices(&self) -> VertexIterator<Self> {
        Box::new(self.0.iter_local_vertices().map(|v| v.as_view_of(self)))
    }

    fn iter_local_vertices_window(&self, window: Range<i64>) -> VertexIterator<Self> {
        Box::new(
            self.0
                .iter_local_vertices_window(window)
                .map(|v| v.as_view_of(self)),
        )
    }

    fn degree(&self, vertex: VertexPointer, direction: Direction) -> usize {
        self.0.degree(vertex, direction)
    }

    fn neighbours(&self, vertex: VertexPointer, direction: Direction) -> NeighboursIterator<Self> {
        Box::new(
            self.0
                .neighbours(vertex, direction)
                .map(|v| v.as_view_of(self)),
        )
    }

    fn edges(&self, vertex: VertexPointer, direction: Direction) -> EdgeIterator<Self> {
        Box::new(self.0.edges(vertex, direction).map(|e| e.as_view_of(self)))
    }

    fn property_history<'a>(
        &'a self,
        vertex: VertexPointer,
        name: &'a str,
    ) -> Option<PropertyHistory<'a>> {
        self.0.property_history(vertex, name)
    }
}

impl GraphView for SinglePartitionGraph {}

impl MutableGraph for SinglePartitionGraph {
    fn add_vertex_with_props(&mut self, v: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.0.add_vertex_with_props(v, t, props)
    }

    fn add_edge_with_props(&mut self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.0.add_edge_with_props(src, dst, t, props)
    }
}
