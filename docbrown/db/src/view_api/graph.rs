use crate::view_api::edge::EdgeViewOps;
use crate::view_api::vertex::VertexViewOps;
use docbrown_core::eval::VertexRef;
use docbrown_core::tgraph::{EdgeReference, VertexReference};
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;

pub trait GraphViewOps: Send + Sync {
    type Vertex: VertexViewOps<Edge = Self::Edge>;
    type VertexIter: Iterator<Item = Self::Vertex> + Send;
    type Vertices: IntoIterator<Item = Self::Vertex, IntoIter = Self::VertexIter> + Send;
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;
    type Edges: IntoIterator<Item = Self::Edge>;

    fn num_vertices(&self) -> usize;
    fn earliest_time(&self) -> Option<i64>;
    fn latest_time(&self) -> Option<i64>;
    fn is_empty(&self) -> bool {
        self.num_vertices() == 0
    }
    fn num_edges(&self) -> usize;
    fn has_vertex(&self, v: u64) -> bool;
    fn has_edge(&self, src: u64, dst: u64) -> bool;
    fn vertex(&self, v: u64) -> Option<Self::Vertex>;
    fn vertices(&self) -> Self::Vertices;
    fn edge(&self, src: u64, dst: u64) -> Option<Self::Edge>;
    fn edges(&self) -> Self::Edges;
}
