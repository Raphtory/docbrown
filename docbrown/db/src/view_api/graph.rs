use crate::view_api::edge::EdgeViewMethods;
use crate::view_api::vertex::VertexViewMethods;

pub trait GraphViewMethods {
    type Vertex: VertexViewMethods<Edge = Self::Edge>;
    type Vertices: IntoIterator<Item = Self::Vertex>;
    type Edge: EdgeViewMethods<Vertex = Self::Vertex>;
    type Edges: IntoIterator<Item = Self::Edge>;

    fn len(&self) -> usize;
    fn edges_len(&self) -> usize;
    fn has_vertex(&self, v: u64) -> bool;
    fn has_edge(&self, src: u64, dst: u64) -> bool;
    fn vertex(&self, v: u64) -> Option<Self::Vertex>;
    fn vertices(&self) -> Self::Vertices;
    fn edge(&self, src: u64, dst: u64) -> Option<Self::Edge>;
    fn edges(&self) -> Self::Edges;
}
