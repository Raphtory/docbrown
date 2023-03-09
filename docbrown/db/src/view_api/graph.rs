use crate::view_api::edge::EdgeView;
use crate::view_api::vertex::VertexView;

pub trait GraphView {
    type Vertex: for<'a> VertexView<'a, Edge = Self::Edge>;
    type Edge: for<'a> EdgeView<'a, Vertex = Self::Vertex>;

    fn len(&self) -> usize;
    fn edges_len(&self) -> usize;
    fn has_vertex(&self, v: u64) -> bool;
    fn has_edge(&self, src: u64, dst: u64) -> bool;
    fn vertex(&self, v: u64) -> Option<Self::Vertex>;
    fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64>>;
    fn vertices(&self) -> Box<dyn Iterator<Item = Self::Vertex>>;
    fn edge(&self, src: u64, dst: u64) -> Option<Self::Edge>;
    fn edges(&self);
}
