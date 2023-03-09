use crate::view_api::edge::EdgeViewMethods;
use crate::view_api::vertex::VertexViewMethods;

pub trait GraphViewMethods {
    type Vertex: VertexViewMethods<Edge = Self::Edge>;
    type Edge: EdgeViewMethods<Vertex = Self::Vertex>;

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
