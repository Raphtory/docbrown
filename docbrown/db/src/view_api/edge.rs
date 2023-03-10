use crate::view_api::vertex::VertexViewMethods;
use docbrown_core::Prop;

pub trait EdgeViewMethods: Sized {
    type Vertex: VertexViewMethods<Edge = Self>;

    fn prop(&self, name: String) -> Vec<(i64, Prop)>;
    fn src(&self) -> Self::Vertex;
    fn dst(&self) -> Self::Vertex;
}

pub trait EdgeListMethods:
    IntoIterator<Item = Self::Edge, IntoIter = Self::IterType> + Sized
{
    type Vertex: VertexViewMethods;
    type Edge: EdgeViewMethods<Vertex = Self::Vertex>;
    type IterType: Iterator<Item = Self::Edge>;
}
