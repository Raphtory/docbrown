use crate::view_api::vertex::VertexView;
use docbrown_core::Prop;

pub trait EdgeView<'a>: Sized {
    type Vertex: VertexView<'a, Edge = Self>;

    fn prop(&self, name: String) -> Vec<(i64, Prop)>;
    fn src(&self) -> Self::Vertex;
    fn dst(&self) -> Self::Vertex;
}

pub trait EdgeList<'a>:
    IntoIterator<Item = Self::Edge, IntoIter = Self::IntoIterType> + FromIterator<Self::Edge>
{
    type Vertex: VertexView<'a, EList = Self>;
    type Edge: EdgeView<'a, Vertex = Self::Vertex>;
    type IntoIterType: Iterator<Item = Self::Edge> + 'a;
}
