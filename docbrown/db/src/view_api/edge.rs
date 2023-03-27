use crate::view_api::vertex::VertexViewOps;
use crate::view_api::VertexListOps;
use docbrown_core::Prop;

/// This trait defines the operations that can be
/// performed on an edge in a temporal graph view.
pub trait EdgeViewOps: Sized + Send + Sync {
    type Vertex: VertexViewOps<Edge = Self>;

    /// gets a property of an edge with the given name
    /// includes the timestamp of the property
    fn prop(&self, name: String) -> Vec<(i64, Prop)>;

    /// gets the source vertex of an edge
    fn src(&self) -> Self::Vertex;

    /// gets the destination vertex of an edge
    fn dst(&self) -> Self::Vertex;

    /// gets the id of an edge
    fn id(&self) -> usize;
}

/// This trait defines the operations that can be
/// performed on a list of edges in a temporal graph view.
pub trait EdgeListOps:
    IntoIterator<Item = Self::Edge, IntoIter = Self::IterType> + Sized + Send
{
    type Vertex: VertexViewOps;
    type VList: VertexListOps;
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;
    type IterType: Iterator<Item = Self::Edge> + Send;

    fn src(self) -> Self::VList;
    fn dst(self) -> Self::VList;
}
