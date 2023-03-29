use std::collections::HashMap;
use crate::view_api::vertex::VertexViewOps;
use crate::view_api::VertexListOps;
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::Prop;

/// This trait defines the operations that can be
/// performed on an edge in a temporal graph view.
pub trait EdgeViewOps: Sized + Send + Sync {
    type Vertex: VertexViewOps<Edge = Self>;

    //TODO need to add specific windowed and non-windowed variants
    fn has_property(&self,name:String,include_static:bool) -> bool ;

    fn property(&self,name:String,include_static:bool) -> Option<Prop>;
    fn properties(&self,include_static:bool) -> HashMap<String,Prop> ;
    fn property_names(&self,include_static:bool) -> Vec<String> ;

    fn has_static_property(&self,name:String)->bool;
    fn static_property(&self,name:String)-> Option<Prop>;

    /// gets a property of an edge with the given name
    /// includes the timestamp of the property
    fn property_history(&self,name:String) -> Vec<(i64, Prop)> ;
    fn property_histories(&self) -> HashMap<String,Vec<(i64, Prop)>> ;

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
    /// The type of vertex on the edge list
    type Vertex: VertexViewOps;

    /// the type of list of vertices
    type VList: VertexListOps;

    /// the type of edge
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;

    /// the type of iterator
    type IterType: Iterator<Item = Self::Edge> + Send;

    /// gets the source vertices of the edges in the list
    fn src(self) -> Self::VList;

    /// gets the destination vertices of the edges in the list
    fn dst(self) -> Self::VList;
}
