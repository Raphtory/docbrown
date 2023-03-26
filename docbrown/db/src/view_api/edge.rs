use std::collections::HashMap;
use crate::view_api::vertex::VertexViewOps;
use crate::view_api::VertexListOps;
use docbrown_core::Prop;

pub trait EdgeViewOps: Sized + Send + Sync {
    type Vertex: VertexViewOps<Edge = Self>;

    fn has_property(&self,name:String,include_static:bool) -> bool ;
    fn property(&self,name:String,include_static:bool) -> Option<Prop>;
    fn properties(&self,include_static:bool) -> HashMap<String,Prop> ;
    fn property_names(&self,include_static:bool) -> Vec<String> ;

    fn has_static_property(&self,name:String)->bool;
    fn static_property(&self,name:String)-> Option<Prop>;

    fn property_history(&self,name:String) -> Vec<(i64, Prop)> ;
    fn property_histories(&self) -> HashMap<String,Vec<(i64, Prop)>> ;

    fn src(&self) -> Self::Vertex;
    fn dst(&self) -> Self::Vertex;
    fn id(&self) -> usize;
}

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
