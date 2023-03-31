use crate::edge::EdgeView;
use crate::view_api::{GraphViewOps, VertexListOps};

pub trait EdgeListOps:
    IntoIterator<Item = EdgeView<Self::Graph>, IntoIter = Self::IterType> + Sized + Send
{
    type Graph: GraphViewOps;
    type VList: VertexListOps<Graph = Self::Graph>;
    type IterType: Iterator<Item = EdgeView<Self::Graph>> + Send;

    fn src(self) -> Self::VList;
    fn dst(self) -> Self::VList;
}
