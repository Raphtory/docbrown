use crate::vertex::VertexView;
use crate::view_api::internal::GraphViewInternalOps;
use crate::view_api::{EdgeListOps, GraphViewOps};
use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::Prop;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct EdgeView<G: GraphViewOps> {
    graph: G,
    edge: EdgeRef,
}

impl<G: GraphViewOps> Debug for EdgeView<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EdgeView({}, {})",
            self.edge.src_g_id, self.edge.dst_g_id
        )
    }
}

impl<G: GraphViewOps> EdgeView<G> {
    pub(crate) fn new(graph: G, edge: EdgeRef) -> Self {
        EdgeView { graph, edge }
    }

    pub fn as_ref(&self) -> EdgeRef {
        self.edge
    }
}

impl<G: GraphViewOps> From<EdgeView<G>> for EdgeRef {
    fn from(value: EdgeView<G>) -> Self {
        value.edge
    }
}

impl<G: GraphViewOps> EdgeView<G> {
    pub fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_props_vec(self.edge, name)
    }

    pub fn src(&self) -> VertexView<G> {
        //FIXME: Make local ids on EdgeReference optional
        let vertex = VertexRef {
            g_id: self.edge.src_g_id,
            pid: None,
        };
        VertexView::new(self.graph.clone(), vertex)
    }

    pub fn dst(&self) -> VertexView<G> {
        //FIXME: Make local ids on EdgeReference optional
        let vertex = VertexRef {
            g_id: self.edge.dst_g_id,
            pid: None,
        };
        VertexView::new(self.graph.clone(), vertex)
    }

    pub fn id(&self) -> usize {
        self.edge.edge_id
    }
}

impl<G: GraphViewOps> EdgeListOps for Box<dyn Iterator<Item = EdgeView<G>> + Send> {
    type Graph = G;
    type VList = Box<dyn Iterator<Item = VertexView<G>> + Send>;
    type IterType = Box<dyn Iterator<Item = EdgeView<G>> + Send>;

    fn src(self) -> Self::VList {
        Box::new(self.map(|e| e.src()))
    }

    fn dst(self) -> Self::VList {
        Box::new(self.into_iter().map(|e| e.dst()))
    }
}

pub type EdgeList<G: GraphViewOps> = Box<dyn Iterator<Item = EdgeView<G>> + Send>;
