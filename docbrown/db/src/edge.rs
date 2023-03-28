use crate::vertex::VertexView;
use crate::view_api::internal::GraphViewInternalOps;
use crate::view_api::{EdgeListOps, EdgeViewOps, GraphViewOps};
use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::Prop;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;

pub struct EdgeView<G: GraphViewOps> {
    graph: Arc<G>,
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
    pub(crate) fn new(graph: Arc<G>, edge: EdgeRef) -> Self {
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

impl<G: GraphViewOps> EdgeViewOps for EdgeView<G> {
    type Vertex = VertexView<G>;

    fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.graph.temporal_edge_props_vec(self.edge, name)
    }

    fn src(&self) -> Self::Vertex {
        //FIXME: Make local ids on EdgeReference optional
        let vertex = VertexRef {
            g_id: self.edge.src_g_id,
            pid: None,
        };
        Self::Vertex::new(self.graph.clone(), vertex)
    }

    fn dst(&self) -> Self::Vertex {
        //FIXME: Make local ids on EdgeReference optional
        let vertex = VertexRef {
            g_id: self.edge.dst_g_id,
            pid: None,
        };
        Self::Vertex::new(self.graph.clone(), vertex)
    }

    fn id(&self) -> usize {
        self.edge.edge_id
    }
}

impl<G: GraphViewOps> EdgeListOps for Box<dyn Iterator<Item = EdgeView<G>> + Send> {
    type Vertex = VertexView<G>;
    type VList = Box<dyn Iterator<Item = Self::Vertex> + Send>;
    type Edge = EdgeView<G>;
    type IterType = Box<dyn Iterator<Item = Self::Edge> + Send>;

    fn src(self) -> Self::VList {
        Box::new(self.map(|e| e.src()))
    }

    fn dst(self) -> Self::VList {
        Box::new(self.into_iter().map(|e| e.dst()))
    }
}
