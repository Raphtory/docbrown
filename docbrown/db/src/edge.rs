use std::collections::HashMap;
use crate::vertex::VertexView;
use crate::view_api::internal::GraphViewInternalOps;
use crate::view_api::{EdgeListOps, EdgeViewOps};
use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::Prop;
use std::fmt::{Debug, Formatter};
use std::sync::Arc;
use crate::graph::Graph;

pub struct EdgeView<G: GraphViewInternalOps> {
    graph: Arc<G>,
    edge: EdgeRef,
}

impl<G: GraphViewInternalOps> Debug for EdgeView<G> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "EdgeView({}, {})",
            self.edge.src_g_id, self.edge.dst_g_id
        )
    }
}

impl<G: GraphViewInternalOps> EdgeView<G> {
    pub(crate) fn new(graph: Arc<G>, edge: EdgeRef) -> Self {
        EdgeView { graph, edge }
    }

    pub fn as_ref(&self) -> EdgeRef {
        self.edge
    }
}

impl<G: GraphViewInternalOps> Into<EdgeRef> for EdgeView<G> {
    fn into(self) -> EdgeRef {
        self.edge
    }
}

impl<G: GraphViewInternalOps + 'static + Send + Sync> EdgeViewOps for EdgeView<G> {
    type Vertex = VertexView<G>;

    fn property(&self,name:String,include_static:bool) ->  Result<Option<Prop>, GraphError> {
        let props= self.property_history(name.clone())?;
        Ok(match props.last() {
            None => {
                if include_static {
                    match self.graph.static_edge_prop(self.edge, name)? {
                        None => { None}
                        Some(prop) => { Some(prop) }
                    }
                }
                else {Ok(None)}
            },
            Some((_,prop)) => {Some(prop.clone())}
        })
    }
    fn property_history(&self,name:String) -> Result<Vec<(i64, Prop)>,GraphError> {
        //MIN MAX given as I can't get the real times from here and the internal graph sorts it out
        self.graph.temporal_edge_props_vec_window(self.edge, name,i64::MIN,i64::MAX)
    }
    fn properties(&self,include_static:bool) -> Result<HashMap<String,Prop>,GraphError> {
        let mut props:HashMap<String,Prop> = self.property_histories()?.iter().map(|(key,values)| {
            (key.clone(),values.last().unwrap().1.clone())
        }).collect();

        if include_static{
            for prop_name in self.graph.static_edge_prop_keys(self.edge)? {
                match self.graph.static_edge_prop(self.edge,prop_name.clone())? {
                    Some(prop) => {props.insert(prop_name,prop);}
                    None => {}
                }
            }
        }
        Ok(props)
    }

    fn property_histories(&self) -> Result<HashMap<String,Vec<(i64, Prop)>>,GraphError> {
        self.graph.temporal_edge_props_window(self.edge,i64::MIN,i64::MAX)
    }
    fn property_names(&self,include_static:bool) -> Result<Vec<String>,GraphError> {
        let mut names:Vec<String> = self.graph.temporal_edge_props_window(self.edge,i64::MIN,i64::MAX)?.into_keys().collect();
        if include_static {
            names.extend(self.graph.static_edge_prop_keys(self.edge)?)
        }
        Ok(names)
    }
    fn has_property(&self,name:String,include_static:bool) -> Result<bool,GraphError> {
        Ok(self.property_names(include_static)?.contains(&name))
    }

    fn has_static_property(&self,name:String)->Result<bool,GraphError>{
        Ok(self.graph.static_edge_prop_keys(self.edge)?.contains(&name))
    }

    fn static_property(&self,name:String)-> Result<Option<Prop>,GraphError>{
        self.graph.static_edge_prop(self.edge,name)
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

impl<G: GraphViewInternalOps + 'static + Send + Sync> EdgeListOps
    for Box<dyn Iterator<Item = EdgeView<G>> + Send>
{
    type Vertex = VertexView<G>;
    type VList = Box<dyn Iterator<Item = Self::Vertex> + Send>;
    type Edge = EdgeView<G>;
    type IterType = Box<dyn Iterator<Item = Self::Edge> + Send>;

    fn src(self) -> Self::VList {
        Box::new(self.into_iter().map(|e| e.src()))
    }

    fn dst(self) -> Self::VList {
        Box::new(self.into_iter().map(|e| e.dst()))
    }
}
