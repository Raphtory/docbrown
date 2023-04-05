use crate::view_api::internal::GraphViewInternalOps;
use docbrown_core::{
    tgraph::{EdgeRef, VertexRef},
    tgraph_shard::errors::GraphError,
    Direction, Prop,
};
use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct LayeredGraph<G: GraphViewInternalOps> {
    /// The underlying `Graph` object.
    pub graph: G,
    /// The layer this graphs points to.
    pub layer: usize,
}

enum ConstrainedLayer {
    Valid(Option<usize>),
    Invalid,
}

impl<G: GraphViewInternalOps> LayeredGraph<G> {
    fn constrain_layer(&self, layer: Option<usize>) -> ConstrainedLayer {
        match layer {
            Some(layer) if layer != self.layer => ConstrainedLayer::Invalid,
            _ => ConstrainedLayer::Valid(Some(self.layer)),
        }
    }
}

impl<G: GraphViewInternalOps> GraphViewInternalOps for LayeredGraph<G> {
    fn earliest_time_global(&self) -> Option<i64> {
        self.graph.earliest_time_global()
    }

    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph.earliest_time_window(t_start, t_end)
    }

    fn latest_time_global(&self) -> Option<i64> {
        self.graph.latest_time_global()
    }

    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64> {
        self.graph.latest_time_window(t_start, t_end)
    }

    fn vertices_len(&self) -> usize {
        self.graph.vertices_len()
    }

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.graph.vertices_len_window(t_start, t_end)
    }

    fn edges_len(&self, layer: Option<usize>) -> usize {
        match self.constrain_layer(layer) {
            ConstrainedLayer::Invalid => 0,
            ConstrainedLayer::Valid(layer) => self.graph.edges_len(layer),
        }
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64, layer: Option<usize>) -> usize {
        match self.constrain_layer(layer) {
            ConstrainedLayer::Invalid => 0,
            ConstrainedLayer::Valid(layer) => self.graph.edges_len_window(t_start, t_end, layer),
        }
    }

    fn has_edge_ref(
        &self,
        src: VertexRef,
        dst: VertexRef,
        layer_name: Option<&str>,
        layer_id: Option<usize>,
    ) -> bool {
        match self.constrain_layer(layer_id) {
            ConstrainedLayer::Invalid => false,
            ConstrainedLayer::Valid(layer) => self.graph.has_edge_ref(src, dst, layer_name, layer),
        }
    }

    fn has_edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer_name: Option<&str>,
        layer_id: Option<usize>,
    ) -> bool {
        match self.constrain_layer(layer_id) {
            ConstrainedLayer::Invalid => false,
            ConstrainedLayer::Valid(layer) => self
                .graph
                .has_edge_ref_window(src, dst, t_start, t_end, layer_name, layer),
        }
    }

    fn has_vertex_ref(&self, v: VertexRef) -> bool {
        self.graph.has_vertex_ref(v)
    }

    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool {
        self.graph.has_vertex_ref_window(v, t_start, t_end)
    }

    fn degree(&self, v: VertexRef, d: Direction, layer: Option<usize>) -> usize {
        match self.constrain_layer(layer) {
            ConstrainedLayer::Invalid => 0,
            ConstrainedLayer::Valid(layer) => self.graph.degree(v, d, layer),
        }
    }

    fn degree_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> usize {
        match self.constrain_layer(layer) {
            ConstrainedLayer::Invalid => 0,
            ConstrainedLayer::Valid(layer) => self.graph.degree(v, t_start, t_end, d, layer),
        }
    }

    fn vertex_ref(&self, v: u64) -> Option<VertexRef> {
        self.graph.vertex_ref(v)
    }

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexRef> {
        self.graph.vertex_ref_window(v, t_start, t_end)
    }

    fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph.vertex_ids()
    }

    fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph.vertex_ids_window(t_start, t_end)
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertex_refs()
    }

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertex_refs_window(t_start, t_end)
    }

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertex_refs_shard(shard)
    }

    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.vertex_refs_window_shard(shard, t_start, t_end)
    }

    fn edge_ref(
        &self,
        src: VertexRef,
        dst: VertexRef,
        layer_name: Option<&str>,
        layer_id: Option<usize>,
    ) -> Option<EdgeRef> {
        self.graph
    }

    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
        layer_name: Option<&str>,
        layer_id: Option<usize>,
    ) -> Option<EdgeRef> {
        match self.constrain_layer(layer_id) {
            ConstrainedLayer::Invalid => None,
            ConstrainedLayer::Valid(layer) => self
                .graph
                .edge_ref_window(src, dst, t_start, t_end, layer_name, layer),
        }
    }

    fn edge_refs(&self, layer: Option<usize>) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        match self.constrain_layer(layer) {
            ConstrainedLayer::Invalid => Box::new(std::iter::empty()),
            ConstrainedLayer::Valid(layer) => self.graph.edge_refs(layer),
        }
    }

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        match self.constrain_layer(layer) {
            ConstrainedLayer::Invalid => Box::new(std::iter::empty()),
            ConstrainedLayer::Valid(layer) => self.graph.edge_refs_window(t_start, t_end, layer),
        }
    }

    fn vertex_edges(
        &self,
        v: VertexRef,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        match self.constrain_layer(layer) {
            ConstrainedLayer::Invalid => Box::new(std::iter::empty()),
            ConstrainedLayer::Valid(layer) => self.graph.vertex_edges(v, d, layer),
        }
    }

    fn vertex_edges_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        match self.constrain_layer(layer) {
            ConstrainedLayer::Invalid => Box::new(std::iter::empty()),
            ConstrainedLayer::Valid(layer) => {
                self.graph.vertex_edges_window(v, t_start, t_end, d, layer)
            }
        }
    }

    fn vertex_edges_window_t(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
        layer: Option<usize>,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send> {
        match self.constrain_layer(layer) {
            ConstrainedLayer::Invalid => Box::new(std::iter::empty()),
            ConstrainedLayer::Valid(layer) => self
                .graph
                .vertex_edges_window_t(v, t_start, t_end, d, layer),
        }
    }

    fn neighbours(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.neighbours(v, d)
    }

    fn neighbours_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send> {
        self.graph.neighbours_window(v, t_start, t_end, d)
    }

    fn neighbours_ids(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph.neighbours_ids(v, d)
    }

    fn neighbours_ids_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph.neighbours_ids_window(v, t_start, t_end, d)
    }

    fn static_vertex_prop(&self, v: VertexRef, name: String) -> Option<Prop> {
        self.graph.static_vertex_prop(v, name)
    }

    fn static_vertex_prop_keys(&self, v: VertexRef) -> Vec<String> {
        self.graph.static_vertex_prop_keys(v)
    }

    fn temporal_vertex_prop_vec(&self, v: VertexRef, name: String) -> Vec<(i64, Prop)> {
        self.graph.temporal_vertex_prop_vec(v, name)
    }

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_vertex_prop_vec_window(v, name, t_start, t_end)
    }

    fn temporal_vertex_props(&self, v: VertexRef) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_vertex_props(v)
    }

    fn temporal_vertex_props_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.temporal_vertex_props_window(v, t_start, t_end)
    }

    fn static_edge_prop(&self, e: EdgeRef, name: String) -> Option<Prop>;

    fn static_edge_prop_keys(&self, e: EdgeRef) -> Vec<String>;

    fn temporal_edge_props_vec(&self, e: EdgeRef, name: String) -> Vec<(i64, Prop)>;

    fn temporal_edge_props_vec_window(
        &self,
        e: EdgeRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)>;

    fn temporal_edge_props(&self, e: EdgeRef) -> HashMap<String, Vec<(i64, Prop)>>;

    fn temporal_edge_props_window(
        &self,
        e: EdgeRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>>;

    fn num_shards(&self) -> usize;

    fn vertices_shard(&self, shard_id: usize) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn vertices_shard_window(
        &self,
        shard_id: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;
}
