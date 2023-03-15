use docbrown_core::tgraph::{EdgeReference, VertexReference};
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;

pub trait GraphViewInternalOps {
    fn vertices_len(&self) -> usize;

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize;

    fn edges_len(&self) -> usize;

    fn edges_len_window(&self, t_start: i64, t_end: i64) -> usize;

    fn has_edge_ref<V1: Into<VertexReference>, V2: Into<VertexReference>>(
        &self,
        src: V1,
        dst: V2,
    ) -> bool;

    fn has_edge_ref_window<V1: Into<VertexReference>, V2: Into<VertexReference>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> bool;

    fn has_vertex_ref<V: Into<VertexReference>>(&self, v: V) -> bool;

    fn has_vertex_ref_window<V: Into<VertexReference>>(
        &self,
        v: V,
        t_start: i64,
        t_end: i64,
    ) -> bool;

    fn degree(&self, v: VertexReference, d: Direction) -> usize;

    fn degree_window(&self, v: VertexReference, t_start: i64, t_end: i64, d: Direction) -> usize;

    fn vertex_ref(&self, v: u64) -> Option<VertexReference>;

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexReference>;

    fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send>;

    fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send>;

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexReference> + Send>;

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexReference> + Send>;

    fn vertices_par<O, F>(&self, f: F) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexReference) -> O + Send + Sync + Copy;

    fn fold_par<S, F, F2>(&self, f: F, agg: F2) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexReference) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy;

    fn vertices_window_par<O, F>(
        &self,
        t_start: i64,
        t_end: i64,
        f: F,
    ) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexReference) -> O + Send + Sync + Copy;

    fn fold_window_par<S, F, F2>(&self, t_start: i64, t_end: i64, f: F, agg: F2) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexReference) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy;

    fn edge_ref<V1: Into<VertexReference>, V2: Into<VertexReference>>(
        &self,
        src: V1,
        dst: V2,
    ) -> Option<EdgeReference>;

    fn edge_ref_window<V1: Into<VertexReference>, V2: Into<VertexReference>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> Option<EdgeReference>;

    fn edge_refs(&self) -> Box<dyn Iterator<Item = EdgeReference> + Send>;

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeReference> + Send>;

    fn vertex_edges(
        &self,
        v: VertexReference,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeReference> + Send>;

    fn vertex_edges_window(
        &self,
        v: VertexReference,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeReference> + Send>;

    fn vertex_edges_window_t(
        &self,
        v: VertexReference,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeReference> + Send>;

    fn neighbours(
        &self,
        v: VertexReference,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexReference> + Send>;

    fn neighbours_window(
        &self,
        v: VertexReference,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexReference> + Send>;

    fn neighbours_ids(
        &self,
        v: VertexReference,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send>;

    fn neighbours_ids_window(
        &self,
        v: VertexReference,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send>;

    fn vertex_prop_vec(&self, v: VertexReference, name: String) -> Vec<(i64, Prop)>;

    fn vertex_prop_vec_window(
        &self,
        v: VertexReference,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)>;

    fn vertex_props(&self, v: VertexReference) -> HashMap<String, Vec<(i64, Prop)>>;

    fn vertex_props_window(
        &self,
        v: VertexReference,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>>;

    fn edge_props_vec(&self, e: EdgeReference, name: String) -> Vec<(i64, Prop)>;

    fn edge_props_vec_window(
        &self,
        e: EdgeReference,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)>;

    fn edge_props(&self, e: EdgeReference) -> HashMap<String, Vec<(i64, Prop)>>;

    fn edge_props_window(
        &self,
        e: EdgeReference,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>>;
}
