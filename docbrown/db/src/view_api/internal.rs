use docbrown_core::tgraph::{EdgeRef, VertexRef};
use docbrown_core::tgraph_shard::TGraphShard;
use docbrown_core::{Direction, Prop};
use rayon::prelude::*;
use std::collections::HashMap;
use std::sync::Arc;

pub trait GraphViewInternalOps {
    fn earliest_time_global(&self) -> Option<i64>;
    fn earliest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64>;
    fn latest_time_global(&self) -> Option<i64>;
    fn latest_time_window(&self, t_start: i64, t_end: i64) -> Option<i64>;

    fn vertices_len(&self) -> usize;

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize;

    fn edges_len(&self) -> usize;

    fn edges_len_window(&self, t_start: i64, t_end: i64) -> usize;

    fn has_edge_ref(&self, src: VertexRef, dst: VertexRef) -> bool;

    fn has_edge_ref_window(&self, src: VertexRef, dst: VertexRef, t_start: i64, t_end: i64)
        -> bool;

    fn has_vertex_ref(&self, v: VertexRef) -> bool;

    fn has_vertex_ref_window(&self, v: VertexRef, t_start: i64, t_end: i64) -> bool;

    fn degree(&self, v: VertexRef, d: Direction) -> usize;

    fn degree_window(&self, v: VertexRef, t_start: i64, t_end: i64, d: Direction) -> usize;

    fn vertex_ref(&self, v: u64) -> Option<VertexRef>;

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexRef>;

    fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send>;

    fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send>;

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn vertex_refs_shard(&self, shard: usize) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn vertex_refs_window_shard(
        &self,
        shard: usize,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn edge_ref(&self, src: VertexRef, dst: VertexRef) -> Option<EdgeRef>;

    fn edge_ref_window(
        &self,
        src: VertexRef,
        dst: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> Option<EdgeRef>;

    fn edge_refs(&self) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    fn vertex_edges(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    fn vertex_edges_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    fn vertex_edges_window_t(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeRef> + Send>;

    fn neighbours(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn neighbours_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexRef> + Send>;

    fn neighbours_ids(&self, v: VertexRef, d: Direction) -> Box<dyn Iterator<Item = u64> + Send>;

    fn neighbours_ids_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send>;

    fn static_vertex_prop(&self, v: VertexRef, name: String) -> Option<Prop>;

    fn static_vertex_prop_keys(&self, v: VertexRef) -> Vec<String>;

    fn temporal_vertex_prop_vec(&self, v: VertexRef, name: String) -> Vec<(i64, Prop)>;

    fn temporal_vertex_prop_vec_window(
        &self,
        v: VertexRef,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)>;

    fn temporal_vertex_props(&self, v: VertexRef) -> HashMap<String, Vec<(i64, Prop)>>;

    fn temporal_vertex_props_window(
        &self,
        v: VertexRef,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>>;

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

pub trait ParIterGraphOps {
    fn vertices_par_map<O, F>(&self, f: F) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexRef) -> O + Send + Sync + Copy;

    fn vertices_par_fold<S, F, F2>(&self, f: F, agg: F2) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexRef) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy;

    fn vertices_window_par_map<O, F>(
        &self,
        t_start: i64,
        t_end: i64,
        f: F,
    ) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexRef) -> O + Send + Sync + Copy;

    fn vertices_window_par_fold<S, F, F2>(
        &self,
        t_start: i64,
        t_end: i64,
        f: F,
        agg: F2,
    ) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexRef) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy;
}

impl<G: GraphViewInternalOps + Send + Sync> ParIterGraphOps for G {
    fn vertices_par_map<O, F>(&self, f: F) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexRef) -> O + Send + Sync + Copy,
    {
        let (tx, rx) = flume::unbounded();

        let arc_tx = Arc::new(tx);
        (0..self.num_shards())
            .into_par_iter()
            .flat_map(|shard_id| self.vertices_shard(shard_id).par_bridge().map(f))
            .for_each(move |o| {
                arc_tx.send(o).unwrap();
            });

        Box::new(rx.into_iter())
    }

    fn vertices_par_fold<S, F, F2>(&self, f: F, agg: F2) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexRef) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy,
    {
        (0..self.num_shards())
            .into_par_iter()
            .flat_map(|shard_id| {
                self.vertices_shard(shard_id)
                    .par_bridge()
                    .map(f)
                    .reduce_with(agg)
            })
            .reduce_with(agg)
    }

    fn vertices_window_par_map<O, F>(
        &self,
        t_start: i64,
        t_end: i64,
        f: F,
    ) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexRef) -> O + Send + Sync + Copy,
    {
        let (tx, rx) = flume::unbounded();

        let arc_tx = Arc::new(tx);
        (0..self.num_shards())
            .into_par_iter()
            .flat_map(|shard_id| {
                self.vertices_shard_window(shard_id, t_start, t_end)
                    .par_bridge()
                    .map(f)
            })
            .for_each(move |o| {
                arc_tx.send(o).unwrap();
            });

        Box::new(rx.into_iter())
    }

    fn vertices_window_par_fold<S, F, F2>(
        &self,
        t_start: i64,
        t_end: i64,
        f: F,
        agg: F2,
    ) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexRef) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy,
    {
        (0..self.num_shards())
            .into_par_iter()
            .flat_map(|shard| {
                self.vertices_shard_window(shard, t_start, t_end)
                    .par_bridge()
                    .map(f)
                    .reduce_with(agg)
            })
            .reduce_with(agg)
    }
}
