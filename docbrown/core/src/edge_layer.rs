use itertools::chain;
use serde::{Deserialize, Serialize};
use std::ops::Range;

use crate::Prop;
use crate::adj::Adj;
use crate::props::Props;
use crate::tadjset::AdjEdge;

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct EdgeLayer {
    num_edges: usize,

    // Vector of adjacency lists
    pub(crate) adj_lists: Vec<Adj>,
    pub(crate) edge_props: Props, // TODO: rename to props
}

impl Default for EdgeLayer {
    fn default() -> Self {
        Self {
            num_edges: 1, // TODO: add big comment here
            adj_lists: Default::default(),
            edge_props: Default::default(),
        }
    }
}

// Ingestion
impl EdgeLayer {
    pub(crate) fn add_edge_with_props(
        &mut self,
        t: i64,
        src: u64,
        dst: u64,
        src_pid: usize,
        dst_pid: usize,
        props: &Vec<(String, Prop)>,
    ) {
        let src_edge_meta_id = self.link_outbound_edge(t, src, src_pid, dst_pid, false);
        let dst_edge_meta_id = self.link_inbound_edge(t, dst, src_pid, dst_pid, false);

        if src_edge_meta_id != dst_edge_meta_id {
            panic!(
                "Failure on {src} -> {dst} at time: {t} {src_edge_meta_id} != {dst_edge_meta_id}"
            );
        }

        self.edge_props.upsert_temporal_props(t, src_edge_meta_id, props);
        self.num_edges += 1; // FIXME: we have this in three different places, prone to errors!
    }

    pub(crate) fn add_edge_remote_out(
        &mut self,
        t: i64,
        src: u64, // we are on the source shard
        dst: u64,
        src_pid: usize,
        props: &Vec<(String, Prop)>,
    ) {
        let src_edge_meta_id =
            self.link_outbound_edge(t, src, src_pid, dst.try_into().unwrap(), true);

        self.edge_props.upsert_temporal_props(t, src_edge_meta_id, props);
        self.num_edges += 1;
    }

    pub(crate) fn add_edge_remote_into(
        &mut self,
        t: i64,
        src: u64,
        dst: u64, // we are on the destination shard
        dst_pid: usize,
        props: &Vec<(String, Prop)>,
    ) {
        let dst_edge_meta_id =
            self.link_inbound_edge(t, dst, src.try_into().unwrap(), dst_pid, true);

        self.edge_props.upsert_temporal_props(t, dst_edge_meta_id, props);
        self.num_edges += 1;
    }
}

// ACCESS:
impl EdgeLayer {
    pub(crate) fn out_edges_len(&self) -> usize {
        self.adj_lists.iter().map(|adj| adj.out_edges_len()).sum()
    }

    // TODO reuse function to return edge
    pub(crate) fn has_local_edge(&self, src_pid: usize, dst_pid: usize) -> bool {
        match &self.adj_lists[src_pid] {
            Adj::Solo(_) => false,
            Adj::List { out, .. } => out.find(dst_pid).is_some()
        }
    }

    pub(crate) fn has_local_edge_window(&self, src_pid: usize, dst_pid: usize, w: &Range<i64>) -> bool {
        match &self.adj_lists[src_pid] {
            Adj::Solo(_) => false,
            Adj::List { out, .. } => out.find_window(dst_pid, w).is_some()
        }
    }

    pub(crate) fn has_remote_edge(&self, src_pid: usize, dst: u64) -> bool {
        match &self.adj_lists[src_pid] {
            Adj::Solo(_) => false,
            Adj::List { remote_out, .. } => remote_out.find(dst as usize).is_some()
        }
    }

    pub(crate) fn has_remote_edge_window(&self, src_pid: usize, dst: u64, w: &Range<i64>) -> bool {
        match &self.adj_lists[src_pid] {
            Adj::Solo(_) => false,
            Adj::List { remote_out, .. } => remote_out.find_window(dst as usize, w).is_some()
        }
    }
}

impl EdgeLayer {
    fn link_inbound_edge(
        &mut self,
        t: i64,
        dst_gid: u64,
        src: usize, // may or may not be physical id depending on remote_edge flag
        dst_pid: usize,
        remote_edge: bool,
    ) -> usize {
        match &mut self.adj_lists[dst_pid] {
            entry @ Adj::Solo(_) => {
                let edge_id = self.num_edges;

                let edge = AdjEdge::new(edge_id, !remote_edge);

                *entry = Adj::new_into(dst_gid, src, t, edge);

                edge_id
            }
            Adj::List {
                into, remote_into, ..
            } => {
                let list = if remote_edge { remote_into } else { into };
                let edge_id: usize = list
                    .find(src)
                    .map(|e| e.edge_id())
                    .unwrap_or(self.num_edges);

                list.push(t, src, AdjEdge::new(edge_id, !remote_edge)); // idempotent
                edge_id
            }
        }
    }

    fn link_outbound_edge(
        &mut self,
        t: i64,
        src_gid: u64,
        src_pid: usize,
        dst: usize, // may or may not pe physical id depending on remote_edge flag
        remote_edge: bool,
    ) -> usize {
        match &mut self.adj_lists[src_pid] {
            entry @ Adj::Solo(_) => {
                let edge_id = self.num_edges;

                let edge = AdjEdge::new(edge_id, !remote_edge);

                *entry = Adj::new_out(src_gid, dst, t, edge);

                edge_id
            }
            Adj::List {
                out, remote_out, ..
            } => {
                let list = if remote_edge { remote_out } else { out };
                let edge_id: usize = list
                    .find(dst)
                    .map(|e| e.edge_id())
                    .unwrap_or(self.num_edges);

                list.push(t, dst, AdjEdge::new(edge_id, !remote_edge));
                edge_id
            }
        }
    }
}