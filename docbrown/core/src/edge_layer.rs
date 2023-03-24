use itertools::chain;
use serde::{Deserialize, Serialize};
use std::ops::Range;

use crate::{Direction, Prop};
use crate::adj::Adj;
use crate::props::Props;
use crate::tadjset::AdjEdge;
use crate::tgraph::EdgeRef;

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

// INGESTION:
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

// INGESTION HELPERS:
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

// SINGLE EDGE ACCESS:
impl EdgeLayer {
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

    // try to merge the next four functions together
    pub(crate) fn local_edge(&self, src: u64, dst: u64, src_pid: usize, dst_pid: usize) -> Option<EdgeRef> {
        match &self.adj_lists[src_pid] {
            Adj::Solo(_) => None,
            Adj::List { out, .. } => {
                let e = out.find(dst_pid)?;
                Some(EdgeRef {
                    edge_id: e.edge_id(),
                    src_g_id: src,
                    dst_g_id: dst,
                    src_id: src_pid,
                    dst_id: dst_pid,
                    time: None,
                    is_remote: false, // TODO: check if we still need AdjEdge.is_local()
                })
            }
        }
    }

    pub(crate) fn local_edge_window(&self, src: u64, dst: u64, src_pid: usize, dst_pid: usize, w: &Range<i64>) -> Option<EdgeRef> {
        match &self.adj_lists[src_pid] {
            Adj::Solo(_) => None,
            Adj::List { out, .. } => {
                let e = out.find_window(dst_pid, w)?;
                Some(EdgeRef {
                    edge_id: e.edge_id(),
                    src_g_id: src,
                    dst_g_id: dst,
                    src_id: src_pid,
                    dst_id: dst_pid,
                    time: None,
                    is_remote: false, // TODO: check if we still need AdjEdge.is_local()
                })
            }
        }
    }

    pub(crate) fn remote_edge(&self, src: u64, dst: u64, src_pid: usize) -> Option<EdgeRef> {
        match &self.adj_lists[src_pid] {
            Adj::Solo(_) => None,
            Adj::List { remote_out, .. } => {
                let e = remote_out.find(dst as usize)?;
                Some(EdgeRef {
                    edge_id: e.edge_id(),
                    src_g_id: src,
                    dst_g_id: dst,
                    src_id: src_pid,
                    dst_id: dst as usize,
                    time: None,
                    is_remote: true,
                })
            }
        }
    }

    pub(crate) fn remote_edge_window(&self, src: u64, dst: u64, src_pid: usize, w: &Range<i64>) -> Option<EdgeRef> {
        match &self.adj_lists[src_pid] {
            Adj::Solo(_) => None,
            Adj::List { remote_out, .. } => {
                let e = remote_out.find_window(dst as usize, w)?;
                Some(EdgeRef {
                    edge_id: e.edge_id(),
                    src_g_id: src,
                    dst_g_id: dst,
                    src_id: src_pid,
                    dst_id: dst as usize,
                    time: None,
                    is_remote: true,
                })
            }
        }
    }
}

// MULTIPLE EDGE ACCESS:
impl EdgeLayer {
    pub(crate) fn out_edges_len(&self) -> usize {
        self.adj_lists.iter().map(|adj| adj.out_edges_len()).sum()
    }

    // FIXME: all the functions using global ID need to be changed to use the physical ID instead
    pub(crate) fn vertex_edges(
        &self,
        v: u64,
        v_pid: usize,
        d: Direction,
    ) -> Box<dyn Iterator<Item=EdgeRef> + Send + '_>
        where
            Self: Sized,
    {
        match d {
            Direction::OUT => Box::new(self.edges_iter(v_pid, d).map(move |(dst, e)| EdgeRef {
                edge_id: e.edge_id(),
                src_g_id: v,
                dst_g_id: self.v_g_id(*dst, e),
                src_id: v_pid,
                dst_id: *dst,
                time: None,
                is_remote: !e.is_local(),
            })),
            Direction::IN => Box::new(self.edges_iter(v_pid, d).map(move |(dst, e)| EdgeRef {
                edge_id: e.edge_id(),
                src_g_id: self.v_g_id(*dst, e),
                dst_g_id: v,
                src_id: *dst,
                dst_id: v_pid,
                time: None,
                is_remote: !e.is_local(),
            })),
            Direction::BOTH => Box::new(itertools::chain!(
                self.vertex_edges(v, v_pid, Direction::IN),
                self.vertex_edges(v, v_pid, Direction::OUT)
            )),
        }
    }

    pub(crate) fn vertex_edges_window(
        &self,
        v: u64,
        v_pid: usize,
        w: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item=EdgeRef> + Send + '_>
        where
            Self: Sized,
    {
        match d {
            Direction::OUT => {
                Box::new(
                    self.edges_iter_window(v_pid, w, d)
                        .map(move |(dst, e)| EdgeRef {
                            edge_id: e.edge_id(),
                            src_g_id: v,
                            dst_g_id: self.v_g_id(dst, e),
                            src_id: v_pid,
                            dst_id: dst,
                            time: None,
                            is_remote: !e.is_local(),
                        }),
                )
            }
            Direction::IN => {
                Box::new(
                    self.edges_iter_window(v_pid, w, d)
                        .map(move |(dst, e)| EdgeRef {
                            edge_id: e.edge_id(),
                            src_g_id: self.v_g_id(dst, e),
                            dst_g_id: v,
                            src_id: dst,
                            dst_id: v_pid,
                            time: None,
                            is_remote: !e.is_local(),
                        }),
                )
            }
            Direction::BOTH => Box::new(itertools::chain!(
                self.vertex_edges_window(v, v_pid, w, Direction::IN),
                self.vertex_edges_window(v, v_pid, w, Direction::OUT)
            )),
        }
    }

    pub(crate) fn vertex_edges_window_t(
        &self,
        v: u64,
        v_pid: usize,
        w: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item=EdgeRef> + Send + '_> {
        match d {
            Direction::OUT => Box::new(self.edges_iter_window_t(v_pid, w, d).map(
                move |(dst, t, e)| EdgeRef {
                    edge_id: e.edge_id(),
                    src_g_id: v,
                    dst_g_id: self.v_g_id(dst, e),
                    src_id: v_pid,
                    dst_id: dst,
                    time: Some(t),
                    is_remote: !e.is_local(),
                },
            )),
            Direction::IN => Box::new(self.edges_iter_window_t(v_pid, w, d).map(
                move |(dst, t, e)| EdgeRef {
                    edge_id: e.edge_id(),
                    src_g_id: self.v_g_id(dst, e),
                    dst_g_id: v,
                    src_id: dst,
                    dst_id: v_pid,
                    time: Some(t),
                    is_remote: !e.is_local(),
                },
            )),
            Direction::BOTH => Box::new(itertools::chain!(
                self.vertex_edges_window_t(v, v_pid, w, Direction::IN),
                self.vertex_edges_window_t(v, v_pid, w, Direction::OUT)
            )),
        }
    }
}

// MULTIPLE EDGE ACCES HELPERS:
impl EdgeLayer {
    fn edges_iter( // TODO: change back to private if appropriate
        &self,
        vertex_pid: usize,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (&usize, AdjEdge)> + Send + '_> {
        match &self.adj_lists[vertex_pid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => {
                match d {
                    Direction::OUT => Box::new(itertools::chain!(out.iter(), remote_out.iter())),
                    Direction::IN => Box::new(itertools::chain!(into.iter(), remote_into.iter())),
                    // This piece of code is only for the sake of symmetry. Not really used.
                    _ => Box::new(itertools::chain!(
                        out.iter(),
                        into.iter(),
                        remote_out.iter(),
                        remote_into.iter()
                    )),
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    fn edges_iter_window( // TODO: change back to private if appropriate
        &self,
        vertex_pid: usize,
        r: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (usize, AdjEdge)> + Send + '_> {
        match &self.adj_lists[vertex_pid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => {
                match d {
                    Direction::OUT => Box::new(itertools::chain!(
                        out.iter_window(r),
                        remote_out.iter_window(r)
                    )),
                    Direction::IN => Box::new(itertools::chain!(
                        into.iter_window(r),
                        remote_into.iter_window(r),
                    )),
                    // This piece of code is only for the sake of symmetry. Not really used.
                    _ => Box::new(itertools::chain!(
                        out.iter_window(r),
                        into.iter_window(r),
                        remote_out.iter_window(r),
                        remote_into.iter_window(r)
                    )),
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    fn edges_iter_window_t( // TODO: change back to private if appropriate
        &self,
        vertex_pid: usize,
        window: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (usize, i64, AdjEdge)> + Send + '_> {
        match &self.adj_lists[vertex_pid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => {
                match d {
                    Direction::OUT => Box::new(itertools::chain!(
                        out.iter_window_t(window),
                        remote_out.iter_window_t(window)
                    )),
                    Direction::IN => Box::new(itertools::chain!(
                        into.iter_window_t(window),
                        remote_into.iter_window_t(window),
                    )),
                    // This piece of code is only for the sake of symmetry. Not really used.
                    _ => Box::new(itertools::chain!(
                        out.iter_window_t(window),
                        into.iter_window_t(window),
                        remote_out.iter_window_t(window),
                        remote_into.iter_window_t(window)
                    )),
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    fn v_g_id(&self, vertex_pid: usize, e: AdjEdge) -> u64 {
        if e.is_local() {
            *self.adj_lists[vertex_pid].logical()
        } else {
            vertex_pid.try_into().unwrap()
        }
    }
}

