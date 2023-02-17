use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::adj::Adj;
use crate::graphview::{
    EdgeIterator, GraphViewInternals, NeighboursIterator, PropertyHistory, VertexIterator,
};
use crate::props::Props;
use crate::vertexview::{VertexPointer, VertexView};
use crate::Prop;
use crate::{bitset::BitSet, tadjset::AdjEdge, Direction};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TemporalGraph {
    // Maps global (logical) id to the local (physical) id which is an index to the adjacency list vector
    logical_to_physical: HashMap<u64, usize>,

    // Vector of adjacency lists
    pub(crate) adj_lists: Vec<Adj>,

    // Time index pointing at the index against adjacency lists.
    index: BTreeMap<i64, BitSet>,

    // Properties abstraction for both vertices and edges
    pub(crate) props: Props,
}

impl Default for TemporalGraph {
    fn default() -> Self {
        Self {
            logical_to_physical: Default::default(),
            adj_lists: Default::default(),
            index: Default::default(),
            props: Default::default(),
        }
    }
}

impl GraphViewInternals for TemporalGraph {
    fn local_n_vertices(&self) -> usize {
        self.logical_to_physical.len()
    }

    fn local_n_edges(&self, direction: Direction) -> usize {
        match direction {
            Direction::OUT => self.adj_lists.iter().map(|adj| adj.out_edges_len()).sum(),
            Direction::IN => self.adj_lists.iter().map(|adj| adj.in_edges_len()).sum(),
            Direction::BOTH => {
                self.local_n_edges(Direction::OUT) + self.local_n_edges(Direction::IN)
            }
        }
    }

    fn local_n_vertices_window(&self, w: Range<i64>) -> usize {
        self.iter_local_vertices_window(w).count()
    }

    fn local_n_edges_window(&self, w: Range<i64>, direction: Direction) -> usize {
        match direction {
            Direction::OUT => self
                .adj_lists
                .iter()
                .map(|adj| adj.out_edges_len_window(w.clone()))
                .sum(),
            Direction::IN => self
                .adj_lists
                .iter()
                .map(|adj| adj.in_edges_len_window(w.clone()))
                .sum(),
            Direction::BOTH => {
                self.local_n_edges_window(w.clone(), Direction::OUT)
                    + self.local_n_edges_window(w.clone(), Direction::IN)
            }
        }
    }

    fn local_vertex(&self, gid: u64) -> Option<VertexView<Self>> {
        self.logical_to_physical.get(&gid).map(|pid| VertexView {
            gid: gid,
            pid: *pid,
            g: self,
            w: None,
        })
    }

    fn local_vertex_window(&self, gid: u64, w: Range<i64>) -> Option<VertexView<Self>> {
        if let Some(v_id) = self.logical_to_physical.get(&gid) {
            if self.index.range(w.clone()).any(|(_, bs)| bs.contains(v_id)) {
                Some(VertexView {
                    gid: gid,
                    pid: *v_id,
                    g: self,
                    w: Some(w.clone()),
                })
            } else {
                None
            }
        } else {
            None
        }
    }

    fn local_contains_vertex(&self, gid: u64) -> bool {
        self.logical_to_physical.contains_key(&gid)
    }

    fn local_contains_vertex_window(&self, gid: u64, w: Range<i64>) -> bool {
        if let Some(v_id) = self.logical_to_physical.get(&gid) {
            self.index.range(w.clone()).any(|(_, bs)| bs.contains(v_id))
        } else {
            false
        }
    }

    fn iter_local_vertices(&self) -> VertexIterator<Self> {
        Box::new(
            self.adj_lists
                .iter()
                .enumerate()
                .map(|(pid, v)| VertexView {
                    gid: *v.logical(),
                    pid,
                    g: self,
                    w: None,
                }),
        )
    }

    fn iter_local_vertices_window(&self, window: Range<i64>) -> VertexIterator<Self> {
        let iter = self
            .index
            .range(window.clone())
            .map(|(_, vs)| vs.iter())
            .kmerge()
            .dedup()
            .map(move |pid| match self.adj_lists[pid] {
                Adj::Solo(lid) => VertexView {
                    gid: lid,
                    pid,
                    g: self,
                    w: Some(window.clone()),
                },
                Adj::List { logical, .. } => VertexView {
                    gid: logical,
                    pid,
                    g: self,
                    w: Some(window.clone()),
                },
            });
        Box::new(iter)
    }

    fn degree(&self, vertex: VertexPointer, direction: Direction) -> usize {
        match &self.adj_lists[vertex.pid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => match direction {
                Direction::OUT => {
                    if let Some(w) = &vertex.w {
                        out.len_window(w) + remote_out.len_window(w)
                    } else {
                        out.len() + remote_out.len()
                    }
                }
                Direction::IN => {
                    if let Some(w) = &vertex.w {
                        into.len_window(w) + remote_into.len_window(w)
                    } else {
                        into.len() + remote_into.len()
                    }
                }
                Direction::BOTH => {
                    if let Some(w) = &vertex.w {
                        into.union_iter_window(out, w)
                            .chain(remote_into.union_iter_window(remote_out, w))
                            .count()
                    } else {
                        into.union_iter(out)
                            .chain(remote_into.union_iter(remote_out))
                            .count()
                    }
                }
            },
            _ => 0,
        }
    }

    fn neighbours(&self, vertex: VertexPointer, direction: Direction) -> NeighboursIterator<Self> {
        match &self.adj_lists[vertex.pid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => {
                if let Some(window) = &vertex.w {
                    match direction {
                        // FIXME: Should probably have local and remote VertexView as an enum to make this easier
                        Direction::OUT => Box::new(
                            out.iter_window(window)
                                .map(|(id, _)| VertexView {
                                    g: self,
                                    gid: *self.adj_lists[id].logical(),
                                    pid: id,
                                    w: None,
                                })
                                .chain(remote_out.iter_window(window).map(|(id, _)| VertexView {
                                    g: self,
                                    gid: id.try_into().unwrap(),
                                    pid: id,
                                    w: None,
                                })),
                        ),
                        Direction::IN => Box::new(
                            into.iter_window(window)
                                .map(|(id, _)| VertexView {
                                    g: self,
                                    gid: *self.adj_lists[id].logical(),
                                    pid: id,
                                    w: None,
                                })
                                .chain(remote_into.iter_window(window).map(|(id, _)| VertexView {
                                    g: self,
                                    gid: id.try_into().unwrap(),
                                    pid: id,
                                    w: None,
                                })),
                        ),
                        Direction::BOTH => Box::new(
                            GraphViewInternals::neighbours(self, vertex.clone(), Direction::IN)
                                .chain(GraphViewInternals::neighbours(self, vertex, Direction::OUT))
                                .unique_by(|v| v.gid),
                        ),
                    }
                } else {
                    match direction {
                        // FIXME: Should probably have local and remote VertexView as an enum to make this easier
                        Direction::OUT => Box::new(
                            out.iter()
                                .map(|(id, _)| VertexView {
                                    g: self,
                                    gid: *self.adj_lists[*id].logical(),
                                    pid: *id,
                                    w: None,
                                })
                                .chain(remote_out.iter().map(|(id, _)| VertexView {
                                    g: self,
                                    gid: (*id).try_into().unwrap(),
                                    pid: *id,
                                    w: None,
                                })),
                        ),
                        Direction::IN => Box::new(
                            into.iter()
                                .map(|(id, _)| VertexView {
                                    g: self,
                                    gid: *self.adj_lists[*id].logical(),
                                    pid: *id,
                                    w: None,
                                })
                                .chain(remote_into.iter().map(|(id, _)| VertexView {
                                    g: self,
                                    gid: (*id).try_into().unwrap(),
                                    pid: *id,
                                    w: None,
                                })),
                        ),
                        Direction::BOTH => Box::new(
                            GraphViewInternals::neighbours(self, vertex.clone(), Direction::IN)
                                .chain(GraphViewInternals::neighbours(self, vertex, Direction::OUT))
                                .unique_by(|v| v.gid),
                        ),
                    }
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    fn edges(&self, vertex: VertexPointer, direction: Direction) -> EdgeIterator<Self> {
        let v_pid = vertex.pid;
        if let Some(w) = &vertex.w {
            match direction {
                Direction::OUT => Box::new(
                    self.internal_neighbours_iter_window(v_pid, w, direction)
                        .map(move |(v, e_meta)| EdgeView {
                            src_id: v_pid,
                            dst_id: v,
                            t: None,
                            g: self,
                            e_meta,
                        }),
                ),
                Direction::IN => Box::new(
                    self.internal_neighbours_iter_window(v_pid, w, direction)
                        .map(move |(v, e_meta)| EdgeView {
                            src_id: v,
                            dst_id: v_pid,
                            t: None,
                            g: self,
                            e_meta,
                        }),
                ),
                Direction::BOTH => Box::new(
                    self.edges(vertex.clone(), Direction::IN)
                        .chain(self.edges(vertex, Direction::OUT)),
                ),
            }
        } else {
            let v_pid = vertex.pid;
            match direction {
                Direction::OUT => Box::new(self.internal_neighbours_iter(v_pid, direction).map(
                    move |(v, e_meta)| EdgeView {
                        src_id: v_pid,
                        dst_id: *v,
                        t: None,
                        g: self,
                        e_meta,
                    },
                )),
                Direction::IN => Box::new(self.internal_neighbours_iter(v_pid, direction).map(
                    move |(v, e_meta)| EdgeView {
                        src_id: *v,
                        dst_id: v_pid,
                        t: None,
                        g: self,
                        e_meta,
                    },
                )),
                Direction::BOTH => Box::new(
                    self.edges(vertex.clone(), Direction::IN)
                        .chain(self.edges(vertex, Direction::OUT)),
                ),
            }
        }
    }

    fn property_history<'a>(
        &'a self,
        vertex: VertexPointer,
        name: &'a str,
    ) -> Option<PropertyHistory<'a>> {
        let index = self.logical_to_physical.get(&vertex.gid)?;
        let meta = self.props.vertex_meta.get(*index)?;
        let prop_id = *self.props.prop_ids.get(name)?;
        match vertex.w {
            Some(r) => Some(
                meta.iter_window(prop_id, r.clone())
                    .map(|(t, prop)| (*t, prop))
                    .collect(),
            ),
            None => Some(
                meta.iter_window(prop_id, i64::MIN..i64::MAX)
                    .map(|(t, prop)| (t.clone(), prop))
                    .collect(),
            ),
        }
    }
}

// FIXME: Implement these using the MutableGraph trait so we have a consistent format everywhere?
impl TemporalGraph {
    // FIXME: would be nice if these return the vertex and edge views for the added item?
    pub(crate) fn add_vertex(&mut self, v: u64, t: i64) {
        self.add_vertex_with_props(v, t, &vec![])
    }

    pub(crate) fn add_vertex_with_props(&mut self, v: u64, t: i64, props: &Vec<(String, Prop)>) {
        let index = match self.logical_to_physical.get(&v) {
            None => {
                let physical_id: usize = self.adj_lists.len();
                self.adj_lists.push(Adj::Solo(v));

                self.logical_to_physical.insert(v, physical_id);

                self.index
                    .entry(t)
                    .and_modify(|set| {
                        set.push(physical_id);
                    })
                    .or_insert_with(|| BitSet::one(physical_id));
                physical_id
            }
            Some(pid) => {
                self.index
                    .entry(t)
                    .and_modify(|set| {
                        set.push(*pid);
                    })
                    .or_insert_with(|| BitSet::one(*pid));
                *pid
            }
        };

        self.props.upsert_vertex_props(index, t, props);
    }

    pub(crate) fn add_edge(&mut self, src: u64, dst: u64, t: i64) {
        self.add_edge_with_props(src, dst, t, &vec![])
    }

    pub(crate) fn add_edge_with_props(
        &mut self,
        src: u64,
        dst: u64,
        t: i64,
        props: &Vec<(String, Prop)>,
    ) {
        // mark the times of the vertices at t
        self.add_vertex(src, t);
        self.add_vertex(dst, t);

        let src_pid = self.logical_to_physical[&src];
        let dst_pid = self.logical_to_physical[&dst];

        let src_edge_meta_id = self.link_outbound_edge(src, t, src_pid, dst_pid, false);
        let dst_edge_meta_id = self.link_inbound_edge(dst, t, src_pid, dst_pid, false);

        if src_edge_meta_id != dst_edge_meta_id {
            panic!(
                "Failure on {src} -> {dst} at time: {t} {src_edge_meta_id} != {dst_edge_meta_id}"
            );
        }

        self.props.upsert_edge_props(src_edge_meta_id, t, props)
    }

    pub(crate) fn add_edge_remote_out(
        &mut self,
        src: u64, // we are on the source shard
        dst: u64,
        t: i64,
        props: &Vec<(String, Prop)>,
    ) {
        self.add_vertex(src, t);
        let src_pid = self.logical_to_physical[&src];
        let src_edge_meta_id =
            self.link_outbound_edge(src, t, src_pid, dst.try_into().unwrap(), true);

        self.props.upsert_edge_props(src_edge_meta_id, t, props)
    }

    pub(crate) fn add_edge_remote_into(
        &mut self,
        src: u64,
        dst: u64, // we are on the destination shard
        t: i64,
        props: &Vec<(String, Prop)>,
    ) {
        self.add_vertex(dst, t);

        let dst_pid = self.logical_to_physical[&dst];

        let dst_edge_meta_id =
            self.link_inbound_edge(dst, t, src.try_into().unwrap(), dst_pid, true);

        self.props.upsert_edge_props(dst_edge_meta_id, t, props)
    }

    /// FIXME: Add explode to EdgeView and clean up edge view to be useable and clear about the different ids
    pub(crate) fn neighbours_window_t(
        &self,
        v: u64,
        r: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + '_> {
        let v_pid = self.logical_to_physical[&v];

        match d {
            Direction::OUT => Box::new(self.internal_neighbours_iter_window_t(v_pid, r, d).map(
                move |(v, t, e_meta)| EdgeView {
                    src_id: v_pid,
                    dst_id: v,
                    t: Some(t),
                    g: self,
                    e_meta,
                },
            )),
            Direction::IN => Box::new(self.internal_neighbours_iter_window_t(v_pid, r, d).map(
                move |(v, t, e_meta)| EdgeView {
                    src_id: v,
                    dst_id: v_pid,
                    t: Some(t),
                    g: self,
                    e_meta,
                },
            )),
            Direction::BOTH => Box::new(itertools::chain!(
                self.neighbours_window_t(v, r, Direction::IN),
                self.neighbours_window_t(v, r, Direction::OUT)
            )),
        }
    }
}

impl TemporalGraph {
    fn link_inbound_edge(
        &mut self,
        dst_gid: u64,
        t: i64,
        src: usize, // may or may not be physical id depending on remote_edge flag
        dst_pid: usize,
        remote_edge: bool,
    ) -> usize {
        match &mut self.adj_lists[dst_pid] {
            entry @ Adj::Solo(_) => {
                let edge_id = self.props.get_next_available_edge_id();

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
                    .map(|e| e.edge_meta_id())
                    .unwrap_or(self.props.get_next_available_edge_id());

                list.push(t, src, AdjEdge::new(edge_id, !remote_edge)); // idempotent
                edge_id
            }
        }
    }

    fn link_outbound_edge(
        &mut self,
        src_gid: u64,
        t: i64,
        src_pid: usize,
        dst: usize, // may or may not pe physical id depending on remote_edge flag
        remote_edge: bool,
    ) -> usize {
        match &mut self.adj_lists[src_pid] {
            entry @ Adj::Solo(_) => {
                let edge_id = self.props.get_next_available_edge_id();

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
                    .map(|e| e.edge_meta_id())
                    .unwrap_or(self.props.get_next_available_edge_id());

                list.push(t, dst, AdjEdge::new(edge_id, !remote_edge));
                edge_id
            }
        }
    }

    fn internal_neighbours_iter(
        &self,
        vid: usize,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (&usize, AdjEdge)> + '_> {
        match &self.adj_lists[vid] {
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

    fn internal_neighbours_iter_window(
        &self,
        vid: usize,
        window: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (usize, AdjEdge)> + '_> {
        match &self.adj_lists[vid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => {
                match d {
                    Direction::OUT => Box::new(itertools::chain!(
                        out.iter_window(window),
                        remote_out.iter_window(window)
                    )),
                    Direction::IN => Box::new(itertools::chain!(
                        into.iter_window(window),
                        remote_into.iter_window(window),
                    )),
                    // This piece of code is only for the sake of symmetry. Not really used.
                    _ => Box::new(itertools::chain!(
                        out.iter_window(window),
                        into.iter_window(window),
                        remote_out.iter_window(window),
                        remote_into.iter_window(window)
                    )),
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    fn internal_neighbours_iter_window_t(
        &self,
        vid: usize,
        window: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (usize, i64, AdjEdge)> + '_> {
        match &self.adj_lists[vid] {
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
}

pub struct EdgeView<'a, G: Sized> {
    pub(crate) src_id: usize,
    pub(crate) dst_id: usize,
    g: &'a G,
    t: Option<i64>,
    e_meta: AdjEdge,
}

impl<'a, G> EdgeView<'a, G>
where
    G: GraphViewInternals,
{
    pub fn as_view_of<'b, GG>(&self, graph: &'b GG) -> EdgeView<'b, GG>
    where
        GG: GraphViewInternals,
    {
        EdgeView {
            g: graph,
            src_id: self.src_id,
            dst_id: self.dst_id,
            t: self.t,
            e_meta: self.e_meta,
        }
    }
}

impl<'a> EdgeView<'a, TemporalGraph> {
    pub fn global_src(&self) -> u64 {
        if self.e_meta.is_local() {
            *self.g.adj_lists[self.src_id].logical()
        } else {
            self.src_id.try_into().unwrap()
        }
    }

    pub fn global_dst(&self) -> u64 {
        if self.e_meta.is_local() {
            *self.g.adj_lists[self.dst_id].logical()
        } else {
            self.dst_id.try_into().unwrap()
        }
    }

    pub fn time(&self) -> Option<i64> {
        self.t
    }

    pub fn is_remote(&self) -> bool {
        !self.e_meta.is_local()
    }

    pub fn props(&self, name: &'a str) -> Option<Box<dyn Iterator<Item = (&'a i64, Prop)> + 'a>> {
        // find the id of the property
        let prop_id = self.g.props.prop_ids.get(name)?;
        Some(self.g.props.edge_meta[self.e_meta.edge_meta_id()].iter(*prop_id))
    }

    pub fn props_window(
        &self,
        name: &'a str,
        r: Range<i64>,
    ) -> Option<Box<dyn Iterator<Item = (&'a i64, Prop)> + 'a>> {
        // find the id of the property
        let prop_id = self.g.props.prop_ids.get(name)?;
        Some(self.g.props.edge_meta[self.e_meta.edge_meta_id()].iter_window(*prop_id, r))
    }
}

#[cfg(test)]
extern crate quickcheck;

#[cfg(test)]
mod graph_test {
    use std::path::PathBuf;

    use crate::vertexview::VertexViewMethods;
    use csv::StringRecord;

    use crate::utils;

    use super::*;

    #[test]
    fn add_vertex_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);

        assert!(g.local_contains_vertex(9));
        assert!(g.local_contains_vertex_window(9, 1..15));
        assert_eq!(
            g.iter_local_vertices()
                .map(|v| v.id())
                .collect::<Vec<u64>>(),
            vec![9]
        );
        assert_eq!(g.props.vertex_meta.get(2), None);
    }

    #[test]
    fn add_vertices_with_1_property() {
        let mut g = TemporalGraph::default();

        let v_id = 1;
        let ts = 1;
        g.add_vertex_with_props(v_id, ts, &vec![("type".into(), Prop::Str("wallet".into()))]);

        assert!(g.local_contains_vertex(v_id));
        assert!(g.local_contains_vertex_window(v_id, 1..15));
        assert_eq!(
            g.iter_local_vertices()
                .map(|v| v.id())
                .collect::<Vec<u64>>(),
            vec![v_id]
        );

        let res = g
            .iter_local_vertices()
            .flat_map(|v| v.property_history("type"))
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(res, vec![(1, Prop::Str("wallet".into()))]);
    }

    #[test]
    fn add_vertices_with_multiple_properties() {
        let mut g = TemporalGraph::default();

        g.add_vertex_with_props(
            1,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(0)),
            ],
        );

        let res = g
            .iter_local_vertices()
            .flat_map(|v| {
                let type_ = v.clone().property_history("type");
                let active = v.property_history("active");
                type_.zip(active).map(|(mut x, mut y)| {
                    x.append(&mut y);
                    x
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            res,
            vec![(1, Prop::Str("wallet".into())), (1, Prop::U32(0)),]
        );
    }

    #[test]
    fn add_vertices_with_1_property_different_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex_with_props(
            1,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(0)),
            ],
        );

        g.add_vertex_with_props(
            1,
            2,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(1)),
            ],
        );

        g.add_vertex_with_props(
            1,
            3,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(2)),
            ],
        );

        let res: Vec<(i64, Prop)> = g
            .iter_local_vertices()
            .flat_map(|v| {
                let type_ = v.clone().with_window(2..3).property_history("type");
                let active = v.with_window(2..3).property_history("active");
                type_.zip(active).map(|(mut x, mut y)| {
                    x.append(&mut y);
                    x
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            res,
            vec![(2, Prop::Str("wallet".into())), (2, Prop::U32(1)),]
        );
    }

    #[test]
    fn add_vertices_with_multiple_properties_at_different_times_window() {
        let mut g = TemporalGraph::default();

        g.add_vertex_with_props(
            1,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(0)),
            ],
        );

        g.add_vertex_with_props(1, 2, &vec![("label".into(), Prop::I32(12345))]);

        g.add_vertex_with_props(
            1,
            3,
            &vec![
                ("origin".into(), Prop::F32(0.1)),
                ("active".into(), Prop::U32(2)),
            ],
        );

        let res = g
            .iter_local_vertices()
            .flat_map(|v| {
                let type_ = v.clone().with_window(1..2).property_history("type");
                let active = v.clone().with_window(2..5).property_history("active");
                let label = v.clone().with_window(2..5).property_history("label");
                let origin = v.clone().with_window(2..5).property_history("origin");
                type_
                    .zip(active)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
                    .zip(label)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
                    .zip(origin)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            res,
            vec![
                (1, Prop::Str("wallet".into())),
                (3, Prop::U32(2)),
                (2, Prop::I32(12345)),
                (3, Prop::F32(0.1)),
            ]
        );
    }

    #[test]
    #[ignore = "Undecided on the semantics of the time window over vertices shoule be supported in Docbrown"]
    fn add_vertex_at_time_t1_window() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);

        assert!(g.local_contains_vertex(9));
        assert!(g.local_contains_vertex_window(9, 1..15));
        assert!(g.local_contains_vertex_window(9, 5..15)); // FIXME: this is wrong and we might need a different kind of window here
    }

    #[test]
    fn add_vertex_at_time_t1_t2() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        let actual: Vec<u64> = g.iter_local_vertices_window(0..2).id().collect();
        assert_eq!(actual, vec![9]);
        let actual: Vec<u64> = g
            .iter_local_vertices_window(2..10)
            .map(|v| v.id())
            .collect();
        assert_eq!(actual, vec![1]);
        let actual: Vec<u64> = g
            .iter_local_vertices_window(0..10)
            .map(|v| v.id())
            .collect();
        assert_eq!(actual, vec![9, 1]);
    }

    #[test]
    fn add_edge_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g
            .iter_local_vertices_window(3..10)
            .map(|v| v.id())
            .collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        g.add_edge(9, 1, 3);

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g
            .iter_local_vertices_window(3..10)
            .map(|v| v.id())
            .collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let v9 = g.local_vertex(9).unwrap();
        let actual: Vec<u64> = v9
            .clone()
            .with_window(0..2)
            .out_edges()
            .map(|e| e.global_dst())
            .collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = v9
            .clone()
            .with_window(0..4)
            .out_edges()
            .map(|e| e.global_dst())
            .collect();
        assert_eq!(actual, vec![1]);

        // the inbound neighbours of 1 at time 0..4 are 9
        let v1 = g.local_vertex(1).unwrap();
        let actual: Vec<u64> = g
            .edges(v1.as_pointer().with_window(0..4), Direction::IN)
            .map(|e| e.global_src())
            .collect();
        assert_eq!(actual, vec![9]);
    }

    #[test]
    fn add_edge_at_time_t1_t2_t3() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g
            .iter_local_vertices_window(3..10)
            .map(|v| v.id())
            .collect();
        assert_eq!(actual, vec![]);

        g.add_edge(9, 1, 3);

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g
            .iter_local_vertices_window(3..10)
            .map(|v| v.id())
            .collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let v9 = g.local_vertex(9).unwrap();
        let actual: Vec<u64> = g
            .edges(v9.as_pointer().with_window(0..2), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .edges(v9.as_pointer().with_window(0..4), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        assert_eq!(actual, vec![1]);

        // the outbound neighbours of 9 at time 0..4 are 1
        let v1 = g.local_vertex(1).unwrap();
        let actual: Vec<u64> = g
            .edges(v1.as_pointer().with_window(0..4), Direction::IN)
            .map(|e| e.global_src())
            .collect();
        assert_eq!(actual, vec![9]);
    }

    #[test]
    fn add_edge_at_time_t1_t2_t3_overwrite() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g
            .iter_local_vertices_window(3..10)
            .map(|v| v.id())
            .collect();
        assert_eq!(actual, vec![]);

        g.add_edge(9, 1, 3);
        g.add_edge(9, 1, 12); // add the same edge again at different time

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g
            .iter_local_vertices_window(3..10)
            .map(|v| v.id())
            .collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let v9 = g.local_vertex(9).unwrap();
        let actual: Vec<u64> = g
            .edges(v9.as_pointer().with_window(0..2), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound_t neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .edges(v9.as_pointer().with_window(0..4), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        assert_eq!(actual, vec![1]);

        // the outbound_t neighbours of 9 at time 0..4 are 1
        let v1 = g.local_vertex(1).unwrap();
        let actual: Vec<u64> = g
            .edges(v1.as_pointer().with_window(0..4), Direction::IN)
            .map(|e| e.global_src())
            .collect();
        assert_eq!(actual, vec![9]);

        let actual: Vec<u64> = g
            .edges(v9.as_pointer().with_window(0..13), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        assert_eq!(actual, vec![1]);

        // when we look for time we see both variants
        let actual: Vec<(i64, u64)> = g
            //FIXME: this is still old, need to implement in trait
            .neighbours_window_t(9, &(0..13), Direction::OUT)
            .map(|e| (e.time().unwrap(), e.global_dst()))
            .collect();
        assert_eq!(actual, vec![(3, 1), (12, 1)]);

        let actual: Vec<(i64, u64)> = g
            .neighbours_window_t(1, &(0..13), Direction::IN)
            .map(|e| (e.time().unwrap(), e.global_src()))
            .collect();
        assert_eq!(actual, vec![(3, 9), (12, 9)]);
    }

    #[test]
    fn add_edges_at_t1t2t3_check_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);
        g.add_vertex(33, 3);
        g.add_vertex(44, 4);

        g.add_edge(11, 22, 4);
        g.add_edge(22, 33, 5);
        g.add_edge(11, 44, 6);

        let actual = g
            .iter_local_vertices_window(1..4)
            .map(|v| v.id())
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![11, 22, 33]);

        let actual = g
            .iter_local_vertices_window(1..6)
            .map(|v| v.id())
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![11, 22, 33, 44]);
        let v11 = g.local_vertex(11).unwrap();
        let actual = v11
            .with_window(1..5)
            .out_edges()
            .map(|e| e.global_dst())
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![22]);

        let actual = g
            .neighbours_window_t(11, &(1..5), Direction::OUT)
            .map(|e| (e.time().unwrap(), e.global_dst()))
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![(4, 22)]);

        let actual = g
            .neighbours_window_t(44, &(1..17), Direction::IN)
            .map(|e| (e.time().unwrap(), e.global_src()))
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![(6, 11)]);

        let v44 = g.local_vertex(44).unwrap();
        let actual = v44
            .clone()
            .with_window(1..6)
            .in_edges()
            .map(|e| e.global_dst())
            .collect::<Vec<_>>();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        let actual = v44
            .clone()
            .with_window(1..7)
            .in_edges()
            .map(|e| e.global_src())
            .collect::<Vec<_>>();
        let expected: Vec<u64> = vec![11];
        assert_eq!(actual, expected);

        let actual = v44
            .clone()
            .with_window(9..100)
            .in_edges()
            .map(|e| e.global_dst())
            .collect::<Vec<_>>();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected)
    }

    #[test]
    fn add_the_same_edge_multiple_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);

        g.add_edge(11, 22, 4);
        g.add_edge(11, 22, 4);

        let actual = g
            .local_vertex_window(11, 1..5)
            .unwrap()
            .out_edges()
            .map(|e| e.global_dst())
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![22]);
    }

    #[test]
    fn add_edge_with_1_property() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);

        g.add_edge_with_props(11, 22, 4, &vec![("weight".into(), Prop::U32(12))]);
        let v11 = g.local_vertex(11).unwrap();
        let edge_weights = g
            .edges(v11.as_pointer(), Direction::OUT)
            .flat_map(|e| {
                e.props("weight").map(|i| {
                    i.flat_map(|(t, prop)| match prop {
                        Prop::U32(weight) => Some((t, weight)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(edge_weights, vec![(&4, 12)])
    }

    #[test]
    fn add_edge_with_multiple_properties() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);

        g.add_edge_with_props(
            11,
            22,
            4,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        let v11 = g.local_vertex(11).unwrap();
        let edge_weights = g
            .edges(v11.as_pointer(), Direction::OUT)
            .flat_map(|e| {
                let weight = e.props("weight").map(|x| x.collect::<Vec<_>>());
                let amount = e.props("amount").map(|x| x.collect::<Vec<_>>());
                let label = e.props("label").map(|x| x.collect::<Vec<_>>());
                weight
                    .zip(amount)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
                    .zip(label)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            edge_weights,
            vec![
                (&4, Prop::U32(12)),
                (&4, Prop::F64(12.34)),
                (&4, Prop::Str("blerg".into())),
            ]
        )
    }

    #[test]
    fn add_edge_with_1_property_different_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);

        g.add_edge_with_props(11, 22, 4, &vec![("amount".into(), Prop::U32(12))]);
        g.add_edge_with_props(11, 22, 7, &vec![("amount".into(), Prop::U32(24))]);
        g.add_edge_with_props(11, 22, 19, &vec![("amount".into(), Prop::U32(48))]);

        let v11 = g.local_vertex(11).unwrap();
        let edge_weights = g
            .edges(v11.as_pointer().with_window(4..8), Direction::OUT)
            .flat_map(|e| {
                e.props_window("amount", 4..8).map(|i| {
                    i.flat_map(|(t, prop)| match prop {
                        Prop::U32(weight) => Some((t, weight)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(edge_weights, vec![(&4, 12), (&7, 24)]);

        let v22 = g.local_vertex(22).unwrap();
        let edge_weights = g
            .edges(v22.as_pointer().with_window(4..8), Direction::IN)
            .flat_map(|e| {
                e.props_window("amount", 4..8).map(|i| {
                    i.flat_map(|(t, prop)| match prop {
                        Prop::U32(weight) => Some((t, weight)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(edge_weights, vec![(&4, 12), (&7, 24)])
    }

    #[test]
    fn add_edges_with_multiple_properties_at_different_times_window() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);

        g.add_edge_with_props(
            11,
            22,
            2,
            &vec![
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            11,
            22,
            3,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            11,
            22,
            4,
            &vec![("label".into(), Prop::Str("blerg_again".into()))],
        );

        g.add_edge_with_props(
            22,
            11,
            5,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
            ],
        );

        let v11_3_5 = g.local_vertex_window(11, 3..5).unwrap();
        let edge_weights = g
            .edges(v11_3_5.as_pointer(), Direction::OUT)
            .flat_map(|e| {
                let weight = e
                    .props_window("weight", 3..5)
                    .map(|x| x.collect::<Vec<_>>());
                let amount = e
                    .props_window("amount", 3..5)
                    .map(|x| x.collect::<Vec<_>>());
                let label = e.props_window("label", 3..5).map(|x| x.collect::<Vec<_>>());
                weight
                    .zip(amount)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
                    .zip(label)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            edge_weights,
            vec![
                (&3, Prop::U32(12)),
                (&3, Prop::Str("blerg".into())),
                (&4, Prop::Str("blerg_again".into()))
            ]
        )
    }

    #[test]
    fn edge_metadata_id_bug() {
        let mut g = TemporalGraph::default();

        let edges: Vec<(u64, u64, i64)> = vec![(1, 2, 1), (3, 4, 2), (5, 4, 3), (1, 4, 4)];

        for (src, dst, t) in edges {
            g.add_vertex(src, t);
            g.add_vertex(dst, t);
            g.add_edge_with_props(src, dst, t, &vec![("amount".into(), Prop::U64(12))]);
        }
    }

    #[test]
    fn add_multiple_edges_with_1_property_same_time() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);
        g.add_vertex(33, 3);
        g.add_vertex(44, 4);

        g.add_edge_with_props(11, 22, 4, &vec![("weight".into(), Prop::F32(1122.0))]);
        g.add_edge_with_props(11, 33, 4, &vec![("weight".into(), Prop::F32(1133.0))]);
        g.add_edge_with_props(44, 11, 4, &vec![("weight".into(), Prop::F32(4411.0))]);

        let v11 = g.local_vertex(11).unwrap();
        let edge_weights_out_11 = g
            .edges(v11.as_pointer(), Direction::OUT)
            .flat_map(|e| {
                e.props("weight").map(|i| {
                    i.flat_map(|(t, prop)| match prop {
                        Prop::F32(weight) => Some((t, weight)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(edge_weights_out_11, vec![(&4, 1122.0), (&4, 1133.0)]);

        let edge_weights_into_11 = g
            .edges(v11.as_pointer(), Direction::IN)
            .flat_map(|e| {
                e.props("weight").map(|i| {
                    i.flat_map(|(t, prop)| match prop {
                        Prop::F32(weight) => Some((t, weight)),
                        _ => None,
                    })
                    .collect::<Vec<_>>()
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(edge_weights_into_11, vec![(&4, 4411.0)])
    }

    #[test]
    fn add_edges_with_multiple_properties_at_different_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);
        g.add_vertex(33, 3);
        g.add_vertex(44, 4);

        g.add_edge_with_props(
            11,
            22,
            2,
            &vec![
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            22,
            33,
            3,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            33,
            44,
            4,
            &vec![("label".into(), Prop::Str("blerg".into()))],
        );

        g.add_edge_with_props(
            44,
            11,
            5,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
            ],
        );

        // betwen t:2 and t:4 (excluded) only 11, 22 and 33 are visible, 11 is visible because it has an edge at time 2
        let vs = g
            .iter_local_vertices_window(2..4)
            .map(|v| v.id())
            .collect::<Vec<_>>();

        assert_eq!(vs, vec![11, 22, 33]);

        // between t: 3 and t:6 (excluded) show the visible outbound edges
        let vs = g
            .iter_local_vertices_window(3..6)
            .flat_map(|v| {
                v.out_edges().map(|e| e.global_dst()).collect::<Vec<_>>() // FIXME: we can't just return v.outbound().map(|e| e.global_dst()) here we might need to do so check lifetimes
            })
            .collect::<Vec<_>>();

        assert_eq!(vs, vec![33, 44, 11]);
        let v11 = g.local_vertex(11).unwrap();
        let edge_weights = g
            .edges(v11.as_pointer(), Direction::OUT)
            .flat_map(|e| {
                let weight = e.props("weight").map(|x| x.collect::<Vec<_>>());
                let amount = e.props("amount").map(|x| x.collect::<Vec<_>>());
                let label = e.props("label").map(|x| x.collect::<Vec<_>>());
                weight
                    .zip(amount)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
                    .zip(label)
                    .map(|(mut x, mut y)| {
                        x.append(&mut y);
                        x
                    })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            edge_weights,
            vec![(&2, Prop::F64(12.34)), (&2, Prop::Str("blerg".into()))]
        )
    }

    #[test]
    fn correctness_degree_test() {
        let mut g = TemporalGraph::default();

        let triplets = vec![
            (1, 2, 1, 1),
            (1, 2, 2, 2),
            (1, 2, 3, 2),
            (1, 2, 4, 1),
            (1, 3, 5, 1),
            (3, 1, 6, 1),
        ];

        for (src, dst, t, w) in triplets {
            g.add_edge_with_props(src, dst, t, &vec![("weight".to_string(), Prop::U32(w))]);
        }

        for v in g.iter_local_vertices() {
            let out1 = g
                .edges(v.as_pointer(), Direction::OUT)
                .map(|e| e.global_dst())
                .collect_vec();
            let out2 = g
                .edges(v.as_pointer().with_window(1..7), Direction::OUT)
                .map(|e| e.global_dst())
                .collect_vec();
            assert_eq!(out1, out2);

            assert_eq!(
                g.degree(v.as_pointer(), Direction::OUT),
                g.degree(v.as_pointer().with_window(1..7), Direction::OUT)
            );
            assert_eq!(
                g.degree(v.as_pointer(), Direction::IN),
                g.degree(v.as_pointer().with_window(1..7), Direction::IN)
            );
        }

        let degrees = g
            .iter_local_vertices()
            .map(|v| {
                (
                    v.clone().id(),
                    v.clone().in_degree(),
                    v.clone().out_degree(),
                    v.clone().degree(),
                )
            })
            .collect_vec();
        let degrees_window = g
            .iter_local_vertices_window(1..7)
            .map(|v| {
                (
                    v.clone().id(),
                    v.clone().in_degree(),
                    v.clone().out_degree(),
                    v.clone().degree(),
                )
            })
            .collect_vec();

        let expected = vec![(1, 1, 2, 2), (2, 1, 0, 1), (3, 1, 1, 1)];

        assert_eq!(degrees, expected);
        assert_eq!(degrees_window, expected);
    }

    #[test]
    fn lotr_degree() {
        let mut g = TemporalGraph::default();

        fn parse_record(rec: &StringRecord) -> Option<(String, String, i64)> {
            let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
            let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
            let t = rec.get(2).and_then(|s| s.parse::<i64>().ok())?;
            Some((src, dst, t))
        }

        let data_dir: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/lotr.csv"]
            .iter()
            .collect();

        if !data_dir.exists() {
            panic!("Missing data dir = {}", data_dir.to_str().unwrap())
        }

        if let Ok(mut reader) = csv::Reader::from_path(data_dir) {
            for rec_res in reader.records() {
                if let Ok(rec) = rec_res {
                    if let Some((src, dst, t)) = parse_record(&rec) {
                        let src_id = utils::calculate_hash(&src);

                        let dst_id = utils::calculate_hash(&dst);

                        g.add_vertex(src_id, t);
                        g.add_vertex(dst_id, t);
                        g.add_edge_with_props(src_id, dst_id, t, &vec![]);
                    }
                }
            }
        }

        // query the various graph windows
        // 9501 .. 10001

        let mut degrees_w1 = g
            .iter_local_vertices_window(9501..10001)
            .map(|v| {
                (
                    v.clone().id(),
                    v.clone().in_degree(),
                    v.clone().out_degree(),
                    v.clone().degree(),
                )
            })
            .collect_vec();

        let mut expected_degrees_w1 = vec![
            ("Balin", 0, 5, 5),
            ("Frodo", 4, 4, 8),
            ("Thorin", 0, 1, 1),
            ("Fundin", 1, 0, 1),
            ("Ori", 0, 1, 1),
            ("Pippin", 0, 3, 3),
            ("Merry", 2, 1, 3),
            ("Bilbo", 4, 0, 4),
            ("Gimli", 2, 2, 4),
            ("Legolas", 2, 0, 2),
            ("Sam", 0, 1, 1),
            ("Gandalf", 1, 2, 3),
            ("Boromir", 1, 0, 1),
            ("Aragorn", 3, 1, 4),
            ("Daeron", 1, 0, 1),
        ]
        .into_iter()
        .map(|(name, indeg, outdeg, deg)| (utils::calculate_hash(&name), indeg, outdeg, deg))
        .collect_vec();

        expected_degrees_w1.sort();
        degrees_w1.sort();

        assert_eq!(degrees_w1, expected_degrees_w1);

        // 19001..20001
        let mut expected_degrees_w2 = vec![
            ("Elrond", 1, 0, 1),
            ("Peregrin", 0, 1, 1),
            ("Pippin", 0, 4, 4),
            ("Merry", 2, 1, 3),
            ("Gimli", 0, 2, 2),
            ("Wormtongue", 0, 1, 1),
            ("Legolas", 1, 1, 2),
            ("Sam", 1, 0, 1),
            ("Saruman", 1, 1, 2),
            ("Treebeard", 0, 1, 1),
            ("Gandalf", 3, 3, 6),
            ("Aragorn", 7, 0, 7),
            ("Shadowfax", 1, 1, 2),
            ("Elendil", 0, 1, 1),
        ]
        .into_iter()
        .map(|(name, indeg, outdeg, deg)| (utils::calculate_hash(&name), indeg, outdeg, deg))
        .collect_vec();

        let mut degrees_w2 = g
            .iter_local_vertices_window(19001..20001)
            .map(|v| {
                (
                    v.clone().id(),
                    v.clone().in_degree(),
                    v.clone().out_degree(),
                    v.clone().degree(),
                )
            })
            .collect_vec();

        expected_degrees_w2.sort();
        degrees_w2.sort();

        assert_eq!(degrees_w2, expected_degrees_w2);
    }

    #[quickcheck]
    fn add_vertices_into_two_graph_partitions(vs: Vec<(u64, u64)>) {
        let mut g1 = TemporalGraph::default();

        let mut g2 = TemporalGraph::default();

        let mut shards = vec![&mut g1, &mut g2];
        let some_props: Vec<(String, Prop)> = vec![("bla".to_string(), Prop::U32(1))];

        let n_shards = shards.len();
        for (t, (src, dst)) in vs.into_iter().enumerate() {
            let src_shard = utils::get_shard_id_from_global_vid(src, n_shards);
            let dst_shard = utils::get_shard_id_from_global_vid(src, n_shards);

            shards[src_shard].add_vertex(src.into(), t.try_into().unwrap());
            shards[dst_shard].add_vertex(dst.into(), t.try_into().unwrap());

            if src_shard == dst_shard {
                shards[src_shard].add_edge_with_props(
                    src.into(),
                    dst.into(),
                    t.try_into().unwrap(),
                    &some_props,
                );
            } else {
                shards[src_shard].add_edge_remote_out(
                    src.into(),
                    dst.into(),
                    t.try_into().unwrap(),
                    &some_props,
                );
                shards[dst_shard].add_edge_remote_into(
                    src.into(),
                    dst.into(),
                    t.try_into().unwrap(),
                    &some_props,
                );
            }
        }
    }

    #[test]
    fn adding_remote_edge_does_not_break_local_indices() {
        let mut g1 = TemporalGraph::default();
        g1.add_edge_remote_out(11, 1, 1, &vec![("bla".to_string(), Prop::U32(1))]);
        g1.add_edge_with_props(11, 0, 2, &vec![("bla".to_string(), Prop::U32(1))]);
    }

    #[test]
    fn check_edges_after_adding_remote() {
        let mut g1 = TemporalGraph::default();
        g1.add_vertex(11, 1);

        g1.add_edge_remote_out(11, 22, 2, &vec![("bla".to_string(), Prop::U32(1))]);

        let actual = g1
            .local_vertex_window(11, 1..3)
            .unwrap()
            .out_edges()
            .map(|e| e.global_dst())
            .collect_vec();
        assert_eq!(actual, vec![22]);

        let actual = g1
            .internal_neighbours_iter_window(0, &(1..3), Direction::OUT)
            .map(|(id, edge)| (id, edge.is_local()))
            .collect_vec();
        assert_eq!(actual, vec![(22, false)])
    }

    // this test checks TemporalGraph can be serialized and deserialized
    #[test]
    fn serialize_and_deserialize_with_bincode() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 1);
        g.add_vertex(2, 2);

        g.add_vertex(3, 3);
        g.add_vertex(4, 1);

        g.add_edge_with_props(1, 2, 3, &vec![("bla".to_string(), Prop::U32(1))]);
        g.add_edge_with_props(3, 4, 4, &vec![("bla1".to_string(), Prop::U64(1))]);
        g.add_edge_with_props(
            4,
            1,
            5,
            &vec![("bla2".to_string(), Prop::Str("blergo blargo".to_string()))],
        );

        let mut buffer: Vec<u8> = Vec::new();

        bincode::serialize_into(&mut buffer, &g).unwrap();

        let g2: TemporalGraph = bincode::deserialize_from(&mut buffer.as_slice()).unwrap();
        assert_eq!(g, g2);
    }
}
