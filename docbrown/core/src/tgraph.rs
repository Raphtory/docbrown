use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
};

use itertools::Itertools;
use serde::{Deserialize, Serialize};

use crate::adj::Adj;
use crate::props::Props;
use crate::Prop;
use crate::{bitset::BitSet, tadjset::AdjEdge, Direction};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TemporalGraph {
    // Maps global (logical) id to the local (physical) id which is an index to the adjacency list vector
    pub(crate) logical_to_physical: HashMap<u64, usize>,

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

impl TemporalGraph {
    pub(crate) fn len(&self) -> usize {
        self.logical_to_physical.len()
    }

    pub(crate) fn out_edges_len(&self) -> usize {
        self.adj_lists
            .iter()
            .map(|adj| adj.out_edges_len())
            .reduce(|s1, s2| s1 + s2)
            .unwrap_or(0)
    }

    pub(crate) fn contains_vertex(&self, v: u64) -> bool {
        self.logical_to_physical.contains_key(&v)
    }

    pub(crate) fn contains_vertex_window(&self, r: &Range<i64>, v: u64) -> bool {
        if let Some(v_id) = self.logical_to_physical.get(&v) {
            self.index.range(r.clone()).any(|(_, bs)| bs.contains(v_id))
        } else {
            false
        }
    }

    pub(crate) fn find(&self, v: u64) -> Option<VertexView<'_, Self>> {
        let pid = self.logical_to_physical.get(&v)?;
        let r = i64::MIN..i64::MAX;
        Some(match self.adj_lists[*pid] {
            Adj::Solo(lid) => VertexView {
                g_id: lid,
                pid: *pid,
                g: self,
                w: Some(r.clone()),
            },
            Adj::List { logical, .. } => VertexView {
                g_id: logical,
                pid: *pid,
                g: self,
                w: Some(r.clone()),
            },
        })
    }

    pub(crate) fn find_window(&self, v: u64, r: &Range<i64>) -> Option<VertexView<'_, Self>> {
        let pid = self.logical_to_physical.get(&v)?;
        let w = r.clone();
        let mut vs = self.index.range(w.clone()).flat_map(|(_, vs)| vs.iter());

        match vs.contains(&pid) {
            true => Some(match self.adj_lists[*pid] {
                Adj::Solo(lid) => VertexView {
                    g_id: lid,
                    pid: *pid,
                    g: self,
                    w: Some(w),
                },
                Adj::List { logical, .. } => VertexView {
                    g_id: logical,
                    pid: *pid,
                    g: self,
                    w: Some(w),
                },
            }),
            false => None,
        }
    }

    pub(crate) fn add_vertex(&mut self, t: i64, v: u64) {
        self.add_vertex_with_props(t, v, &vec![])
    }

    pub(crate) fn add_vertex_with_props(&mut self, t: i64, v: u64, props: &Vec<(String, Prop)>) {
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

        self.props.upsert_vertex_props(t, index, props);
    }

    pub(crate) fn add_edge(&mut self, t: i64, src: u64, dst: u64) {
        self.add_edge_with_props(t, src, dst, &vec![])
    }

    pub(crate) fn add_edge_with_props(
        &mut self,
        t: i64,
        src: u64,
        dst: u64,
        props: &Vec<(String, Prop)>,
    ) {
        // mark the times of the vertices at t
        self.add_vertex(t, src);
        self.add_vertex(t, dst);

        let src_pid = self.logical_to_physical[&src];
        let dst_pid = self.logical_to_physical[&dst];

        let src_edge_meta_id = self.link_outbound_edge(t, src, src_pid, dst_pid, false);
        let dst_edge_meta_id = self.link_inbound_edge(t, dst, src_pid, dst_pid, false);

        if src_edge_meta_id != dst_edge_meta_id {
            panic!(
                "Failure on {src} -> {dst} at time: {t} {src_edge_meta_id} != {dst_edge_meta_id}"
            );
        }

        self.props.upsert_edge_props(t, src_edge_meta_id, props)
    }

    pub(crate) fn add_edge_remote_out(
        &mut self,
        t: i64,
        src: u64, // we are on the source shard
        dst: u64,
        props: &Vec<(String, Prop)>,
    ) {
        self.add_vertex(t, src);
        let src_pid = self.logical_to_physical[&src];
        let src_edge_meta_id =
            self.link_outbound_edge(t, src, src_pid, dst.try_into().unwrap(), true);

        self.props.upsert_edge_props(t, src_edge_meta_id, props)
    }

    pub(crate) fn add_edge_remote_into(
        &mut self,
        t: i64,
        src: u64,
        dst: u64, // we are on the destination shard
        props: &Vec<(String, Prop)>,
    ) {
        self.add_vertex(t, dst);

        let dst_pid = self.logical_to_physical[&dst];

        let dst_edge_meta_id =
            self.link_inbound_edge(t, dst, src.try_into().unwrap(), dst_pid, true);

        self.props.upsert_edge_props(t, dst_edge_meta_id, props)
    }

    pub(crate) fn degree(&self, v: u64, d: Direction) -> usize {
        let v_pid = self.logical_to_physical[&v];

        match &self.adj_lists[v_pid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => match d {
                Direction::OUT => out.len() + remote_out.len(),
                Direction::IN => into.len() + remote_into.len(),
                _ => {
                    vec![
                        out.iter(),
                        into.iter(),
                        remote_out.iter(),
                        remote_into.iter(),
                    ] // FIXME: there are better ways of doing this, all adj lists are sorted except for the HashMap
                    .into_iter()
                    .flatten()
                    .unique_by(|(v, _)| *v)
                    .count()
                }
            },
            _ => 0,
        }
    }

    pub(crate) fn degree_window(&self, v: u64, r: &Range<i64>, d: Direction) -> usize {
        let v_pid = self.logical_to_physical[&v];

        match &self.adj_lists[v_pid] {
            Adj::List {
                out,
                into,
                remote_out,
                remote_into,
                ..
            } => match d {
                Direction::OUT => out.len_window(r) + remote_out.len_window(r),
                Direction::IN => into.len_window(r) + remote_into.len_window(r),
                _ => vec![
                    out.iter_window(r),
                    into.iter_window(r),
                    remote_out.iter_window(r),
                    remote_into.iter_window(r),
                ]
                .into_iter()
                .flatten()
                .unique_by(|(v, _)| *v)
                .count(),
            },
            _ => 0,
        }
    }

    pub(crate) fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send + '_> {
        Box::new(self.adj_lists.iter().map(|adj| *adj.logical()))
    }

    pub(crate) fn vertex_ids_window(
        &self,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item = u64> + Send + '_> {
        Box::new(
            self.index
                .range(r.clone())
                .map(|(_, vs)| vs.iter())
                .kmerge()
                .dedup()
                .map(move |pid| match self.adj_lists[pid] {
                    Adj::Solo(lid) => lid,
                    Adj::List { logical, .. } => logical,
                }),
        )
    }

    pub(crate) fn vertices(&self) -> Box<dyn Iterator<Item = VertexView<'_, Self>> + Send + '_> {
        Box::new(
            self.adj_lists
                .iter()
                .enumerate()
                .map(|(pid, v)| VertexView {
                    g_id: *v.logical(),
                    pid,
                    g: self,
                    w: None,
                }),
        )
    }

    pub(crate) fn vertices_window(
        &self,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item = VertexView<'_, Self>> + Send + '_> {
        let unique_vids = self
            .index
            .range(r.clone())
            .map(|(_, vs)| vs.iter())
            .kmerge()
            .dedup();

        let vs = unique_vids.map(move |pid| match self.adj_lists[pid] {
            Adj::Solo(lid) => VertexView {
                g_id: lid,
                pid,
                g: self,
                w: Some(r.clone()),
            },
            Adj::List { logical, .. } => VertexView {
                g_id: logical,
                pid,
                g: self,
                w: Some(r.clone()),
            },
        });

        Box::new(vs)
    }

    pub(crate) fn neighbours(
        &self,
        v: u64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + Send + '_>
    where
        Self: Sized,
    {
        let v_pid = self.logical_to_physical[&v];

        match d {
            Direction::OUT => {
                Box::new(
                    self.neighbours_iter(v_pid, d)
                        .map(move |(v, e_meta)| EdgeView {
                            src_id: v_pid,
                            dst_id: *v,
                            t: None,
                            g: self,
                            e_meta,
                        }),
                )
            }
            Direction::IN => {
                Box::new(
                    self.neighbours_iter(v_pid, d)
                        .map(move |(v, e_meta)| EdgeView {
                            src_id: *v,
                            dst_id: v_pid,
                            t: None,
                            g: self,
                            e_meta,
                        }),
                )
            }
            Direction::BOTH => Box::new(itertools::chain!(
                self.neighbours(v, Direction::IN),
                self.neighbours(v, Direction::OUT)
            )),
        }
    }

    pub(crate) fn neighbours_window(
        &self,
        v: u64,
        w: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + Send + '_> {
        let v_pid = self.logical_to_physical[&v];

        match d {
            Direction::OUT => Box::new(self.neighbours_iter_window(v_pid, w, d).map(
                move |(v, e_meta)| EdgeView {
                    src_id: v_pid,
                    dst_id: v,
                    t: None,
                    g: self,
                    e_meta,
                },
            )),
            Direction::IN => Box::new(self.neighbours_iter_window(v_pid, w, d).map(
                move |(v, e_meta)| EdgeView {
                    src_id: v,
                    dst_id: v_pid,
                    t: None,
                    g: self,
                    e_meta,
                },
            )),
            Direction::BOTH => Box::new(itertools::chain!(
                self.neighbours_window(v, w, Direction::IN),
                self.neighbours_window(v, w, Direction::OUT)
            )),
        }
    }

    pub(crate) fn neighbours_window_t(
        &self,
        v: u64,
        r: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + Send + '_> {
        let v_pid = self.logical_to_physical[&v];

        match d {
            Direction::OUT => Box::new(self.neighbours_iter_window_t(v_pid, r, d).map(
                move |(v, t, e_meta)| EdgeView {
                    src_id: v_pid,
                    dst_id: v,
                    t: Some(t),
                    g: self,
                    e_meta,
                },
            )),
            Direction::IN => Box::new(self.neighbours_iter_window_t(v_pid, r, d).map(
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
        t: i64,
        dst_gid: u64,
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
        t: i64,
        src_gid: u64,
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

    fn neighbours_iter(
        &self,
        vid: usize,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (&usize, AdjEdge)> + Send + '_> {
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

    fn neighbours_iter_window(
        &self,
        vid: usize,
        r: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (usize, AdjEdge)> + Send + '_> {
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

    fn neighbours_iter_window_t(
        &self,
        vid: usize,
        window: &Range<i64>,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (usize, i64, AdjEdge)> + Send + '_> {
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

#[derive(Debug, PartialEq)]
pub(crate) struct VertexView<'a, G> {
    g_id: u64,
    pid: usize,
    g: &'a G,
    w: Option<Range<i64>>,
}

impl<'a> VertexView<'a, TemporalGraph> {
    pub fn graph(&self) -> &TemporalGraph {
        self.g
    }

    pub fn global_id(&self) -> u64 {
        self.g_id
    }

    pub fn partition_id(&self) -> usize {
        self.pid
    }

    pub fn window(&self) -> Option<Range<i64>> {
        self.w.clone()
    }

    pub fn degree(&self, d: Direction) -> usize {
        if let Some(w) = &self.w {
            self.g.degree_window(self.g_id, w, d)
        } else {
            self.g.degree(self.g_id, d)
        }
    }

    // FIXME: all the functions using global ID need to be changed to use the physical ID instead
    pub fn edges(
        &'a self,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView<'a, TemporalGraph>> + 'a> {
        if let Some(r) = &self.w {
            self.g.neighbours_window(self.g_id, r, d)
        } else {
            self.g.neighbours(self.g_id, d)
        }
    }

    pub fn props(&self, name: &'a str) -> Option<Box<dyn Iterator<Item = (&'a i64, Prop)> + 'a>> {
        let index = self.g.logical_to_physical.get(&self.g_id)?;
        let meta = self.g.props.vertex_meta.get(*index)?;
        let prop_id = self.g.props.prop_ids.get(name)?;
        Some(meta.iter(*prop_id))
    }

    pub fn props_window(
        &self,
        name: &'a str,
        r: Range<i64>,
    ) -> Option<Box<dyn Iterator<Item = (&'a i64, Prop)> + 'a>> {
        let index = self.g.logical_to_physical.get(&self.g_id)?;
        let meta = self.g.props.vertex_meta.get(*index)?;
        let prop_id = self.g.props.prop_ids.get(name)?;
        Some(meta.iter_window(*prop_id, r))
    }

    pub fn all_props<T: From<Prop>>(&self) -> Option<HashMap<String, Vec<(i64, T)>>> {
        let index = self.g.logical_to_physical.get(&self.g_id)?;
        let meta = self.g.props.vertex_meta.get(*index)?;

        let mut hm: HashMap<String, Vec<(i64, T)>> = HashMap::new();

        self.g.props.prop_ids.iter().for_each(|(k, v)| {
            if hm.contains_key(k) {
                let vs = hm.get_mut(k).unwrap();
                vs.append(
                    &mut meta
                        .iter(*v)
                        .map(|(x, y)| (*x, y.into()))
                        .collect::<Vec<(i64, T)>>(),
                )
            } else {
                let value = meta
                    .iter(*v)
                    .map(|(x, y)| (*x, y.into()))
                    .collect::<Vec<(i64, T)>>();
                if !value.is_empty() {
                    // self.g.props.prop_ids returns all prop ids, including edge property ids
                    hm.insert(k.clone(), Vec::from(value));
                }
            }
        });

        Some(hm) // Don't return "None" if hm.is_empty for Some({}) gets translated as {} in python
    }

    pub fn all_props_window<T: From<Prop>>(
        &self,
        r: Range<i64>,
    ) -> Option<HashMap<String, Vec<(i64, T)>>> {
        let index = self.g.logical_to_physical.get(&self.g_id)?;
        let meta = self.g.props.vertex_meta.get(*index)?;

        let mut hm: HashMap<String, Vec<(i64, T)>> = HashMap::new();

        self.g.props.prop_ids.iter().for_each(|(k, v)| {
            if hm.contains_key(k) {
                let vs = hm.get_mut(k).unwrap();
                vs.append(
                    &mut meta
                        .iter_window(*v, r.clone())
                        .map(|(x, y)| (*x, y.into()))
                        .collect::<Vec<(i64, T)>>(),
                )
            } else {
                let value = meta
                    .iter_window(*v, r.clone())
                    .map(|(x, y)| (*x, y.into()))
                    .collect::<Vec<(i64, T)>>();
                if !value.is_empty() {
                    // self.g.props.prop_ids returns all prop ids, including edge property ids
                    hm.insert(k.clone(), Vec::from(value));
                }
            }
        });

        Some(hm) // Don't return "None" if hm.is_empty for Some({}) gets translated as {} in python
    }
}

pub(crate) struct EdgeView<'a, G: Sized> {
    src_id: usize,
    dst_id: usize,
    g: &'a G,
    t: Option<i64>,
    e_meta: AdjEdge,
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

    use csv::StringRecord;

    use crate::utils;

    use super::*;

    #[test]
    fn testhm() {
        let map = std::collections::HashMap::from([("a", 1), ("b", 2), ("c", 3)]);

        for val in map.values() {
            println!("sk: {:?}", val);
        }
    }

    #[test]
    fn add_vertex_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 9);

        assert!(g.contains_vertex(9));
        assert!(g.contains_vertex_window(&(1..15), 9));
        assert_eq!(
            g.vertices().map(|v| v.global_id()).collect::<Vec<u64>>(),
            vec![9]
        );
        assert_eq!(g.props.vertex_meta.get(2), None);
    }

    #[test]
    fn add_vertices_with_1_property() {
        let mut g = TemporalGraph::default();

        let v_id = 1;
        let ts = 1;
        g.add_vertex_with_props(ts, v_id, &vec![("type".into(), Prop::Str("wallet".into()))]);

        assert!(g.contains_vertex(v_id));
        assert!(g.contains_vertex_window(&(1..15), v_id));
        assert_eq!(
            g.vertices().map(|v| v.global_id()).collect::<Vec<u64>>(),
            vec![v_id]
        );

        let res = g
            .vertices()
            .flat_map(|v| v.props("type"))
            .flat_map(|v| v.collect::<Vec<_>>())
            .collect::<Vec<_>>();

        assert_eq!(res, vec![(&1, Prop::Str("wallet".into()))]);
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
            .vertices()
            .flat_map(|v| {
                let type_ = v.props("type").map(|x| x.collect::<Vec<_>>());
                let active = v.props("active").map(|x| x.collect::<Vec<_>>());
                type_.zip(active).map(|(mut x, mut y)| {
                    x.append(&mut y);
                    x
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            res,
            vec![(&1, Prop::Str("wallet".into())), (&1, Prop::U32(0)),]
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
            2,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(1)),
            ],
        );

        g.add_vertex_with_props(
            3,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("active".into(), Prop::U32(2)),
            ],
        );

        let res: Vec<(&i64, Prop)> = g
            .vertices()
            .flat_map(|v| {
                let type_ = v.props_window("type", 2..3).map(|x| x.collect::<Vec<_>>());
                let active = v
                    .props_window("active", 2..3)
                    .map(|x| x.collect::<Vec<_>>());
                type_.zip(active).map(|(mut x, mut y)| {
                    x.append(&mut y);
                    x
                })
            })
            .flatten()
            .collect::<Vec<_>>();

        assert_eq!(
            res,
            vec![(&2, Prop::Str("wallet".into())), (&2, Prop::U32(1)),]
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

        g.add_vertex_with_props(2, 1, &vec![("label".into(), Prop::I32(12345))]);

        g.add_vertex_with_props(
            3,
            1,
            &vec![
                ("origin".into(), Prop::F32(0.1)),
                ("active".into(), Prop::U32(2)),
            ],
        );

        let res = g
            .vertices()
            .flat_map(|v| {
                let type_ = v.props_window("type", 1..2).map(|x| x.collect::<Vec<_>>());
                let active = v
                    .props_window("active", 2..5)
                    .map(|x| x.collect::<Vec<_>>());
                let label = v.props_window("label", 2..5).map(|x| x.collect::<Vec<_>>());
                let origin = v
                    .props_window("origin", 2..5)
                    .map(|x| x.collect::<Vec<_>>());
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
                (&1, Prop::Str("wallet".into())),
                (&3, Prop::U32(2)),
                (&2, Prop::I32(12345)),
                (&3, Prop::F32(0.1)),
            ]
        );
    }

    #[test]
    #[ignore = "Undecided on the semantics of the time window over vertices shoule be supported in Docbrown"]
    fn add_vertex_at_time_t1_window() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);

        assert!(g.contains_vertex(9));
        assert!(g.contains_vertex_window(&(1..15), 9));
        assert!(g.contains_vertex_window(&(5..15), 9)); // FIXME: this is wrong and we might need a different kind of window here
    }

    #[test]
    fn add_vertex_at_time_t1_t2() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 9);
        g.add_vertex(2, 1);

        let actual: Vec<u64> = g.vertices_window(0..2).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![9]);
        let actual: Vec<u64> = g.vertices_window(2..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![1]);
        let actual: Vec<u64> = g.vertices_window(0..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![9, 1]);
    }

    #[test]
    fn add_edge_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 9);
        g.add_vertex(2, 1);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.global_id()).collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        g.add_edge(3, 9, 1);

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<u64> = g
            .neighbours_window(9, &(0..2), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .neighbours_window(9, &(0..4), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        assert_eq!(actual, vec![1]);

        // the inbound neighbours of 1 at time 0..4 are 9
        let actual: Vec<u64> = g
            .neighbours_window(1, &(0..4), Direction::IN)
            .map(|e| e.global_src())
            .collect();
        assert_eq!(actual, vec![9]);
    }

    #[test]
    fn add_edge_at_time_t1_t2_t3() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 9);
        g.add_vertex(2, 1);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![]);

        g.add_edge(3, 9, 1);

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<u64> = g
            .neighbours_window(9, &(0..2), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .neighbours_window(9, &(0..4), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        assert_eq!(actual, vec![1]);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .neighbours_window(1, &(0..4), Direction::IN)
            .map(|e| e.global_src())
            .collect();
        assert_eq!(actual, vec![9]);
    }

    #[test]
    fn add_edge_at_time_t1_t2_t3_overwrite() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 9);
        g.add_vertex(2, 1);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![]);

        g.add_edge(3, 9, 1);
        g.add_edge(12, 9, 1); // add the same edge again at different time

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.vertices_window(3..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<u64> = g
            .neighbours_window(9, &(0..2), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound_t neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .neighbours_window(9, &(0..4), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        assert_eq!(actual, vec![1]);

        // the outbound_t neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g
            .neighbours_window(1, &(0..4), Direction::IN)
            .map(|e| e.global_src())
            .collect();
        assert_eq!(actual, vec![9]);

        let actual: Vec<u64> = g
            .neighbours_window(9, &(0..13), Direction::OUT)
            .map(|e| e.global_dst())
            .collect();
        assert_eq!(actual, vec![1]);

        // when we look for time we see both variants
        let actual: Vec<(i64, u64)> = g
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

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);
        g.add_vertex(3, 33);
        g.add_vertex(4, 44);

        g.add_edge(4, 11, 22);
        g.add_edge(5, 22, 33);
        g.add_edge(6, 11, 44);

        let actual = g
            .vertices_window(1..4)
            .map(|v| v.global_id())
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![11, 22, 33]);

        let actual = g
            .vertices_window(1..6)
            .map(|v| v.global_id())
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![11, 22, 33, 44]);

        let actual = g
            .neighbours_window(11, &(1..5), Direction::OUT)
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

        let actual = g
            .neighbours_window(44, &(1..6), Direction::IN)
            .map(|e| e.global_dst())
            .collect::<Vec<_>>();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        let actual = g
            .neighbours_window(44, &(1..7), Direction::IN)
            .map(|e| e.global_src())
            .collect::<Vec<_>>();
        let expected: Vec<u64> = vec![11];
        assert_eq!(actual, expected);

        let actual = g
            .neighbours_window(44, &(9..100), Direction::IN)
            .map(|e| e.global_dst())
            .collect::<Vec<_>>();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected)
    }

    #[test]
    fn add_the_same_edge_multiple_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);

        g.add_edge(4, 11, 22);
        g.add_edge(4, 11, 22);

        let actual = g
            .neighbours_window(11, &(1..5), Direction::OUT)
            .map(|e| e.global_dst())
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![22]);
    }

    #[test]
    fn add_edge_with_1_property() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);

        g.add_edge_with_props(4, 11, 22, &vec![("weight".into(), Prop::U32(12))]);

        let edge_weights = g
            .neighbours(11, Direction::OUT)
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

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);

        g.add_edge_with_props(
            4,
            11,
            22,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        let edge_weights = g
            .neighbours(11, Direction::OUT)
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

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);

        g.add_edge_with_props(4, 11, 22, &vec![("amount".into(), Prop::U32(12))]);
        g.add_edge_with_props(7, 11, 22, &vec![("amount".into(), Prop::U32(24))]);
        g.add_edge_with_props(19, 11, 22, &vec![("amount".into(), Prop::U32(48))]);

        let edge_weights = g
            .neighbours_window(11, &(4..8), Direction::OUT)
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

        let edge_weights = g
            .neighbours_window(22, &(4..8), Direction::IN)
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

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);

        g.add_edge_with_props(
            2,
            11,
            22,
            &vec![
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            3,
            11,
            22,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            4,
            11,
            22,
            &vec![("label".into(), Prop::Str("blerg_again".into()))],
        );

        g.add_edge_with_props(
            5,
            22,
            11,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
            ],
        );

        let edge_weights = g
            .neighbours_window(11, &(3..5), Direction::OUT)
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

        let edges: Vec<(i64, u64, u64)> = vec![(1, 1, 2), (2, 3, 4), (3, 5, 4), (4, 1, 4)];

        for (t, src, dst) in edges {
            g.add_vertex(t, src);
            g.add_vertex(t, dst);
            g.add_edge_with_props(t, src, dst, &vec![("amount".into(), Prop::U64(12))]);
        }
    }

    #[test]
    fn add_multiple_edges_with_1_property_same_time() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);
        g.add_vertex(3, 33);
        g.add_vertex(4, 44);

        g.add_edge_with_props(4, 11, 22, &vec![("weight".into(), Prop::F32(1122.0))]);
        g.add_edge_with_props(4, 11, 33, &vec![("weight".into(), Prop::F32(1133.0))]);
        g.add_edge_with_props(4, 44, 11, &vec![("weight".into(), Prop::F32(4411.0))]);

        let edge_weights_out_11 = g
            .neighbours(11, Direction::OUT)
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
            .neighbours(11, Direction::IN)
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

        g.add_vertex(1, 11);
        g.add_vertex(2, 22);
        g.add_vertex(3, 33);
        g.add_vertex(4, 44);

        g.add_edge_with_props(
            2,
            11,
            22,
            &vec![
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            3,
            22,
            33,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_with_props(
            4,
            33,
            44,
            &vec![("label".into(), Prop::Str("blerg".into()))],
        );

        g.add_edge_with_props(
            5,
            44,
            11,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
            ],
        );

        // betwen t:2 and t:4 (excluded) only 11, 22 and 33 are visible, 11 is visible because it has an edge at time 2
        let vs = g
            .vertices_window(2..4)
            .map(|v| v.global_id())
            .collect::<Vec<_>>();

        assert_eq!(vs, vec![11, 22, 33]);

        // between t: 3 and t:6 (excluded) show the visible outbound edges
        let vs = g
            .vertices_window(3..6)
            .flat_map(|v| {
                v.edges(Direction::OUT)
                    .map(|e| e.global_dst())
                    .collect::<Vec<_>>() // FIXME: we can't just return v.outbound().map(|e| e.global_dst()) here we might need to do so check lifetimes
            })
            .collect::<Vec<_>>();

        assert_eq!(vs, vec![33, 44, 11]);

        let edge_weights = g
            .neighbours(11, Direction::OUT)
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
            (1, 1, 2, 1),
            (2, 1, 2, 2),
            (2, 1, 2, 3),
            (1, 1, 2, 4),
            (1, 1, 3, 5),
            (1, 3, 1, 6),
        ];

        for (t, src, dst, w) in triplets {
            g.add_edge_with_props(t, src, dst, &vec![("weight".to_string(), Prop::U32(w))]);
        }

        for i in 1..4 {
            let out1 = g
                .neighbours(i, Direction::OUT)
                .map(|e| e.global_dst())
                .collect_vec();
            let out2 = g
                .neighbours_window(i, &(1..7), Direction::OUT)
                .map(|e| e.global_dst())
                .collect_vec();
            assert_eq!(out1, out2);

            assert_eq!(
                g.degree(i, Direction::OUT),
                g.degree_window(i, &(1..7), Direction::OUT)
            );
            assert_eq!(
                g.degree(i, Direction::IN),
                g.degree_window(i, &(1..7), Direction::IN)
            );
        }

        let degrees = g
            .vertices()
            .map(|v| {
                (
                    v.global_id(),
                    v.degree(Direction::IN),
                    v.degree(Direction::OUT),
                    v.degree(Direction::BOTH),
                )
            })
            .collect_vec();

        let degrees_window = g
            .vertices_window(1..7)
            .map(|v| {
                (
                    v.global_id(),
                    v.degree(Direction::IN),
                    v.degree(Direction::OUT),
                    v.degree(Direction::BOTH),
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

        let data_dir: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources/test/lotr.csv"]
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

                        g.add_vertex(t, src_id);
                        g.add_vertex(t, dst_id);
                        g.add_edge_with_props(t, src_id, dst_id, &vec![]);
                    }
                }
            }
        }

        // query the various graph windows
        // 9501 .. 10001

        let mut degrees_w1 = g
            .vertices_window(9501..10001)
            .map(|v| {
                (
                    v.global_id(),
                    v.degree(Direction::IN),
                    v.degree(Direction::OUT),
                    v.degree(Direction::BOTH),
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
            .vertices_window(19001..20001)
            .map(|v| {
                (
                    v.global_id(),
                    v.degree(Direction::IN),
                    v.degree(Direction::OUT),
                    v.degree(Direction::BOTH),
                )
            })
            .collect_vec();

        expected_degrees_w2.sort();
        degrees_w2.sort();

        assert_eq!(degrees_w2, expected_degrees_w2);
    }

    #[test]
    fn find_vertex() {
        let mut g = TemporalGraph::default();

        let triplets = vec![
            (1, 1, 2, 1),
            (2, 1, 2, 2),
            (2, 1, 2, 3),
            (1, 1, 2, 4),
            (1, 1, 3, 5),
            (1, 3, 1, 6),
        ];

        for (t, src, dst, w) in triplets {
            g.add_edge_with_props(t, src, dst, &vec![("weight".to_string(), Prop::U32(w))]);
        }

        let pid = *(g.logical_to_physical.get(&1).unwrap());

        let actual = g.find(1);
        let expected = Some(VertexView {
            g_id: 1,
            pid,
            g: &g,
            w: Some(i64::MIN..i64::MAX),
        });

        assert_eq!(actual, expected);

        let actual = g.find(10);
        let expected = None;

        assert_eq!(actual, expected);

        let actual = g.find_window(1, &(0..3));
        let expected = Some(VertexView {
            g_id: 1,
            pid,
            g: &g,
            w: Some(0..3),
        });

        assert_eq!(actual, expected);

        let actual = g.find_window(10, &(0..3));
        let expected = None;

        assert_eq!(actual, expected);

        let actual = g.find_window(1, &(0..1));
        let expected = None;

        assert_eq!(actual, expected);
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

            shards[src_shard].add_vertex(t.try_into().unwrap(), src.into());
            shards[dst_shard].add_vertex(t.try_into().unwrap(), dst.into());

            if src_shard == dst_shard {
                shards[src_shard].add_edge_with_props(
                    t.try_into().unwrap(),
                    src.into(),
                    dst.into(),
                    &some_props,
                );
            } else {
                shards[src_shard].add_edge_remote_out(
                    t.try_into().unwrap(),
                    src.into(),
                    dst.into(),
                    &some_props,
                );
                shards[dst_shard].add_edge_remote_into(
                    t.try_into().unwrap(),
                    src.into(),
                    dst.into(),
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
        g1.add_vertex(1, 11);

        g1.add_edge_remote_out(2, 11, 22, &vec![("bla".to_string(), Prop::U32(1))]);

        let actual = g1
            .neighbours_window(11, &(1..3), Direction::OUT)
            .map(|e| e.global_dst())
            .collect_vec();
        assert_eq!(actual, vec![22]);

        let actual = g1
            .neighbours_iter_window(0, &(1..3), Direction::OUT)
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
