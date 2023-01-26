use std::{
    collections::{BTreeMap, HashMap},
    ops::Range,
};

use itertools::Itertools;
use serde::{Serialize, Deserialize};

use crate::adj::Adj;
use crate::Prop;
use crate::{bitset::BitSet, props::TPropVec, tadjset::AdjEdge, Direction};

#[derive(Debug, Serialize, Deserialize, PartialEq)]
pub struct TemporalGraph {
    // maps the global id to the local index id
    logical_to_physical: HashMap<u64, usize>,
    // holds the adjacency lists
    pub(crate) index: Vec<Adj>,
    // time index pointing at the index with adjacency lists
    t_index: BTreeMap<i64, BitSet>,
    // attributes for props
    pub(crate) prop_ids: HashMap<String, usize>,
    pub(crate) edge_meta: Vec<TPropVec>, // table of edges
}

impl Default for TemporalGraph {
    fn default() -> Self {
        Self {
            logical_to_physical: Default::default(),
            index: Default::default(),
            t_index: Default::default(),
            prop_ids: Default::default(),
            // remote/local edges are encoded as i64 with negatives as remote and positives as local,
            // 0 breaks the symetry so we just ignore it
            edge_meta: vec![Default::default()],
        }
    }
}

impl TemporalGraph {
    pub(crate) fn vertices_window_iter(
        &self,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item = usize> + '_> {
        let iter = self
            .t_index
            .range(r.clone())
            .map(|(_, vs)| vs.iter())
            .kmerge()
            .dedup();
        Box::new(iter)
    }

    fn neighbours_iter(
        &self,
        vid: usize,
        d: Direction,
    ) -> Box<dyn Iterator<Item = (&usize, AdjEdge)> + '_> {
        match &self.index[vid] {
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
                    _ => {
                        Box::new(itertools::chain!(
                            out.iter(),
                            into.iter(),
                            remote_out.iter(),
                            remote_into.iter()
                        )) // probably awful but will have to do for now
                    }
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    pub(crate) fn neighbours_iter_window(
        &self,
        vid: usize,
        d: Direction,
        window: &Range<i64>,
    ) -> Box<dyn Iterator<Item = (usize, AdjEdge)> + '_> {
        match &self.index[vid] {
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
                    _ => {
                        Box::new(itertools::chain!(
                            out.iter_window(window),
                            into.iter_window(window),
                            remote_out.iter_window(window),
                            remote_into.iter_window(window)
                        )) // probably awful but will have to do for now
                    }
                }
            }
            _ => Box::new(std::iter::empty()),
        }
    }

    pub(crate) fn _degree(&self, vid: usize, d: Direction) -> usize {
        match &self.index[vid] {
            Adj::List { out, into, .. } => match d {
                Direction::OUT => out.len(),
                Direction::IN => into.len(),
                _ => {
                    vec![out.iter(), into.iter()] // FIXME: there are better ways of doing this, all adj lists are sorted except for the HashMap
                        .into_iter()
                        .flatten()
                        .unique_by(|(v, _)| *v)
                        .count()
                }
            },
            _ => 0,
        }
    }

    pub(crate) fn _degree_window(&self, vid: usize, d: Direction, window: &Range<i64>) -> usize {
        match &self.index[vid] {
            Adj::List { out, into, .. } => match d {
                Direction::OUT => out.len_window(window),
                Direction::IN => into.len_window(window),
                _ => vec![out.iter_window(window), into.iter_window(window)]
                    .into_iter()
                    .flatten()
                    .unique_by(|(v, _)| *v)
                    .count(),
            },
            _ => 0,
        }
    }
}

impl TemporalGraph {

    pub fn contains(&self, v: u64) -> bool {
        self.logical_to_physical.contains_key(&v)
    }

    pub fn contains_vertex_w(&self, w:Range<i64>, v: u64) -> bool {
        if let Some(v_id) = self.logical_to_physical.get(&v) {
            self.t_index.range(w).any(|(_, bs)| bs.contains(v_id))
        } else {
            false
        }
    }

    pub fn len(&self) -> usize {
        self.logical_to_physical.len()
    }

    pub fn edges_len(&self) -> usize {
        self.edge_meta.len()
    }

    pub fn add_vertex(&mut self, v: u64, t: i64) {
        self.add_vertex_props(v, t, &vec![])
    }

    pub fn add_vertex_props(&mut self, v: u64, t: i64, props: &Vec<Prop>) {
        match self.logical_to_physical.get(&v) {
            None => {
                let physical_id: usize = self.index.len();
                self.index.push(Adj::Empty(v));

                self.logical_to_physical.insert(v, physical_id);
                self.t_index
                    .entry(t)
                    .and_modify(|set| {
                        set.push(physical_id);
                    })
                    .or_insert_with(|| BitSet::one(physical_id));
            }
            Some(pid) => {
                self.t_index
                    .entry(t)
                    .and_modify(|set| {
                        set.push(*pid);
                    })
                    .or_insert_with(|| BitSet::one(*pid));
            }
        }
    }

    pub(crate) fn iter_vertices(&self) -> Box<dyn Iterator<Item = VertexView<'_, Self>> + '_> {
        Box::new(self.index.iter().enumerate().map(|(pid, v)| VertexView {
            g_id: *v.logical(),
            pid,
            g: self,
            w: None,
        }))
    }

    pub(crate) fn iter_vs_window(
        &self,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item = VertexView<'_, Self>> + '_> {
        let iter = self
            .t_index
            .range(r.clone())
            .map(|(_, vs)| vs.iter())
            .kmerge()
            .dedup()
            .map(move |pid| match self.index[pid] {
                Adj::Empty(lid) => VertexView {
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
        Box::new(iter)
    }

    pub fn add_edge(&mut self, src: u64, dst: u64, t: i64) {
        self.add_edge_props(src, dst, t, &vec![])
    }

    pub fn add_edge_remote_out(
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

        self.update_edge_props(src_edge_meta_id, t, props)
    }

    pub fn add_edge_remote_into(
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

        self.update_edge_props(dst_edge_meta_id, t, props)
    }

    fn update_edge_props(&mut self, src_edge_meta_id: usize, t: i64, props: &Vec<(String, Prop)>) {
        //FIXME: ensure the self.edge_meta is updated even if the props vector is null
        for (name, prop) in props {
            // find where do we slot this property in the temporal vec for each edge
            let property_id = if let Some(prop_id) = self.prop_ids.get(name) {
                // there is an existing prop set here
                *prop_id
            } else {
                // first time we see this prop
                let id = self.prop_ids.len();
                self.prop_ids.insert(name.to_string(), id);
                id
            };

            if let Some(edge_props) = self.edge_meta.get_mut(src_edge_meta_id) {
                edge_props.set(property_id, t, prop)
            } else {
                // we don't have metadata for this edge
                let prop_cell = TPropVec::from(property_id, t, prop);
                self.edge_meta.insert(src_edge_meta_id, prop_cell)
            }
        }
    }

    pub fn add_edge_props(&mut self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
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

        self.update_edge_props(src_edge_meta_id, t, props)
    }

    fn link_inbound_edge(
        &mut self,
        global_dst_id: u64,
        t: i64,
        src: usize, // may or may not be physical id depending on remote_edge flag
        dst_pid: usize,
        remote_edge: bool,
    ) -> usize {
        match &mut self.index[dst_pid] {
            entry @ Adj::Empty(_) => {
                let edge_id = self.edge_meta.len();

                let edge = AdjEdge::new(edge_id, !remote_edge);

                *entry = Adj::new_into(global_dst_id, src, t, edge);

                edge_id
            }
            Adj::List {
                    into, remote_into, ..
                } => {
                let list = if remote_edge { remote_into } else { into };
                let edge_id: usize = list
                    .find(src)
                    .map(|e| e.edge_meta_id())
                    .unwrap_or(self.edge_meta.len());

                list.push(t, src, AdjEdge::new(edge_id, !remote_edge)); // idempotent
                edge_id
            }
        }
    }

    fn link_outbound_edge(
        &mut self,
        global_src_id: u64,
        t: i64,
        src_pid: usize,
        dst: usize, // may or may not pe physical id depending on remote_edge flag
        remote_edge: bool,
    ) -> usize {
        match &mut self.index[src_pid] {
            entry @ Adj::Empty(_) => {
                let edge_id = self.edge_meta.len();

                let edge = AdjEdge::new(edge_id, !remote_edge);

                *entry = Adj::new_out(global_src_id, dst, t, edge);

                edge_id
            }
            Adj::List {
                    out, remote_out, ..
                } => {
                let list = if remote_edge { remote_out } else { out };
                let edge_id: usize = list
                    .find(dst)
                    .map(|e| e.edge_meta_id())
                    .unwrap_or(self.edge_meta.len());

                list.push(t, dst, AdjEdge::new(edge_id, !remote_edge));
                edge_id
            }
        }
    }

    pub(crate) fn neighbours_window(
        &self,
        w: Range<i64>,
        v: u64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + '_> {
        let v_pid = self.logical_to_physical[&v];

        match d {
            Direction::OUT => Box::new(self.neighbours_iter_window(v_pid, d, &w).map(
                move |(v, e_meta)| EdgeView {
                    src_id: v_pid,
                    dst_id: v,
                    t: None,
                    g: self,
                    e_meta,
                },
            )),
            Direction::IN => Box::new(self.neighbours_iter_window(v_pid, d, &w).map(
                move |(v, e_meta)| EdgeView {
                    src_id: v,
                    dst_id: v_pid,
                    t: None,
                    g: self,
                    e_meta,
                },
            )),
            Direction::BOTH => todo!(),
        }
    }

    pub fn outbound_degree(&self, src: u64) -> usize {
        let src_pid = self.logical_to_physical[&src];
        self._degree(src_pid, Direction::OUT)
    }

    pub fn inbound_degree(&self, dst: u64) -> usize {
        let dst_pid = self.logical_to_physical[&dst];
        self._degree(dst_pid, Direction::IN)
    }

    pub fn outbound_degree_t(&self, src: u64, r: Range<i64>) -> usize {
        let src_pid = self.logical_to_physical[&src];
        self._degree_window(src_pid, Direction::OUT, &r)
    }

    pub fn inbound_degree_t(&self, dst: u64, r: Range<i64>) -> usize {
        let dst_pid = self.logical_to_physical[&dst];
        self._degree_window(dst_pid, Direction::IN, &r)
    }

    pub fn degree(&self, v: u64) -> usize {
        let v_pid = self.logical_to_physical[&v];
        self._degree(v_pid, Direction::BOTH)
    }

    pub fn degree_window(&self, v: u64, r: Range<i64>) -> usize {
        let v_pid = self.logical_to_physical[&v];
        self._degree_window(v_pid, Direction::BOTH, &r)
    }

    pub(crate) fn neighbours_window_t(
        &self,
        r: Range<i64>,
        v: usize,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + '_> {
        //TODO: this could use some improving but I'm bored now
        match d {
            Direction::OUT => {
                let src_pid = v;
                if let Adj::List { out, .. } = &self.index[src_pid] {
                    Box::new(out.iter_window_t(&r).map(move |(v, t, e_meta)| EdgeView {
                        src_id: src_pid,
                        dst_id: v,
                        t: Some(t),
                        g: self,
                        e_meta,
                    }))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            Direction::IN => {
                let dst_pid = v;
                if let Adj::List { into, .. } = &self.index[dst_pid] {
                    Box::new(into.iter_window_t(&r).map(move |(v, t, e_meta)| EdgeView {
                        src_id: v,
                        dst_id: dst_pid,
                        t: Some(t),
                        g: self,
                        e_meta,
                    }))
                } else {
                    Box::new(std::iter::empty())
                }
            }
            Direction::BOTH => {
                panic!()
            }
        }
    }

    pub(crate) fn outbound(&self, src: u64) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + '_> {
        self.neighbours(src, Direction::OUT)
    }

    pub(crate) fn inbound(&self, dst: u64) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + '_> {
        self.neighbours(dst, Direction::IN)
    }

    pub(crate) fn outbound_window(
        &self,
        src: u64,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + '_> {
        self.neighbours_window(r, src, Direction::OUT)
    }

    pub(crate) fn inbound_window(
        &self,
        dst: u64,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + '_> {
        self.neighbours_window(r, dst, Direction::IN)
    }

    pub(crate) fn outbound_window_t(
        &self,
        src: u64,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + '_> {
        let src_pid = self.logical_to_physical[&src];
        self.neighbours_window_t(r, src_pid, Direction::OUT)
    }

    pub(crate) fn inbound_window_t(
        &self,
        dst: u64,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + '_> {
        let dst_pid = self.logical_to_physical[&dst];
        self.neighbours_window_t(r, dst_pid, Direction::IN)
    }

    pub(crate) fn neighbours(
        &self,
        v: u64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView<'_, Self>> + '_>
    where
        Self: Sized,
    {
        let v_pid = self.logical_to_physical[&v];

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
}

pub(crate) struct VertexView<'a, G> {
    g_id: u64,
    pub(crate) pid: usize,
    g: &'a G,
    w: Option<Range<i64>>,
}

impl<'a> VertexView<'a, TemporalGraph> {
    pub fn global_id(&self) -> u64 {
        self.g_id
    }

    pub fn outbound_degree(&self) -> usize {
        if let Some(w) = &self.w {
            self.g._degree_window(self.pid, Direction::OUT, w)
        } else {
            self.g._degree(self.pid, Direction::OUT)
        }
    }

    pub fn inbound_degree(&self) -> usize {
        if let Some(w) = &self.w {
            self.g._degree_window(self.pid, Direction::IN, w)
        } else {
            self.g._degree(self.pid, Direction::IN)
        }
    }

    pub fn degree(&self) -> usize {
        if let Some(w) = &self.w {
            self.g._degree_window(self.pid, Direction::BOTH, w)
        } else {
            self.g._degree(self.pid, Direction::BOTH)
        }
    }

    // FIXME: all the functions using global ID need to be changed to use the physical ID instead
    pub fn outbound(&'a self) -> Box<dyn Iterator<Item = EdgeView<'a, TemporalGraph>> + 'a> {
        if let Some(r) = &self.w {
            self.g.outbound_window(self.g_id, r.clone())
        } else {
            self.g.outbound(self.g_id)
        }
    }

    pub fn inbound(&'a self) -> Box<dyn Iterator<Item = EdgeView<'a, TemporalGraph>> + 'a> {
        if let Some(r) = &self.w {
            self.g.inbound_window(self.g_id, r.clone())
        } else {
            self.g.inbound(self.g_id)
        }
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
            *self.g.index[self.src_id].logical()
        } else {
            self.src_id.try_into().unwrap()
        }
    }

    pub fn global_dst(&self) -> u64 {
        if self.e_meta.is_local() {
            *self.g.index[self.dst_id].logical()
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

    pub fn props(&self, name: &'a str) -> Box<dyn Iterator<Item = (&'a i64, Prop)> + 'a> {
        // find the id of the property
        let prop_id: usize = self.g.prop_ids[name]; // FIXME this can break
        self.g.edge_meta[self.e_meta.edge_meta_id()].iter(prop_id)
    }

    pub fn props_window(
        &self,
        name: &'a str,
        r: Range<i64>,
    ) -> Box<dyn Iterator<Item = (&'a i64, Prop)> + 'a> {
        // find the id of the property
        let prop_id: usize = self.g.prop_ids[name]; // FIXME this can break

        self.g.edge_meta[self.e_meta.edge_meta_id()].iter_window(prop_id, r)
    }
}

#[cfg(test)]
extern crate quickcheck;

#[cfg(test)]
mod graph_test {

    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
        path::PathBuf,
    };

    use csv::StringRecord;

    use super::*;

    #[test]
    fn add_vertex_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);

        assert!(g.contains(9));
        assert!(g.contains_vertex_w(1..15, 9));
        assert_eq!(
            g.iter_vertices()
                .map(|v| v.global_id())
                .collect::<Vec<u64>>(),
            vec![9]
        )
    }


    #[test]
    #[ignore = "Undecided on the semantics of the time window over vertices shoule be supported in Docbrown"]
    fn add_vertex_at_time_t1_window() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);

        assert!(g.contains(9));
        assert!(g.contains_vertex_w(1..15, 9));
        assert!(g.contains_vertex_w(5..15, 9)); // FIXME: this is wrong and we might need a different kind of window here

    }

    #[test]
    fn add_vertex_at_time_t1_t2() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        let actual: Vec<u64> = g.iter_vs_window(0..2).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![9]);
        let actual: Vec<u64> = g.iter_vs_window(2..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![1]);
        let actual: Vec<u64> = g.iter_vs_window(0..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![9, 1]);
    }

    #[test]
    fn add_edge_at_time_t1() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).map(|v| v.global_id()).collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        g.add_edge(9, 1, 3);

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<u64> = g.outbound_window(9, 0..2).map(|e| e.global_dst()).collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g.outbound_window(9, 0..4).map(|e| e.global_dst()).collect();
        assert_eq!(actual, vec![1]);

        // the inbound neighbours of 1 at time 0..4 are 9
        let actual: Vec<u64> = g.inbound_window(1, 0..4).map(|e| e.global_src()).collect();
        assert_eq!(actual, vec![9]);
    }

    #[test]
    fn add_edge_at_time_t1_t2_t3() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![]);

        g.add_edge(9, 1, 3);

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<u64> = g.outbound_window(9, 0..2).map(|e| e.global_dst()).collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g.outbound_window(9, 0..4).map(|e| e.global_dst()).collect();
        assert_eq!(actual, vec![1]);

        // the outbound neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g.inbound_window(1, 0..4).map(|e| e.global_src()).collect();
        assert_eq!(actual, vec![9]);
    }

    #[test]
    fn add_edge_at_time_t1_t2_t3_overwrite() {
        let mut g = TemporalGraph::default();

        g.add_vertex(9, 1);
        g.add_vertex(1, 2);

        // 9 and 1 are not visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![]);

        g.add_edge(9, 1, 3);
        g.add_edge(9, 1, 12); // add the same edge again at different time

        // 9 and 1 are now visible at time 3
        let actual: Vec<u64> = g.iter_vs_window(3..10).map(|v| v.global_id()).collect();
        assert_eq!(actual, vec![9, 1]);

        // the outbound neighbours of 9 at time 0..2 is the empty set
        let actual: Vec<u64> = g.outbound_window(9, 0..2).map(|e| e.global_dst()).collect();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        // the outbound_t neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g.outbound_window(9, 0..4).map(|e| e.global_dst()).collect();
        assert_eq!(actual, vec![1]);

        // the outbound_t neighbours of 9 at time 0..4 are 1
        let actual: Vec<u64> = g.inbound_window(1, 0..4).map(|e| e.global_src()).collect();
        assert_eq!(actual, vec![9]);

        let actual: Vec<u64> = g
            .outbound_window(9, 0..13)
            .map(|e| e.global_dst())
            .collect();
        assert_eq!(actual, vec![1]);

        // when we look for time we see both variants
        let actual: Vec<(i64, u64)> = g
            .outbound_window_t(9, 0..13)
            .map(|e| (e.time().unwrap(), e.global_dst()))
            .collect();
        assert_eq!(actual, vec![(3, 1), (12, 1)]);

        let actual: Vec<(i64, u64)> = g
            .inbound_window_t(1, 0..13)
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
            .iter_vs_window(1..4)
            .map(|v| v.global_id())
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![11, 22, 33]);

        let actual = g
            .iter_vs_window(1..6)
            .map(|v| v.global_id())
            .collect::<Vec<_>>();

        assert_eq!(actual, vec![11, 22, 33, 44]);

        let actual = g
            .outbound_window(11, 1..5)
            .map(|e| e.global_dst())
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![22]);

        let actual = g
            .outbound_window_t(11, 1..5)
            .map(|e| (e.time().unwrap(), e.global_dst()))
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![(4, 22)]);

        let actual = g
            .inbound_window_t(44, 1..17)
            .map(|e| (e.time().unwrap(), e.global_src()))
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![(6, 11)]);

        let actual = g
            .inbound_window(44, 1..6)
            .map(|e| e.global_dst())
            .collect::<Vec<_>>();
        let expected: Vec<u64> = vec![];
        assert_eq!(actual, expected);

        let actual = g
            .inbound_window(44, 1..7)
            .map(|e| e.global_src())
            .collect::<Vec<_>>();
        let expected: Vec<u64> = vec![11];
        assert_eq!(actual, expected);

        let actual = g
            .inbound_window(44, 9..100)
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
            .outbound_window(11, 1..5)
            .map(|e| e.global_dst())
            .collect::<Vec<_>>();
        assert_eq!(actual, vec![22]);
    }

    #[test]
    fn add_edge_with_1_property() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);

        g.add_edge_props(11, 22, 4, &vec![("weight".into(), Prop::U32(12))]);

        let edge_weights = g
            .outbound(11)
            .flat_map(|e| {
                e.props("weight").flat_map(|(t, prop)| match prop {
                    Prop::U32(weight) => Some((t, weight)),
                    _ => None,
                })
            })
            .collect::<Vec<_>>();

        assert_eq!(edge_weights, vec![(&4, 12)])
    }

    #[test]
    fn add_edge_with_multiple_properties() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);

        g.add_edge_props(
            11,
            22,
            4,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        let edge_weights = g
            .outbound(11)
            .flat_map(|e| {
                let mut weight = e.props("weight").collect::<Vec<_>>();

                let mut amount = e.props("amount").collect::<Vec<_>>();

                let mut label = e.props("label").collect::<Vec<_>>();

                weight.append(&mut amount);
                weight.append(&mut label);
                weight
            })
            .collect::<Vec<_>>();

        assert_eq!(
            edge_weights,
            vec![
                (&4, Prop::U32(12)),
                (&4, Prop::F64(12.34)),
                (&4, Prop::Str("blerg".into()))
            ]
        )
    }

    #[test]
    fn add_edge_with_1_property_different_times() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);

        g.add_edge_props(11, 22, 4, &vec![("amount".into(), Prop::U32(12))]);
        g.add_edge_props(11, 22, 7, &vec![("amount".into(), Prop::U32(24))]);
        g.add_edge_props(11, 22, 19, &vec![("amount".into(), Prop::U32(48))]);

        let edge_weights = g
            .outbound_window(11, 4..8)
            .flat_map(|e| {
                e.props_window("amount", 4..8)
                    .flat_map(|(t, prop)| match prop {
                        Prop::U32(weight) => Some((t, weight)),
                        _ => None,
                    })
            })
            .collect::<Vec<_>>();

        assert_eq!(edge_weights, vec![(&4, 12), (&7, 24)]);

        let edge_weights = g
            .inbound_window(22, 4..8)
            .flat_map(|e| {
                e.props_window("amount", 4..8)
                    .flat_map(|(t, prop)| match prop {
                        Prop::U32(weight) => Some((t, weight)),
                        _ => None,
                    })
            })
            .collect::<Vec<_>>();

        assert_eq!(edge_weights, vec![(&4, 12), (&7, 24)])
    }

    #[test]
    fn edge_metadata_id_bug() {
        let mut g = TemporalGraph::default();

        let edges: Vec<(u64, u64, i64)> = vec![(1, 2, 1), (3, 4, 2), (5, 4, 3), (1, 4, 4)];

        for (src, dst, t) in edges {
            g.add_vertex(src, t);
            g.add_vertex(dst, t);
            g.add_edge_props(src, dst, t, &vec![("amount".into(), Prop::U64(12))]);
        }
    }

    #[test]
    fn add_multiple_edges_with_1_property_same_time() {
        let mut g = TemporalGraph::default();

        g.add_vertex(11, 1);
        g.add_vertex(22, 2);
        g.add_vertex(33, 3);
        g.add_vertex(44, 4);

        g.add_edge_props(11, 22, 4, &vec![("weight".into(), Prop::F32(1122.0))]);
        g.add_edge_props(11, 33, 4, &vec![("weight".into(), Prop::F32(1133.0))]);
        g.add_edge_props(44, 11, 4, &vec![("weight".into(), Prop::F32(4411.0))]);

        let edge_weights_out_11 = g
            .outbound(11)
            .flat_map(|e| {
                e.props("weight").flat_map(|(t, prop)| match prop {
                    Prop::F32(weight) => Some((t, weight)),
                    _ => None,
                })
            })
            .collect::<Vec<_>>();

        assert_eq!(edge_weights_out_11, vec![(&4, 1122.0), (&4, 1133.0)]);

        let edge_weights_into_11 = g
            .inbound(11)
            .flat_map(|e| {
                e.props("weight").flat_map(|(t, prop)| match prop {
                    Prop::F32(weight) => Some((t, weight)),
                    _ => None,
                })
            })
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

        g.add_edge_props(
            11,
            22,
            2,
            &vec![
                ("amount".into(), Prop::F64(12.34)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_props(
            22,
            33,
            3,
            &vec![
                ("weight".into(), Prop::U32(12)),
                ("label".into(), Prop::Str("blerg".into())),
            ],
        );

        g.add_edge_props(
            33,
            44,
            4,
            &vec![("label".into(), Prop::Str("blerg".into()))],
        );

        g.add_edge_props(
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
            .iter_vs_window(2..4)
            .map(|v| v.global_id())
            .collect::<Vec<_>>();
        assert_eq!(vs, vec![11, 22, 33]);

        // between t: 3 and t:6 (excluded) show the visible outbound edges
        let vs = g
            .iter_vs_window(3..6)
            .flat_map(|v| {
                v.outbound().map(|e| e.global_dst()).collect::<Vec<_>>() // FIXME: we can't just return v.outbound().map(|e| e.global_dst()) here we might need to do so check lifetimes
            })
            .collect::<Vec<_>>();

        assert_eq!(vs, vec![33, 44, 11]);

        let edge_weights = g
            .outbound(11)
            .flat_map(|e| {
                let mut weight = e.props("weight").collect::<Vec<_>>();

                let mut amount = e.props("amount").collect::<Vec<_>>();

                let mut label = e.props("label").collect::<Vec<_>>();

                weight.append(&mut amount);
                weight.append(&mut label);
                weight
            })
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
            g.add_edge_props(src, dst, t, &vec![("weight".to_string(), Prop::U32(w))]);
        }

        for i in 1..4 {
            let out1 = g.outbound(i).map(|e| e.global_dst()).collect_vec();
            let out2 = g
                .outbound_window(i, 1..7)
                .map(|e| e.global_dst())
                .collect_vec();
            assert_eq!(out1, out2);

            assert_eq!(g.outbound_degree(i), g.outbound_degree_t(i, 1..7));
            assert_eq!(g.inbound_degree(i), g.inbound_degree_t(i, 1..7));
        }

        let degrees = g
            .iter_vertices()
            .map(|v| {
                (
                    v.global_id(),
                    v.inbound_degree(),
                    v.outbound_degree(),
                    v.degree(),
                )
            })
            .collect_vec();
        let degrees_window = g
            .iter_vs_window(1..7)
            .map(|v| {
                (
                    v.global_id(),
                    v.inbound_degree(),
                    v.outbound_degree(),
                    v.degree(),
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

        fn calculate_hash<T: Hash>(t: &T) -> u64 {
            let mut s = DefaultHasher::new();
            t.hash(&mut s);
            s.finish()
        }

        fn parse_record(rec: &StringRecord) -> Option<(String, String, i64)> {
            let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
            let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
            let t = rec.get(2).and_then(|s| s.parse::<i64>().ok())?;
            Some((src, dst, t))
        }

        let lotr_csv: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources/test/lotr.csv"]
            .iter()
            .collect();

        if let Ok(mut reader) = csv::Reader::from_path(lotr_csv) {
            for rec_res in reader.records() {
                if let Ok(rec) = rec_res {
                    if let Some((src, dst, t)) = parse_record(&rec) {
                        let src_id = calculate_hash(&src);

                        let dst_id = calculate_hash(&dst);

                        g.add_vertex(src_id, t);
                        g.add_vertex(dst_id, t);
                        g.add_edge_props(src_id, dst_id, t, &vec![]);
                    }
                }
            }
        }

        // query the various graph windows
        // 9501 .. 10001

        let mut degrees_w1 = g
            .iter_vs_window(9501..10001)
            .map(|v| {
                (
                    v.global_id(),
                    v.inbound_degree(),
                    v.outbound_degree(),
                    v.degree(),
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
        .map(|(name, indeg, outdeg, deg)| (calculate_hash(&name), indeg, outdeg, deg))
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
        .map(|(name, indeg, outdeg, deg)| (calculate_hash(&name), indeg, outdeg, deg))
        .collect_vec();

        let mut degrees_w2 = g
            .iter_vs_window(19001..20001)
            .map(|v| {
                (
                    v.global_id(),
                    v.inbound_degree(),
                    v.outbound_degree(),
                    v.degree(),
                )
            })
            .collect_vec();

        expected_degrees_w2.sort();
        degrees_w2.sort();

        assert_eq!(degrees_w2, expected_degrees_w2);
    }

    fn shard_from_id<N: Into<usize>>(v_id: N, n_shards: usize) -> usize {
        let v: usize = v_id.try_into().unwrap();
        v % n_shards
    }

    #[quickcheck]
    fn add_vertices_into_two_graph_partitions(vs: Vec<(u16, u16)>) {
        let mut g1 = TemporalGraph::default();

        let mut g2 = TemporalGraph::default();

        let mut shards = vec![&mut g1, &mut g2];
        let some_props: Vec<(String, Prop)> = vec![("bla".to_string(), Prop::U32(1))];

        let n_shards = shards.len();
        for (t, (src, dst)) in vs.into_iter().enumerate() {
            let src_shard = shard_from_id(src, n_shards);
            let dst_shard = shard_from_id(src, n_shards);

            shards[src_shard].add_vertex(src.into(), t.try_into().unwrap());
            shards[dst_shard].add_vertex(dst.into(), t.try_into().unwrap());

            if src_shard == dst_shard {
                shards[src_shard].add_edge_props(
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
        g1.add_edge_props(11, 0, 2, &vec![("bla".to_string(), Prop::U32(1))]);
    }

    #[test]
    fn check_edges_after_adding_remote() {
        let mut g1 = TemporalGraph::default();
        g1.add_vertex(11, 1);

        g1.add_edge_remote_out(11, 22, 2, &vec![("bla".to_string(), Prop::U32(1))]);

        let actual = g1.outbound_window(11, 1 .. 3).map(|e| e.global_dst()).collect_vec();
        assert_eq!(actual, vec![22]);

        let actual = g1.neighbours_iter_window(0, Direction::OUT, &(1..3)).map(|(id, edge)| (id, edge.is_local())).collect_vec();
        assert_eq!(actual, vec![(22, false)])
    }

    // this test checks TemporalGraph can be serialized and deserialized
    #[test]
    fn serialize_and_deserialize_with_bincode(){
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 1);
        g.add_vertex(2, 2);

        g.add_vertex(3, 3);
        g.add_vertex(4, 1);

        g.add_edge_props(1, 2, 3, &vec![("bla".to_string(), Prop::U32(1))]);
        g.add_edge_props(3, 4, 4, &vec![("bla1".to_string(), Prop::U64(1))]);
        g.add_edge_props(4, 1, 5, &vec![("bla2".to_string(), Prop::Str("blergo blargo".to_string()))]);

        let mut buffer:Vec<u8> = Vec::new();
        
        bincode::serialize_into(&mut buffer, &g).unwrap();

        let g2:TemporalGraph = bincode::deserialize_from(&mut buffer.as_slice()).unwrap();
        assert_eq!(g, g2);

    }
}
