use crate::graph::Graph;
use crate::perspective::{Perspective, PerspectiveSet};
use docbrown_core::{
    tgraph::{EdgeReference, VertexReference},
    Direction, Prop,
};

use crate::edge::EdgeView;
use crate::vertex::VertexView;
use crate::view_api::internal::GraphViewInternalOps;
use crate::view_api::GraphViewOps;
use crate::view_api::*;
use std::cmp::{max, min};
use std::{collections::HashMap, sync::Arc};

pub struct GraphWindowSet {
    graph: Graph,
    perspectives: Box<dyn Iterator<Item = Perspective> + Send>,
}

impl GraphWindowSet {
    pub fn new(
        graph: Graph,
        perspectives: Box<dyn Iterator<Item = Perspective> + Send>,
    ) -> GraphWindowSet {
        GraphWindowSet {
            graph,
            perspectives,
        }
    }
}

impl Iterator for GraphWindowSet {
    type Item = WindowedGraph;
    fn next(&mut self) -> Option<Self::Item> {
        let perspective = self.perspectives.next()?;
        Some(WindowedGraph {
            graph: self.graph.clone(),
            t_start: perspective.start.unwrap_or(i64::MIN),
            t_end: perspective.end.unwrap_or(i64::MAX),
        })
    }
}

#[derive(Debug, Clone)]
pub struct WindowedGraph {
    pub(crate) graph: Graph,
    pub t_start: i64, // inclusive
    pub t_end: i64,   // exclusive
}

impl WindowedGraph {
    fn actual_start(&self, t_start: i64) -> i64 {
        max(self.t_start, t_start)
    }

    fn actual_end(&self, t_end: i64) -> i64 {
        min(self.t_end, t_end)
    }
}

impl GraphViewInternalOps for WindowedGraph {
    fn vertices_len(&self) -> usize {
        self.graph.vertices_len_window(self.t_start, self.t_end)
    }

    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.graph
            .vertices_len_window(self.actual_start(t_start), self.actual_end(self.t_end))
    }

    fn edges_len(&self) -> usize {
        self.graph.edges_len_window(self.t_start, self.t_end)
    }

    fn edges_len_window(&self, t_start: i64, t_end: i64) -> usize {
        self.graph
            .edges_len_window(self.actual_start(t_start), self.actual_end(self.t_end))
    }

    fn has_edge_ref<V1: Into<VertexReference>, V2: Into<VertexReference>>(
        &self,
        src: V1,
        dst: V2,
    ) -> bool {
        self.graph
            .has_edge_ref_window(src, dst, self.t_start, self.t_end)
    }

    fn has_edge_ref_window<V1: Into<VertexReference>, V2: Into<VertexReference>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> bool {
        self.graph
            .has_edge_ref_window(src, dst, self.actual_start(t_start), self.actual_end(t_end))
    }

    fn has_vertex_ref<V: Into<VertexReference>>(&self, v: V) -> bool {
        self.graph
            .has_vertex_ref_window(v, self.t_start, self.t_end)
    }

    fn has_vertex_ref_window<V: Into<VertexReference>>(
        &self,
        v: V,
        t_start: i64,
        t_end: i64,
    ) -> bool {
        self.graph
            .has_vertex_ref_window(v, self.actual_start(t_start), self.actual_end(t_end))
    }

    fn degree(&self, v: VertexReference, d: Direction) -> usize {
        self.graph.degree_window(v, self.t_start, self.t_end, d)
    }

    fn degree_window(&self, v: VertexReference, t_start: i64, t_end: i64, d: Direction) -> usize {
        self.graph
            .degree_window(v, self.actual_start(t_start), self.actual_end(t_end), d)
    }

    fn vertex_ref(&self, v: u64) -> Option<VertexReference> {
        self.graph.vertex_ref_window(v, self.t_start, self.t_end)
    }

    fn vertex_ref_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexReference> {
        self.graph
            .vertex_ref_window(v, self.actual_start(t_start), self.actual_end(t_end))
    }

    fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph.vertex_ids_window(self.t_start, self.t_end)
    }

    fn vertex_ids_window(&self, t_start: i64, t_end: i64) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph
            .vertex_ids_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    fn vertex_refs(&self) -> Box<dyn Iterator<Item = VertexReference> + Send> {
        self.graph.vertex_refs_window(self.t_start, self.t_end)
    }

    fn vertex_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexReference> + Send> {
        self.graph
            .vertex_refs_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    fn vertices_par<O, F>(&self, f: F) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexReference) -> O + Send + Sync + Copy,
    {
        self.graph.vertices_window_par(self.t_start, self.t_end, f)
    }

    fn fold_par<S, F, F2>(&self, f: F, agg: F2) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexReference) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy,
    {
        self.graph.fold_window_par(self.t_start, self.t_end, f, agg)
    }

    fn vertices_window_par<O, F>(
        &self,
        t_start: i64,
        t_end: i64,
        f: F,
    ) -> Box<dyn Iterator<Item = O>>
    where
        O: Send + 'static,
        F: Fn(VertexReference) -> O + Send + Sync + Copy,
    {
        self.graph
            .vertices_window_par(self.actual_start(t_start), self.actual_end(t_end), f)
    }

    fn fold_window_par<S, F, F2>(&self, t_start: i64, t_end: i64, f: F, agg: F2) -> Option<S>
    where
        S: Send + 'static,
        F: Fn(VertexReference) -> S + Send + Sync + Copy,
        F2: Fn(S, S) -> S + Sync + Send + Copy,
    {
        self.graph
            .fold_window_par(self.actual_start(t_start), self.actual_end(t_end), f, agg)
    }

    fn edge_ref<V1: Into<VertexReference>, V2: Into<VertexReference>>(
        &self,
        src: V1,
        dst: V2,
    ) -> Option<EdgeReference> {
        self.graph
            .edge_ref_window(src, dst, self.t_start, self.t_end)
    }

    fn edge_ref_window<V1: Into<VertexReference>, V2: Into<VertexReference>>(
        &self,
        src: V1,
        dst: V2,
        t_start: i64,
        t_end: i64,
    ) -> Option<EdgeReference> {
        self.graph
            .edge_ref_window(src, dst, self.actual_start(t_start), self.actual_end(t_end))
    }

    fn edge_refs(&self) -> Box<dyn Iterator<Item = EdgeReference> + Send> {
        self.graph.edge_refs_window(self.t_start, self.t_end)
    }

    fn edge_refs_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = EdgeReference> + Send> {
        self.graph
            .edge_refs_window(self.actual_start(t_start), self.actual_end(t_end))
    }

    fn vertex_edges(
        &self,
        v: VertexReference,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeReference> + Send> {
        self.graph
            .vertex_edges_window(v, self.t_start, self.t_end, d)
    }

    fn vertex_edges_window(
        &self,
        v: VertexReference,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeReference> + Send> {
        self.graph
            .vertex_edges_window(v, self.actual_start(t_start), self.actual_end(t_end), d)
    }

    fn vertex_edges_window_t(
        &self,
        v: VertexReference,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeReference> + Send> {
        self.graph
            .vertex_edges_window_t(v, self.actual_start(t_start), self.actual_end(t_end), d)
    }

    fn neighbours(
        &self,
        v: VertexReference,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexReference> + Send> {
        self.graph.neighbours_window(v, self.t_start, self.t_end, d)
    }

    fn neighbours_window(
        &self,
        v: VertexReference,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexReference> + Send> {
        self.graph
            .neighbours_window(v, self.actual_start(t_start), self.actual_end(t_end), d)
    }

    fn neighbours_ids(
        &self,
        v: VertexReference,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph
            .neighbours_ids_window(v, self.t_start, self.t_end, d)
    }

    fn neighbours_ids_window(
        &self,
        v: VertexReference,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph
            .neighbours_ids_window(v, self.actual_start(t_start), self.actual_end(t_end), d)
    }

    fn vertex_prop_vec(&self, v: VertexReference, name: String) -> Vec<(i64, Prop)> {
        self.graph
            .vertex_prop_vec_window(v, name, self.t_start, self.t_end)
    }

    fn vertex_prop_vec_window(
        &self,
        v: VertexReference,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph.vertex_prop_vec_window(
            v,
            name,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }

    fn vertex_props(&self, v: VertexReference) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.vertex_props_window(v, self.t_start, self.t_end)
    }

    fn vertex_props_window(
        &self,
        v: VertexReference,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph
            .vertex_props_window(v, self.actual_start(t_start), self.actual_end(t_end))
    }

    fn edge_props_vec(&self, e: EdgeReference, name: String) -> Vec<(i64, Prop)> {
        self.graph
            .edge_props_vec_window(e, name, self.t_start, self.t_end)
    }

    fn edge_props_vec_window(
        &self,
        e: EdgeReference,
        name: String,
        t_start: i64,
        t_end: i64,
    ) -> Vec<(i64, Prop)> {
        self.graph.edge_props_vec_window(
            e,
            name,
            self.actual_start(t_start),
            self.actual_end(t_end),
        )
    }

    fn edge_props(&self, e: EdgeReference) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph.edge_props_window(e, self.t_start, self.t_end)
    }

    fn edge_props_window(
        &self,
        e: EdgeReference,
        t_start: i64,
        t_end: i64,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph
            .edge_props_window(e, self.actual_start(t_start), self.actual_end(t_end))
    }
}

impl WindowedGraph {
    pub fn new(graph: Graph, t_start: i64, t_end: i64) -> Self {
        WindowedGraph {
            graph,
            t_start,
            t_end,
        }
    }
}

impl GraphViewOps for WindowedGraph {
    type Vertex = WindowedVertex;
    type VertexIter = Self::Vertices;
    type Vertices = Box<dyn Iterator<Item = WindowedVertex> + Send>;
    type Edge = WindowedEdge;
    type Edges = Box<dyn Iterator<Item = WindowedEdge> + Send>;

    fn num_vertices(&self) -> usize {
        // FIXME: This needs Optimising badly
        self.vertices().count()
    }

    fn earliest_time(&self) -> Option<i64> {
        // FIXME: This should return the actual earliest_time in the view, need low-level method
        Some(self.actual_start(self.graph.earliest_time()?))
    }

    fn latest_time(&self) -> Option<i64> {
        // FIXME: This should return the actual latest_time in the view, need low-level method
        Some(self.actual_end(self.graph.latest_time()?))
    }

    fn num_edges(&self) -> usize {
        // FIXME: This needs Optimising badly
        self.edges().count()
    }

    fn has_vertex(&self, v: u64) -> bool {
        self.graph
            .has_vertex_ref_window(v, self.t_start, self.t_end)
    }

    fn has_edge(&self, src: u64, dst: u64) -> bool {
        self.graph
            .has_edge_ref_window(src, dst, self.t_start, self.t_end)
    }

    fn vertex(&self, v: u64) -> Option<WindowedVertex> {
        let graph_w = Arc::new(self.clone());
        self.graph
            .vertex_ref_window(v, self.t_start, self.t_end)
            .map(move |vv| WindowedVertex::new(graph_w, vv))
    }

    fn vertices(&self) -> Self::Vertices {
        let graph_w = self.clone();
        Box::new(
            self.graph
                .vertex_refs_window(self.t_start, self.t_end)
                .map(move |vv| WindowedVertex::new(Arc::new(graph_w.clone()), vv)),
        )
    }

    fn edge(&self, src: u64, dst: u64) -> Option<WindowedEdge> {
        let graph_w = self.clone();
        self.graph
            .edge_ref_window(src, dst, self.t_start, self.t_end)
            .map(|ev| WindowedEdge::new(Arc::new(graph_w.clone()), ev))
    }

    fn edges(&self) -> Self::Edges {
        Box::new(self.vertices().flat_map(|v| v.out_edges()))
    }
}

pub type WindowedVertex = VertexView<WindowedGraph>;

pub type WindowedEdge = EdgeView<WindowedGraph>;

#[cfg(test)]
mod views_test {

    use std::collections::HashMap;

    use super::*;
    use crate::graph::Graph;
    use crate::view_api::*;
    use docbrown_core::Prop;
    use itertools::Itertools;
    use quickcheck::TestResult;
    use rand::Rng;

    #[test]
    fn windowed_graph_vertices_degree() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(2);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let wg = WindowedGraph::new(g.into(), -1, 1);

        let actual = wg
            .vertices()
            .map(|v| (v.id(), v.degree()))
            .collect::<Vec<_>>();

        let expected = vec![(2, 1), (1, 2)];

        assert_eq!(actual, expected);
    }

    #[test]
    fn windowed_graph_edge() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(2);

        for (t, src, dst) in vs {
            g.add_edge(t, src, dst, &vec![]);
        }

        let wg = g.window(i64::MIN, i64::MAX);
        assert_eq!(wg.edge(1, 3).unwrap().src().id(), 1);
        assert_eq!(wg.edge(1, 3).unwrap().dst().id(), 3);
    }

    #[test]
    fn windowed_graph_vertex_edges() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(2);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let wg = WindowedGraph::new(g.into(), -1, 1);

        let v = wg.vertex(1).unwrap();
    }

    #[quickcheck]
    fn windowed_graph_has_vertex(mut vs: Vec<(i64, u64)>) -> TestResult {
        if vs.is_empty() {
            return TestResult::discard();
        }

        let g = Graph::new(2);

        for (t, v) in &vs {
            g.add_vertex(*t, *v, &vec![]);
        }

        vs.sort(); // Sorted by time
        vs.dedup();

        let rand_start_index = rand::thread_rng().gen_range(0..vs.len());
        let rand_end_index = rand::thread_rng().gen_range(0..vs.len());

        if rand_end_index < rand_start_index {
            return TestResult::discard();
        }

        let g = Graph::new(2);

        for (t, v) in &vs {
            g.add_vertex(*t, *v, &vec![]);
        }

        let start = vs.get(rand_start_index).unwrap().0;
        let end = vs.get(rand_end_index).unwrap().0;

        let wg = WindowedGraph::new(g.into(), start, end);
        if start == end {
            let v = vs.get(rand_start_index).unwrap().1;
            return TestResult::from_bool(!wg.has_vertex(v));
        }

        if rand_start_index == rand_end_index {
            let v = vs.get(rand_start_index).unwrap().1;
            return TestResult::from_bool(!wg.has_vertex(v));
        }

        let rand_index_within_rand_start_end: usize =
            rand::thread_rng().gen_range(rand_start_index..rand_end_index);

        let (i, v) = vs.get(rand_index_within_rand_start_end).unwrap();

        if *i == end {
            return TestResult::from_bool(!wg.has_vertex(*v));
        } else {
            return TestResult::from_bool(wg.has_vertex(*v));
        }
    }

    #[test]
    fn windowed_graph_vertex_ids() {
        let vs = vec![(1, 1, 2), (3, 3, 4), (5, 5, 6), (7, 7, 1)];

        let args = vec![(i64::MIN, 8), (i64::MIN, 2), (i64::MIN, 4), (3, 6)];

        let expected = vec![
            vec![1, 2, 3, 4, 5, 6, 7],
            vec![1, 2],
            vec![1, 2, 3, 4],
            vec![3, 4, 5, 6],
        ];

        let g = Graph::new(1);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = g.window(args[i].0, args[i].1);
                let mut e = wg.vertex_ids().collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();

        assert_eq!(res, expected);

        let g = Graph::new(3);
        for (src, dst, t) in &vs {
            g.add_edge(*src, *dst, *t, &vec![]);
        }
        let res: Vec<_> = (0..=3)
            .map(|i| {
                let wg = g.window(args[i].0, args[i].1);
                let mut e = wg.vertex_ids().collect::<Vec<_>>();
                e.sort();
                e
            })
            .collect_vec();
        assert_eq!(res, expected);
    }

    #[test]
    fn windowed_graph_vertices() {
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (-1, 2, 1),
            (0, 1, 1),
            (7, 3, 2),
            (1, 1, 1),
        ];

        let g = Graph::new(1);

        g.add_vertex(
            0,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(99.5)),
            ],
        );

        g.add_vertex(
            -1,
            2,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(10.0)),
            ],
        );

        g.add_vertex(
            6,
            3,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(76.2)),
            ],
        );

        for (t, src, dst) in &vs {
            g.add_edge(
                *t,
                *src,
                *dst,
                &vec![("eprop".into(), Prop::Str("commons".into()))],
            );
        }

        let wg = g.window(-2, 0);

        let actual = wg.vertices().map(|tv| tv.id()).collect::<Vec<_>>();

        let hm: HashMap<String, Vec<(i64, Prop)>> = HashMap::new();
        let expected = vec![1, 2];

        assert_eq!(actual, expected);

        // Check results from multiple graphs with different number of shards
        let g = Graph::new(10);

        g.add_vertex(
            0,
            1,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(99.5)),
            ],
        );

        g.add_vertex(
            -1,
            2,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(10.0)),
            ],
        );

        g.add_vertex(
            6,
            3,
            &vec![
                ("type".into(), Prop::Str("wallet".into())),
                ("cost".into(), Prop::F32(76.2)),
            ],
        );

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let expected = wg.vertices().map(|tv| tv.id()).collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
