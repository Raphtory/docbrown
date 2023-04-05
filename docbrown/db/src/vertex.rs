//! Defines the `Vertex` struct, which represents a vertex in the graph.

use crate::edge::{EdgeList, EdgeView};
use crate::path::{Operations, PathFromVertex};
use crate::view_api::vertex::VertexViewOps;
use crate::view_api::{GraphViewOps, TimeOps, VertexListOps};
use docbrown_core::tgraph::VertexRef;
use docbrown_core::tgraph_shard::errors::GraphError;
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;
use std::ops::Range;

#[derive(Debug, Clone)]
pub struct VertexView<G: GraphViewOps> {
    pub graph: G,
    pub(crate) vertex: VertexRef,
    window: Option<Range<i64>>,
}

impl<G: GraphViewOps> From<VertexView<G>> for VertexRef {
    fn from(value: VertexView<G>) -> Self {
        value.vertex
    }
}

impl<G: GraphViewOps> From<&VertexView<G>> for VertexRef {
    fn from(value: &VertexView<G>) -> Self {
        value.vertex
    }
}

impl<G: GraphViewOps> VertexView<G> {
    /// Creates a new `VertexView` wrapping a vertex reference and a graph.
    pub(crate) fn new(graph: G, vertex: VertexRef) -> VertexView<G> {
        VertexView {
            graph,
            vertex,
            window: None,
        }
    }
    pub(crate) fn new_windowed(
        graph: G,
        vertex: VertexRef,
        window: Option<Range<i64>>,
    ) -> VertexView<G> {
        VertexView {
            graph,
            vertex,
            window,
        }
    }
}

/// View of a Vertex in a Graph
impl<G: GraphViewOps> VertexViewOps for VertexView<G> {
    type Graph = G;

    fn id(&self) -> u64 {
        self.vertex.g_id
    }

    fn name(&self) -> String {
        match self.static_property("_id".to_string()) {
            None => self.id().to_string(),
            Some(prop) => prop.to_string(),
        }
    }

    fn earliest_time(&self) -> Option<i64> {
        match &self.window {
            None => self.graph.vertex_earliest_time(self.vertex),
            Some(w) => self
                .graph
                .vertex_earliest_time_window(self.vertex, w.start, w.end),
        }
    }

    fn latest_time(&self) -> Option<i64> {
        match &self.window {
            None => self.graph.vertex_latest_time(self.vertex),
            Some(w) => self
                .graph
                .vertex_latest_time_window(self.vertex, w.start, w.end),
        }
    }

    fn property(&self, name: String, include_static: bool) -> Option<Prop> {
        let props = self.property_history(name.clone());
        match props.last() {
            None => {
                if include_static {
                    match self.graph.static_vertex_prop(self.vertex, name) {
                        None => None,
                        Some(prop) => Some(prop),
                    }
                } else {
                    None
                }
            }
            Some((_, prop)) => Some(prop.clone()),
        }
    }

    fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        match &self.window {
            None => self.graph.temporal_vertex_prop_vec(self.vertex, name),
            Some(w) => {
                self.graph
                    .temporal_vertex_prop_vec_window(self.vertex, name, w.start, w.end)
            }
        }
    }

    fn properties(&self, include_static: bool) -> HashMap<String, Prop> {
        let mut props: HashMap<String, Prop> = self
            .property_histories()
            .iter()
            .map(|(key, values)| (key.clone(), values.last().unwrap().1.clone()))
            .collect();

        if include_static {
            for prop_name in self.graph.static_vertex_prop_names(self.vertex) {
                match self
                    .graph
                    .static_vertex_prop(self.vertex, prop_name.clone())
                {
                    Some(prop) => {
                        props.insert(prop_name, prop);
                    }
                    None => {}
                }
            }
        }
        props
    }

    fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        match &self.window {
            None => self.graph.temporal_vertex_props(self.vertex),
            Some(w) => self
                .graph
                .temporal_vertex_props_window(self.vertex, w.start, w.end),
        }
    }

    fn property_names(&self, include_static: bool) -> Vec<String> {
        let mut names: Vec<String> = self.graph.temporal_vertex_prop_names(self.vertex);
        if include_static {
            names.extend(self.graph.static_vertex_prop_names(self.vertex))
        }
        names
    }

    fn has_property(&self, name: String, include_static: bool) -> bool {
        (!self.property_history(name.clone()).is_empty())
            || (include_static
                && self
                    .graph
                    .static_vertex_prop_names(self.vertex)
                    .contains(&name))
    }

    fn has_static_property(&self, name: String) -> bool {
        self.graph
            .static_vertex_prop_names(self.vertex)
            .contains(&name)
    }

    fn static_property(&self, name: String) -> Option<Prop> {
        self.graph.static_vertex_prop(self.vertex, name)
    }

    fn degree(&self) -> usize {
        let dir = Direction::BOTH;
        match &self.window {
            None => self.graph.degree(self.vertex, dir),
            Some(w) => self.graph.degree_window(self.vertex, w.start, w.end, dir),
        }
    }

    fn in_degree(&self) -> usize {
        let dir = Direction::IN;
        match &self.window {
            None => self.graph.degree(self.vertex, dir),
            Some(w) => self.graph.degree_window(self.vertex, w.start, w.end, dir),
        }
    }

    fn out_degree(&self) -> usize {
        let dir = Direction::OUT;
        match &self.window {
            None => self.graph.degree(self.vertex, dir),
            Some(w) => self.graph.degree_window(self.vertex, w.start, w.end, dir),
        }
    }

    fn edges(&self) -> EdgeList<G> {
        let g = self.graph.clone();
        let dir = Direction::BOTH;
        match &self.window {
            None => Box::new(
                self.graph
                    .vertex_edges(self.vertex, dir)
                    .map(move |e| EdgeView::new(g.clone(), e)),
            ),
            Some(w) => Box::new(
                self.graph
                    .vertex_edges_window(self.vertex, w.start, w.end, dir)
                    .map(move |e| EdgeView::new(g.clone(), e)),
            ),
        }
    }

    fn in_edges(&self) -> EdgeList<G> {
        let g = self.graph.clone();
        let dir = Direction::IN;
        match &self.window {
            None => Box::new(
                self.graph
                    .vertex_edges(self.vertex, dir)
                    .map(move |e| EdgeView::new(g.clone(), e)),
            ),
            Some(w) => Box::new(
                self.graph
                    .vertex_edges_window(self.vertex, w.start, w.end, dir)
                    .map(move |e| EdgeView::new(g.clone(), e)),
            ),
        }
    }

    fn out_edges(&self) -> EdgeList<G> {
        let g = self.graph.clone();
        let dir = Direction::OUT;
        match &self.window {
            None => Box::new(
                self.graph
                    .vertex_edges(self.vertex, dir)
                    .map(move |e| EdgeView::new(g.clone(), e)),
            ),
            Some(w) => Box::new(
                self.graph
                    .vertex_edges_window(self.vertex, w.start, w.end, dir)
                    .map(move |e| EdgeView::new(g.clone(), e)),
            ),
        }
    }

    fn neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        let dir = Direction::BOTH;
        match &self.window {
            None => PathFromVertex::new(g, self, Operations::Neighbours { dir }),
            Some(w) => PathFromVertex::new(
                g,
                self,
                Operations::NeighboursWindow {
                    dir,
                    t_start: w.start,
                    t_end: w.end,
                },
            ),
        }
    }

    fn in_neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        let dir = Direction::IN;
        match &self.window {
            None => PathFromVertex::new(g, self, Operations::Neighbours { dir }),
            Some(w) => PathFromVertex::new(
                g,
                self,
                Operations::NeighboursWindow {
                    dir,
                    t_start: w.start,
                    t_end: w.end,
                },
            ),
        }
    }

    fn out_neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        let dir = Direction::OUT;
        match &self.window {
            None => PathFromVertex::new(g, self, Operations::Neighbours { dir }),
            Some(w) => PathFromVertex::new(
                g,
                self,
                Operations::NeighboursWindow {
                    dir,
                    t_start: w.start,
                    t_end: w.end,
                },
            ),
        }
    }
}

impl<G: GraphViewOps> TimeOps for VertexView<G> {
    type WindowedViewType = VertexView<G>;

    fn start(&self) -> Option<i64> {
        self.graph.start()
    }

    fn end(&self) -> Option<i64> {
        self.graph.end()
    }

    fn window(&self, t_start: i64, t_end: i64) -> Self::WindowedViewType {
        Self {
            graph: self.graph.clone(),
            vertex: self.vertex,
            window: Some(t_start..t_end),
        }
    }
}

/// Implementation of the VertexListOps trait for an iterator of VertexView objects.
///
impl<G: GraphViewOps, V: VertexViewOps<Graph = G> + 'static> VertexListOps
    for Box<dyn Iterator<Item = V> + Send>
{
    type Graph = G;
    type Vertex = V;
    type IterType = Box<dyn Iterator<Item = V> + Send>;
    type EList = Box<dyn Iterator<Item = EdgeView<Self::Graph>> + Send>;
    type VList = Box<dyn Iterator<Item = VertexView<Self::Graph>> + Send>;
    type ValueIterType<U> = Box<dyn Iterator<Item = U> + Send>;

    fn earliest_time(self) -> Self::ValueIterType<Option<i64>> {
        Box::new(self.map(|v| v.start()))
    }

    fn latest_time(self) -> Self::ValueIterType<Option<i64>> {
        Box::new(self.map(|v| v.end()))
    }

    fn window(
        self,
        t_start: i64,
        t_end: i64,
    ) -> Self::ValueIterType<<V as TimeOps>::WindowedViewType> {
        Box::new(self.map(move |v| v.window(t_start, t_end)))
    }

    fn id(self) -> Self::ValueIterType<u64> {
        Box::new(self.map(|v| v.id()))
    }

    fn name(self) -> Self::ValueIterType<String> {
        Box::new(self.map(|v| v.name()))
    }

    fn property(self, name: String, include_static: bool) -> Self::ValueIterType<Option<Prop>> {
        let r: Vec<_> = self
            .map(|v| v.property(name.clone(), include_static.clone()))
            .collect();
        Box::new(r.into_iter())
    }

    fn property_history(self, name: String) -> Self::ValueIterType<Vec<(i64, Prop)>> {
        let r: Vec<_> = self.map(|v| v.property_history(name.clone())).collect();
        Box::new(r.into_iter())
    }

    fn properties(self, include_static: bool) -> Self::ValueIterType<HashMap<String, Prop>> {
        let r: Vec<_> = self.map(|v| v.properties(include_static.clone())).collect();
        Box::new(r.into_iter())
    }

    fn property_histories(self) -> Self::ValueIterType<HashMap<String, Vec<(i64, Prop)>>> {
        let r: Vec<_> = self.map(|v| v.property_histories()).collect();
        Box::new(r.into_iter())
    }

    fn property_names(self, include_static: bool) -> Self::ValueIterType<Vec<String>> {
        let r: Vec<_> = self
            .map(|v| v.property_names(include_static.clone()))
            .collect();
        Box::new(r.into_iter())
    }

    fn has_property(self, name: String, include_static: bool) -> Self::ValueIterType<bool> {
        let r: Vec<_> = self
            .map(|v| v.has_property(name.clone(), include_static.clone()))
            .collect();
        Box::new(r.into_iter())
    }

    fn has_static_property(self, name: String) -> Self::ValueIterType<bool> {
        let r: Vec<_> = self.map(|v| v.has_static_property(name.clone())).collect();
        Box::new(r.into_iter())
    }

    fn static_property(self, name: String) -> Self::ValueIterType<Option<Prop>> {
        let r: Vec<_> = self.map(|v| v.static_property(name.clone())).collect();
        Box::new(r.into_iter())
    }

    fn degree(self) -> Self::ValueIterType<usize> {
        let r: Vec<_> = self.map(|v| v.degree()).collect();
        Box::new(r.into_iter())
    }

    fn in_degree(self) -> Self::ValueIterType<usize> {
        let r: Vec<_> = self.map(|v| v.in_degree()).collect();
        Box::new(r.into_iter())
    }

    fn out_degree(self) -> Self::ValueIterType<usize> {
        let r: Vec<_> = self.map(|v| v.out_degree()).collect();
        Box::new(r.into_iter())
    }

    fn edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.edges()))
    }

    fn in_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.in_edges()))
    }

    fn out_edges(self) -> Self::EList {
        Box::new(self.flat_map(|v| v.out_edges()))
    }

    fn neighbours(self) -> Self::VList {
        Box::new(self.flat_map(|v| v.neighbours()))
    }

    fn in_neighbours(self) -> Self::VList {
        Box::new(self.flat_map(|v| v.in_neighbours()))
    }

    fn out_neighbours(self) -> Self::VList {
        Box::new(self.flat_map(|v| v.out_neighbours()))
    }
}

#[cfg(test)]
mod vertex_test {
    use crate::view_api::*;

    #[test]
    fn test_all_degrees_window() {
        let g = crate::graph_loader::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().degree(), 49);
        assert_eq!(
            g.vertex("Gandalf").unwrap().window(1356, 24792).degree(),
            34
        );
        assert_eq!(g.vertex("Gandalf").unwrap().in_degree(), 24);
        assert_eq!(
            g.vertex("Gandalf").unwrap().window(1356, 24792).in_degree(),
            16
        );
        assert_eq!(g.vertex("Gandalf").unwrap().out_degree(), 35);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .out_degree(),
            20
        );
    }

    #[test]
    fn test_all_neighbours_window() {
        let g = crate::graph_loader::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().neighbours().iter().count(), 49);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .neighbours()
                .iter()
                .count(),
            34
        );
        assert_eq!(
            g.vertex("Gandalf").unwrap().in_neighbours().iter().count(),
            24
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .in_neighbours()
                .iter()
                .count(),
            16
        );
        assert_eq!(
            g.vertex("Gandalf").unwrap().out_neighbours().iter().count(),
            35
        );
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .out_neighbours()
                .iter()
                .count(),
            20
        );
    }

    #[test]
    fn test_all_edges_window() {
        let g = crate::graph_loader::lotr_graph::lotr_graph(4);

        assert_eq!(g.num_edges(), 701);
        assert_eq!(g.vertex("Gandalf").unwrap().edges().count(), 59);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .edges()
                .count(),
            36
        );
        assert_eq!(g.vertex("Gandalf").unwrap().in_edges().count(), 24);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .in_edges()
                .count(),
            16
        );
        assert_eq!(g.vertex("Gandalf").unwrap().out_edges().count(), 35);
        assert_eq!(
            g.vertex("Gandalf")
                .unwrap()
                .window(1356, 24792)
                .out_edges()
                .count(),
            20
        );
    }
}
