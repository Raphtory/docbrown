use crate::edge::EdgeView;
use crate::path::{Operations, PathFromGraph};
use crate::vertex::VertexView;
use crate::view_api::*;
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;
use std::ops::Range;

#[derive(Clone)]
pub struct Vertices<G: GraphViewOps> {
    graph: G,
    window: Option<Range<i64>>,
}

impl<G: GraphViewOps> Vertices<G> {
    pub(crate) fn new(graph: G) -> Vertices<G> {
        Self {
            graph,
            window: None,
        }
    }
    pub fn iter(&self) -> Box<dyn Iterator<Item = VertexView<G>> + Send> {
        let g = self.graph.clone();
        let w = self.window.clone();
        Box::new(
            g.vertex_refs()
                .map(move |v| VertexView::new_windowed(g.clone(), v, w.clone())),
        )
    }

    pub fn id(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.iter().id()
    }

    pub fn name(&self) -> Box<dyn Iterator<Item = String> + Send> {
        self.iter().name()
    }

    pub fn property(
        &self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Option<Prop>> + Send> {
        self.iter().property(name, include_static)
    }

    pub fn property_history(
        &self,
        name: String,
    ) -> Box<dyn Iterator<Item = Vec<(i64, Prop)>> + Send> {
        self.iter().property_history(name)
    }

    pub fn properties(
        &self,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = HashMap<String, Prop>> + Send> {
        self.iter().properties(include_static)
    }

    pub fn property_histories(
        &self,
    ) -> Box<dyn Iterator<Item = HashMap<String, Vec<(i64, Prop)>>> + Send> {
        self.iter().property_histories()
    }

    pub fn property_names(
        &self,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = Vec<String>> + Send> {
        self.iter().property_names(include_static)
    }

    pub fn has_property(
        &self,
        name: String,
        include_static: bool,
    ) -> Box<dyn Iterator<Item = bool> + Send> {
        self.iter().has_property(name, include_static)
    }

    pub fn has_static_property(&self, name: String) -> Box<dyn Iterator<Item = bool> + Send> {
        self.iter().has_static_property(name)
    }

    pub fn static_property(&self, name: String) -> Box<dyn Iterator<Item = Option<Prop>> + Send> {
        self.iter().static_property(name)
    }

    pub fn degree(&self) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().degree()
    }

    pub fn in_degree(&self) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().in_degree()
    }

    pub fn out_degree(&self) -> Box<dyn Iterator<Item = usize> + Send> {
        self.iter().out_degree()
    }

    pub fn edges(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send> {
        Box::new(self.iter().map(|v| v.edges()))
    }

    pub fn in_edges(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send> {
        Box::new(self.iter().map(|v| v.in_edges()))
    }

    pub fn out_edges(
        &self,
    ) -> Box<dyn Iterator<Item = Box<dyn Iterator<Item = EdgeView<G>> + Send>> + Send> {
        Box::new(self.iter().map(|v| v.out_edges()))
    }

    pub fn neighbours(&self) -> PathFromGraph<G> {
        let dir = Direction::BOTH;
        match &self.window {
            None => PathFromGraph::new(self.graph.clone(), Operations::Neighbours { dir }),
            Some(w) => PathFromGraph::new(
                self.graph.clone(),
                Operations::NeighboursWindow {
                    dir,
                    t_start: w.start,
                    t_end: w.end,
                },
            ),
        }
    }

    pub fn in_neighbours(&self) -> PathFromGraph<G> {
        let dir = Direction::IN;
        match &self.window {
            None => PathFromGraph::new(self.graph.clone(), Operations::Neighbours { dir }),
            Some(w) => PathFromGraph::new(
                self.graph.clone(),
                Operations::NeighboursWindow {
                    dir,
                    t_start: w.start,
                    t_end: w.end,
                },
            ),
        }
    }

    pub fn out_neighbours(&self) -> PathFromGraph<G> {
        let dir = Direction::OUT;
        match &self.window {
            None => PathFromGraph::new(self.graph.clone(), Operations::Neighbours { dir }),
            Some(w) => PathFromGraph::new(
                self.graph.clone(),
                Operations::NeighboursWindow {
                    dir,
                    t_start: w.start,
                    t_end: w.end,
                },
            ),
        }
    }
}

impl<G: GraphViewOps> TimeOps for Vertices<G> {
    type WindowedViewType = Self;

    fn start(&self) -> Option<i64> {
        match &self.window {
            None => self.graph.start(),
            Some(w) => Some(w.start),
        }
    }

    fn end(&self) -> Option<i64> {
        match &self.window {
            None => self.graph.end(),
            Some(w) => Some(w.end),
        }
    }

    fn window(&self, t_start: i64, t_end: i64) -> Self::WindowedViewType {
        Self {
            graph: self.graph.clone(),
            window: Some(self.actual_start(t_start)..self.actual_end(t_end)),
        }
    }
}

impl<G: GraphViewOps> IntoIterator for Vertices<G> {
    type Item = VertexView<G>;
    type IntoIter = Box<dyn Iterator<Item = VertexView<G>> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter()
    }
}
