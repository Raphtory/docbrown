use crate::edge::{EdgeList, EdgeView};
use crate::path::{Operations, PathFromVertex};
use crate::vertex::VertexView;
use crate::view_api::time::WindowedView;
use crate::view_api::{GraphViewOps, TimeOps, VertexViewOps};
use docbrown_core::tgraph::VertexRef;
use docbrown_core::{Direction, Prop};
use std::collections::HashMap;

#[derive(Clone)]
pub struct WindowedVertex<G: GraphViewOps> {
    graph: G,
    vertex: VertexRef,
    t_start: i64,
    t_end: i64,
}

impl<G: GraphViewOps> WindowedVertex<G> {
    pub fn from_vertex(vertex: &VertexView<G>, t_start: i64, t_end: i64) -> Self {
        Self {
            graph: vertex.graph.clone(),
            vertex: vertex.vertex,
            t_start,
            t_end,
        }
    }
    pub fn from_windowed_vertex(vertex: &WindowedVertex<G>, t_start: i64, t_end: i64) -> Self {
        Self {
            graph: vertex.graph.clone(),
            vertex: vertex.vertex,
            t_start: vertex.actual_start(t_start),
            t_end: vertex.actual_end(t_end),
        }
    }
}

impl<G: GraphViewOps> VertexViewOps for WindowedVertex<G> {
    type Graph = G;

    fn id(&self) -> u64 {
        self.vertex.g_id
    }

    fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.graph
            .temporal_vertex_prop_vec_window(self.vertex, name, self.t_start, self.t_end)
    }

    fn props(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.graph
            .temporal_vertex_props_window(self.vertex, self.t_start, self.t_end)
    }

    fn degree(&self) -> usize {
        self.graph
            .degree_window(self.vertex, self.t_start, self.t_end, Direction::BOTH)
    }

    fn in_degree(&self) -> usize {
        self.graph
            .degree_window(self.vertex, self.t_start, self.t_end, Direction::IN)
    }

    fn out_degree(&self) -> usize {
        self.graph
            .degree_window(self.vertex, self.t_start, self.t_end, Direction::OUT)
    }

    fn edges(&self) -> EdgeList<G> {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, self.t_start, self.t_end, Direction::BOTH)
                .map(move |e| EdgeView::new(g.clone(), e)),
        )
    }

    fn in_edges(&self) -> EdgeList<G> {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, self.t_start, self.t_end, Direction::IN)
                .map(move |e| EdgeView::new(g.clone(), e)),
        )
    }

    fn out_edges(&self) -> EdgeList<G> {
        let g = self.graph.clone();
        Box::new(
            self.graph
                .vertex_edges_window(self.vertex, self.t_start, self.t_end, Direction::OUT)
                .map(move |e| EdgeView::new(g.clone(), e)),
        )
    }

    fn neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        PathFromVertex::new(
            g,
            self,
            Operations::NeighboursWindow {
                dir: Direction::BOTH,
                t_start: self.t_start,
                t_end: self.t_end,
            },
        )
    }

    fn in_neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        PathFromVertex::new(
            g,
            self,
            Operations::NeighboursWindow {
                dir: Direction::IN,
                t_start: self.t_start,
                t_end: self.t_end,
            },
        )
    }

    fn out_neighbours(&self) -> PathFromVertex<G> {
        let g = self.graph.clone();
        PathFromVertex::new(
            g,
            self,
            Operations::NeighboursWindow {
                dir: Direction::OUT,
                t_start: self.t_start,
                t_end: self.t_end,
            },
        )
    }
}

impl<G: GraphViewOps> WindowedView for WindowedVertex<G> {
    fn start(&self) -> i64 {
        self.t_start
    }

    fn end(&self) -> i64 {
        self.t_end
    }
}

impl<G: GraphViewOps> TimeOps for WindowedVertex<G> {
    type WindowedView = Self;

    fn earliest_time(&self) -> Option<i64> {
        self.graph
            .vertex_earliest_time_window(self.vertex, self.t_start, self.t_end)
    }

    fn latest_time(&self) -> Option<i64> {
        self.graph
            .vertex_latest_time_window(self.vertex, self.t_start, self.t_end)
    }

    fn window(&self, t_start: i64, t_end: i64) -> Self::WindowedView {
        WindowedVertex::from_windowed_vertex(self, t_start, t_end)
    }
}

impl<G: GraphViewOps> From<WindowedVertex<G>> for VertexRef {
    fn from(value: WindowedVertex<G>) -> Self {
        value.vertex
    }
}

impl<G: GraphViewOps> From<&WindowedVertex<G>> for VertexRef {
    fn from(value: &WindowedVertex<G>) -> Self {
        value.vertex
    }
}
