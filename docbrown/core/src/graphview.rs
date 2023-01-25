use std::ops::Range;
use itertools::Itertools;
use polars::export::regex::internal::Input;
use crate::graph::{EdgeView, TemporalGraph};
use crate::graph::VertexView;
use polars_lazy::prelude::*;
use polars::prelude::*;
use crate::Direction;
use crate::tadjset::AdjEdge;

type State = DataFrame;

struct Vertices<'a> {
    graph_view: &'a GraphView<'a>
}

impl<'a> Vertices<'a> {
    fn new(graph_view: &'a GraphView) -> Vertices<'a> {
        Vertices { graph_view}
    }
}



pub struct GraphView<'a> {
    graph: &'a TemporalGraph,
    window: &'a Range<u64>,
    state: State
}


pub struct LocalVertexView<'a> {
    graph_view: &'a GraphView<'a>,
    g_id: u64,
    pid: usize
}

impl<'a> LocalVertexView<'a> {
    fn new(graph_view: &'a GraphView, vertex: &VertexView<TemporalGraph>)-> LocalVertexView<'a> {
        LocalVertexView {graph_view, g_id: vertex.global_id(), pid: vertex.pid}
    }
    fn new_neighbour(&'a self, pid: usize) -> LocalVertexView<'a> {
        LocalVertexView {
            graph_view: self.graph_view,
            g_id: self.graph_view.graph.index[pid].logical().clone(),
            pid
        }
    }

    pub fn global_id(&self) -> u64 {
        self.g_id
    }

    pub fn get_state(&self, name: &str) -> AnyValue {
        self.graph_view.get_state(name).get(self.pid).unwrap()
    }

    pub fn out_neighbours(&'a self) -> impl Iterator<Item=LocalVertexView<'a>> {
        self.graph_view.graph.neighbours_iter_window(self.pid, Direction::OUT, self.graph_view.window )
            .map(|(dst, AdjEdge(id)) | {
                assert!(id >= 0, "tried to construct remote neighbour but we are assuming everything is local");
                self.new_neighbour(dst)
            })
    }

    pub fn in_neighbours(&'a self) -> impl Iterator<Item=LocalVertexView<'a>> {
        self.graph_view.graph.neighbours_iter_window(self.pid, Direction::IN, self.graph_view.window)
            .map(|(src, AdjEdge(id))| {
                assert!(id >= 0, "tried to construct remote neighbour but we are assuming everything is local");
                self.new_neighbour(src)
            })
    }
}


impl<'a> GraphView<'a> {
    pub fn new(graph: &'a TemporalGraph, window: &'a Range<u64>) -> GraphView<'a> {
        GraphView {graph, window, state: State::default()}
    }

    pub fn n_nodes(&self) -> usize {
        self.iter_vertices().map(|v| 1).sum()
    }

    pub fn iter_vertices(&'a self) -> impl Iterator<Item = LocalVertexView<'a>> {
        let outer = self.graph.iter_vs_window(self.window.clone());
        outer.map(|v| LocalVertexView::new(self, &v))
    }

    pub fn with_state<S>(&self, name: &str, value: S) -> GraphView<'a>
    where S: IntoSeries
    {
        let s = Series::new(name, value);

        let new_state = self.state.clone().with_column(s).unwrap().to_owned();
        GraphView {graph: self.graph, window: self.window, state: new_state}
    }

    pub fn get_state(&self, name: &str) -> &Series {
        self.state.column(name).unwrap()
    }

    pub fn ids(&self) -> Series {
        self.iter_vertices().map(|v| v.global_id()).collect()
    }

    fn local_ids(&self) -> Box<dyn Iterator<Item = usize> + '_> {
        Box::new(self.iter_vertices().map(|v| v.pid))
    }
}


#[cfg(test)]
mod graph_view_tests {
    use itertools::Itertools;
    use super::*;
    use crate::graph::TemporalGraph;

    #[test]
    fn test_vertex_window() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 0);
        g.add_vertex(2, 0);
        g.add_vertex(3, 1);

        let window = 0..1;
        let view = GraphView::new(&g, &window);
        let vertices = view.iter_vertices().map(|v| v.global_id()).collect_vec();
        assert_eq!(vertices, vec![1,2])
    }

    #[test]
    fn test_we_have_state() {
        let mut g = TemporalGraph::default();

        g.add_vertex(1, 0);
        g.add_vertex(2, 0);
        g.add_vertex(3, 1);

        let view = GraphView::new(&g, &(0..2));
        let view = view.with_state("ids", view.ids());
        for v in view.iter_vertices() {
            let state = v.get_state("ids");
            let id: u64 = state.extract().unwrap();
            assert_eq!(v.global_id(), id)
        }
    }
}