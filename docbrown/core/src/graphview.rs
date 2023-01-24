use std::ops::Range;
use itertools::Itertools;
use crate::graph::TemporalGraph;
use crate::graph::VertexView;
use polars_lazy::prelude::*;
use polars::prelude::*;

type State = LazyFrame;

struct Vertices<'a> {
    graph_view: &'a GraphView<'a>,
    state: &'a State
}

impl<'a> Vertices<'a> {
    fn new(graph_view: &'a GraphView, state: &'a State) -> Vertices<'a> {
        Vertices { graph_view, state}
    }
}



struct GraphView<'a> {
    graph: &'a TemporalGraph,
    window: &'a Range<u64>,
    state: State
}




impl<'a> GraphView<'a> {
    pub fn new(graph: &'a TemporalGraph, window: &'a Range<u64>) -> GraphView<'a> {
        GraphView {graph, window, state: State::default()}
    }

    pub fn iter_vertices(&self) -> Box<dyn Iterator<Item = VertexView<'_, TemporalGraph>> + '_> {
        self.graph.iter_vs_window(self.window.clone())
    }

    pub fn with_state(&self, name: &str, value: Expr) -> GraphView<'a> {
        let new_state = self.state.clone().with_column(value.alias(name));
        GraphView {graph: self.graph, window: self.window, state: new_state}
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
}