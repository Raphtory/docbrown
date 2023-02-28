use crate::graph::Graph;
use docbrown_core::{
    tgraph_shard::{TEdge, TVertex},
    Direction,
};

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct WindowedGraph {
    pub(crate) graph: Arc<Graph>,
    pub t_start: i64,
    pub t_end: i64,
}

impl WindowedGraph {
    pub fn new(graph: Arc<Graph>, t_start: i64, t_end: i64) -> Self {
        WindowedGraph {
            graph,
            t_start,
            t_end,
        }
    }

    pub fn contains(&self, v: u64) -> bool {
        self.graph.contains_window(v, self.t_start, self.t_end)
    }

    pub fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.graph.vertex_ids_window(self.t_start, self.t_end)
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = WindowedVertex> + Send> {
        let graph_w = self.clone();
        Box::new(
            self.graph
                .vertices_window(self.t_start, self.t_end)
                .map(move |tv| WindowedVertex::from(tv, Arc::new(graph_w.clone()))),
        )
    }
}

pub struct WindowedVertex {
    pub g_id: u64,
    pub graph_w: Arc<WindowedGraph>,
}

impl WindowedVertex {
    fn from(value: TVertex, graph_w: Arc<WindowedGraph>) -> Self {
        Self {
            g_id: value.g_id,
            graph_w,
        }
    }
}

impl WindowedVertex {
    pub fn degree(&self) -> usize {
        self.graph_w.graph.degree_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::BOTH,
        )
    }

    pub fn in_degree(&self) -> usize {
        self.graph_w.graph.degree_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::IN,
        )
    }

    pub fn out_degree(&self) -> usize {
        self.graph_w.graph.degree_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::OUT,
        )
    }

    pub fn neighbours(&self) -> Box<dyn Iterator<Item = TEdge> + Send> {
        self.graph_w.graph.neighbours_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::BOTH,
        )
    }

    pub fn in_neighbours(&self) -> Box<dyn Iterator<Item = TEdge> + Send> {
        self.graph_w.graph.neighbours_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::IN,
        )
    }

    pub fn out_neighbours(&self) -> Box<dyn Iterator<Item = TEdge> + Send> {
        self.graph_w.graph.neighbours_window(
            self.g_id,
            self.graph_w.t_start,
            self.graph_w.t_end,
            Direction::OUT,
        )
    }
}

#[cfg(test)]
mod views_test {

    use super::WindowedGraph;
    use crate::graph::Graph;

    #[test]
    fn get_vertex_ids() {
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

        let wg = WindowedGraph::new(g.into(), 0, 7);

        let mut vw = wg.vertex_ids().collect::<Vec<_>>();
        vw.sort();
        assert_eq!(vw, vec![1, 2, 3])
    }

    #[test]
    fn get_vertices() {
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

        let mut vw = wg.vertices().map(|tv| tv.g_id).collect::<Vec<_>>();
        vw.sort();
        assert_eq!(vw, vec![1, 2])
    }

    #[test]
    fn get_windowed_vertices_degree() {
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
            .map(|v| (v.g_id, v.degree()))
            .collect::<Vec<_>>();

        let expected = vec![(2, 1), (1, 2)];

        assert_eq!(actual, expected);
    }
}
