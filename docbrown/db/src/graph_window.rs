use crate::graph::Graph;
use docbrown_core::{
    tgraph_shard::{TEdge, TVertex},
    Direction,
};

use std::sync::Arc;

#[derive(Debug, Clone)]
pub struct WindowedGraph {
    graph: Arc<Graph>,
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

    pub fn degree(&self, v: u64, d: Direction) -> usize {
        self.graph.degree_window(v, self.t_start, self.t_end, d)
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

    pub fn neighbours(&self, v: u64, d: Direction) -> Box<dyn Iterator<Item = TEdge> + Send> {
        self.graph.neighbours_window(v, self.t_start, self.t_end, d)
    }
}

pub struct WindowedVertex {
    g_id: u64,
    graph_w: Arc<WindowedGraph>,
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
    pub fn degree(&self, d: Direction) -> usize {
        self.graph_w.degree(self.g_id, d)
    }

    pub fn neighbours(&self, d: Direction) -> Box<dyn Iterator<Item = TEdge> + Send> {
        self.graph_w.neighbours(self.g_id, d)
    }
}

#[cfg(test)]
mod views_test {
    use docbrown_core::Direction;

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

        let vw = wg.vertices();
        for v in vw {
            println!("vid = {}, degree = {}", v.g_id, v.degree(Direction::BOTH))
        }
    }
}
