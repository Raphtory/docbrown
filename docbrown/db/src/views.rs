// pub struct Vertex {
//     id: u64,
//     time: i64,
// }

// pub trait TemporalGraphView {
//     fn vertices(&self) -> Box<dyn Iterator<Item = Vertex> + Send>;
// }

// struct WindowedGraph<G: TemporalGraphView> {
//     graph: G,
//     window: (i64, i64), // fancy window stuff could be here
// }

use crate::graphdb::GraphDB;
use docbrown_core::tpartition::TVertex;

use std::{ops::Range, sync::Arc};

pub struct WindowedGraph {
    gdb: Arc<GraphDB>,
    t_start: i64,
    t_end: i64,
}

impl WindowedGraph {
    pub fn new(gdb: Arc<GraphDB>, t_start: i64, t_end: i64) -> Self {
        WindowedGraph {
            gdb,
            t_start,
            t_end,
        }
    }

    pub fn say_hello(&self) {
        println!("Hello world!")
    }

    pub fn vertex_ids(&self) -> Box<dyn Iterator<Item = u64> + Send> {
        self.gdb.vertex_ids_window(self.t_start, self.t_end)
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = TVertex> + Send> {
        self.gdb.vertices_window(self.t_start, self.t_end)
    }
}

#[cfg(test)]
mod views_test {
    use crate::graphdb::GraphDB;

    use super::WindowedGraph;

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

        let g = GraphDB::new(2);

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let wg = WindowedGraph::new(g.into(), 0, 7);

        // assert_eq!(wg.ver().collect(), [])
    }
}
