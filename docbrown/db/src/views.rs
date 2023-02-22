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

use std::{ops::Range, sync::Arc};

use crate::graphdb::GraphDB;

pub struct WindowedGraph {
    gdb: Arc<GraphDB>,
    window: Range<i64>,
}

impl WindowedGraph {
    pub fn new(gdb: Arc<GraphDB>, t_start: i64, t_end: i64) -> Self {
        WindowedGraph {
            gdb,
            window: t_start..t_end,
        }
    }

    pub fn say_hello(&self) {
        println!("Hello world!")
    }

    pub fn vertices() {
        
    }
}
