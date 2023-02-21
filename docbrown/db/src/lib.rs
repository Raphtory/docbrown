#[cfg(test)]
#[macro_use(quickcheck)]
extern crate quickcheck_macros;

pub struct Vertex {
    id: u64,
    time: i64,
}

pub trait TemporalGraphView {
    fn vertices(&self) -> Box<dyn Iterator<Item = Vertex> + Send>;
}

struct WindowedGraph<G: TemporalGraphView> {
    graph: G,
    window: (i64, i64), // fancy window stuff could be here
}

// needs to implement TemporalGraphView

pub mod data;
pub mod graphdb;
pub mod loaders;
