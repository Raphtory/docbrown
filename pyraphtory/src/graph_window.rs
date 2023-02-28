use std::sync::Arc;

use crate::{
    graph::Graph,
    wrappers::{Direction, EdgeIterator, VertexIdsIterator, VertexIterator},
};
use docbrown_db::graph_window;
use pyo3::prelude::*;

#[pyclass]
pub struct WindowedGraph {
    pub(crate) windowed_graph: graph_window::WindowedGraph,
}

#[pymethods]
impl WindowedGraph {
    #[new]
    pub fn new(graph: &Graph, t_start: i64, t_end: i64) -> Self {
        Self {
            windowed_graph: graph_window::WindowedGraph::new(
                Arc::new(graph.graph.clone()),
                t_start,
                t_end,
            ),
        }
    }

    pub fn contains(&self, v: u64) -> bool {
        self.windowed_graph.contains(v)
    }

    pub fn degree(&self, v: u64, d: Direction) -> usize {
        self.windowed_graph.degree(v, d.into())
    }

    pub fn vertex_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: self.windowed_graph.vertex_ids(),
        }
    }

    pub fn vertices(&self) -> VertexIterator {
        let iter = self.windowed_graph.vertices().map(|tv| tv.into());

        VertexIterator {
            iter: Box::new(iter),
        }
    }

    pub fn neighbours(&self, v: u64, d: Direction) -> EdgeIterator {
        let iter = self
            .windowed_graph
            .neighbours(v, d.into())
            .map(|te| te.into());

        EdgeIterator {
            iter: Box::new(iter),
        }
    }
}
