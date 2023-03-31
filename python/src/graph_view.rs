use std::collections::HashMap;
use std::sync::Arc;

use crate::dynamic::DynamicGraph;
use crate::edge::{PyEdge, PyEdgeIter};
use crate::util::extract_vertex_ref;
use crate::vertex::{PyVertex, PyVertices};
use crate::{graph::Graph, wrappers::*};
use crate::{util, wrappers};
use docbrown_core::tgraph::VertexRef;
use docbrown_db::edge::EdgeView;
use docbrown_db::graph_window;
use docbrown_db::graph_window::GraphWindowSet;
use docbrown_db::vertex::VertexView;
use docbrown_db::view_api::internal::GraphViewInternalOps;
use docbrown_db::view_api::*;
use itertools::Itertools;
use pyo3::exceptions::PyTypeError;
use pyo3::prelude::*;

#[pyclass(name = "GraphView")]
pub struct PyGraphView {
    pub(crate) graph: DynamicGraph,
}

impl<G: GraphViewOps> From<G> for PyGraphView {
    fn from(value: G) -> Self {
        PyGraphView {
            graph: DynamicGraph::new(value),
        }
    }
}

#[pyclass(name = "PyGraphWindowSet")]
pub struct PyGraphWindowSet {
    window_set: GraphWindowSet<DynamicGraph>,
}

impl From<GraphWindowSet<DynamicGraph>> for PyGraphWindowSet {
    fn from(value: GraphWindowSet<DynamicGraph>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyGraphWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python) -> Option<PyGraphView> {
        slf.window_set.next().map(|g| g.into())
    }
}

#[pymethods]
impl PyGraphView {
    //******  Metrics APIs ******//

    pub fn earliest_time(&self) -> Option<i64> {
        self.graph.earliest_time()
    }

    pub fn latest_time(&self) -> Option<i64> {
        self.graph.latest_time()
    }

    pub fn num_edges(&self) -> usize {
        self.graph.num_edges()
    }

    pub fn num_vertices(&self) -> usize {
        self.graph.num_vertices()
    }

    pub fn has_vertex(&self, vertex: &PyAny) -> PyResult<bool> {
        let v = util::extract_vertex_ref(vertex)?;
        Ok(self.graph.has_vertex(v))
    }

    pub fn has_edge(&self, src: &PyAny, dst: &PyAny) -> PyResult<bool> {
        let src = util::extract_vertex_ref(src)?;
        let dst = util::extract_vertex_ref(dst)?;
        Ok(self.graph.has_edge(src, dst))
    }

    //******  Getter APIs ******//

    pub fn vertex(&self, id: &PyAny) -> PyResult<Option<PyVertex>> {
        let v = util::extract_vertex_ref(id)?;
        Ok(self.graph.vertex(v).map(|v| v.into()))
    }

    pub fn vertices(&self) -> PyVertices {
        self.graph.vertices().into()
    }

    pub fn edge(&self, src: &PyAny, dst: &PyAny) -> PyResult<Option<PyEdge>> {
        let src = extract_vertex_ref(src)?;
        let dst = extract_vertex_ref(dst)?;
        Ok(self.graph.edge(src, dst).map(|we| we.into()))
    }

    pub fn edges(&self) -> PyEdgeIter {
        self.graph.edges().into()
    }
}
