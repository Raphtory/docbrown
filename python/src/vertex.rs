use crate::dynamic::DynamicGraph;
use crate::edge::PyEdgeIter;
use crate::wrappers::{NestedU64Iter, NestedUsizeIter, Prop, U64Iter, UsizeIter};
use docbrown_core::tgraph::VertexRef;
use docbrown_db::path::{PathFromGraph, PathFromVertex};
use docbrown_db::vertex::VertexView;
use docbrown_db::vertices::Vertices;
use docbrown_db::view_api::*;
use itertools::Itertools;
use pyo3::{pyclass, pymethods, PyRef, PyRefMut};
use std::collections::HashMap;

#[pyclass(name = "Vertex")]
#[derive(Clone)]
pub struct PyVertex {
    vertex: VertexView<DynamicGraph>,
}

impl From<VertexView<DynamicGraph>> for PyVertex {
    fn from(value: VertexView<DynamicGraph>) -> Self {
        PyVertex { vertex: value }
    }
}

impl From<PyVertex> for VertexRef {
    fn from(value: PyVertex) -> Self {
        value.vertex.into()
    }
}

#[pymethods]
impl PyVertex {
    pub fn id(&self) -> u64 {
        self.vertex.id()
    }

    pub fn __getitem__(&self, name: String) -> Option<Prop> {
        self.property(name, Some(true))
    }

    pub fn has_property(&self, name: String, include_static: Option<bool>) -> bool {
        let include_static = include_static.unwrap_or(true);
        self.vertex.has_property(name, include_static)
    }

    pub fn name(&self) -> String {
        self.vertex.name()
    }

    pub fn property(&self, name: String, include_static: Option<bool>) -> Option<Prop> {
        let include_static = include_static.unwrap_or(true);
        match self.vertex.property(name, include_static) {
            None => None,
            Some(prop) => Some(prop.into()),
        }
    }

    pub fn properties(&self, include_static: Option<bool>) -> HashMap<String, Prop> {
        let include_static = include_static.unwrap_or(true);
        self.vertex
            .properties(include_static)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    pub fn property_names(&self, include_static: Option<bool>) -> Vec<String> {
        let include_static = include_static.unwrap_or(true);
        self.vertex.property_names(include_static)
    }

    pub fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        self.vertex
            .property_history(name)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    pub fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.vertex
            .property_histories()
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(|(t, p)| (t, p.into())).collect()))
            .collect()
    }

    pub fn has_static_property(&self, name: String) -> bool {
        self.vertex.has_static_property(name)
    }
    pub fn static_property(&self, name: String) -> Option<Prop> {
        match self.vertex.static_property(name) {
            None => None,
            Some(prop) => Some(prop.into()),
        }
    }

    pub fn degree(&self) -> usize {
        self.vertex.degree()
    }
    pub fn in_degree(&self) -> usize {
        self.vertex.in_degree()
    }
    pub fn out_degree(&self) -> usize {
        self.vertex.out_degree()
    }
    pub fn edges(&self) -> PyEdgeIter {
        self.vertex.edges().into()
    }

    pub fn in_edges(&self) -> PyEdgeIter {
        self.vertex.in_edges().into()
    }

    pub fn out_edges(&self) -> PyEdgeIter {
        self.vertex.out_edges().into()
    }

    pub fn neighbours(&self) -> PyPathFromVertex {
        self.vertex.neighbours().into()
    }

    pub fn in_neighbours(&self) -> PyPathFromVertex {
        self.vertex.in_neighbours().into()
    }

    pub fn out_neighbours(&self) -> PyPathFromVertex {
        self.vertex.out_neighbours().into()
    }

    pub fn __repr__(&self) -> String {
        let properties: String = "{".to_string()
            + &self
                .properties(Some(true))
                .iter()
                .map(|(k, v)| k.to_string() + " : " + &v.to_string())
                .join(", ")
            + &"}".to_string();

        let property_string = if properties.is_empty() {
            "Properties({})".to_string()
        } else {
            format!("Properties({})", properties)
        };
        format!(
            "Vertex(VertexName({}), {})",
            self.name().trim_matches('"'),
            property_string
        )
    }
}

#[pyclass(name = "Vertices")]
pub struct PyVertices {
    pub(crate) vertices: Vertices<DynamicGraph>,
}

impl From<Vertices<DynamicGraph>> for PyVertices {
    fn from(value: Vertices<DynamicGraph>) -> Self {
        Self { vertices: value }
    }
}

#[pymethods]
impl PyVertices {
    fn __iter__(&self) -> PyVertexIterator {
        self.vertices.iter().into()
    }

    fn id(&self) -> U64Iter {
        self.vertices.id().into()
    }

    fn out_neighbours(&self) -> PyPathFromGraph {
        self.vertices.out_neighbours().into()
    }

    fn in_neighbours(&self) -> PyPathFromGraph {
        self.vertices.in_neighbours().into()
    }

    fn neighbours(&self) -> PyPathFromGraph {
        self.vertices.neighbours().into()
    }

    fn in_degree(&self) -> UsizeIter {
        self.vertices.in_degree().into()
    }

    fn out_degree(&self) -> UsizeIter {
        self.vertices.out_degree().into()
    }

    fn degree(&self) -> UsizeIter {
        self.vertices.degree().into()
    }

    fn __repr__(&self) -> String {
        let values = self
            .__iter__()
            .into_iter()
            .take(11)
            .map(|v| v.__repr__())
            .collect_vec();
        if values.len() < 11 {
            "Vertices(".to_string() + &values.join(", ") + ")"
        } else {
            "Vertices(".to_string() + &values[0..10].join(", ") + " ... )"
        }
    }
}

#[pyclass(name = "PathFromGraph")]
pub struct PyPathFromGraph {
    path: PathFromGraph<DynamicGraph>,
}

#[pymethods]
impl PyPathFromGraph {
    fn __iter__(&self) -> PathIterator {
        self.path.iter().into()
    }

    fn id(&self) -> NestedU64Iter {
        self.path.id().into()
    }

    fn out_neighbours(&self) -> Self {
        self.path.out_neighbours().into()
    }

    fn in_neighbours(&self) -> Self {
        self.path.in_neighbours().into()
    }

    fn neighbours(&self) -> Self {
        self.path.neighbours().into()
    }

    fn in_degree(&self) -> NestedUsizeIter {
        self.path.in_degree().into()
    }

    fn out_degree(&self) -> NestedUsizeIter {
        self.path.out_degree().into()
    }

    fn degree(&self) -> NestedUsizeIter {
        self.path.degree().into()
    }

    fn __repr__(&self) -> String {
        let values = self
            .__iter__()
            .into_iter()
            .take(11)
            .map(|v| v.__repr__())
            .collect_vec();
        if values.len() < 11 {
            "WindowedVerticesPath(".to_string() + &values.join(", ") + ")"
        } else {
            "WindowedVerticesPath(".to_string() + &values.join(", ") + " ... )"
        }
    }
}

impl From<PathFromGraph<DynamicGraph>> for PyPathFromGraph {
    fn from(value: PathFromGraph<DynamicGraph>) -> Self {
        Self { path: value }
    }
}

#[pyclass(name = "PathFromVertex")]
pub struct PyPathFromVertex {
    path: PathFromVertex<DynamicGraph>,
}

impl From<PathFromVertex<DynamicGraph>> for PyPathFromVertex {
    fn from(value: PathFromVertex<DynamicGraph>) -> Self {
        Self { path: value }
    }
}

#[pymethods]
impl PyPathFromVertex {
    fn __iter__(&self) -> PyVertexIterator {
        self.path.iter().into()
    }

    fn id(&self) -> U64Iter {
        self.path.id().into()
    }

    fn out_neighbours(&self) -> Self {
        self.path.out_neighbours().into()
    }

    fn in_neighbours(&self) -> Self {
        self.path.in_neighbours().into()
    }

    fn neighbours(&self) -> Self {
        self.path.neighbours().into()
    }

    fn in_degree(&self) -> UsizeIter {
        self.path.in_degree().into()
    }

    fn out_degree(&self) -> UsizeIter {
        self.path.out_degree().into()
    }

    fn degree(&self) -> UsizeIter {
        self.path.degree().into()
    }

    fn __repr__(&self) -> String {
        let values = self
            .__iter__()
            .into_iter()
            .take(11)
            .map(|v| v.__repr__())
            .collect_vec();
        if values.len() < 11 {
            "WindowedVertexIterable(".to_string() + &values.join(", ") + ")"
        } else {
            "WindowedVertexIterable(".to_string() + &values[0..10].join(", ") + " ... )"
        }
    }
}

#[pyclass(name = "VertexIterator")]
pub struct PyVertexIterator {
    iter: Box<dyn Iterator<Item = PyVertex> + Send>,
}

impl From<Box<dyn Iterator<Item = VertexView<DynamicGraph>> + Send>> for PyVertexIterator {
    fn from(value: Box<dyn Iterator<Item = VertexView<DynamicGraph>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|v| v.into())),
        }
    }
}

impl IntoIterator for PyVertexIterator {
    type Item = PyVertex;
    type IntoIter = Box<dyn Iterator<Item = PyVertex> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

#[pymethods]
impl PyVertexIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyVertex> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = PyVertex> + Send>> for PyVertexIterator {
    fn from(value: Box<dyn Iterator<Item = PyVertex> + Send>) -> Self {
        Self { iter: value }
    }
}

#[pyclass]
pub struct PathIterator {
    pub(crate) iter: Box<dyn Iterator<Item = PyPathFromVertex> + Send>,
}

impl IntoIterator for PathIterator {
    type Item = PyPathFromVertex;
    type IntoIter = Box<dyn Iterator<Item = PyPathFromVertex> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

impl From<Box<dyn Iterator<Item = PathFromVertex<DynamicGraph>> + Send>> for PathIterator {
    fn from(value: Box<dyn Iterator<Item = PathFromVertex<DynamicGraph>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|path| path.into())),
        }
    }
}

#[pymethods]
impl PathIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyPathFromVertex> {
        slf.iter.next()
    }
}
