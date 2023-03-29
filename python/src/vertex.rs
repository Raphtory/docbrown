use crate::wrappers;
use crate::wrappers::{
    DegreeIterable, Direction, IdIterable, NestedDegreeIterable, NestedIdIterable,
    NestedU64Iterator, NestedUsizeIter, Operations, Prop, PyPathFromVertex, U64Iterator, USizeIter,
    WindowedEdgeIterator,
};
use docbrown_core::tgraph::VertexRef;
use docbrown_db::path::{PathFromGraph, PathFromVertex};
use docbrown_db::vertex::VertexView;
use docbrown_db::vertices::Vertices;
use docbrown_db::view_api::{GraphViewOps, VertexListOps, VertexViewOps};
use itertools::Itertools;
use pyo3::{pyclass, pymethods, Py, PyRef, PyRefMut, PyResult, Python};
use std::collections::HashMap;

#[pyclass(name = "Vertex")]
pub struct PyVertex {
    vertex: VertexView<Box<dyn GraphViewOps>>,
}

impl From<VertexView<Box<dyn GraphViewOps>>> for PyVertex {
    fn from(value: VertexView<G>) -> Self {
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
    pub fn __getitem__(&self, name: String) -> Vec<(i64, Prop)> {
        self.prop(name)
    }

    pub fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.vertex
            .prop(name)
            .into_iter()
            .map(|(t, p)| (t, p.into()))
            .collect_vec()
    }

    pub fn props(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.vertex
            .props()
            .into_iter()
            .map(|(n, p)| {
                let prop = p
                    .into_iter()
                    .map(|(t, p)| (t, p.into()))
                    .collect::<Vec<(i64, wrappers::Prop)>>();
                (n, prop)
            })
            .into_iter()
            .collect::<HashMap<String, Vec<(i64, Prop)>>>()
    }

    pub fn degree(&self, t_start: Option<i64>, t_end: Option<i64>) -> usize {
        match (t_start, t_end) {
            (None, None) => self.vertex.degree(),
            _ => self
                .vertex
                .degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX)),
        }
    }
    pub fn in_degree(&self, t_start: Option<i64>, t_end: Option<i64>) -> usize {
        match (t_start, t_end) {
            (None, None) => self.vertex.in_degree(),
            _ => self
                .vertex
                .in_degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX)),
        }
    }
    pub fn out_degree(&self, t_start: Option<i64>, t_end: Option<i64>) -> usize {
        match (t_start, t_end) {
            (None, None) => self.vertex.out_degree(),
            _ => self
                .vertex
                .out_degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX)),
        }
    }
    pub fn edges(&self, t_start: Option<i64>, t_end: Option<i64>) -> WindowedEdgeIterator {
        match (t_start, t_end) {
            (None, None) => WindowedEdgeIterator {
                iter: Box::new(self.vertex.edges().map(|te| te.into())),
            },
            _ => WindowedEdgeIterator {
                iter: Box::new(
                    self.vertex
                        .edges_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                        .map(|te| te.into()),
                ),
            },
        }
    }

    pub fn in_edges(&self, t_start: Option<i64>, t_end: Option<i64>) -> WindowedEdgeIterator {
        match (t_start, t_end) {
            (None, None) => WindowedEdgeIterator {
                iter: Box::new(self.vertex.in_edges().map(|te| te.into())),
            },
            _ => WindowedEdgeIterator {
                iter: Box::new(
                    self.vertex
                        .in_edges_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                        .map(|te| te.into()),
                ),
            },
        }
    }

    pub fn out_edges(&self, t_start: Option<i64>, t_end: Option<i64>) -> WindowedEdgeIterator {
        match (t_start, t_end) {
            (None, None) => WindowedEdgeIterator {
                iter: Box::new(self.vertex.out_edges().map(|te| te.into())),
            },
            _ => WindowedEdgeIterator {
                iter: Box::new(
                    self.vertex
                        .out_edges_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                        .map(|te| te.into()),
                ),
            },
        }
    }

    pub fn neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyPathFromVertex {
        if t_start.is_none() && t_end.is_none() {
            self.vertex.neighbours().into()
        } else {
            self.vertex
                .neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    pub fn in_neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyPathFromVertex {
        if t_start.is_none() && t_end.is_none() {
            self.vertex.in_neighbours().into()
        } else {
            self.vertex
                .in_neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    pub fn out_neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyPathFromVertex {
        if t_start.is_none() && t_end.is_none() {
            self.vertex.out_neighbours().into()
        } else {
            self.vertex
                .out_neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    pub fn __repr__(&self) -> String {
        format!("Vertex({})", self.id)
    }
}

#[pyclass(name = "Vertices")]
pub struct PyVertices {
    pub(crate) vertices: Vertices<Box<dyn GraphViewOps>>,
}

impl From<Vertices<Box<dyn GraphViewOps>>> for PyVertices {
    fn from(value: Vertices<Box<dyn GraphViewOps>>) -> Self {
        Self { vertices: value }
    }
}

#[pymethods]
impl PyVertices {
    fn __iter__(&self) -> PyVertexIterator {
        let iter = Box::new(self.vertices.iter().map(|v| PyVertex::from(v)));
        PyVertexIterator { iter }
    }

    fn id(&self) -> U64Iterator {
        self.vertices.id().into()
    }

    fn out_neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyPathFromGraph {
        if t_start.is_none() && t_end.is_none() {
            self.vertices.out_neighbours().into()
        } else {
            self.vertices
                .out_neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn in_neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyPathFromGraph {
        if t_start.is_none() && t_end.is_none() {
            self.vertices.in_neighbours().into()
        } else {
            self.vertices
                .in_neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyPathFromGraph {
        if t_start.is_none() && t_end.is_none() {
            self.vertices.neighbours().into()
        } else {
            self.vertices
                .neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn in_degree(&self, t_start: Option<i64>, t_end: Option<i64>) -> NestedUsizeIter {
        if t_start.is_none() && t_end.is_none() {
            self.vertices.in_degree().into()
        } else {
            self.vertices
                .in_degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn out_degree(
        &self,
        py: Python,
        t_start: Option<i64>,
        t_end: Option<i64>,
    ) -> PyResult<DegreeIterable> {
        if t_start.is_none() && t_end.is_none() {
            self.vertices.out_degree().into()
        } else {
            self.vertices
                .out_degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn degree(&self, t_start: Option<i64>, t_end: Option<i64>) -> NestedUsizeIter {
        if t_start.is_none() && t_end.is_none() {
            self.vertices.degree().into()
        } else {
            self.vertices
                .degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
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
    path: PathFromGraph<Box<dyn GraphViewOps>>,
}

#[pymethods]
impl PyPathFromGraph {
    fn __iter__(&self) -> NestedVertexIterator {
        self.path.iter().into()
    }

    fn id(&self) -> NestedU64Iterator {
        self.path.id().into()
    }

    fn out_neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> Self {
        if t_start.is_none() && t_end.is_none() {
            self.path.out_neighbours().into()
        } else {
            self.path
                .out_neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn in_neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> Self {
        if t_start.is_none() && t_end.is_none() {
            self.path.in_neighbours().into()
        } else {
            self.path
                .in_neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> Self {
        if t_start.is_none() && t_end.is_none() {
            self.path.neighbours().into()
        } else {
            self.path
                .neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn in_degree(&self, t_start: Option<i64>, t_end: Option<i64>) -> NestedUsizeIter {
        if t_start.is_none() && t_end.is_none() {
            self.path.in_degree().into()
        } else {
            self.path
                .in_degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn out_degree(&self, t_start: Option<i64>, t_end: Option<i64>) -> NestedUsizeIter {
        if t_start.is_none() && t_end.is_none() {
            self.path.out_degree().into()
        } else {
            self.path
                .out_degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn degree(&self, t_start: Option<i64>, t_end: Option<i64>) -> NestedUsizeIter {
        if t_start.is_none() && t_end.is_none() {
            self.path.degree().into()
        } else {
            self.path
                .degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
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

#[pyclass(name = "PathFromVertex")]
pub struct PyPathFromVertex {
    path: PathFromVertex<Box<dyn GraphViewOps>>,
}

impl From<PathFromVertex<Box<dyn GraphViewOps>>> for PyPathFromVertex {
    fn from(value: PathFromVertex<Box<dyn GraphViewOps>>) -> Self {
        Self { path: value }
    }
}

#[pymethods]
impl PyPathFromVertex {
    fn __iter__(&self) -> PyVertexIterator {
        self.path.iter().into()
    }

    fn id(&self) -> U64Iterator {
        self.path.id().into()
    }

    fn out_neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> Self {
        if t_start.is_none() && t_end.is_none() {
            self.path.out_neighbours().into()
        } else {
            self.path
                .out_neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn in_neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> Self {
        if t_start.is_none() && t_end.is_none() {
            self.path.in_neighbours().into()
        } else {
            self.path
                .in_neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn neighbours(&self, t_start: Option<i64>, t_end: Option<i64>) -> Self {
        if t_start.is_none() && t_end.is_none() {
            self.path.neighbours().into()
        } else {
            self.path
                .neighbours_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn in_degree(&self, t_start: Option<i64>, t_end: Option<i64>) -> USizeIter {
        if t_start.is_none() && t_end.is_none() {
            self.path.in_degree().into()
        } else {
            self.path
                .in_degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn out_degree(&self, t_start: Option<i64>, t_end: Option<i64>) -> USizeIter {
        if t_start.is_none() && t_end.is_none() {
            self.path.out_degree().into()
        } else {
            self.path
                .out_degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
    }

    fn degree(&self, t_start: Option<i64>, t_end: Option<i64>) -> USizeIter {
        if t_start.is_none() && t_end.is_none() {
            self.path.degree().into()
        } else {
            self.path
                .degree_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX))
                .into()
        }
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
pub struct NestedVertexIterator {
    pub(crate) iter: Box<dyn Iterator<Item = PyPathFromVertex> + Send>,
}

impl IntoIterator for NestedVertexIterator {
    type Item = PyPathFromVertex;
    type IntoIter = Box<dyn Iterator<Item = PyPathFromVertex> + Send>;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

#[pymethods]
impl NestedVertexIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyPathFromVertex> {
        slf.iter.next()
    }
}
