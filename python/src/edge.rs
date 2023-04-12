use crate::dynamic::DynamicGraph;
use crate::vertex::PyVertex;
use crate::wrappers::prop::Prop;
use docbrown::db::edge::EdgeView;
use docbrown::db::view_api::*;
use itertools::Itertools;
use pyo3::{PyAny, pyclass, pymethods, PyRef, PyRefMut, PyResult};
use std::collections::HashMap;
use docbrown::db::graph_window::WindowSet;
use crate::util::*;

#[pyclass(name = "Edge")]
pub struct PyEdge {
    pub(crate) edge: EdgeView<DynamicGraph>,
}

impl From<EdgeView<DynamicGraph>> for PyEdge {
    fn from(value: EdgeView<DynamicGraph>) -> Self {
        Self { edge: value }
    }
}

#[pymethods]
impl PyEdge {
    pub fn __getitem__(&self, name: String) -> Option<Prop> {
        self.property(name, Some(true))
    }

    pub fn property(&self, name: String, include_static: Option<bool>) -> Option<Prop> {
        let include_static = include_static.unwrap_or(true);
        self.edge
            .property(name, include_static)
            .map(|prop| prop.into())
    }

    pub fn property_history(&self, name: String) -> Vec<(i64, Prop)> {
        self.edge
            .property_history(name)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    pub fn properties(&self, include_static: Option<bool>) -> HashMap<String, Prop> {
        let include_static = include_static.unwrap_or(true);
        self.edge
            .properties(include_static)
            .into_iter()
            .map(|(k, v)| (k, v.into()))
            .collect()
    }

    pub fn property_histories(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.edge
            .property_histories()
            .into_iter()
            .map(|(k, v)| (k, v.into_iter().map(|(t, p)| (t, p.into())).collect()))
            .collect()
    }

    pub fn property_names(&self, include_static: Option<bool>) -> Vec<String> {
        let include_static = include_static.unwrap_or(true);
        self.edge.property_names(include_static)
    }

    pub fn has_property(&self, name: String, include_static: Option<bool>) -> bool {
        let include_static = include_static.unwrap_or(true);
        self.edge.has_property(name, include_static)
    }

    pub fn has_static_property(&self, name: String) -> bool {
        self.edge.has_static_property(name)
    }
    pub fn static_property(&self, name: String) -> Option<Prop> {
        self.edge.static_property(name).map(|prop| prop.into())
    }

    pub fn id(&self) -> usize {
        self.edge.id()
    }

    fn src(&self) -> PyVertex {
        self.edge.src().into()
    }

    fn dst(&self) -> PyVertex {
        self.edge.dst().into()
    }

    //******  Perspective APIS  ******//
    pub fn start(&self) -> Option<i64> {
        self.edge.start()
    }

    pub fn end(&self) -> Option<i64> {
        self.edge.end()
    }

    fn expanding(&self, step: u64, start: Option<i64>, end: Option<i64>) -> PyEdgeWindowSet {
        self.edge.expanding(step, start, end).into()
    }

    fn rolling(
        &self,
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PyEdgeWindowSet {
        self.edge.rolling(window, step, start, end).into()
    }

    pub fn window(&self, t_start: Option<i64>, t_end: Option<i64>) -> PyEdge {
        window_impl(&self.edge, t_start, t_end).into()
    }

    pub fn at(&self, end: i64) -> PyEdge {
        self.edge.at(end).into()
    }

    pub fn through(&self, perspectives: &PyAny) -> PyResult<PyEdgeWindowSet> {
        through_impl(&self.edge, perspectives).map(|p| p.into())
    }

    pub fn __repr__(&self) -> String {
        let properties = &self
            .properties(Some(true))
            .iter()
            .map(|(k, v)| k.to_string() + " : " + &v.to_string())
            .join(", ");

        let source = self.edge.src().name();
        let target = self.edge.dst().name();
        if properties.is_empty() {
            format!(
                "Edge(source={}, target={})",
                source.trim_matches('"'),
                target.trim_matches('"')
            )
        } else {
            let property_string: String = "{".to_string() + &properties + "}";
            format!(
                "Edge(source={}, target={}, properties={})",
                source.trim_matches('"'),
                target.trim_matches('"'),
                property_string
            )
        }
    }
}

#[pyclass(name = "EdgeIter")]
pub struct PyEdgeIter {
    iter: Box<dyn Iterator<Item = PyEdge> + Send>,
}

#[pymethods]
impl PyEdgeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyEdge> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = PyEdge> + Send>> for PyEdgeIter {
    fn from(value: Box<dyn Iterator<Item = PyEdge> + Send>) -> Self {
        Self { iter: value }
    }
}

impl From<Box<dyn Iterator<Item = EdgeView<DynamicGraph>> + Send>> for PyEdgeIter {
    fn from(value: Box<dyn Iterator<Item = EdgeView<DynamicGraph>> + Send>) -> Self {
        Self {
            iter: Box::new(value.map(|e| e.into())),
        }
    }
}

#[pyclass(name = "NestedEdgeIter")]
pub struct PyNestedEdgeIter {
    iter: BoxedIter<PyEdgeIter>,
}

#[pymethods]
impl PyNestedEdgeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyEdgeIter> {
        slf.iter.next()
    }
}

impl From<BoxedIter<BoxedIter<EdgeView<DynamicGraph>>>> for PyNestedEdgeIter {
    fn from(value: BoxedIter<BoxedIter<EdgeView<DynamicGraph>>>) -> Self {
        Self {
            iter: Box::new(value.map(|e| e.into())),
        }
    }
}

#[pyclass(name = "EdgeWindowSet")]
pub struct PyEdgeWindowSet {
    window_set: WindowSet<EdgeView<DynamicGraph>>,
}

impl From<WindowSet<EdgeView<DynamicGraph>>> for PyEdgeWindowSet {
    fn from(value: WindowSet<EdgeView<DynamicGraph>>) -> Self {
        Self { window_set: value }
    }
}

#[pymethods]
impl PyEdgeWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<PyEdge> {
        slf.window_set.next().map(|g| g.into())
    }
}
