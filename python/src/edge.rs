use crate::dynamic::DynamicGraph;
use crate::vertex::PyVertex;
use crate::wrappers::prop::Prop;
use docbrown::db::edge::EdgeView;
use docbrown::db::view_api::*;
use itertools::Itertools;
use pyo3::{pyclass, pymethods, PyRef, PyRefMut};
use std::collections::HashMap;

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

    pub fn explode(&self) -> Vec<PyEdge> {
        self.edge
            .explode()
            .into_iter()
            .map(|e| e.into())
            .collect::<Vec<PyEdge>>()
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

#[pyclass(name = "Edges")]
pub struct PyEdges {
    builder: Box<dyn Fn() -> BoxedIter<EdgeView<DynamicGraph>> + Send + 'static>,
}

impl PyEdges {
    fn iter(&self) -> BoxedIter<EdgeView<DynamicGraph>> {
        (self.builder)()
    }

    fn py_iter(&self) -> BoxedIter<PyEdge> {
        Box::new(self.iter().map(|e| e.into()))
    }
}

#[pymethods]
impl PyEdges {
    fn __iter__(&self) -> PyEdgeIter {
        PyEdgeIter {
            iter: Box::new(self.py_iter()),
        }
    }

    fn collect(&self) -> Vec<PyEdge> {
        self.py_iter().collect()
    }

    fn first(&self) -> Option<PyEdge> {
        self.py_iter().next()
    }

    fn count(&self) -> usize {
        self.py_iter().count()
    }

    fn explode(&self) -> PyEdgeIter {
        let res: BoxedIter<EdgeView<DynamicGraph>> =
            Box::new(self.iter().flat_map(|e| e.explode()));
        res.into()
    }
}

impl<F: Fn() -> BoxedIter<EdgeView<DynamicGraph>> + Send + 'static> From<F> for PyEdges {
    fn from(value: F) -> Self {
        Self {
            builder: Box::new(value),
        }
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
