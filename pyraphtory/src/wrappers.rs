use pyo3::prelude::*;
use std::ops::Range;

use dbc::tpartition;
use docbrown_core as dbc;

#[pyclass]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Direction {
    OUT,
    IN,
    BOTH,
}

impl Direction {
    pub(crate) fn convert(&self) -> dbc::Direction {
        match self {
            Direction::OUT => dbc::Direction::OUT,
            Direction::IN => dbc::Direction::IN,
            Direction::BOTH => dbc::Direction::BOTH,
        }
    }
}

#[derive(FromPyObject, Debug)]
pub enum Prop {
    Str(String),
    I64(i64),
    U64(u64),
    F64(f64),
    Bool(bool),
}

impl Prop {
    pub(crate) fn convert(&self) -> dbc::Prop {
        match self {
            Prop::Str(string) => dbc::Prop::Str(string.clone()),
            Prop::I64(i64) => dbc::Prop::I64(*i64),
            Prop::U64(u64) => dbc::Prop::U64(*u64),
            Prop::F64(f64) => dbc::Prop::F64(*f64),
            Prop::Bool(bool) => dbc::Prop::Bool(*bool),
        }
    }
}

#[pyclass]
pub struct TEdge {
    #[pyo3(get)]
    pub src: u64,
    #[pyo3(get)]
    pub dst: u64,
    #[pyo3(get)]
    pub t: Option<i64>,
    #[pyo3(get)]
    pub is_remote: bool,
}

impl TEdge {
    pub(crate) fn convert(edge: tpartition::TEdge) -> TEdge {
        let tpartition::TEdge {
            src,
            dst,
            t,
            is_remote,
        } = edge;
        TEdge {
            src,
            dst,
            t,
            is_remote,
        }
    }
}

#[pyclass]
pub struct TVertex {
    #[pyo3(get)]
    pub g_id: u64,
}

impl TVertex {
    pub(crate) fn convert(vertex: tpartition::TVertex) -> TVertex {
        let tpartition::TVertex { g_id, .. } = vertex;
        TVertex { g_id }
    }
}

#[pyclass]
pub struct VertexIterator {
    pub(crate) iter: Box<dyn Iterator<Item = u64> + Send>,
}

#[pymethods]
impl VertexIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<u64> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct EdgeIterator {
    pub(crate) iter: std::vec::IntoIter<TEdge>,
}

#[pymethods]
impl EdgeIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<TEdge> {
        slf.iter.next()
    }
}
