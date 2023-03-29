use itertools::Itertools;
use pyo3::prelude::*;
use std::borrow::Borrow;

use docbrown_core as db_c;
use docbrown_db::vertices::Vertices;
use docbrown_db::view_api::*;
use docbrown_db::{graph_window, perspective};

use crate::graph_view::{WindowedEdge, WindowedGraph, WindowedVertex};
use crate::vertex::{PyPathFromGraph, PyPathFromVertex};

#[derive(Copy, Clone)]
pub(crate) enum Direction {
    OUT,
    IN,
    BOTH,
    OutWindow { t_start: i64, t_end: i64 },
    InWindow { t_start: i64, t_end: i64 },
    BothWindow { t_start: i64, t_end: i64 },
}

impl From<Direction> for db_c::Direction {
    fn from(d: Direction) -> db_c::Direction {
        match d {
            Direction::OUT => db_c::Direction::OUT,
            Direction::IN => db_c::Direction::IN,
            Direction::BOTH => db_c::Direction::BOTH,
            Direction::OutWindow { t_start, t_end } => db_c::Direction::OUT,
            Direction::InWindow { t_start, t_end } => db_c::Direction::IN,
            Direction::BothWindow { t_start, t_end } => db_c::Direction::BOTH,
        }
    }
}

#[derive(Copy, Clone)]
pub(crate) enum Operations {
    OutNeighbours,
    InNeighbours,
    Neighbours,
    InNeighboursWindow { t_start: i64, t_end: i64 },
    OutNeighboursWindow { t_start: i64, t_end: i64 },
    NeighboursWindow { t_start: i64, t_end: i64 },
}

#[derive(FromPyObject, Debug, Clone)]
pub enum Prop {
    Str(String),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Bool(bool),
}

impl IntoPy<PyObject> for Prop {
    fn into_py(self, py: Python<'_>) -> PyObject {
        match self {
            Prop::Str(s) => s.into_py(py),
            Prop::I32(i32) => i32.into_py(py),
            Prop::I64(i64) => i64.into_py(py),
            Prop::U32(u32) => u32.into_py(py),
            Prop::U64(u64) => u64.into_py(py),
            Prop::F32(f32) => f32.into_py(py),
            Prop::F64(f64) => f64.into_py(py),
            Prop::Bool(bool) => bool.into_py(py),
        }
    }
}

impl From<Prop> for db_c::Prop {
    fn from(prop: Prop) -> db_c::Prop {
        match prop {
            Prop::Str(string) => db_c::Prop::Str(string.clone()),
            Prop::I32(i32) => db_c::Prop::I32(i32),
            Prop::I64(i64) => db_c::Prop::I64(i64),
            Prop::U32(u32) => db_c::Prop::U32(u32),
            Prop::U64(u64) => db_c::Prop::U64(u64),
            Prop::F32(f32) => db_c::Prop::F32(f32),
            Prop::F64(f64) => db_c::Prop::F64(f64),
            Prop::Bool(bool) => db_c::Prop::Bool(bool),
        }
    }
}

impl From<db_c::Prop> for Prop {
    fn from(prop: db_c::Prop) -> Prop {
        match prop {
            db_c::Prop::Str(string) => Prop::Str(string.clone()),
            db_c::Prop::I32(i32) => Prop::I32(i32),
            db_c::Prop::I64(i64) => Prop::I64(i64),
            db_c::Prop::U32(u32) => Prop::U32(u32),
            db_c::Prop::U64(u64) => Prop::U64(u64),
            db_c::Prop::F32(f32) => Prop::F32(f32),
            db_c::Prop::F64(f64) => Prop::F64(f64),
            db_c::Prop::Bool(bool) => Prop::Bool(bool),
        }
    }
}

#[pyclass]
pub struct U64Iterator {
    iter: Box<dyn Iterator<Item = u64> + Send>,
}

#[pymethods]
impl U64Iterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<u64> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = u64> + Send>> for U64Iterator {
    fn from(value: Box<dyn Iterator<Item = u64> + Send>) -> Self {
        Self { iter: value }
    }
}

#[pyclass]
pub struct NestedU64Iterator {
    iter: Box<dyn Iterator<Item = U64Iterator> + Send>,
}

#[pymethods]
impl NestedU64Iterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<U64Iterator> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = U64Iterator> + Send>> for NestedU64Iterator {
    fn from(value: Box<dyn Iterator<Item = U64Iterator> + Send>) -> Self {
        Self { iter: value }
    }
}

impl From<Box<dyn Iterator<Item = Box<dyn Iterator<Item = u64> + Send>> + Send>>
    for NestedU64Iterator
{
    fn from(value: Box<dyn Iterator<Item = Box<dyn Iterator<Item = u64> + Send>> + Send>) -> Self {
        let iter = Box::new(value.map(|iter| iter.into()));
        Self { iter }
    }
}

#[pyclass]
pub struct NestedUsizeIter {
    iter: Box<dyn Iterator<Item = PyResult<DegreeIterable>> + Send>,
}

#[pymethods]
impl NestedUsizeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> PyResult<Option<DegreeIterable>> {
        slf.iter.next().transpose()
    }
}

#[pyclass]
pub struct USizeIter {
    iter: Box<dyn Iterator<Item = usize> + Send>,
}

#[pymethods]
impl USizeIter {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<usize> {
        slf.iter.next()
    }
}

impl From<Box<dyn Iterator<Item = usize> + Send>> for USizeIter {
    fn from(value: Box<dyn Iterator<Item = usize> + Send>) -> Self {
        Self { iter: value }
    }
}


#[derive(Clone)]
#[pyclass]
pub struct Perspective {
    pub start: Option<i64>,
    pub end: Option<i64>,
}

#[pymethods]
impl Perspective {
    #[new]
    #[pyo3(signature = (start=None, end=None))]
    fn new(start: Option<i64>, end: Option<i64>) -> Self {
        Perspective { start, end }
    }

    #[staticmethod]
    #[pyo3(signature = (step, start=None, end=None))]
    fn expanding(step: u64, start: Option<i64>, end: Option<i64>) -> PerspectiveSet {
        PerspectiveSet {
            ps: perspective::Perspective::expanding(step, start, end),
        }
    }

    #[staticmethod]
    #[pyo3(signature = (window, step=None, start=None, end=None))]
    fn rolling(
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PerspectiveSet {
        PerspectiveSet {
            ps: perspective::Perspective::rolling(window, step, start, end),
        }
    }
}

impl From<perspective::Perspective> for Perspective {
    fn from(value: perspective::Perspective) -> Self {
        Perspective {
            start: value.start,
            end: value.end,
        }
    }
}

impl From<Perspective> for perspective::Perspective {
    fn from(value: Perspective) -> Self {
        perspective::Perspective {
            start: value.start,
            end: value.end,
        }
    }
}

#[pyclass]
#[derive(Clone)]
pub struct PerspectiveSet {
    pub(crate) ps: perspective::PerspectiveSet,
}
