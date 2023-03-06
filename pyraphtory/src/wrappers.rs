use pyo3::prelude::*;

use db_c::tgraph_shard;
use docbrown_core as db_c;
use docbrown_db as db_db;

use crate::graph_window::{WindowedEdge, WindowedVertex};
use docbrown_db::perspective;

#[pyclass]
#[derive(Copy, Clone, PartialEq, Eq)]
pub enum Direction {
    OUT,
    IN,
    BOTH,
}

impl From<Direction> for db_c::Direction {
    fn from(d: Direction) -> db_c::Direction {
        match d {
            Direction::OUT => db_c::Direction::OUT,
            Direction::IN => db_c::Direction::IN,
            Direction::BOTH => db_c::Direction::BOTH,
        }
    }
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
pub struct VertexIdsIterator {
    pub(crate) iter: Box<dyn Iterator<Item = u64> + Send>,
}

#[pymethods]
impl VertexIdsIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<u64> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct WindowedVertexIterator {
    pub(crate) iter: Box<dyn Iterator<Item = WindowedVertex> + Send>,
}

#[pymethods]
impl WindowedVertexIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<WindowedVertex> {
        slf.iter.next()
    }
}

#[pyclass]
pub struct WindowedEdgeIterator {
    pub(crate) iter: Box<dyn Iterator<Item = WindowedEdge> + Send>,
}

#[pymethods]
impl WindowedEdgeIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<WindowedEdge> {
        slf.iter.next()
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
        Perspective {
            start,
            end,
        }
    }

    #[staticmethod]
    #[pyo3(signature = (start, end, increment))]
    pub fn range(start: i64, end: i64, increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            ps: perspective::Perspective::range(start, end, increment)
        }
    }

    #[staticmethod]
    #[pyo3(signature = (increment))]
    pub fn walk(increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            ps: perspective::Perspective::walk(increment)
        }
    }

    #[staticmethod]
    #[pyo3(signature = (start, increment))]
    pub fn depart(start: i64, increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            ps: perspective::Perspective::depart(start, increment)
        }
    }

    #[staticmethod]
    #[pyo3(signature = (end, increment))]
    pub fn climb(end: i64, increment: u64) -> PerspectiveSet {
        PerspectiveSet {
            ps: perspective::Perspective::climb(end, increment)
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
    pub(crate) ps: perspective::PerspectiveSet
}

#[pymethods]
impl PerspectiveSet {
    fn window(&mut self, size: u64) -> PerspectiveSet {
        PerspectiveSet {
            ps: self.ps.window(size)
        }
    }
}