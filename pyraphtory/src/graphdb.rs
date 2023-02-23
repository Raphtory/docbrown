use docbrown_core as dbc;
use docbrown_db::graphdb as gdb;
use pyo3::exceptions;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::wrappers::Direction;
use crate::wrappers::EdgeIterator;
use crate::wrappers::Prop;
use crate::wrappers::TEdge;
use crate::wrappers::VertexIterator;

#[pyclass]
pub struct GraphDB {
    pub(crate) graphdb: gdb::GraphDB,
}

#[pymethods]
impl GraphDB {
    #[new]
    pub fn new(nr_shards: usize) -> Self {
        Self {
            graphdb: gdb::GraphDB::new(nr_shards),
        }
    }

    #[staticmethod]
    pub fn load_from_file(path: String) -> PyResult<Self> {
        let file_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), &path].iter().collect();

        match gdb::GraphDB::load_from_file(file_path) {
            Ok(g) => Ok(GraphDB { graphdb: g }),
            Err(e) => Err(exceptions::PyException::new_err(format!(
                "Failed to load graph from the files. Reason: {}",
                e.to_string()
            ))),
        }
    }

    pub fn save_to_file(&self, path: String) -> PyResult<()> {
        match self.graphdb.save_to_file(Path::new(&path)) {
            Ok(()) => Ok(()),
            Err(e) => Err(exceptions::PyException::new_err(format!(
                "Failed to save graph to the files. Reason: {}",
                e.to_string()
            ))),
        }
    }

    pub fn len(&self) -> usize {
        self.graphdb.len()
    }

    pub fn edges_len(&self) -> usize {
        self.graphdb.edges_len()
    }

    pub fn contains(&self, v: u64) -> bool {
        self.graphdb.contains(v)
    }

    pub fn contains_window(&self, v: u64, t_start: i64, t_end: i64) -> bool {
        self.graphdb.contains_window(v, t_start, t_end)
    }

    pub fn add_vertex(&self, v: u64, t: i64, props: HashMap<String, Prop>) {
        self.graphdb.add_vertex(
            t,
            v,
            &props
                .into_iter()
                .map(|(key, value)| (key, value.convert()))
                .collect::<Vec<(String, dbc::Prop)>>(),
        )
    }

    pub fn add_edge(&self, src: u64, dst: u64, t: i64, props: HashMap<String, Prop>) {
        self.graphdb.add_edge(
            t,
            src,
            dst,
            &props
                .into_iter()
                .map(|f| (f.0.clone(), f.1.convert()))
                .collect::<Vec<(String, dbc::Prop)>>(),
        )
    }

    pub fn degree(&self, v: u64, d: Direction) -> usize {
        self.graphdb.degree(v, d.convert())
    }

    pub fn degree_window(&self, v: u64, t_start: i64, t_end: i64, d: Direction) -> usize {
        self.graphdb.degree_window(v, t_start, t_end, d.convert())
    }

    pub fn vertex_ids(&self) -> VertexIterator {
        VertexIterator {
            iter: self.graphdb.vertex_ids(),
        }
    }

    pub fn neighbours(&self, v: u64, d: Direction) -> EdgeIterator {
        EdgeIterator {
            iter: self
                .graphdb
                .neighbours(v, d.convert())
                .map(|f| TEdge::convert(f))
                .collect::<Vec<_>>()
                .into_iter(),
        }
    }

    pub fn neighbours_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> EdgeIterator {
        EdgeIterator {
            iter: self
                .graphdb
                .neighbours_window(v, t_start, t_end, d.convert())
                .map(|f| TEdge::convert(f))
                .collect::<Vec<_>>()
                .into_iter(),
        }
    }

    pub fn neighbours_window_t(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> EdgeIterator {
        EdgeIterator {
            iter: self
                .graphdb
                .neighbours_window_t(v, t_start, t_end, d.convert())
                .map(|f| TEdge::convert(f))
                .collect::<Vec<_>>()
                .into_iter(),
        }
    }
}
