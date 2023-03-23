use std::{collections::HashMap, sync::Arc};

use crate::wrappers;
use crate::wrappers::Perspective;
use crate::{graph::Graph, wrappers::*};
use docbrown_core::tgraph::EdgeRef;
use docbrown_db::graph_window;
use docbrown_db::view_api::*;
use itertools::Itertools;
use pyo3::prelude::*;
use pyo3::types::PyIterator;

#[pyclass]
pub struct GraphWindowSet {
    window_set: graph_window::GraphWindowSet,
}

impl From<graph_window::GraphWindowSet> for GraphWindowSet {
    fn from(value: graph_window::GraphWindowSet) -> Self {
        GraphWindowSet::new(value)
    }
}

impl GraphWindowSet {
    pub fn new(window_set: graph_window::GraphWindowSet) -> GraphWindowSet {
        GraphWindowSet { window_set }
    }
}

#[pymethods]
impl GraphWindowSet {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }
    fn __next__(mut slf: PyRefMut<'_, Self>, py: Python) -> Option<WindowedGraph> {
        let windowed_graph = slf.window_set.next()?;
        Some(windowed_graph.into())
    }
}

#[pyclass]
pub struct WindowedGraph {
    pub(crate) graph_w: graph_window::WindowedGraph,
}

impl From<graph_window::WindowedGraph> for WindowedGraph {
    fn from(value: graph_window::WindowedGraph) -> Self {
        WindowedGraph { graph_w: value }
    }
}

#[pymethods]
impl WindowedGraph {
    #[new]
    pub fn new(graph: &Graph, t_start: i64, t_end: i64) -> Self {
        Self {
            graph_w: graph_window::WindowedGraph::new(graph.graph.clone(), t_start, t_end),
        }
    }

    //******  Metrics APIs ******//

    pub fn earliest_time(&self) -> Option<i64> { self.graph_w.earliest_time() }

    pub fn latest_time(&self) -> Option<i64> { self.graph_w.latest_time() }

    pub fn num_edges(&self) -> usize {self.graph_w.num_edges()}

    pub fn num_vertices(&self) -> usize {self.graph_w.num_vertices()}

    pub fn has_vertex(&self, v: &PyAny) -> bool {
        if let Ok(v) = v.extract::<String>() {
            self.graph_w.has_vertex(v)
        }
        else if let Ok(v) = v.extract::<u64>(){
             self.graph_w.has_vertex(v)
        }
        else {
            panic!("Input must be a string or integer.")
        }
    }

    pub fn has_edge(&self, src: &PyAny, dst: &PyAny) -> bool {
        if src.extract::<String>().is_ok() && dst.extract::<String>().is_ok() {
            self.graph_w.has_edge(
                src.extract::<String>().unwrap(),
                dst.extract::<String>().unwrap(),
            )
        }
        else if  src.extract::<u64>().is_ok() && dst.extract::<u64>().is_ok() {
            self.graph_w.has_edge(
                src.extract::<u64>().unwrap(),
                dst.extract::<u64>().unwrap(),
            )
        }
        else {
            //FIXME This probably should just throw an error not fully panic
            panic!("Types of src and dst must be the same (either Int or str)")
        }
    }

    //******  Getter APIs ******//

    pub fn vertex(slf: PyRef<'_, Self>, v: u64) -> Option<WindowedVertex> {
        let v = slf.graph_w.vertex(v)?;
        let g: Py<Self> = slf.into();
        Some(WindowedVertex::new(g, v))
    }

    pub fn vertex_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: self.graph_w.vertices().id(),
        }
    }

    pub fn vertices(slf: PyRef<'_, Self>) -> WindowedVertices {
        let g: Py<Self> = slf.into();
        WindowedVertices { graph: g }
    }

    pub fn edge(&self, src: u64, dst: u64) -> Option<WindowedEdge> {
        self.graph_w.edge(src, dst).map(|we| we.into())
    }

    pub fn edges(&self) -> WindowedEdgeIterator {
        WindowedEdgeIterator {
            iter: Box::new(self.graph_w.edges().map(|te| te.into())),
        }
    }
}

#[pyclass]
pub struct WindowedVertex {
    #[pyo3(get)]
    pub id: u64,
    pub(crate) graph: Py<WindowedGraph>,
    pub(crate) vertex_w: graph_window::WindowedVertex,
}

//TODO need to implement but would need to change a lot of things
//Have to rely on internal from for the moment
// impl From<graph_window::WindowedVertex> for WindowedVertex {
//     fn from(value: graph_window::WindowedVertex) ->WindowedVertex {
//
//     }
// }


impl WindowedVertex {
    fn from(&self, value: graph_window::WindowedVertex) -> WindowedVertex {
        WindowedVertex {
            id: value.id(),
            graph: self.graph.clone(),
            vertex_w: value,
        }
    }

    pub(crate) fn new(
        graph: Py<WindowedGraph>,
        vertex: graph_window::WindowedVertex,
    ) -> WindowedVertex {
        WindowedVertex {
            graph,
            id: vertex.id(),
            vertex_w: vertex,
        }
    }
}

#[pymethods]
impl WindowedVertex {

    pub fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.vertex_w
            .prop(name)
            .into_iter()
            .map(|(t, p)| (t, p.into()))
            .collect_vec()
    }

    pub fn props(&self) -> HashMap<String, Vec<(i64, Prop)>> {
        self.vertex_w
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

    pub fn degree(&self) -> usize {
        self.vertex_w.degree()
    }

    pub fn degree_window(&self, t_start: i64, t_end: i64) -> usize {
        self.vertex_w.degree_window(t_start, t_end)
    }

    pub fn in_degree(&self) -> usize {
        self.vertex_w.in_degree()
    }

    pub fn in_degree_window(&self, t_start: i64, t_end: i64) -> usize {
        self.vertex_w.in_degree_window(t_start, t_end)
    }

    pub fn out_degree(&self) -> usize {
        self.vertex_w.out_degree()
    }

    pub fn out_degree_window(&self, t_start: i64, t_end: i64) -> usize {
        self.vertex_w.out_degree_window(t_start, t_end)
    }

    pub fn edges(&self) -> WindowedEdgeIterator {
        WindowedEdgeIterator {
            iter: Box::new(self.vertex_w.edges().map(|te| te.into())),
        }
    }

    pub fn edges_window(&self, t_start: i64, t_end: i64) -> WindowedEdgeIterator {
        WindowedEdgeIterator {
            iter: Box::new(self.vertex_w.edges_window(t_start, t_end).map(|te| te.into())),
        }
    }

    // pub fn in_edges(&self) -> WindowedEdgeIterator {
    //     WindowedEdgeIterator {
    //         iter: Box::new(self.vertex_w.in_edges().map(|te| te.into())),
    //     }
    // }

    #[args(t_start=None, t_end=None)]
    pub fn in_edges(&self, t_start: Option<i64>, t_end: Option<i64>1) -> WindowedEdgeIterator {
        WindowedEdgeIterator {
            iter: Box::new(self.vertex_w.in_edges_window(t_start.unwrap_or(i64::MIN), t_end.unwrap_or(i64::MAX)).map(|te| te.into())),
        }
    }

    pub fn out_edges(&self) -> WindowedEdgeIterator {
        WindowedEdgeIterator {
            iter: Box::new(self.vertex_w.out_edges().map(|te| te.into())),
        }
    }

    pub fn out_edges_window(&self, t_start: i64, t_end: i64) -> WindowedEdgeIterator {
        WindowedEdgeIterator {
            iter: Box::new(self.vertex_w.out_edges_window(t_start, t_end).map(|te| te.into())),
        }
    }

    pub fn neighbours(&self) -> WindowedVertexIterable {
        WindowedVertexIterable {
            graph: self.graph.clone(),
            operations: vec![Operations::Neighbours],
            start_at: Some(self.id),
        }
    }

    pub fn neighbours_window(&self, t_start: i64, t_end: i64) -> WindowedVertexIterable {
        WindowedVertexIterable {
            graph: self.graph.clone(),
            operations: vec![Operations::NeighboursWindow{t_start, t_end}],
            start_at: Some(self.id),
        }
    }

    pub fn in_neighbours(&self) -> WindowedVertexIterable {
        WindowedVertexIterable {
            graph: self.graph.clone(),
            operations: vec![Operations::InNeighbours],
            start_at: Some(self.id),
        }
    }


    pub fn in_neighbours_window(&self, t_start: i64, t_end: i64) -> WindowedVertexIterable {
        WindowedVertexIterable {
            graph: self.graph.clone(),
            operations: vec![Operations::InNeighboursWindow{t_start, t_end}],
            start_at: Some(self.id)
        }
    }

    pub fn out_neighbours(&self) -> WindowedVertexIterable {
        WindowedVertexIterable {
            graph: self.graph.clone(),
            operations: vec![Operations::OutNeighbours],
            start_at: Some(self.id)
        }
    }

    pub fn out_neighbours_window(&self, t_start: i64, t_end: i64) -> WindowedVertexIterable {
        WindowedVertexIterable {
            graph: self.graph.clone(),
            operations: vec![Operations::OutNeighboursWindow{t_start, t_end}],
            start_at: Some(self.id)
        }
    }

    pub fn neighbours_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: Box::new(self.vertex_w.neighbours().id()),
        }
    }

    pub fn in_neighbours_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: Box::new(self.vertex_w.in_neighbours().id()),
        }
    }

    pub fn out_neighbours_ids(&self) -> VertexIdsIterator {
        VertexIdsIterator {
            iter: Box::new(self.vertex_w.out_neighbours().id()),
        }
    }

    pub fn __repr__(&self) -> String {
        format!("Vertex({})", self.id)
    }
}

#[pyclass]
pub struct WindowedEdge {
    #[pyo3(get)]
    pub edge_id: usize,
    #[pyo3(get)]
    pub src: u64,
    #[pyo3(get)]
    pub dst: u64,
    #[pyo3(get)]
    pub time: Option<i64>,
    pub is_remote: bool,
    pub(crate) edge_w: graph_window::WindowedEdge,
}

impl From<graph_window::WindowedEdge> for WindowedEdge {
    fn from(value: graph_window::WindowedEdge) -> WindowedEdge {
        WindowedEdge {
            edge_id: value.id(),
            src: value.src().id(),
            dst: value.dst().id(),
            time: None,
            is_remote: false,
            edge_w: value,
        }
    }
}

#[pymethods]
impl WindowedEdge {
    pub fn prop(&self, name: String) -> Vec<(i64, Prop)> {
        self.edge_w
            .prop(name)
            .into_iter()
            .map(|(t, p)| (t, p.into()))
            .collect_vec()
    }

    pub fn id(&self) -> usize {
        self.edge_w.id()
    }

    fn src(&self) -> u64 {
        //FIXME can't currently return the WindowedVertex as can't create a Py<WindowedGraph>
        self.edge_w.src().id()
    }

    fn dst(&self) -> u64 {
        //FIXME can't currently return the WindowedVertex as can't create a Py<WindowedGraph>
        self.edge_w.dst().id()
    }
}
