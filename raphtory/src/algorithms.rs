use crate::graph_window::WindowedGraph;
use docbrown_db::algorithms::local_triangle_count::local_triangle_count;
use docbrown_db::algorithms::reciprocity::{
    global_reciprocity as global_reciprocity_rs,
    local_reciprocity as local_reciprocity_rs,
    all_local_reciprocity as all_local_reciprocity_rs};

use pyo3::prelude::*;


#[pyfunction]
pub(crate) fn triangle_count(g: &WindowedGraph, v: u64) -> u32 {
    local_triangle_count(&g.graph_w, v)
}

#[pyfunction]
pub(crate) fn global_reciprocity(g: &WindowedGraph) -> f64 {
    global_reciprocity_rs(&g.graph_w)
}

#[pyfunction]
pub(crate) fn local_reciprocity(g: &WindowedGraph, v: u64) -> f64 {
    local_reciprocity_rs(&g.graph_w, v)
}

#[pyfunction]
pub(crate) fn all_local_reciprocity(g: &WindowedGraph) -> Vec<(u64, f64)> {
    all_local_reciprocity_rs(&g.graph_w)
}