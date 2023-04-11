use crate::db::graphgen::preferential_attachment::ba_preferential_attachment as pa;
use crate::db::graphgen::random_attachment::random_attachment as ra;
use crate::PyGraph;
use pyo3::prelude::*;

#[pyfunction]
pub(crate) fn random_attachment(g: &PyGraph, vertices_to_add: usize, edges_per_step: usize) {
    ra(&g.graph, vertices_to_add, edges_per_step);
}

#[pyfunction]
pub(crate) fn ba_preferential_attachment(
    g: &PyGraph,
    vertices_to_add: usize,
    edges_per_step: usize,
) {
    pa(&g.graph, vertices_to_add, edges_per_step);
}
