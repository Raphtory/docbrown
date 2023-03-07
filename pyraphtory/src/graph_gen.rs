
use pyo3::prelude::*;
use docbrown_db::graphgen::random_attachment::random_attachment as ra;
use docbrown_db::graphgen::preferential_attachment::preferential_attachment as pa;
use crate::Graph;


#[pyfunction]
pub(crate) fn random_attachment(g:&Graph,vertices_to_add:usize, edges_per_step:usize) {
    ra(&g.graph,vertices_to_add, edges_per_step);
}

#[pyfunction]
pub(crate) fn preferential_attachment(g:&Graph,vertices_to_add:usize, edges_per_step:usize) {
    pa(&g.graph,vertices_to_add, edges_per_step);
}