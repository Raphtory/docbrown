pub mod algorithms;
mod dynamic;
pub mod edge;
pub mod graph;
pub mod graph_gen;
pub mod graph_loader;
pub mod graph_view;
mod util;
pub mod vertex;
pub mod wrappers;

use pyo3::prelude::*;

use crate::algorithms::{
    all_local_reciprocity, global_clustering_coefficient, global_reciprocity, triplet_count,
};

use crate::graph::PyGraph;
use crate::wrappers::PyPerspective;

use crate::algorithms::*;
use crate::graph_gen::*;
use crate::graph_loader::*;
#[allow(unused_imports)]
use pyo3::prelude::*;

#[pymodule]
fn raphtory(py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyGraph>()?;
    m.add_class::<PyPerspective>()?;

    let algorithm_module = PyModule::new(py, "algorithms")?;
    algorithm_module.add_function(wrap_pyfunction!(global_reciprocity, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(all_local_reciprocity, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(triplet_count, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(
        global_clustering_coefficient,
        algorithm_module
    )?)?;
    algorithm_module.add_function(wrap_pyfunction!(local_triangle_count, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(
        local_clustering_coefficient,
        algorithm_module
    )?)?;
    algorithm_module.add_function(wrap_pyfunction!(average_degree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(directed_graph_density, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(max_out_degree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(max_in_degree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(min_out_degree, algorithm_module)?)?;
    algorithm_module.add_function(wrap_pyfunction!(min_in_degree, algorithm_module)?)?;
    m.add_submodule(algorithm_module)?;

    let graph_loader_module = PyModule::new(py, "graph_loader")?;
    graph_loader_module.add_function(wrap_pyfunction!(lotr_graph, graph_loader_module)?)?;
    graph_loader_module.add_function(wrap_pyfunction!(
        reddit_hyperlink_graph,
        graph_loader_module
    )?)?;
    m.add_submodule(graph_loader_module)?;

    let graph_gen_module = PyModule::new(py, "graph_gen")?;
    graph_gen_module.add_function(wrap_pyfunction!(random_attachment, graph_gen_module)?)?;
    graph_gen_module.add_function(wrap_pyfunction!(
        ba_preferential_attachment,
        graph_gen_module
    )?)?;
    m.add_submodule(graph_gen_module)?;

    Ok(())
}
