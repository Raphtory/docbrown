pub mod wrappers;
pub mod graph;
pub mod graph_window;
pub mod algorithms;

use pyo3::prelude::*;

use crate::wrappers::Direction;
use crate::wrappers::TEdge;
use crate::graph::Graph;
use crate::algorithms::triangle_count; 

#[pymodule]
fn pyraphtory(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Direction>()?;
    m.add_class::<Graph>()?;
    m.add_class::<TEdge>()?;
    let submod = PyModule::new(_py, "algorithms")?;
    submod.add_wrapped(wrap_pyfunction!(triangle_count))?;
    m.add_submodule(submod)?;
    Ok(())
}
