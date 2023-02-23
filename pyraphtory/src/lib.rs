pub mod graphdb;
pub mod wrappers;

use pyo3::prelude::*;

use crate::wrappers::Direction;
use crate::wrappers::TEdge;
use crate::graphdb::GraphDB;

#[pymodule]
fn pyraphtory(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<Direction>()?;
    m.add_class::<GraphDB>()?;
    m.add_class::<TEdge>()?;
    Ok(())
}
