use pyo3::{PyAny, PyResult};
use docbrown_core::tgraph::VertexRef;
use pyo3::exceptions::PyTypeError;
use crate::vertex::PyVertex;

pub fn extract_vertex_ref(vref: &PyAny) -> PyResult<VertexRef> {
    if let Ok(s) = vref.extract::<String>() {
        Ok(s.into())
    } else if let Ok(gid) = vref.extract::<u64>() {
        Ok(gid.into())
    } else if let Ok(v) = vref.extract::<PyVertex>() {
        Ok(v.into())
    } else {
        PyTypeError::new_err("Not a valid vertex")
    }
}
