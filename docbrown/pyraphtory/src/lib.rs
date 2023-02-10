use pyo3::prelude::*;

mod loaders;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pymodule]
fn pyraphtory(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<loaders::PyCSV>()?;
    Ok(())
}