use std::iter;

use pyo3::prelude::*;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyclass]
struct BlergIterator {
    iter: Box<dyn iter::Iterator<Item = i32> + Send>,
}

#[pymethods]
impl BlergIterator {
    fn __iter__(slf: PyRef<'_, Self>) -> PyRef<'_, Self> {
        slf
    }

    fn __next__(mut slf: PyRefMut<'_, Self>) -> Option<i32> {
        slf.iter.next()
    }
}

#[pyfunction]
fn iter_n(n: i32) -> PyResult<BlergIterator> {
    Ok(BlergIterator { iter: Box::new(vec![n].into_iter()) })
}


/// A Python module implemented in Rust.
#[pymodule]
fn pyraphtory(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(iter_n, m)?)?;
    Ok(())
}