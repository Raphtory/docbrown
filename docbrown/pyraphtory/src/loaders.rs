use csv::StringRecord;
use docbrown_db::graphdb::GraphDB;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::process;

#[pyclass]
pub struct PyCSV {
    #[pyo3(get)]
    path: PathBuf,
    delimiter: String,
    header: bool,
    src_id_column: usize,
    dst_id_column: usize,
    timestamp_column: usize,
}

fn graph_loader(record: &StringRecord, graphdb: &GraphDB) {}

#[pymethods]
impl PyCSV {
    #[new]
    pub fn new(
        path: PathBuf,
        delimiter: String,
        header: bool,
        src_id_column: usize,
        dst_id_column: usize,
        timestamp_column: usize,
    ) -> Self {
        PyCSV {
            path,
            delimiter,
            header,
            src_id_column,
            dst_id_column,
            timestamp_column,
        }
    }

    pub fn read_csv(&self) -> PyResult<()> {
        // READ THE FILE
        let g = GraphDB::new(2);
        let csv_loader = docbrown_db::loaders::csv::CsvLoader::new(self.path.as_path());
        csv_loader
            .load_file_into_graph_with_record(&g, &graph_loader)
            .expect("Died a horrible death!");
        Ok(())
    }
}


