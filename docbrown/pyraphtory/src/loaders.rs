use csv::StringRecord;
use docbrown_db::graphdb::GraphDB;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::process;

#[pymodule]
fn pyraphtory(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyCSV>()?;
    Ok(())
}

#[pyclass]
struct PyCSV {
    path: PathBuf,
    delimiter: String,
    header: bool,
    src_id_column: usize,
    dst_id_column: usize,
    timestamp_column: usize,
}

fn parse_record(rec: &StringRecord) -> Option<(String, String, i64)> {
    let src = rec.get(0).and_then(|s| s.parse::<String>().ok())?;
    let dst = rec.get(1).and_then(|s| s.parse::<String>().ok())?;
    let t = rec.get(2).and_then(|s| s.parse::<i64>().ok())?;
    Some((src, dst, t))
}

fn graph_loader(ldr: &PyCSV, record: &StringRecord, graphdb: &GraphDB) {
    println!("hello");
    println!("{:?}", &record)
}

#[pymethods]
impl PyCSV {
    #[new]
    fn new(
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



    fn read_csv(&self) -> PyResult<()> {
        // READ THE FILE
        let g = GraphDB::new(2);
        let csv_loader = docbrown_db::loaders::csv::CsvLoader::new(self.path.as_path());
        csv_loader
            .load_file_into_graph_with_record(&g, &|rec, graph|{

                graph_loader(&self, rec, graph)
            })
            .expect("Died a horrible death!");
        Ok(())
    }
}


