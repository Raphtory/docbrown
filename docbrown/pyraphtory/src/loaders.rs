use csv::StringRecord;
use docbrown_db::graphdb::GraphDB;
use pyo3::prelude::*;
use pyo3::wrap_pyfunction;
use std::error::Error;
use std::io;
use std::path::{Path, PathBuf};
use std::process;
use docbrown_core::Prop;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
};

#[pymodule]
fn pyraphtory(_py: Python<'_>, m: &PyModule) -> PyResult<()> {
    m.add_class::<PyCSV>()?;
    Ok(())
}

// TODO - make this a builder type so it can be optional
#[pyclass]
struct PyCSV {
    path: PathBuf,
    delimiter: String,
    header: bool,
    src_id_column: usize,
    dst_id_column: usize,
    timestamp_column: usize,
}

fn parse_record(rec: &StringRecord, src_id: usize, dst_id: usize, t_id: usize) -> Option<(String, String, i64)> {
    let src = rec.get(src_id).and_then(|s| s.parse::<String>().ok())?;
    let dst = rec.get(dst_id).and_then(|s| s.parse::<String>().ok())?;
    let t = rec.get(t_id).and_then(|s| s.parse::<i64>().ok())?;
    println!("{:?}", (&src, &dst, &t));
    Some((src, dst, t))
}

fn calculate_hash<T: Hash>(t: &T) -> u64 {
    let mut s = DefaultHasher::new();
    t.hash(&mut s);
    s.finish()
}


fn graph_loader(ldr: &PyCSV, record: &StringRecord, graphdb: &GraphDB) ->  () {   
    let src_id : usize = ldr.src_id_column; 
    let dst_id : usize  = ldr.dst_id_column;
    let timestamp_id : usize = ldr.timestamp_column;
    let tuple = parse_record(&record, src_id, dst_id, timestamp_id).unwrap();
    let (src, dst, time)  = tuple;

    let srcid = calculate_hash(&src);
    let dstid = calculate_hash(&dst);

    graphdb.add_vertex(srcid, time, &vec![("name".to_string(), Prop::Str("Character".to_string()))]);
    graphdb.add_vertex(dstid, time, &vec![("name".to_string(), Prop::Str("Character".to_string()))]);
    graphdb.add_edge(
        srcid,
        dstid,
        time,
        &vec![(
            "name".to_string(),
            Prop::Str("Character Co-occurrence".to_string()),
        )]
    );
    print!("Added edge.")
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
        let delimiter: u8 = self.delimiter.as_bytes()[0];
        let g = GraphDB::new(2);
        let csv_loader = docbrown_db::loaders::csv::CsvLoader::new(self.path.as_path());
        csv_loader
            .set_header(self.header)
            .set_delimiter(delimiter)
            .load_file_into_graph_with_record(&g, &|rec, graph|{
                graph_loader(&self, rec, graph);
            })
            .expect("Csv did not parse.");
        Ok(())
    }
}

mod pycsv_loader_test {
    fn graph_loader_adds_edge() {
        graph.add_edge()
    }

    fn graph_loader_adds_vertices() {
        graph.add_vertex()
    }

    fn graph_loader_loads_src_correctly() {
     assert!("goes into correct column")
    }

    fn graph_loader_loads_dst_correctly() {
        assert!("goes into correct column")
    }

    fn graph_loader_loads_time_correctly() {
        assert!("goes into correct column")
    }

    fn record_parses_correctly() {
        assert!(srcid = "");
        assert!(dstid = "");
        assert!(time = "");
    }

    fn correct_hash_produced() {
        calculate_hash(hash == "")
    }
}

