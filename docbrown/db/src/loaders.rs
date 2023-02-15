pub mod csv {
    use flate2; // 1.0
    use flate2::read::GzDecoder;
    use serde::de::DeserializeOwned;
    use std::collections::VecDeque;
    use std::fmt::Debug;
    use std::fs::File;
    use std::io::BufReader;
    use std::path::{Path, PathBuf};
    use std::{fs, io};

    use rayon::prelude::*;
    use regex::Regex;
    use crate::graphdb::GraphDB;

    #[derive(Debug)]
    pub struct CsvErr(io::Error);

    #[derive(Debug)]
    pub struct CsvLoader {
        path: PathBuf,
        regex_filter: Option<Regex>,
        header: bool,
        delimiter: u8
    }


    impl CsvLoader {
        pub fn new<P: Into<PathBuf>>(p: P) -> Self {
            Self {
                path: p.into(),
                regex_filter: None,
                header: false,
                delimiter: b','
            }
        }

        pub fn set_header(mut self, h: bool) -> Self {
            self.header = h;
            self
        }

        pub fn set_delimiter(mut self, d: u8) -> Self {
            self.delimiter = d;
            self
        }

        pub fn with_filter(mut self, r: Regex) -> Self {
            self.regex_filter = Some(r);
            self
        }

        fn is_dir<P: AsRef<Path>>(p: &P) -> bool {
            fs::metadata(p).unwrap().is_dir()
        }

        fn accept_file<P: Into<PathBuf>>(&self, path: P, paths: &mut Vec<PathBuf>) {
            let p: PathBuf = path.into();
            // this is an actual file so push it into the paths vec if it matches the pattern
            if let Some(pattern) = &self.regex_filter {
                let is_match = &p
                    .to_str()
                    .filter(|file_name| pattern.is_match(file_name))
                    .is_some();
                if *is_match {
                    paths.push(p);
                }
            } else {
                paths.push(p)
            }
        }

        fn files_vec(&self) -> Result<Vec<PathBuf>, CsvErr> {
            let mut paths = vec![];
            let mut queue = VecDeque::from([self.path.to_path_buf()]);

            while let Some(ref path) = queue.pop_back() {
                match fs::read_dir(path) {
                    Ok(entries) => {
                        for entry in entries {
                            if let Ok(f_path) = entry {
                                let p = f_path.path();
                                if Self::is_dir(&p) {
                                    queue.push_back(p.clone())
                                } else {
                                    self.accept_file(f_path.path(), &mut paths);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        if !Self::is_dir(path) {
                            self.accept_file(path.to_path_buf(), &mut paths);
                        }
                        return Err(CsvErr(err));
                    }
                }
            }

            Ok(paths)
        }

        pub fn load_into_graph<F, REC>(&self, g: &GraphDB, loader: F) -> Result<(), CsvErr>
        where
            REC: DeserializeOwned + std::fmt::Debug,
            F: Fn(REC, &GraphDB) -> () + Send + Sync,
        {
            let paths = self.files_vec()?;

            println!("LOADING {paths:?}");

            paths
                .par_iter()
                .try_for_each(move |path| self.load_file_into_graph(path, g, &loader))?;
            Ok(())
        }

        fn load_file_into_graph<F, REC, P: Into<PathBuf> + Debug>(
            &self,
            path: P,
            g: &GraphDB,
            loader: &F,
        ) -> Result<(), CsvErr>
        where
            REC: DeserializeOwned + std::fmt::Debug,
            F: Fn(REC, &GraphDB) -> (),
        {
            let file_path: PathBuf = path.into();

            let mut csv_reader = self.csv_reader(file_path);
            let mut records_iter = csv_reader.deserialize::<REC>();

            while let Some(rec) = records_iter.next() {
                let record = rec.map_err(|err| CsvErr(err.into()))?;
                loader(record, g)
            }

            Ok(())
        } 


        pub fn load_file_into_graph_with_record<F>(
            &self,
            g: &GraphDB,
            loader: &F) -> Result<(), CsvErr>
        where
            F: Fn(&csv::StringRecord, &GraphDB) -> (),
            {
                let f = File::open(&self.path).expect(&format!("Can't open file {:?}", self.path));
                let mut csv_gz_reader = csv::ReaderBuilder::new()
                .has_headers(self.header)
                .delimiter(self.delimiter)
                .from_reader(Box::new(BufReader::new(GzDecoder::new(f))));
        
                let mut rec = csv::StringRecord::new();

                while csv_gz_reader.read_record(&mut rec).unwrap() {
                    loader(&rec, g);
                }
            
                Ok(())
            }
            

        fn csv_reader(&self, file_path: PathBuf) -> csv::Reader<Box<dyn io::Read>> {
            let is_gziped = file_path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| name.ends_with(".gz"))
                .is_some();

            let f = File::open(&file_path).expect(&format!("Can't open file {file_path:?}"));
            if is_gziped {
                csv::ReaderBuilder::new()
                    .has_headers(self.header)
                    .from_reader(Box::new(BufReader::new(GzDecoder::new(f))))
            } else {
                csv::ReaderBuilder::new()
                    .has_headers(self.header)
                    .from_reader(Box::new(f))
            }
        }

        pub fn load(&self) -> Result<GraphDB, CsvErr> {
            let g = GraphDB::new(2);
            // self.load_into(&g)?;
            Ok(g)
        }
    }
}

#[cfg(test)]
mod csv_loader_test {
    use regex::Regex;
    use crate::loaders::csv::CsvLoader;
    use crate::graphdb::GraphDB;
    use std::path::{Path,PathBuf};
    use std::{
        collections::hash_map::DefaultHasher,
        hash::{Hash, Hasher},
    };
    use csv::StringRecord;
    use docbrown_core::Prop;
    #[test]
    fn regex_match() {
        let r = Regex::new(r".+address").unwrap();
        let text = "bitcoin/address_000000000001.csv.gz";
        assert!(r.is_match(&text));
        let text = "bitcoin/received_000000000001.csv.gz";
        assert!(!r.is_match(&text));
    }

    #[test]
    fn regex_match_2() {
        let r = Regex::new(r".+(sent|received)").unwrap();
        let text = "bitcoin/sent_000000000001.csv.gz";
        assert!(r.is_match(&text));
        let text = "bitcoin/received_000000000001.csv.gz";
        assert!(r.is_match(&text));
        let text = "bitcoin/address_000000000001.csv.gz";
        assert!(!r.is_match(&text));
    }
    pub struct Lotr {
        src_id_column: usize,
        dst_id_column: usize,
        timestamp_column: usize,
    }


    fn parse_record(rec: &StringRecord, src_id: usize, dst_id: usize, t_id: usize) -> Option<(String, String, i64)> {
        let src = rec.get(src_id).and_then(|s| s.parse::<String>().ok())?;
        let dst = rec.get(dst_id).and_then(|s| s.parse::<String>().ok())?;
        let t = rec.get(t_id).and_then(|s| s.parse::<i64>().ok())?;
        // println!("{:?}", (&src, &dst, &t));
        Some((src, dst, t))
    }
    
    fn calculate_hash<T: Hash>(t: &T) -> u64 {
        let mut s = DefaultHasher::new();
        t.hash(&mut s);
        s.finish()
    }

    fn graph_loader(ldr: &Lotr, record: &StringRecord, graphdb: &GraphDB) ->  () {   
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
    }

    #[test]
    fn test_headers_flag_and_delimiter() {
        let g = GraphDB::new(2);
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources/test/lotr-withheaders.csv.gz"]
                                .iter()
                                .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = true;
        let delimiter_string = ",";
        let delimiter: u8 = delimiter_string.as_bytes()[0];
        let src_id_column = 0;
        let dst_id_column = 1;
        let timestamp_column = 2;
   
       csv_loader 
            .set_header(has_header)
            .set_delimiter(delimiter)
            .load_file_into_graph_with_record(&g, &|rec, graph|{
                graph_loader(&Lotr{
                    src_id_column: src_id_column, 
                    dst_id_column: dst_id_column, 
                    timestamp_column: timestamp_column
                    }, rec, graph);
            })
            .expect("Csv did not parse.");
         
        assert_eq!(g.edges_len(),701);
    }

    #[test]
    #[should_panic]
    fn test_wrong_header_flag_with_header() {
        let g = GraphDB::new(2);
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources/test/lotr-withheaders.csv.gz"]
                                .iter()
                                .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = false;
        let delimiter_string = ",";
        let delimiter: u8 = delimiter_string.as_bytes()[0];
        let src_id_column = 0;
        let dst_id_column = 1;
        let timestamp_column = 2;
   
       csv_loader 
            .set_header(has_header)
            .set_delimiter(delimiter)
            .load_file_into_graph_with_record(&g, &|rec, graph|{
                graph_loader(&Lotr{
                    src_id_column: src_id_column, 
                    dst_id_column: dst_id_column, 
                    timestamp_column: timestamp_column
                    }, rec, graph);
            })
            .expect("Csv did not parse.");
         
        assert_eq!(g.edges_len(),701);
    }

    #[test]
    fn test_no_headers_flag() {
        let g = GraphDB::new(2);
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources/test/lotr-withoutheaders.csv.gz"]
                                .iter()
                                .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = false;
        let delimiter_string = ",";
        let delimiter: u8 = delimiter_string.as_bytes()[0];
        let src_id_column = 0;
        let dst_id_column = 1;
        let timestamp_column = 2;
   
       csv_loader 
            .set_header(has_header)
            .set_delimiter(delimiter)
            .load_file_into_graph_with_record(&g, &|rec, graph|{
                graph_loader(&Lotr{
                    src_id_column: src_id_column, 
                    dst_id_column: dst_id_column, 
                    timestamp_column: timestamp_column
                    }, rec, graph);
            })
            .expect("Csv did not parse.");
            
        assert_eq!(g.edges_len(), 701);
    }

    #[test]
    #[should_panic]
    fn test_wrong_header_flag_without_header() {
        let g = GraphDB::new(2);
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources/test/lotr-withoutheaders.csv.gz"]
                                .iter()
                                .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = true;
        let delimiter_string = ",";
        let delimiter: u8 = delimiter_string.as_bytes()[0];
        let src_id_column = 0;
        let dst_id_column = 1;
        let timestamp_column = 2;
   
       csv_loader 
            .set_header(has_header)
            .set_delimiter(delimiter)
            .load_file_into_graph_with_record(&g, &|rec, graph|{
                graph_loader(&Lotr{
                    src_id_column: src_id_column, 
                    dst_id_column: dst_id_column, 
                    timestamp_column: timestamp_column
                    }, rec, graph);
            })
            .expect("Csv did not parse.");
        
            assert_eq!(g.edges_len(), 701);
    }

    #[test]
    #[should_panic]
    fn test_out_of_bounds_columns() {
        let g = GraphDB::new(2);
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources/test/lotr.csv.gz"]
                                .iter()
                                .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
       
        let delimiter_string = ",";
        let delimiter: u8 = delimiter_string.as_bytes()[0];
        let src_id_column = 3;
        let dst_id_column = 4;
        let timestamp_column = 5;
        let has_header = true;
       csv_loader 
            .set_header(has_header)
            .set_delimiter(delimiter)
            .load_file_into_graph_with_record(&g, &|rec, graph|{
                graph_loader(&Lotr{
                    src_id_column: src_id_column, 
                    dst_id_column: dst_id_column, 
                    timestamp_column: timestamp_column
                    }, rec, graph);
            })
            .expect("Csv did not parse.");
    }


    #[test]
    #[should_panic]
    fn test_wrong_delimiter() {
        let g = GraphDB::new(2);
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "resources/test/lotr.csv.gz"]
                                .iter()
                                .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
       
        let delimiter_string = ".";
        let delimiter: u8 = delimiter_string.as_bytes()[0];
        let src_id_column = 0;
        let dst_id_column = 1;
        let timestamp_column = 2;
        let has_header = true;
       csv_loader 
            .set_header(has_header)
            .set_delimiter(delimiter)
            .load_file_into_graph_with_record(&g, &|rec, graph|{
                graph_loader(&Lotr{
                    src_id_column: src_id_column, 
                    dst_id_column: dst_id_column, 
                    timestamp_column: timestamp_column
                    }, rec, graph);
            })
            .expect("Csv did not parse.");
    }


}
