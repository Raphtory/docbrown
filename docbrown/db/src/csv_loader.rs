pub mod csv {
    use flate2; // 1.0
    use flate2::read::GzDecoder;
    use serde::de::DeserializeOwned;
    use std::collections::VecDeque;
    use std::error::Error;
    use std::fmt::{Debug, Display, Formatter};
    use std::fs::File;
    use std::io::BufReader;
    use std::path::{Path, PathBuf};
    use std::{fs, io};

    use crate::graph::Graph;
    use rayon::prelude::*;
    use regex::Regex;

    #[derive(Debug)]
    pub enum CsvErr {
        IoError(io::Error),
        CsvError(csv::Error),
    }

    impl From<io::Error> for CsvErr {
        fn from(value: io::Error) -> Self {
            Self::IoError(value)
        }
    }


    impl From<csv::Error> for CsvErr {
        fn from(value: csv::Error) -> Self {
            Self::CsvError(value)
        }
    }

    impl Display for CsvErr {
        fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
            match self.source() {
                Some(error) => write!(f, "CSV loader failed with error: {}", error),
                None => write!(f, "CSV loader failed with unknonw error"),
            }
        }
    }

    impl Error for CsvErr {
        fn source(&self) -> Option<&(dyn Error + 'static)> {
            match self {
                CsvErr::IoError(error) => Some(error),
                CsvErr::CsvError(error) => Some(error),
            }
        }
    }

    #[derive(Debug)]
    pub struct CsvLoader {
        path: PathBuf,
        regex_filter: Option<Regex>,
        header: bool,
        delimiter: u8,
    }

    impl CsvLoader {
        pub fn new<P: Into<PathBuf>>(p: P) -> Self {
            Self {
                path: p.into(),
                regex_filter: None,
                header: false,
                delimiter: b',',
            }
        }

        pub fn set_header(mut self, h: bool) -> Self {
            self.header = h;
            self
        }

        pub fn set_delimiter(mut self, d: &str) -> Self {
            self.delimiter = d.as_bytes()[0];
            self
        }

        pub fn with_filter(mut self, r: Regex) -> Self {
            self.regex_filter = Some(r);
            self
        }

        fn is_dir<P: AsRef<Path>>(p: &P) -> Result<bool, CsvErr> {
            Ok(fs::metadata(p)?.is_dir())
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
                                if Self::is_dir(&p)? {
                                    queue.push_back(p.clone())
                                } else {
                                    self.accept_file(f_path.path(), &mut paths);
                                }
                            }
                        }
                    }
                    Err(err) => {
                        if !Self::is_dir(path)? {
                            self.accept_file(path.to_path_buf(), &mut paths);
                        } else {
                            return Err(err.into());
                        }
                    }
                }
            }

            Ok(paths)
        }

        pub fn load_into_graph<F, REC>(&self, g: &Graph, loader: F) -> Result<(), CsvErr>
        where
            REC: DeserializeOwned + std::fmt::Debug,
            F: Fn(REC, &Graph) -> () + Send + Sync,
        {
            let paths = self.files_vec()?;
            paths
                .par_iter()
                .try_for_each(move |path| self.load_file_into_graph(path, g, &loader))?;
            Ok(())
        }

        fn load_file_into_graph<F, REC, P: Into<PathBuf> + Debug>(
            &self,
            path: P,
            g: &Graph,
            loader: &F,
        ) -> Result<(), CsvErr>
        where
            REC: DeserializeOwned + std::fmt::Debug,
            F: Fn(REC, &Graph) -> (),
        {
            let file_path: PathBuf = path.into();

            let mut csv_reader = self.csv_reader(file_path)?;
            let mut records_iter = csv_reader.deserialize::<REC>();

            for rec in records_iter {
                let record = rec?;
                loader(record, g)
            }

            Ok(())
        }

        fn csv_reader(&self, file_path: PathBuf) -> Result<csv::Reader<Box<dyn io::Read>>, CsvErr> {
            let is_gziped = file_path
                .file_name()
                .and_then(|name| name.to_str())
                .filter(|name| name.ends_with(".gz"))
                .is_some();

            let f =
                File::open(&file_path)?;
            if is_gziped {
                Ok(csv::ReaderBuilder::new()
                    .has_headers(self.header)
                    .delimiter(self.delimiter)
                    .from_reader(Box::new(BufReader::new(GzDecoder::new(f)))))
            } else {
                Ok(csv::ReaderBuilder::new()
                    .has_headers(self.header)
                    .delimiter(self.delimiter)
                    .from_reader(Box::new(f)))
            }
        }

        // pub fn load(&self) -> Result<Graph, CsvErr> {
        //     let g = Graph::new(2);
        //     self.load_into(&g)?;
        //     Ok(g)
        // }
    }
}

#[cfg(test)]
mod csv_loader_test {
    use crate::csv_loader::csv::CsvLoader;
    use crate::graph::Graph;
    use docbrown_core::utils::calculate_hash;
    use docbrown_core::Prop;
    use regex::Regex;
    use serde::Deserialize;
    use std::path::{Path, PathBuf};

    #[test]
    fn regex_match() {
        let r = Regex::new(r".+address").unwrap();
        // todo: move file path to data module
        let text = "bitcoin/address_000000000001.csv.gz";
        assert!(r.is_match(&text));
        let text = "bitcoin/received_000000000001.csv.gz";
        assert!(!r.is_match(&text));
    }

    #[test]
    fn regex_match_2() {
        let r = Regex::new(r".+(sent|received)").unwrap();
        // todo: move file path to data module
        let text = "bitcoin/sent_000000000001.csv.gz";
        assert!(r.is_match(&text));
        let text = "bitcoin/received_000000000001.csv.gz";
        assert!(r.is_match(&text));
        let text = "bitcoin/address_000000000001.csv.gz";
        assert!(!r.is_match(&text));
    }

    #[derive(Deserialize, std::fmt::Debug)]
    pub struct Lotr {
        src_id: String,
        dst_id: String,
        time: i64,
    }

    fn lotr_test(g: Graph, csv_loader: CsvLoader, has_header: bool, delimiter: &str, r: Regex) {
        csv_loader
            .set_header(has_header)
            .set_delimiter(delimiter)
            .with_filter(r)
            .load_into_graph(&g, |lotr: Lotr, g: &Graph| {
                let src_id = calculate_hash(&lotr.src_id);
                let dst_id = calculate_hash(&lotr.dst_id);
                let time = lotr.time;

                g.add_vertex(
                    time,
                    src_id,
                    &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                );
                g.add_vertex(
                    time,
                    dst_id,
                    &vec![("name".to_string(), Prop::Str("Character".to_string()))],
                );
                g.add_edge(
                    time,
                    src_id,
                    dst_id,
                    &vec![(
                        "name".to_string(),
                        Prop::Str("Character Co-occurrence".to_string()),
                    )],
                );
            })
            .expect("Csv did not parse.");
    }

    #[test]
    fn test_headers_flag_and_delimiter() {
        let g = Graph::new(2);
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();

        println!("path = {}", csv_path.as_path().to_str().unwrap());
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = true;
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_wrong_header_flag_file_with_header() {
        let g = Graph::new(2);
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = false;
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_flag_has_header_but_file_has_no_header() {
        let g = Graph::new(2);
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let has_header = true;
        let r = Regex::new(r".+(lotr-without-header.csv)").unwrap();
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_wrong_header_names() {
        let g = Graph::new(2);
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let r = Regex::new(r".+(lotr-wrong.csv)").unwrap();
        let has_header = true;
        let delimiter = ",";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }

    #[test]
    #[should_panic]
    fn test_wrong_delimiter() {
        let g = Graph::new(2);
        // todo: move file path to data module
        let csv_path: PathBuf = [env!("CARGO_MANIFEST_DIR"), "../../resource/"]
            .iter()
            .collect();
        let csv_loader = CsvLoader::new(Path::new(&csv_path));
        let r = Regex::new(r".+(lotr.csv)").unwrap();
        let has_header = true;
        let delimiter = ".";
        lotr_test(g, csv_loader, has_header, delimiter, r);
    }
}
