pub mod csv {
    use docbrown_db::loaders::csv;
    use regex::Regex;
    use serde::de::DeserializeOwned;
    use std::io;
    use std::path::PathBuf;

    use crate::graphdb::GraphDB;

    pub struct CsvErr(io::Error);

    pub struct CsvLoader {
        csv_loader: csv::CsvLoader,
    }

    impl CsvLoader {
        pub fn new<P: Into<PathBuf>>(p: P) -> Self {
            Self {
                csv_loader: csv::CsvLoader::new(p),
            }
        }

        pub fn with_filter(self, r: Regex) -> Self {
            Self {
                csv_loader: self.csv_loader.with_filter(r),
            }
        }

        pub fn load_into_graph<F, REC>(&self, g: &GraphDB, loader: F) -> Result<(), CsvErr>
        where
            REC: DeserializeOwned + std::fmt::Debug,
            F: Fn(REC, &docbrown_db::graphdb::GraphDB) -> () + Send + Sync,
        {
            match self.csv_loader.load_into_graph(&g.graphdb, loader) {
                Ok(_) => Ok(()),
                Err(csv::CsvErr(e)) => Err(CsvErr(e)),
            }
        }
    }
}
