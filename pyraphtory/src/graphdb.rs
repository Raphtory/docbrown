use docbrown_core as dbc;
use docbrown_db::graphdb as gdb;
use std::path::Path;

pub enum Direction {
    OUT,
    IN,
    BOTH,
}

impl Direction {
    fn convert(&self) -> dbc::Direction {
        match self {
            Direction::OUT => dbc::Direction::OUT,
            Direction::IN => dbc::Direction::IN,
            Direction::BOTH => dbc::Direction::BOTH,
        }
    }
}

pub enum Prop {
    Str(String),
    I32(i32),
    I64(i64),
    U32(u32),
    U64(u64),
    F32(f32),
    F64(f64),
    Bool(bool),
}

impl Prop {
    fn convert(&self) -> dbc::Prop {
        match self {
            Prop::Str(String) => dbc::Prop::Str(String.clone()),
            Prop::I32(i32) => dbc::Prop::I32(*i32),
            Prop::I64(i64) => dbc::Prop::I64(*i64),
            Prop::U32(u32) => dbc::Prop::U32(*u32),
            Prop::U64(u64) => dbc::Prop::U64(*u64),
            Prop::F32(f32) => dbc::Prop::F32(*f32),
            Prop::F64(f64) => dbc::Prop::F64(*f64),
            Prop::Bool(bool) => dbc::Prop::Bool(*bool),
        }
    }
}

pub struct TEdge {
    pub src: u64,
    pub dst: u64,
    pub t: Option<i64>,
    pub is_remote: bool,
}

impl TEdge {
    fn convert(edge: dbc::tpartition::TEdge) -> TEdge {
        let dbc::tpartition::TEdge {
            src,
            dst,
            t,
            is_remote,
        } = edge;
        TEdge {
            src: src,
            dst: dst,
            t: t,
            is_remote: is_remote,
        }
    }
}

pub struct GraphDB {
    pub(crate) graphdb: gdb::GraphDB,
}

impl GraphDB {
    pub fn new(nr_shards: usize) -> Self {
        Self {
            graphdb: gdb::GraphDB::new(nr_shards),
        }
    }

    pub fn load_from_file<P: AsRef<Path>>(path: P) -> Result<Self, Box<bincode::ErrorKind>> {
        match gdb::GraphDB::load_from_file(path) {
            Ok(g) => Ok(GraphDB { graphdb: g }),
            Err(e) => Err(e),
        }
    }

    pub fn save_to_file<P: AsRef<Path>>(&self, path: P) -> Result<(), Box<bincode::ErrorKind>> {
        self.graphdb.save_to_file(path)
    }

    pub fn len(&self) -> usize {
        self.graphdb.len()
    }

    pub fn edges_len(&self) -> usize {
        self.graphdb.edges_len()
    }

    pub fn contains(&self, v: u64) -> bool {
        self.graphdb.contains(v)
    }

    pub fn contains_window(&self, v: u64, t_start: i64, t_end: i64) -> bool {
        self.graphdb.contains_window(v, t_start, t_end)
    }

    pub fn add_vertex(&self, v: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.graphdb.add_vertex(
            v,
            t,
            &props
                .iter()
                .map(|f| (f.0.clone(), f.1.convert()))
                .collect::<Vec<(String, dbc::Prop)>>(),
        )
    }

    pub fn add_edge(&self, src: u64, dst: u64, t: i64, props: &Vec<(String, Prop)>) {
        self.graphdb.add_edge(
            src,
            dst,
            t,
            &props
                .iter()
                .map(|f| (f.0.clone(), f.1.convert()))
                .collect::<Vec<(String, dbc::Prop)>>(),
        )
    }

    pub fn degree(&self, v: u64, d: Direction) -> usize {
        self.graphdb.degree(v, d.convert())
    }

    pub fn degree_window(&self, v: u64, t_start: i64, t_end: i64, d: Direction) -> usize {
        self.graphdb.degree_window(v, t_start, t_end, d.convert())
    }

    pub fn vertices(&self) -> Box<dyn Iterator<Item = u64> + '_> {
        self.graphdb.vertices()
    }

    pub fn neighbours(&self, v: u64, d: Direction) -> Box<dyn Iterator<Item = TEdge>> {
        Box::new(
            self.graphdb
                .neighbours(v, d.convert())
                .map(|f| TEdge::convert(f)),
        )
    }

    pub fn neighbours_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = TEdge>> {
        Box::new(
            self.graphdb
                .neighbours_window(v, t_start, t_end, d.convert())
                .map(|f| TEdge::convert(f)),
        )
    }

    pub fn neighbours_window_t(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = TEdge>> {
        Box::new(
            self.graphdb
                .neighbours_window_t(v, t_start, t_end, d.convert())
                .map(|f| TEdge::convert(f)),
        )
    }
}
