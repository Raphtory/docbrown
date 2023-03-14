use crate::view_api::edge::EdgeViewOps;
use crate::view_api::vertex::VertexViewOps;

pub(crate) trait GraphViewInternalOps {
    fn vertices_len(&self) -> usize;
    
    fn vertices_len_window(&self, t_start: i64, t_end: i64) -> usize;
    
    fn edges_len(&self) -> usize;

    fn has_edge(&self, src: u64, dst: u64) -> bool {
        let shard_id = utils::get_shard_id_from_global_vid(src, self.nr_shards);
        self.shards[shard_id].has_edge(src, dst)
    }

    pub fn has_edge_window(&self, src: u64, dst: u64, t_start: i64, t_end: i64) -> bool {
        let shard_id = utils::get_shard_id_from_global_vid(src, self.nr_shards);
        self.shards[shard_id].has_edge_window(src, dst, t_start..t_end)
    }

    pub fn has_vertex(&self, v: u64) -> bool {
        self.shards.iter().any(|shard| shard.has_vertex(v))
    }

    pub(crate) fn has_vertex_window(&self, v: u64, t_start: i64, t_end: i64) -> bool {
        self.shards
            .iter()
            .any(|shard| shard.has_vertex_window(v, t_start..t_end))
    }

    pub(crate) fn vertex(&self, v: u64) -> Option<VertexView> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex(v)
    }

    pub(crate) fn vertex_window(&self, v: u64, t_start: i64, t_end: i64) -> Option<VertexView> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex_window(v, t_start..t_end)
    }

    pub(crate) fn degree_window(&self, v: u64, t_start: i64, t_end: i64, d: Direction) -> usize {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id].degree_window(v, t_start..t_end, d);
        iter
    }

    pub(crate) fn vertex_ids_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = u64> + Send> {
        let shards = self.shards.clone();
        Box::new(
            shards
                .into_iter()
                .map(move |shard| shard.vertex_ids_window(t_start..t_end))
                .into_iter()
                .flatten(),
        )
    }

    pub(crate) fn vertices_window(
        &self,
        t_start: i64,
        t_end: i64,
    ) -> Box<dyn Iterator<Item = VertexView> + Send> {
        let shards = self.shards.clone();
        Box::new(
            shards
                .into_iter()
                .map(move |shard| shard.vertices_window(t_start..t_end))
                .flatten(),
        )
    }

    pub(crate) fn fold_par<S, F, F2>(&self, t_start: i64, t_end: i64, f: F, agg: F2) -> Option<S>
        where
            S: Send,
            F: Fn(VertexView) -> S + Send + Sync + Copy,
            F2: Fn(S, S) -> S + Sync + Send + Copy,
    {
        let shards = self.shards.clone();

        let out = shards
            .into_par_iter()
            .map(|shard| {
                shard.read_shard(|tg_core| {
                    tg_core
                        .vertices_window(t_start..t_end)
                        .par_bridge()
                        .map(f)
                        .reduce_with(agg)
                })
            })
            .flatten()
            .reduce_with(agg);

        out
    }

    pub(crate) fn vertex_window_par<O, F>(
        &self,
        t_start: i64,
        t_end: i64,
        f: F,
    ) -> Box<dyn Iterator<Item = O>>
        where
            O: Send + 'static,
            F: Fn(VertexView) -> O + Send + Sync + Copy,
    {
        let shards = self.shards.clone();
        let (tx, rx) = flume::unbounded();

        let arc_tx = Arc::new(tx);
        shards
            .into_par_iter()
            .map(|shard| shard.vertices_window(t_start..t_end).par_bridge().map(f))
            .flatten()
            .for_each(move |o| {
                arc_tx.send(o).unwrap();
            });

        Box::new(rx.into_iter())
    }

    pub(crate) fn edge(&self, v1: u64, v2: u64) -> Option<EdgeView> {
        let shard_id = utils::get_shard_id_from_global_vid(v1, self.nr_shards);
        self.shards[shard_id].edge(v1, v2)
    }

    pub(crate) fn edge_window(
        &self,
        src: u64,
        dst: u64,
        t_start: i64,
        t_end: i64,
    ) -> Option<EdgeView> {
        let shard_id = utils::get_shard_id_from_global_vid(src, self.nr_shards);
        self.shards[shard_id].edge_window(src, dst, t_start..t_end)
    }

    pub(crate) fn vertex_edges_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView> + Send> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id].vertex_edges_window(v, t_start..t_end, d);
        Box::new(iter)
    }

    pub(crate) fn vertex_edges_window_t(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = EdgeView> + Send> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id].vertex_edges_window_t(v, t_start..t_end, d);
        Box::new(iter)
    }

    pub(crate) fn neighbours_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = VertexView> + Send> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id].neighbours_window(v, t_start..t_end, d);
        Box::new(iter)
    }

    pub(crate) fn neighbours_ids_window(
        &self,
        v: u64,
        t_start: i64,
        t_end: i64,
        d: Direction,
    ) -> Box<dyn Iterator<Item = u64> + Send>
        where
            Self: Sized,
    {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        let iter = self.shards[shard_id]
            .neighbours_ids_window(v, t_start..t_end, d)
            .unique();
        Box::new(iter)
    }

    pub(crate) fn vertex_prop_vec(&self, v: u64, name: String) -> Vec<(i64, Prop)> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex_prop_vec(v, name)
    }

    pub(crate) fn vertex_prop_vec_window(
        &self,
        v: u64,
        name: String,
        w: Range<i64>,
    ) -> Vec<(i64, Prop)> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex_prop_vec_window(v, name, w)
    }

    pub(crate) fn vertex_props(&self, v: u64) -> HashMap<String, Vec<(i64, Prop)>> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex_props(v)
    }

    pub(crate) fn vertex_props_window(
        &self,
        v: u64,
        w: Range<i64>,
    ) -> HashMap<String, Vec<(i64, Prop)>> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].vertex_props_window(v, w)
    }

    pub fn edge_props_vec_window(
        &self,
        v: u64,
        e: usize,
        name: String,
        w: Range<i64>,
    ) -> Vec<(i64, Prop)> {
        let shard_id = utils::get_shard_id_from_global_vid(v, self.nr_shards);
        self.shards[shard_id].edge_props_vec_window(e, name, w)
    }
}

pub trait GraphViewOps {
    type Vertex: VertexViewOps<Edge = Self::Edge>;
    type Vertices: IntoIterator<Item = Self::Vertex>;
    type Edge: EdgeViewOps<Vertex = Self::Vertex>;
    type Edges: IntoIterator<Item = Self::Edge>;

    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn edges_len(&self) -> usize;
    fn has_vertex(&self, v: u64) -> bool;
    fn has_edge(&self, src: u64, dst: u64) -> bool;
    fn vertex(&self, v: u64) -> Option<Self::Vertex>;
    fn vertices(&self) -> Self::Vertices;
    fn edge(&self, src: u64, dst: u64) -> Option<Self::Edge>;
    fn edges(&self) -> Self::Edges;
}
