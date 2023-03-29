use crate::edge::EdgeView;
use crate::graph_window::{GraphWindowSet, WindowedGraph};
use crate::perspective::{Perspective, PerspectiveIterator, PerspectiveSet};
use crate::vertex::VertexView;
use crate::vertices::Vertices;
use crate::view_api::internal::GraphViewInternalOps;
use docbrown_core::vertex::InputVertex;
use std::iter;

pub trait GraphViewOps: Send + Sync + Sized + GraphViewInternalOps + 'static + Clone {
    fn num_vertices(&self) -> usize;
    fn earliest_time(&self) -> Option<i64>;
    fn latest_time(&self) -> Option<i64>;
    fn is_empty(&self) -> bool {
        self.num_vertices() == 0
    }
    fn num_edges(&self) -> usize;
    fn has_vertex<T: InputVertex>(&self, v: T) -> bool;
    fn has_edge<T: InputVertex>(&self, src: T, dst: T) -> bool;
    fn vertex<T: InputVertex>(&self, v: T) -> Option<VertexView<Self>>;
    fn vertices(&self) -> Vertices<Self>;
    fn edge<T: InputVertex>(&self, src: T, dst: T) -> Option<EdgeView<Self>>;
    fn edges(&self) -> Box<dyn Iterator<Item = EdgeView<Self>> + Send>;
    fn vertices_shard(&self, shard: usize) -> Box<dyn Iterator<Item = VertexView<Self>> + Send>;
    fn window(&self, t_start: i64, t_end: i64) -> WindowedGraph<Self>;
    fn at(&self, end: i64) -> WindowedGraph<Self> {
        self.window(i64::MIN, end.saturating_add(1))
    }
    fn through_perspectives(&self, perspectives: PerspectiveSet) -> GraphWindowSet<Self> {
        let iter = match (self.earliest_time(), self.latest_time()) {
            (Some(start), Some(end)) => perspectives.build_iter(start..end),
            _ => PerspectiveIterator::empty(),
        };
        GraphWindowSet::new(self.clone(), Box::new(iter))
    }

    fn through_iter(
        &self,
        perspectives: Box<dyn Iterator<Item = Perspective> + Send>,
    ) -> GraphWindowSet<Self> {
        let iter = match (self.earliest_time(), self.latest_time()) {
            (Some(start), Some(end)) => perspectives,
            _ => Box::new(iter::empty::<Perspective>()),
        };
        GraphWindowSet::new(self.clone(), iter)
    }
}
