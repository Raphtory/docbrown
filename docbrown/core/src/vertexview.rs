use crate::graph::{EdgeView, TemporalGraph};
use crate::graphview::{
    EdgeIterator, GraphView, GraphViewInternals, IteratorWithLifetime, NeighboursIterator,
    PropertyHistory,
};
use crate::state::{State, StateVec};
use crate::tpartition::TemporalGraphPart;
use crate::{Direction, Prop};
use std::borrow::Borrow;
use std::ops::Range;

pub struct VertexView<'a, G>
where
    G: GraphViewInternals,
{
    pub(crate) g_id: u64,
    pub(crate) pid: usize,
    pub(crate) g: &'a G,
    pub(crate) w: Option<Range<i64>>,
}

impl<'a, G> VertexView<'a, G>
where
    G: GraphViewInternals,
{
    /// Change underlying graph this vertex view points to (useful when implementing a view)
    pub(crate) fn as_view_of<'b, GG: GraphViewInternals>(
        &self,
        graph: &'b GG,
    ) -> VertexView<'b, GG> {
        VertexView {
            g_id: self.g_id,
            pid: self.pid,
            g: graph,
            w: self.w.clone(),
        }
    }
    // pub fn global_id(&self) -> u64 {
    //     self.g_id
    // }
    //
    // pub fn degree(&self, d: Direction) -> usize {
    //     if let Some(w) = &self.w {
    //         self.g.degree_window(self.g_id, w, d)
    //     } else {
    //         self.g.degree(self.g_id, d)
    //     }
    // }
    //
}

pub trait VertexViewMethods<'a, G>
where
    G: GraphViewInternals,
{
    type ItemType<T: 'a>;
    fn out_neighbours(self) -> Self::ItemType<NeighboursIterator<'a, G>>;
    fn in_neighbours(self) -> Self::ItemType<NeighboursIterator<'a, G>>;
    fn neighbours(self) -> Self::ItemType<NeighboursIterator<'a, G>>;
    fn out_edges(self) -> Self::ItemType<EdgeIterator<'a, G>>;
    fn in_edges(self) -> Self::ItemType<EdgeIterator<'a, G>>;
    fn edges(self) -> Self::ItemType<EdgeIterator<'a, G>>;
    fn id(self) -> Self::ItemType<u64>;
    fn out_degree(self) -> Self::ItemType<usize>;
    fn in_degree(self) -> Self::ItemType<usize>;
    fn degree(self) -> Self::ItemType<usize>;
    fn with_state<A: Clone>(self, state: &'a StateVec<A>) -> Self::ItemType<A>;
    fn with_window(self, window: Range<i64>) -> Self::ItemType<VertexView<'a, G>>;
    fn property_history(self, name: &str) -> Self::ItemType<Option<PropertyHistory<'a>>>;
}

impl<'a, G> VertexViewMethods<'a, G> for VertexView<'a, G>
where
    G: GraphViewInternals,
{
    type ItemType<T: 'a> = T;
    fn out_neighbours(self) -> NeighboursIterator<'a, G> {
        self.g.neighbours(&self, Direction::OUT)
    }

    fn in_neighbours(self) -> NeighboursIterator<'a, G> {
        self.g.neighbours(&self, Direction::IN)
    }

    fn neighbours(self) -> NeighboursIterator<'a, G> {
        self.g.neighbours(&self, Direction::BOTH)
    }

    fn out_edges(self) -> Self::ItemType<EdgeIterator<'a, G>> {
        self.g.edges(&self, Direction::OUT)
    }

    fn in_edges(self) -> Self::ItemType<EdgeIterator<'a, G>> {
        self.g.edges(&self, Direction::IN)
    }

    fn edges(self) -> Self::ItemType<EdgeIterator<'a, G>> {
        self.g.edges(&self, Direction::BOTH)
    }

    fn id(self) -> Self::ItemType<u64> {
        // need to take ownership for chaining iterators
        self.g_id
    }

    fn out_degree(self) -> Self::ItemType<usize> {
        // need to take ownership for chaining iterators
        self.g.degree(&self, Direction::OUT)
    }

    fn in_degree(self) -> Self::ItemType<usize> {
        // need to take ownership for chaining iterators
        self.g.degree(&self, Direction::IN)
    }

    fn degree(self) -> Self::ItemType<usize> {
        // need to take ownership for chaining iterators
        self.g.degree(&self, Direction::BOTH)
    }

    fn with_state<A: Clone>(self, state: &'a StateVec<A>) -> Self::ItemType<A> {
        state.get(&self).clone()
    }

    fn with_window(self, window: Range<i64>) -> Self {
        VertexView {
            w: Some(window),
            ..self
        }
    }

    fn property_history(self, name: &str) -> Option<PropertyHistory<'a>> {
        self.g.property_history(&self, name)
    }
}

impl<'a, T, R, G> VertexViewMethods<'a, G> for T
where
    T: IntoIterator<Item = R> + 'a,
    R: VertexViewMethods<'a, G> + 'a,
    G: GraphViewInternals,
{
    type ItemType<U: 'a> = Box<dyn Iterator<Item = R::ItemType<U>> + 'a>;

    fn out_neighbours(self) -> Self::ItemType<NeighboursIterator<'a, G>> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.out_neighbours()))
    }

    fn in_neighbours(self) -> Self::ItemType<NeighboursIterator<'a, G>> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.in_neighbours()))
    }

    fn neighbours(self) -> Self::ItemType<NeighboursIterator<'a, G>> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.neighbours()))
    }

    fn out_edges(self) -> Self::ItemType<EdgeIterator<'a, G>> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.out_edges()))
    }

    fn in_edges(self) -> Self::ItemType<EdgeIterator<'a, G>> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.in_edges()))
    }

    fn edges(self) -> Self::ItemType<EdgeIterator<'a, G>> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.edges()))
    }

    fn id(self) -> Self::ItemType<u64> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.id()))
    }

    fn out_degree(self) -> Self::ItemType<usize> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.out_degree()))
    }

    fn in_degree(self) -> Self::ItemType<usize> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.in_degree()))
    }

    fn degree(self) -> Self::ItemType<usize> {
        let inner = self.into_iter();
        Box::new(inner.map(|v| v.degree()))
    }

    fn with_state<A: Clone>(self, state: &'a StateVec<A>) -> Self::ItemType<A> {
        let inner = self.into_iter();

        Box::new(inner.map(move |v: R| v.with_state(state)))
    }

    fn with_window(self, window: Range<i64>) -> Self::ItemType<VertexView<'a, G>> {
        let inner = self.into_iter();

        Box::new(inner.map(move |v: R| v.with_window(window)))
    }

    fn property_history(self, name: &str) -> Self::ItemType<Option<PropertyHistory<'a>>> {
        let inner = self.into_iter();

        Box::new(inner.map(move |v: R| v.property_history(name)))
    }
}
