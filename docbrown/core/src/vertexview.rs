use crate::graph::{EdgeView, TemporalGraph};
use crate::graphview::{
    GraphView, GraphViewInternals, IteratorWithLifetime, NeighboursIterator, PropertyHistory,
};
use crate::state::{State, StateVec};
use crate::{Direction, Prop};
use std::borrow::Borrow;
use std::ops::Range;

pub struct VertexView<'a, G>
where
    G: GraphViewInternals,
{
    g_id: u64,
    pid: usize,
    g: &'a G,
    w: Option<Range<i64>>,
}

impl<'a> VertexView<'a, TemporalGraph> {
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
    // // FIXME: all the functions using global ID need to be changed to use the physical ID instead
    // pub fn edges(
    //     &'a self,
    //     d: Direction,
    // ) -> Box<dyn Iterator<Item = EdgeView<'a, TemporalGraph>> + 'a> {
    //     if let Some(r) = &self.w {
    //         self.g.neighbours_window(self.g_id, r, d)
    //     } else {
    //         self.g.neighbours(self.g_id, d)
    //     }
    // }
    //
    // pub fn props(&self, name: &'a str) -> Option<Box<dyn Iterator<Item = (&'a i64, Prop)> + 'a>> {
    //     let index = self.g.logical_to_physical.get(&self.g_id)?;
    //     let meta = self.g.props.vertex_meta.get(*index)?;
    //     let prop_id = self.g.props.prop_ids.get(name)?;
    //     Some(meta.iter(*prop_id))
    // }
    //
    // pub fn props_window(
    //     &self,
    //     name: &'a str,
    //     r: Range<i64>,
    // ) -> Option<Box<dyn Iterator<Item = (&'a i64, Prop)> + 'a>> {
    //     let index = self.g.logical_to_physical.get(&self.g_id)?;
    //     let meta = self.g.props.vertex_meta.get(*index)?;
    //     let prop_id = self.g.props.prop_ids.get(name)?;
    //     Some(meta.iter_window(*prop_id, r))
    // }
}

pub trait VertexViewMethods<'a, G>
where
    G: GraphViewInternals,
{
    type ItemType<T: 'a>;
    fn out_neighbours(self) -> Self::ItemType<NeighboursIterator<'a, G>>;
    fn in_neighbours(self) -> Self::ItemType<NeighboursIterator<'a, G>>;
    fn neighbours(self) -> Self::ItemType<NeighboursIterator<'a, G>>;
    fn id(self) -> Self::ItemType<u64>;
    fn out_degree(self) -> Self::ItemType<usize>;
    fn in_degree(self) -> Self::ItemType<usize>;
    fn degree(self) -> Self::ItemType<usize>;
    fn with_state<A: Clone>(self, state: &'a StateVec<A>) -> Self::ItemType<A>;
    fn property_history(self, name: &str) -> Self::ItemType<PropertyHistory>;
}

impl<'a, G> VertexViewMethods<'a, G> for VertexView<'a, G>
where
    G: GraphViewInternals,
{
    type ItemType<T: 'a> = T;
    fn out_neighbours(self) -> NeighboursIterator<'a, G> {
        Box::new(self.g.neighbours(&self, Direction::OUT))
    }

    fn in_neighbours(self) -> NeighboursIterator<'a, G> {
        Box::new(self.g.neighbours(&self, Direction::IN))
    }

    fn neighbours(self) -> NeighboursIterator<'a, G> {
        Box::new(self.g.neighbours(&self, Direction::BOTH))
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

    fn property_history(self, name: &str) -> Self::ItemType<PropertyHistory> {
        todo!()
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

    fn property_history(self, name: &str) -> Self::ItemType<PropertyHistory> {
        todo!()
    }
}
