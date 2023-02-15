use crate::graphview::GraphViewInternals;
use crate::vertexview::VertexView;
use std::slice::Iter;

pub struct StateVec<T> {
    pub(crate) values: Vec<T>,
}

pub trait State<T, G>
where
    G: GraphViewInternals,
{
    fn get(&self, vertex: &VertexView<G>) -> &T;

    fn set(&mut self, vertex: &VertexView<G>, value: T);
}

impl<T: Clone> StateVec<T> {
    pub(crate) fn empty(size: usize) -> StateVec<Option<T>> {
        StateVec {
            values: vec![None; size],
        }
    }

    pub(crate) fn full(value: T, size: usize) -> StateVec<T> {
        StateVec {
            values: vec![value; size],
        }
    }
}

impl<T> StateVec<T> {
    pub(crate) fn len(&self) -> usize {
        self.values.len()
    }

    pub fn iter(&self) -> Iter<'_, T> {
        self.values.iter()
    }
}

impl<T> FromIterator<T> for StateVec<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        Self {
            values: Vec::from_iter(iter),
        }
    }
}

impl<T> From<Vec<T>> for StateVec<T> {
    fn from(value: Vec<T>) -> Self {
        StateVec { values: value }
    }
}

impl<T> From<StateVec<T>> for Vec<T> {
    fn from(value: StateVec<T>) -> Self {
        value.values
    }
}

impl<T: Clone> Clone for StateVec<T> {
    fn clone(&self) -> Self {
        Self {
            values: self.values.clone(),
        }
    }
}

impl<T, G> State<T, G> for StateVec<T>
where
    G: GraphViewInternals,
{
    fn get(&self, vertex: &VertexView<G>) -> &T {
        &self.values[vertex.pid]
    }

    fn set(&mut self, vertex: &VertexView<G>, value: T) {
        self.values[vertex.pid] = value
    }
}
