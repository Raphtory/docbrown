use crate::graphview::LocalVertexView;

pub struct StateVec<T> {
    values: Vec<T>,
}

pub trait State<T> {
    fn get(&self, vertex: &LocalVertexView) -> &T;

    fn set(&mut self, vertex: &LocalVertexView, value: T);
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

impl<T: Clone> Clone for StateVec<T> {
    fn clone(&self) -> Self {
        Self{values: self.values.clone()}
    }
}

impl<T> State<T> for StateVec<T> {
    fn get(&self, vertex: &LocalVertexView) -> &T {
        &self.values[vertex.pid]
    }

    fn set(&mut self, vertex: &LocalVertexView, value: T) {
        self.values[vertex.pid] = value
    }
}
