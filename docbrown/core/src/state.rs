use crate::vertexview::VertexPointer;
use std::slice::Iter;

pub struct StateVec<T> {
    pub(crate) values: Vec<T>,
}

pub trait State<T> {
    fn get(&self, vertex: VertexPointer) -> &T;

    fn set(&mut self, vertex: VertexPointer, value: T);

    fn iter(&self) -> Box<dyn Iterator<Item = &T> + '_>;
}

impl<T: Clone> StateVec<T> {
    pub fn empty(size: usize) -> StateVec<Option<T>> {
        StateVec {
            values: vec![None; size],
        }
    }

    pub fn full(value: T, size: usize) -> StateVec<T> {
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

impl<T> State<T> for StateVec<T> {
    fn get(&self, vertex: VertexPointer) -> &T {
        &self.values[vertex.pid]
    }

    fn set(&mut self, vertex: VertexPointer, value: T) {
        self.values[vertex.pid] = value
    }

    fn iter(&self) -> Box<(dyn Iterator<Item = &T> + '_)> {
        Box::new(self.values.iter())
    }
}
