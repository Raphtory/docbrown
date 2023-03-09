pub mod edge;
pub mod graph;
pub mod vertex;

pub struct WrappedIterator<V, I: Iterator<Item=V>> {
    pub(crate) iter: I,
}

impl<V, I: Iterator<Item=V>> IntoIterator for WrappedIterator<V, I> {
    type Item = V;
    type IntoIter = I;

    fn into_iter(self) -> Self::IntoIter {
        self.iter
    }
}

