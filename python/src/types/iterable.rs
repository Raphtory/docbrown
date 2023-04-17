use crate::types::repr::Repr;
use docbrown::db::view_api::BoxedIter;
use std::fmt::Formatter;
use std::sync::Arc;

pub struct Iterable<I: Send> {
    pub builder: Arc<dyn Fn() -> BoxedIter<I> + Send + Sync + 'static>,
}

impl<I: Send> Iterable<I> {
    pub fn iter(&self) -> BoxedIter<I> {
        (self.builder)()
    }
    pub fn new<F: Fn() -> BoxedIter<I> + Send + Sync + 'static>(name: String, builder: F) -> Self {
        Self {
            builder: Arc::new(builder),
        }
    }
}

impl<I: Send + Repr> Repr for Iterable<I> {
    fn repr(&self) -> String {
        let values: Vec<String> = self.iter().take(11).map(|v| v.repr()).collect();
        let res = if values.len() < 11 {
            "[".to_string() + &values.join(", ") + "]"
        } else {
            "[".to_string() + &values[0..10].join(", ") + " ...]"
        };
        res
    }
}

pub struct NestedIterable<I: Send> {
    pub name: String,
    pub builder: Arc<dyn Fn() -> BoxedIter<BoxedIter<I>> + Send + Sync + 'static>,
}

impl<I: Send> NestedIterable<I> {
    pub fn iter(&self) -> BoxedIter<BoxedIter<I>> {
        (self.builder)()
    }
    pub fn new<F: Fn() -> BoxedIter<BoxedIter<I>> + Send + Sync + 'static>(
        name: String,
        builder: F,
    ) -> Self {
        Self {
            name,
            builder: Arc::new(builder),
        }
    }
}

impl<I: Send + Repr> Repr for NestedIterable<I> {
    fn repr(&self) -> String {
        let values: Vec<String> = self
            .iter()
            .take(11)
            .map(|it| {
                let values: Vec<String> = it.take(11).map(|v| v.repr()).collect();
                let res = if values.len() < 11 {
                    "[".to_string() + &values.join(", ") + "]"
                } else {
                    "[".to_string() + &values[0..10].join(", ") + " ...]"
                };
                res
            })
            .collect();

        let res = if values.len() < 11 {
            "[".to_string() + &values.join(", ") + "]"
        } else {
            "[".to_string() + &values[0..10].join(", ") + " ...]"
        };
        res
    }
}
