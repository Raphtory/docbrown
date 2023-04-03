use crate::graph_window::WindowSet;
use crate::perspective::{Perspective, PerspectiveIterator, PerspectiveSet};
use std::iter;

/// Trait defining time query operations
pub trait TimeOps: Clone {
    type WindowedView;

    /// Return the timestamp of the earliest activity in the view (if any).
    fn earliest_time(&self) -> Option<i64>;

    /// Return the timestamp of the latest activity in the view (if any).
    fn latest_time(&self) -> Option<i64>;

    /// Create a view including all events between `t_start` (inclusive) and `t_end` (exclusive)
    fn window(&self, t_start: i64, t_end: i64) -> Self::WindowedView;

    /// Create a view including all events until `end` (inclusive)
    fn at(&self, end: i64) -> Self::WindowedView {
        self.window(i64::MIN, end.saturating_add(1))
    }
    fn through_perspectives(&self, perspectives: PerspectiveSet) -> WindowSet<Self>
    where
        Self: Sized,
    {
        let iter = match (self.earliest_time(), self.latest_time()) {
            (Some(start), Some(end)) => perspectives.build_iter(start..end),
            _ => PerspectiveIterator::empty(),
        };
        WindowSet::new(self.clone(), Box::new(iter))
    }

    fn through_iter(
        &self,
        perspectives: Box<dyn Iterator<Item = Perspective> + Send>,
    ) -> WindowSet<Self>
    where
        Self: Sized,
    {
        let iter = if self.earliest_time().is_some() && self.latest_time().is_some() {
            perspectives
        } else {
            Box::new(iter::empty::<Perspective>())
        };
        WindowSet::new(self.clone(), iter)
    }
}
