//! This module defines the `Perspective` struct and the `PerspectiveSet` iterator.
//!
//! `Perspective` is a simple struct representing a time range from `start` to `end`.
//! The start time is inclusive and the end time is exclusive.
//!
//! `PerspectiveSet` is an iterator over a range of time periods (`Perspective`s).
//! It can be used to generate rolling or expanding perspectives based on a `step` size and an optional `window` size.
//!
//! These perpectives are used when querying the graph to determine the time bounds.
//!
//! # Examples
//! ```rust
//! use docbrown::algorithms::degree::average_degree;
//! use docbrown::db::graph::Graph;
//! use docbrown::db::perspective::Perspective;
//! use docbrown::db::view_api::*;
//!
//! let graph = Graph::new(1);
//! graph.add_edge(0, 1, 2, &vec![], None);
//! graph.add_edge(0, 1, 3, &vec![], None);
//! graph.add_edge(1, 2, 3, &vec![], None);
//! graph.add_edge(2, 2, 4, &vec![], None);
//! graph.add_edge(3, 2, 1, &vec![], None);
//!
//! let start = graph.start();
//! let end = graph.end();
//! let perspectives = Perspective::expanding(1, start, end);
//!
//! // A rolling perspective with a window size of 2 and a step size of 1
//! let view_persp = graph.through_perspectives(perspectives);
//!
//! for window in view_persp {
//!   println!("Degree: {:?}", average_degree(&window.graph));
//! }
//!
//! ```
use crate::core::time::{Interval, IntoBoundWithFormat, ParseTimeError};
use chrono::{NaiveDate, NaiveDateTime, ParseError};
use std::ops::Range;

/// A struct representing a time range from `start` to `end`.
///
/// The start time is inclusive and the end time is exclusive.
#[derive(Debug, PartialEq)]
pub struct Perspective {
    pub start: Option<i64>, // inclusive
    pub end: Option<i64>,   // exclusive
}

/// Representing a time range from `start` to `end` for a graph
impl Perspective {
    /// Creates a new `Perspective` with the given `start` and `end` times.
    pub fn new(start: Option<i64>, end: Option<i64>) -> Perspective {
        Perspective { start, end }
    }

    /// Creates a new `Perspective` with a backward-facing window of size `window`
    /// that ends inclusively at `inclusive_end`.
    pub(crate) fn new_backward(window: Option<Interval>, inclusive_end: i64) -> Perspective {
        Perspective {
            start: window.map(|w| inclusive_end + 1 - w),
            end: Some(inclusive_end + 1),
        }
    }

    /// Creates an `PerspectiveSet` with the given `step` size and optional `start` and `end` times,
    /// using an expanding window.
    ///
    /// An expanding window is a window that grows by `step` size at each iteration.
    pub fn expanding(step: u64, start: Option<i64>, end: Option<i64>) -> PerspectiveSet {
        PerspectiveSet {
            start,
            end,
            step: step.into(),
            window: None,
            epoch_alignment: false,
        }
    }

    pub fn expanding_dates(
        step: &str,
        start: Option<&str>,
        end: Option<&str>,
        format: Option<&str>,
    ) -> Result<PerspectiveSet, ParseTimeError> {
        let start = start.map(|start| start.into_bound(format)).transpose()?;
        let end = end.map(|end| end.into_bound(format)).transpose()?;
        Ok(PerspectiveSet {
            start,
            end,
            step: step.try_into()?,
            window: None,
            epoch_alignment: true,
        })
    }

    /// Creates an `PerspectiveSet` with the given `window` size and optional `step`, `start` and `end` times,
    /// using a rolling window.
    ///
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    pub fn rolling(
        window: u64,
        step: Option<u64>,
        start: Option<i64>,
        end: Option<i64>,
    ) -> PerspectiveSet {
        PerspectiveSet {
            start,
            end,
            step: step.unwrap_or(window).into(),
            window: Some(window.into()),
            epoch_alignment: false,
        }
    }

    pub fn rolling_dates(
        window: &str,
        step: Option<&str>,
        start: Option<&str>,
        end: Option<&str>,
        format: Option<&str>, // format shouldn't be optional here!!!
    ) -> Result<PerspectiveSet, ParseTimeError> {
        let start = start.map(|start| start.into_bound(format)).transpose()?;
        let end = end.map(|end| end.into_bound(format)).transpose()?;
        Ok(PerspectiveSet {
            start,
            end,
            step: step.unwrap_or(window).try_into()?,
            window: Some(window.try_into()?),
            epoch_alignment: true,
        })
    }

    // TODO pub fn weeks(n), days(n), hours(n), minutes(n), seconds(n), millis(n)
}

/// A PerspectiveSet represents a set of windows on a timeline,
/// defined by a start, end, step, and window size.
#[derive(Clone)]
pub struct PerspectiveSet {
    start: Option<i64>,
    end: Option<i64>,
    step: Interval,
    window: Option<Interval>,
    epoch_alignment: bool,
}

/// A PerspectiveSet represents a set of windows on a timeline,
/// defined by a start, end, step, and window size.
impl PerspectiveSet {
    /// Given a timeline, build an iterator over this PerspectiveSet.
    /// If a start is specified, use it. Otherwise, start the iterator just before the timeline's start.
    /// If an end is specified, use it. Otherwise, end the iterator at the timeline's end.
    /// If a window is specified, use it. Otherwise, use a window size of 1.
    /// If the cursor of the iterator is more than or equal to end, return None.

    // FIXME: having timeline as a Range is not clear, because I think this end is inclusive !!!
    pub(crate) fn build_iter(&self, timeline: Range<i64>) -> PerspectiveIterator {
        let start = self.start.unwrap_or(self.default_start(&timeline));
        let end = self.end.unwrap_or(match self.window {
            None => timeline.end,
            Some(window) => timeline.end + window - self.step,
            // the iterator returns None when cursor - step >= end and we want to do so when:
            // perspective.start > timeline.end <==> perspective.start >= timeline.end + 1
            // as: cursor = perspective.end - 1
            // and: perspective.start = perspective.end - window
            // then: cursor - step >= timeline.end + window - step = end
            // that's why we do `end = timeline.end + window - self.step` here
        });
        PerspectiveIterator {
            cursor: start,
            end,
            step: self.step.clone(),
            window: self.window.clone(),
        }
    }

    /// this function returns the last time point included in the first perspective of the range
    fn default_start(&self, timeline: &Range<i64>) -> i64 {
        if self.epoch_alignment {
            // assuming that there are no months, we can actually use division
            let step = self.step.to_millis().unwrap();
            // we are calculating here the exclusive end of the last perspective omitted:
            let prev_perspective_exclusive_end = (timeline.start / step) * step; // timeline.start 5 step 3 -> 3, timeline.start 6 step 3 -> 6, timeline.start 7 step 3 -> 6
            let prev_perspective_inclusive_end = prev_perspective_exclusive_end - 1;
            prev_perspective_inclusive_end + self.step
        } else {
            // it doesn't make any sense that it only includes 1 point if the the step is
            // large. Instead we put the first window such that the previous one ends just before
            // the start of the timeline, i.e. the previous cursor position = timeline.start - 1
            timeline.start - 1 + self.step
        }
    }
}

/// An iterator over a PerspectiveSet. Yields Perspectives over a timeline.
pub(crate) struct PerspectiveIterator {
    cursor: i64, // last point to be included in the next perspective
    // if cursor - step >= end, this iterator returns None. This means that if the cursor matches
    // the end at some point, that will be the last perspective to be returned. If the cursor
    // doesn't match the end at any point, we will return perspectives until we get a perspective
    // that surpass the end, and that perspective will be the last perspective returned
    end: i64,
    step: Interval,
    window: Option<Interval>,
}

/// An iterator over a PerspectiveSet. Yields Perspectives over a timeline.
impl PerspectiveIterator {
    /// Create an empty PerspectiveIterator. Used when the PerspectiveSet has no windows.
    pub(crate) fn empty() -> PerspectiveIterator {
        PerspectiveIterator {
            cursor: i64::MAX,
            end: i64::MIN,
            step: 1.into(),
            window: None,
        }
    }
}

impl Iterator for PerspectiveIterator {
    type Item = Perspective;

    fn next(&mut self) -> Option<Self::Item> {
        if self.cursor - self.step >= self.end {
            None
        } else {
            let current_cursor = self.cursor;
            self.cursor = self.cursor + self.step;
            Some(Perspective::new_backward(self.window, current_cursor))
        }
    }
}

#[cfg(test)]
mod perspective_tests {
    use crate::db::perspective::Perspective;
    use chrono::format::parse;
    use chrono::NaiveDateTime;
    use itertools::Itertools;

    fn gen_rolling(tuples: Vec<(i64, i64)>) -> Vec<Perspective> {
        tuples
            .iter()
            .map(|(start, end)| Perspective::new(Some(*start), Some(*end)))
            .collect()
    }

    fn gen_expanding(tuples: Vec<i64>) -> Vec<Perspective> {
        tuples
            .iter()
            .map(|point| Perspective::new(None, Some(*point)))
            .collect()
    }

    #[test]
    fn rolling_with_all_none() {
        let windows = Perspective::rolling(2, None, None, None);
        let expected = gen_rolling(vec![(1, 3), (3, 5)]);
        assert_eq!(windows.build_iter(1..3).collect_vec(), expected);
    }

    #[test]
    fn rolling_with_start_and_end() {
        let windows = Perspective::rolling(3, Some(2), Some(-5), Some(-1));
        let expected = gen_rolling(vec![(-7, -4), (-5, -2), (-3, 0)]);
        assert_eq!(windows.build_iter(0..0).collect_vec(), expected);

        let windows = Perspective::rolling(3, Some(2), Some(-5), Some(0));
        let expected = gen_rolling(vec![(-7, -4), (-5, -2), (-3, 0), (-1, 2)]);
        assert_eq!(windows.build_iter(0..0).collect_vec(), expected);
    }

    #[test]
    fn rolling_with_end() {
        let windows = Perspective::rolling(3, Some(2), None, Some(4));
        let expected = gen_rolling(vec![(-1, 2), (1, 4), (3, 6)]);
        assert_eq!(windows.build_iter(0..2).collect_vec(), expected);
    }

    #[test]
    fn rolling_with_start() {
        let windows = Perspective::rolling(3, Some(2), Some(2), None);
        let expected = gen_rolling(vec![(0, 3), (2, 5), (4, 7)]);
        assert_eq!(windows.build_iter(0..4).collect_vec(), expected);
    }

    #[test]
    fn expanding_with_all_none() {
        let windows = Perspective::expanding(2, None, None);
        let expected = gen_expanding(vec![2, 4, 6]);
        assert_eq!(windows.build_iter(0..4).collect_vec(), expected);
    }

    #[test]
    fn expanding_with_start_and_end() {
        let windows = Perspective::expanding(2, Some(3), Some(6));
        let expected = gen_expanding(vec![4, 6, 8]);
        assert_eq!(windows.build_iter(0..0).collect_vec(), expected);
    }

    #[test]
    fn expanding_with_start() {
        let windows = Perspective::expanding(2, Some(3), None);
        let expected = gen_expanding(vec![4, 6, 8]);
        assert_eq!(windows.build_iter(0..6).collect_vec(), expected);
    }

    #[test]
    fn expanding_with_end() {
        let windows = Perspective::expanding(2, None, Some(5));
        let expected = gen_expanding(vec![2, 4, 6]);
        assert_eq!(windows.build_iter(0..4).collect_vec(), expected);
    }

    fn parse_date(datetime: &str) -> i64 {
        NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S%.3f")
            .unwrap()
            .timestamp_millis()
    }

    fn gen_rolling_dates(tuples: Vec<(&str, &str)>) -> Vec<Perspective> {
        tuples
            .iter()
            .map(|(start, end)| (parse_date(start), parse_date(end)))
            .map(|(start, end)| Perspective::new(Some(start), Some(end)))
            .collect()
    }

    // For dates, we just really want to check two things:
    // - the epoch alignment works
    // - dates work as bounds

    #[test]
    fn rolling_dates() {
        let windows = Perspective::rolling_dates("1 day", None, None, None, None).unwrap();

        let start = parse_date("2020-06-06 00:00:00.000");
        let end = parse_date("2020-06-06 23:59:59.999");
        let expected = gen_rolling_dates(vec![
            ("2020-06-06 00:00:00.000", "2020-06-07 00:00:00.000"), // entire 2020-06-06
        ]);
        assert_eq!(windows.build_iter(start..end).collect_vec(), expected);

        let start = parse_date("2020-06-06 00:00:00.000");
        let end = parse_date("2020-06-07 00:00:00.000");
        let expected = gen_rolling_dates(vec![
            ("2020-06-06 00:00:00.000", "2020-06-07 00:00:00.000"), // entire 2020-06-06
            ("2020-06-07 00:00:00.000", "2020-06-08 00:00:00.000"), // entire 2020-06-07
        ]);
        assert_eq!(windows.build_iter(start..end).collect_vec(), expected);

        let start = parse_date("2020-06-05 23:59:59.999");
        let end = parse_date("2020-06-06 23:59:59.999");
        let expected = gen_rolling_dates(vec![
            ("2020-06-05 00:00:00.000", "2020-06-06 00:00:00.000"), // entire 2020-06-05
            ("2020-06-06 00:00:00.000", "2020-06-07 00:00:00.000"), // entire 2020-06-06
        ]);
        assert_eq!(windows.build_iter(start..end).collect_vec(), expected);
    }

    #[test]
    fn rolling_dates_with_bounds() {
        let windows =
            Perspective::rolling_dates("1 day", None, Some("2020-06-06"), Some("2020-06-07"), None)
                .unwrap();
        let expected = gen_rolling_dates(vec![
            ("2020-06-06 00:00:00.000", "2020-06-07 00:00:00.000"), // entire 2020-06-06
            ("2020-06-07 00:00:00.000", "2020-06-08 00:00:00.000"), // entire 2020-06-07
        ]);
        assert_eq!(windows.build_iter(0..0).collect_vec(), expected);
    }
}
