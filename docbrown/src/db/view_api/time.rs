use crate::core::time::{AlignmentType, Interval, ParseTimeError};
use std::cmp::{max, min};
use std::iter;

/// Trait defining time query operations
pub trait TimeOps {
    type WindowedViewType;

    /// Return the timestamp of the default start for perspectives of the view (if any).
    fn start(&self) -> Option<i64>;

    /// Return the timestamp of the default for perspectives of the view (if any).
    fn end(&self) -> Option<i64>;

    /// the larger of `t_start` and `self.start()` (useful for creating nested windows)
    fn actual_start(&self, t_start: i64) -> i64 {
        match self.start() {
            None => t_start,
            Some(start) => max(t_start, start),
        }
    }

    /// the smaller of `t_end` and `self.end()` (useful for creating nested windows)
    fn actual_end(&self, t_end: i64) -> i64 {
        match self.end() {
            None => t_end,
            Some(end) => min(t_end, end),
        }
    }

    /// Create a view including all events between `t_start` (inclusive) and `t_end` (exclusive)
    fn window(&self, t_start: i64, t_end: i64) -> Self::WindowedViewType;

    /// Create a view including all events until `end` (inclusive)
    fn at(&self, end: i64) -> Self::WindowedViewType {
        self.window(i64::MIN, end.saturating_add(1))
    }

    /// Creates a `WindowSet` with the given `step` size and optional `start` and `end` times,    
    /// using an expanding window.
    ///
    /// An expanding window is a window that grows by `step` size at each iteration.
    fn expanding<I>(
        &self,
        step: I,
    ) -> Result<Box<dyn Iterator<Item = Self::WindowedViewType>>, ParseTimeError>
    where
        Self: Sized + Clone + 'static, // TODO: is this fine???
        I: TryInto<Interval, Error = ParseTimeError> + AlignmentType,
    {
        let parent = self.clone();
        match (self.start(), self.end()) {
            (Some(start), Some(end)) => {
                let step: Interval = step.try_into()?;
                let epoch_alignment = I::epoch_alignment();
                let cursor_iter = build_cursor(start, end, step, epoch_alignment);
                let window_iter = cursor_iter.map(move |cursor| parent.at(cursor));
                Ok(Box::new(window_iter))
            }
            _ => Ok(Box::new(iter::empty())),
        }
    }

    /// Creates a `WindowSet` with the given `window` size and optional `step`, `start` and `end` times,
    /// using a rolling window.
    ///
    /// A rolling window is a window that moves forward by `step` size at each iteration.
    fn rolling<I>(
        &self,
        window: I,
        step: Option<I>,
    ) -> Result<Box<dyn Iterator<Item = Self::WindowedViewType>>, ParseTimeError>
    where
        Self: Sized + Clone + 'static, // TODO: is this fine???
        I: TryInto<Interval, Error = ParseTimeError> + AlignmentType,
    {
        let parent = self.clone();
        match (self.start(), self.end()) {
            (Some(start), Some(end)) => {
                let window: Interval = window.try_into()?;
                let step: Interval = match step {
                    Some(step) => step.try_into()?,
                    None => window,
                };
                let epoch_alignment = I::epoch_alignment();
                let cursor_iter = build_cursor(start, end, step, epoch_alignment);
                let window_iter =
                    cursor_iter.map(move |cursor| parent.window(cursor - window + 1, cursor + 1));
                Ok(Box::new(window_iter))
            }
            _ => Ok(Box::new(iter::empty())),
        }
    }

    // TODO pub fn weeks(n), days(n), hours(n), minutes(n), seconds(n), millis(n)
}

fn build_cursor(
    timeline_start: i64,
    end: i64,
    step: Interval,
    epoch_alignment: bool,
) -> Box<dyn Iterator<Item = i64>> {
    let cursor_start = if epoch_alignment {
        let step = step.to_millis().unwrap();
        let prev_perspective_exclusive_end = (timeline_start / step) * step; // timeline.start 5 step 3 -> 3, timeline.start 6 step 3 -> 6, timeline.start 7 step 3 -> 6
        let prev_perspective_inclusive_end = prev_perspective_exclusive_end - 1;
        prev_perspective_inclusive_end + step
    } else {
        timeline_start + step - 1
    };
    let cursor_iter = iter::successors(Some(cursor_start), move |cursor| Some(*cursor + step))
        .take_while(move |cursor| *cursor < end);
    Box::new(cursor_iter)
}

#[cfg(test)]
mod time_tests {
    use crate::db::graph::Graph;
    use crate::db::graph_window::WindowedGraph;
    use crate::db::view_api::internal::GraphViewInternalOps;
    use crate::db::view_api::{GraphViewOps, TimeOps};
    use chrono::format::parse;
    use chrono::NaiveDateTime;
    use itertools::Itertools;

    // start inclusive, end exclusive
    fn graph_with_timeline(start: i64, end: i64) -> Graph {
        let g = Graph::new(4);
        g.add_vertex(start, 0, &vec![]).unwrap();
        g.add_vertex(end - 1, 0, &vec![]).unwrap();
        assert_eq!(g.start().unwrap(), start);
        assert_eq!(g.end().unwrap(), end);
        g
    }

    fn assert_bounds<G>(
        windows: Box<dyn Iterator<Item = WindowedGraph<G>>>,
        expected: Vec<(i64, i64)>,
    ) where
        G: GraphViewOps + GraphViewInternalOps,
    {
        let window_bounds = windows
            .map(|w| (w.start().unwrap(), w.end().unwrap()))
            .collect_vec();
        assert_eq!(window_bounds, expected)
    }

    #[test]
    fn rolling() {
        let g = graph_with_timeline(1, 7);
        let windows = g.rolling(2, None).unwrap();
        let expected = vec![(1, 3), (3, 5), (5, 7)];
        assert_bounds(windows, expected);

        let g = graph_with_timeline(1, 6);
        let windows = g.rolling(3, Some(2)).unwrap();
        let expected = vec![(0, 3), (2, 5)];
        assert_bounds(windows, expected.clone());

        let g = graph_with_timeline(0, 9).window(1, 6);
        let windows = g.rolling(3, Some(2)).unwrap();
        assert_bounds(windows, expected);
    }

    #[test]
    fn expanding() {
        let min = i64::MIN;
        let g = graph_with_timeline(1, 7);
        let windows = g.expanding(2).unwrap();
        let expected = vec![(min, 3), (min, 5), (min, 7)];
        assert_bounds(windows, expected);

        let g = graph_with_timeline(1, 6);
        let windows = g.expanding(2).unwrap();
        let expected = vec![(min, 3), (min, 5)];
        assert_bounds(windows, expected.clone());

        let g = graph_with_timeline(0, 9).window(1, 6);
        let windows = g.expanding(2).unwrap();
        assert_bounds(windows, expected);
    }

    fn parse_date(datetime: &str) -> i64 {
        NaiveDateTime::parse_from_str(datetime, "%Y-%m-%d %H:%M:%S%.3f")
            .unwrap()
            .timestamp_millis()
    }

    #[test]
    fn rolling_dates() {
        let start = parse_date("2020-06-06 00:00:00.000");
        let end = parse_date("2020-06-07 23:59:59.999");
        let g = graph_with_timeline(start, end);
        let windows = g.rolling("1 day", None).unwrap();
        let expected = vec![(
            parse_date("2020-06-06 00:00:00.000"), // entire 2020-06-06
            parse_date("2020-06-07 00:00:00.000"),
        )];
        assert_bounds(windows, expected);

        let start = parse_date("2020-06-06 00:00:00.000");
        let end = parse_date("2020-06-08 00:00:00.000");
        let g = graph_with_timeline(start, end);
        let windows = g.rolling("1 day", None).unwrap();
        let expected = vec![
            (
                parse_date("2020-06-06 00:00:00.000"), // entire 2020-06-06
                parse_date("2020-06-07 00:00:00.000"),
            ),
            (
                parse_date("2020-06-07 00:00:00.000"), // entire 2020-06-07
                parse_date("2020-06-08 00:00:00.000"),
            ),
        ];
        assert_bounds(windows, expected);

        let start = parse_date("2020-06-05 23:59:59.999");
        let end = parse_date("2020-06-07 00:00:00.000");
        let g = graph_with_timeline(start, end);
        let windows = g.rolling("1 day", None).unwrap();
        let expected = vec![
            (
                parse_date("2020-06-05 00:00:00.000"), // entire 2020-06-06
                parse_date("2020-06-06 00:00:00.000"),
            ),
            (
                parse_date("2020-06-06 00:00:00.000"), // entire 2020-06-07
                parse_date("2020-06-07 00:00:00.000"),
            ),
        ];
        assert_bounds(windows, expected);
    }

    #[test]
    fn expanding_dates() {
        let min = i64::MIN;

        let start = parse_date("2020-06-06 10:01:00.010");
        let end = parse_date("2020-06-07 23:59:59.999");
        let g = graph_with_timeline(start, end);
        let windows = g.expanding("1 day").unwrap();
        let expected = vec![(min, parse_date("2020-06-07 00:00:00.000"))];
        assert_bounds(windows, expected);

        let start = parse_date("2020-06-06 10:01:00.010");
        let end = parse_date("2020-06-08 00:00:00.000");
        let g = graph_with_timeline(start, end);
        let windows = g.expanding("1 day").unwrap();
        let expected = vec![
            (min, parse_date("2020-06-07 00:00:00.000")),
            (min, parse_date("2020-06-08 00:00:00.000")),
        ];
        assert_bounds(windows, expected);
    }
}
