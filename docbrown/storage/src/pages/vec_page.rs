use std::{ops::{Range, RangeBounds}, cmp::Ordering};

use crate::Time;

use super::{PageData, PageError, PageId};

#[derive(Debug, PartialEq, Ord, Eq, Hash)]
enum Direction<V> {
    Out(V),
    In(V),
}

impl <T:PartialOrd> PartialOrd for Direction<T> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Direction::Out(a), Direction::Out(b)) => a.partial_cmp(b),
            (Direction::In(a), Direction::In(b)) => a.partial_cmp(b),
            (Direction::Out(_), Direction::In(_)) => Some(Ordering::Less),
            (Direction::In(_), Direction::Out(_)) => Some(Ordering::Greater),
        }
    }
}

impl<V> Direction<V> {
    fn into_inner(&self) -> &V {
        match self {
            Direction::Out(v) => v,
            Direction::In(v) => v,
        }
    }
}

#[derive(Debug, PartialEq)]
pub struct TemporalAdjacencySetPage<T, const N: usize> {
    // FIXME: N is not used
    sorted_values_index: Vec<usize>, // these ids are sorted by the values in the values vector
    values: Vec<Direction<T>>,
    pages: Vec<PageId>, // w can find ourselves in a state where the vertex doesn't have an adjacency list
    sorted_timestamps_index: Vec<usize>, // these ids are sorted by the values in the timestamps vector
    timestamps: Vec<Time>,
}

// [t1, t3, t3 t3, t1]
// [9,   3,  1, 6,  2]
// [2, 4, 1, 3, 0]
// [0, 4, 2, 3, 1]

impl<T: std::cmp::Ord, const N: usize> TemporalAdjacencySetPage<T, N> {
    pub fn new() -> TemporalAdjacencySetPage<T, N> {
        TemporalAdjacencySetPage {
            sorted_values_index: Vec::with_capacity(N),
            values: Vec::with_capacity(N),
            pages: Vec::with_capacity(N),
            sorted_timestamps_index: Vec::with_capacity(N),
            timestamps: Vec::with_capacity(N),
        }
    }

    fn insert_sorted<A: std::cmp::Ord>(
        sorted_vec: &mut Vec<usize>,
        values: &Vec<A>,
        value: &A,
        position_idx: usize,
    ) {
        match sorted_vec.binary_search_by(|probe| values[*probe].cmp(value)) {
            Ok(i) | Err(i) => sorted_vec.insert(i, position_idx),
        }
    }

    pub fn append_out(&mut self, value: T, t: Time, page: PageId) -> Result<(), PageError> {
        self.append(Direction::Out(value), t, page)
    }

    pub fn append_in(&mut self, value: T, t: Time, page: PageId) -> Result<(), PageError> {
        self.append(Direction::In(value), t, page)
    }

    fn append(&mut self, value: Direction<T>, t: Time, page: PageId) -> Result<(), PageError> {
        let position_idx = self.timestamps.len();
        // just add the tuples in the values and timestamps vectors

        // find where the position of t should be inserted and update the index
        Self::insert_sorted(
            &mut self.sorted_timestamps_index,
            &self.timestamps,
            &t,
            position_idx,
        );
        // find where the position of value should be inserted and update the index
        Self::insert_sorted(
            &mut self.sorted_values_index,
            &self.values,
            &value,
            position_idx,
        );

        self.values.push(value);
        self.timestamps.push(t);
        self.pages.push(page);
        Ok(())
    }

    pub fn is_full(&self) -> bool {
        self.values.len() == N
    }

    pub fn tuples_by_timestamp<'a>(&'a self) -> impl Iterator<Item = (Time, PageId, &'a T)> + 'a {
        self.sorted_timestamps_index.iter().map(move |idx| {
            (
                self.timestamps[*idx],
                self.pages[*idx],
                self.values[*idx].into_inner(),
            )
        })
    }

    pub fn tuples_sorted<'a>(&'a self) -> impl Iterator<Item = (Time, PageId, &'a T)> + 'a {
        self.sorted_values_index.iter().map(move |idx| {
            (
                self.timestamps[*idx],
                self.pages[*idx],
                self.values[*idx].into_inner(),
            )
        })
    }

    fn range_bounds_to_range<R: RangeBounds<Time>>(&self, w: R) -> Range<usize> {
        let (start, end) = match (w.start_bound(), w.end_bound()) {
            (std::ops::Bound::Included(start), std::ops::Bound::Included(end)) => {
                (*start, *end + 1)
            }
            (std::ops::Bound::Included(start), std::ops::Bound::Excluded(end)) => (*start, *end),
            (std::ops::Bound::Included(start), std::ops::Bound::Unbounded) => (*start, Time::MAX),

            (std::ops::Bound::Excluded(start), std::ops::Bound::Included(end)) => {
                (*start + 1, *end + 1)
            }
            (std::ops::Bound::Excluded(start), std::ops::Bound::Excluded(end)) => {
                (*start + 1, *end)
            }
            (std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded) => (*start, Time::MAX),
            (std::ops::Bound::Unbounded, std::ops::Bound::Included(end)) => (Time::MIN, *end + 1),
            (std::ops::Bound::Unbounded, std::ops::Bound::Excluded(end)) => (Time::MIN, *end),
            (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded) => (Time::MIN, Time::MAX),
        };

        let start_idx = match self.find_first_timestamp_position(start) {
            Ok(i) | Err(i) => i,
        };

        let range = match self.find_last_timestamp_position(end) {
            Ok(i) | Err(i) => start_idx..i,
        };
        range
    }

    pub fn tuples_window<R: RangeBounds<Time>>(&self, w: R) -> impl Iterator<Item = (Time, &T)> {
        let range = self.range_bounds_to_range(w);
        self.sorted_timestamps_index[range]
            .iter()
            .map(move |idx| (self.timestamps[*idx], self.values[*idx].into_inner()))
    }

    pub fn find_value(&self, value: &T) -> Option<&T> {
        match self
            .sorted_values_index
            .binary_search_by(|probe| self.values[*probe].into_inner().cmp(value))
        {
            Ok(i) => Some(&self.values[self.sorted_values_index[i]].into_inner()),
            Err(_) => None,
        }
    }

    fn find_last_timestamp_position(&self, t: Time) -> Result<usize, usize> {
        let res = self
            .sorted_timestamps_index
            .binary_search_by(|probe| self.timestamps[*probe].cmp(&t));

        match res {
            Ok(i) => {
                let mut k = i;
                for j in i..self.sorted_timestamps_index.len() {
                    if self.timestamps[self.sorted_timestamps_index[j]] != t {
                        break;
                    }
                    k = j;
                }
                Ok(k)
            }
            Err(i) => Err(i),
        }
    }

    fn find_first_timestamp_position(&self, t: Time) -> Result<usize, usize> {
        let res = self
            .sorted_timestamps_index
            .binary_search_by(|probe| self.timestamps[*probe].cmp(&t));

        match res {
            Ok(i) => {
                let mut k = i;

                for j in (0..i).rev() {
                    if self.timestamps[self.sorted_timestamps_index[j]] != t {
                        break;
                    }
                    k = j;
                }

                Ok(k)
            }
            Err(i) => Err(i),
        }
    }

    // TODO: revise this function, the current representation of the page that uses Triplet
    // is inneficient when looking up the adjacency list of a given vertex
    // since a page can represent the adjacency list of multiple vertices
    // might want to change the representation to include the source vertex separately
    pub fn tuples_window_for_source<'a, R: RangeBounds<Time>, F>(
        &'a self,
        w: R,
        source: &T,
        prefix: F,
    ) -> Box<dyn Iterator<Item = (Time, &T)> + '_>
    where
        F: Fn(&T) -> bool + 'a,
    {
        match self
            .sorted_values_index
            .binary_search_by(|probe| self.values[*probe].into_inner().cmp(source))
        {
            Ok(i) | Err(i) => {
                // the Err is temporary, we could make the page aware of the source vertex
                let r = self.range_bounds_to_range(w);

                // short circuit the entire function if the value of i is larger than N
                if i >= self.sorted_values_index.len() {
                    return Box::new(std::iter::empty());
                }

                let loc = self.sorted_values_index[i];
                let t_loc = self.timestamps[loc];

                // TODO: is this the right way to handle this?
                let loc_time_start_idx = self
                    .find_first_timestamp_position(t_loc)
                    .unwrap_or_else(|i| i);

                // println!("initial range: {:?} i: {i}, location in log {} time at loc {t_loc} time index for loc {loc_time_start_idx}", r, loc);

                // short circuit the entire function if the actual time is on the right side of the range
                if loc_time_start_idx > r.end {
                    return Box::new(std::iter::empty());
                }

                // find the actual time of the first occurrence of the value
                let start = if loc_time_start_idx > r.start && r.contains(&loc_time_start_idx) {
                    loc_time_start_idx
                } else {
                    r.start
                };

                let range = start..r.end;

                // println!("reduced range: {:?} i: {i}", range);
                Box::new(
                    self.sorted_timestamps_index[range]
                        .iter()
                        .map(move |idx| (self.timestamps[*idx], self.values[*idx].into_inner()))
                        .filter(move |(_, v)| prefix(v)),
                )
            }
        }
    }
}

impl<T: Ord, const N: usize> PageData for TemporalAdjacencySetPage<T, N> {
    fn new() -> Self {
        TemporalAdjacencySetPage::new()
    }

    fn is_full(&self) -> bool {
        self.is_full()
    }

    fn next_free_offset(&self) -> usize {
        self.values.len()
    }
}

#[cfg(test)]
mod vec_pages_tests {
    use crate::{graph::Triplet, pages::PageError};

    use super::*;

    #[test]
    fn page_with_zero_items_has_empty_window_iterator() {
        let page = TemporalAdjacencySetPage::<u64, 3>::new();
        let actual = page.tuples_window(3..12).collect::<Vec<_>>();
        assert_eq!(actual, vec![]);
    }

    #[test]
    fn page_with_one_item_test_window_iterator() -> Result<(), PageError> {
        let mut page = TemporalAdjacencySetPage::<u64, 3>::new();
        page.append_out(3, 3, 1)?;

        // the value is included in the window
        let actual = page.tuples_window(3..12).collect::<Vec<_>>();
        assert_eq!(actual, vec![(3, &3)]);
        // the value is on the right side of the window
        let actual = page.tuples_window(2..3).collect::<Vec<_>>();
        assert_eq!(actual, vec![]);
        // the value is on the left side of the window
        let actual = page.tuples_window(4..12).collect::<Vec<_>>();
        assert_eq!(actual, vec![]);
        Ok(())
    }

    // test window iterator on page with two items
    #[test]
    fn page_with_two_items_test_window_iterator() -> Result<(), PageError> {
        let mut page = TemporalAdjacencySetPage::<u64, 3>::new();
        page.append_out(3, 3, 5)?;
        page.append_out(12, 1, 7)?;

        // the first value is included in the window
        let actual = page.tuples_window(3..12).collect::<Vec<_>>();
        assert_eq!(actual, vec![(3, &3)]);
        // the second value is included in the window
        let actual = page.tuples_window(1..3).collect::<Vec<_>>();
        assert_eq!(actual, vec![(1, &12)]);
        // both values are included in the window
        let actual = page.tuples_window(1..12).collect::<Vec<_>>();
        assert_eq!(actual, vec![(1, &12), (3, &3)]);
        // both values are outside the window
        let actual = page.tuples_window(13..14).collect::<Vec<_>>();
        assert_eq!(actual, vec![]);
        // test inclusive bounds for first item
        let actual = page.tuples_window(2..=3).collect::<Vec<_>>();
        assert_eq!(actual, vec![(3, &3)]);
        // test exclusive bounds for first item
        let actual = page.tuples_window(2..3).collect::<Vec<_>>();
        assert_eq!(actual, vec![]);
        Ok(())
    }

    #[test]
    fn insert_two_items_check_page_is_full() -> Result<(), PageError> {
        let mut page = TemporalAdjacencySetPage::<u64, 2>::new();

        page.append_out(2, 2, 3)?; // add edge *->2 at time 2 on where 2 is on page 3

        assert!(!page.is_full());

        page.append_out(1, 1, 3)?;

        assert!(page.is_full());
        Ok(())
    }

    #[test]
    fn iterate_values_times_tuples_in_sorted_order_by_time() -> Result<(), PageError> {
        let mut page = TemporalAdjacencySetPage::<u64, 3>::new();

        page.append_out(9, 2, 4)?; // add edge * -> 9 at time 2 on page 4
        page.append_out(12, 1, 5)?; // add edge * -> 12 at time 1 on page 5
        page.append_out(0, 3, 8)?; // add edge * -> 0 at time 3 on page 8

        let pairs = page.tuples_by_timestamp().collect::<Vec<_>>();

        assert_eq!(
            pairs,
            vec![(1, 5, &12), (2, 4, &9), (3, 8, &0)]
        );
        Ok(())
    }

    #[test]
    fn iterate_values_times_tuples_in_sorted_order_by_values() -> Result<(), PageError> {
        let mut page = TemporalAdjacencySetPage::<u64, 3>::new();

        page.append_out(9, 2, 1)?;
        page.append_out(12, 1, 2)?;
        page.append_out(0, 3, 3)?;

        let pairs = page.tuples_sorted().collect::<Vec<_>>();

        assert_eq!(
            pairs,
            vec![(3, 3, &0), (2, 1, &9), (1, 2, &12)]
        );
        Ok(())
    }

    #[test]
    fn a_vertex_with_no_entries_results_in_empty_adjacency_list() -> Result<(), PageError> {
        let mut page = TemporalAdjacencySetPage::<Triplet<u64, String>, 3>::new();

        page.append_out(Triplet::new(2, 9, "friend".to_owned()), 1, 5)?; // add edge 2->9 at time 1, 9 is on page 5
        page.append_out(Triplet::new(2, 7, "co-worker".to_owned()), 3, 8)?; // add edge 2->7 at time 3 7 is on page 8
        page.append_out(Triplet::new(3, 6, "friend".to_owned()), 2, 2)?; // add edge 3->6 at time 2, 6 is on page 2
        page.append_out(Triplet::new(2, 6, "friend".to_owned()), 2, 2)?; // add edge 2->6 at time 2, 6 is on page 2

        let pairs = page
            .tuples_window_for_source(1..22, &Triplet::prefix(&19), |t| t.source == 19)
            .map(|(time, triplet)| (time, triplet.clone()))
            .collect::<Vec<_>>();

        assert_eq!(pairs, vec![]);
        Ok(())
    }

    #[test]
    fn non_overlapping_time_interval_results_in_empty_adjacency_list() -> Result<(), PageError> {
        let mut page = TemporalAdjacencySetPage::<Triplet<u64, String>, 3>::new();

        page.append_out(Triplet::new(2, 9, "friend".to_owned()), 1, 5)?;
        page.append_out(Triplet::new(2, 7, "co-worker".to_owned()), 3, 8)?;
        page.append_out(Triplet::new(3, 6, "friend".to_owned()), 2, 2)?;
        page.append_out(Triplet::new(2, 6, "friend".to_owned()), 2, 2)?;

        let pairs = page
            .tuples_window_for_source(-13..1, &Triplet::prefix(&2), |t| t.source == 2)
            .map(|(time, triplet)| (time, triplet.clone()))
            .collect::<Vec<_>>();

        assert_eq!(pairs, vec![]);
        Ok(())
    }

    #[test]
    fn interleaving_sources_can_find_the_right_window() -> Result<(), PageError> {
        let mut page = TemporalAdjacencySetPage::<Triplet<u64, String>, 3>::new();
        let source_n = 5;

        for i in 0..1000u32 {
            let t: Time = i.into();
            let source: u64 = (i % source_n).into();
            let vertex: u64 = (i % 100).into();
            page.append_out(
                Triplet::new(source, vertex, "friend".to_owned()),
                t,
                3,
            )?;
        }

        for source in 0..source_n {
            let source: u64 = source.into();
            let pairs = page
                .tuples_window_for_source(0..1000, &Triplet::prefix(&source), |t| {
                    t.source == source
                })
                .map(|(time, triplet)| (time, triplet.clone()))
                .collect::<Vec<_>>();

            for (time, triplet) in pairs {
                assert_eq!(triplet.source, source);
                assert!(time >= 0);
                assert!(time < 1000);
            }
        }
        Ok(())
    }

    #[test]
    fn iterate_values_by_window_and_source() -> Result<(), PageError> {
        let mut page = TemporalAdjacencySetPage::<Triplet<u64, String>, 3>::new();

        page.append_out(Triplet::new(1, 9, "friend".to_owned()), 1, 4)?;
        page.append_out(Triplet::new(1, 7, "co-worker".to_owned()), 3, 5)?;
        page.append_out(Triplet::new(2, 3, "friend".to_owned()), 2, 6)?;
        page.append_out(Triplet::new(1, 3, "friend".to_owned()), 2, 6)?;

        // first we get all the tuples for the [2..3] window
        let pairs = page
            .tuples_window(2..4)
            .map(|(time, triplet)| (time, triplet.clone()))
            .collect::<Vec<_>>();

        assert_eq!(
            pairs,
            vec![
                (2, Triplet::new(1, 3, "friend".to_string())),
                (2, Triplet::new(2, 3, "friend".to_string())),
                (3, Triplet::new(1, 7, "co-worker".to_string()))
            ]
        );

        // second we get all the tuples for the [2..3] window fir the source 1 prefix
        let pairs = page
            .tuples_window_for_source(2..=3, &Triplet::prefix(&1), |t| t.source == 1)
            .map(|(time, triplet)| (time, triplet.clone()))
            .collect::<Vec<_>>();

        assert_eq!(
            pairs,
            vec![
                (2, Triplet::new(1, 3, "friend".to_string())),
                (3, Triplet::new(1, 7, "co-worker".to_string()))
            ]
        );

        // third we get all the tuples for the [2..3] window fir the source 2 prefix
        let pairs = page
            .tuples_window_for_source(2..=3, &Triplet::prefix(&2), |t| t.source == 2)
            .map(|(time, triplet)| (time, triplet.clone()))
            .collect::<Vec<_>>();

        assert_eq!(pairs, vec![(2, Triplet::new(2, 3, "friend".to_string()))]);
        Ok(())
    }

    #[test]
    fn find_value() -> Result<(), PageError> {
        let mut page = TemporalAdjacencySetPage::<u64, 3>::new();

        page.append_out(9, 2, 8)?;
        page.append_out(12, 1, 7)?;
        page.append_out(0, 3, 6)?;

        let value = page.find_value(&12);

        assert_eq!(value, Some(&12));

        let value = page.find_value(&13);

        assert_eq!(value, None);
        Ok(())
    }

    #[test]
    fn direction_order_first_out_then_in() {

        let mut orders = vec![Direction::In(1), Direction::Out(2), Direction::In(3), Direction::Out(4), Direction::In(5), Direction::Out(6)];
        orders.sort();

        assert_eq!(orders, vec![Direction::Out(2), Direction::Out(4), Direction::Out(6), Direction::In(1), Direction::In(3), Direction::In(5)]);


    }
}
