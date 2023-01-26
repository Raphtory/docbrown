use std::ops::RangeBounds;

trait Page {
    fn page_id(&self) -> u64;
    fn is_full(&self) -> bool;
}

#[derive(Debug, PartialEq)]
enum PageError {
    PageFull,
}

trait TemporalAdjacencySetPage<T: Sized>: Page {
    fn append(&mut self, value: T, t: i64) -> Result<(), PageError>;

    fn tuples_window<R: RangeBounds<i64>>(&self, w: R) -> Box<dyn Iterator<Item = (i64, &T)> + '_>;

    fn tuples_by_timestamp(&self) -> Box<dyn Iterator<Item = (i64, &T)> + '_>;
    fn tuples_sorted(&self) -> Box<dyn Iterator<Item = (i64, &T)> + '_>;
    fn find_value(&self, value: &T) -> Option<&T>;
}

pub mod vec {
    use std::ops::RangeBounds;

    #[derive(Debug, PartialEq)]
    pub struct TemporalAdjacencySetPage<T, const N: usize> {
        pub page_id: usize,
        pub overflow_page_id: Option<usize>,
        pub page_size: usize,
        pub sorted_values_index: Vec<usize>, // these ids are sorted by the values in the values vector
        pub values: Vec<T>,
        pub sorted_timestamps_index: Vec<usize>, // these ids are sorted by the values in the timestamps vector
        pub timestamps: Vec<i64>,
    }

    impl<T: std::cmp::Ord, const N: usize> TemporalAdjacencySetPage<T, N> {
        pub fn new(page_id: usize) -> TemporalAdjacencySetPage<T, N> {
            TemporalAdjacencySetPage {
                page_id,
                overflow_page_id: None,
                page_size: N,
                sorted_values_index: Vec::with_capacity(N),
                values: Vec::with_capacity(N),
                sorted_timestamps_index: Vec::with_capacity(N),
                timestamps: Vec::with_capacity(N),
            }
        }

        pub(crate) fn overflow_page_id(&self) -> Option<usize> {
            self.overflow_page_id
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

        pub fn set_overflow_page_id(&mut self, overflow_page_id: usize) {
            self.overflow_page_id = Some(overflow_page_id);
        }

        pub fn append(&mut self, value: T, t: i64) {
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
        }

        pub fn is_full(&self) -> bool {
            self.values.len() == self.page_size
        }

        pub fn tuples_by_timestamp<'a>(&'a self) -> impl Iterator<Item = (i64, &'a T)> + 'a {
            self.sorted_timestamps_index
                .iter()
                .map(move |idx| (self.timestamps[*idx], &self.values[*idx]))
        }

        pub fn tuples_sorted<'a>(&'a self) -> impl Iterator<Item = (i64, &'a T)> + 'a {
            self.sorted_values_index
                .iter()
                .map(move |idx| (self.timestamps[*idx], &self.values[*idx]))
        }

        pub fn tuples_window<R: RangeBounds<i64>>(&self, w: R) -> impl Iterator<Item = (i64, &T)> {
            let (start, end) = match (w.start_bound(), w.end_bound()) {
                (std::ops::Bound::Included(start), std::ops::Bound::Included(end)) => {
                    (*start, *end + 1)
                }
                (std::ops::Bound::Included(start), std::ops::Bound::Excluded(end)) => {
                    (*start, *end)
                }
                (std::ops::Bound::Included(start), std::ops::Bound::Unbounded) => {
                    (*start, i64::MAX)
                }

                (std::ops::Bound::Excluded(start), std::ops::Bound::Included(end)) => {
                    (*start + 1, *end + 1)
                }
                (std::ops::Bound::Excluded(start), std::ops::Bound::Excluded(end)) => {
                    (*start + 1, *end)
                }
                (std::ops::Bound::Excluded(start), std::ops::Bound::Unbounded) => {
                    (*start, i64::MAX)
                }
                (std::ops::Bound::Unbounded, std::ops::Bound::Included(end)) => {
                    (i64::MIN, *end + 1)
                }
                (std::ops::Bound::Unbounded, std::ops::Bound::Excluded(end)) => (i64::MIN, *end),
                (std::ops::Bound::Unbounded, std::ops::Bound::Unbounded) => (i64::MIN, i64::MAX),
            };

            let start_idx = match self.find_timestamp_position(start) {
                Ok(i) | Err(i) => i,
            };

            let range = match self.find_timestamp_position(end) {
                Ok(i) | Err(i) => start_idx..i,
            };

            self.sorted_timestamps_index[range]
                .iter()
                .map(move |idx| (self.timestamps[*idx], &self.values[*idx]))
        }

        pub fn find_value(&self, value: &T) -> Option<&T> {
            match self
                .sorted_values_index
                .binary_search_by(|probe| self.values[*probe].cmp(value))
            {
                Ok(i) => Some(&self.values[self.sorted_values_index[i]]),
                Err(_) => None,
            }
        }

        fn find_timestamp_position(&self, t: i64) -> Result<usize, usize> {
            self.sorted_timestamps_index
                .binary_search_by(|probe| self.timestamps[*probe].cmp(&t))
        }
    }
}

pub mod graph {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use crate::vec;

    const PAGE_SIZE: usize = 4;

    #[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
    struct Triplet<V, E>(V, Option<V>, Option<E>);

    impl<V, E> Triplet<V, E> {
        fn new(v: V, source: V, e: E) -> Self {
            Self(v, Some(source), Some(e))
        }
    }

    #[derive(Debug, Default)]
    pub struct PagedGraph<V, E> {
        pages: Vec<vec::TemporalAdjacencySetPage<Triplet<V, E>, PAGE_SIZE>>,
        temporal_index: BTreeMap<i64, BTreeSet<usize>>,
        global_to_logical_map: HashMap<V, usize>,
    }

    impl<V, E> PagedGraph<V, E>
    where
        V: Ord + std::hash::Hash + Clone,
        E: Ord,
    {
        // we follow the chain of overflow pages until we find a page that is not full
        // if we reach the end of the chain, and the last page is perfectly full
        // we return Err(last_page_id) so we can create a new page and add it to the chain
        fn find_free_page(&self, page_id: usize) -> Result<usize, usize> {
            // FIXME: this is problematic as we'll be forced to lift every page in the chain, just to check if it's full
            let page = &self.pages[page_id];
            if !page.is_full() {
                return Ok(page_id);
            } else if let Some(overflow_page_id) = page.overflow_page_id() {
                return self.find_free_page(overflow_page_id);
            } else {
                return Err(page_id);
            }
        }

        pub fn add_outbound_edge(&mut self, t: i64, src: V, dst: V, e: E) {
            if let Some(page_idx) = self.global_to_logical_map.get(&src) {
                match self.find_free_page(*page_idx) {
                    Ok(page_idx) => {
                        let page = &mut self.pages[page_idx];
                        page.append(Triplet::new(src, dst, e), t);
                        self.temporal_index.entry(t).or_default().insert(page_idx);
                    }
                    Err(parent_page_idx) => {
                        let page_idx = self.pages.len();
                        let mut page =
                            vec::TemporalAdjacencySetPage::<Triplet<V, E>, PAGE_SIZE>::new(
                                page_idx,
                            );
                        page.append(Triplet::new(src, dst, e), t);
                        self.pages.push(page);
                        self.temporal_index.entry(t).or_default().insert(page_idx);
                        // now set this page as the overflow page of the parent page
                        self.pages[parent_page_idx].set_overflow_page_id(page_idx);
                    }
                }
            } else {
                let page_idx = self.pages.len();
                let mut page =
                    vec::TemporalAdjacencySetPage::<Triplet<V, E>, PAGE_SIZE>::new(page_idx);
                page.append(Triplet::new(src.clone(), dst.clone(), e), t);
                self.pages.push(page);
                self.temporal_index.entry(t).or_default().insert(page_idx);
                self.global_to_logical_map.insert(src, page_idx);
                // self.global_to_logical_map.insert(dst, page_idx);
            }
        }
    }
}

#[cfg(test)]
mod paged_graph_tests {
    use super::*;

    #[test]
    fn test_paged_graph() {
        let mut graph = graph::PagedGraph::<u64, u64>::default();
        graph.add_outbound_edge(1, 1, 2, 1);
        graph.add_outbound_edge(2, 1, 3, 1);
        graph.add_outbound_edge(3, 1, 4, 2);
        graph.add_outbound_edge(4, 2, 5, 2);
        graph.add_outbound_edge(5, 3, 6, 2);

        println!("{:?}", graph);
    }
}
#[cfg(test)]
mod vec_pages_tests {
    use super::*;

    #[test]
    fn page_with_zero_items_has_empty_window_iterator() {
        let page = vec::TemporalAdjacencySetPage::<u64, 3>::new(0);
        let actual = page.tuples_window(3..12).collect::<Vec<_>>();
        assert_eq!(actual, vec![]);
    }

    #[test]
    fn page_with_one_item_test_window_iterator() {
        let mut page = vec::TemporalAdjacencySetPage::<u64, 3>::new(0);
        page.append(3, 3);

        // the value is included in the window
        let actual = page.tuples_window(3..12).collect::<Vec<_>>();
        assert_eq!(actual, vec![(3, &3)]);
        // the value is on the right side of the window
        let actual = page.tuples_window(2..3).collect::<Vec<_>>();
        assert_eq!(actual, vec![]);
        // the value is on the left side of the window
        let actual = page.tuples_window(4..12).collect::<Vec<_>>();
        assert_eq!(actual, vec![]);
    }

    // test window iterator on page with two items
    #[test]
    fn page_with_two_items_test_window_iterator() {
        let mut page = vec::TemporalAdjacencySetPage::<u64, 3>::new(0);
        page.append(3, 3);
        page.append(12, 1);

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
    }

    #[test]
    fn insert_two_items_check_page_is_full() {
        let mut page = vec::TemporalAdjacencySetPage::<u64, 2>::new(0);

        page.append(2, 2);

        assert!(!page.is_full());

        page.append(1, 1);

        assert!(page.is_full());

        println!("{:?}", page);
    }

    #[test]
    fn iterate_values_times_tuples_in_sorted_order_by_time() {
        let mut page = vec::TemporalAdjacencySetPage::<u64, 3>::new(0);

        page.append(9, 2);
        page.append(12, 1);
        page.append(0, 3);

        let pairs = page.tuples_by_timestamp().collect::<Vec<_>>();

        assert_eq!(pairs, vec![(1, &12), (2, &9), (3, &0)]);
    }

    #[test]
    fn iterate_values_times_tuples_in_sorted_order_by_values() {
        let mut page = vec::TemporalAdjacencySetPage::<u64, 3>::new(0);

        page.append(9, 2);
        page.append(12, 1);
        page.append(0, 3);

        let pairs = page.tuples_sorted().collect::<Vec<_>>();

        assert_eq!(pairs, vec![(3, &0), (2, &9), (1, &12)]);
    }

    #[test]
    fn find_value() {
        let mut page = vec::TemporalAdjacencySetPage::<u64, 3>::new(0);

        page.append(9, 2);
        page.append(12, 1);
        page.append(0, 3);

        let value = page.find_value(&12);

        assert_eq!(value, Some(&12));

        let value = page.find_value(&13);

        assert_eq!(value, None);
    }
}
