// the arrow page is an enum with 2 variants
// one just a light wrapper over the TemporalAdjacencySetPage
// the second is an arrow2 structure with vertex_id, adjacency_list, timestamps and sorted_timestamps_index
// the sorted_timestamps_index is a vector of usize that is sorted by the timestamps vector
// it is used to create a BooleanArray that is used to filter the adjacency_list and timestamps vectors

use std::ops::{Deref, Range};

use arrow2::{
    array::{Array, ListArray, MutableListArray, MutablePrimitiveArray, PrimitiveArray, TryPush},
    chunk::Chunk,
    types::NativeType,
};
use itertools::izip;

use crate::{graph::Direction, Time};

use super::vec_page::TemporalAdjacencySetPage;

fn find_first_timestamp_position<A, B>(
    sorted_timestamps_index: B,
    timestamps: A,
    t: i64,
) -> Result<usize, usize>
where
    A: Deref<Target = [Time]>,
    B: Deref<Target = [u16]>,
{
    let res = sorted_timestamps_index.binary_search_by(|probe| {
        let i: usize = (*probe).into();
        timestamps[i].cmp(&t)
    });

    match res {
        Ok(i) => {
            let mut k = i;

            for j in (0..i).rev() {
                let ti: usize = sorted_timestamps_index[j].into();
                if timestamps[ti] != t {
                    break;
                }
                k = j;
            }

            Ok(k)
        }
        Err(i) => Err(i),
    }
}

fn find_last_timestamp_position<A, B>(
    sorted_timestamps_index: B,
    timestamps: A,
    t: i64,
) -> Result<usize, usize>
where
    A: Deref<Target = [Time]>,
    B: Deref<Target = [u16]>,
{
    let res = sorted_timestamps_index.binary_search_by(|probe| {
        let i: usize = (*probe).into();
        timestamps[i].cmp(&t)
    });

    match res {
        Ok(i) => {
            let mut k = i;

            for j in i..sorted_timestamps_index.len() {
                let ti: usize = sorted_timestamps_index[j].into();
                if timestamps[ti] != t {
                    break;
                }
                k = j;
            }

            Ok(k)
        }
        Err(i) => Err(i),
    }
}

pub(crate) fn temporal_index_range(
    t_index_maybe: Option<Box<dyn Array>>,
    t_maybe: Option<Box<dyn Array>>,
    w: &Range<Time>,
) -> Option<Range<usize>> {
    let a = t_index_maybe?;

    let t_index_arr = a
        .as_any()
        .downcast_ref::<PrimitiveArray<u16>>()?
        .values()
        .as_slice();

    let b = t_maybe?;

    let t = b
        .as_any()
        .downcast_ref::<PrimitiveArray<i64>>()?
        .values()
        .as_slice();

    let start = find_first_timestamp_position(t_index_arr, t, w.start);

    let end = find_last_timestamp_position(t_index_arr, t, w.end);

    let start_idx = match start {
        Ok(i) | Err(i) => usize::max(i, 0),
    };

    let end_idx = match end {
        Ok(i) | Err(i) => usize::min(i, t_index_arr.len()),
    };

    Some(start_idx..end_idx)
}

fn rebuild_index<T: Ord + NativeType, A>(arr: A) -> PrimitiveArray<u16>
where
    A: Deref<Target = [T]>,
{
    if arr.len() > u16::MAX as usize {
        panic!("too many elements in the array");
    }

    let mut index = arr
        .iter()
        .enumerate()
        .map(|(i, t)| (t, i))
        .collect::<Vec<_>>();

    index.sort_by(|a, b| a.0.cmp(&b.0));

    index
        .iter()
        .map(|(_, i)| Some(*i as u16))
        .collect::<MutablePrimitiveArray<u16>>()
        .into()
}

#[derive(Debug, PartialEq)]
pub(crate) struct ImmutableArrowPage<V>
where
    V: NativeType,
{
    vertex_id: PrimitiveArray<V>,
    adj_list_out: ListArray<i32>,
    adj_list_in: ListArray<i32>,

    pages_out: ListArray<i32>,
    pages_in: ListArray<i32>,

    timestamps_out: ListArray<i32>,
    timestamps_in: ListArray<i32>,

    sorted_timestamps_index_out: ListArray<i32>,
    sorted_timestamps_index_in: ListArray<i32>,
}

pub struct ImmutableArrowTemporalView<V>
where
    V: NativeType,
{
    w: Range<Time>,
    vertex_id: PrimitiveArray<V>,
    adj_list: ListArray<i32>,
    pages: ListArray<i32>,
    timestamps: ListArray<i32>,
    sorted_timestamps_index: ListArray<i32>,
}

impl<V> ImmutableArrowPage<V>
where
    V: NativeType + Ord + PartialEq + Copy,
{
    pub fn as_chunk(&self) -> Chunk<Box<dyn Array>> {
        Chunk::new(vec![self.vertex_id.to_boxed()])
    }

    pub fn temporal_view(&self, w: Range<Time>, d: Direction) -> ImmutableArrowTemporalView<V> {
        let mut timestamps = MutableListArray::<i32, MutablePrimitiveArray<Time>>::new();
        let mut sorted_timestamps = MutableListArray::<i32, MutablePrimitiveArray<u16>>::new();
        let mut adj_list_mut = MutableListArray::<i32, MutablePrimitiveArray<V>>::new();
        let mut pages_mut = MutableListArray::<i32, MutablePrimitiveArray<u32>>::new();

        let adj_list_iter = match d {
            Direction::Outbound => &self.adj_list_out,
            Direction::Inbound => &self.adj_list_in,
            Direction::Both => todo!(),
        };

        let timestamps_iter = match d {
            Direction::Outbound => &self.timestamps_out,
            Direction::Inbound => &self.timestamps_in,
            Direction::Both => todo!(),
        };

        let sorted_timestamps_index_iter = match d {
            Direction::Outbound => &self.sorted_timestamps_index_out,
            Direction::Inbound => &self.sorted_timestamps_index_in,
            Direction::Both => todo!(),
        };

        let pages_iter = match d {
            Direction::Outbound => &self.pages_out,
            Direction::Inbound => &self.pages_in,
            Direction::Both => todo!(),
        };

        izip!(
            adj_list_iter.iter(),
            timestamps_iter.iter(),
            sorted_timestamps_index_iter.iter(),
            pages_iter.iter()
        )
        .for_each(|(adj_list_opt, ts_opt, ts_index_opt, page_opt)| {
            if let Some(w) = temporal_index_range(ts_index_opt.clone(), ts_opt.clone(), &w) {
                // using arrow2 compute binary function use the w range to select only the items that match the temporal index

                let a = ts_index_opt.unwrap(); // this is because of the borrow checker
                let ts_index = a.as_any().downcast_ref::<PrimitiveArray<u16>>().unwrap();

                let b = ts_opt.unwrap(); // this is because of the borrow checker
                let ts = b.as_any().downcast_ref::<PrimitiveArray<Time>>().unwrap();

                let c = adj_list_opt.unwrap(); // this is because of the borrow checker
                let adj_list = c.as_any().downcast_ref::<PrimitiveArray<V>>().unwrap();

                let d = page_opt.unwrap(); // this is because of the borrow checker
                let page = d.as_any().downcast_ref::<PrimitiveArray<u32>>().unwrap();

                let new_t_index = ts_index.slice(w.start, w.end);

                let mut new_t_vec = MutablePrimitiveArray::<Time>::with_capacity(new_t_index.len());
                for i in new_t_index.iter() {
                    if let Some(i) = i {
                        let j: usize = (*i).into();
                        new_t_vec.push(Some(ts.values()[j]));
                    }
                }

                let mut new_adj_vec: MutablePrimitiveArray<V> =
                    MutablePrimitiveArray::<V>::with_capacity(new_t_index.len());

                let mut new_page_vec: MutablePrimitiveArray<u32> =
                    MutablePrimitiveArray::<u32>::with_capacity(new_t_index.len());

                for i in new_t_index.iter() {
                    if let Some(i) = i {
                        let j: usize = (*i).into();
                        new_adj_vec.push(Some(adj_list.values()[j]));
                        new_page_vec.push(Some(page.values()[j]));
                    }
                }

                let new_t: PrimitiveArray<Time> = new_t_vec.into();

                let new_t_index = rebuild_index(new_t.values().as_slice());

                let new_adj_vec: PrimitiveArray<V> = new_adj_vec.into();
                let new_page_vec: PrimitiveArray<u32> = new_page_vec.into();

                adj_list_mut
                    .try_push(Some(new_adj_vec))
                    .expect("could not push");

                pages_mut
                    .try_push(Some(new_page_vec))
                    .expect("could not push");

                timestamps.try_push(Some(new_t)).expect("could not push");

                sorted_timestamps
                    .try_push(Some(new_t_index))
                    .expect("could not push");
            }
        });

        ImmutableArrowTemporalView {
            w,
            vertex_id: self.vertex_id.clone(),
            adj_list: adj_list_mut.into(),
            timestamps: timestamps.into(),
            sorted_timestamps_index: sorted_timestamps.into(),
            pages: pages_mut.into(),
        }
    }

    fn extract_colums_adj_list<E: Ord, const N: usize>(
        page: &TemporalAdjacencySetPage<(V, V, E), N>,
        dir: Direction,
    ) -> Option<(
        PrimitiveArray<V>,
        ListArray<i32>,
        ListArray<i32>,
        ListArray<i32>,
        ListArray<i32>,
    )> {
        let mut vertex_ids = MutablePrimitiveArray::<V>::new();

        let mut timestamps = MutableListArray::<i32, MutablePrimitiveArray<i64>>::new();
        let mut sorted_timestamps = MutableListArray::<i32, MutablePrimitiveArray<u16>>::new();

        let mut cur: Option<V> = None;

        let mut adj_list = MutableListArray::<i32, MutablePrimitiveArray<V>>::new();
        let mut pages = MutableListArray::<i32, MutablePrimitiveArray<u32>>::new();

        let mut cur_adj_list: Vec<Option<V>> = vec![];
        let mut cur_pages: Vec<Option<u32>> = vec![];
        let mut cur_timestamps: Vec<Option<i64>> = vec![];
        let mut cur_sorted_timestamps: Vec<Option<u16>> = vec![];

        // this for loop should return the tuples in order sorted by src
        // so we create the adjacency list and the timestamps list
        let iter = match dir {
            Direction::Outbound => page.tuples_sorted_out(),
            Direction::Inbound => page.tuples_sorted_in(),
            _ => return None,
        };

        for (t, dst_page, (src, dst, _)) in iter {
            match cur.as_ref() {
                Some(v) if v == src => {
                    cur_adj_list.push(Some(*dst));
                    cur_pages.push(Some(dst_page));
                    cur_sorted_timestamps.push(Some(cur_timestamps.len().try_into().unwrap()));
                    cur_timestamps.push(Some(t));
                }
                Some(_) => {
                    cur = Some(*src);
                    vertex_ids.try_push(Some(*src)).unwrap();

                    let mut a = vec![];
                    std::mem::swap(&mut cur_adj_list, &mut a);
                    adj_list.try_push(Some(a)).unwrap();

                    let mut a = vec![];
                    std::mem::swap(&mut cur_pages, &mut a);
                    pages.try_push(Some(a)).unwrap();

                    cur_sorted_timestamps.sort_by(|i_t1, i_t2| {
                        let i1: usize = i_t1.unwrap().try_into().unwrap();
                        let i2: usize = i_t2.unwrap().try_into().unwrap();
                        let t1 = cur_timestamps[i1].unwrap();
                        let t2 = cur_timestamps[i2].unwrap();
                        t1.cmp(&t2)
                    });

                    let mut a = vec![];
                    std::mem::swap(&mut cur_timestamps, &mut a);
                    timestamps.try_push(Some(a)).unwrap();

                    let mut a: Vec<Option<u16>> = vec![];
                    std::mem::swap(&mut cur_sorted_timestamps, &mut a);
                    sorted_timestamps.try_push(Some(a)).unwrap();

                    cur_adj_list.push(Some(*dst));
                    cur_pages.push(Some(dst_page));
                    cur_sorted_timestamps.push(Some(cur_timestamps.len().try_into().unwrap()));
                    cur_timestamps.push(Some(t));
                }
                None => {
                    cur = Some(*src);
                    vertex_ids.try_push(Some(*src)).unwrap();

                    cur_adj_list.push(Some(*dst));
                    cur_pages.push(Some(dst_page));
                    cur_sorted_timestamps.push(Some(cur_timestamps.len().try_into().unwrap()));
                    cur_timestamps.push(Some(t));
                }
            }
        }

        cur_sorted_timestamps.sort_by(|i_t1, i_t2| {
            let i1: usize = i_t1.unwrap().try_into().unwrap();
            let i2: usize = i_t2.unwrap().try_into().unwrap();
            let t1 = cur_timestamps[i1].unwrap();
            let t2 = cur_timestamps[i2].unwrap();
            t1.cmp(&t2)
        });

        adj_list.try_push(Some(cur_adj_list)).unwrap();
        pages.try_push(Some(cur_pages)).unwrap();
        timestamps.try_push(Some(cur_timestamps)).unwrap();
        sorted_timestamps
            .try_push(Some(cur_sorted_timestamps))
            .unwrap();

        Some((
            vertex_ids.into(),
            adj_list.into(),
            pages.into(),
            timestamps.into(),
            sorted_timestamps.into(),
        ))
    }
}

impl<V, E, const N: usize> From<TemporalAdjacencySetPage<(V, V, E), N>> for ImmutableArrowPage<V>
where
    V: NativeType + Ord + PartialEq + Copy,
    E: NativeType + Ord,
{
    fn from(page: TemporalAdjacencySetPage<(V, V, E), N>) -> Self {
        let (vertex_id, adj_list_out, pages_out, timestamps_out, sorted_timestamps_out) =
            Self::extract_colums_adj_list(&page, Direction::Outbound)
                .expect("Error extracting columns from TemporalAdjacencySetPage");
        let (_, adj_list_in, pages_in, timestamps_in, sorted_timestamps_in) =
            Self::extract_colums_adj_list(&page, Direction::Inbound)
                .expect("Error extracting columns from TemporalAdjacencySetPage");

        Self {
            vertex_id,
            adj_list_out,
            pages_out,
            timestamps_out,
            sorted_timestamps_index_out: sorted_timestamps_out,
            adj_list_in,
            pages_in,
            timestamps_in,
            sorted_timestamps_index_in: sorted_timestamps_in,
        }
    }
}

pub(crate) enum ArrowPage<V: NativeType, E: NativeType, const N: usize> {
    Mutable(TemporalAdjacencySetPage<(V, V, E), N>),
    Immutable(ImmutableArrowPage<V>),
}

impl<V: NativeType, E: NativeType, const N: usize> ArrowPage<V, E, N> {}

#[cfg(test)]
mod arrow_page_tests {
    use super::*;

    #[test]
    fn can_create_immutable_arrow_page_from_temporal_adjacency_set_page() {
        let mut page = TemporalAdjacencySetPage::<(u64, u64, u64), 4>::new();
        page.append_out((1, 2, 5), 7, 3).unwrap();
        page.append_out((2, 3, 6), 2, 4).unwrap();
        page.append_out((1, 2, 7), 5, 3).unwrap();
        page.append_out((1, 4, 8), 1, 4).unwrap();
        let _arrow_page: ImmutableArrowPage<u64> = page.into();
        println!("{:?}", _arrow_page);
    }

    #[test]
    fn can_have_temporal_view_find_range_1() {
        let t: PrimitiveArray<Time> = vec![7, 1, 5].into_iter().map(Some).collect();
        let t_index: PrimitiveArray<u16> = vec![1, 2, 0].into_iter().map(Some).collect();

        let actual = temporal_index_range(Some(t_index.boxed()), Some(t.boxed()), &(0..6));
        assert_eq!(actual, Some(0..2))
    }

    #[test]
    fn can_have_temporal_view_find_range_2() {
        let init_vec = vec![1, 9, 2, 8, 3, 4, 5, 7, 6];
        let mut sorted_t_vec = init_vec.iter().enumerate().collect::<Vec<_>>();
        sorted_t_vec.sort_by(|(_, t1), (_, t2)| t1.cmp(t2));
        let sorted_t_vec: Vec<u16> = sorted_t_vec
            .iter()
            .map(|(i, _)| (*i).try_into().unwrap())
            .collect::<Vec<_>>();

        let t: PrimitiveArray<Time> = init_vec.clone().into_iter().map(Some).collect();

        let t_index: PrimitiveArray<u16> = sorted_t_vec.clone().into_iter().map(Some).collect();

        let actual = temporal_index_range(Some(t_index.boxed()), Some(t.boxed()), &(7..10));
        assert_eq!(actual, Some(6..9))
    }

    #[test]
    fn can_have_temporal_view() {
        let mut page = TemporalAdjacencySetPage::<(u64, u64, u64), 4>::new();
        page.append_out((1, 2, 5), 7, 3).unwrap();
        page.append_out((2, 3, 6), 2, 4).unwrap();
        page.append_out((1, 9, 7), 5, 3).unwrap();
        page.append_out((1, 4, 8), 1, 4).unwrap();
        let _arrow_page: ImmutableArrowPage<u64> = page.into();

        let actual = _arrow_page.temporal_view(0..6, Direction::Outbound);

        let mut page2 = TemporalAdjacencySetPage::<(u64, u64, u64), 4>::new();
        page2.append_out((2, 3, 6), 2, 4).unwrap();
        page2.append_out((1, 9, 7), 5, 3).unwrap();
        page2.append_out((1, 4, 8), 1, 4).unwrap();

        let expected: ImmutableArrowPage<u64> = page2.into();

        assert_eq!(actual.adj_list, expected.adj_list_out);
        assert_eq!(actual.pages, expected.pages_out);
        assert_eq!(actual.timestamps, expected.timestamps_out);
        assert_eq!(actual.sorted_timestamps_index, expected.sorted_timestamps_index_out);
    }
}
