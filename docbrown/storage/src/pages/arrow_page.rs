// the arrow page is an enum with 2 variants
// one just a light wrapper over the TemporalAdjacencySetPage
// the second is an arrow2 structure with vertex_id, adjacency_list, timestamps and sorted_timestamps_index
// the sorted_timestamps_index is a vector of usize that is sorted by the timestamps vector
// it is used to create a BooleanArray that is used to filter the adjacency_list and timestamps vectors

use std::ops::Range;

use arrow2::{
    array::{ListArray, MutableListArray, MutablePrimitiveArray, PrimitiveArray, TryPush},
    types::NativeType,
};

use super::vec_page::TemporalAdjacencySetPage;

#[derive(Debug)]
pub(crate) struct ImmutableArrowPage<V>
where
    V: NativeType,
{
    vertex_id: PrimitiveArray<V>,
    // need the location of the adjacency list of the vertex on the other side (page_id)
    // need the label of the edge + the vertex on the other side
    // TODO: need the pointer to the edge properties page
    adj_list: ListArray<i32>,
    // these need to be the pointers to the pages where the other vertex is stored
    // adj_list_pointers: ListArray<i32>,
    timestamps: ListArray<i32>, // if we sort these do we need the sorted_timestamp_index?
    sorted_timestamps_index: ListArray<i32>,
}

impl<V> ImmutableArrowPage<V>
where
    V: NativeType + Ord + PartialEq + Copy,
{
    pub fn temporal_view(&self, w: Range<i64>) -> Self {
        for arr in &self.timestamps {
            println!("ARR {:?}", arr);
        }
        todo!()
    }
}
// generic From TemporaryAdjacencySetPage impl for ImmutableArrowPage
impl<V, E, const N: usize> From<TemporalAdjacencySetPage<(V, V, E), N>> for ImmutableArrowPage<V>
where
    V: NativeType + Ord + PartialEq + Copy,
    E: NativeType + Ord,
{
    fn from(page: TemporalAdjacencySetPage<(V, V, E), N>) -> Self {
        let mut vertex_ids = MutablePrimitiveArray::<V>::new();
        let mut timestamps = MutableListArray::<i32, MutablePrimitiveArray<i64>>::new();
        let mut sorted_timestamps = MutableListArray::<i32, MutablePrimitiveArray<u16>>::new();

        let mut cur: Option<V> = None;

        let mut adj_list = MutableListArray::<i32, MutablePrimitiveArray<V>>::new();

        let mut cur_adj_list: Vec<Option<V>> = vec![];
        let mut cur_timestamps: Vec<Option<i64>> = vec![];
        let mut cur_sorted_timestamps: Vec<Option<u16>> = vec![];

        // this for loop should return the tuples in order sorted by src
        // so we create the adjacency list and the timestamps list
        for (t, (src, dst, _)) in page.tuples_sorted() {
            println!("SRC {:?} DST {:?} T: {t}", src, dst);
            match cur.as_ref() {
                Some(v) if v == src => {
                    cur_adj_list.push(Some(*dst));
                    cur_sorted_timestamps.push(Some(cur_timestamps.len().try_into().unwrap()));
                    cur_timestamps.push(Some(t));
                }
                Some(_) => {
                    cur = Some(*src);
                    vertex_ids.try_push(Some(*src)).unwrap();

                    let mut a = vec![];
                    std::mem::swap(&mut cur_adj_list, &mut a);
                    adj_list.try_push(Some(a)).unwrap();

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
                    cur_sorted_timestamps.push(Some(cur_timestamps.len().try_into().unwrap()));
                    cur_timestamps.push(Some(t));
                }
                None => {
                    cur = Some(*src);
                    vertex_ids.try_push(Some(*src)).unwrap();

                    cur_adj_list.push(Some(*dst));
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
        timestamps.try_push(Some(cur_timestamps)).unwrap();
        sorted_timestamps.try_push(Some(cur_sorted_timestamps)).unwrap();

        Self {
            vertex_id: vertex_ids.into(),
            adj_list: adj_list.into(),
            timestamps: timestamps.into(),
            sorted_timestamps_index: sorted_timestamps.into(),
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
        page.append((1, 2, 5), 7).unwrap();
        page.append((2, 3, 6), 2).unwrap();
        page.append((1, 2, 7), 5).unwrap();
        page.append((1, 4, 8), 1).unwrap();
        let _arrow_page: ImmutableArrowPage<u64> = page.into();
        println!("{:?}", _arrow_page);
    }

    #[test]
    fn can_have_temporal_view() {
        let mut page = TemporalAdjacencySetPage::<(u64, u64, u64), 4>::new();
        page.append((1, 2, 5), 7).unwrap();
        page.append((2, 3, 6), 2).unwrap();
        page.append((1, 9, 7), 5).unwrap();
        page.append((1, 4, 8), 1).unwrap();
        let _arrow_page: ImmutableArrowPage<u64> = page.into();
        println!("{:?}", _arrow_page);
        _arrow_page.temporal_view(0..6);
    }
}
