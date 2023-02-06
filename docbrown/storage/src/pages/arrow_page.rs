// the arrow page is an enum with 2 variants
// one just a light wrapper over the TemporalAdjacencySetPage
// the second is an arrow2 structure with vertex_id, adjacency_list, timestamps and sorted_timestamps_index
// the sorted_timestamps_index is a vector of usize that is sorted by the timestamps vector
// it is used to create a BooleanArray that is used to filter the adjacency_list and timestamps vectors

use std::ops::Range;

use arrow2::{
    array::{ListArray, PrimitiveArray},
    types::NativeType,
};

use super::vec_page::TemporalAdjacencySetPage;

struct ImmutableArrowPage {
    vertex_id: PrimitiveArray<u64>,
    // need the location of the adjacency list of the vertex on the other side (page_id)
    // need the label of the edge + the vertex on the other side
    // TODO: need the pointer to the edge properties page
    adj_list: ListArray<i32>,
    // these need to be the pointers to the pages where the other vertex is stored
    adj_list_pointers: ListArray<i32>,
    timestamps: ListArray<i32>, // if we sort these do we need the sorted_timestamp_index?
    sorted_timestamps_index: Vec<usize>,
}

impl ImmutableArrowPage {
    // using the sorted_timestamps_index we can create a BooleanArray that is used to filter the adjacency_list and timestamps vectors
    pub fn temporal_projection(&self, r: Range<i64>) -> Self {
        todo!()
    }
}

// generic From TemporaryAdjacencySetPage impl for ImmutableArrowPage
// impl<V, E, const N: usize> From<vec::TemporalAdjacencySetPage<(V, V, E), N>>
//     for ImmutableArrowPage
// where V: NativeType, E: NativeType {
//     fn from(page: vec::TemporalAdjacencySetPage<(V, V, E), N>) -> Self {
//         todo!()
//     }
// }

enum ArrowPage<V: NativeType, E: NativeType, const N: usize> {
    Mutable(TemporalAdjacencySetPage<(V, V, E), N>),
    Immutable(ImmutableArrowPage),
}
