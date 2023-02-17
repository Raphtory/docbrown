// use docbrown_core::error::GraphError;
// use docbrown_core::graphview::{GraphView, StateView};
// use docbrown_core::state::StateVec;
// use docbrown_core::vertexview::VertexViewMethods;
// use itertools::{izip, Itertools};
// use polars::prelude::*;
// use std::cmp::min;
// use std::iter::zip;

// pub fn triangle_count<G>(g: &G,id:u64) -> Result<usize, GraphError>
// where
//     G: GraphView + StateView,
// {

//     let vertex = g.vertex(id);
//     let neighbour_ids = vertex.neighbours().map(|n|n.id());

//    Ok(1)
// }


// #[cfg(test)]
// mod triangle_count_test {
//     use docbrown_core::*;
//     use docbrown_core::graphview::{MutableGraph, WindowedView};
//     use docbrown_core::singlepartitiongraph::SinglePartitionGraph;
//     use docbrown_core::vertexview::VertexViewMethods;
//     use crate::wcc::connected_components;
//     use docbrown_core::graphview::GraphView;
//     use itertools::Itertools;
//     use docbrown_core::graphview::StateView;
//     #[test]
//     fn triangle_count_test() {
        
//         let mut g = SinglePartitionGraph::default();
//         g.add_edge(1, 2, 0);
//         g.add_edge(2, 3, 0);
//         g.add_edge(3, 1, 0);
//         g.add_edge(3, 4, 0);
//         g.add_edge(4, 5, 0);
//         g.add_edge(5, 3, 0);

        
//         assert_eq!(v.get_property("cc_label").unwrap().extract(), Some(3))
//     }
// }
