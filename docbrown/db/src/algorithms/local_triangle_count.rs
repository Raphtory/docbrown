use std::collections::HashSet;
use itertools::Itertools;
use docbrown_core::Direction;
use crate::graph_window::WindowedGraph;

pub fn local_triangle_count(windowed_graph: &WindowedGraph, v: u64) -> u32 {
    let mut number_of_triangles: u32 = 0;
    let vertex = windowed_graph.vertex(v).unwrap();

    if windowed_graph.has_vertex(v) && vertex.degree() >= 2 {

        windowed_graph.neighbours_ids(v, Direction::BOTH).unique()
            .combinations(2)
            .for_each(|nb| {
                dbg!(&nb, &v);
                if windowed_graph.has_edge(nb[0], nb[1]) || (windowed_graph.has_edge(nb[1], nb[0])) {
                    number_of_triangles += 1;
                }
            })
    }
    number_of_triangles
}

// pub fn local_triangle_count(graph: &WindowedGraph, v: u64) -> u32 {
//     let mut number_of_triangles: u32 = 0;
//     let vertex = graph.vertex(v).unwrap();

//     if graph.has_vertex(v) && vertex.degree() >= 2 {
//             .combinations(2)
//             .for_each(|v| {
//                 if graph.has_edge(v[0], v[1]) || (graph.has_edge(v[1], v[0])) {
//                     number_of_triangles += 1;
//                 }
//             })
//     }
//     number_of_triangles / 3
// }

#[cfg(test)]
mod triangle_count_tests {

    use crate::graph::Graph;

    use super::local_triangle_count;

    #[test]
    fn counts_triangles() {
        let g = Graph::new(1);
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let windowed_graph = g.window(0, 5);
        // let expected = vec![(1), (1), (1)];
        let expected = 1;

        let actual = local_triangle_count(&windowed_graph, 1);
        // let actual = (1..=3)
        //     .map(|v| local_triangle_count(&windowed_graph, v))
        //     .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
