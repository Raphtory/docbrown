use docbrown_core::Direction;
use crate::graph_window::WindowedGraph;

pub fn local_triangle_count(windowed_graph: &WindowedGraph, v: u64) -> u32 {
    let mut number_of_triangles: u32 = 0;
    let vertex = windowed_graph.vertex(v).unwrap();

    if windowed_graph.has_vertex(v) && vertex.degree() >= 2 {
        for j in windowed_graph.neighbours_ids(v, Direction::BOTH) {
            for k in windowed_graph.neighbours_ids(j, Direction::BOTH) {
                windowed_graph.neighbours_ids(k, Direction::BOTH).for_each(|l| {
                    if l == v {
                        number_of_triangles += 1;
                    }
                }) 
            }
        }
    }
    number_of_triangles / 3
}

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
        let expected = vec![(1), (1), (1)];

        let actual = (1..=3)
            .map(|v| local_triangle_count(&windowed_graph, v))
            .collect::<Vec<_>>();

        assert_eq!(actual, expected);
    }
}
