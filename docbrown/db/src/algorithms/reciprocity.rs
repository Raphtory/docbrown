use docbrown_core::tgraph_shard::exceptions::GraphError;

use crate::view_api::*;
use std::collections::HashSet;

fn get_reciprocal_edge_count<V: VertexViewOps>(v: &V) -> (u64, u64, u64) {
    let out_neighbours: HashSet<u64> = v.out_neighbours().id().filter(|x| *x != v.id()).collect();
    let in_neighbours: HashSet<u64> = v.in_neighbours().id().filter(|x| *x != v.id()).collect();
    (
        out_neighbours.len() as u64,
        in_neighbours.len() as u64,
        out_neighbours.intersection(&in_neighbours).count() as u64,
    )
}

pub fn global_reciprocity<G: GraphViewOps>(graph: &G) -> f64 {
    let edges = graph.vertices().into_iter().fold((0, 0), |acc, v| {
        let r_e_c = get_reciprocal_edge_count(&v);
        (acc.0 + r_e_c.0, acc.1 + r_e_c.2)
    });
    edges.1 as f64 / edges.0 as f64
}

// Returns the reciprocity of every vertex in the graph as a tuple of
// vector id and the reciprocity
pub fn all_local_reciprocity<G: GraphViewOps>(graph: &G) -> Result<Vec<(u64, f64)>, GraphError> {
    graph
        .vertices()
        .into_iter()
        .map(|v| {
            let lr = local_reciprocity(graph, v.id());
            lr.map(move |r| (v.id(), r))
        })
        .collect()
}

// Returns the reciprocity value of a single vertex
pub fn local_reciprocity<G: GraphViewOps>(graph: &G, v: u64) -> Result<f64, GraphError> {
    match graph.vertex(v)? {
        None => Ok(0 as f64),
        Some(vertex) => {
            let intersection = get_reciprocal_edge_count(&vertex);
            Ok(2.0 * intersection.2 as f64 / (intersection.0 + intersection.1) as f64)
        }
    }
}

#[cfg(test)]
mod reciprocity_test {
    use super::{all_local_reciprocity, global_reciprocity, local_reciprocity};
    use crate::graph::Graph;

    #[test]
    fn check_all_reciprocities() {
        let g = Graph::new(1);
        let vs = vec![
            (1, 1, 2),
            (1, 1, 4),
            (1, 2, 3),
            (1, 3, 2),
            (1, 3, 1),
            (1, 4, 3),
            (1, 4, 1),
            (1, 1, 5),
        ];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let windowed_graph = g.window(0, 2);
        let expected = 0.0;
        let actual = local_reciprocity(&windowed_graph, 5).unwrap();
        assert_eq!(actual, expected);

        let expected: Vec<(u64, f64)> =
            vec![(1, 0.4), (2, 2.0 / 3.0), (3, 0.5), (4, 2.0 / 3.0), (5, 0.0)];

        let mut actual = all_local_reciprocity(&windowed_graph).unwrap();
        actual.sort_by(|a, b| a.0.cmp(&b.0));
        assert_eq!(actual, expected);

        let actual = global_reciprocity(&windowed_graph);
        let expected = 0.5;
        assert_eq!(actual, expected);
    }
}
