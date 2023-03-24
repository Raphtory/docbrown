use crate::view_api::*;
use docbrown_core::tgraph_shard::exceptions::GraphError;
use itertools::Itertools;
use rayon::prelude::*;

pub fn global_triangle_count<G: GraphViewOps>(graph: &G) -> Result<usize, GraphError> {
    let r: Result<Vec<_>, _> = graph
        .vertices()
        .into_iter()
        .par_bridge()
        .map(|v| {
            let r: Result<Vec<_>, _> = v
                .neighbours()
                .id()
                .into_iter()
                .combinations(2)
                .filter_map(|nb| match graph.has_edge(nb[0], nb[1]) {
                    Ok(true) => Some(Ok(nb)),
                    Ok(false) => match graph.has_edge(nb[1], nb[0]) {
                        Ok(true) => Some(Ok(nb)),
                        Ok(false) => None,
                        Err(e) => Some(Err(e)),
                    },
                    Err(e) => Some(Err(e)),
                })
                .collect();
            r.map(|t| t.len())
        })
        .collect();

    let count: usize = r?.into_iter().sum();
    Ok(count / 3)
}

#[cfg(test)]
mod triangle_count_tests {
    use super::global_triangle_count;
    use crate::graph::Graph;

    #[test]
    fn counts_triangles() {
        let g = Graph::new(1);
        let vs = vec![(1, 1, 2), (2, 1, 3), (3, 2, 1), (4, 3, 2)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let windowed_graph = g.window(0, 5);
        let expected = 1;

        let actual = global_triangle_count(&windowed_graph).unwrap();

        assert_eq!(actual, expected);
    }
}
