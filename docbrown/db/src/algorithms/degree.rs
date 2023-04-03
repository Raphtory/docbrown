use crate::view_api::*;
use docbrown_core::tgraph_shard::errors::GraphError;

pub fn max_out_degree<G: GraphViewOps>(graph: &G) -> Result<usize, GraphError> {
    let r: Result<Vec<usize>, GraphError> = graph
        .vertices()
        .into_iter()
        .map(|v| v.out_degree())
        .collect();

    Ok(r?.into_iter().max().unwrap_or(0))
}

pub fn max_in_degree<G: GraphViewOps>(graph: &G) -> Result<usize, GraphError> {
    let r: Result<Vec<usize>, GraphError> = graph
        .vertices()
        .into_iter()
        .map(|v| v.in_degree())
        .collect();

    Ok(r?.into_iter().max().unwrap_or(0))
}

pub fn min_out_degree<G: GraphViewOps>(graph: &G) -> Result<usize, GraphError> {
    let r: Result<Vec<usize>, GraphError> = graph
        .vertices()
        .into_iter()
        .map(|v| v.out_degree())
        .collect();

    Ok(r?.into_iter().min().unwrap_or(0))
}

pub fn min_in_degree<G: GraphViewOps>(graph: &G) -> Result<usize, GraphError> {
    let r: Result<Vec<usize>, GraphError> = graph
        .vertices()
        .into_iter()
        .map(|v| v.in_degree())
        .collect();

    Ok(r?.into_iter().min().unwrap_or(0))
}

pub fn average_degree<G: GraphViewOps>(graph: &G) -> Result<f64, GraphError> {
    let r: Result<Vec<usize>, GraphError> =
        graph.vertices().into_iter().map(|v| v.degree()).collect();

    let degree_totals = r?
        .into_iter()
        .map(|i| (i as f64, 1.0))
        .fold((0.0, 0.0), |acc, elem| (acc.0 + elem.0, acc.1 + elem.1));

    Ok(degree_totals.0 / degree_totals.1)
}

#[cfg(test)]
mod degree_test {

    use crate::{
        algorithms::degree::{average_degree, max_in_degree, min_in_degree, min_out_degree},
        graph::Graph,
    };

    use super::max_out_degree;

    #[test]
    fn degree_test() {
        let g = Graph::new(1);
        let vs = vec![
            (1, 1, 2),
            (2, 1, 3),
            (3, 2, 1),
            (4, 3, 2),
            (5, 1, 4),
            (6, 4, 5),
        ];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![], None).unwrap();
        }

        let expected_max_out_degree = 3;
        let actual_max_out_degree = max_out_degree(&g).unwrap();

        let expected_max_in_degree = 2;
        let actual_max_in_degree = max_in_degree(&g).unwrap();

        let expected_min_out_degree = 0;
        let actual_min_out_degree = min_out_degree(&g).unwrap();

        let expected_min_in_degree = 1;
        let actual_min_in_degree = min_in_degree(&g).unwrap();

        let expected_average_degree = 2.0;
        let actual_average_degree = average_degree(&g).unwrap();

        assert_eq!(expected_max_out_degree, actual_max_out_degree);
        assert_eq!(expected_max_in_degree, actual_max_in_degree);
        assert_eq!(expected_min_out_degree, actual_min_out_degree);
        assert_eq!(expected_min_in_degree, actual_min_in_degree);
        assert_eq!(expected_average_degree, actual_average_degree);
    }
}
