use itertools::Itertools;
use crate::graph_window::{WindowedGraph, WindowedVertex};


fn get_unique_out_neighbours(v: &WindowedVertex) -> Vec<u64> {
    let mut all_out_neighbours: Vec<u64> = v.out_neighbours_ids().unique().collect();
    // remove self-loop
    if let Some(pos) = all_out_neighbours.iter().position(|x| *x == v.g_id) {
        all_out_neighbours.remove(pos);
    }
    all_out_neighbours
}

fn get_unique_in_neighbours(v: &WindowedVertex) -> Vec<u64> {
    let mut in_neighbours: Vec<u64>  = v.in_neighbours_ids().unique().collect();
    // remove self-loop
    if let Some(pos) = in_neighbours.iter().position(|x| *x == v.g_id) {
        in_neighbours.remove(pos);
    }
    in_neighbours
}

fn get_reciprocal_edge_count(v: &WindowedVertex) -> usize {
    let out_neighbours: Vec<u64> = get_unique_out_neighbours(&v);
    let in_neighbours: Vec<u64> = get_unique_in_neighbours(&v);
    out_neighbours.iter().filter(|x| in_neighbours.contains(x)).count()
}

pub fn global_reciprocity(graph: &WindowedGraph) -> f64 {
    let edges = graph
        .vertices()
        .fold((0,0), |acc, v| {
            (acc.0 + get_unique_out_neighbours(&v).len(),
             acc.1 + get_reciprocal_edge_count(&v))
        });
    (edges.1 as f64 / edges.0 as f64)
}

// Returns the reciprocity of every vertex in the grph as a tuple of
// vector id and the reciprocity
pub fn all_local_reciprocity(graph: &WindowedGraph) -> Vec<(u64, f64)> {
    let mut res = graph
        .vertices()
        .map(|v|
                 (v.g_id, local_reciprocity(&graph, v.g_id)))
        .collect::<Vec<(u64, f64)>>();
    res.sort_by(|a, b| a.0.cmp(&b.0));
    res
}


// Returns the reciprocity value of a single vertex
pub fn local_reciprocity(graph: &WindowedGraph, v: u64) -> f64 {
    let vertex = graph.vertex(v).unwrap();
    let out_neighbours: Vec<u64> = get_unique_out_neighbours(&vertex);
    let intersection = get_reciprocal_edge_count(&vertex);
    (intersection as f64 / out_neighbours.len() as f64)
}

#[cfg(test)]
mod reciprocity_test {
    use crate::algorithms::reciprocity;
    use crate::graph::Graph;
    use super::{local_reciprocity, all_local_reciprocity, global_reciprocity};

    #[test]
    fn check_all_reciprocities() {
        let g = Graph::new(1);
        let vs = vec![
            (1, 1, 2), (1, 1, 4),
            (1, 2, 3),
            (1, 3, 2), (1, 3, 1),
            (1, 4, 3), (1, 4, 1)];

        for (t, src, dst) in &vs {
            g.add_edge(*t, *src, *dst, &vec![]);
        }

        let windowed_graph = g.window(0, 2);
        let mut expected = 0.5;
        let mut actual = local_reciprocity(&windowed_graph, 1);
        assert_eq!(actual, expected);

        let mut expected: Vec<(u64, f64)> = vec![
            (1, 0.5),
            (2, 1.0),
            (3, 0.5),
            (4, 0.5)
        ];
        let mut actual = all_local_reciprocity(&windowed_graph);
        assert_eq!(actual, expected);

        let actual = global_reciprocity(&windowed_graph);
        let expected = 4.0 / 7.0;
        assert_eq!(actual, expected);
    }
}