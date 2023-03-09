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

pub fn global_reciprocity(graph: &WindowedGraph) {
    let num_unique_edges = graph
        .vertices()
        .map(|v|
            get_unique_out_neighbours(&v).len()
        ).sum::<usize>();
    println!("num_unique_edges: {}", num_unique_edges);

    let reciprocal_edges = graph
        .vertices()
        .map(|v|

    )


}

// Returns the reciprocity of every vertex in the grph as a tuple of
// vector id and the reciprocity
pub fn all_local_reciprocity(graph: &WindowedGraph) -> Vec<f64> {
    graph
        .vertices()
        .map(|v|
                 Tuple(v.g_id(), local_reciprocity(&graph, v.g_id)))
        .collect::<Vec<_>>()
}


// Returns the reciprocity value of a single vertex
pub fn local_reciprocity(graph: &WindowedGraph, v: u64) -> f64 {
    let vertex = graph.vertex(v).unwrap();

    let out_neighbours: Vec<u64> = get_unique_out_neighbours(&vertex);
    let in_neighbours: Vec<u64> = get_unique_in_neighbours(&vertex);
    let intersection = out_neighbours.iter().filter(|x| in_neighbours.contains(x)).count();

    (intersection as f64 / out_neighbours.len() as f64)
}

#[cfg(test)]
mod reciprocity_test {
    use crate::graph::Graph;
    use super::local_reciprocity;
    use super::global_reciprocity;

    #[test]
    fn check_local_reciprocity() {
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
        let expected = 0.5;
        let actual = local_reciprocity(&windowed_graph, 1);

        global_reciprocity(&windowed_graph);

        assert_eq!(actual, expected);
    }
}