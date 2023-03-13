use crate::graph::Graph;
use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::collections::HashSet;

pub fn duplication_divergence_graph(graph:&Graph, vertices_to_add:usize, probability_retain:f64){

    let mut random = rand::thread_rng();

    let mut latest_time = match graph.latest_time() {
        None => {0}
        Some(time) => {time}
    };
    let view = graph.window(i64::MIN,i64::MAX);
    let mut ids:Vec<u64> = view.vertex_ids().collect();

    let mut max_id = match ids.iter().max() {
        Some(id) => {*id},
        None=>0
    };

    for _ in 0..vertices_to_add {
        max_id += 1;
        latest_time += 1;
        let random_vertex = view.vertex_ids().choose(&mut random).unwrap();
        let poss_neighbours: Vec<u64> = view.neighbours_ids(random_vertex, docbrown_core::Direction::BOTH).collect();
        graph.add_vertex(latest_time, max_id, &vec![]);
        for nb in poss_neighbours {
            let r = random.gen::<f64>();
            if r < probability_retain {
                graph.add_edge(latest_time, max_id, nb, &vec![]);
            }
        }
    }

}

// pub fn duplication_divergence_seeded(graph:&Graph, vertices_to_add:usize, probability_retain:f64, random_seed:u64) {
//     let mut random = ChaCha8Rng::seed_from_u64(random_seed);
//     duplication_divergence_graph(graph, vertices_to_add, probability_retain, random)
// }

#[cfg(test)]
mod duplication_divergence_tests {
    use super::*;
    use crate::graphgen::preferential_attachment::ba_preferential_attachment;
    #[test]
    fn blank_graph() {
        let graph = Graph::new(2);
        ba_preferential_attachment(&graph,100,3);
        duplication_divergence_graph(&graph, 1000, 0.4);
        let window = graph.window(i64::MIN,i64::MAX);
                let mut degree:Vec<usize> =window.vertices()
            .map(|v| v.degree()).collect();
        // assert_eq!(graph.edges_len(), 10009);
        println!("{}",graph.edges_len());
        assert_eq!(graph.len(),1103);
    }
}