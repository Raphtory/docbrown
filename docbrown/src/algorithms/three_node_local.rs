use std::collections::{HashMap, HashSet};

use crate::db::{
    graph::Graph,
    program::{GlobalEvalState, LocalState, Program},
    view_api::*,
};
use crate::algorithms::three_node_motifs::*;
use rustc_hash::FxHashMap;

pub fn twonode_motif_count<G:GraphViewOps>(graph:&G, v:u64) {
    if let Some(vertex) = graph.vertex(v) {
        let neighMap : HashMap<u64,usize> = vertex.neighbours().iter().enumerate().map(|(num,nb)| (nb.id(), num) ).into_iter().collect();
        neighMap.keys().for_each({|nb| let exploded_edges =  graph.edge(v, nb, layer);})
    }
}

#[cfg(test)]
mod local_motif_test {
    use crate::db::graph::Graph;
    use crate::algorithms::three_node_local::twonode_motif_count;

    #[test]
    fn test_init() {
        let graph = Graph::new(1);

        let vs = vec![
            (1, 2),
            (1, 4),
            (2, 3),
            (3, 2),
            (3, 1),
            (4, 3),
            (4, 1),
            (1, 5),
        ];
        
        for (src, dst) in &vs {
            graph.add_edge(0, *src, *dst, &vec![], None);
        }

        twonode_motif_count(&graph, 1);
    }
}