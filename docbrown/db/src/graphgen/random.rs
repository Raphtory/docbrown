use crate::graph::Graph;

pub fn random_graph(graph:Graph,edges:u64) -> Graph {
    use rand::seq::IteratorRandom;
    let mut rng = rand::thread_rng();
    graph.add_edge(1,2,1,&vec![]); //kick things off }
    for i in 2..edges+1 {
        let prev_node = graph.vertex_ids().choose(&mut rng).unwrap();
        graph.add_edge(1,prev_node,i,&vec![]);
    }
    graph
}

#[cfg(test)]
mod random_test {
    use super::*;
    #[test]
    fn graph_size() {
        let graph = Graph::new(2);
        let graph =random_graph(graph,10);
        assert_eq!(graph.edges_len(), 10);
    }
}
