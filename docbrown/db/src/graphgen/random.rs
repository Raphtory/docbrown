use crate::{graphdb::GraphDB};

pub fn random_graph(graph:GraphDB,edges:u64) -> GraphDB {
    use rand::seq::IteratorRandom;
    let mut rng = rand::thread_rng();
    graph.add_edge(1,2,1,&vec![]); //kick things off
    for i in 2..edges+1 {
        let prev_node = graph.vertices().choose(&mut rng).unwrap();
        graph.add_edge(i,prev_node,1,&vec![]);
    }
    graph
}

#[cfg(test)]
mod random_test {
    use super::*;
    #[test]
    fn graph_size() {
        let graph = GraphDB::new(2);
        let graph =random_graph(graph,10);
        assert_eq!(graph.edges_len(), 10);
    }
}