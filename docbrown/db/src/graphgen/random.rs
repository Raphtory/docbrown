use crate::{graphdb::GraphDB};



pub fn random_graph(shards:usize,size:u64) -> GraphDB {
    use crate::{graphdb::GraphDB};
    use rand::seq::IteratorRandom;
    let graph = GraphDB::new(shards);
    let mut rng = rand::thread_rng();
    graph.add_edge(1,2,1,Vec()); //kick things off
    for i in size-2 {
        let prev_node = graph.vertices().choose(&mut rng).unwrap();
        graph.add_edge(i,prev_node,1,Vec());
    }
    graph
}

#[cfg(test)]
mod random_test {
    use super::*;
    #[test]
    fn graph_size() {
        let graph =random_graph(1,10);
        assert_eq!(should_be_10, 10);
    }
}