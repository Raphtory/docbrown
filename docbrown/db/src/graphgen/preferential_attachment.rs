use crate::{graphdb::GraphDB};
use rand::prelude::*;
use rand::distributions::WeightedIndex;
use docbrown_core::Direction;
use crate::graphgen::random::random_graph;

pub fn preferential_attachment(graph:GraphDB,nodes:u64) -> GraphDB {
    let mut rng = rand::thread_rng();
    let max_id = graph.vertices().max().expect("Could not get max vertex ID of graph");
    let range = max_id+1..max_id+nodes+1;
    println!("{}",range.start);
    println!("{}",range.end);
    let mut nodes:Vec<u64> = graph.vertices().collect();
    let mut weights:Vec<usize> =graph.vertices()
        .map(|v| graph.degree(v, Direction::BOTH)).collect();

    for i in range {
        nodes.push(i);//add the new node
        weights.push(0); // make sure we can't add self loops
        //recreating this is heavy as all hell - may need to be rethought - goes from 4 secs to 200ms
        let dist = WeightedIndex::new(&weights).unwrap();
        //TODO do not select the same nodes
        for j in 0..3{
            let sample_index = dist.sample(&mut rng);
            let node = nodes[sample_index];
            graph.add_edge(i,node,1,&vec![]);
            weights[sample_index]+=1;
        }
        let new_node_pos = weights.len()-1;
        weights[new_node_pos]+=3;//Update the new node to have the correct weight
    }
    graph
}


//TODO need to add a known seed and test that a correct distribution is generated
//TODO need to benchmark the creation of these networks
#[cfg(test)]
mod preferential_attachment_tests {
    use super::*;
    #[test]
    fn graph_size() {
        let graph = GraphDB::new(2);
        let graph = random_graph(graph,10);
        let graph =preferential_attachment(graph,10000);
        let mut degree:Vec<usize> =graph.vertices()
            .map(|v| graph.degree(v, Direction::BOTH)).collect();
        degree.sort();
        println!("{:?}",degree);
        //TODO this fails for sure
        assert_eq!(graph.edges_len(), 10);
    }
}